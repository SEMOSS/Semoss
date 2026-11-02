/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.automation.project;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.definition.AutomationNodeType;
import prerna.util.AssetUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;

/**
 * Coordinates project-level access, persistence, and derived assets for
 * Automation projects.
 *
 * <p>
 * All reads and mutations enter through standard SEMOSS project permission
 * checks. Mutations hold the project lock while replacing the graph aggregate,
 * regenerating MCP metadata, synchronizing cluster assets, and updating the
 * project edit timestamp.
 *
 * <p>
 * Runs of inline definition JSON have no project. They are stored under the
 * caller's own inline run scope, and only that login can read or control them.
 */
public final class AutomationProjectService {

	private static final Logger classLogger = LogManager.getLogger(AutomationProjectService.class);
	private static final String UPDATE_COMMIT_MESSAGE = "Update automation definition";

	private AutomationProjectService() {
	}

	/**
	 * Resolves a project ID or alias and requires view access to an automation
	 * project.
	 *
	 * @param user             user requesting access
	 * @param projectIdOrAlias project ID or alias
	 * @return resolved automation project
	 */
	public static IProject getViewableAutomationProject(User user, String projectIdOrAlias) {
		return getAutomationProject(user, projectIdOrAlias, false);
	}

	/**
	 * Resolves a project ID or alias and requires edit access to an automation
	 * project.
	 *
	 * @param user             user requesting access
	 * @param projectIdOrAlias project ID or alias
	 * @return resolved automation project
	 */
	public static IProject getEditableAutomationProject(User user, String projectIdOrAlias) {
		return getAutomationProject(user, projectIdOrAlias, true);
	}

	/**
	 * Returns the caller's own run scope for runs of inline definition JSON. It is
	 * derived from the primary login, so every session of one login shares it.
	 *
	 * @param user user running or reading inline runs
	 * @return {@link AutomationConstants#INLINE_RUN_SCOPE_PREFIX} plus a hash of
	 *         the login type and id
	 * @throws IllegalArgumentException for a missing, anonymous, or not logged-in
	 *                                  user
	 */
	public static String inlineRunScope(User user) {
		Pair<String, String> login = User.getPrimaryUserIdAndTypePair(user);
		if (login.getValue0() == null || login.getValue0().isBlank()) {
			throw new IllegalArgumentException("User must have primary login");
		}
		return AutomationConstants.INLINE_RUN_SCOPE_PREFIX
				+ AutomationDefinitionService.calculateSourceHash(login.getValue1() + ":" + login.getValue0());
	}

	/**
	 * Reports whether a run scope belongs to inline runs rather than a project.
	 *
	 * @param id run scope or project id
	 * @return {@code true} for an inline run scope
	 */
	public static boolean isInlineRunScope(String id) {
		return id != null && id.startsWith(AutomationConstants.INLINE_RUN_SCOPE_PREFIX);
	}

	/**
	 * Resolves the run scope a run reactor reads or controls. A blank project
	 * means the caller's own inline runs; an inline scope is accepted only when it
	 * is the caller's own; anything else must be an accessible automation project.
	 *
	 * @param user             user requesting access
	 * @param projectIdOrAlias project ID or alias, inline run scope, or blank
	 * @param requireEdit      whether a project must grant edit access
	 * @return run scope to match against {@code AUTOMATION_RUNS.PROJECT_ID}
	 */
	public static String resolveRunScope(User user, String projectIdOrAlias, boolean requireEdit) {
		if (projectIdOrAlias == null || projectIdOrAlias.isBlank()) {
			return inlineRunScope(user);
		}
		if (isInlineRunScope(projectIdOrAlias)) {
			if (!projectIdOrAlias.equals(inlineRunScope(user))) {
				throw new IllegalArgumentException("Run does not exist or user does not have access.");
			}
			return projectIdOrAlias;
		}
		return getAutomationProject(user, projectIdOrAlias, requireEdit).getProjectId();
	}

	private static IProject getAutomationProject(User user, String projectIdOrAlias, boolean requireEdit) {
		if (projectIdOrAlias == null || projectIdOrAlias.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id.");
		}

		String projectId = isInlineRunScope(projectIdOrAlias) ? projectIdOrAlias
				: SecurityProjectUtils.testUserProjectIdForAlias(user, projectIdOrAlias);
		// An inline run scope is never a project, even when an uploaded project claims that id.
		boolean hasAccess = !isInlineRunScope(projectId) && (requireEdit
				? SecurityProjectUtils.userCanEditProject(user, projectId)
				: SecurityProjectUtils.userCanViewProject(user, projectId));
		if (!hasAccess) {
			throw new IllegalArgumentException(requireEdit ? "Project does not exist or user does not have edit access."
					: "Project does not exist or user does not have access.");
		}

		IProject project = Utility.getProject(projectId);
		if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.AUTOMATION) {
			throw new IllegalArgumentException("Project is not an automation project: " + projectId);
		}
		return project;
	}

	/**
	 * Saves an automation definition and synchronizes its derived project assets.
	 *
	 * @param projectId      project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources    source by non-start node ID
	 * @param user           user performing the save
	 * @return persisted graph and node sources
	 */
	public static AutomationDefinitionService.DefinitionFiles saveDefinition(String projectId, String definitionJson,
			Map<String, String> nodeSources, User user) {
		return saveDefinition(projectId, definitionJson, nodeSources, null, user);
	}

	/**
	 * Loads and mutates one definition while holding the same project lock used by
	 * persistence. Granular authoring tools use this boundary so concurrent
	 * operations cannot overwrite each other's graph changes.
	 *
	 * @param projectId project ID
	 * @param mutation  operation based on the latest persisted aggregate
	 * @param <T>       mutation result type
	 * @return mutation result
	 */
	public static <T> T withLockedDefinition(String projectId,
			Function<AutomationDefinitionService.DefinitionFiles, T> mutation) {
		ReentrantLock projectLock = ProjectSyncUtility.getProjectLock(projectId);
		projectLock.lock();
		try {
			return mutation.apply(AutomationDefinitionService.load(projectId));
		} finally {
			projectLock.unlock();
		}
	}

	/**
	 * Saves an automation definition when the currently persisted aggregate matches
	 * the caller's expected revision. A null or blank revision preserves
	 * compatibility for callers that have not adopted optimistic concurrency yet.
	 *
	 * @param projectId        project ID
	 * @param definitionJson   canonical graph JSON
	 * @param nodeSources      source by non-start node ID
	 * @param expectedRevision revision returned by the prior read, or null during
	 *                         migration
	 * @param user             user performing the save
	 * @return persisted graph and node sources
	 */
	public static AutomationDefinitionService.DefinitionFiles saveDefinition(String projectId, String definitionJson,
			Map<String, String> nodeSources, String expectedRevision, User user) {
		return saveDefinition(projectId, definitionJson, nodeSources, expectedRevision, user, UPDATE_COMMIT_MESSAGE);
	}

	/**
	 * Saves an automation definition with an operation-specific project Git commit
	 * message. Package-owned operations such as version restore use this boundary so
	 * they retain the same locking, validation, and persistence behavior as an
	 * ordinary save.
	 *
	 * @param projectId        project ID
	 * @param definitionJson   canonical graph JSON
	 * @param nodeSources      source by non-start node ID
	 * @param expectedRevision revision returned by the prior read
	 * @param user             user performing the save
	 * @param commitMessage    project Git commit message
	 * @return persisted graph and node sources
	 */
	static AutomationDefinitionService.DefinitionFiles saveDefinition(String projectId, String definitionJson,
			Map<String, String> nodeSources, String expectedRevision, User user, String commitMessage) {
		if (commitMessage == null || commitMessage.isBlank()) {
			throw new IllegalArgumentException("Automation version commit message must be provided.");
		}
		ReentrantLock projectLock = ProjectSyncUtility.getProjectLock(projectId);
		projectLock.lock();
		try {
			AutomationDefinitionService.DefinitionFiles current = AutomationDefinitionService.load(projectId);
			if (expectedRevision != null && !expectedRevision.isBlank()) {
				String currentRevision = AutomationDefinitionService.calculateRevision(current.definition(),
						current.nodeSources());
				if (!currentRevision.equals(expectedRevision)) {
					throw new IllegalArgumentException(
							"Automation changed since it was loaded. Refresh and reapply your changes.");
				}
			}
			validateDefinitionReferences(AutomationDefinitionValidator.parseAndValidateForAuthoring(definitionJson),
					user);
			AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.save(projectId,
					definitionJson, nodeSources);
			AutomationMcpSync.sync(projectId, files.definition(), user);
			syncDefinitionAssets(projectId, user, commitMessage);
			// Return the same normalized aggregate a subsequent GetAutomation call will
			// read. This keeps the optimistic-concurrency token stable after Save.
			return AutomationDefinitionService.load(projectId);
		} finally {
			projectLock.unlock();
		}
	}

	/**
	 * Creates and synchronizes the starter definition for a new automation project.
	 *
	 * @param project automation project
	 * @param user    user creating the automation
	 * @return persisted starter graph and node sources
	 */
	public static AutomationDefinitionService.DefinitionFiles createStarterDefinition(IProject project, User user) {
		if (project == null) {
			throw new IllegalArgumentException("Automation project must not be null.");
		}
		String projectId = project.getProjectId();
		ReentrantLock projectLock = ProjectSyncUtility.getProjectLock(projectId);
		projectLock.lock();
		try {
			AutomationDefinitionService.createStarter(projectId);
			AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.load(projectId);
			AutomationMcpSync.sync(projectId, files.definition(), user);
			syncDefinitionAssets(project, projectId, user, "Create automation definition");
			return files;
		} finally {
			projectLock.unlock();
		}
	}

	/**
	 * Validates every catalog reference in an already parsed definition against the
	 * current user. Called on save and again immediately before execution because
	 * access and active state can change after an automation is authored.
	 *
	 * @param definition validated automation definition
	 * @param user       current user
	 */
	@SuppressWarnings("unchecked")
	public static void validateDefinitionReferences(AutomationDefinitionValidator.ValidatedDefinition definition,
			User user) {
		for (Map<String, Object> node : AutomationRuntime.allNodes(definition)) {
			String nodeType = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			AutomationNodeType typedNode = AutomationNodeType.fromType(nodeType);
			Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (!(rawConfig instanceof Map<?, ?>)) {
				continue;
			}
			Map<String, Object> config = (Map<String, Object>) rawConfig;
			if (typedNode == AutomationNodeType.AGENT_RUN) {
				validateAgentWorkspaceReference(node, config, user);
			} else if (typedNode == AutomationNodeType.APP_PIXEL) {
				validateAppProjectReference(node, config, user);
			}

			IEngine.CATALOG_TYPE expectedType = typedNode.getEngineType();
			if (expectedType == null) {
				continue;
			}
			Object rawEngineId = config.get(AutomationConstants.CONFIG_ENGINE_ID);
			if (!(rawEngineId instanceof String engineId) || engineId.isBlank()) {
				if (typedNode == AutomationNodeType.AGENT_RUN) {
					String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
					throw new IllegalArgumentException("Automation agent node '" + nodeId
							+ "' requires an execution model. Select an accessible MODEL engine before saving or running.");
				}
				continue;
			}
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw invalidEngineReference(nodeId, engineId, expectedType);
			}
			IEngine.CATALOG_TYPE actualType;
			try {
				actualType = SecurityEngineUtils.getEngineType(engineId);
			} catch (RuntimeException e) {
				throw invalidEngineReference(nodeId, engineId, expectedType);
			}
			if (actualType != expectedType) {
				throw new IllegalArgumentException("Automation node '" + nodeId + "' requires a " + expectedType
						+ " engine, but engineId '" + engineId + "' is a " + actualType + " engine.");
			}
			if (typedNode == AutomationNodeType.CONTROL_JEV) {
				Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
				if (typeAndSubtype.length < 2 || !"TYPESAFE".equalsIgnoreCase(String.valueOf(typeAndSubtype[1]))) {
					throw new IllegalArgumentException("Automation Jev decision node '" + nodeId
							+ "' requires a TYPESAFE model engine.");
				}
			}
			if (typedNode.getPermission() == AutomationNodeType.Permission.EDIT
					&& !SecurityEngineUtils.userCanEditEngine(user, engineId)) {
				throw new IllegalArgumentException("Automation node '" + nodeId + "' of type '" + nodeType
						+ "' requires edit access to engineId '" + engineId + "'.");
			}
		}
	}

	private static void validateAgentWorkspaceReference(Map<String, Object> node, Map<String, Object> config,
			User user) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Object rawWorkspaceId = config.get(AutomationConstants.CONFIG_WORKSPACE_ID);
		// Without a workspace the node runs a plain agent with the configured model.
		if (rawWorkspaceId == null || rawWorkspaceId instanceof String blank && blank.isBlank()) {
			return;
		}
		if (!(rawWorkspaceId instanceof String workspaceId)) {
			throw invalidAgentReference(nodeId, String.valueOf(rawWorkspaceId));
		}
		workspaceId = workspaceId.trim();
		if (!SecurityProjectUtils.userCanViewProject(user, workspaceId) || !IProject.PROJECT_TYPE.WORKSPACE.name()
				.equals(SecurityProjectUtils.getProjectTypeForId(workspaceId))) {
			throw invalidAgentReference(nodeId, workspaceId);
		}
		Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (workspace == null) {
			throw invalidAgentReference(nodeId, workspaceId);
		}
		if (!Boolean.TRUE.equals(workspace.get("is_active"))) {
			throw new IllegalArgumentException(
					"Automation node '" + nodeId + "' references disabled agent workspaceId '" + workspaceId + "'.");
		}
	}

	private static IllegalArgumentException invalidAgentReference(String nodeId, String workspaceId) {
		return new IllegalArgumentException("Automation node '" + nodeId + "' workspaceId '" + workspaceId
				+ "' is not an active accessible WORKSPACE agent. Call MyProjects with "
				+ "projectType=['WORKSPACE'] and use its project_id value.");
	}

	private static void validateAppProjectReference(Map<String, Object> node, Map<String, Object> config, User user) {
		Object rawAppId = config.get(AutomationConstants.CONFIG_APP_ID);
		if (rawAppId == null || rawAppId instanceof String appId && appId.isBlank()) {
			return;
		}
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		if (!(rawAppId instanceof String appId)) {
			throw invalidAppReference(nodeId, String.valueOf(rawAppId));
		}
		appId = appId.trim();
		if (!SecurityProjectUtils.userCanViewProject(user, appId)) {
			throw invalidAppReference(nodeId, appId);
		}
		String projectType = SecurityProjectUtils.getProjectTypeForId(appId);
		if (!(IProject.PROJECT_TYPE.CODE.name().equals(projectType)
				|| IProject.PROJECT_TYPE.BLOCKS.name().equals(projectType))) {
			throw invalidAppReference(nodeId, appId);
		}
	}

	private static IllegalArgumentException invalidAppReference(String nodeId, String appId) {
		return new IllegalArgumentException("Automation node '" + nodeId + "' appId '" + appId
				+ "' is not an accessible CODE or BLOCKS app. Call MyProjects with "
				+ "projectType=['CODE','BLOCKS'] and use its project_id value.");
	}

	private static IllegalArgumentException invalidEngineReference(String nodeId, String engineId,
			IEngine.CATALOG_TYPE expectedType) {
		return new IllegalArgumentException("Automation node '" + nodeId + "' engineId '" + engineId
				+ "' is not an accessible " + expectedType + " engine. Call MyEngines with engineTypes=['"
				+ expectedType + "'] and use its engine_id value.");
	}

	private static void syncDefinitionAssets(String projectId, User user, String commitMessage) {
		IProject project = Utility.getProject(projectId);
		syncDefinitionAssets(project, projectId, user, commitMessage);
	}

	private static void syncDefinitionAssets(IProject project, String projectId, User user, String commitMessage) {
		if (project == null) {
			classLogger.error("Unable to version automation definition because project {} is not available", projectId);
			throw new IllegalArgumentException("Automation project is unavailable for version history: " + projectId);
		}

		List<String> files = new ArrayList<>();
		for (Path path : AutomationDefinitionService.getArtifactPaths(projectId)) {
			files.add(path.toString());
		}
		Path mcpFile = Path.of(AssetUtility.getProjectAssetsFolder(projectId), "mcp", "pixel_mcp.json");
		if (Files.isRegularFile(mcpFile)) {
			files.add(mcpFile.toString());
		}

		String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		try {
			commitDefinitionAssets(versionFolder, files, commitMessage, user);
		} catch (RuntimeException e) {
			classLogger.error("Failed to version automation definition for project {}", projectId, e);
			throw e;
		}
		if (ClusterUtil.IS_CLUSTER) {
			try {
				ClusterUtil.pushProjectFolder(project, versionFolder);
			} catch (Exception e) {
				classLogger.warn("Cluster push failed for automation project {}", projectId, e);
			}
		}
		SecurityProjectUtils.updateProjectLastEditedDate(projectId);
	}

	/**
	 * Stages and commits Automation-owned project assets through the project's
	 * standard Git repository. This operation uses JGit directly because the shared
	 * convenience helpers intentionally log and suppress add/commit API failures,
	 * while version history must report a failed save to its caller.
	 *
	 * @param versionFolder project Git working directory
	 * @param files         Automation-owned absolute artifact paths
	 * @param commitMessage project Git commit message
	 * @param user          commit author
	 */
	static void commitDefinitionAssets(String versionFolder, List<String> files, String commitMessage, User user) {
		Set<String> ownedPaths = repositoryRelativePaths(versionFolder, files);
		try (Git git = Git.open(new File(versionFolder))) {
			AddCommand add = git.add();
			ownedPaths.forEach(add::addFilepattern);
			add.call();

			Status status = git.status().call();
			if (hasOwnedChanges(status.getModified(), ownedPaths) || hasOwnedChanges(status.getMissing(), ownedPaths)
					|| hasOwnedChanges(status.getUntracked(), ownedPaths)
					|| hasOwnedChanges(status.getConflicting(), ownedPaths)) {
				throw new IllegalArgumentException("Unable to stage Automation definition assets for version history.");
			}
			boolean hasStagedChanges = hasOwnedChanges(status.getAdded(), ownedPaths)
					|| hasOwnedChanges(status.getChanged(), ownedPaths)
					|| hasOwnedChanges(status.getRemoved(), ownedPaths);
			if (!hasStagedChanges) {
				return;
			}

			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			String author = accessToken == null ? null : accessToken.getResolvedUsername();
			String email = accessToken == null ? null : accessToken.getEmail();
			git.commit().setMessage(commitMessage).setAuthor(defaultIfBlank(author, "SEMOSS"),
					defaultIfBlank(email, "semoss@semoss.org")).call();
		} catch (IOException | GitAPIException | NoWorkTreeException e) {
			throw new IllegalArgumentException("Unable to commit Automation definition assets to version history.", e);
		}
	}

	private static Set<String> repositoryRelativePaths(String versionFolder, List<String> files) {
		Path repository = Path.of(versionFolder).toAbsolutePath().normalize();
		Set<String> relativePaths = new HashSet<>();
		for (String file : files) {
			Path artifact = Path.of(file).toAbsolutePath().normalize();
			if (!artifact.startsWith(repository)) {
				throw new IllegalArgumentException("Automation definition asset is outside the project version folder.");
			}
			relativePaths.add(repository.relativize(artifact).toString().replace(File.separatorChar, '/'));
		}
		return relativePaths;
	}

	private static boolean hasOwnedChanges(Collection<String> changedPaths, Set<String> ownedPaths) {
		for (String changedPath : changedPaths) {
			String normalizedChangedPath = changedPath.replace('\\', '/');
			for (String ownedPath : ownedPaths) {
				if (ownedPath.equals(normalizedChangedPath) || ownedPath.startsWith(normalizedChangedPath + "/")) {
					return true;
				}
			}
		}
		return false;
	}

	private static String defaultIfBlank(String value, String defaultValue) {
		return value == null || value.isBlank() ? defaultValue : value;
	}
}
