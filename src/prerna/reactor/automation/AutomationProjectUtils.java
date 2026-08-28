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
package prerna.reactor.automation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.util.AssetUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/** Coordinates project-level persistence for automation definitions and derived assets. */
public final class AutomationProjectUtils {

	private static final Logger classLogger = LogManager.getLogger(AutomationProjectUtils.class);
	private static final Set<String> EDIT_ENGINE_NODE_TYPES = Set.of(
			AutomationConstants.NODE_DATABASE_INSERT,
			AutomationConstants.NODE_DATABASE_UPDATE,
			AutomationConstants.NODE_STORAGE_READ,
			AutomationConstants.NODE_STORAGE_UPLOAD,
			AutomationConstants.NODE_STORAGE_DELETE,
			AutomationConstants.NODE_VECTOR_ADD,
			AutomationConstants.NODE_VECTOR_DELETE);

	private AutomationProjectUtils() {
		// utility class
	}

	/**
	 * Resolves a project ID or alias and requires view access to an automation project.
	 *
	 * @param user user requesting access
	 * @param projectIdOrAlias project ID or alias
	 * @return resolved automation project
	 */
	public static IProject getViewableAutomationProject(User user, String projectIdOrAlias) {
		return getAutomationProject(user, projectIdOrAlias, false);
	}

	/**
	 * Resolves a project ID or alias and requires edit access to an automation project.
	 *
	 * @param user user requesting access
	 * @param projectIdOrAlias project ID or alias
	 * @return resolved automation project
	 */
	public static IProject getEditableAutomationProject(User user, String projectIdOrAlias) {
		return getAutomationProject(user, projectIdOrAlias, true);
	}

	private static IProject getAutomationProject(User user, String projectIdOrAlias, boolean requireEdit) {
		if (projectIdOrAlias == null || projectIdOrAlias.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id.");
		}

		String projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectIdOrAlias);
		boolean hasAccess = requireEdit
				? SecurityProjectUtils.userCanEditProject(user, projectId)
				: SecurityProjectUtils.userCanViewProject(user, projectId);
		if (!hasAccess) {
			throw new IllegalArgumentException(requireEdit
					? "Project does not exist or user does not have edit access."
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
	 * @param projectId project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources source by non-start node ID
	 * @param user user performing the save
	 * @return persisted graph and node sources
	 */
	public static AutomationDefinitionService.DefinitionFiles saveDefinition(String projectId, String definitionJson,
			Map<String, String> nodeSources, User user) {
		return saveDefinition(projectId, definitionJson, nodeSources, null, user);
	}

	/**
	 * Loads and mutates one definition while holding the same project lock used by persistence.
	 * Granular authoring tools use this boundary so concurrent operations cannot overwrite each
	 * other's graph changes.
	 *
	 * @param projectId project ID
	 * @param mutation operation based on the latest persisted aggregate
	 * @param <T> mutation result type
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
	 * Saves an automation definition when the currently persisted aggregate matches the caller's
	 * expected revision. A null or blank revision preserves compatibility for callers that have not
	 * adopted optimistic concurrency yet.
	 *
	 * @param projectId project ID
	 * @param definitionJson canonical graph JSON
	 * @param nodeSources source by non-start node ID
	 * @param expectedRevision revision returned by the prior read, or null during migration
	 * @param user user performing the save
	 * @return persisted graph and node sources
	 */
	public static AutomationDefinitionService.DefinitionFiles saveDefinition(String projectId, String definitionJson,
			Map<String, String> nodeSources, String expectedRevision, User user) {
		ReentrantLock projectLock = ProjectSyncUtility.getProjectLock(projectId);
		projectLock.lock();
		try {
			AutomationDefinitionService.DefinitionFiles current = AutomationDefinitionService.load(projectId);
			if (expectedRevision != null && !expectedRevision.isBlank()) {
				String currentRevision = AutomationDefinitionService.calculateRevision(
						current.definition(), current.nodeSources());
				if (!currentRevision.equals(expectedRevision)) {
					throw new IllegalArgumentException(
							"Automation changed since it was loaded. Refresh and reapply your changes.");
				}
			}
			validateDefinitionReferences(
					AutomationDefinitionValidator.parseAndValidateForAuthoring(definitionJson), user);
			AutomationDefinitionService.DefinitionFiles files =
					AutomationDefinitionService.save(projectId, definitionJson, nodeSources);
			AutomationMcpSync.sync(projectId, files.definition(), user);
			syncDefinitionAssets(projectId, user, "Update automation definition");
			return files;
		} finally {
			projectLock.unlock();
		}
	}

	/**
	 * Creates and synchronizes the starter definition for a new automation project.
	 *
	 * @param project automation project
	 * @param user user creating the automation
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
	 * Validates every catalog reference in an already parsed definition against the current user.
	 * Called on save and again immediately before execution because access and active state can
	 * change after an automation is authored.
	 *
	 * @param definition validated automation definition
	 * @param user current user
	 */
	@SuppressWarnings("unchecked")
	public static void validateDefinitionReferences(
			AutomationDefinitionValidator.ValidatedDefinition definition, User user) {
		for (Map<String, Object> node : definition.nodes()) {
			String nodeType = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (!(rawConfig instanceof Map<?, ?>)) {
				continue;
			}
			Map<String, Object> config = (Map<String, Object>) rawConfig;
			if (AutomationConstants.NODE_AGENT_RUN.equals(nodeType)) {
				validateAgentWorkspaceReference(node, config, user);
			} else if (AutomationConstants.NODE_APP_PIXEL.equals(nodeType)) {
				validateAppProjectReference(node, config, user);
			}

			IEngine.CATALOG_TYPE expectedType = expectedEngineType(nodeType);
			if (expectedType == null) {
				continue;
			}
			Object rawEngineId = config.get(AutomationConstants.CONFIG_ENGINE_ID);
			if (!(rawEngineId instanceof String engineId) || engineId.isBlank()) {
				if (AutomationConstants.NODE_AGENT_RUN.equals(nodeType)) {
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
			if (EDIT_ENGINE_NODE_TYPES.contains(nodeType)
					&& !SecurityEngineUtils.userCanEditEngine(user, engineId)) {
				throw new IllegalArgumentException("Automation node '" + nodeId + "' of type '" + nodeType
						+ "' requires edit access to engineId '" + engineId + "'.");
			}
		}
	}

	private static void validateAgentWorkspaceReference(Map<String, Object> node,
			Map<String, Object> config, User user) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Object rawWorkspaceId = config.get(AutomationConstants.CONFIG_WORKSPACE_ID);
		if (!(rawWorkspaceId instanceof String workspaceId) || workspaceId.isBlank()) {
			throw invalidAgentReference(nodeId, String.valueOf(rawWorkspaceId));
		}
		workspaceId = workspaceId.trim();
		if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)
				|| !IProject.PROJECT_TYPE.WORKSPACE.name().equals(
						SecurityProjectUtils.getProjectTypeForId(workspaceId))) {
			throw invalidAgentReference(nodeId, workspaceId);
		}
		Map<String, Object> workspace = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (workspace == null) {
			throw invalidAgentReference(nodeId, workspaceId);
		}
		if (!Boolean.TRUE.equals(workspace.get("is_active"))) {
			throw new IllegalArgumentException("Automation node '" + nodeId
					+ "' references disabled agent workspaceId '" + workspaceId + "'.");
		}
	}

	private static IllegalArgumentException invalidAgentReference(String nodeId, String workspaceId) {
		return new IllegalArgumentException("Automation node '" + nodeId + "' workspaceId '" + workspaceId
				+ "' is not an active accessible WORKSPACE agent. Call MyProjects with "
				+ "projectType=['WORKSPACE'] and use its project_id value.");
	}

	private static void validateAppProjectReference(Map<String, Object> node,
			Map<String, Object> config, User user) {
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

	private static IEngine.CATALOG_TYPE expectedEngineType(String nodeType) {
		if (nodeType == null) {
			return null;
		}
		if (nodeType.startsWith("database.")) {
			return IEngine.CATALOG_TYPE.DATABASE;
		}
		if (nodeType.startsWith("model.") || AutomationConstants.NODE_AGENT_RUN.equals(nodeType)) {
			return IEngine.CATALOG_TYPE.MODEL;
		}
		if (nodeType.startsWith("storage.")) {
			return IEngine.CATALOG_TYPE.STORAGE;
		}
		if (nodeType.startsWith("vector.")) {
			return IEngine.CATALOG_TYPE.VECTOR;
		}
		if (AutomationConstants.NODE_FUNCTION_EXECUTE.equals(nodeType)) {
			return IEngine.CATALOG_TYPE.FUNCTION;
		}
		return null;
	}

	private static IllegalArgumentException invalidEngineReference(String nodeId, String engineId,
			IEngine.CATALOG_TYPE expectedType) {
		return new IllegalArgumentException("Automation node '" + nodeId + "' engineId '" + engineId
				+ "' is not an accessible " + expectedType
				+ " engine. Call MyEngines with engineTypes=['" + expectedType + "'] and use its engine_id value.");
	}

	private static void syncDefinitionAssets(String projectId, User user, String commitMessage) {
		IProject project = Utility.getProject(projectId);
		syncDefinitionAssets(project, projectId, user, commitMessage);
	}

	private static void syncDefinitionAssets(IProject project, String projectId, User user, String commitMessage) {
		if (project == null) {
			classLogger.warn("Project {} not found in registry; skipping automation asset synchronization", projectId);
			SecurityProjectUtils.updateProjectLastEditedDate(projectId);
			return;
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
			GitRepoUtils.addSpecificFiles(versionFolder, files);
			GitRepoUtils.commitAddedFiles(versionFolder, commitMessage, user);
		} catch (Exception e) {
			classLogger.warn("Git commit failed for automation project {}", projectId, e);
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
}
