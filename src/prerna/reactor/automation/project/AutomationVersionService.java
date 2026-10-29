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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.EmptyTreeIterator;
import org.eclipse.jgit.treewalk.TreeWalk;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.util.AssetUtility;
import prerna.util.git.GitAssetUtils;

/**
 * Reads and restores saved Automation definitions from the Automation project's
 * standard Git repository.
 *
 * <p>
 * Version identifiers are full commit SHAs reachable from the repository's
 * current {@code HEAD}. A restore never rewinds repository history; it persists
 * the historical aggregate through {@link AutomationProjectService}, producing
 * a new forward commit after optimistic-concurrency and reference validation.
 */
final class AutomationVersionService {

	private static final Logger classLogger = LogManager.getLogger(AutomationVersionService.class);

	private static final String WORKFLOW_GIT_PATH = "assets/"
			+ AutomationConstants.AUTOMATION_PYTHON_DEFINITION_FILE_NAME;
	private static final String NODES_GIT_PREFIX = "assets/"
			+ AutomationConstants.AUTOMATION_NODE_SOURCES_FOLDER_NAME + "/";
	private static final String RESTORE_COMMIT_PREFIX = "Restore automation definition from ";

	private AutomationVersionService() {
	}

	/**
	 * Lists newest-first commits that changed the Automation graph or its owned
	 * developer-node sources.
	 *
	 * @param project Automation project
	 * @param limit   maximum number of matching versions to return
	 * @return bounded commit metadata with full SHA identifiers
	 */
	static List<Map<String, Object>> listVersions(IProject project, int limit) {
		String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
		try (Git git = Git.open(new File(versionFolder))) {
			Repository repository = git.getRepository();
			if (repository.resolve(Constants.HEAD) == null) {
				return List.of();
			}
			Map<String, RevCommit> matchingCommits = new LinkedHashMap<>();
			for (String path : List.of(WORKFLOW_GIT_PATH,
					NODES_GIT_PREFIX.substring(0, NODES_GIT_PREFIX.length() - 1))) {
				for (RevCommit commit : git.log().addPath(path).setMaxCount(limit).call()) {
					matchingCommits.putIfAbsent(commit.getId().name(), commit);
				}
			}
			List<RevCommit> orderedCommits = new ArrayList<>(matchingCommits.values());
			orderedCommits.sort(Comparator.comparingInt(RevCommit::getCommitTime).reversed()
					.thenComparing(commit -> commit.getId().name()));

			List<Map<String, Object>> versions = new ArrayList<>();
			for (int index = 0; index < Math.min(limit, orderedCommits.size()); index++) {
				RevCommit commit = orderedCommits.get(index);
				Map<String, Object> version = new LinkedHashMap<>();
				version.put("date", GitAssetUtils.getDate(commit.getCommitTime() * 1_000L));
				version.put("user", commit.getAuthorIdent().getName());
				version.put("message", commit.getFullMessage());
				version.put("id", commit.getId().name());
				versions.add(version);
			}
			return versions;
		} catch (Exception e) {
			classLogger.error("Failed to list versions for automation project {}", project.getProjectId(), e);
			throw new IllegalArgumentException("Unable to read automation version history.", e);
		}
	}

	/**
	 * Restores one historical aggregate using the normal project save path.
	 *
	 * @param project          editable Automation project
	 * @param revision         full SHA returned by {@link #listVersions(IProject, int)}
	 * @param expectedRevision current aggregate revision returned by GetAutomation
	 * @param user             user performing the restore
	 * @return newly persisted aggregate
	 */
	static AutomationDefinitionService.DefinitionFiles restore(IProject project, String revision,
			String expectedRevision, User user) {
		if (revision == null || !revision.matches("[0-9a-fA-F]{40}")) {
			throw new IllegalArgumentException("revision must be a full commit SHA returned by ListAutomationVersions.");
		}
		if (expectedRevision == null || expectedRevision.isBlank()) {
			throw new IllegalArgumentException("expectedRevision must be provided.");
		}

		String projectId = project.getProjectId();
		String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		String definitionJson;
		Map<String, String> nodeSources = new LinkedHashMap<>();
		try (Git git = Git.open(new File(versionFolder))) {
			Repository repository = git.getRepository();
			RevCommit target = requireReachableCommit(repository, revision);
			if (!isAutomationVersion(repository, target)) {
				throw new IllegalArgumentException("Revision is not an Automation definition version.");
			}
			definitionJson = readFile(repository, target, WORKFLOW_GIT_PATH);
			if (definitionJson == null || definitionJson.isBlank()) {
				throw new IllegalArgumentException("No automation definition found at revision: " + revision);
			}

			AutomationDefinitionValidator.ValidatedDefinition definition = AutomationDefinitionValidator
					.parseAndValidateForAuthoring(definitionJson);
			for (Map<String, Object> node : AutomationRuntime.allNodes(definition)) {
				if (!AutomationConstants.NODE_DEVELOPER_PYTHON
						.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
					continue;
				}
				String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
				String path = NODES_GIT_PREFIX + AutomationDefinitionService.safeNodeFileName(node) + ".py";
				String source = readFile(repository, target, path);
				if (source == null) {
					throw new IllegalArgumentException(
							"Developer source for automation node '" + nodeId + "' is unavailable at revision: "
									+ revision);
				}
				nodeSources.put(nodeId, source);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to load revision {} for automation project {}", revision, projectId, e);
			throw new IllegalArgumentException("Unable to restore the selected automation version.", e);
		}

		// saveDefinition rechecks the current aggregate while holding the project lock,
		// validates current resource access, and writes a new project Git commit.
		return AutomationProjectService.saveDefinition(projectId, definitionJson, nodeSources, expectedRevision, user,
				RESTORE_COMMIT_PREFIX + revision);
	}

	private static RevCommit requireReachableCommit(Repository repository, String revision) throws IOException {
		ObjectId headId = repository.resolve(Constants.HEAD);
		if (headId == null) {
			throw new IllegalArgumentException("Automation project has no saved versions.");
		}
		ObjectId targetId;
		try {
			targetId = ObjectId.fromString(revision);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid automation revision.", e);
		}
		try (RevWalk walk = new RevWalk(repository)) {
			RevCommit head = walk.parseCommit(headId);
			RevCommit target;
			try {
				target = walk.parseCommit(targetId);
			} catch (IOException e) {
				throw new IllegalArgumentException("Automation revision does not exist.", e);
			}
			if (!walk.isMergedInto(target, head)) {
				throw new IllegalArgumentException("Automation revision is not part of the current project history.");
			}
			return target;
		}
	}

	private static boolean changesAutomationDefinition(RevWalk walk, ObjectReader reader, DiffFormatter formatter,
			RevCommit commit) throws IOException {
		AbstractTreeIterator previous = new EmptyTreeIterator();
		if (commit.getParentCount() > 0) {
			RevCommit parent = walk.parseCommit(commit.getParent(0).getId());
			previous = treeIterator(reader, parent);
		}
		for (DiffEntry entry : formatter.scan(previous, treeIterator(reader, commit))) {
			if (isAutomationPath(entry.getOldPath()) || isAutomationPath(entry.getNewPath())) {
				return true;
			}
		}
		return false;
	}

	private static boolean isAutomationVersion(Repository repository, RevCommit commit) throws IOException {
		try (RevWalk walk = new RevWalk(repository);
				ObjectReader reader = repository.newObjectReader();
				DiffFormatter formatter = new DiffFormatter(new ByteArrayOutputStream())) {
			formatter.setRepository(repository);
			return changesAutomationDefinition(walk, reader, formatter, commit);
		}
	}

	private static CanonicalTreeParser treeIterator(ObjectReader reader, RevCommit commit) throws IOException {
		CanonicalTreeParser parser = new CanonicalTreeParser();
		parser.reset(reader, commit.getTree().getId());
		return parser;
	}

	private static boolean isAutomationPath(String path) {
		return WORKFLOW_GIT_PATH.equals(path) || path.startsWith(NODES_GIT_PREFIX);
	}

	private static String readFile(Repository repository, RevCommit commit, String path) throws IOException {
		try (TreeWalk tree = TreeWalk.forPath(repository, path, commit.getTree())) {
			if (tree == null) {
				return null;
			}
			ObjectLoader loader = repository.open(tree.getObjectId(0));
			return new String(loader.getBytes(), StandardCharsets.UTF_8);
		}
	}
}
