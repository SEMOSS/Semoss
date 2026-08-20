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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.util.AssetUtility;
import prerna.util.ProjectSyncUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/** Coordinates project-level persistence for automation definitions and derived assets. */
public final class AutomationProjectUtils {

	private static final Logger classLogger = LogManager.getLogger(AutomationProjectUtils.class);

	private AutomationProjectUtils() {
		// utility class
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
		ReentrantLock projectLock = ProjectSyncUtility.getProjectLock(projectId);
		projectLock.lock();
		try {
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
