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
package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateAppFromTemplateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateAppFromTemplateReactor.class);

	private static final String CLASS_NAME = CreateAppFromTemplateReactor.class.getName();

	/*
	 * This class is used to construct a new project using an existing project as a
	 * template. It can be considered a deep copy in that all insights from the
	 * template are also copied to the new project
	 */

	public CreateAppFromTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.PROJECT_TEMPLATE.getKey(),
				ReactorKeysEnum.GLOBAL.getKey(), ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.URL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();
		int index = 0;
		String newProjectName = this.keyValue.get(this.keysToGet[index++]);
		String projectTemplateId = this.keyValue.get(this.keysToGet[index++]);
		boolean global = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[index++]) + "");
		String gitProvider = this.keyValue.get(this.keysToGet[index++]);
		String gitCloneUrl = this.keyValue.get(this.keysToGet[index++]);

		// make sure valid id for user
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectTemplateId)) {
			// you dont have access
			throw new IllegalArgumentException(
					"The template you are attempting to use does not exist or user does not have access to the project");
		}
		// Instantiate an object using the projectTemplate
		IProject templateProject = Utility.getProject(projectTemplateId);

		// Use the template to populate the parameters needed to create the new project
		IProject.PROJECT_TYPE projectEnumType = templateProject.getProjectType();

		// Create new project
		IProject newProject = ProjectHelper.generateNewProject(newProjectName, projectEnumType, global, gitProvider,
				gitCloneUrl, this.insight.getUser(), logger);

		String templateProjectVersionFolder = AssetUtility.getProjectVersionFolder(templateProject.getProjectName(),
				projectTemplateId);
		String newProjectVersionFolder = AssetUtility.getProjectVersionFolder(newProjectName,
				newProject.getProjectId());

		// now we just need to move over the files for assets
		String templateProjectAssetFolder = AssetUtility.getProjectAssetsFolder(projectTemplateId);
		String newProjectAssetFolder = AssetUtility.getProjectAssetsFolder(newProject.getProjectId());

		Path sourceDir = Paths.get(templateProjectAssetFolder);
		Path destinationDir = Paths.get(newProjectAssetFolder);

		try {
			try (Stream<Path> stream = Files.walk(sourceDir)) {
				stream.forEach(sourcePath -> {
					try {
						Path targetPath = destinationDir.resolve(sourceDir.relativize(sourcePath));
						if (sourcePath.toFile().isFile()) {
							targetPath.toFile().getParentFile().mkdirs();
							Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
						}
					} catch (IOException ex) {
						classLogger.error("Failed to copy project template assets: {}", ex.getMessage(), ex);
					}
				});
			}

			// also see for image
			File templateImage = new File(templateProjectVersionFolder, "image.png");
			if (templateImage.exists()) {
				Files.copy(templateImage.toPath(), new File(newProjectVersionFolder, "image.png").toPath(),
						java.nio.file.StandardCopyOption.REPLACE_EXISTING);
			}

			if (ClusterUtil.IS_CLUSTER) {
				logger.info("Syncing project for cloud backup");
				ClusterUtil.pushProjectFolder(newProject, newProjectVersionFolder);
			}
		} catch (IOException e) {
			classLogger.error("Failed to copy project template assets: {}", e.getMessage(), e);
			throw new IllegalArgumentException(
					"New project was created but could not transfer over the assets from the template. Errror = "
							+ e.getMessage());
		}

		// If the template is a WORKSPACE (agent), clone the inference-log workspace
		// entry and CONFIG_JSON so the new agent inherits all configuration:
		// system prompt, model selection, MCPs, skills, budgets, hooks, subagents.
		if (IProject.PROJECT_TYPE.WORKSPACE == projectEnumType) {
			try {
				User user = this.insight.getUser();
				String newProjectId = newProject.getProjectId();

				Map<String, Object> sourceEntry = ModelInferenceLogsUtils.getWorkspaceEntry(projectTemplateId);
				String sourceDescription = sourceEntry != null ? (String) sourceEntry.get("description") : null;
				String sourceSystemPrompt = sourceEntry != null ? (String) sourceEntry.get("system_prompt") : null;
				JSONObject sourceConfigJson = ModelInferenceLogsUtils.getWorkspaceConfigJson(projectTemplateId);
				List<Map<String, Object>> sourceResources = ModelInferenceLogsUtils
						.getWorkspaceResourcesByType(projectTemplateId, null);

				List<Map<String, String>> clonedResources = new ArrayList<>();
				List<Map<String, Object>> dependencyList = new ArrayList<>();
				if (sourceResources != null) {
					for (Map<String, Object> r : sourceResources) {
						String resourceId = (String) r.get("resource_id");
						String resourceType = (String) r.get("resource_type");
						String resourceSubtype = (String) r.get("resource_subtype");
						Map<String, String> entry = new HashMap<>();
						entry.put("workspace_resource_id", UUID.randomUUID().toString());
						entry.put("workspace_id", newProjectId);
						entry.put("resource_id", resourceId);
						entry.put("resource_type", resourceType);
						entry.put("resource_subtype", resourceSubtype);
						clonedResources.add(entry);
						Map<String, Object> dep = new HashMap<>();
						dep.put("ENGINEID", resourceId);
						dep.put("ENGINETYPE", resourceType);
						dependencyList.add(dep);
					}
				}

				SecurityProjectUtils.updateProjectDependencies(user, newProjectId, dependencyList);
				ModelInferenceLogsUtils.createNewWorkspaceEntry(newProjectId,
						user.getPrimaryLoginToken().getId(),
						newProjectName, sourceDescription, sourceSystemPrompt, clonedResources);
				if (sourceConfigJson != null) {
					ModelInferenceLogsUtils.updateWorkspaceConfigJson(newProjectId, sourceConfigJson);
				}
			} catch (Exception e) {
				classLogger.error("Failed to clone workspace inference log entry from template '{}' to new project '{}'.",
						projectTemplateId, newProject.getProjectId(), e);
				// Roll back the project so the user doesn't end up with a broken agent
				// that exists in the list but throws "Workspace not found" when opened.
				try {
					newProject.delete();
				} catch (Exception rollbackEx) {
					classLogger.error("Failed to roll back project '{}' after workspace entry clone failure.",
							newProject.getProjectId(), rollbackEx);
				}
				throw new IllegalArgumentException(
						"Failed to create workspace configuration for cloned agent: " + e.getMessage(), e);
			}
		}

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(),
				newProject.getProjectId());
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this project. Note: the project ID is randomly generated and is not passed into this method";
		} else if (key.equals("projectTemplate")) {
			return "The id of the existing project that is serving as the template for the copy";
		} else if (key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The GIT provider - user must be logged in with this provider for credentials";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The GIT repository URL to clone for this project";
		}
		return super.getDescriptionForKey(key);
	}

}
