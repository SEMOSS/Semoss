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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.gson.GsonUtility;

public class CreateProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateProjectReactor.class);
	private static final String CLASS_NAME = CreateProjectReactor.class.getName();

	/*
	 * This class is used to construct a new project This project only contains
	 * insights
	 */

	public CreateProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.PROJECT_TYPE.getKey(),
				ReactorKeysEnum.GLOBAL.getKey(), ReactorKeysEnum.PORTAL.getKey(), ReactorKeysEnum.PORTAL_NAME.getKey(),
				ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.URL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		this.organizeKeys();
		IProject.PROJECT_TYPE projectType = null;

		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		int index = 0;

		String projectName = this.keyValue.get(this.keysToGet[index++]);
		// if projectName is valid then set the name, else throw error
		if (!Utility.validateName(projectName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		// String projectName = this.keyValue.get(this.keysToGet[index++]);
		String projectTypeStr = this.keyValue.get(this.keysToGet[index++]);
		boolean global = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[index++]) + "");

		NounMetadata warning = null;
		if (global) {
			if (AbstractSecurityUtils.adminOnlyProjectSetPublic() && !SecurityAdminUtils.userIsAdmin(user)) {
				warning = NounMetadata.getWarningNounMessage(
						"Public access can only be enabled by administrators. This item will be created as private.");
				global = false;
			}
		}

		boolean hasPortal = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[index++]) + "");

		// Determine project type:
		// 1. If an explicit projectType is provided, use it (via enum valueOf)
		// 2. If no type but hasPortal, default to CODE
		// 3. Otherwise default to INSIGHTS
		if (projectTypeStr != null && !(projectTypeStr = projectTypeStr.trim()).isEmpty()) {
			projectType = IProject.PROJECT_TYPE.valueOf(projectTypeStr);
		} else if (hasPortal) {
			projectType = IProject.PROJECT_TYPE.CODE;
		} else {
			projectType = IProject.PROJECT_TYPE.INSIGHTS;
		}
		String portalName = this.keyValue.get(this.keysToGet[index++]);
		String gitProvider = this.keyValue.get(this.keysToGet[index++]);
		String gitCloneUrl = this.keyValue.get(this.keysToGet[index++]);

		IProject project = ProjectHelper.generateNewProject(projectName, projectType, global, hasPortal, portalName,
				gitProvider, gitCloneUrl, this.insight.getUser(), logger);

		// Scaffold workflow project with default workflow.json
		if (projectType == IProject.PROJECT_TYPE.WORKFLOW) {
			scaffoldWorkflowProject(project, this.insight.getUser(), logger);
		}

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(),
				project.getProjectId());
		NounMetadata retNoun = new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP,
				PixelOperationType.MARKET_PLACE_ADDITION);
		if (warning != null) {
			retNoun.addAdditionalReturn(warning);
		}
		return retNoun;
	}

	/**
	 * Creates the workflow directory structure and default workflow.json for a new WORKFLOW project.
	 * 
	 * @param project  the newly created project
	 * @param user     the user creating the project
	 * @param logger   logger instance
	 */
	private void scaffoldWorkflowProject(IProject project, User user, Logger logger) {
		String projectId = project.getProjectId();
		String assetFolder = AssetUtility.getProjectAssetsFolder(projectId);

		// Create the workflow directory under assets
		File workflowDir = new File(assetFolder + File.separator + IProject.WORKFLOW_FOLDER);
		if (!workflowDir.exists()) {
			workflowDir.mkdirs();
		}

		// Create the executions directory for storing run history
		File executionsDir = new File(workflowDir, "executions");
		if (!executionsDir.exists()) {
			executionsDir.mkdirs();
		}

		// Build the default empty workflow.json scaffold
		Map<String, Object> workflowJson = new HashMap<>();
		workflowJson.put("workflowId", projectId);
		workflowJson.put("name", project.getProjectName());
		workflowJson.put("version", 1);
		workflowJson.put("steps", new java.util.ArrayList<>());
		workflowJson.put("variables", new HashMap<>());
		workflowJson.put("trigger", null);

		Map<String, Object> settings = new HashMap<>();
		settings.put("maxSteps", 50);
		settings.put("timeoutMs", 300000);
		settings.put("onError", "stop");
		workflowJson.put("settings", settings);

		// Write the workflow.json file
		File workflowFile = new File(workflowDir, IProject.WORKFLOW_FILE_NAME);
		try {
			GsonUtility.writeObjectToJsonFile(workflowFile,
					new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create(), workflowJson);
		} catch (IOException e) {
			classLogger.error("Failed to write default workflow.json for project {}", projectId, e);
			throw new IllegalArgumentException(
					"Project was created but could not write the workflow.json. Error = " + e.getMessage());
		}

		// Add the scaffold to git and commit
		String projectVersionFolder = AssetUtility.getProjectVersionFolder(
				project.getProjectName(), project.getProjectId());
		List<String> files = new Vector<>();
		files.add(workflowFile.getAbsolutePath());
		GitRepoUtils.addSpecificFiles(projectVersionFolder, files);
		GitRepoUtils.commitAddedFiles(projectVersionFolder, "Initial workflow scaffold", user);

		if (ClusterUtil.IS_CLUSTER) {
			logger.info("Syncing workflow project for cloud backup");
			ClusterUtil.pushProjectFolder(project, projectVersionFolder);
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this project. Note: the project ID is randomly generated and is not passed into this method";
		} else if (key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The GIT provider - user must be logged in with this provider for credentials";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The GIT repository URL to clone for this project";
		}
		return super.getDescriptionForKey(key);
	}

}
