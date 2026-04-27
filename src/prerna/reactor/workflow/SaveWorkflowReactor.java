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
package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.workflow.engine.WorkflowDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.gson.GsonUtility;

/**
 * Saves or updates the workflow.json for a WORKFLOW project.
 * 
 * Pixel: SaveWorkflow(project=["workflow-project-uuid"], json=[{...}], comment=["optional commit message"]);
 * 
 * Keys:
 *   project — (required) the workflow project ID
 *   json    — (required) the workflow definition JSON (map)
 *   comment — (optional) git commit message
 */
public class SaveWorkflowReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveWorkflowReactor.class);

	private static final String CLASS_NAME = SaveWorkflowReactor.class.getName();

	public SaveWorkflowReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(),
				ReactorKeysEnum.JSON.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey()
		};
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("You are not properly logged in"));
		}

		// Resolve and validate project ID
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null || projectId.isEmpty()) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("Must input a project id"));
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Project does not exist or user does not have access to edit the project"));
		}

		// Get workflow JSON payload
		Map<String, Object> workflowJson = getWorkflowJSON();
		if (workflowJson == null || workflowJson.isEmpty()) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("Must provide the workflow JSON"));
		}

		// Optional commit message
		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment != null && !comment.isEmpty()) {
			comment = Utility.decodeURIComponent(comment);
		} else {
			comment = "Update workflow definition";
		}

		// Load project and verify it is a WORKFLOW type
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Project '" + projectId + "' could not be loaded"));
		}
		IProject.PROJECT_TYPE projectType = project.getProjectType();
		if (projectType != IProject.PROJECT_TYPE.WORKFLOW) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Project '" + projectId + "' is not a WORKFLOW project (type=" + projectType + ")"));
		}

		// Validate the workflow definition before persisting
		// This catches dangling references, orphaned steps, cycles, duplicate IDs, etc.
		String workflowJsonStr = GSON.toJson(workflowJson);
		WorkflowDefinition definition = WorkflowDefinition.parse(workflowJsonStr);
		List<String> validationErrors = definition.validate();
		if (!validationErrors.isEmpty()) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Workflow validation failed: " + String.join("; ", validationErrors)));
		}

		// Write workflow.json to assets/workflow/workflow.json
		String assetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		String workflowDir = assetFolder + File.separator + IProject.WORKFLOW_FOLDER;
		File workflowDirFile = new File(workflowDir);
		if (!workflowDirFile.exists()) {
			workflowDirFile.mkdirs();
		}

		File workflowFile = new File(workflowDir + File.separator + IProject.WORKFLOW_FILE_NAME);
		if (workflowFile.exists() && workflowFile.isFile()) {
			workflowFile.delete();
		}

		try {
			GsonUtility.writeObjectToJsonFile(workflowFile, GSON, workflowJson);
		} catch (IOException e) {
			classLogger.error("Failed to write workflow.json for project {}", projectId, e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Unable to save the workflow JSON to the project folder. Error = " + e.getMessage()));
		}

		// Git add and commit
		List<String> files = new Vector<>();
		files.add(workflowFile.getAbsolutePath());
		String projectVersionFolder = AssetUtility.getProjectVersionFolder(
				project.getProjectName(), projectId);
		GitRepoUtils.addSpecificFiles(projectVersionFolder, files);
		GitRepoUtils.commitAddedFiles(projectVersionFolder, comment, user);

		// Cluster sync
		if (ClusterUtil.IS_CLUSTER) {
			logger.info("Syncing workflow project for cloud backup");
			ClusterUtil.pushProjectFolder(project, projectVersionFolder);
		}

		SecurityProjectUtils.updateProjectLastEditedDate(projectId);

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getWorkflowJSON() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.JSON.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}

			List<NounMetadata> encodedStrGrs = mapGrs.getNounsOfType(PixelDataType.CONST_STRING);
			if (encodedStrGrs != null && !encodedStrGrs.isEmpty()) {
				String encodedStr = (String) encodedStrGrs.get(0).getValue();
				String mapStr = Utility.decodeURIComponent(encodedStr);
				return GSON.fromJson(mapStr, Map.class);
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}

		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The ID of the workflow project";
		} else if (key.equals(ReactorKeysEnum.JSON.getKey())) {
			return "The workflow definition JSON to save";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Optional git commit message";
		}
		return super.getDescriptionForKey(key);
	}
}
