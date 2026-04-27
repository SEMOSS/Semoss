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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

/**
 * Returns the current workflow definition and project metadata for a WORKFLOW project.
 * 
 * Pixel: GetWorkflowStatus(project=["workflow-project-uuid"]);
 * 
 * Returns a map with:
 *   projectId    — the resolved project ID
 *   projectName  — the project name
 *   projectType  — should be WORKFLOW
 *   workflow      — the parsed workflow.json contents (or null if not yet saved)
 */
public class GetWorkflowStatusReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetWorkflowStatusReactor.class);

	public GetWorkflowStatusReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@SuppressWarnings("unchecked")
	@Override
	public NounMetadata execute() {
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
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(
					"Project does not exist or user does not have access to the project"));
		}

		// Load project and verify type
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

		// Pull latest if clustered
		if (project.requirePublish(true)) {
			classLogger.info("{} pulled from cloud", projectId);
		}

		// Read workflow.json
		String assetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		File workflowFile = new File(assetFolder
				+ File.separator + IProject.WORKFLOW_FOLDER
				+ File.separator + IProject.WORKFLOW_FILE_NAME);

		Map<String, Object> workflowJson = null;
		if (workflowFile.exists() && workflowFile.isFile()) {
			try {
				workflowJson = (Map<String, Object>) GsonUtility.readJsonFileToObject(
						workflowFile,
						new TypeToken<Map<String, Object>>() {}.getType());
			} catch (IOException e) {
				classLogger.error("Failed to read workflow.json for project {}", projectId, e);
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(
						"Unable to read the workflow JSON file. Error = " + e.getMessage()));
			}
		}

		// Build return map
		Map<String, Object> returnMap = new LinkedHashMap<>();
		returnMap.put("projectId", projectId);
		returnMap.put("projectName", project.getProjectName());
		returnMap.put("projectType", projectType.name());
		returnMap.put("workflow", workflowJson);

		// Include execution history summaries (latest first, capped at 50)
		File executionsDir = new File(assetFolder
				+ File.separator + IProject.WORKFLOW_FOLDER
				+ File.separator + "executions");
		List<Map<String, Object>> executions = loadExecutionSummaries(executionsDir, 50);
		returnMap.put("executions", executions);

		return new NounMetadata(returnMap, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/**
	 * Load execution log files and extract summary info from each.
	 * Returns newest-first, capped at {@code limit} entries.
	 */
	private List<Map<String, Object>> loadExecutionSummaries(File executionsDir, int limit) {
		List<Map<String, Object>> summaries = new ArrayList<>();
		if (!executionsDir.exists() || !executionsDir.isDirectory()) {
			return summaries;
		}

		File[] logFiles = executionsDir.listFiles((dir, name) -> name.endsWith(".json"));
		if (logFiles == null || logFiles.length == 0) {
			return summaries;
		}

		// Sort newest first (by last modified)
		Arrays.sort(logFiles, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));

		int count = Math.min(logFiles.length, limit);
		for (int i = 0; i < count; i++) {
			try {
				Map<String, Object> fullLog = (Map<String, Object>) GsonUtility.readJsonFileToObject(
						logFiles[i],
						new TypeToken<Map<String, Object>>() {}.getType());

				// Extract summary fields only (not the full step results)
				Map<String, Object> summary = new LinkedHashMap<>();
				summary.put("executionId", fullLog.get("executionId"));
				summary.put("status", fullLog.get("status"));
				summary.put("durationMs", fullLog.get("durationMs"));
				summary.put("triggeredBy", fullLog.get("triggeredBy"));
				summary.put("startTimeMs", fullLog.get("startTimeMs"));
				summary.put("endTimeMs", fullLog.get("endTimeMs"));
				if (fullLog.containsKey("error")) {
					summary.put("error", fullLog.get("error"));
				}
				summaries.add(summary);
			} catch (IOException e) {
				classLogger.warn("Failed to read execution log file: {}", logFiles[i].getName(), e);
			}
		}

		return summaries;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The ID of the workflow project to get status for";
		}
		return super.getDescriptionForKey(key);
	}
}
