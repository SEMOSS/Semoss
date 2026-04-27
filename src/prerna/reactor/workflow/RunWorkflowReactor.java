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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.workflow.engine.WorkflowExecutionResult;
import prerna.reactor.workflow.engine.WorkflowExecutor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Executes a workflow project and returns the execution result.
 * 
 * Pixel: RunWorkflow(project=["workflow-project-uuid"], variables=[{"key": "value"}]);
 * 
 * Keys:
 *   project   — (required) the workflow project ID
 *   variables — (optional) runtime variable overrides as a map
 */
public class RunWorkflowReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RunWorkflowReactor.class);

	public RunWorkflowReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(),
				"variables",
				"trigger"
		};
		this.keyRequired = new int[] { 1, 0, 0 };
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

		// Pull latest if clustered
		if (project.requirePublish(true)) {
			classLogger.info("Project {} pulled from cloud before workflow execution", projectId);
		}

		// Get optional variable overrides
		Map<String, Object> variables = (Map<String, Object>) getMap("variables");

		// Determine trigger source (defaults to "manual")
		String triggeredBy = this.keyValue.get("trigger");
		if (triggeredBy == null || triggeredBy.isEmpty()) {
			triggeredBy = "manual";
		}

		// Execute the workflow
		WorkflowExecutor executor = new WorkflowExecutor();
		WorkflowExecutionResult result = executor.execute(project, variables, this.insight, triggeredBy);

		// Build return map
		Map<String, Object> returnMap = new LinkedHashMap<>();
		returnMap.put("executionId", result.getExecutionId());
		returnMap.put("workflowId", result.getWorkflowId());
		returnMap.put("status", result.getStatus().name());
		returnMap.put("durationMs", result.getDurationMs());
		returnMap.put("triggeredBy", result.getTriggeredBy());
		returnMap.put("finalOutput", result.getFinalOutput());
		if (result.getError() != null) {
			returnMap.put("error", result.getError());
		}

		return new NounMetadata(returnMap, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The ID of the workflow project to execute";
		} else if (key.equals("variables")) {
			return "Optional runtime variable overrides as a JSON map";
		} else if (key.equals("trigger")) {
			return "Trigger source: 'manual' (default), 'schedule', 'webhook', or 'api'";
		}
		return super.getDescriptionForKey(key);
	}
}
