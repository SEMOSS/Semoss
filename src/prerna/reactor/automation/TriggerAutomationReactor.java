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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Manually triggers an automation run for a project. Validates access, claims the single-run slot,
 * runs the automation synchronously on the calling thread (expected to be a virtual thread from
 * the platform's {@code runPixelAsync} endpoint), and returns the completed run result.
 *
 * <p>Pixel: {@code TriggerAutomation(project=["appId"])}
 */
public class TriggerAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TriggerAutomationReactor.class);

	public TriggerAutomationReactor() {
		this.keysToGet = new String[] { "project" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String userId = getUserId();
		String runId = UUID.randomUUID().toString();

		if (!AutomationDatabaseUtility.claimActiveRun(projectId, runId)) {
			String activeRun = AutomationDatabaseUtility.getActiveRun(projectId);
			throw new IllegalArgumentException(
					"Automation already has an active run: " + activeRun +
					". Wait for it to complete or cancel it before starting a new run.");
		}

		boolean runStarted = false;
		try {
			Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);
			@SuppressWarnings("unchecked")
			Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
			Map<String, String> configMap = AutomationExecutionUtils.loadConfig(projectId);

			List<Map<String, Object>> ordered = nodes != null ? nodes : new ArrayList<>();
			if (ordered.isEmpty()) {
				throw new IllegalArgumentException("Automation has no nodes to execute");
			}

			AutomationDatabaseUtility.insertRun(runId, projectId, AutomationConstants.DEFAULT_AUTOMATION_ID,
					AutomationConstants.TRIGGER_MANUAL, ordered.size(), userId);
			AutomationDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			classLogger.info("Automation run {} starting for project {}", runId, projectId);
			runStarted = true;
			AutomationRunEngine.run(runId, projectId, ordered, configMap, this.insight);

			// Build final result in the same shape as GetAutomationRunReactor
			Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
			List<Map<String, Object>> nodeOutputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
			List<Map<String, Object>> nodeResults = new ArrayList<>();
			if (nodeOutputs != null) {
				for (Map<String, Object> output : nodeOutputs) {
					Map<String, Object> nodeResult = new HashMap<>();
					nodeResult.put(AutomationConstants.NODE_ID, output.get(AutomationConstants.NODE_ID));
					nodeResult.put(AutomationConstants.NODE_LABEL, output.get(AutomationConstants.NODE_LABEL));
					nodeResult.put(AutomationConstants.STATUS, output.get(AutomationConstants.STATUS));
					nodeResult.put(AutomationConstants.DURATION_MS, output.get(AutomationConstants.DURATION_MS));
					nodeResult.put(AutomationConstants.OUTPUT_PREVIEW, output.get(AutomationConstants.OUTPUT_PREVIEW));
					nodeResult.put(AutomationConstants.ERROR_MESSAGE, output.get(AutomationConstants.ERROR_MESSAGE));
					nodeResults.add(nodeResult);
				}
			}
			if (runDetail == null) {
				runDetail = new HashMap<>();
				runDetail.put(AutomationConstants.RUN_ID, runId);
				runDetail.put(AutomationConstants.PROJECT_ID, projectId);
			}
			runDetail.put("nodeResults", nodeResults);
			return new NounMetadata(runDetail, PixelDataType.MAP, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error("Automation run setup failed for project {}, run {}", projectId, runId, e);
			// Only release if AutomationRunEngine.run() was never called - once it starts,
			// its own finally block handles releaseActiveRun.
			if (!runStarted) {
				AutomationDatabaseUtility.releaseActiveRun(projectId, runId);
			}
			if (e instanceof RuntimeException re) throw re;
			throw new RuntimeException(e);
		}
	}

	// -- Helpers -------------------------------------------------------------------

	private String getProjectId() {
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}
		return projectId;
	}

	private String getUserId() {
		if (this.insight.getUser() != null && this.insight.getUser().getPrimaryLoginToken() != null) {
			return this.insight.getUser().getPrimaryLoginToken().getId();
		}
		return "system";
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Manually triggers an automation run for the given project and returns a run ID for polling.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if ("project".equals(key)) return "The project (app) ID or alias to run the automation for.";
		return super.getDescriptionForKey(key);
	}
}
