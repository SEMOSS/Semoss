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
import prerna.sablecc2.om.ReactorKeysEnum;
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
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), AutomationConstants.AUTOMATION_INPUTS_KEY, AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String userId = getUserId();
		String runId = UUID.randomUUID().toString();

		// Validate the automation has runnable steps BEFORE claiming the run slot,
		// so a bad automation never leaves a stale active-run record.
		Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) doc.get(AutomationConstants.DOC_GRAPH);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get(AutomationConstants.DOC_NODES);
		List<Map<String, Object>> ordered = nodes != null ? nodes : new ArrayList<>();
		if (ordered.isEmpty()) {
			throw new IllegalArgumentException("Automation has no nodes to execute");
		}
		long nonTriggerCount = ordered.stream()
				.filter(n -> !AutomationConstants.NODE_TRIGGER.equals(n.get(AutomationConstants.NODE_FIELD_TYPE)))
				.count();
		if (nonTriggerCount == 0) {
			throw new IllegalArgumentException("Automation has no steps to run. Add at least one step before running.");
		}

		if (!AutomationDatabaseUtility.claimActiveRun(projectId, runId)) {
			String activeRun = AutomationDatabaseUtility.getActiveRun(projectId);
			throw new IllegalArgumentException(
					"Automation already has an active run: " + activeRun +
					". Wait for it to complete or cancel it before starting a new run.");
		}

		boolean runStarted = false;
		try {
			Map<String, String> configMap = AutomationExecutionUtils.loadConfig(projectId);

			@SuppressWarnings("unchecked")
			Map<String, Object> inputsMap = this.getMap(AutomationConstants.AUTOMATION_INPUTS_KEY);
			AutomationExecutionUtils.applyPlaygroundInputs(ordered, inputsMap);

			String triggerType = this.keyValue.get(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY);
			if (triggerType == null || triggerType.isBlank()) {
				triggerType = AutomationConstants.TRIGGER_MANUAL;
			} else if (!AutomationConstants.TRIGGER_MANUAL.equals(triggerType)
					&& !AutomationConstants.TRIGGER_PLAYGROUND.equals(triggerType)) {
				classLogger.warn("Unknown triggerType '{}' for run {}, defaulting to MANUAL", triggerType, runId);
				triggerType = AutomationConstants.TRIGGER_MANUAL;
			}
			AutomationDatabaseUtility.insertRun(runId, projectId, AutomationConstants.DEFAULT_AUTOMATION_ID,
					triggerType, ordered.size(), userId);
			AutomationDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			classLogger.info("Automation run {} starting for project {}", runId, projectId);
			runStarted = true;
			Map<String, String> finalScope = AutomationRunEngine.run(runId, projectId, ordered, configMap, this.insight);

			// Build final result in the same shape as GetAutomationRunReactor
			Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
			List<Map<String, Object>> nodeOutputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
			List<Map<String, Object>> nodeResults = new ArrayList<>();
			int completedCount = 0;
			if (nodeOutputs != null) {
				for (Map<String, Object> output : nodeOutputs) {
					Map<String, Object> nodeResult = new HashMap<>();
					nodeResult.put(AutomationConstants.NODE_ID, output.get(AutomationConstants.NODE_ID));
					nodeResult.put(AutomationConstants.NODE_LABEL, output.get(AutomationConstants.NODE_LABEL));
					nodeResult.put(AutomationConstants.STATUS, output.get(AutomationConstants.STATUS));
					nodeResult.put(AutomationConstants.DURATION_MS, output.get(AutomationConstants.DURATION_MS));
					String outputForDisplay = (String) output.get(AutomationConstants.OUTPUT_VALUE);
					if (outputForDisplay == null || outputForDisplay.isBlank()) {
						outputForDisplay = (String) output.get(AutomationConstants.OUTPUT_PREVIEW);
					}
					nodeResult.put(AutomationConstants.OUTPUT_PREVIEW, outputForDisplay);
					nodeResult.put(AutomationConstants.OUTPUT_VALUE, output.get(AutomationConstants.OUTPUT_VALUE));
					nodeResult.put(AutomationConstants.ERROR_MESSAGE, output.get(AutomationConstants.ERROR_MESSAGE));
					nodeResults.add(nodeResult);
					if (AutomationConstants.NODE_STATUS_SUCCESS.equals(output.get(AutomationConstants.STATUS))) {
						completedCount++;
					}
				}
			}
			// Trigger nodes succeed immediately in the engine but never write a SUCCESS
			// DB record, so add them back so the count reflects what the user sees.
			int triggerCount = (int) ordered.stream()
					.filter(n -> AutomationConstants.NODE_TRIGGER.equals(n.get(AutomationConstants.NODE_FIELD_TYPE)))
					.count();
			completedCount += triggerCount;

			if (runDetail == null) {
				runDetail = new HashMap<>();
				runDetail.put(AutomationConstants.RUN_ID, runId);
				runDetail.put(AutomationConstants.PROJECT_ID, projectId);
			}
			runDetail.put(AutomationConstants.RESULT_NODE_RESULTS, nodeResults);

			boolean runSucceeded = AutomationConstants.STATUS_SUCCESS.equals(runDetail.get(AutomationConstants.STATUS));
			// Short summary shown in the sidebar UI.
			String summary = runSucceeded
					? AutomationExecutionUtils.buildSummaryMessage(doc, finalScope, configMap, completedCount, ordered.size())
					: buildFailureSummary(runDetail);
			runDetail.put(AutomationConstants.RESULT_SUMMARY, summary);
			AutomationDatabaseUtility.updateRunSummary(runId, summary);

			// Enriched context sent to the LLM as the MCP tool response, so it can describe
			// what each step actually did rather than just echoing the node count.
			if (runSucceeded) {
				StringBuilder llmContext = new StringBuilder(summary);
				for (Map<String, Object> step : nodeResults) {
					String label = (String) step.get(AutomationConstants.NODE_LABEL);
					String preview = (String) step.get(AutomationConstants.OUTPUT_PREVIEW);
					if (preview != null && !preview.isBlank()) {
						llmContext.append("\n- ").append(label).append(": ").append(preview);
					}
				}
				runDetail.put(AutomationConstants.RESULT_LLM_CONTEXT, llmContext.toString());
			}

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

	/** Null-safe failure summary - {@code FAILED_NODE_ID}/{@code ERROR_MESSAGE} may be absent. */
	private static String buildFailureSummary(Map<String, Object> runDetail) {
		Object failedNodeId = runDetail.get(AutomationConstants.FAILED_NODE_ID);
		Object errorMessage = runDetail.get(AutomationConstants.ERROR_MESSAGE);
		return "Automation failed at node " + (failedNodeId != null ? failedNodeId : "unknown")
				+ ": " + (errorMessage != null ? errorMessage : "no error details available");
	}

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
		return AutomationConstants.SYSTEM_USER_ID;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		// Same "system app" resourceURI scheme used by the Playwright browser-sockets tool
		// (see PlaywrightMCPToolBuilder) - resolved client-side to the automation workspace's
		// own build output (../../automation-workspace/dist/) so the exact same UI renders
		// whether embedded directly in the client app or iframed as an MCP sidebar tool.
		meta.put(MCPUtility.UI_RESOURCE_URI, "system://automation-workspace/?readOnly=1");
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Manually triggers an automation run for the given project and returns a per-step summary once complete.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) return "The project (app) ID or alias to run the automation for.";
		if (AutomationConstants.AUTOMATION_INPUTS_KEY.equals(key)) return "Optional map of playground-supplied values to inject into automation node fields before running. Keys are parameter names from the automation's MCP tool schema.";
		if (AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY.equals(key)) return "Caller-supplied trigger source. Defaults to MANUAL when omitted; MCP/playground callers pass PLAYGROUND.";
		return super.getDescriptionForKey(key);
	}
}
