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

import java.io.File;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.EngineUtility;

/**
 * Executes automation through the project's authenticated Python insight one node at a time.
 *
 * <p>Java owns persistence, authorization, cancellation, and traversal of the canonical
 * control-edge path. Python receives only one source file and one Java-bound node id at a time.
 *
 * <p>Pixel: {@code TriggerAutomation(project=["appId"])}
 */
public class TriggerAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TriggerAutomationReactor.class);

	public TriggerAutomationReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(),
				AutomationConstants.AUTOMATION_INPUTS_KEY,
				AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String runId = UUID.randomUUID().toString();
		AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.load(projectId);
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate(files.definition());
		List<Map<String, Object>> runNodes = AutomationRuntime.nodesForRun(definition);
		@SuppressWarnings("unchecked")
		Map<String, Object> inputs = this.getMap(AutomationConstants.AUTOMATION_INPUTS_KEY);

		if (!AutomationDatabaseUtility.claimActiveRun(projectId, runId)) {
			throw new IllegalArgumentException("Automation already has an active run: "
					+ AutomationDatabaseUtility.getActiveRun(projectId)
					+ ". Wait for it to complete or cancel it before starting a new run.");
		}

		boolean runPersisted = false;
		try {
			if (!AutomationDatabaseUtility.insertRun(runId, projectId, AutomationConstants.DEFAULT_AUTOMATION_ID,
					AutomationConstants.PYTHON_DOC_CURRENT_VERSION, definition.hash(), definition.snapshot(),
					getTriggerType(), runNodes.size(), getUserId())) {
				throw new IllegalStateException("Unable to create automation run history.");
			}
			if (!AutomationDatabaseUtility.insertAllNodeOutputs(runId, runNodes)) {
				throw new IllegalStateException("Unable to create automation node history.");
			}
			runPersisted = true;
			PyTranslator translator = this.insight.getPyTranslator();
			if (translator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			AutomationPythonRunRegistry.register(runId, translator, this.insight, ThreadStore.getJobId());

			Map<String, String> scope = AutomationRuntimeUtils.buildInitialScope(runId, this.insight.getUser());
			if (inputs != null) {
				for (Map.Entry<String, Object> entry : inputs.entrySet()) {
					scope.put(entry.getKey(), valueAsString(entry.getValue()));
				}
			}
			Map<String, Object> result = executeInControlOrder(projectId, runId, runNodes,
					files.nodeSources(), scope, AutomationRuntimeUtils.loadConfig(projectId));
			finishRun(runId);
			return new NounMetadata(buildResult(runId, projectId, result), PixelDataType.MAP,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Python automation run failed for project {}, run {}", projectId, runId, e);
			if (runPersisted) {
				finishFailedRun(runId, e);
				return new NounMetadata(buildResult(runId, projectId, Map.of("error", safeMessage(e))),
						PixelDataType.MAP, PixelOperationType.OPERATION);
			}
			throw e instanceof RuntimeException runtimeException
					? runtimeException
					: new RuntimeException(e);
		} finally {
			AutomationPythonRunRegistry.unregister(runId);
			AutomationDatabaseUtility.releaseActiveRun(projectId, runId);
		}
	}

	private Map<String, Object> executeInControlOrder(String projectId, String runId,
			List<Map<String, Object>> runNodes, Map<String, String> nodeSources, Map<String, String> scope,
			Map<String, String> config) {
		for (Map<String, Object> node : runNodes) {
			if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
				break;
			}
			String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			Map<String, Object> nodeResult = AutomationConstants.NODE_START.equals(type)
					? executeStartNode(runId, node, scope)
					: executeNodeSource(projectId, runId, node,
							AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
									node.get(AutomationConstants.NODE_FIELD_CODE_MODE))
											? AutomationSourceRenderer.renderNode(node)
											: nodeSources.get(nodeId),
							scope, config);
			if (!AutomationConstants.NODE_STATUS_SUCCESS.equals(nodeResult.get(AutomationConstants.STATUS))) {
				break;
			}
			Object output = nodeResult.get(AutomationConstants.RESULT_OUTPUT_VALUE);
			String outputVar = (String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
			if (output != null && outputVar != null) {
				scope.put(outputVar, output.toString());
			}
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("scope", scope);
		return result;
	}

	private Map<String, Object> executeStartNode(String runId, Map<String, Object> node, Map<String, String> scope) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Timestamp started = Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
		long startedMs = System.currentTimeMillis();
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		String output = scope.get(AutomationConstants.SCOPE_TRIGGERED_AT);
		AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, System.currentTimeMillis() - startedMs,
				null, output, AutomationRuntimeUtils.generatePreview(output));
		AutomationPythonRunRegistry.nodeCompleted(runId);
		return nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, output, null);
	}

	private Map<String, Object> executeNodeSource(String projectId, String runId, Map<String, Object> node,
			String source, Map<String, String> scope, Map<String, String> config) {
		if (source == null || source.isBlank()) {
			throw new IllegalStateException("Automation node has no persisted Python source: "
					+ node.get(AutomationConstants.NODE_FIELD_ID));
		}
		PyTranslator translator = this.insight.getPyTranslator();
		if (translator == null) {
			throw new IllegalStateException("Python runtime is not available for this insight.");
		}
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Timestamp started = Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
		long startedMs = System.currentTimeMillis();
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		try {
			Object raw = translator.runScriptWithExplicitAssetPaths(this.insight,
					AutomationRuntime.buildNodeInvocationScript(source, scope, config),
					getProjectAssetsFolder(projectId), new String[] { getProjectPyFolder(projectId) });
			Object value = AutomationRuntime.normalizeNodeResult(raw);
			return persistNativeNodeResult(runId, node, value, started, startedMs);
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			throw e instanceof RuntimeException runtimeException
					? runtimeException
					: new RuntimeException(e);
		}
	}

	private Map<String, Object> persistNativeNodeResult(String runId, Map<String, Object> node, Object value,
			Timestamp started, long startedMs) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, System.currentTimeMillis() - startedMs,
					"Run cancelled by user");
			return nodeResult(nodeId, AutomationConstants.STATUS_CANCELLED, null, "Run cancelled by user");
		}
		String output = AutomationRuntimeUtils.GSON.toJson(value);
		long duration = System.currentTimeMillis() - startedMs;
		AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
				(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output,
				AutomationRuntimeUtils.generatePreview(output));
		AutomationPythonRunRegistry.nodeCompleted(runId);
		return nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, output, null);
	}

	private static Map<String, Object> nodeResult(String nodeId, String status, String output, String error) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("nodeId", nodeId);
		result.put(AutomationConstants.STATUS, status);
		if (output != null) {
			result.put(AutomationConstants.RESULT_OUTPUT_VALUE, output);
		}
		if (error != null) {
			result.put(AutomationConstants.ERROR_MESSAGE, error);
		}
		return result;
	}

	private void finishRun(String runId) {
		List<Map<String, Object>> outputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Run cancelled by user");
			AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_CANCELLED, null,
					"Run cancelled by user");
			return;
		}

		Map<String, Object> failed = outputs.stream()
				.filter(output -> AutomationConstants.NODE_STATUS_FAILED.equals(output.get(AutomationConstants.STATUS)))
				.findFirst().orElse(null);
		if (failed != null) {
			String nodeId = (String) failed.get(AutomationConstants.NODE_ID);
			AutomationDatabaseUtility.skipPendingNodes(runId, "Skipped because an earlier node failed");
			AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_FAILED, nodeId,
					(String) failed.get(AutomationConstants.ERROR_MESSAGE));
			return;
		}

		Map<String, Object> incomplete = outputs.stream()
				.filter(output -> AutomationConstants.NODE_STATUS_PENDING.equals(output.get(AutomationConstants.STATUS))
						|| AutomationConstants.NODE_STATUS_RUNNING.equals(output.get(AutomationConstants.STATUS)))
				.findFirst().orElse(null);
		if (incomplete != null) {
			String nodeId = (String) incomplete.get(AutomationConstants.NODE_ID);
			String message = "Python source did not return a structured result for node " + nodeId + ".";
			AutomationDatabaseUtility.skipPendingNodes(runId, message);
			AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_FAILED, nodeId, message);
			return;
		}

		int completed = (int) outputs.stream()
				.filter(output -> AutomationConstants.NODE_STATUS_SUCCESS.equals(output.get(AutomationConstants.STATUS)))
				.count();
		AutomationDatabaseUtility.updateHeartbeat(runId, completed);
		AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_SUCCESS, null, null);
	}

	private void finishFailedRun(String runId, Exception error) {
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Run cancelled by user");
			AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_CANCELLED, null,
					"Run cancelled by user");
			return;
		}
		String failedNodeId = AutomationDatabaseUtility.getNodeOutputsForRun(runId).stream()
				.filter(output -> AutomationConstants.NODE_STATUS_FAILED.equals(output.get(AutomationConstants.STATUS)))
				.map(output -> (String) output.get(AutomationConstants.NODE_ID))
				.findFirst().orElse(null);
		AutomationDatabaseUtility.skipPendingNodes(runId, "Python runtime failed before this node executed");
		AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_FAILED, failedNodeId,
				safeMessage(error));
	}

	private Map<String, Object> buildResult(String runId, String projectId, Map<String, Object> pythonResult) {
		Map<String, Object> detail = AutomationDatabaseUtility.getRunDetail(runId);
		if (detail == null) {
			detail = new LinkedHashMap<>();
			detail.put(AutomationConstants.RUN_ID, runId);
			detail.put(AutomationConstants.PROJECT_ID, projectId);
		}
		List<Map<String, Object>> nodeResults =
				AutomationDatabaseUtility.buildNodeResults(AutomationDatabaseUtility.getNodeOutputsForRun(runId));
		detail.put(AutomationConstants.RESULT_NODE_RESULTS, nodeResults);
		detail.put("scope", normalizeScope(pythonResult.get("scope")));
		detail.put("pythonResult", pythonResult);
		String summary = AutomationConstants.STATUS_SUCCESS.equals(detail.get(AutomationConstants.STATUS))
				? "Automation completed successfully (" + nodeResults.size() + " nodes)."
				: buildFailureSummary(detail);
		detail.put(AutomationConstants.RESULT_SUMMARY, summary);
		AutomationDatabaseUtility.updateRunSummary(runId, summary);
		return detail;
	}

	private static Map<String, String> normalizeScope(Object value) {
		Map<String, String> scope = new LinkedHashMap<>();
		if (!(value instanceof Map<?, ?> map)) {
			return scope;
		}
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			if (entry.getKey() instanceof String key) {
				scope.put(key, valueAsString(entry.getValue()));
			}
		}
		return scope;
	}

	private String getProjectAssetsFolder(String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project was not found: " + projectId);
		}
		return EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				project.getProjectName());
	}

	private String getProjectPyFolder(String projectId) {
		return getProjectAssetsFolder(projectId) + File.separator + "py";
	}

	private String getProjectId() {
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id.");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have edit access.");
		}
		return projectId;
	}

	private String getTriggerType() {
		String triggerType = this.keyValue.get(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY);
		if (triggerType == null || triggerType.isBlank()) {
			return AutomationConstants.TRIGGER_MANUAL;
		}
		if (!AutomationConstants.TRIGGER_MANUAL.equals(triggerType)
				&& !AutomationConstants.TRIGGER_PLAYGROUND.equals(triggerType)) {
			throw new IllegalArgumentException("triggerType must be MANUAL or PLAYGROUND.");
		}
		return triggerType;
	}

	private String getUserId() {
		if (this.insight.getUser() != null && this.insight.getUser().getPrimaryLoginToken() != null) {
			return this.insight.getUser().getPrimaryLoginToken().getId();
		}
		return AutomationConstants.SYSTEM_USER_ID;
	}

	private static String valueAsString(Object value) {
		if (value == null) {
			return "";
		}
		return value instanceof String string ? string : AutomationRuntimeUtils.GSON.toJson(value);
	}

	private static String safeMessage(Exception error) {
		return error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
	}

	private static String buildFailureSummary(Map<String, Object> runDetail) {
		Object failedNodeId = runDetail.get(AutomationConstants.FAILED_NODE_ID);
		Object errorMessage = runDetail.get(AutomationConstants.ERROR_MESSAGE);
		return "Automation failed at node " + (failedNodeId != null ? failedNodeId : "unknown")
				+ ": " + (errorMessage != null ? errorMessage : "no error details available");
	}

	@Override
	public String getReactorDescription() {
		return "Runs automation through the authenticated Python workflow runtime.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The project ID or alias containing the automation workflow.";
		}
		if (AutomationConstants.AUTOMATION_INPUTS_KEY.equals(key)) {
			return "Optional values for fields declared in node playgroundFillable arrays.";
		}
		if (AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY.equals(key)) {
			return "Optional trigger source: MANUAL (default) or PLAYGROUND.";
		}
		return super.getDescriptionForKey(key);
	}
}
