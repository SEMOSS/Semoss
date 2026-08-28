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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.comm.PixelJobManager;
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
	private static final String AUTOMATION_STREAM_TYPE = "automation";
	private static final String AUTOMATION_NODE_STATUS_KIND = "node-status";
	private static final String RESULT_BRANCH_PORT = "branchPort";

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
		AutomationProjectUtils.validateDefinitionReferences(definition, this.insight.getUser());
		List<Map<String, Object>> runNodes = AutomationRuntime.nodesForRun(definition);
		@SuppressWarnings("unchecked")
		Map<String, Object> inputs = this.getMap(AutomationConstants.AUTOMATION_INPUTS_KEY);
		validateInputs(inputs);
		Map<String, String> traceRoomIds = allocateTraceRoomIds(runNodes);

		if (!initializeRun(runId, projectId, definition, runNodes, traceRoomIds)) {
			// Startup cleanup may see a recently-heartbeating run and correctly leave it
			// alone. Retry that cleanup on a later trigger so an orphaned lock does not
			// remain forever after its heartbeat crosses the stale threshold.
			AutomationDatabaseUtility.markStaleRunsInterrupted();
			if (!initializeRun(runId, projectId, definition, runNodes, traceRoomIds)) {
				throw new IllegalArgumentException("Automation already has an active run: "
						+ AutomationDatabaseUtility.getClaimedActiveRun(projectId)
						+ ". Wait for it to complete or cancel it before starting a new run.");
			}
		}

		Map<String, Object> result;
		try {
			PyTranslator translator = this.insight.getPyTranslator();
			if (translator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			AutomationPythonRunRegistry.register(runId, translator, this.insight, ThreadStore.getJobId());

			Map<String, Object> scope = AutomationRuntimeUtils.buildInitialScope(runId, this.insight.getUser());
			if (inputs != null) {
				for (Map.Entry<String, Object> entry : inputs.entrySet()) {
					scope.put(entry.getKey(), entry.getValue());
				}
			}
			result = executeInControlOrder(projectId, runId, definition, runNodes,
					files.nodeSources(), scope, traceRoomIds);
			finishRun(runId, projectId);
		} catch (Exception e) {
			classLogger.error("Python automation run failed for project {}, run {}", projectId, runId, e);
			finishFailedRun(runId, projectId, e);
			result = Map.of("error", safeMessage(e));
		} finally {
			AutomationPythonRunRegistry.unregister(runId);
		}
		return new NounMetadata(buildResult(runId, projectId, result), PixelDataType.MAP,
				PixelOperationType.OPERATION);
	}

	private boolean initializeRun(String runId, String projectId,
			AutomationDefinitionValidator.ValidatedDefinition definition,
			List<Map<String, Object>> runNodes, Map<String, String> traceRoomIds) {
		return AutomationDatabaseUtility.claimAndInitializeRun(runId, projectId,
				AutomationConstants.DEFAULT_AUTOMATION_ID, AutomationConstants.PYTHON_DOC_CURRENT_VERSION,
				definition.hash(), definition.snapshot(), getTriggerType(), getUserId(), runNodes, traceRoomIds);
	}

	private Map<String, Object> executeInControlOrder(String projectId, String runId,
			AutomationDefinitionValidator.ValidatedDefinition definition, List<Map<String, Object>> runNodes,
			Map<String, String> nodeSources, Map<String, Object> scope, Map<String, String> traceRoomIds) {
		Map<String, Object> result = new LinkedHashMap<>();
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : runNodes) {
			nodesById.put((String) node.get(AutomationConstants.NODE_FIELD_ID), node);
		}
		Map<String, Map<String, String>> controlTargets = AutomationRuntime.controlTargets(definition);
		Map<String, String> branchDecisions = new LinkedHashMap<>();
		Set<String> visited = new HashSet<>();
		String currentNodeId = AutomationRuntime.startNodeId(definition);
		boolean pathCompleted = true;
		while (currentNodeId != null) {
			if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
				pathCompleted = false;
				break;
			}
			if (!visited.add(currentNodeId)) {
				throw new IllegalStateException("Automation control traversal revisited node '"
						+ currentNodeId + "'.");
			}
			Map<String, Object> node = nodesById.get(currentNodeId);
			if (node == null) {
				throw new IllegalStateException("Automation control edge selected unknown node '"
						+ currentNodeId + "'.");
			}
			String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			Map<String, Object> nodeResult;
			if (AutomationConstants.NODE_START.equals(type)) {
				nodeResult = executeStartNode(projectId, runId, node,
						AutomationRuntime.triggerSource(node, nodeSources.get(nodeId)), scope);
			} else if (AutomationConstants.NODE_CONTROL_IF.equals(type)) {
				nodeResult = executeConditionNode(runId, node, scope);
			} else {
				nodeResult = executeNodeSource(projectId, runId, node,
							AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
									node.get(AutomationConstants.NODE_FIELD_CODE_MODE))
										? AutomationSourceRenderer.renderNode(node)
										: nodeSources.get(nodeId),
						scope, traceRoomIds.get(nodeId));
			}
			if (!AutomationConstants.NODE_STATUS_SUCCESS.equals(nodeResult.get(AutomationConstants.STATUS))) {
				pathCompleted = false;
				break;
			}
			if (AutomationConstants.NODE_START.equals(type)) {
				result.put(AutomationConstants.RESULT_GLOBALS,
						nodeResult.getOrDefault(AutomationConstants.RESULT_GLOBALS, Map.of()));
			}
			String outputVar = (String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
			if (!AutomationConstants.NODE_START.equals(type) && outputVar != null) {
				scope.put(outputVar, nodeResult.get(AutomationConstants.RESULT_OUTPUT_VALUE));
			}
			String selectedPort = (String) nodeResult.get(RESULT_BRANCH_PORT);
			if (selectedPort == null) {
				selectedPort = AutomationConstants.CONTROL_PORT_OUT;
			} else {
				branchDecisions.put(nodeId, selectedPort);
			}
			currentNodeId = controlTargets.getOrDefault(nodeId, Map.of()).get(selectedPort);
		}
		if (pathCompleted) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Control branch was not selected");
		}
		result.put("scope", scope);
		result.put("branchDecisions", branchDecisions);
		return result;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> executeConditionNode(String runId, Map<String, Object> node,
			Map<String, Object> scope) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Timestamp started = Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
		long startedMs = System.currentTimeMillis();
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null);
		try {
			Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
			String expression = (String) config.get(AutomationConstants.CONFIG_CONDITION);
			boolean decision = AutomationConditionEvaluator.evaluate(expression, scope);
			String output = AutomationRuntimeUtils.toBoundedRuntimeJson(decision,
					AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation condition '" + nodeId + "' output");
			Map<String, Object> prospectiveScope = new LinkedHashMap<>(scope);
			prospectiveScope.put((String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), decision);
			AutomationRuntimeUtils.toBoundedRuntimeJson(prospectiveScope,
					AutomationConstants.RUN_SCOPE_MAX_BYTES, "Automation run scope");
			long duration = System.currentTimeMillis() - startedMs;
			String preview = AutomationRuntimeUtils.generatePreview(output);
			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
					(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview, null, null);
			AutomationPythonRunRegistry.nodeCompleted(runId);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null);
			Map<String, Object> result = nodeResult(nodeId,
					AutomationConstants.NODE_STATUS_SUCCESS, decision, null);
			result.put(RESULT_BRANCH_PORT, decision
					? AutomationConstants.CONTROL_PORT_THEN
					: AutomationConstants.CONTROL_PORT_ELSE);
			return result;
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, null, message);
			throw e instanceof RuntimeException runtimeException
					? runtimeException
					: new RuntimeException(e);
		}
	}

	private Map<String, Object> executeStartNode(String projectId, String runId, Map<String, Object> node,
			String source, Map<String, Object> scope) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Timestamp started = Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
		long startedMs = System.currentTimeMillis();
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null);
		try {
			Map<String, Object> declaredGlobals = AutomationRuntime.triggerGlobalDefaults(node);
			for (Map.Entry<String, Object> entry : declaredGlobals.entrySet()) {
				if (!scope.containsKey(entry.getKey())) {
					scope.put(entry.getKey(), entry.getValue());
				}
			}
			PyTranslator translator = this.insight.getPyTranslator();
			if (translator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			Object raw = translator.runScriptWithExplicitAssetPaths(this.insight,
					AutomationRuntime.buildTriggerInvocationScript(source, scope),
					getProjectAssetsFolder(projectId), new String[] { getProjectPyFolder(projectId) });
			Object value = AutomationRuntime.normalizeNodeResult(raw);
			Map<String, Object> sourceGlobals = normalizeScope(value);
			for (Map.Entry<String, Object> entry : sourceGlobals.entrySet()) {
				if (!scope.containsKey(entry.getKey())) {
					scope.put(entry.getKey(), entry.getValue());
				}
			}
			Map<String, Object> globals = new LinkedHashMap<>(sourceGlobals);
			for (String name : declaredGlobals.keySet()) {
				globals.put(name, scope.get(name));
			}
			String output = AutomationRuntimeUtils.toBoundedRuntimeJson(globals,
					AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation trigger output");
			AutomationRuntimeUtils.toBoundedRuntimeJson(scope, AutomationConstants.RUN_SCOPE_MAX_BYTES,
					"Automation run scope");
			String branchPort = branchPort(node, scope);
			long duration = System.currentTimeMillis() - startedMs;
			String preview = AutomationRuntimeUtils.generatePreview(output);
			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
					null, output, preview, null, null);
			AutomationPythonRunRegistry.nodeCompleted(runId);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null);
			Map<String, Object> result = nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, output, null);
			result.put(AutomationConstants.RESULT_GLOBALS, globals);
			if (branchPort != null) {
				result.put(RESULT_BRANCH_PORT, branchPort);
			}
			return result;
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, null, message);
			throw e instanceof RuntimeException runtimeException
					? runtimeException
					: new RuntimeException(e);
		}
	}

	private Map<String, Object> executeNodeSource(String projectId, String runId, Map<String, Object> node,
			String source, Map<String, Object> scope, String traceRoomId) {
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
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null);
		try {
			prepareGeneratedAgentRoom(node, traceRoomId);
			Map<String, Object> nodeScope = scope;
			if (traceRoomId != null) {
				nodeScope = new LinkedHashMap<>(scope);
				nodeScope.put(AutomationConstants.SCOPE_ROOM_ID, traceRoomId);
			}
			Object raw = translator.runScriptWithExplicitAssetPaths(this.insight,
					AutomationRuntime.buildNodeInvocationScript(source, nodeScope),
					getProjectAssetsFolder(projectId), new String[] { getProjectPyFolder(projectId) });
			Object value = AutomationRuntime.normalizeNodeResult(raw);
			return persistNativeNodeResult(runId, node, value, started, startedMs, traceRoomId, scope);
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, null, message);
			throw e instanceof RuntimeException runtimeException
					? runtimeException
					: new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private void prepareGeneratedAgentRoom(Map<String, Object> node, String roomId) {
		if (roomId == null
				|| !AutomationConstants.NODE_AGENT_RUN.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
				|| !AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
						node.get(AutomationConstants.NODE_FIELD_CODE_MODE))) {
			return;
		}
		Map<String, Object> config = node.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> map
				? (Map<String, Object>) map
				: Map.of();
		String workspaceId = stringValue(config.get(AutomationConstants.CONFIG_WORKSPACE_ID));
		RoomUtils.createRoomIfNotExists(roomId, this.insight, null, null,
				workspaceId == null ? null : workspaceId.trim(), null, null, null, null);
	}

	private static Map<String, String> allocateTraceRoomIds(List<Map<String, Object>> runNodes) {
		Map<String, String> roomIds = new LinkedHashMap<>();
		for (Map<String, Object> node : runNodes) {
			String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			if (AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
					node.get(AutomationConstants.NODE_FIELD_CODE_MODE))
					&& (AutomationConstants.NODE_MODEL_CHAT.equals(type)
					|| AutomationConstants.NODE_MODEL_VISION.equals(type)
					|| AutomationConstants.NODE_AGENT_RUN.equals(type))) {
				roomIds.put((String) node.get(AutomationConstants.NODE_FIELD_ID), UUID.randomUUID().toString());
			}
		}
		return roomIds;
	}

	private Map<String, Object> persistNativeNodeResult(String runId, Map<String, Object> node, Object value,
			Timestamp started, long startedMs, String traceRoomId, Map<String, Object> scope) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			long duration = System.currentTimeMillis() - startedMs;
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, "Run cancelled by user");
			streamNodeProgress(runId, node, AutomationConstants.STATUS_CANCELLED, duration, null,
					"Run cancelled by user");
			return nodeResult(nodeId, AutomationConstants.STATUS_CANCELLED, null, "Run cancelled by user");
		}
		GeneratedNodeResult generatedResult = splitGeneratedNodeResult(node, value);
		Object persistedValue = generatedResult.value();
		Object traceMetadata = generatedResult.metadata();
		String agentRunId = null;
		String agentFailure = null;
		boolean generatedAgentNode = AutomationConstants.NODE_AGENT_RUN.equals(
				node.get(AutomationConstants.NODE_FIELD_TYPE))
				&& AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
						node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
		if (generatedAgentNode) {
			Map<String, Object> agentResult = normalizeAgentResult(node, traceMetadata, traceRoomId);
			agentRunId = stringValue(agentResult.get("runId"));
			agentFailure = agentFailureMessage(agentResult, agentRunId);
			if (agentFailure == null && stringValue(persistedValue) == null) {
				agentFailure = "Agent run '" + agentRunId + "' completed without final output.";
			}
		}
		String output = AutomationRuntimeUtils.toBoundedRuntimeJson(persistedValue,
				AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation node '" + nodeId + "' output");
		String branchPort = null;
		if (agentFailure == null) {
			Map<String, Object> prospectiveScope = new LinkedHashMap<>(scope);
			prospectiveScope.put((String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), persistedValue);
			AutomationRuntimeUtils.toBoundedRuntimeJson(prospectiveScope,
					AutomationConstants.RUN_SCOPE_MAX_BYTES, "Automation run scope");
			branchPort = branchPort(node, prospectiveScope);
		}
		long duration = System.currentTimeMillis() - startedMs;
		String preview = AutomationRuntimeUtils.generatePreview(output);
		String modelMessageId = generatedAgentNode
				? null
				: extractModelMessageId(node, traceMetadata, traceRoomId);
		if (agentFailure != null) {
			AutomationDatabaseUtility.updateNodeFailedWithResult(runId, nodeId, started, duration,
					(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview,
					agentRunId, agentFailure);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, preview, agentFailure);
			return nodeResult(nodeId, AutomationConstants.NODE_STATUS_FAILED, persistedValue, agentFailure);
		}
		AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
				(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview,
				modelMessageId, agentRunId);
		AutomationPythonRunRegistry.nodeCompleted(runId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null);
		Map<String, Object> result = nodeResult(nodeId,
				AutomationConstants.NODE_STATUS_SUCCESS, persistedValue, null);
		if (branchPort != null) {
			result.put(RESULT_BRANCH_PORT, branchPort);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private static String branchPort(Map<String, Object> node, Map<String, Object> scope) {
		Map<String, Object> config = node.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> map
				? (Map<String, Object>) map : Map.of();
		Object expression = config.get(AutomationConstants.CONFIG_BRANCH_CONDITION);
		if (!(expression instanceof String condition)) {
			return null;
		}
		return AutomationConditionEvaluator.evaluate(condition, scope)
				? AutomationConstants.CONTROL_PORT_THEN
				: AutomationConstants.CONTROL_PORT_ELSE;
	}

	private static GeneratedNodeResult splitGeneratedNodeResult(Map<String, Object> node, Object value) {
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		boolean internalResultType = AutomationConstants.NODE_MODEL_CHAT.equals(type)
				|| AutomationConstants.NODE_MODEL_VISION.equals(type)
				|| AutomationConstants.NODE_AGENT_RUN.equals(type);
		if (!internalResultType || !AutomationConstants.NODE_CODE_MODE_GENERATED.equals(
				node.get(AutomationConstants.NODE_FIELD_CODE_MODE)) || !(value instanceof Map<?, ?> map)) {
			return new GeneratedNodeResult(value, value);
		}
		boolean hasValue = map.containsKey(AutomationConstants.INTERNAL_RESULT_VALUE);
		boolean hasMetadata = map.containsKey(AutomationConstants.INTERNAL_RESULT_METADATA);
		if (!hasValue && !hasMetadata) {
			return new GeneratedNodeResult(value, value);
		}
		if (!hasValue || !hasMetadata) {
			throw new IllegalStateException("Generated automation node returned an incomplete internal result.");
		}
		return new GeneratedNodeResult(map.get(AutomationConstants.INTERNAL_RESULT_VALUE),
				map.get(AutomationConstants.INTERNAL_RESULT_METADATA));
	}

	private record GeneratedNodeResult(Object value, Object metadata) {
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> normalizeAgentResult(Map<String, Object> node, Object value,
			String expectedRoomId) {
		Object response = value;
		if (response instanceof List<?> values && values.size() == 1) {
			response = values.get(0);
		}
		if (!(response instanceof Map<?, ?> responseMap)) {
			throw missingAgentTrace(node);
		}
		Map<String, Object> normalized = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : responseMap.entrySet()) {
			if (entry.getKey() instanceof String key) {
				normalized.put(key, entry.getValue());
			}
		}

		String returnedRunId = stringValue(normalized.get("runId"));
		String returnedRoomId = stringValue(normalized.get(AutomationConstants.TRACE_ROOM_ID));
		if (returnedRunId == null || expectedRoomId == null || !expectedRoomId.equals(returnedRoomId)) {
			throw missingAgentTrace(node);
		}

		Map<String, Object> config = node.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> map
				? (Map<String, Object>) map
				: Map.of();
		String expectedWorkspaceId = stringValue(config.get(AutomationConstants.CONFIG_WORKSPACE_ID));
		if (expectedWorkspaceId != null) {
			expectedWorkspaceId = expectedWorkspaceId.trim();
		}
		String returnedWorkspaceId = stringValue(normalized.get(AutomationConstants.CONFIG_WORKSPACE_ID));
		if (returnedWorkspaceId != null && !returnedWorkspaceId.equals(expectedWorkspaceId)) {
			throw new IllegalStateException("Automation agent node '"
					+ node.get(AutomationConstants.NODE_FIELD_ID)
					+ "' returned a different workspaceId than the configured agent.");
		}
		normalized.put(AutomationConstants.CONFIG_WORKSPACE_ID, expectedWorkspaceId);
		return normalized;
	}

	private static String agentFailureMessage(Map<String, Object> result, String runId) {
		String status = stringValue(result.get("status"));
		if (Boolean.TRUE.equals(result.get("waitTimedOut"))) {
			return "Agent run '" + runId + "' did not complete before the wait timeout.";
		}
		if (status == null) {
			return "Agent run '" + runId + "' returned no durable status.";
		}
		return switch (status.toUpperCase()) {
			case "COMPLETED" -> null;
			case "INPUT_REQUIRED" -> "Agent run '" + runId
					+ "' requires user input before the automation can continue.";
			case "FAILED" -> {
				String error = stringValue(result.get("errorMessage"));
				yield error != null ? error : "Agent run '" + runId + "' failed.";
			}
			case "CANCELLED" -> "Agent run '" + runId + "' was cancelled.";
			default -> "Agent run '" + runId + "' did not reach COMPLETED status (" + status + ").";
		};
	}

	private static IllegalStateException missingAgentTrace(Map<String, Object> node) {
		return new IllegalStateException("Automation agent node '"
				+ node.get(AutomationConstants.NODE_FIELD_ID)
				+ "' did not return matching runId and roomId trace metadata.");
	}

	private static String extractModelMessageId(Map<String, Object> node, Object value, String expectedRoomId) {
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		if (expectedRoomId == null || !(AutomationConstants.NODE_MODEL_CHAT.equals(type)
				|| AutomationConstants.NODE_MODEL_VISION.equals(type))) {
			return null;
		}
		Object response = value;
		if (value instanceof List<?> values && !values.isEmpty()) {
			response = values.get(0);
		}
		if (!(response instanceof Map<?, ?> responseMap)) {
			throw missingModelTrace(node);
		}
		String returnedRoomId = stringValue(responseMap.get(AutomationConstants.TRACE_ROOM_ID));
		String messageId = stringValue(responseMap.get("messageId"));
		if (!expectedRoomId.equals(returnedRoomId)) {
			throw new IllegalStateException("Automation model node '"
					+ node.get(AutomationConstants.NODE_FIELD_ID)
					+ "' returned a different roomId than the room assigned to this run.");
		}
		if (messageId == null) {
			throw missingModelTrace(node);
		}
		return messageId;
	}

	private static IllegalStateException missingModelTrace(Map<String, Object> node) {
		return new IllegalStateException("Automation model node '"
				+ node.get(AutomationConstants.NODE_FIELD_ID)
				+ "' did not return the required roomId and messageId trace metadata.");
	}

	private static String stringValue(Object value) {
		if (value == null || value.toString().isBlank()) {
			return null;
		}
		return value.toString();
	}

	private static void streamNodeProgress(String runId, Map<String, Object> node, String status,
			Long durationMs, String outputPreview, String errorMessage) {
		String jobId = ThreadStore.getJobId();
		if (jobId == null || jobId.isBlank()) {
			return;
		}
		PixelJobManager jobManager = PixelJobManager.getManager();
		if (jobManager.getJob(jobId) == null) {
			return;
		}
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", AUTOMATION_NODE_STATUS_KIND);
		data.put(AutomationConstants.RUN_ID, runId);
		data.put(AutomationConstants.NODE_ID, node.get(AutomationConstants.NODE_FIELD_ID));
		data.put(AutomationConstants.NODE_LABEL, node.get(AutomationConstants.NODE_FIELD_LABEL));
		data.put(AutomationConstants.STATUS, status);
		if (durationMs != null) {
			data.put(AutomationConstants.DURATION_MS, durationMs);
		}
		if (outputPreview != null) {
			data.put(AutomationConstants.OUTPUT_PREVIEW, outputPreview);
		}
		if (errorMessage != null) {
			data.put(AutomationConstants.ERROR_MESSAGE, errorMessage);
		}

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", AUTOMATION_STREAM_TYPE);
		envelope.put("data", data);
		jobManager.addStreamOut(jobId, envelope);
	}

	private static Map<String, Object> nodeResult(String nodeId, String status, Object output, String error) {
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

	private void finishRun(String runId, String projectId) {
		List<Map<String, Object>> outputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Run cancelled by user");
			AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_CANCELLED, null,
					"Run cancelled by user");
			return;
		}

		Map<String, Object> failed = outputs.stream()
				.filter(output -> AutomationConstants.NODE_STATUS_FAILED.equals(output.get(AutomationConstants.STATUS)))
				.findFirst().orElse(null);
		if (failed != null) {
			String nodeId = (String) failed.get(AutomationConstants.NODE_ID);
			AutomationDatabaseUtility.skipPendingNodes(runId, "Skipped because an earlier node failed");
			AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_FAILED, nodeId,
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
			AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_FAILED, nodeId, message);
			return;
		}

		int completed = (int) outputs.stream()
				.filter(output -> AutomationConstants.NODE_STATUS_SUCCESS.equals(output.get(AutomationConstants.STATUS))
						|| AutomationConstants.NODE_STATUS_SKIPPED.equals(output.get(AutomationConstants.STATUS)))
				.count();
		AutomationDatabaseUtility.updateHeartbeat(runId, completed);
		AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_SUCCESS, null, null);
	}

	private void finishFailedRun(String runId, String projectId, Exception error) {
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Run cancelled by user");
			AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_CANCELLED, null,
					"Run cancelled by user");
			return;
		}
		String failedNodeId = AutomationDatabaseUtility.getNodeOutputsForRun(runId).stream()
				.filter(output -> AutomationConstants.NODE_STATUS_FAILED.equals(output.get(AutomationConstants.STATUS)))
				.map(output -> (String) output.get(AutomationConstants.NODE_ID))
				.findFirst().orElse(null);
		AutomationDatabaseUtility.skipPendingNodes(runId, "Python runtime failed before this node executed");
		AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_FAILED, failedNodeId,
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
		detail.put(AutomationConstants.RESULT_GLOBALS, normalizeScope(
				pythonResult.get(AutomationConstants.RESULT_GLOBALS)));
		detail.put("pythonResult", pythonResult);
		String summary = AutomationConstants.STATUS_SUCCESS.equals(detail.get(AutomationConstants.STATUS))
				? "Automation completed successfully (" + nodeResults.size() + " nodes)."
				: buildFailureSummary(detail);
		detail.put(AutomationConstants.RESULT_SUMMARY, summary);
		AutomationDatabaseUtility.updateRunSummary(runId, summary);
		return detail;
	}

	private static Map<String, Object> normalizeScope(Object value) {
		Map<String, Object> scope = new LinkedHashMap<>();
		if (!(value instanceof Map<?, ?> map)) {
			return scope;
		}
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			if (entry.getKey() instanceof String key) {
				scope.put(key, entry.getValue());
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
		return AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(),
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
	}

	private String getTriggerType() {
		String triggerType = this.keyValue.get(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY);
		if (triggerType == null || triggerType.isBlank()) {
			return AutomationConstants.TRIGGER_MANUAL;
		}
		if (!AutomationConstants.TRIGGER_MANUAL.equals(triggerType)
				&& !AutomationConstants.TRIGGER_PLAYGROUND.equals(triggerType)
				&& !AutomationConstants.TRIGGER_SCHEDULED.equals(triggerType)) {
			throw new IllegalArgumentException("triggerType must be MANUAL, PLAYGROUND, or SCHEDULED.");
		}
		return triggerType;
	}

	private String getUserId() {
		if (this.insight.getUser() != null && this.insight.getUser().getPrimaryLoginToken() != null) {
			return this.insight.getUser().getPrimaryLoginToken().getId();
		}
		return AutomationConstants.SYSTEM_USER_ID;
	}

	private static void validateInputs(Map<String, Object> inputs) {
		if (inputs == null) {
			return;
		}
		for (String key : inputs.keySet()) {
			if (AutomationConstants.RESERVED_SCOPE_KEYS.contains(key)) {
				throw new IllegalArgumentException("Automation input '" + key
						+ "' is reserved for runtime metadata and cannot be overridden.");
			}
		}
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
			return "Optional values overriding trigger globals; date, triggered_at, and run_id are runtime-owned.";
		}
		if (AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY.equals(key)) {
			return "Optional trigger source: MANUAL (default), PLAYGROUND, or SCHEDULED.";
		}
		return super.getDescriptionForKey(key);
	}
}
