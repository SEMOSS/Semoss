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
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.project.api.IProject;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

/**
 * Owns execution of one already initialized Automation run.
 *
 * <p>
 * The triggering reactor performs access checks and atomically creates submitted run history
 * before entering this boundary. This class claims that individual run, creates a run-local
 * {@link Insight}, traverses the validated control path, executes the immutable source snapshot one
 * node at a time, persists each transition, and always tears down the run-local Insight. Python
 * receives only the selected node source and a read-only scope; it does not own graph traversal or
 * persistence.
 *
 * <p>
 * Keeping this lifecycle independent from the Pixel reactor allows the same executor to be called
 * by a Quartz job without duplicating execution behavior.
 */
final class AutomationRunExecutionService {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunExecutionService.class);
	private static final String AUTOMATION_STREAM_TYPE = "automation";
	private static final String AUTOMATION_RUN_STARTED_KIND = "run-start";
	private static final String AUTOMATION_NODE_STATUS_KIND = "node-status";
	private final Insight requestInsight;
	private final String streamJobId;

	AutomationRunExecutionService(Insight requestInsight, String streamJobId) {
		if (requestInsight == null || requestInsight.getUser() == null) {
			throw new IllegalArgumentException("Automation execution requires an authenticated Insight.");
		}
		this.requestInsight = requestInsight;
		this.streamJobId = streamJobId;
	}

	/**
	 * Claims and executes a run whose graph, node rows, and source snapshot already exist in the
	 * scheduler database. A caller that loses the claim receives the current durable state without
	 * executing the run again.
	 *
	 * @param runId durable run identifier
	 * @param projectId Automation project identifier
	 * @param definition validated graph snapshot for this run
	 * @param runNodes nodes in deterministic history order
	 * @param traceRoomIds preallocated room identifiers keyed by traceable node ID
	 * @return persisted run detail and node results
	 */
	Map<String, Object> executeInitializedRun(String runId, String projectId,
			AutomationDefinitionValidator.ValidatedDefinition definition,
			List<Map<String, Object>> runNodes, Map<String, String> traceRoomIds) {
		if (!AutomationDatabaseUtility.claimRun(runId)) {
			return buildCurrentRunResult(runId, projectId);
		}
		streamRunStarted(runId, definition);

		Map<String, Object> result;
		Insight executionInsight = null;
		try {
			executionInsight = createExecutionInsight(projectId);
			PyTranslator translator = executionInsight.getPyTranslator();
			if (translator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			AutomationPythonRunRegistry.register(runId, translator, executionInsight, streamJobId);

			Map<String, Object> scope = AutomationRuntimeUtils.buildInitialScope(runId, executionInsight.getUser());
			scope.putAll(AutomationDatabaseUtility.getRunInputs(runId));
			Map<String, String> runNodeSources = AutomationDatabaseUtility.getRunNodeSources(runId);
			result = executeInControlOrder(executionInsight, projectId, runId, definition, runNodes,
					runNodeSources, scope, traceRoomIds);
			finishRun(runId, projectId);
		} catch (Exception e) {
			classLogger.error("Python automation run failed for project {}, run {}", projectId, runId, e);
			finishFailedRun(runId, projectId, e);
			result = Map.of("error", safeMessage(e));
		} finally {
			AutomationPythonRunRegistry.unregister(runId);
			cleanupExecutionInsight(executionInsight);
		}
		return buildResult(runId, projectId, result);
	}

	private static Map<String, Object> buildCurrentRunResult(String runId, String projectId) {
		Map<String, Object> persisted = AutomationDatabaseUtility.getRunDetail(runId);
		if (persisted == null) {
			throw new IllegalStateException("Automation run '" + runId
					+ "' no longer exists for project '" + projectId + "'.");
		}
		Map<String, Object> result = new LinkedHashMap<>(persisted);
		result.put(AutomationConstants.RESULT_NODE_RESULTS,
				AutomationDatabaseUtility.buildNodeResults(
						AutomationDatabaseUtility.getNodeOutputsForRun(runId)));
		return result;
	}

	private Map<String, Object> executeInControlOrder(Insight executionInsight, String projectId, String runId,
			AutomationDefinitionValidator.ValidatedDefinition definition, List<Map<String, Object>> runNodes,
			Map<String, String> nodeSources, Map<String, Object> scope, Map<String, String> traceRoomIds) {
		Map<String, Object> result = new LinkedHashMap<>();
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : runNodes) {
			nodesById.put((String) node.get(AutomationConstants.NODE_FIELD_ID), node);
		}
		Map<String, Map<String, String>> controlTargets = AutomationRuntime.controlTargets(definition);
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
				nodeResult = executeStartNode(executionInsight, projectId, runId, node,
						AutomationRuntime.triggerSource(node, nodeSources.get(nodeId)), scope);
			} else if (AutomationConstants.NODE_CONTROL_IF.equals(type)) {
				nodeResult = executeConditionNode(runId, node, scope);
			} else {
				nodeResult = executeNodeSource(executionInsight, projectId, runId, node,
							nodeSources.get(nodeId),
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
			String selectedPort = AutomationConstants.CONTROL_PORT_OUT;
			if (AutomationConstants.NODE_CONTROL_IF.equals(type)) {
				@SuppressWarnings("unchecked")
				Map<String, Object> decision = (Map<String, Object>) nodeResult.get(
						AutomationConstants.RESULT_OUTPUT_VALUE);
				selectedPort = (String) decision.get("branch");
			}
			currentNodeId = controlTargets.getOrDefault(nodeId, Map.of()).get(selectedPort);
		}
		if (pathCompleted) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Control branch was not selected");
		}
		result.put("scope", scope);
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
			List<Map<String, Object>> clauses = (List<Map<String, Object>>) config.get(
					AutomationConstants.CONFIG_CLAUSES);
			String selectedPort = AutomationConstants.CONTROL_PORT_ELSE;
			boolean matched = false;
			for (Map<String, Object> clause : clauses) {
				String expression = (String) clause.get(AutomationConstants.CONFIG_CONDITION);
				if (AutomationConditionEvaluator.evaluate(expression, scope)) {
					selectedPort = AutomationConstants.CONTROL_PORT_CASE_PREFIX
							+ clause.get(AutomationConstants.CONFIG_CLAUSE_ID);
					matched = true;
					break;
				}
			}
			Map<String, Object> decision = Map.of("branch", selectedPort, "value", matched);
			String output = AutomationRuntimeUtils.toBoundedRuntimeJson(decision,
					AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation condition '" + nodeId + "' output");
			long duration = System.currentTimeMillis() - startedMs;
			String preview = AutomationRuntimeUtils.generatePreview(output);
			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
					(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview, null, null);
			AutomationPythonRunRegistry.nodeCompleted(runId);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null);
			return nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, decision, null);
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

	private Map<String, Object> executeStartNode(Insight executionInsight, String projectId, String runId,
			Map<String, Object> node, String source, Map<String, Object> scope) {
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
			PyTranslator translator = executionInsight.getPyTranslator();
			if (translator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			Object raw = translator.runScriptWithExplicitAssetPaths(executionInsight,
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
			long duration = System.currentTimeMillis() - startedMs;
			String preview = AutomationRuntimeUtils.generatePreview(output);
			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
					null, output, preview, null, null);
			AutomationPythonRunRegistry.nodeCompleted(runId);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null);
			Map<String, Object> result = nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, output, null);
			result.put(AutomationConstants.RESULT_GLOBALS, globals);
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

	private Map<String, Object> executeNodeSource(Insight executionInsight, String projectId, String runId,
			Map<String, Object> node, String source, Map<String, Object> scope, String traceRoomId) {
		if (source == null || source.isBlank()) {
			throw new IllegalStateException("Automation node has no persisted Python source: "
					+ node.get(AutomationConstants.NODE_FIELD_ID));
		}
		PyTranslator translator = executionInsight.getPyTranslator();
		if (translator == null) {
			throw new IllegalStateException("Python runtime is not available for this insight.");
		}
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Timestamp started = Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
		long startedMs = System.currentTimeMillis();
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null,
				trace(traceRoomId, null, null));
		try {
			prepareGeneratedAgentRoom(executionInsight, node, traceRoomId);
			Map<String, Object> nodeScope = scope;
			if (traceRoomId != null) {
				nodeScope = new LinkedHashMap<>(scope);
				nodeScope.put(AutomationConstants.SCOPE_ROOM_ID, traceRoomId);
			}
			Object raw = translator.runScriptWithExplicitAssetPaths(executionInsight,
					AutomationRuntime.buildNodeInvocationScript(source, nodeScope),
					getProjectAssetsFolder(projectId), new String[] { getProjectPyFolder(projectId) });
			Object value = AutomationRuntime.normalizeNodeResult(raw);
			return persistNativeNodeResult(runId, node, value, started, startedMs, traceRoomId, scope);
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, null, message,
					trace(traceRoomId, null, null));
			throw e instanceof RuntimeException runtimeException
					? runtimeException
					: new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private void prepareGeneratedAgentRoom(Insight executionInsight, Map<String, Object> node, String roomId) {
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
		RoomUtils.createRoomIfNotExists(roomId, executionInsight, null, null,
				workspaceId == null ? null : workspaceId.trim(), null, null, null, null);
	}

	private Insight createExecutionInsight(String projectId) {
		Insight executionInsight = new Insight();
		executionInsight.setUser(requestInsight.getUser());
		executionInsight.setBaseURL(requestInsight.getBaseURL());
		executionInsight.setSchedulerMode(requestInsight.isSchedulerMode());
		executionInsight.setProjectId(projectId);
		IProject project = Utility.getProject(projectId);
		if (project != null) {
			executionInsight.setProjectName(project.getProjectName());
		}
		InsightStore.getInstance().put(executionInsight);
		return executionInsight;
	}

	private static void cleanupExecutionInsight(Insight executionInsight) {
		if (executionInsight == null) {
			return;
		}
		try {
			InsightUtility.dropInsight(executionInsight);
		} catch (Exception e) {
			classLogger.warn("Unable to fully clean up Automation execution insight '{}': {}",
					executionInsight.getInsightId(), e.getMessage(), e);
			InsightStore.getInstance().remove(executionInsight.getInsightId());
		}
	}

	/**
	 * Allocates one run-local room for each generated conversational node.
	 *
	 * @param runNodes validated nodes in run-history order
	 * @return room IDs keyed by node ID
	 */
	static Map<String, String> allocateTraceRoomIds(List<Map<String, Object>> runNodes) {
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
					"Run cancelled by user", trace(traceRoomId, null, null));
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
		if (agentFailure == null) {
			Map<String, Object> prospectiveScope = new LinkedHashMap<>(scope);
			prospectiveScope.put((String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), persistedValue);
			AutomationRuntimeUtils.toBoundedRuntimeJson(prospectiveScope,
					AutomationConstants.RUN_SCOPE_MAX_BYTES, "Automation run scope");
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
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, preview, agentFailure,
					trace(traceRoomId, null, agentRunId));
			return nodeResult(nodeId, AutomationConstants.NODE_STATUS_FAILED, persistedValue, agentFailure);
		}
		AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
				(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview,
				modelMessageId, agentRunId);
		AutomationPythonRunRegistry.nodeCompleted(runId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null,
				trace(traceRoomId, modelMessageId, agentRunId));
		return nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, persistedValue, null);
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

	private void streamNodeProgress(String runId, Map<String, Object> node, String status,
			Long durationMs, String outputPreview, String errorMessage) {
		streamNodeProgress(runId, node, status, durationMs, outputPreview, errorMessage, null);
	}

	private void streamRunStarted(String runId,
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		String jobId = streamJobId;
		if (jobId == null || jobId.isBlank()) {
			return;
		}
		PixelJobManager jobManager = PixelJobManager.getManager();
		if (jobManager.getJob(jobId) == null) {
			return;
		}
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", AUTOMATION_RUN_STARTED_KIND);
		data.put(AutomationConstants.RUN_ID, runId);
		data.put(AutomationConstants.DEFINITION_VERSION,
				AutomationConstants.PYTHON_DOC_CURRENT_VERSION);
		data.put(AutomationConstants.DEFINITION_HASH, definition.hash());
		data.put(AutomationConstants.DEFINITION_SNAPSHOT, definition.snapshot());

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", AUTOMATION_STREAM_TYPE);
		envelope.put("data", data);
		jobManager.addStreamOut(jobId, envelope);
	}

	private void streamNodeProgress(String runId, Map<String, Object> node, String status,
			Long durationMs, String outputPreview, String errorMessage, Map<String, Object> trace) {
		String jobId = streamJobId;
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
		if (trace != null && !trace.isEmpty()) {
			data.put(AutomationConstants.RESULT_TRACE, trace);
		}

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", AUTOMATION_STREAM_TYPE);
		envelope.put("data", data);
		jobManager.addStreamOut(jobId, envelope);
	}

	private static Map<String, Object> trace(String roomId, String modelMessageId, String agentRunId) {
		Map<String, Object> trace = new LinkedHashMap<>();
		if (roomId != null) {
			trace.put(AutomationConstants.TRACE_ROOM_ID, roomId);
		}
		if (modelMessageId != null) {
			trace.put(AutomationConstants.TRACE_MODEL_MESSAGE_ID, modelMessageId);
		}
		if (agentRunId != null) {
			trace.put(AutomationConstants.TRACE_AGENT_RUN_ID, agentRunId);
		}
		return trace;
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

	private static String safeMessage(Exception error) {
		return error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
	}

	private static String buildFailureSummary(Map<String, Object> runDetail) {
		Object failedNodeId = runDetail.get(AutomationConstants.FAILED_NODE_ID);
		Object errorMessage = runDetail.get(AutomationConstants.ERROR_MESSAGE);
		return "Automation failed at node " + (failedNodeId != null ? failedNodeId : "unknown")
				+ ": " + (errorMessage != null ? errorMessage : "no error details available");
	}

}
