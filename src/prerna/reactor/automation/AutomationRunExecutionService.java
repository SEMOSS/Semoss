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
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.project.api.IProject;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

/**
 * Owns execution of one already initialized Automation run.
 *
 * <p>
 * The triggering reactor performs access checks and atomically creates
 * submitted run history before entering this boundary. This class claims that
 * individual run, creates a run-local {@link Insight}, traverses the validated
 * control path, executes the immutable source snapshot one node at a time,
 * persists each transition, and always tears down the run-local Insight. Python
 * receives only the selected node source and a read-only scope; it does not own
 * graph traversal or persistence.
 *
 * <p>
 * Keeping this lifecycle independent from the Pixel reactor allows the same
 * executor to be called by a Quartz job without duplicating execution behavior.
 */
final class AutomationRunExecutionService {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunExecutionService.class);

	private static final String AUTOMATION_STREAM_TYPE = "automation";
	private static final String AUTOMATION_RUN_STARTED_KIND = "run-start";
	private static final String AUTOMATION_NODE_STATUS_KIND = "node-status";
	private static final long AGENT_RUN_POLL_INTERVAL_MS = 500L;
	private static final String AGENT_RUN_WAIT_TIMEOUT_PROPERTY = "AGENT_RUN_WAIT_TIMEOUT_MS";
	private static final long DEFAULT_AGENT_RUN_WAIT_TIMEOUT_MS = 3600000L;
	private static final long DEFAULT_AGENT_APPROVAL_TIMEOUT_HOURS = 24L;
	private static final Pattern EXACT_SCOPE_REFERENCE = Pattern.compile("^\\$\\{([^}]+)}$");
	private final Insight requestInsight;
	private final String streamJobId;

	/**
	 * Binds this service to the caller's authenticated Insight and, when the caller
	 * is a live Pixel job, to the job that streams progress back to it. The request
	 * Insight supplies the user, base URL, and scheduler mode for every run-local
	 * Insight created here; it never executes node Python itself.
	 */
	AutomationRunExecutionService(Insight requestInsight, String streamJobId) {
		if (requestInsight == null || requestInsight.getUser() == null) {
			throw new IllegalArgumentException("Automation execution requires an authenticated Insight.");
		}
		this.requestInsight = requestInsight;
		this.streamJobId = streamJobId;
	}

	/**
	 * Claims and executes a run whose graph, node rows, and source snapshot already
	 * exist in the scheduler database. A caller that loses the claim receives the
	 * current durable state without executing the run again.
	 *
	 * @param runId        durable run identifier
	 * @param projectId    Automation project identifier
	 * @param definition   validated graph snapshot for this run
	 * @param runNodes     nodes in deterministic history order
	 * @param traceRoomIds preallocated room identifiers keyed by traceable node ID
	 * @return persisted run detail and node results
	 */
	Map<String, Object> executeInitializedRun(String runId, String projectId,
			AutomationDefinitionValidator.ValidatedDefinition definition, List<Map<String, Object>> runNodes,
			Map<String, String> traceRoomIds) {
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
			result = executeInControlOrder(executionInsight, projectId, runId, definition, runNodes, runNodeSources,
					scope, traceRoomIds, AutomationRuntime.startNodeId(definition));
			if (!Boolean.TRUE.equals(result.get("waitingForInput"))) {
				finishRun(runId, projectId);
			}
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

	/**
	 * Returns the durable state of a run this caller did not claim, in the same
	 * shape a claimed execution returns. A caller that lost the claim, or that
	 * asked again after the run settled, sees node results and any active wait
	 * rather than an error.
	 */
	private static Map<String, Object> buildCurrentRunResult(String runId, String projectId) {
		Map<String, Object> persisted = AutomationDatabaseUtility.getRunDetail(runId);
		if (persisted == null) {
			throw new IllegalStateException(
					"Automation run '" + runId + "' no longer exists for project '" + projectId + "'.");
		}
		Map<String, Object> result = new LinkedHashMap<>(persisted);
		result.put(AutomationConstants.RESULT_NODE_RESULTS,
				AutomationDatabaseUtility.buildNodeResults(AutomationDatabaseUtility.getNodeOutputsForRun(runId)));
		Map<String, Object> wait = AutomationDatabaseUtility.getActiveWait(runId);
		if (wait != null) {
			result.put("wait", wait);
		}
		return result;
	}

	/**
	 * Walks the control path one node at a time starting at {@code initialNodeId},
	 * publishing each node's output into scope and following the port that node
	 * selected.
	 *
	 * <p>
	 * The loop ends when the path runs out of edges, a node does not reach SUCCESS,
	 * or cancellation is observed at a node boundary. Nodes left pending are marked
	 * skipped only when the path ran to its end, so an interrupted run keeps its
	 * unreached nodes pending for the caller to see.
	 */
	private Map<String, Object> executeInControlOrder(Insight executionInsight, String projectId, String runId,
			AutomationDefinitionValidator.ValidatedDefinition definition, List<Map<String, Object>> runNodes,
			Map<String, String> nodeSources, Map<String, Object> scope, Map<String, String> traceRoomIds,
			String initialNodeId) {
		Map<String, Object> result = new LinkedHashMap<>();
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : runNodes) {
			nodesById.put((String) node.get(AutomationConstants.NODE_FIELD_ID), node);
		}
		Map<String, Map<String, String>> controlTargets = AutomationRuntime.controlTargets(definition);
		Set<String> visited = new HashSet<>();
		String currentNodeId = initialNodeId;
		boolean pathCompleted = true;
		while (currentNodeId != null) {
			if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
				pathCompleted = false;
				break;
			}
			if (!visited.add(currentNodeId)) {
				throw new IllegalStateException("Automation control traversal revisited node '" + currentNodeId + "'.");
			}
			Map<String, Object> node = nodesById.get(currentNodeId);
			if (node == null) {
				throw new IllegalStateException(
						"Automation control edge selected unknown node '" + currentNodeId + "'.");
			}
			String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			Map<String, Object> nodeResult;
			if (AutomationConstants.NODE_START.equals(type)) {
				nodeResult = executeStartNode(executionInsight, projectId, runId, node,
						AutomationRuntime.triggerSource(node), scope);
			} else if (AutomationConstants.NODE_CONTROL_IF.equals(type)) {
				nodeResult = executeConditionNode(runId, node, scope);
			} else if (AutomationConstants.NODE_CONTROL_JEV.equals(type)) {
				nodeResult = executeJevDecisionNode(executionInsight, runId, node, scope);
			} else {
				nodeResult = executeNodeSource(executionInsight, projectId, runId, node, nodeSources.get(nodeId), scope,
						traceRoomIds.get(nodeId),
						controlTargets.getOrDefault(nodeId, Map.of()).get(AutomationConstants.CONTROL_PORT_OUT));
			}
			if (!AutomationConstants.NODE_STATUS_SUCCESS.equals(nodeResult.get(AutomationConstants.STATUS))) {
				if (AutomationConstants.NODE_STATUS_WAITING_FOR_INPUT
						.equals(nodeResult.get(AutomationConstants.STATUS))) {
					result.put("waitingForInput", true);
				}
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
			if (AutomationConstants.NODE_CONTROL_IF.equals(type)
					|| AutomationConstants.NODE_CONTROL_JEV.equals(type)) {
				@SuppressWarnings("unchecked")
				Map<String, Object> decision = (Map<String, Object>) nodeResult
						.get(AutomationConstants.RESULT_OUTPUT_VALUE);
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

	/**
	 * Evaluates one typed routing question through the configured TypeSafe/Jev
	 * engine. The model may select only a configured route; responses below the
	 * configured confidence threshold use the explicit fallback port.
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> executeJevDecisionNode(Insight executionInsight, String runId,
			Map<String, Object> node, Map<String, Object> scope) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		Timestamp started = Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
		long startedMs = System.currentTimeMillis();
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null);
		try {
			Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
			String engineId = (String) config.get(AutomationConstants.CONFIG_ENGINE_ID);
			if (!SecurityEngineUtils.userCanViewEngine(executionInsight.getUser(), engineId)) {
				throw new IllegalArgumentException(
						"Model " + engineId + " does not exist or user does not have access to this model");
			}
			IModelEngine model = Utility.getModel(engineId);
			if (!(model instanceof ITypeSafeEngine engine)) {
				throw new IllegalArgumentException("Jev decision nodes require a TYPESAFE model engine.");
			}

			List<Map<String, Object>> clauses = (List<Map<String, Object>>) config
					.get(AutomationConstants.CONFIG_CLAUSES);
			String questionType = jevQuestionType(config);
			Map<String, Object> criteria = new LinkedHashMap<>();
			for (Map<String, Object> clause : clauses) {
				Object criterion = AutomationConstants.JEV_QUESTION_TYPE_NOUL.equals(questionType)
						? String.valueOf(clause.get(AutomationConstants.CONFIG_ANSWER))
						: clause.get(AutomationConstants.CONFIG_CLAUSE_ID);
				criteria.put(String.valueOf(criterion),
						clause.get(AutomationConstants.CONFIG_DESCRIPTION));
			}
			Map<String, Object> routeQuestion = new LinkedHashMap<>();
			routeQuestion.put("type", questionType);
			routeQuestion.put("instructions", resolveJevText(config.get(AutomationConstants.CONFIG_QUESTION), scope));
			routeQuestion.put("criteria", criteria);
			Map<String, Object> questions = Map.of("route", routeQuestion);
			Map<String, Object> parameters = config.get(AutomationConstants.CONFIG_PARAM_VALUES) instanceof Map<?, ?> map
					? (Map<String, Object>) map
					: Map.of();
			Object state = resolveJevState(config.get(AutomationConstants.CONFIG_STATE), scope);
			TypeSafeModelEngineResponse response = engine.evaluate(state, questions, executionInsight, parameters);
			Map<String, Object> decision = jevDecision(response, clauses, config);

			String output = AutomationRuntimeUtils.toBoundedRuntimeJson(decision,
					AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation Jev decision '" + nodeId + "' output");
			long duration = System.currentTimeMillis() - startedMs;
			String preview = AutomationRuntimeUtils.generatePreview(output);
			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration, null, output, preview, null,
					null);
			AutomationPythonRunRegistry.nodeCompleted(runId);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null);
			return nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, decision, null);
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, null, message);
			throw e instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(e);
		}
	}

	/**
	 * Resolves an exact scope reference while preserving its native value. Literal
	 * state remains unchanged so maps and lists are not coerced to text.
	 */
	private static Object resolveJevState(Object configuredState, Map<String, Object> scope) {
		if (configuredState instanceof String value) {
			Matcher reference = EXACT_SCOPE_REFERENCE.matcher(value.trim());
			if (reference.matches()) {
				String name = reference.group(1);
				if (!scope.containsKey(name)) {
					throw new IllegalArgumentException("Jev decision state references unavailable scope value: " + name);
				}
				return scope.get(name);
			}
		}
		return configuredState;
	}

	/** Resolves the optional Jev instruction text from the current run scope. */
	private static String resolveJevText(Object configuredText, Map<String, Object> scope) {
		Object resolved = resolveJevState(configuredText, scope);
		return resolved == null ? "" : String.valueOf(resolved);
	}

	/**
	 * Validates a typed response and maps it to a configured graph port.
	 * Low-confidence responses use the explicit fallback instead of guessing.
	 */
	static Map<String, Object> jevDecision(TypeSafeModelEngineResponse response, List<Map<String, Object>> clauses,
			Map<String, Object> config) {
		Map<String, Object> providerResponse = response.getResponse();
		Object rawAnswers = providerResponse.get("answers");
		if (!(rawAnswers instanceof Map<?, ?> answers) || !(answers.get("route") instanceof Map<?, ?> routeAnswer)) {
			throw new IllegalStateException("Jev response did not include answers.route.");
		}
		return AutomationConstants.JEV_QUESTION_TYPE_NOUL.equals(jevQuestionType(config))
				? jevNoulDecision(providerResponse, routeAnswer, clauses, config)
				: jevChoiceDecision(providerResponse, routeAnswer, clauses, config);
	}

	/** Maps an arbitrary choice answer to its stable route identifier. */
	private static Map<String, Object> jevChoiceDecision(Map<String, Object> providerResponse,
			Map<?, ?> routeAnswer, List<Map<String, Object>> clauses, Map<String, Object> config) {
		Set<String> routeIds = new HashSet<>();
		for (Map<String, Object> clause : clauses) {
			routeIds.add((String) clause.get(AutomationConstants.CONFIG_CLAUSE_ID));
		}
		Object choiceValue = routeAnswer.get("choice");
		if (!(choiceValue instanceof String choice) || !routeIds.contains(choice)) {
			throw new IllegalStateException("Jev response selected an unknown route.");
		}
		Object confidenceValue = routeAnswer.get("confidence");
		if (!(confidenceValue instanceof Number confidenceNumber)
				|| !Double.isFinite(confidenceNumber.doubleValue()) || confidenceNumber.doubleValue() < 0
				|| confidenceNumber.doubleValue() > 1) {
			throw new IllegalStateException("Jev response route confidence must be a number from 0 through 1.");
		}
		double confidence = confidenceNumber.doubleValue();
		double threshold = config.get(AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD) instanceof Number number
				? number.doubleValue()
				: 0.0;
		String branch = confidence >= threshold ? AutomationConstants.CONTROL_PORT_CASE_PREFIX + choice
				: AutomationConstants.CONTROL_PORT_ELSE;
		Map<String, Object> decision = new LinkedHashMap<>();
		decision.put("branch", branch);
		decision.put("choice", choice);
		decision.put("confidence", confidence);
		decision.put("confidenceThreshold", threshold);
		decision.put("probabilities",
				routeAnswer.containsKey("probabilities") ? routeAnswer.get("probabilities") : Map.of());
		decision.put("model", providerResponse.get("model"));
		return decision;
	}

	/** Maps a Noul probability to the explicitly configured Yes or No route. */
	private static Map<String, Object> jevNoulDecision(Map<String, Object> providerResponse,
			Map<?, ?> routeAnswer, List<Map<String, Object>> clauses, Map<String, Object> config) {
		Object probabilityValue = routeAnswer.get("noul");
		if (!(probabilityValue instanceof Number probabilityNumber)
				|| !Double.isFinite(probabilityNumber.doubleValue()) || probabilityNumber.doubleValue() < 0
				|| probabilityNumber.doubleValue() > 1) {
			throw new IllegalStateException("Jev response route.noul must be a number from 0 through 1.");
		}
		double probabilityYes = probabilityNumber.doubleValue();
		boolean answer = probabilityYes >= 0.5;
		double confidence = answer ? probabilityYes : 1 - probabilityYes;
		double threshold = config.get(AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD) instanceof Number number
				? number.doubleValue()
				: 0.5;
		String routeId = null;
		for (Map<String, Object> clause : clauses) {
			if (Boolean.valueOf(answer).equals(clause.get(AutomationConstants.CONFIG_ANSWER))) {
				routeId = (String) clause.get(AutomationConstants.CONFIG_CLAUSE_ID);
				break;
			}
		}
		if (routeId == null) {
			throw new IllegalStateException("Jev Noul decision has no route for answer=" + answer + ".");
		}
		Map<String, Object> decision = new LinkedHashMap<>();
		decision.put("branch", confidence >= threshold ? AutomationConstants.CONTROL_PORT_CASE_PREFIX + routeId
				: AutomationConstants.CONTROL_PORT_ELSE);
		decision.put("answer", answer);
		decision.put("probabilityYes", probabilityYes);
		decision.put("confidence", confidence);
		decision.put("confidenceThreshold", threshold);
		decision.put("probabilities", Map.of("true", probabilityYes, "false", 1 - probabilityYes));
		decision.put("model", providerResponse.get("model"));
		return decision;
	}

	/** Returns the canonical Jev question type, defaulting older graphs to choice. */
	private static String jevQuestionType(Map<String, Object> config) {
		Object value = config.get(AutomationConstants.CONFIG_QUESTION_TYPE);
		return value instanceof String questionType ? questionType : AutomationConstants.JEV_QUESTION_TYPE_CHOICE;
	}

	/**
	 * Evaluates an if node's ordered clauses in Java and persists the port it
	 * selected. The first clause whose expression is true wins; with no match the
	 * else port is selected. No Python runs for this node, and its decision is
	 * consumed by the control loop rather than entering scope.
	 */
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
			List<Map<String, Object>> clauses = (List<Map<String, Object>>) config
					.get(AutomationConstants.CONFIG_CLAUSES);
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
			throw e instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(e);
		}
	}

	/**
	 * Seeds the trigger's declared global defaults into scope without overwriting
	 * supplied inputs, runs the optional trigger setup Python, and merges any map
	 * it returns. The resolved globals are persisted as the trigger node's output
	 * and returned for the run result.
	 */
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
					AutomationRuntime.buildTriggerInvocationScript(source, scope), getProjectAssetsFolder(projectId),
					new String[] { getProjectPyFolder(projectId) });
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
			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration, null, output, preview, null,
					null);
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
			throw e instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(e);
		}
	}

	/**
	 * Executes one node's persisted Python against a scope snapshot and persists
	 * the outcome.
	 *
	 * <p>
	 * A node allocated a run-local room receives it as private scope metadata. When
	 * the node starts a durable child agent, this waits for that child to settle or
	 * to ask for human input before the result is persisted.
	 */
	private Map<String, Object> executeNodeSource(Insight executionInsight, String projectId, String runId,
			Map<String, Object> node, String source, Map<String, Object> scope, String traceRoomId,
			String resumeNodeId) {
		if (source == null || source.isBlank()) {
			throw new IllegalStateException(
					"Automation node has no persisted Python source: " + node.get(AutomationConstants.NODE_FIELD_ID));
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
				traceForNode(node, traceRoomId, null, null));
		try {
			prepareGeneratedAgentRoom(executionInsight, node, traceRoomId);
			Map<String, Object> nodeScope = scope;
			if (traceRoomId != null) {
				nodeScope = new LinkedHashMap<>(scope);
				nodeScope.put(AutomationConstants.SCOPE_ROOM_ID, traceRoomId);
			}
			Object raw = translator.runScriptWithExplicitAssetPaths(executionInsight,
					AutomationRuntime.buildNodeInvocationScript(source, nodeScope, executionInsight.getInsightFolder()),
					getProjectAssetsFolder(projectId),
					new String[] { getProjectPyFolder(projectId) });
			Object value = AutomationRuntime.normalizeNodeResult(raw);
			value = awaitGeneratedAgentRun(executionInsight, runId, node, value, traceRoomId, scope);
			return persistNativeNodeResult(runId, projectId, node, value, started, startedMs, traceRoomId, resumeNodeId,
					scope);
		} catch (Exception e) {
			long duration = System.currentTimeMillis() - startedMs;
			String message = safeMessage(e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, null, message,
					traceForNode(node, traceRoomId, null, null));
			throw e instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(e);
		}
	}

	/**
	 * The validator treats every codeMode except the literal "custom" as generated,
	 * including an absent value. Matching that convention here (rather than
	 * requiring the literal "generated") keeps live agent-run tracking from
	 * silently no-oping on a node that omits the field.
	 */
	private static boolean isGeneratedAgentRunNode(Map<String, Object> node) {
		return AutomationConstants.NODE_AGENT_RUN.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
				&& !AutomationConstants.NODE_CODE_MODE_CUSTOM
						.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
	}

	/**
	 * Creates the run-local room a generated agent node posts into, before its
	 * Python submits the child run, so the room exists for the trace the node is
	 * about to return.
	 */
	@SuppressWarnings("unchecked")
	private void prepareGeneratedAgentRoom(Insight executionInsight, Map<String, Object> node, String roomId) {
		if (roomId == null || !isGeneratedAgentRunNode(node)) {
			return;
		}
		Map<String, Object> config = node.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> map
				? (Map<String, Object>) map
				: Map.of();
		String workspaceId = stringValue(config.get(AutomationConstants.CONFIG_WORKSPACE_ID));
		RoomUtils.createRoomIfNotExists(roomId, executionInsight, null, null,
				workspaceId == null ? null : workspaceId.trim(), null, null, null, null);
	}

	/**
	 * Monitors an asynchronous durable agent run while it is actively executing. A
	 * terminal result is returned for normal node persistence;
	 * {@code INPUT_REQUIRED} returns immediately so the caller can persist a
	 * durable Automation wait and release its worker and run-local Insight.
	 */
	Object awaitGeneratedAgentRun(Insight executionInsight, String automationRunId, Map<String, Object> node,
			Object value, String traceRoomId, Map<String, Object> scope) throws InterruptedException {
		return awaitGeneratedAgentRun(executionInsight, automationRunId, node, value, traceRoomId, scope,
				System::nanoTime, Thread::sleep);
	}

	/**
	 * Polls a durable child agent run until it settles or asks for human input,
	 * streaming each status change and cascading cancellation down to the child.
	 *
	 * <p>
	 * The clock and sleep are supplied by the caller so the poll loop can be driven
	 * without real time. Time the child spends waiting on a human does not count
	 * against the wait timeout.
	 */
	Object awaitGeneratedAgentRun(Insight executionInsight, String automationRunId, Map<String, Object> node,
			Object value, String traceRoomId, Map<String, Object> scope, LongSupplier monotonicTime,
			AgentRunSleeper sleeper) throws InterruptedException {
		if (!isGeneratedAgentRunNode(node)) {
			return value;
		}

		GeneratedNodeResult generated = splitGeneratedNodeResult(node, value);
		Map<String, Object> submittedRun = normalizeAgentResult(node, generated.metadata(), traceRoomId);
		String agentRunId = stringValue(submittedRun.get("runId"));
		if (agentRunId == null) {
			throw missingAgentTrace(node);
		}
		long waitStartedAt = monotonicTime.getAsLong();
		AutomationDatabaseUtility.updateNodeAgentRunTrace(automationRunId,
				(String) node.get(AutomationConstants.NODE_FIELD_ID), agentRunId);

		String previousStatus = stringValue(submittedRun.get("status"));
		ActiveAgentWaitTimeout waitTimeout = new ActiveAgentWaitTimeout(configuredAgentWaitTimeoutMs(node, scope),
				previousStatus, waitStartedAt);
		boolean cancellationSignalled = false;
		try {
			streamNodeProgress(automationRunId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null,
					traceForNode(node, traceRoomId, null, agentRunId, previousStatus));
			while (true) {
				if (AutomationPythonRunRegistry.isCancellationRequested(automationRunId) && !cancellationSignalled) {
					cancellationSignalled = true;
					AgentRunService.get().cancelRun(agentRunId, "Automation run cancelled");
				}

				Map<String, Object> durableRun = AgentRunService.get().getRun(agentRunId, executionInsight);
				Map<String, Object> currentRun = normalizeAgentResult(node, durableRun, traceRoomId);
				String status = stringValue(currentRun.get("status"));
				if (status == null) {
					throw new IllegalStateException("Agent run '" + agentRunId + "' returned no durable status.");
				}
				boolean statusChanged = previousStatus == null || !status.equalsIgnoreCase(previousStatus);
				previousStatus = status;
				if (statusChanged) {
					streamNodeProgress(automationRunId, node, AutomationConstants.NODE_STATUS_RUNNING, null, null, null,
							traceForNode(node, traceRoomId, null, agentRunId, status));
				}

				if (isInputRequiredStatus(status) || isAgentRunTerminalStatus(status)) {
					return generatedAgentRunResult(currentRun);
				}

				waitTimeout.observe(status, monotonicTime.getAsLong());
				if (waitTimeout.isExpired()) {
					if (!cancellationSignalled) {
						cancellationSignalled = true;
						AgentRunService.get().cancelRun(agentRunId, "Automation agent wait timeout");
					}
					currentRun.put("status", "FAILED");
					currentRun.put("waitTimedOut", true);
					return generatedAgentRunResult(currentRun);
				}

				sleeper.sleep(waitTimeout.nextPollDelayMs());
			}
		} catch (InterruptedException e) {
			if (!cancellationSignalled && !isAgentRunTerminalStatus(previousStatus)) {
				cancellationSignalled = true;
				cancelAgentRunAfterMonitoringFailure(agentRunId, traceRoomId, "Automation agent monitoring interrupted",
						e);
			}
			Thread.currentThread().interrupt();
			throw e;
		} catch (RuntimeException e) {
			if (!cancellationSignalled && !isAgentRunTerminalStatus(previousStatus)) {
				cancellationSignalled = true;
				cancelAgentRunAfterMonitoringFailure(agentRunId, traceRoomId, "Automation agent monitoring failed", e);
			}
			throw e;
		}
	}

	/**
	 * Returns whether a durable agent run has settled and will not change again.
	 */
	static boolean isAgentRunTerminalStatus(String status) {
		return "COMPLETED".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status)
				|| "CANCELLED".equalsIgnoreCase(status);
	}

	/**
	 * Sleep strategy for the agent-monitor poll loop, injected so the loop's timing
	 * can be controlled by the caller.
	 */
	@FunctionalInterface
	interface AgentRunSleeper {
		/**
		 * Pauses the poll loop for the requested duration, or throws when the waiting
		 * thread is interrupted so cancellation still propagates.
		 */
		void sleep(long durationMs) throws InterruptedException;
	}

	/**
	 * Repackages a settled child agent run into the internal envelope generated
	 * agent nodes return, with finalText as the business value and the run's
	 * identifiers kept as trace metadata.
	 */
	private static Map<String, Object> generatedAgentRunResult(Map<String, Object> currentRun) {
		Map<String, Object> metadata = new LinkedHashMap<>();
		for (String key : List.of("runId", AutomationConstants.TRACE_ROOM_ID, AutomationConstants.TRACE_WORKSPACE_ID,
				"status", "waitTimedOut", "errorMessage")) {
			metadata.put(key, currentRun.get(key));
		}
		Map<String, Object> completed = new LinkedHashMap<>();
		completed.put(AutomationConstants.INTERNAL_RESULT_VALUE, currentRun.get("finalText"));
		completed.put(AutomationConstants.INTERNAL_RESULT_METADATA, metadata);
		return completed;
	}

	/**
	 * Cancels the child agent after monitoring fails, so a child is never left
	 * running without a parent watching it. A cancellation failure is attached to
	 * the original error rather than replacing it.
	 */
	private static void cancelAgentRunAfterMonitoringFailure(String agentRunId, String traceRoomId, String reason,
			Throwable monitoringFailure) {
		try {
			AgentRunService.get().cancelRun(agentRunId, reason);
		} catch (RuntimeException cancellationFailure) {
			monitoringFailure.addSuppressed(cancellationFailure);
		}
	}

	/**
	 * Resolves how long this node may wait on its child agent. A configured value
	 * that is an exact {@code ${name}} reference is read from scope; anything
	 * absent or not positive falls back to the platform default.
	 */
	@SuppressWarnings("unchecked")
	private static long configuredAgentWaitTimeoutMs(Map<String, Object> node, Map<String, Object> scope) {
		Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		Object configured = rawConfig instanceof Map<?, ?> config
				? ((Map<String, Object>) config).get(AutomationConstants.CONFIG_WAIT_TIMEOUT_MS)
				: null;
		if (configured instanceof String value) {
			Matcher reference = EXACT_SCOPE_REFERENCE.matcher(value);
			if (reference.matches() && scope != null && scope.containsKey(reference.group(1))) {
				configured = scope.get(reference.group(1));
			}
		}
		long timeoutMs = positiveLong(configured);
		return timeoutMs > 0 ? timeoutMs : configuredAgentRunDefaultWaitTimeoutMs();
	}

	/**
	 * Returns the platform-wide agent wait timeout from
	 * {@code AGENT_RUN_WAIT_TIMEOUT_MS}, or the built-in default when it is unset
	 * or invalid.
	 */
	private static long configuredAgentRunDefaultWaitTimeoutMs() {
		long timeoutMs = positiveLong(Utility.getDIHelperProperty(AGENT_RUN_WAIT_TIMEOUT_PROPERTY));
		return timeoutMs > 0 ? timeoutMs : DEFAULT_AGENT_RUN_WAIT_TIMEOUT_MS;
	}

	/**
	 * Parses a positive millisecond value, returning zero for anything absent,
	 * unparseable, or not positive, so callers can treat zero as "not configured".
	 */
	private static long positiveLong(Object value) {
		if (value == null) {
			return 0L;
		}
		try {
			long parsed = Long.parseLong(value.toString().trim());
			return parsed > 0 ? parsed : 0L;
		} catch (NumberFormatException e) {
			return 0L;
		}
	}

	/**
	 * Wait budget that advances only while the child agent is working.
	 *
	 * <p>
	 * Time the child spends in {@code INPUT_REQUIRED} is not charged against the
	 * timeout, so a person taking hours to approve an action never causes the node
	 * to time out, while an agent that hangs while working still does.
	 */
	private static final class ActiveAgentWaitTimeout {

		private final long timeoutNanos;
		private long activeNanos;
		private long lastObservedNanos;
		private boolean paused;

		/**
		 * Starts the budget from the child's current status, so a child that is already
		 * waiting on a human begins paused. A timeout large enough to overflow
		 * nanoseconds is clamped rather than wrapping negative.
		 */
		private ActiveAgentWaitTimeout(long timeoutMs, String initialStatus, long initialObservedNanos) {
			this.timeoutNanos = timeoutMs > Long.MAX_VALUE / 1000000L ? Long.MAX_VALUE
					: TimeUnit.MILLISECONDS.toNanos(timeoutMs);
			this.lastObservedNanos = initialObservedNanos;
			this.paused = isInputRequiredStatus(initialStatus);
		}

		/**
		 * Charges elapsed time against the budget unless the child was paused for
		 * input, then records the status this observation saw as the new pause state.
		 */
		private void observe(String status, long observedNanos) {
			if (!this.paused && this.activeNanos < this.timeoutNanos) {
				long elapsed = observedNanos - this.lastObservedNanos;
				if (elapsed > 0) {
					long remainingNanos = this.timeoutNanos - this.activeNanos;
					this.activeNanos = elapsed >= remainingNanos ? this.timeoutNanos : this.activeNanos + elapsed;
				}
			}
			this.lastObservedNanos = observedNanos;
			this.paused = isInputRequiredStatus(status);
		}

		/**
		 * Returns whether the working budget is exhausted while the child is not
		 * waiting on a human.
		 */
		private boolean isExpired() {
			return !this.paused && this.activeNanos >= this.timeoutNanos;
		}

		/**
		 * Returns the next poll delay, shortened so the loop never sleeps past the
		 * remaining budget.
		 */
		private long nextPollDelayMs() {
			if (this.paused) {
				return AGENT_RUN_POLL_INTERVAL_MS;
			}
			long remainingNanos = this.timeoutNanos - this.activeNanos;
			if (remainingNanos <= 0) {
				return 1L;
			}
			long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
			return Math.min(AGENT_RUN_POLL_INTERVAL_MS, Math.max(1L, remainingMs));
		}
	}

	/**
	 * Returns whether the child agent has paused itself for a human decision.
	 */
	private static boolean isInputRequiredStatus(String status) {
		return "INPUT_REQUIRED".equalsIgnoreCase(status);
	}

	/**
	 * Creates the run-local Insight that owns this run's Python session, carrying
	 * the caller's user, base URL, and scheduler mode and scoped to the automation
	 * project.
	 */
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

	/**
	 * Drops the run-local Insight and its Python session. When normal teardown
	 * fails the Insight is still removed from the store so a failed run cannot leak
	 * a session.
	 */
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
			boolean generated = !AutomationConstants.NODE_CODE_MODE_CUSTOM
					.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
			if (generated && (AutomationConstants.NODE_MODEL_CHAT.equals(type)
					|| AutomationConstants.NODE_MODEL_VISION.equals(type)
					|| AutomationConstants.NODE_AGENT_RUN.equals(type))) {
				roomIds.put((String) node.get(AutomationConstants.NODE_FIELD_ID), UUID.randomUUID().toString());
			}
		}
		return roomIds;
	}

	/**
	 * Records a node whose work finished after cancellation was requested.
	 *
	 * <p>
	 * The node is terminal rather than successful, because the run is no longer proceeding, but
	 * its output is retained: the node already produced whatever side effects it produces, and
	 * this row is the only record of what they were. The value is kept out of scope, since no
	 * later node runs. A value that cannot be serialized is dropped rather than losing the
	 * cancellation record itself.
	 */
	private Map<String, Object> persistCancelledNodeResult(String runId, Map<String, Object> node, String nodeId,
			Object value, Timestamp started, long startedMs, String traceRoomId) {
		long duration = System.currentTimeMillis() - startedMs;
		String message = "Run cancelled by user";
		Object persistedValue = null;
		String output = null;
		try {
			persistedValue = splitGeneratedNodeResult(node, value).value();
			output = AutomationRuntimeUtils.toBoundedRuntimeJson(persistedValue,
					AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation node '" + nodeId + "' output");
		} catch (RuntimeException e) {
			classLogger.warn("Unable to retain the output of cancelled automation node '{}': {}", nodeId,
					e.getMessage());
		}
		String preview = AutomationRuntimeUtils.generatePreview(output);
		if (output == null) {
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, started, duration, message);
		} else {
			// A null agent run id preserves the one recorded when the child run started.
			AutomationDatabaseUtility.updateNodeFailedWithResult(runId, nodeId, started, duration,
					(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview, null, message);
		}
		streamNodeProgress(runId, node, AutomationConstants.STATUS_CANCELLED, duration, preview, message,
				traceForNode(node, traceRoomId, null, null));
		return nodeResult(nodeId, AutomationConstants.STATUS_CANCELLED, persistedValue, message);
	}

	/**
	 * Persists one node's outcome and returns what the control loop needs to
	 * continue.
	 *
	 * <p>
	 * Generated conversational nodes have their internal envelope split into value
	 * and trace. A child agent that asked for input becomes a durable wait instead
	 * of a result, which releases this worker. The prospective scope is
	 * size-checked before a success is recorded, so a node cannot succeed into a
	 * scope the next node could not carry.
	 */
	private Map<String, Object> persistNativeNodeResult(String runId, String projectId, Map<String, Object> node,
			Object value, Timestamp started, long startedMs, String traceRoomId, String resumeNodeId,
			Map<String, Object> scope) {
		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			return persistCancelledNodeResult(runId, node, nodeId, value, started, startedMs, traceRoomId);
		}
		GeneratedNodeResult generatedResult = splitGeneratedNodeResult(node, value);
		Object persistedValue = generatedResult.value();
		Object traceMetadata = generatedResult.metadata();
		String agentRunId = null;
		String agentFailure = null;
		boolean generatedAgentNode = isGeneratedAgentRunNode(node);
		if (generatedAgentNode) {
			Map<String, Object> agentResult = normalizeAgentResult(node, traceMetadata, traceRoomId);
			agentRunId = stringValue(agentResult.get("runId"));
			if (isInputRequiredStatus(stringValue(agentResult.get("status")))) {
				String output = AutomationRuntimeUtils.toBoundedRuntimeJson(persistedValue,
						AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation node '" + nodeId + "' waiting output");
				long duration = System.currentTimeMillis() - startedMs;
				String preview = AutomationRuntimeUtils.generatePreview(output);
				AutomationDatabaseUtility.persistAgentWait(runId, projectId, nodeId,
						(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview, agentRunId,
						traceRoomId, resumeNodeId, currentUserId(),
						Instant.now().plus(DEFAULT_AGENT_APPROVAL_TIMEOUT_HOURS, ChronoUnit.HOURS), duration);
				streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_WAITING_FOR_INPUT, duration, preview,
						null, traceForNode(node, traceRoomId, null, agentRunId, "INPUT_REQUIRED"));
				return nodeResult(nodeId, AutomationConstants.NODE_STATUS_WAITING_FOR_INPUT, persistedValue, null);
			}
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
			AutomationRuntimeUtils.toBoundedRuntimeJson(prospectiveScope, AutomationConstants.RUN_SCOPE_MAX_BYTES,
					"Automation run scope");
		}
		long duration = System.currentTimeMillis() - startedMs;
		String preview = AutomationRuntimeUtils.generatePreview(output);
		String modelMessageId = generatedAgentNode ? null : extractModelMessageId(node, traceMetadata, traceRoomId);
		if (agentFailure != null) {
			AutomationDatabaseUtility.updateNodeFailedWithResult(runId, nodeId, started, duration,
					(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview, agentRunId,
					agentFailure);
			streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_FAILED, duration, preview, agentFailure,
					traceForNode(node, traceRoomId, null, agentRunId));
			return nodeResult(nodeId, AutomationConstants.NODE_STATUS_FAILED, persistedValue, agentFailure);
		}
		AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, started, duration,
				(String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output, preview, modelMessageId,
				agentRunId);
		AutomationPythonRunRegistry.nodeCompleted(runId);
		streamNodeProgress(runId, node, AutomationConstants.NODE_STATUS_SUCCESS, duration, preview, null,
				traceForNode(node, traceRoomId, modelMessageId, agentRunId));
		return nodeResult(nodeId, AutomationConstants.NODE_STATUS_SUCCESS, persistedValue, null);
	}

	/**
	 * Terminally cancels a run parked on a durable agent wait.
	 *
	 * <p>
	 * The caller authorizes the trace, records the durable cancellation request,
	 * and stops the child agent before entering this boundary. This method owns
	 * only the parent run's terminal transition, and unlike
	 * {@link #resumeWaitingRun} it never continues the remaining graph - a child
	 * that settled as COMPLETED while the user was cancelling must not extend the
	 * run.
	 *
	 * @param runId     waiting Automation run
	 * @param projectId owning Automation project
	 * @return cancelled or current run detail
	 */
	Map<String, Object> cancelWaitingRun(String runId, String projectId) {
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !projectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run not found: " + runId);
		}
		if (!AutomationConstants.STATUS_WAITING_FOR_INPUT.equals(run.get(AutomationConstants.STATUS))) {
			return buildCurrentRunResult(runId, projectId);
		}
		Map<String, Object> wait = AutomationDatabaseUtility.claimWaitingRun(runId, projectId);
		if (wait == null) {
			// A concurrent resume owns the run. It observes the durable cancellation
			// request before its next node and completes the run as CANCELLED.
			return buildCurrentRunResult(runId, projectId);
		}
		String waitingNodeId = stringValue(wait.get(AutomationConstants.NODE_ID));
		String message = "Run cancelled by user";
		AutomationDatabaseUtility.updateNodeFailed(runId, waitingNodeId, utcNow(), 0, message);
		AutomationDatabaseUtility.resolveWait(runId, stringValue(wait.get(AutomationConstants.WAIT_ID)),
				currentUserId());
		AutomationDatabaseUtility.skipPendingNodes(runId, message);
		AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_CANCELLED, waitingNodeId,
				message);
		return buildResult(runId, projectId, Map.of("error", message));
	}

	/**
	 * Continues the same immutable Automation run after a durable agent input flow
	 * reaches a terminal state. The original worker and run-local Insight are never
	 * retained across the human wait; scope is reconstructed from the persisted
	 * input and successful node-output snapshots.
	 *
	 * @param runId     waiting Automation run
	 * @param projectId owning Automation project
	 * @return current or resumed run detail
	 */
	Map<String, Object> resumeWaitingRun(String runId, String projectId) {
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !projectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run not found: " + runId);
		}
		if (!AutomationConstants.STATUS_WAITING_FOR_INPUT.equals(run.get(AutomationConstants.STATUS))) {
			return buildCurrentRunResult(runId, projectId);
		}

		Map<String, Object> wait = AutomationDatabaseUtility.getActiveWait(runId);
		if (wait == null) {
			throw new IllegalStateException("Automation run has no active input boundary: " + runId);
		}
		String waitingNodeId = stringValue(wait.get(AutomationConstants.NODE_ID));
		String agentRunId = stringValue(wait.get(AutomationConstants.AGENT_RUN_ID));
		Map<String, Object> agent = AgentRunService.get().getRunForAutomation(agentRunId, requestInsight, false);
		if (!String.valueOf(wait.get(AutomationConstants.ROOM_ID)).equals(stringValue(agent.get("roomId")))) {
			throw new IllegalStateException("Agent run room does not match the Automation wait reference.");
		}
		String agentStatus = stringValue(agent.get("status"));
		if (!isAgentRunTerminalStatus(agentStatus)) {
			throw new IllegalStateException(
					"Agent run '" + agentRunId + "' has not completed its input flow (" + agentStatus + ").");
		}

		wait = AutomationDatabaseUtility.claimWaitingRun(runId, projectId);
		if (wait == null) {
			return buildCurrentRunResult(runId, projectId);
		}
		String waitId = stringValue(wait.get(AutomationConstants.WAIT_ID));
		if (!"COMPLETED".equalsIgnoreCase(agentStatus)) {
			return finishTerminalAgentWait(runId, projectId, waitingNodeId, agentRunId, agentStatus, agent, waitId);
		}

		Insight executionInsight = null;
		Map<String, Object> continuation = new LinkedHashMap<>();
		try {
			AutomationDefinitionValidator.ValidatedDefinition definition = AutomationDefinitionValidator
					.parseAndValidate(String.valueOf(run.get(AutomationConstants.DEFINITION_SNAPSHOT)));
			List<Map<String, Object>> runNodes = AutomationRuntime.nodesForRun(definition);
			Map<String, Object> waitingNode = runNodes.stream()
					.filter(node -> waitingNodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))).findFirst()
					.orElseThrow(() -> new IllegalStateException(
							"Saved waiting node is absent from the run snapshot: " + waitingNodeId));
			Object finalText = agent.get("finalText");
			if (stringValue(finalText) == null) {
				throw new IllegalStateException("Agent run '" + agentRunId + "' completed without final output.");
			}
			String output = AutomationRuntimeUtils.toBoundedRuntimeJson(finalText,
					AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation agent result");
			AutomationDatabaseUtility.updateNodeSuccess(runId, waitingNodeId, utcNow(), 0,
					(String) waitingNode.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR), output,
					AutomationRuntimeUtils.generatePreview(output), null, agentRunId);
			AutomationDatabaseUtility.resolveWait(runId, waitId, currentUserId());

			executionInsight = createExecutionInsight(projectId);
			PyTranslator translator = executionInsight.getPyTranslator();
			if (translator == null) {
				throw new IllegalStateException("Python runtime is not available for this insight.");
			}
			int completedNodes = (int) AutomationDatabaseUtility.getNodeOutputsForRun(runId).stream()
					.filter(row -> AutomationConstants.NODE_STATUS_SUCCESS.equals(row.get(AutomationConstants.STATUS))
							|| AutomationConstants.NODE_STATUS_SKIPPED.equals(row.get(AutomationConstants.STATUS)))
					.count();
			AutomationPythonRunRegistry.register(runId, translator, executionInsight, streamJobId, completedNodes);
			Map<String, Object> scope = reconstructScope(runId, executionInsight.getUser(),
					AutomationRuntime.startNodeId(definition));
			String resumeNodeId = stringValue(wait.get(AutomationConstants.RESUME_NODE_ID));
			if (resumeNodeId != null) {
				continuation = executeInControlOrder(executionInsight, projectId, runId, definition, runNodes,
						AutomationDatabaseUtility.getRunNodeSources(runId), scope, traceRoomIds(runId), resumeNodeId);
			} else {
				continuation.put("scope", scope);
			}
			if (!Boolean.TRUE.equals(continuation.get("waitingForInput"))) {
				finishRun(runId, projectId);
			}
		} catch (Exception e) {
			classLogger.error("Failed to resume Automation run '{}' after agent input", runId, e);
			finishFailedRun(runId, projectId, e);
			continuation = Map.of("error", safeMessage(e));
		} finally {
			AutomationPythonRunRegistry.unregister(runId);
			cleanupExecutionInsight(executionInsight);
		}
		return buildResult(runId, projectId, continuation);
	}

	/**
	 * Terminally reconciles a waiting run whose child agent settled as FAILED or
	 * CANCELLED: records the node result, resolves the wait, skips the remainder,
	 * and completes the run without executing any further node.
	 */
	private Map<String, Object> finishTerminalAgentWait(String runId, String projectId, String waitingNodeId,
			String agentRunId, String agentStatus, Map<String, Object> agent, String waitId) {
		String message = stringValue(agent.get("errorMessage"));
		if (message == null) {
			message = "Agent run '" + agentRunId + "' " + agentStatus.toLowerCase() + ".";
		}
		String output = AutomationRuntimeUtils.toBoundedRuntimeJson(agent.get("finalText"),
				AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Automation agent terminal result");
		AutomationDatabaseUtility.updateNodeFailedWithResult(runId, waitingNodeId, utcNow(), 0, null, output,
				AutomationRuntimeUtils.generatePreview(output), agentRunId, message);
		AutomationDatabaseUtility.resolveWait(runId, waitId, currentUserId());
		AutomationDatabaseUtility.skipPendingNodes(runId, "Skipped because the agent run did not complete");
		AutomationDatabaseUtility.completeRun(runId, projectId,
				"CANCELLED".equalsIgnoreCase(agentStatus) ? AutomationConstants.STATUS_CANCELLED
						: AutomationConstants.STATUS_FAILED,
				waitingNodeId, message);
		return buildResult(runId, projectId, Map.of("error", message));
	}

	/**
	 * Rebuilds the run scope from durable rows after a human wait, because the
	 * original in-memory scope was released with its worker.
	 *
	 * <p>
	 * Runtime metadata and the persisted trigger inputs are seeded first, then each
	 * successful node output is replayed under the name that node publishes it as.
	 *
	 * <p>
	 * The trigger is the one node that publishes many names at once, so its map is spread back
	 * as individual globals. It is identified by node ID rather than by an absent output name,
	 * because {@code control.if} rows also carry no name: their routing decision is consumed by
	 * the control loop and must stay out of scope, exactly as it does on a straight-through run.
	 *
	 * @param runId       run being resumed
	 * @param user        user the run executes as, for timezone-local runtime values
	 * @param startNodeId trigger node ID taken from this run's definition snapshot
	 * @return scope equivalent to the one the run held before it paused
	 */
	private static Map<String, Object> reconstructScope(String runId, prerna.auth.User user, String startNodeId) {
		Map<String, Object> scope = AutomationRuntimeUtils.buildInitialScope(runId, user);
		scope.putAll(AutomationDatabaseUtility.getRunInputs(runId));
		for (Map<String, Object> row : AutomationDatabaseUtility.getNodeOutputsForRun(runId)) {
			if (!AutomationConstants.NODE_STATUS_SUCCESS.equals(row.get(AutomationConstants.STATUS))) {
				continue;
			}
			Object raw = row.get(AutomationConstants.OUTPUT_VALUE);
			Object value = raw == null ? null : AutomationRuntimeUtils.GSON.fromJson(raw.toString(), Object.class);
			String outputVar = stringValue(row.get(AutomationConstants.OUTPUT_VAR_NAME));
			if (outputVar != null) {
				scope.put(outputVar, value);
			} else if (startNodeId.equals(stringValue(row.get(AutomationConstants.NODE_ID)))
					&& value instanceof Map<?, ?> globals) {
				for (Map.Entry<?, ?> entry : globals.entrySet()) {
					if (entry.getKey() instanceof String key) {
						scope.put(key, entry.getValue());
					}
				}
			}
		}
		AutomationRuntimeUtils.toBoundedRuntimeJson(scope, AutomationConstants.RUN_SCOPE_MAX_BYTES,
				"Reconstructed automation run scope");
		return scope;
	}

	/**
	 * Returns the rooms already allocated to this run's nodes, so a resumed run
	 * reuses them instead of creating new ones for nodes that already posted.
	 */
	private static Map<String, String> traceRoomIds(String runId) {
		Map<String, String> roomIds = new LinkedHashMap<>();
		for (Map<String, Object> row : AutomationDatabaseUtility.getNodeOutputsForRun(runId)) {
			String roomId = stringValue(row.get(AutomationConstants.ROOM_ID));
			if (roomId != null) {
				roomIds.put(String.valueOf(row.get(AutomationConstants.NODE_ID)), roomId);
			}
		}
		return roomIds;
	}

	/**
	 * Returns the acting user's ID, or the system user when the request carries no
	 * login token.
	 */
	private String currentUserId() {
		return requestInsight.getUser().getPrimaryLoginToken() == null ? AutomationConstants.SYSTEM_USER_ID
				: requestInsight.getUser().getPrimaryLoginToken().getId();
	}

	/**
	 * Returns the current UTC timestamp in the form the scheduler tables store.
	 */
	private static Timestamp utcNow() {
		return Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
	}

	/**
	 * Separates the business value from the trace metadata for generated
	 * conversational nodes, which return both inside one envelope. Any other node's
	 * result is its own value. A partial envelope is rejected rather than guessed
	 * at.
	 */
	private static GeneratedNodeResult splitGeneratedNodeResult(Map<String, Object> node, Object value) {
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		boolean internalResultType = AutomationConstants.NODE_MODEL_CHAT.equals(type)
				|| AutomationConstants.NODE_MODEL_VISION.equals(type)
				|| AutomationConstants.NODE_AGENT_RUN.equals(type);
		boolean generated = !AutomationConstants.NODE_CODE_MODE_CUSTOM
				.equals(node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
		if (!internalResultType || !generated || !(value instanceof Map<?, ?> map)) {
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

	/**
	 * One node result split into the value persisted as its output and the metadata
	 * kept for trace.
	 */
	private record GeneratedNodeResult(Object value, Object metadata) {
	}

	/**
	 * Normalizes a child agent response and proves it belongs to this node: the
	 * returned room must match the room allocated for this run, and any returned
	 * workspace must match the agent the node is configured with.
	 */
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

		String expectedWorkspaceId = AutomationRuntime.configuredAgentWorkspaceId(node);
		String returnedWorkspaceId = stringValue(normalized.get(AutomationConstants.CONFIG_WORKSPACE_ID));
		if (returnedWorkspaceId != null && !returnedWorkspaceId.equals(expectedWorkspaceId)) {
			throw new IllegalStateException("Automation agent node '" + node.get(AutomationConstants.NODE_FIELD_ID)
					+ "' returned a different workspaceId than the configured agent.");
		}
		normalized.put(AutomationConstants.CONFIG_WORKSPACE_ID, expectedWorkspaceId);
		return normalized;
	}

	/**
	 * Returns the message describing why a child agent run is not a usable result,
	 * or null when it completed.
	 */
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
		case "INPUT_REQUIRED" -> "Agent run '" + runId + "' requires user input before the automation can continue.";
		case "FAILED" -> {
			String error = stringValue(result.get("errorMessage"));
			yield error != null ? error : "Agent run '" + runId + "' failed.";
		}
		case "CANCELLED" -> "Agent run '" + runId + "' was cancelled.";
		default -> "Agent run '" + runId + "' did not reach COMPLETED status (" + status + ").";
		};
	}

	/**
	 * Error for a generated agent node whose Python did not return matching runId
	 * and roomId trace metadata, which is what binds the child run to this node.
	 */
	private static IllegalStateException missingAgentTrace(Map<String, Object> node) {
		return new IllegalStateException("Automation agent node '" + node.get(AutomationConstants.NODE_FIELD_ID)
				+ "' did not return matching runId and roomId trace metadata.");
	}

	/**
	 * Returns the message ID a generated chat or vision node produced, verifying it
	 * came from the room allocated to this run. Returns null for node types that do
	 * not post a message.
	 */
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
			throw new IllegalStateException("Automation model node '" + node.get(AutomationConstants.NODE_FIELD_ID)
					+ "' returned a different roomId than the room assigned to this run.");
		}
		if (messageId == null) {
			throw missingModelTrace(node);
		}
		return messageId;
	}

	/**
	 * Error for a generated model node whose Python did not return the required
	 * roomId and messageId.
	 */
	private static IllegalStateException missingModelTrace(Map<String, Object> node) {
		return new IllegalStateException("Automation model node '" + node.get(AutomationConstants.NODE_FIELD_ID)
				+ "' did not return the required roomId and messageId trace metadata.");
	}

	/**
	 * Returns the value as a string, or null when it is absent or blank, so callers
	 * can treat blank database columns and missing keys the same way.
	 */
	private static String stringValue(Object value) {
		if (value == null || value.toString().isBlank()) {
			return null;
		}
		return value.toString();
	}

	/**
	 * Streams a node transition that carries no trace metadata.
	 */
	private void streamNodeProgress(String runId, Map<String, Object> node, String status, Long durationMs,
			String outputPreview, String errorMessage) {
		streamNodeProgress(runId, node, status, durationMs, outputPreview, errorMessage, null);
	}

	/**
	 * Streams the run-start envelope carrying the immutable definition snapshot, so
	 * a live client renders the graph this run will actually execute rather than
	 * the one currently being edited.
	 */
	private void streamRunStarted(String runId, AutomationDefinitionValidator.ValidatedDefinition definition) {
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
		data.put(AutomationConstants.DEFINITION_VERSION, AutomationConstants.PYTHON_DOC_CURRENT_VERSION);
		data.put(AutomationConstants.DEFINITION_HASH, definition.hash());
		data.put(AutomationConstants.DEFINITION_SNAPSHOT, definition.snapshot());

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", AUTOMATION_STREAM_TYPE);
		envelope.put("data", data);
		jobManager.addStreamOut(jobId, envelope);
	}

	/**
	 * Streams one node transition to the caller's Pixel job. This is a no-op
	 * without a live job, so a scheduled run needs no listener and progress
	 * streaming never affects execution.
	 */
	private void streamNodeProgress(String runId, Map<String, Object> node, String status, Long durationMs,
			String outputPreview, String errorMessage, Map<String, Object> trace) {
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

	/**
	 * Builds a node trace with no agent status attached.
	 */
	private static Map<String, Object> traceForNode(Map<String, Object> node, String roomId, String modelMessageId,
			String agentRunId) {
		return traceForNode(node, roomId, modelMessageId, agentRunId, null);
	}

	/**
	 * Same as {@link #traceForNode(Map, String, String, String)}, additionally
	 * stamping the durable agent run's current status (e.g. INPUT_REQUIRED) so the
	 * frontend can distinguish "actively working" from "waiting on a human" without
	 * any separate plumbing — it flows through the same trace object the node's
	 * agentRunId already relies on.
	 */
	private static Map<String, Object> traceForNode(Map<String, Object> node, String roomId, String modelMessageId,
			String agentRunId, String agentStatus) {
		Map<String, Object> trace = trace(roomId, modelMessageId, agentRunId,
				AutomationRuntime.configuredAgentWorkspaceId(node));
		Object nodeId = node.get(AutomationConstants.NODE_FIELD_ID);
		if (nodeId != null) {
			trace.put(AutomationConstants.TRACE_NODE_ID, nodeId);
		}
		if (agentStatus != null) {
			trace.put(AutomationConstants.TRACE_AGENT_STATUS, agentStatus);
		}
		return trace;
	}

	/**
	 * Assembles the trace map, omitting every identifier this node does not have so
	 * a consumer can treat presence as meaning.
	 */
	private static Map<String, Object> trace(String roomId, String modelMessageId, String agentRunId,
			String workspaceId) {
		Map<String, Object> trace = new LinkedHashMap<>();
		if (roomId != null) {
			trace.put(AutomationConstants.TRACE_ROOM_ID, roomId);
		}
		if (workspaceId != null) {
			trace.put(AutomationConstants.TRACE_WORKSPACE_ID, workspaceId);
		}
		if (modelMessageId != null) {
			trace.put(AutomationConstants.TRACE_MODEL_MESSAGE_ID, modelMessageId);
		}
		if (agentRunId != null) {
			trace.put(AutomationConstants.TRACE_AGENT_RUN_ID, agentRunId);
		}
		return trace;
	}

	/**
	 * Builds the in-memory result the control loop reads to decide whether and
	 * where to continue.
	 */
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

	/**
	 * Writes the terminal status for a run whose control path ended.
	 *
	 * <p>
	 * A cancellation request wins over everything. Otherwise the first failed node
	 * fails the run, and a node still pending or running means its Python returned
	 * no structured result, which is also a failure. Only a run with every node
	 * settled is recorded as successful.
	 */
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

	/**
	 * Writes the terminal status after execution threw, attributing the failure to
	 * the first node recorded as failed and reporting cancellation when that is
	 * what interrupted the run.
	 */
	private void finishFailedRun(String runId, String projectId, Exception error) {
		if (AutomationPythonRunRegistry.isCancellationRequested(runId)) {
			AutomationDatabaseUtility.skipPendingNodes(runId, "Run cancelled by user");
			AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_CANCELLED, null,
					"Run cancelled by user");
			return;
		}
		String failedNodeId = AutomationDatabaseUtility.getNodeOutputsForRun(runId).stream()
				.filter(output -> AutomationConstants.NODE_STATUS_FAILED.equals(output.get(AutomationConstants.STATUS)))
				.map(output -> (String) output.get(AutomationConstants.NODE_ID)).findFirst().orElse(null);
		AutomationDatabaseUtility.skipPendingNodes(runId, "Python runtime failed before this node executed");
		AutomationDatabaseUtility.completeRun(runId, projectId, AutomationConstants.STATUS_FAILED, failedNodeId,
				safeMessage(error));
	}

	/**
	 * Assembles the reactor response from durable run state plus this execution's
	 * scope and globals, and persists the human-readable summary alongside it.
	 */
	private Map<String, Object> buildResult(String runId, String projectId, Map<String, Object> pythonResult) {
		Map<String, Object> detail = AutomationDatabaseUtility.getRunDetail(runId);
		if (detail == null) {
			detail = new LinkedHashMap<>();
			detail.put(AutomationConstants.RUN_ID, runId);
			detail.put(AutomationConstants.PROJECT_ID, projectId);
		}
		List<Map<String, Object>> nodeResults = AutomationDatabaseUtility
				.buildNodeResults(AutomationDatabaseUtility.getNodeOutputsForRun(runId));
		detail.put(AutomationConstants.RESULT_NODE_RESULTS, nodeResults);
		detail.put("scope", normalizeScope(pythonResult.get("scope")));
		detail.put(AutomationConstants.RESULT_GLOBALS,
				normalizeScope(pythonResult.get(AutomationConstants.RESULT_GLOBALS)));
		detail.put("pythonResult", pythonResult);
		String summary = AutomationConstants.STATUS_SUCCESS.equals(detail.get(AutomationConstants.STATUS))
				? "Automation completed successfully (" + nodeResults.size() + " nodes)."
				: AutomationConstants.STATUS_WAITING_FOR_INPUT.equals(detail.get(AutomationConstants.STATUS))
						? "Automation paused for agent approval or input."
						: AutomationConstants.STATUS_CANCELLED.equals(detail.get(AutomationConstants.STATUS))
								? "Automation cancelled."
								: buildFailureSummary(detail);
		detail.put(AutomationConstants.RESULT_SUMMARY, summary);
		AutomationDatabaseUtility.updateRunSummary(runId, summary);
		return detail;
	}

	/**
	 * Returns a string-keyed copy of a scope-shaped value, and an empty map for
	 * anything that is not a map.
	 */
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

	/**
	 * Returns the project's asset folder, which bounds the file access granted to
	 * node Python.
	 */
	private String getProjectAssetsFolder(String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Project was not found: " + projectId);
		}
		return EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				project.getProjectName());
	}

	/**
	 * Returns the project's py folder, added to the Python path so node source can
	 * import project modules.
	 */
	private String getProjectPyFolder(String projectId) {
		return getProjectAssetsFolder(projectId) + File.separator + "py";
	}

	/**
	 * Returns an error's message, or its class name when the message is null, so a
	 * persisted failure always says something.
	 */
	private static String safeMessage(Exception error) {
		return error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
	}

	/**
	 * Returns the human-readable summary for a run that neither succeeded nor was
	 * cancelled.
	 */
	private static String buildFailureSummary(Map<String, Object> runDetail) {
		Object failedNodeId = runDetail.get(AutomationConstants.FAILED_NODE_ID);
		Object errorMessage = runDetail.get(AutomationConstants.ERROR_MESSAGE);
		return "Automation failed at node " + (failedNodeId != null ? failedNodeId : "unknown") + ": "
				+ (errorMessage != null ? errorMessage : "no error details available");
	}

}
