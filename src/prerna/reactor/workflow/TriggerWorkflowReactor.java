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
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.f4b6a3.uuid.alt.GUID;

import org.apache.hc.core5.http.ContentType;

import prerna.engine.api.IRDBMSEngine;
import prerna.security.HttpHelperUtility;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Executes a workflow app's graph top-to-bottom with DB-backed state.
 *
 * <p>Pixel: {@code TriggerWorkflow(project=["appId"], manual=["true"])}
 * <p>Pixel: {@code TriggerWorkflow(project=["appId"], resumeRunId=["uuid"])}
 *
 * <p>Execution model:
 * <ul>
 *   <li>Concurrency guard — rejects if a run is already active for this project</li>
 *   <li>DB checkpoint per node — each completed node is committed immediately</li>
 *   <li>Stop on error — first node failure halts the pipeline</li>
 *   <li>Heartbeat — updated every 30s to prove liveness</li>
 *   <li>Resume — skips nodes that succeeded in a prior run, re-runs from failure</li>
 * </ul>
 *
 * <p>State is written to WORKFLOW_RUNS and WORKFLOW_NODE_OUTPUTS in the scheduler DB
 * via {@link WorkflowDatabaseUtility}.
 */
public class TriggerWorkflowReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TriggerWorkflowReactor.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * Registry of active run cancellation flags. Keyed by runId.
	 * When a cancel is requested, the flag is set to true and the executor checks
	 * between nodes.
	 */
	private static final ConcurrentHashMap<String, AtomicBoolean> CANCELLATION_FLAGS = new ConcurrentHashMap<>();

	/**
	 * Per-thread JS engine cache — avoids the expensive SPI classpath scan that
	 * ScriptEngineManager performs on every construction. Each thread in WORKFLOW_EXECUTOR
	 * initialises its own engine once and reuses it for all subsequent evaluations.
	 * The value is null when no JS engine is available on this JVM.
	 */
	private static final ThreadLocal<javax.script.ScriptEngine> JS_ENGINE_CACHE =
			ThreadLocal.withInitial(() -> {
				javax.script.ScriptEngineManager m = new javax.script.ScriptEngineManager();
				javax.script.ScriptEngine e = m.getEngineByName("js");
				if (e == null) e = m.getEngineByName("JavaScript");
				if (e == null) e = m.getEngineByName("nashorn");
				return e;
			});

	/**
	 * Background pool for workflow execution. Bounded at 20 concurrent runs with a small queue
	 * for brief spikes. Rejects beyond capacity so the caller gets an immediate error rather than
	 * unbounded thread growth.
	 */
	private static final ExecutorService WORKFLOW_EXECUTOR = new ThreadPoolExecutor(
			2, 20, 60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(10),
			r -> {
				Thread t = new Thread(r, "workflow-run-" + System.nanoTime());
				t.setDaemon(true);
				return t;
			},
			new ThreadPoolExecutor.AbortPolicy()
	);

	public TriggerWorkflowReactor() {
		this.keysToGet = new String[]{ "project", "manual", "resumeRunId", "triggerType" };
		this.keyRequired = new int[]{ 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String resumeRunId = this.keyValue.get(this.keysToGet[2]);

		// Concurrency guard
		String activeRun = WorkflowDatabaseUtility.getActiveRun(projectId);
		if (activeRun != null) {
			throw new IllegalArgumentException(
					"Workflow already has an active run: " + activeRun +
					". Wait for it to complete or cancel it before starting a new run.");
		}

		// Determine trigger type
		String triggerType = determineTriggerType(resumeRunId);
		String userId = getUserId();
		String runId = UUID.randomUUID().toString();

		// Load workflow definition and config
		Map<String, Object> doc = loadWorkflowDoc(projectId);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> edges = (List<Map<String, Object>>) graph.get("edges");
		Map<String, String> configMap = WorkflowExecutionUtils.loadConfig(projectId);

		// Topological sort
		List<Map<String, Object>> ordered = topoSort(nodes, edges);
		if (ordered.isEmpty()) {
			throw new IllegalArgumentException("Workflow has no nodes to execute");
		}

		// Create run record in DB
		WorkflowDatabaseUtility.insertRun(runId, projectId, WorkflowConstants.DEFAULT_WORKFLOW_ID,
				triggerType, resumeRunId, ordered.size(), userId);
		WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

		// Load prior outputs if resuming
		Map<String, String> priorOutputs = loadPriorOutputs(resumeRunId);

		// Execute nodes on a background thread — a workflow can run for hours (large for-each
		// ingestion jobs), so it must never block the calling request/websocket thread.
		// Progress is checkpointed to WORKFLOW_RUNS/WORKFLOW_NODE_OUTPUTS per node; the caller
		// (FE) polls GetWorkflowRun(runId) for live status instead of awaiting this call.
		try {
			WORKFLOW_EXECUTOR.submit(() -> {
				try {
					executeNodes(runId, projectId, ordered, configMap, priorOutputs);
				} catch (Exception e) {
					classLogger.error("Unhandled error executing workflow run {}: {}", runId, e.getMessage(), e);
					WorkflowDatabaseUtility.updateRunStatus(runId,
							WorkflowConstants.STATUS_FAILED, null, e.getMessage());
				}
			});
		} catch (RejectedExecutionException e) {
			WorkflowDatabaseUtility.updateRunStatus(runId, WorkflowConstants.STATUS_FAILED,
					null, "Server is at capacity — too many concurrent workflow runs");
			throw new IllegalStateException("Too many concurrent workflow runs. Please try again shortly.");
		}

		Map<String, Object> result = buildRunResult(runId, projectId, WorkflowConstants.STATUS_RUNNING,
				ordered.size(), 0, null, new ArrayList<>());
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	// ── Core Execution ────────────────────────────────────────────────────────────

	private Map<String, Object> executeNodes(String runId, String projectId,
			List<Map<String, Object>> ordered, Map<String, String> configMap,
			Map<String, String> priorOutputs) {
		return executeNodes(runId, projectId, ordered, configMap, priorOutputs,
				null, java.util.Collections.singleton(projectId));
	}

	/**
	 * Executes an ordered node list for a run. Used both for top-level runs (manual/scheduled/
	 * resume, {@code extraInitialScope} null) and for sub-workflow calls, where
	 * {@code extraInitialScope} carries the resolved {@code inputMapping} values and
	 * {@code ancestorProjectIds} carries the chain of project ids already executing on this
	 * call stack (self/transitive-call cycle guard).
	 */
	private Map<String, Object> executeNodes(String runId, String projectId,
			List<Map<String, Object>> ordered, Map<String, String> configMap,
			Map<String, String> priorOutputs, Map<String, String> extraInitialScope,
			Set<String> ancestorProjectIds) {

		// Register cancellation flag
		AtomicBoolean cancelled = new AtomicBoolean(false);
		CANCELLATION_FLAGS.put(runId, cancelled);

		// Start heartbeat
		ScheduledExecutorService heartbeat = startHeartbeat(runId);

		Map<String, String> scope = buildInitialScope(runId);
		if (extraInitialScope != null) {
			scope.putAll(extraInitialScope);
		}
		List<Map<String, Object>> nodeResults = new ArrayList<>();
		int completedCount = 0;

		try {
			for (int i = 0; i < ordered.size(); i++) {
				Map<String, Object> node = ordered.get(i);
				String nodeId = (String) node.get("id");
				String nodeLabel = (String) node.get("label");
				String outputVar = (String) node.get("outputVar");
				String nodeType = (String) node.get("type");

				// Check cancellation between nodes
				if (cancelled.get()) {
					WorkflowDatabaseUtility.updateRunStatus(runId,
							WorkflowConstants.STATUS_CANCELLED, nodeId, "Run cancelled by user");
					nodeResults.add(buildNodeResult(nodeId, nodeLabel,
							WorkflowConstants.STATUS_CANCELLED, 0, null, "Run cancelled by user"));
					return buildRunResult(runId, projectId, WorkflowConstants.STATUS_CANCELLED,
							ordered.size(), completedCount, nodeId, nodeResults);
				}

				// Resume: skip nodes that already succeeded in the prior run
				if (shouldSkipForResume(nodeId, outputVar, priorOutputs, scope)) {
					nodeResults.add(buildNodeResult(nodeId, nodeLabel,
							WorkflowConstants.NODE_STATUS_SKIPPED, 0,
							PixelExecutionUtils.generatePreview(priorOutputs.get(nodeId)), null));
					completedCount++;
					WorkflowDatabaseUtility.updateHeartbeat(runId, completedCount);
					continue;
				}

				// Execute this node
				Map<String, Object> nodeResult = executeSingleNode(
						runId, node, scope, configMap, completedCount, ancestorProjectIds);

				String status = (String) nodeResult.get(WorkflowConstants.STATUS);
				nodeResults.add(nodeResult);

				if (WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					// Store output in scope for downstream nodes.
					// set-variable nodes write individual variables directly into scope
					// inside executeSetVariableNode — skip the generic put to avoid
					// overwriting those keys with the JSON blob.
					if (outputVar != null && !outputVar.isEmpty()
							&& !WorkflowConstants.NODE_SET_VARIABLE.equals(nodeType)) {
						String outputValue = (String) nodeResult.get("outputValue");
						scope.put(outputVar, outputValue != null ? outputValue : "");
					}
					completedCount++;
					WorkflowDatabaseUtility.updateHeartbeat(runId, completedCount);
				} else {
					// STOP on error
					String errorMsg = (String) nodeResult.get(WorkflowConstants.ERROR_MESSAGE);
					WorkflowDatabaseUtility.updateRunStatus(runId,
							WorkflowConstants.STATUS_FAILED, nodeId, errorMsg);
					return buildRunResult(runId, projectId, WorkflowConstants.STATUS_FAILED,
							ordered.size(), completedCount, nodeId, nodeResults);
				}
			}

			// All nodes succeeded
			WorkflowDatabaseUtility.updateRunStatus(runId,
					WorkflowConstants.STATUS_SUCCESS, null, null);
			return buildRunResult(runId, projectId, WorkflowConstants.STATUS_SUCCESS,
					ordered.size(), completedCount, null, nodeResults);

		} finally {
			heartbeat.shutdownNow();
			CANCELLATION_FLAGS.remove(runId);
		}
	}

	private Map<String, Object> executeSingleNode(String runId, Map<String, Object> node,
			Map<String, String> scope, Map<String, String> configMap, int completedCount,
			Set<String> ancestorProjectIds) {

		String nodeId = (String) node.get("id");
		String nodeLabel = (String) node.get("label");
		String outputVar = (String) node.get("outputVar");
		String type = (String) node.get("type");

		// Mark node as running
		WorkflowDatabaseUtility.markNodeRunning(runId, nodeId);
		Timestamp startedAt = toTimestamp(Instant.now());
		long startMs = System.currentTimeMillis();

		try {
			Object rawOutput;
			Integer rowCount = null;

			if (WorkflowConstants.NODE_FOR_EACH.equals(type)) {
				// For-each nodes delegate to ForEachNodeExecutor
				AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
				Map<String, Object> forEachResult = prerna.reactor.workflow.foreach.ForEachNodeExecutor.execute(
						this.insight, node, scope, configMap, runId, cancelFlag);
				rawOutput = forEachResult;
				rowCount = (Integer) forEachResult.get("totalRows");
			} else if (WorkflowConstants.NODE_SUB_WORKFLOW.equals(type)) {
				rawOutput = executeSubWorkflowNode(runId, nodeId, node, scope, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_CONDITIONAL.equals(type)) {
				rawOutput = executeConditionalNode(runId, node, scope, configMap, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_WHILE_LOOP.equals(type)) {
				rawOutput = executeWhileLoopNode(runId, node, scope, configMap, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_TRY_CATCH.equals(type)) {
				rawOutput = executeTryCatchNode(runId, node, scope, configMap, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_WAIT.equals(type)) {
				rawOutput = executeWaitNode(node, scope, configMap);
			} else if (WorkflowConstants.NODE_SET_VARIABLE.equals(type)) {
				rawOutput = executeSetVariableNode(node, scope, configMap);
			} else if (WorkflowConstants.NODE_EMAIL.equals(type)) {
				rawOutput = executeEmailNode(node, scope, configMap);
			} else if (WorkflowConstants.NODE_HTTP_REQUEST.equals(type)) {
				rawOutput = executeHttpRequestNode(node, scope, configMap);
			} else if (WorkflowConstants.NODE_NOTIFICATION.equals(type)) {
				rawOutput = executeNotificationNode(node, scope, configMap);
			} else if (WorkflowConstants.NODE_SWITCH.equals(type)) {
				rawOutput = executeSwitchNode(runId, node, scope, configMap, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_RETRY.equals(type)) {
				rawOutput = executeRetryNode(runId, node, scope, configMap, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_PARALLEL.equals(type)) {
				rawOutput = executeParallelNode(runId, node, scope, configMap, ancestorProjectIds);
			} else if (WorkflowConstants.NODE_TRANSFORM.equals(type)) {
				rawOutput = executeTransformNode(node, scope);
			} else {
				rawOutput = executeNodePixel(node, scope, configMap);
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get("outputTransform");
			String transformed = WorkflowExecutionUtils.applyOutputTransform(rawOutput, transformConfig);

			long durationMs = System.currentTimeMillis() - startMs;
			String preview;
			if (WorkflowConstants.NODE_WHILE_LOOP.equals(type) && transformed != null
					&& transformed.contains("\"__whileResult\":true")) {
				try {
					@SuppressWarnings("unchecked")
					Map<String, Object> wr = new Gson().fromJson(transformed, Map.class);
					Number count = (Number) wr.get("iterationCount");
					long n = count != null ? count.longValue() : 0;
					preview = n + " iteration" + (n == 1 ? "" : "s");
				} catch (Exception ignored) {
					preview = PixelExecutionUtils.generatePreview(transformed);
				}
			} else {
				preview = PixelExecutionUtils.generatePreview(transformed);
			}

			WorkflowDatabaseUtility.updateNodeSuccess(runId, nodeId, startedAt,
					durationMs, outputVar, transformed, preview, rowCount);

			Map<String, Object> result = buildNodeResult(nodeId, nodeLabel,
					WorkflowConstants.NODE_STATUS_SUCCESS, durationMs, preview, null);
			result.put("outputValue", transformed);
			if (rowCount != null) {
				result.put(WorkflowConstants.ROW_COUNT, rowCount);
			}
			return result;

		} catch (Exception e) {
			long durationMs = System.currentTimeMillis() - startMs;
			String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			classLogger.error("Node {} ({}) failed: {}", nodeId, nodeLabel, errorMsg, e);

			WorkflowDatabaseUtility.updateNodeFailed(runId, nodeId, startedAt, durationMs, errorMsg);

			return buildNodeResult(nodeId, nodeLabel,
					WorkflowConstants.NODE_STATUS_FAILED, durationMs, null, errorMsg);
		}
	}

	// ── Sub-Workflow Execution ─────────────────────────────────────────────────────

	/**
	 * Executes a "sub-workflow" node: recurses into another project's workflow.json,
	 * runs it to completion synchronously (we're already on the background executor thread
	 * from the parent run), and returns its final run result as this node's raw output.
	 *
	 * <p>Config: {@code {targetProjectId, inputMapping: {childVar: "${parentVar} literal"}}}.
	 * {@code inputMapping} values are resolved against the parent's current scope before
	 * being seeded into the child run's initial scope.
	 */
	@SuppressWarnings("unchecked")
	private Object executeSubWorkflowNode(String parentRunId, String parentNodeId,
			Map<String, Object> node, Map<String, String> scope, Set<String> ancestorProjectIds) {

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		String targetProjectId = config != null
				? (String) config.get(WorkflowConstants.SUB_WORKFLOW_TARGET_PROJECT) : null;
		if (targetProjectId == null || targetProjectId.isBlank()) {
			throw new IllegalArgumentException("Sub-workflow node \"" + node.get("label") +
					"\" has no targetProjectId configured");
		}

		targetProjectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), targetProjectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), targetProjectId)) {
			throw new IllegalArgumentException("Target workflow project does not exist or user does not have access");
		}

		if (ancestorProjectIds.contains(targetProjectId)) {
			throw new IllegalStateException("Cycle detected: workflow " + targetProjectId +
					" is already running upstream in this call chain (" +
					String.join(" -> ", ancestorProjectIds) + ") — a workflow cannot call itself, " +
					"directly or transitively");
		}
		if (ancestorProjectIds.size() >= WorkflowConstants.MAX_SUB_WORKFLOW_DEPTH) {
			throw new IllegalStateException("Sub-workflow call depth exceeded (" +
					WorkflowConstants.MAX_SUB_WORKFLOW_DEPTH + ") — possible runaway recursion");
		}

		String activeRun = WorkflowDatabaseUtility.getActiveRun(targetProjectId);
		if (activeRun != null) {
			throw new IllegalStateException("Target workflow " + targetProjectId +
					" already has an active run (" + activeRun + ") — cannot start a sub-workflow call " +
					"while it is busy");
		}

		Map<String, Object> childDoc = loadWorkflowDoc(targetProjectId);
		Map<String, Object> childGraph = (Map<String, Object>) childDoc.get("graph");
		List<Map<String, Object>> childNodes = (List<Map<String, Object>>) childGraph.get("nodes");
		List<Map<String, Object>> childEdges = (List<Map<String, Object>>) childGraph.get("edges");
		Map<String, String> childConfigMap = WorkflowExecutionUtils.loadConfig(targetProjectId);

		List<Map<String, Object>> childOrdered = topoSort(childNodes, childEdges);
		if (childOrdered.isEmpty()) {
			throw new IllegalArgumentException("Target workflow " + targetProjectId + " has no nodes to execute");
		}

		Map<String, String> childInitialScope = new HashMap<>();
		Object inputMappingRaw = config.get(WorkflowConstants.SUB_WORKFLOW_INPUT_MAPPING);
		Map<String, Object> inputMapping = coerceToMap(inputMappingRaw);
		for (Map.Entry<String, Object> e : inputMapping.entrySet()) {
			String template = e.getValue() != null ? e.getValue().toString() : "";
			childInitialScope.put(e.getKey(), WorkflowExecutionUtils.resolve(template, scope, java.util.Collections.emptyMap()));
		}

		String childRunId = UUID.randomUUID().toString();
		Set<String> childAncestors = new java.util.HashSet<>(ancestorProjectIds);
		childAncestors.add(targetProjectId);

		WorkflowDatabaseUtility.insertRun(childRunId, targetProjectId, WorkflowConstants.DEFAULT_WORKFLOW_ID,
				WorkflowConstants.TRIGGER_SUB_WORKFLOW, null, childOrdered.size(), getUserId(),
				parentRunId, parentNodeId);
		WorkflowDatabaseUtility.insertAllNodeOutputs(childRunId, childOrdered);

		Map<String, Object> childResult = executeNodes(childRunId, targetProjectId, childOrdered,
				childConfigMap, new HashMap<>(), childInitialScope, childAncestors);

		String childStatus = (String) childResult.get(WorkflowConstants.STATUS);
		if (!WorkflowConstants.STATUS_SUCCESS.equals(childStatus)) {
			throw new IllegalStateException("Sub-workflow " + targetProjectId + " (run " + childRunId +
					") did not complete successfully: status=" + childStatus + ", error=" +
					childResult.get(WorkflowConstants.ERROR_MESSAGE));
		}
		return childResult;
	}

	// ── Conditional Node Execution ────────────────────────────────────────────────

	/**
	 * Executes a "conditional" node: evaluates the condition expression against the
	 * current scope, then runs the chosen branch (trueGraph or falseGraph) synchronously.
	 * Branch node outputs are merged into the parent scope so downstream nodes can
	 * reference them. Returns the last branch node's output, or the string "true"/"false"
	 * if the chosen branch has no nodes.
	 */
	@SuppressWarnings("unchecked")
	private Object executeConditionalNode(String runId, Map<String, Object> node,
			Map<String, String> scope, Map<String, String> configMap,
			Set<String> ancestorProjectIds) {

		// Fix: null-safe label extraction (node.get("label") can be null/absent)
		String nodeLabel = node.get("label") != null ? node.get("label").toString() : "unnamed";

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		if (config == null) {
			throw new IllegalArgumentException("Conditional node \"" + nodeLabel + "\" has no config");
		}

		String conditionTemplate = (String) config.get("condition");
		if (conditionTemplate == null || conditionTemplate.isBlank()) {
			throw new IllegalArgumentException("Conditional node \"" + nodeLabel + "\" has no condition set");
		}

		// Substitute scope variables into the condition expression, then evaluate as JS
		String condition = WorkflowExecutionUtils.resolve(conditionTemplate, scope, configMap);
		if (condition.equals(conditionTemplate)) {
			classLogger.warn("Conditional node \"{}\": condition template unchanged after resolve — " +
					"check that variable names match outputVar fields. Available scope keys: {}",
					nodeLabel, scope.keySet());
		}
		boolean result = evaluateCondition(condition, nodeLabel);

		// Pick the appropriate branch graph
		Map<String, Object> branchGraph = (Map<String, Object>) config.get(result ? "trueGraph" : "falseGraph");
		List<Map<String, Object>> branchNodes = branchGraph != null
				? (List<Map<String, Object>>) branchGraph.get("nodes") : null;
		List<Map<String, Object>> branchEdges = branchGraph != null
				? (List<Map<String, Object>>) branchGraph.get("edges") : null;

		if (branchNodes == null || branchNodes.isEmpty()) {
			return result ? "true" : "false";
		}

		// Sort and execute branch nodes, inheriting (and writing back into) the parent scope
		List<Map<String, Object>> ordered = topoSort(branchNodes, branchEdges);

		// Fix: pre-insert branch node rows so markNodeRunning/updateNodeSuccess land correctly
		WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

		// Fix: honour cancellation between branch steps
		AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
		String lastOutput = result ? "true" : "false";
		for (Map<String, Object> branchNode : ordered) {
			if (cancelFlag != null && cancelFlag.get()) {
				throw new IllegalStateException("Run cancelled by user");
			}
			String branchNodeId = (String) branchNode.get("id");
			String branchOutputVar = (String) branchNode.get("outputVar");
			Map<String, Object> branchResult = executeSingleNode(
					runId, branchNode, scope, configMap, 0, ancestorProjectIds);
			String status = (String) branchResult.get(WorkflowConstants.STATUS);
			if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
				String errorMsg = (String) branchResult.get(WorkflowConstants.ERROR_MESSAGE);
				throw new IllegalStateException("Conditional branch node " + branchNodeId +
						" failed: " + errorMsg);
			}
			// Fix: update lastOutput whenever a value is produced, regardless of outputVar
			String outputValue = (String) branchResult.get("outputValue");
			if (outputValue != null) {
				lastOutput = outputValue;
				if (branchOutputVar != null && !branchOutputVar.isEmpty()) {
					scope.put(branchOutputVar, outputValue);
				}
			}
		}
		return lastOutput;
	}

	/**
	 * Evaluates a condition expression string as JavaScript. After scope variable
	 * substitution the expression is passed to the JS engine — e.g. {@code "0.85 > 0.8"},
	 * {@code '"hello" === "hello"'}, {@code "null != null"}.
	 *
	 * <p>Falls back to a truthy string check if no JS engine is available.
	 */
	private boolean evaluateCondition(String expression, String nodeLabel) {
		javax.script.ScriptEngine engine = JS_ENGINE_CACHE.get();

		if (engine == null) {
			// No JS engine — fall back to simple truthy check
			String trimmed = expression.trim();
			return !trimmed.isEmpty() && !"false".equalsIgnoreCase(trimmed)
					&& !"null".equalsIgnoreCase(trimmed) && !"0".equals(trimmed);
		}

		try {
			Object evalResult = engine.eval(expression);
			if (evalResult instanceof Boolean) return (Boolean) evalResult;
			if (evalResult instanceof Number) return ((Number) evalResult).doubleValue() != 0;
			if (evalResult == null) return false;
			String s = evalResult.toString().trim();
			return !s.isEmpty() && !"false".equalsIgnoreCase(s)
					&& !"null".equalsIgnoreCase(s) && !"0".equals(s);
		} catch (javax.script.ScriptException e) {
			throw new IllegalArgumentException("Conditional node \"" + nodeLabel +
					"\" — condition evaluation failed: " + e.getMessage() +
					"\n  Expression: " + expression);
		}
	}

	// ── While-Loop Execution ──────────────────────────────────────────────────────

	/**
	 * Executes a "while-loop" node: evaluates a JS condition before each iteration
	 * and runs the inner sub-pipeline while it remains true. Iteration stops when the
	 * condition becomes false or {@code maxIterations} is reached.
	 *
	 * <p>Config: {@code {condition, maxIterations, subGraph: {nodes, edges}}}.
	 * Inner node outputs are written back into the parent scope after each iteration
	 * so the condition can test them.
	 */
	@SuppressWarnings("unchecked")
	private Object executeWhileLoopNode(String runId, Map<String, Object> node,
			Map<String, String> scope, Map<String, String> configMap,
			Set<String> ancestorProjectIds) {

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		String nodeLabel = node.get("label") != null ? node.get("label").toString() : "unnamed";

		String conditionTemplate = (String) config.get("condition");
		if (conditionTemplate == null || conditionTemplate.isBlank()) {
			throw new IllegalArgumentException("While-loop node \"" + nodeLabel + "\" has no condition set");
		}

		int maxIterations = 100;
		Object maxRaw = config.get("maxIterations");
		if (maxRaw != null) {
			try { maxIterations = Integer.parseInt(maxRaw.toString()); } catch (NumberFormatException ignored) {}
		}

		Map<String, Object> subGraph = config.get("subGraph") instanceof Map
				? (Map<String, Object>) config.get("subGraph") : null;
		List<Map<String, Object>> loopNodes = subGraph != null
				? (List<Map<String, Object>>) subGraph.get("nodes") : null;
		List<Map<String, Object>> loopEdges = subGraph != null
				? (List<Map<String, Object>>) subGraph.get("edges") : null;

		AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
		String lastOutput = "0";
		List<Map<String, Object>> iterationSummaries = new ArrayList<>();

		for (int iteration = 0; iteration < maxIterations; iteration++) {
			if (cancelFlag != null && cancelFlag.get()) {
				throw new IllegalStateException("Run cancelled by user");
			}

			String condition = WorkflowExecutionUtils.resolve(conditionTemplate, scope, configMap);
			if (!evaluateCondition(condition, nodeLabel)) break;

			if (loopNodes == null || loopNodes.isEmpty()) break;

			List<Map<String, Object>> ordered = topoSort(loopNodes, loopEdges);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			List<Map<String, Object>> iterNodes = new ArrayList<>();
			for (Map<String, Object> loopNode : ordered) {
				if (cancelFlag != null && cancelFlag.get()) {
					throw new IllegalStateException("Run cancelled by user");
				}
				String loopOutputVar = (String) loopNode.get("outputVar");
				String loopNodeType = (String) loopNode.get("type");
				Map<String, Object> loopResult = executeSingleNode(
						runId, loopNode, scope, configMap, 0, ancestorProjectIds);
				String status = (String) loopResult.get(WorkflowConstants.STATUS);
				if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					throw new IllegalStateException("While-loop body node failed at iteration " +
							iteration + ": " + loopResult.get(WorkflowConstants.ERROR_MESSAGE));
				}
				String outputValue = (String) loopResult.get("outputValue");
				if (outputValue != null) {
					lastOutput = outputValue;
					if (loopOutputVar != null && !loopOutputVar.isEmpty()
							&& !WorkflowConstants.NODE_SET_VARIABLE.equals(loopNodeType)) {
						scope.put(loopOutputVar, outputValue);
					}
				}
				// Capture sub-node summary for history display
				Map<String, Object> nodeSummary = new java.util.LinkedHashMap<>();
				nodeSummary.put("label", loopResult.get(WorkflowConstants.NODE_LABEL));
				nodeSummary.put("status", status);
				Object dur = loopResult.get(WorkflowConstants.DURATION_MS);
				if (dur != null) nodeSummary.put("durationMs", dur);
				Object preview = loopResult.get(WorkflowConstants.OUTPUT_PREVIEW);
				if (preview != null) nodeSummary.put("preview", preview);
				iterNodes.add(nodeSummary);
			}
			Map<String, Object> iterSummary = new java.util.LinkedHashMap<>();
			iterSummary.put("iteration", iteration);
			iterSummary.put("nodes", iterNodes);
			iterationSummaries.add(iterSummary);
		}

		if (iterationSummaries.isEmpty()) {
			return lastOutput;
		}
		// Wrap result so GetWorkflowRunReactor can surface per-iteration data in history
		Map<String, Object> wrapper = new java.util.LinkedHashMap<>();
		wrapper.put("__whileResult", true);
		wrapper.put("iterationCount", iterationSummaries.size());
		wrapper.put("lastOutput", lastOutput);
		wrapper.put("iterations", iterationSummaries);
		return new Gson().toJson(wrapper);
	}

	// ── Try-Catch Execution ───────────────────────────────────────────────────────

	/**
	 * Executes a "try-catch" node: runs the try-branch; on any failure injects the
	 * error message into scope and runs the catch-branch instead.
	 *
	 * <p>Config: {@code {errorVar, tryGraph: {nodes, edges}, catchGraph: {nodes, edges}}}.
	 * The variable named by {@code errorVar} is available in the catch branch as
	 * {@code ${errorVar}}.
	 */
	@SuppressWarnings("unchecked")
	private Object executeTryCatchNode(String runId, Map<String, Object> node,
			Map<String, String> scope, Map<String, String> configMap,
			Set<String> ancestorProjectIds) {

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		String errorVar = config.get("errorVar") instanceof String
				? (String) config.get("errorVar") : "error";
		if (errorVar.isBlank()) errorVar = "error";

		Map<String, Object> tryGraph = config.get("tryGraph") instanceof Map
				? (Map<String, Object>) config.get("tryGraph") : null;
		List<Map<String, Object>> tryNodes = tryGraph != null
				? (List<Map<String, Object>>) tryGraph.get("nodes") : null;
		List<Map<String, Object>> tryEdges = tryGraph != null
				? (List<Map<String, Object>>) tryGraph.get("edges") : null;

		Map<String, Object> catchGraph = config.get("catchGraph") instanceof Map
				? (Map<String, Object>) config.get("catchGraph") : null;
		List<Map<String, Object>> catchNodes = catchGraph != null
				? (List<Map<String, Object>>) catchGraph.get("nodes") : null;
		List<Map<String, Object>> catchEdges = catchGraph != null
				? (List<Map<String, Object>>) catchGraph.get("edges") : null;

		// Try branch
		if (tryNodes != null && !tryNodes.isEmpty()) {
			try {
				List<Map<String, Object>> ordered = topoSort(tryNodes, tryEdges);
				WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

				String lastOutput = "success";
				AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
				for (Map<String, Object> tryNode : ordered) {
					if (cancelFlag != null && cancelFlag.get()) {
						throw new IllegalStateException("Run cancelled by user");
					}
					String tryOutputVar = (String) tryNode.get("outputVar");
					Map<String, Object> tryResult = executeSingleNode(
							runId, tryNode, scope, configMap, 0, ancestorProjectIds);
					String status = (String) tryResult.get(WorkflowConstants.STATUS);
					if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
						throw new IllegalStateException((String) tryResult.get(WorkflowConstants.ERROR_MESSAGE));
					}
					String outputValue = (String) tryResult.get("outputValue");
					if (outputValue != null) {
						lastOutput = outputValue;
						if (tryOutputVar != null && !tryOutputVar.isEmpty()) {
							scope.put(tryOutputVar, outputValue);
						}
					}
				}
				return lastOutput;
			} catch (IllegalStateException e) {
				// Cancellation must always propagate — don't swallow it
				if (e.getMessage() != null && e.getMessage().contains("cancelled")) throw e;
				scope.put(errorVar, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
			}
		}

		// Catch branch
		if (catchNodes != null && !catchNodes.isEmpty()) {
			List<Map<String, Object>> ordered = topoSort(catchNodes, catchEdges);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			String lastOutput = "caught";
			AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
			for (Map<String, Object> catchNode : ordered) {
				if (cancelFlag != null && cancelFlag.get()) {
					throw new IllegalStateException("Run cancelled by user");
				}
				String catchOutputVar = (String) catchNode.get("outputVar");
				Map<String, Object> catchResult = executeSingleNode(
						runId, catchNode, scope, configMap, 0, ancestorProjectIds);
				String status = (String) catchResult.get(WorkflowConstants.STATUS);
				if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					throw new IllegalStateException("Catch branch node failed: " +
							catchResult.get(WorkflowConstants.ERROR_MESSAGE));
				}
				String outputValue = (String) catchResult.get("outputValue");
				if (outputValue != null) {
					lastOutput = outputValue;
					if (catchOutputVar != null && !catchOutputVar.isEmpty()) {
						scope.put(catchOutputVar, outputValue);
					}
				}
			}
			return lastOutput;
		}

		return "caught";
	}

	// ── Wait Execution ────────────────────────────────────────────────────────────

	/**
	 * Executes a "wait" node: sleeps for the configured number of seconds.
	 * The {@code seconds} value supports {@code ${var}} template substitution.
	 * Maximum 3600 seconds (1 hour) per invocation.
	 */
	@SuppressWarnings("unchecked")
	private Object executeWaitNode(Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap) {

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		String nodeLabel = node.get("label") != null ? node.get("label").toString() : "unnamed";

		String secondsTemplate = config.get("seconds") != null
				? config.get("seconds").toString() : "1";
		String resolved = WorkflowExecutionUtils.resolve(secondsTemplate, scope, configMap);

		int seconds;
		try {
			seconds = Integer.parseInt(resolved.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Wait node \"" + nodeLabel +
					"\" — seconds value is not a valid integer after resolution: \"" + resolved + "\"");
		}
		seconds = Math.min(Math.max(seconds, 0), 3600);

		try {
			TimeUnit.SECONDS.sleep(seconds);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Wait node \"" + nodeLabel + "\" was interrupted");
		}

		return seconds + " seconds";
	}

	// ── Set-Variable Execution ────────────────────────────────────────────────────

	/**
	 * Executes a "set-variable" node: resolves each configured variable's value
	 * template against the current scope and writes the result back into scope.
	 *
	 * <p>Config: {@code {variables: {varName: "value template"}}}.
	 * Returns a JSON object of the resolved values.
	 */
	/** Matches a resolved value that is a pure numeric arithmetic expression safe to eval. */
	private static final java.util.regex.Pattern NUMERIC_EXPR_PATTERN =
			java.util.regex.Pattern.compile("^[\\d\\s+\\-*/%.()]+$");

	@SuppressWarnings("unchecked")
	private Object executeSetVariableNode(Map<String, Object> node,
			Map<String, String> scope, Map<String, String> configMap) {

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		Object variablesRaw = config.get("variables");

		if (!(variablesRaw instanceof Map)) {
			return "{}";
		}

		Map<String, Object> variables = (Map<String, Object>) variablesRaw;
		Map<String, String> resolved = new HashMap<>();

		for (Map.Entry<String, Object> entry : variables.entrySet()) {
			String varName = entry.getKey();
			if (varName == null || varName.isBlank()) continue;
			String template = entry.getValue() != null ? entry.getValue().toString() : "";
			String value = WorkflowExecutionUtils.resolve(template, scope, configMap);

			// If the resolved value is a pure arithmetic expression (e.g. "5 - 1"),
			// evaluate it so variable math like "${counter} - 1" works as expected.
			value = tryEvalNumeric(value);

			scope.put(varName, value);
			resolved.put(varName, value);
		}

		return new Gson().toJson(resolved);
	}

	/**
	 * If {@code value} consists only of digits, arithmetic operators, spaces, and
	 * parentheses, evaluates it as a JS expression and returns the numeric result
	 * (as an integer string when the result is whole). Returns {@code value}
	 * unchanged if it does not match the pattern or evaluation fails.
	 */
	private String tryEvalNumeric(String value) {
		if (value == null || !NUMERIC_EXPR_PATTERN.matcher(value.trim()).matches()) return value;
		javax.script.ScriptEngine engine = JS_ENGINE_CACHE.get();
		if (engine == null) return value;
		try {
			Object result = engine.eval(value);
			if (!(result instanceof Number)) return value;
			double d = ((Number) result).doubleValue();
			if (Double.isNaN(d) || Double.isInfinite(d)) return value;
			// Return as integer string when there is no fractional part
			return d == Math.floor(d) ? String.valueOf((long) d) : String.valueOf(d);
		} catch (Exception ignored) {
			return value;
		}
	}

	// ── Transform Execution ───────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeTransformNode(Map<String, Object> node, Map<String, String> scope) {
		Map<String, Object> config = (Map<String, Object>) node.get("config");
		if (config == null) return "{}";

		String inputVar = strCfg(config.get("inputVar"));
		String raw = inputVar != null && !inputVar.isBlank() ? scope.getOrDefault(inputVar, "") : "";
		String operation = strCfg(config.get("operation"));
		String expression = strCfg(config.get("expression"));

		if (operation == null) return raw;

		switch (operation) {
			case "convert-to-objects":
				return WorkflowExecutionUtils.applyOutputTransform(raw,
						java.util.Collections.singletonMap("mode", "rows-as-objects"));
			case "extract-field": {
				// expression is a dot-notation path like "[0].name" or "data.items"
				String path = expression != null ? expression.replaceAll("^\\[\\d+\\]\\.", "$0") : "";
				return WorkflowExecutionUtils.applyOutputTransform(raw,
						java.util.Map.of("mode", "jsonpath", "path", path));
			}
			case "map": {
				// expression like "item.fieldName" — extract named field from each array element
				if (expression == null || !expression.startsWith("item.")) return raw;
				String field = expression.substring(5).trim();
				try {
					com.google.gson.JsonElement el = new com.google.gson.JsonParser().parse(raw);
					if (!el.isJsonArray()) return raw;
					java.util.List<Object> out = new java.util.ArrayList<>();
					for (com.google.gson.JsonElement item : el.getAsJsonArray()) {
						if (item.isJsonObject() && item.getAsJsonObject().has(field)) {
							com.google.gson.JsonElement val = item.getAsJsonObject().get(field);
							out.add(val.isJsonPrimitive() ? val.getAsString() : val.toString());
						} else {
							out.add(null);
						}
					}
					return new Gson().toJson(out);
				} catch (Exception e) { return raw; }
			}
			case "filter": {
				// expression like "item.field === \"value\"" — simple equality filter
				if (expression == null || !expression.startsWith("item.")) return raw;
				java.util.regex.Matcher m = java.util.regex.Pattern
						.compile("item\\.([\\w]+)\\s*===?\\s*[\"']?([^\"']+)[\"']?")
						.matcher(expression);
				if (!m.find()) return raw;
				String field = m.group(1);
				String expected = m.group(2).trim();
				try {
					com.google.gson.JsonElement el = new com.google.gson.JsonParser().parse(raw);
					if (!el.isJsonArray()) return raw;
					java.util.List<Object> out = new java.util.ArrayList<>();
					for (com.google.gson.JsonElement item : el.getAsJsonArray()) {
						if (item.isJsonObject() && item.getAsJsonObject().has(field)) {
							String actual = item.getAsJsonObject().get(field).getAsString();
							if (expected.equals(actual)) out.add(new Gson().fromJson(item, Object.class));
						}
					}
					return new Gson().toJson(out);
				} catch (Exception e) { return raw; }
			}
			case "flatten": {
				try {
					com.google.gson.JsonElement el = new com.google.gson.JsonParser().parse(raw);
					if (!el.isJsonArray()) return raw;
					java.util.List<Object> out = new java.util.ArrayList<>();
					for (com.google.gson.JsonElement item : el.getAsJsonArray()) {
						if (item.isJsonArray()) {
							for (com.google.gson.JsonElement inner : item.getAsJsonArray()) {
								out.add(new Gson().fromJson(inner, Object.class));
							}
						} else {
							out.add(new Gson().fromJson(item, Object.class));
						}
					}
					return new Gson().toJson(out);
				} catch (Exception e) { return raw; }
			}
			default:
				return raw;
		}
	}

	// ── Email Execution ───────────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeEmailNode(Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap) {
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String to = WorkflowExecutionUtils.resolve(strCfg(config.get("to")), scope, configMap);
		String subject = WorkflowExecutionUtils.resolve(strCfg(config.get("subject")), scope, configMap);
		String body = WorkflowExecutionUtils.resolve(strCfg(config.get("body")), scope, configMap);

		if (to == null || to.isBlank()) throw new IllegalArgumentException("Email node: 'to' is required");
		if (subject == null || subject.isBlank()) throw new IllegalArgumentException("Email node: 'subject' is required");
		if (body == null) body = "";

		boolean isHtml = Boolean.parseBoolean(strCfg(config.getOrDefault("isHtml", "false")));
		String cc = config.get("cc") != null ? WorkflowExecutionUtils.resolve(strCfg(config.get("cc")), scope, configMap) : null;
		String bcc = config.get("bcc") != null ? WorkflowExecutionUtils.resolve(strCfg(config.get("bcc")), scope, configMap) : null;

		// URL-encode body so it can be safely embedded in a pixel string
		String encodedBody = java.net.URLEncoder.encode(body, StandardCharsets.UTF_8);

		StringBuilder pixel = new StringBuilder("SendEmail(");
		pixel.append("to=[").append(buildEmailAddressParam(to)).append("]");
		if (cc != null && !cc.isBlank()) pixel.append(", cc=[").append(buildEmailAddressParam(cc)).append("]");
		if (bcc != null && !bcc.isBlank()) pixel.append(", bcc=[").append(buildEmailAddressParam(bcc)).append("]");
		pixel.append(", subject=[\"").append(subject.replace("\"", "\\\"")).append("\"]");
		pixel.append(", message=[\"<encode>").append(encodedBody).append("</encode>\"]");
		if (isHtml) pixel.append(", html=[\"true\"]");
		pixel.append(");");

		try {
			this.insight.runPixel(pixel.toString());
		} catch (Exception e) {
			throw new IllegalStateException("Email send failed: " + e.getMessage(), e);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("sent", true);
		result.put("to", to);
		return new Gson().toJson(result);
	}

	private static String buildEmailAddressParam(String addresses) {
		String[] parts = addresses.split("\\s*,\\s*");
		StringBuilder sb = new StringBuilder();
		for (String addr : parts) {
			if (sb.length() > 0) sb.append(", ");
			sb.append("\"").append(addr.trim()).append("\"");
		}
		return sb.toString();
	}

	// ── HTTP Request Execution ────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeHttpRequestNode(Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap) {
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String method = strCfg(config.getOrDefault("method", "GET")).toUpperCase();
		String url = WorkflowExecutionUtils.resolve(strCfg(config.get("url")), scope, configMap);
		if (url == null || url.isBlank()) throw new IllegalArgumentException("HTTP Request node: 'url' is required");

		// Parse headers JSON
		Map<String, String> headers = new LinkedHashMap<>();
		String headersJson = strCfg(config.get("headers"));
		if (headersJson != null && !headersJson.isBlank()) {
			try {
				Map<String, Object> parsed = new Gson().fromJson(headersJson, new com.google.gson.reflect.TypeToken<Map<String, Object>>(){}.getType());
				if (parsed != null) parsed.forEach((k, v) -> { if (v != null) headers.put(k, v.toString()); });
			} catch (Exception e) {
				classLogger.warn("HTTP node: could not parse headers JSON: {}", e.getMessage());
			}
		}

		// Basic auth
		String username = strCfg(config.get("username"));
		String password = strCfg(config.get("password"));
		if (username != null && !username.isBlank() && password != null) {
			String creds = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
			headers.put("Authorization", "Basic " + creds);
		}

		String body = strCfg(config.get("body"));
		if (body != null) body = WorkflowExecutionUtils.resolve(body, scope, configMap);
		final String resolvedBody = body != null ? body : "";

		String response;
		try {
			switch (method) {
				case "GET":
					response = HttpHelperUtility.getRequest(url, headers, null, null, null);
					break;
				case "POST":
					response = HttpHelperUtility.postRequestStringBody(url, headers, resolvedBody, ContentType.APPLICATION_JSON, null, null, null);
					break;
				case "PUT":
					response = HttpHelperUtility.putRequestStringBody(url, headers, resolvedBody, ContentType.APPLICATION_JSON, null, null, null);
					break;
				case "PATCH":
					response = HttpHelperUtility.patchRequestStringBody(url, headers, resolvedBody, ContentType.APPLICATION_JSON, null, null, null);
					break;
				case "DELETE":
					response = HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
					break;
				default:
					throw new IllegalArgumentException("Unsupported HTTP method: " + method);
			}
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("HTTP request failed [" + method + " " + url + "]: " + e.getMessage(), e);
		}

		if (response == null) return "{\"response\": null}";
		// Return as-is if valid JSON, otherwise wrap it
		try {
			new Gson().fromJson(response, Object.class);
			return response;
		} catch (Exception e) {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("response", response);
			return new Gson().toJson(r);
		}
	}

	// ── Notification Execution ────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeNotificationNode(Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap) {
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String recipientId = WorkflowExecutionUtils.resolve(strCfg(config.get("recipientId")), scope, configMap);
		String title = WorkflowExecutionUtils.resolve(strCfg(config.get("title")), scope, configMap);
		String message = config.get("message") != null
				? WorkflowExecutionUtils.resolve(strCfg(config.get("message")), scope, configMap) : "";
		String priority = strCfg(config.getOrDefault("priority", "MEDIUM"));

		if (recipientId == null || recipientId.isBlank()) throw new IllegalArgumentException("Notification node: 'recipientId' is required");
		if (title == null || title.isBlank()) throw new IllegalArgumentException("Notification node: 'title' is required");

		IRDBMSEngine notifDb = SystemEngineRegistry.getNotificationDb();
		if (notifDb == null) throw new IllegalStateException("Notification database is not configured in this SEMOSS instance");

		String sql = "INSERT INTO NOTIFICATION (NOTIFICATIONID,RECIPIENTID,RECIPIENTTYPE,NOTIFICATIONTITLE,MESSAGE,ACTIONTYPE,ACTIONTARGET,ISREAD,PRIORITY,NOTIFICATIONTYPE,CATALOGID,CREATEDBY,CREATEDDATE,READDATE,NOTIFICATIONSOURCE,USERID,USERTYPE,USEREXISTINGROLE,USERNEWROLE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		String notifId = GUID.v7().toUUID().toString();
		try {
			ps = notifDb.getPreparedStatement(sql);
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			ps.setString(1, notifId);
			ps.setString(2, recipientId);
			ps.setString(3, "NATIVE");
			ps.setString(4, title);
			ps.setString(5, message);
			ps.setString(6, "NEW");
			ps.setString(7, "IN-APP");
			ps.setBoolean(8, false);
			ps.setString(9, priority);
			ps.setString(10, "WORKFLOW");
			ps.setString(11, null);
			ps.setString(12, "WORKFLOW");
			ps.setTimestamp(13, now);
			ps.setTimestamp(14, null);
			ps.setString(15, "WORKFLOW");
			ps.setString(16, recipientId);
			ps.setString(17, "NATIVE");
			ps.setString(18, null);
			ps.setString(19, null);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) ps.getConnection().commit();
		} catch (SQLException e) {
			throw new IllegalStateException("Failed to create notification: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notifDb, ps);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("sent", true);
		result.put("notificationId", notifId);
		result.put("recipientId", recipientId);
		return new Gson().toJson(result);
	}

	// ── Switch Execution ──────────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeSwitchNode(String runId, Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap, Set<String> ancestorProjectIds) {
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		String switchVar = strCfg(config.get("switchVar"));
		if (switchVar == null || switchVar.isBlank()) throw new IllegalArgumentException("Switch node requires a switchVar");

		String switchValue = WorkflowExecutionUtils.resolve("${" + switchVar + "}", scope, configMap);

		List<Map<String, Object>> cases = config.get("cases") instanceof List
				? (List<Map<String, Object>>) config.get("cases") : new ArrayList<>();

		Map<String, Object> matchedGraph = null;
		String matchedLabel = "default";

		for (Map<String, Object> c : cases) {
			String caseValue = strCfg(c.get("value"));
			if (switchValue != null && switchValue.equals(caseValue)) {
				matchedGraph = c.get("subGraph") instanceof Map ? (Map<String, Object>) c.get("subGraph") : null;
				matchedLabel = strCfg(c.getOrDefault("label", caseValue));
				break;
			}
		}
		if (matchedGraph == null) {
			matchedGraph = config.get("defaultSubGraph") instanceof Map
					? (Map<String, Object>) config.get("defaultSubGraph") : null;
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("matched", matchedLabel);
		result.put("switchValue", switchValue);

		if (matchedGraph == null) {
			result.put("executed", false);
			return new Gson().toJson(result);
		}

		List<Map<String, Object>> subNodes = matchedGraph.get("nodes") instanceof List
				? (List<Map<String, Object>>) matchedGraph.get("nodes") : null;
		List<Map<String, Object>> subEdges = matchedGraph.get("edges") instanceof List
				? (List<Map<String, Object>>) matchedGraph.get("edges") : null;

		String lastOutput = "{}";
		if (subNodes != null && !subNodes.isEmpty()) {
			AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
			List<Map<String, Object>> ordered = topoSort(subNodes, subEdges);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);
			for (Map<String, Object> subNode : ordered) {
				if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");
				String subOutputVar = (String) subNode.get("outputVar");
				Map<String, Object> subResult = executeSingleNode(runId, subNode, scope, configMap, 0, ancestorProjectIds);
				String status = (String) subResult.get(WorkflowConstants.STATUS);
				if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					throw new IllegalStateException("Switch branch failed: " + subResult.get(WorkflowConstants.ERROR_MESSAGE));
				}
				String out = (String) subResult.get("outputValue");
				if (out != null) {
					lastOutput = out;
					if (subOutputVar != null && !subOutputVar.isBlank()) scope.put(subOutputVar, out);
				}
			}
		}

		result.put("executed", true);
		result.put("output", lastOutput);
		return new Gson().toJson(result);
	}

	// ── Retry Execution ───────────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeRetryNode(String runId, Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap, Set<String> ancestorProjectIds) throws InterruptedException {
		Map<String, Object> config = (Map<String, Object>) node.get("config");

		int maxAttempts = 3;
		Object maxRaw = config.get("maxAttempts");
		if (maxRaw != null) try { maxAttempts = Integer.parseInt(maxRaw.toString()); } catch (NumberFormatException ignored) {}

		int backoffSeconds = 5;
		Object backoffRaw = config.get("backoffSeconds");
		if (backoffRaw != null) try { backoffSeconds = Integer.parseInt(backoffRaw.toString()); } catch (NumberFormatException ignored) {}

		boolean exponential = Boolean.parseBoolean(strCfg(config.getOrDefault("exponential", "false")));

		Map<String, Object> subGraph = config.get("subGraph") instanceof Map
				? (Map<String, Object>) config.get("subGraph") : null;
		List<Map<String, Object>> subNodes = subGraph != null
				? (List<Map<String, Object>>) subGraph.get("nodes") : null;
		List<Map<String, Object>> subEdges = subGraph != null
				? (List<Map<String, Object>>) subGraph.get("edges") : null;

		if (subNodes == null || subNodes.isEmpty()) {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("attempts", 0);
			r.put("succeeded", false);
			return new Gson().toJson(r);
		}

		AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
		Exception lastError = null;

		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");

			Map<String, String> attemptScope = new HashMap<>(scope);
			try {
				List<Map<String, Object>> ordered = topoSort(subNodes, subEdges);
				String lastOutput = "{}";
				for (Map<String, Object> subNode : ordered) {
					if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");
					String subOutputVar = (String) subNode.get("outputVar");
					Map<String, Object> subResult = executeSingleNode(runId, subNode, attemptScope, configMap, 0, ancestorProjectIds);
					String status = (String) subResult.get(WorkflowConstants.STATUS);
					if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
						throw new IllegalStateException((String) subResult.get(WorkflowConstants.ERROR_MESSAGE));
					}
					String out = (String) subResult.get("outputValue");
					if (out != null) {
						lastOutput = out;
						if (subOutputVar != null && !subOutputVar.isBlank()) attemptScope.put(subOutputVar, out);
					}
				}
				// Success — promote attempt scope to parent
				scope.putAll(attemptScope);
				Map<String, Object> r = new LinkedHashMap<>();
				r.put("attempts", attempt);
				r.put("succeeded", true);
				return new Gson().toJson(r);

			} catch (IllegalStateException e) {
				if (e.getMessage() != null && e.getMessage().contains("cancelled")) throw e;
				lastError = e;
				classLogger.warn("Retry attempt {}/{} failed: {}", attempt, maxAttempts, e.getMessage());
				if (attempt < maxAttempts) {
					int delay = exponential ? backoffSeconds * attempt : backoffSeconds;
					TimeUnit.SECONDS.sleep(Math.min(delay, 300));
				}
			}
		}
		throw new IllegalStateException("All " + maxAttempts + " retry attempts failed. Last: " +
				(lastError != null ? lastError.getMessage() : "unknown"), lastError);
	}

	// ── Parallel Execution ────────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Object executeParallelNode(String runId, Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap, Set<String> ancestorProjectIds) {
		Map<String, Object> config = (Map<String, Object>) node.get("config");
		List<Map<String, Object>> branches = config.get("branches") instanceof List
				? (List<Map<String, Object>>) config.get("branches") : new ArrayList<>();

		List<Object> branchResults = new ArrayList<>();
		AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);

		for (int i = 0; i < branches.size(); i++) {
			if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");

			Map<String, Object> branch = branches.get(i);
			String branchOutputVar = strCfg(branch.get("outputVar"));
			String branchLabel = strCfg(branch.getOrDefault("label", "branch_" + i));

			Map<String, Object> branchGraph = branch.get("subGraph") instanceof Map
					? (Map<String, Object>) branch.get("subGraph") : null;
			List<Map<String, Object>> branchNodes = branchGraph != null
					? (List<Map<String, Object>>) branchGraph.get("nodes") : null;
			List<Map<String, Object>> branchEdges = branchGraph != null
					? (List<Map<String, Object>>) branchGraph.get("edges") : null;

			if (branchNodes == null || branchNodes.isEmpty()) {
				branchResults.add(null);
				continue;
			}

			Map<String, String> branchScope = new HashMap<>(scope);
			try {
				List<Map<String, Object>> ordered = topoSort(branchNodes, branchEdges);
				WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);
				String lastOutput = "{}";
				for (Map<String, Object> branchNode : ordered) {
					if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");
					String subOutputVar = (String) branchNode.get("outputVar");
					Map<String, Object> subResult = executeSingleNode(runId, branchNode, branchScope, configMap, 0, ancestorProjectIds);
					String status = (String) subResult.get(WorkflowConstants.STATUS);
					if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
						throw new IllegalStateException("Parallel branch \"" + branchLabel + "\" failed: " + subResult.get(WorkflowConstants.ERROR_MESSAGE));
					}
					String out = (String) subResult.get("outputValue");
					if (out != null) {
						lastOutput = out;
						if (subOutputVar != null && !subOutputVar.isBlank()) branchScope.put(subOutputVar, out);
					}
				}
				branchResults.add(lastOutput);
				if (branchOutputVar != null && !branchOutputVar.isBlank()) scope.put(branchOutputVar, lastOutput);
			} catch (IllegalStateException e) {
				if (e.getMessage() != null && e.getMessage().contains("cancelled")) throw e;
				classLogger.error("Parallel branch \"{}\" failed (continuing): {}", branchLabel, e.getMessage());
				branchResults.add(null);
			}
		}
		return new Gson().toJson(branchResults);
	}

	private static String strCfg(Object v) {
		return (v != null && !v.toString().isBlank()) ? v.toString() : null;
	}

	// ── Node Pixel Execution ──────────────────────────────────────────────────────

	private Object executeNodePixel(Map<String, Object> node, Map<String, String> scope,
			Map<String, String> configMap) {

		String type = (String) node.get("type");

		if (WorkflowConstants.NODE_TRIGGER.equals(type)) {
			return scope.get("triggered_at");
		}

		if (WorkflowConstants.NODE_FOR_EACH.equals(type)) {
			// Delegate to ForEachNodeExecutor — handled separately in executeSingleNode
			throw new IllegalStateException("For-each nodes should not reach executeNodePixel directly");
		}

		String builtPixel = (String) node.get("builtPixel");
		if (builtPixel == null || builtPixel.isBlank() || builtPixel.startsWith("//")) {
			throw new IllegalStateException("Node \"" + node.get("label") +
					"\" has no compiled pixel — please Save the workflow before running");
		}

		int timeoutSeconds = WorkflowExecutionUtils.getNodeTimeout(node);
		String resolvedPixel = WorkflowExecutionUtils.resolve(builtPixel, scope, configMap);

		// For custom-pixel nodes with an appId, the builtPixel is "LoadApp(...); actualPixel".
		// Run LoadApp as a fire-and-forget setup step so only the actual pixel's output
		// is captured and stored as the node's result.
		if (WorkflowConstants.NODE_CUSTOM_PIXEL.equals(type)) {
			@SuppressWarnings("unchecked")
			Map<String, Object> config = (Map<String, Object>) node.get("config");
			Object appIdObj = config != null ? config.get("appId") : null;
			if (appIdObj != null && !appIdObj.toString().isBlank()) {
				int semicolon = resolvedPixel.indexOf(';');
				if (semicolon > 0) {
					String setupPixel = resolvedPixel.substring(0, semicolon).trim();
					String actualPixel = resolvedPixel.substring(semicolon + 1).trim();
					if (!setupPixel.isBlank() && !actualPixel.isBlank()) {
						// SEMOSS pixel parser requires a trailing semicolon on every statement
						if (!setupPixel.endsWith(";")) setupPixel += ";";
						if (!actualPixel.endsWith(";")) actualPixel += ";";
						this.insight.runPixel(setupPixel); // set context, discard output
						return PixelExecutionUtils.runAndCollect(this.insight, actualPixel, timeoutSeconds);
					}
				}
			}
		}

		return PixelExecutionUtils.runAndCollect(this.insight, resolvedPixel, timeoutSeconds);
	}

	// ── Resume Logic ──────────────────────────────────────────────────────────────

	private Map<String, String> loadPriorOutputs(String resumeRunId) {
		Map<String, String> outputs = new HashMap<>();
		if (resumeRunId == null || resumeRunId.isEmpty()) {
			return outputs;
		}

		List<Map<String, Object>> nodeOutputs = WorkflowDatabaseUtility.getNodeOutputsForRun(resumeRunId);
		for (Map<String, Object> nodeOutput : nodeOutputs) {
			String status = (String) nodeOutput.get(WorkflowConstants.STATUS);
			if (WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
				String nodeId = (String) nodeOutput.get(WorkflowConstants.NODE_ID);
				String outputValue = nodeOutput.get(WorkflowConstants.OUTPUT_VALUE) != null
						? nodeOutput.get(WorkflowConstants.OUTPUT_VALUE).toString() : "";
				outputs.put(nodeId, outputValue);
			}
		}
		return outputs;
	}

	private boolean shouldSkipForResume(String nodeId, String outputVar,
			Map<String, String> priorOutputs, Map<String, String> scope) {
		if (priorOutputs.isEmpty() || !priorOutputs.containsKey(nodeId)) {
			return false;
		}
		// Copy prior output to scope so downstream nodes can reference it
		String priorValue = priorOutputs.get(nodeId);
		if (outputVar != null && !outputVar.isEmpty()) {
			scope.put(outputVar, priorValue);
		}
		return true;
	}

	// ── Heartbeat ─────────────────────────────────────────────────────────────────

	private ScheduledExecutorService startHeartbeat(String runId) {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "workflow-heartbeat-" + runId.substring(0, 8));
			t.setDaemon(true);
			return t;
		});
		// Heartbeat fires every 30 seconds — just proves liveness; per-node count updates
		// happen in executeNodes() after each node completes.
		scheduler.scheduleAtFixedRate(() -> {
			try {
				WorkflowDatabaseUtility.touchHeartbeat(runId);
			} catch (Exception e) {
				classLogger.warn("Heartbeat update failed for run {}: {}", runId, e.getMessage());
			}
		}, WorkflowConstants.HEARTBEAT_INTERVAL_SECONDS,
				WorkflowConstants.HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
		return scheduler;
	}

	// ── Cancellation Support ──────────────────────────────────────────────────────

	/**
	 * Requests cancellation of a running workflow. Called by CancelWorkflowRunReactor.
	 * Cancellation takes effect between nodes (cannot interrupt mid-pixel).
	 */
	public static boolean requestCancellation(String runId) {
		AtomicBoolean flag = CANCELLATION_FLAGS.get(runId);
		if (flag != null) {
			flag.set(true);
			return true;
		}
		return false;
	}

	// (resolve, getNodeTimeout, applyOutputTransform moved to WorkflowExecutionUtils)

	// ── Topological Sort ──────────────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> topoSort(List<Map<String, Object>> nodes,
			List<Map<String, Object>> edges) {
		if (nodes == null || nodes.isEmpty()) return new ArrayList<>();

		Map<String, Integer> inDegree = new HashMap<>();
		Map<String, List<String>> adj = new HashMap<>();

		for (Map<String, Object> n : nodes) {
			String id = (String) n.get("id");
			inDegree.put(id, 0);
			adj.put(id, new ArrayList<>());
		}
		if (edges != null) {
			for (Map<String, Object> e : edges) {
				String src = (String) e.get("source");
				String tgt = (String) e.get("target");
				adj.computeIfAbsent(src, k -> new ArrayList<>()).add(tgt);
				inDegree.merge(tgt, 1, Integer::sum);
			}
		}

		// Seed in nodes-array order so a linear workflow with no edges runs top-to-bottom
		Queue<String> queue = new LinkedList<>();
		for (Map<String, Object> n : nodes) {
			String id = (String) n.get("id");
			if (inDegree.getOrDefault(id, 0) == 0) queue.add(id);
		}

		Map<String, Map<String, Object>> nodeById = new HashMap<>();
		for (Map<String, Object> n : nodes) nodeById.put((String) n.get("id"), n);

		List<Map<String, Object>> sorted = new ArrayList<>();
		while (!queue.isEmpty()) {
			String id = queue.poll();
			sorted.add(nodeById.get(id));
			for (String neighbor : adj.getOrDefault(id, new ArrayList<>())) {
				int deg = inDegree.merge(neighbor, -1, Integer::sum);
				if (deg == 0) queue.add(neighbor);
			}
		}
		return sorted;
	}

	// ── Helpers ───────────────────────────────────────────────────────────────────

	private String getProjectId() {
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
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

	private String determineTriggerType(String resumeRunId) {
		if (resumeRunId != null && !resumeRunId.isEmpty()) {
			return WorkflowConstants.TRIGGER_RESUME;
		}
		// Explicit triggerType param (webhook, storage-poll, db-poll) takes precedence
		String explicit = this.keyValue.get(this.keysToGet[3]);
		if (explicit != null && !explicit.isBlank()) {
			return explicit.toUpperCase().replace("-", "_");
		}
		String manual = this.keyValue.get(this.keysToGet[1]);
		if ("true".equalsIgnoreCase(manual)) {
			return WorkflowConstants.TRIGGER_MANUAL;
		}
		return WorkflowConstants.TRIGGER_SCHEDULED;
	}

	private Map<String, String> buildInitialScope(String runId) {
		Map<String, String> scope = new HashMap<>();
		String now = Instant.now().toString();
		scope.put("date", now.substring(0, 10));
		scope.put("triggered_at", now);
		scope.put("run_id", runId);
		return scope;
	}


	/**
	 * Config values that are conceptually maps (e.g. sub-workflow {@code inputMapping}) may
	 * arrive either as an already-parsed JSON object (workflow.json authored programmatically)
	 * or as a raw JSON string (FE forms that store map-shaped config in a textarea, matching
	 * the convention used by fields like {@code paramValues}/{@code metaFilters}). Normalize
	 * either shape to a Map, or an empty map if absent/unparseable.
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> coerceToMap(Object raw) {
		if (raw instanceof Map) {
			return (Map<String, Object>) raw;
		}
		if (raw instanceof String str && !str.isBlank()) {
			Map<String, Object> parsed = parseJsonToMap(str);
			if (parsed != null) return parsed;
		}
		return new HashMap<>();
	}

	private Map<String, Object> parseJsonToMap(String json) {
		try {
			return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
		} catch (Exception e) {
			classLogger.warn("Could not parse config value as JSON map: {}", e.getMessage());
			return null;
		}
	}

	private Timestamp toTimestamp(Instant instant) {
		return Utility.getSqlTimestampUTC(
				LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
	}

	// ── Workflow Document Loading ─────────────────────────────────────────────────

	@SuppressWarnings("unchecked")
	private Map<String, Object> loadWorkflowDoc(String projectId) {
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File f = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_FILE_NAME);
		if (!f.exists()) {
			throw new IllegalArgumentException("No workflow.json found for this project. Save a workflow first.");
		}
		try {
			String json = java.nio.file.Files.readString(f.toPath(), java.nio.charset.StandardCharsets.UTF_8);
			return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
		} catch (IOException e) {
			throw new IllegalStateException("Failed to read workflow.json: " + e.getMessage(), e);
		}
	}

	// ── Result Building ───────────────────────────────────────────────────────────

	private Map<String, Object> buildNodeResult(String nodeId, String nodeLabel,
			String status, long durationMs, String outputPreview, String errorMessage) {
		Map<String, Object> result = new HashMap<>();
		result.put(WorkflowConstants.NODE_ID, nodeId);
		result.put(WorkflowConstants.NODE_LABEL, nodeLabel);
		result.put(WorkflowConstants.STATUS, status);
		result.put(WorkflowConstants.DURATION_MS, durationMs);
		if (outputPreview != null) {
			result.put(WorkflowConstants.OUTPUT_PREVIEW, outputPreview);
		}
		if (errorMessage != null) {
			result.put(WorkflowConstants.ERROR_MESSAGE, errorMessage);
		}
		return result;
	}

	private Map<String, Object> buildRunResult(String runId, String projectId, String status,
			int totalNodes, int completedNodes, String failedNodeId,
			List<Map<String, Object>> nodeResults) {
		// Read actual timestamps from DB rather than synthesizing "now" on every call.
		Map<String, Object> stored = WorkflowDatabaseUtility.getRunDetail(runId);
		Map<String, Object> result = new HashMap<>();
		result.put(WorkflowConstants.RUN_ID, runId);
		result.put(WorkflowConstants.PROJECT_ID, projectId);
		result.put(WorkflowConstants.STATUS, status);
		result.put(WorkflowConstants.TOTAL_NODES, totalNodes);
		result.put(WorkflowConstants.COMPLETED_NODES, completedNodes);
		if (stored != null) {
			result.put(WorkflowConstants.STARTED_AT, stored.get(WorkflowConstants.STARTED_AT));
			result.put(WorkflowConstants.COMPLETED_AT, stored.get(WorkflowConstants.COMPLETED_AT));
		}
		if (failedNodeId != null) {
			result.put(WorkflowConstants.FAILED_NODE_ID, failedNodeId);
		}
		result.put("nodeResults", nodeResults);
		return result;
	}
}
