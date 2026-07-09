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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
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
		this.keysToGet = new String[]{ "project", "manual", "resumeRunId" };
		this.keyRequired = new int[]{ 1, 0, 0 };
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
					// Store output in scope for downstream nodes
					if (outputVar != null && !outputVar.isEmpty()) {
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
			} else {
				rawOutput = executeNodePixel(node, scope, configMap);
			}

			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get("outputTransform");
			String transformed = WorkflowExecutionUtils.applyOutputTransform(rawOutput, transformConfig);

			long durationMs = System.currentTimeMillis() - startMs;
			String preview = PixelExecutionUtils.generatePreview(transformed);

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
