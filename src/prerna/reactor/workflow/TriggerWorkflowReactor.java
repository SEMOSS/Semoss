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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.workflow.foreach.ForEachNodeExecutor;
import prerna.reactor.workflow.nodes.ChildWorkflowRunner;
import prerna.reactor.workflow.nodes.ConditionalNodeExecutor;
import prerna.reactor.workflow.nodes.DatabaseEngineNodeExecutor;
import prerna.reactor.workflow.nodes.EmailNodeExecutor;
import prerna.reactor.workflow.nodes.FunctionEngineNodeExecutor;
import prerna.reactor.workflow.nodes.HttpRequestNodeExecutor;
import prerna.reactor.workflow.nodes.IWorkflowNodeExecutor;
import prerna.reactor.workflow.nodes.ModelEngineNodeExecutor;
import prerna.reactor.workflow.nodes.NodeDispatcher;
import prerna.reactor.workflow.nodes.NotificationNodeExecutor;
import prerna.reactor.workflow.nodes.ParallelNodeExecutor;
import prerna.reactor.workflow.nodes.PixelNodeExecutor;
import prerna.reactor.workflow.nodes.RetryNodeExecutor;
import prerna.reactor.workflow.nodes.SetVariableNodeExecutor;
import prerna.reactor.workflow.nodes.StorageEngineNodeExecutor;
import prerna.reactor.workflow.nodes.SubWorkflowNodeExecutor;
import prerna.reactor.workflow.nodes.SwitchNodeExecutor;
import prerna.reactor.workflow.nodes.TransformNodeExecutor;
import prerna.reactor.workflow.nodes.TryCatchNodeExecutor;
import prerna.reactor.workflow.nodes.VectorEngineNodeExecutor;
import prerna.reactor.workflow.nodes.WaitNodeExecutor;
import prerna.reactor.workflow.nodes.WhileLoopNodeExecutor;
import prerna.reactor.workflow.nodes.WorkflowNodeContext;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.om.ThreadStore;
import java.util.Collections;

/**
 * Executes a workflow app's graph top-to-bottom with DB-backed state.
 *
 * <p>Pixel: {@code TriggerWorkflow(project=["appId"], manual=["true"])}
 * <p>Pixel: {@code TriggerWorkflow(project=["appId"], resumeRunId=["uuid"])}
 *
 * <p>Execution model:
 * <ul>
 *   <li>Concurrency guard - rejects if a run is already active for this project</li>
 *   <li>DB checkpoint per node - each completed node is committed immediately</li>
 *   <li>Stop on error - first node failure halts the pipeline</li>
 *   <li>Heartbeat - updated every 30s to prove liveness</li>
 *   <li>Resume - skips nodes that succeeded in a prior run, re-runs from failure</li>
 * </ul>
 *
 * <p>State is written to WORKFLOW_RUNS and WORKFLOW_NODE_OUTPUTS in the scheduler DB
 * via {@link WorkflowDatabaseUtility}.
 */
public class TriggerWorkflowReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TriggerWorkflowReactor.class);

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

	/**
	 * Registry mapping a node's {@code type} to the executor that runs it - replaces the
	 * previous 13-way {@code if/else} chain in {@link #executeSingleNode}. Mirrors the existing
	 * SEMOSS pattern for "one operation, many type-specific implementations, resolved by a type
	 * key" (see {@code IModelEngine} -> {@code Utility.getModel(engineId)}, {@code IMCP}).
	 * Executors are stateless and shared across every run/node.
	 */
	private static final Map<String, IWorkflowNodeExecutor> EXECUTORS = Map.ofEntries(
			Map.entry(WorkflowConstants.NODE_FOR_EACH, new ForEachNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_SUB_WORKFLOW, new SubWorkflowNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_CONDITIONAL, new ConditionalNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_WHILE_LOOP, new WhileLoopNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_TRY_CATCH, new TryCatchNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_WAIT, new WaitNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_SET_VARIABLE, new SetVariableNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_EMAIL, new EmailNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_HTTP_REQUEST, new HttpRequestNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_NOTIFICATION, new NotificationNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_SWITCH, new SwitchNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_RETRY, new RetryNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_PARALLEL, new ParallelNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_TRANSFORM, new TransformNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_DATABASE_ENGINE, new DatabaseEngineNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_MODEL_ENGINE, new ModelEngineNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_VECTOR_ENGINE, new VectorEngineNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_STORAGE_ENGINE, new StorageEngineNodeExecutor()),
			Map.entry(WorkflowConstants.NODE_FUNCTION_ENGINE, new FunctionEngineNodeExecutor())
	);

	/**
	 * Default executor for node types with no dedicated entry above - {@code trigger},
	 * {@code app}, and {@code custom-pixel} - which are genuinely arbitrary/composed Pixel with
	 * no single backing engine. See {@link PixelNodeExecutor}.
	 */
	private static final IWorkflowNodeExecutor PIXEL_EXECUTOR = new PixelNodeExecutor();

	public TriggerWorkflowReactor() {
		this.keysToGet = new String[]{ "project", "manual", "resumeRunId", "triggerType" };
		this.keyRequired = new int[]{ 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();
		String resumeRunId = this.keyValue.get(this.keysToGet[2]);

		// Determine trigger type
		String triggerType = determineTriggerType(resumeRunId);
		String userId = getUserId();
		String runId = UUID.randomUUID().toString();

		// Concurrency guard - atomic claim against the shared scheduler DB, so this is correct
		// across every pod in a cluster, not just within this JVM. Prevents two concurrent
		// triggers for the same project from both starting a run (which would double up any
		// node with side effects, e.g. a database-update node running twice).
		if (!WorkflowDatabaseUtility.claimActiveRun(projectId, runId)) {
			String activeRun = WorkflowDatabaseUtility.getActiveRun(projectId);
			throw new IllegalArgumentException(
					"Workflow already has an active run: " + activeRun +
					". Wait for it to complete or cancel it before starting a new run.");
		}

		try {
			// Load workflow definition and config
			Map<String, Object> doc = WorkflowExecutionUtils.loadWorkflowDoc(projectId);
			@SuppressWarnings("unchecked")
			Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> edges = (List<Map<String, Object>>) graph.get("edges");
			Map<String, String> configMap = WorkflowExecutionUtils.loadConfig(projectId);

			// Topological sort
			List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(nodes, edges);
			if (ordered.isEmpty()) {
				throw new IllegalArgumentException("Workflow has no nodes to execute");
			}

			// Create run record in DB
			WorkflowDatabaseUtility.insertRun(runId, projectId, WorkflowConstants.DEFAULT_WORKFLOW_ID,
					triggerType, resumeRunId, ordered.size(), userId);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			// Load prior outputs if resuming
			Map<String, String> priorOutputs = loadPriorOutputs(resumeRunId);

			// Execute nodes on a background thread - a workflow can run for hours (large for-each
			// ingestion jobs), so it must never block the calling request/websocket thread.
			// Progress is checkpointed to WORKFLOW_RUNS/WORKFLOW_NODE_OUTPUTS per node; the caller
			// (FE) polls GetWorkflowRun(runId) for live status instead of awaiting this call.
			// Capture the calling thread's ThreadStore (user, session, insight id, scheduler mode)
			// so it can be re-seeded on the background executor thread. ThreadStore is a plain
			// ThreadLocal and is NOT inherited by pool threads; without this, reactors that read
			// ThreadStore during node execution would see null context.
			Map<String, Object> parentContext = ThreadStore.getTheadMapObject();
			final Map<String, Object> contextSnapshot =
					parentContext != null ? new HashMap<>(parentContext) : null;

			try {
				WORKFLOW_EXECUTOR.submit(() -> {
					installThreadContext(contextSnapshot);
					try {
						// executeNodes' own finally always releases the active-run slot -
						// including when it throws, which is caught here - so no explicit
						// release is needed in this catch block.
						executeNodes(runId, projectId, ordered, configMap, priorOutputs);
					} catch (Exception e) {
						classLogger.error("Unhandled error executing workflow run {}: {}", runId, e.getMessage(), e);
						WorkflowDatabaseUtility.updateRunStatus(runId,
								WorkflowConstants.STATUS_FAILED, null, e.getMessage());
					} finally {
						ThreadStore.remove();
					}
				});
			} catch (RejectedExecutionException e) {
				// Never submitted - executeNodes' own finally (which normally releases the
				// active-run slot) will never run. Release happens in the outer catch below.
				WorkflowDatabaseUtility.updateRunStatus(runId, WorkflowConstants.STATUS_FAILED,
						null, "Server is at capacity - too many concurrent workflow runs");
				throw new IllegalStateException("Too many concurrent workflow runs. Please try again shortly.");
			}

			Map<String, Object> result = buildRunResult(runId, projectId, WorkflowConstants.STATUS_RUNNING,
					ordered.size(), 0, null, new ArrayList<>());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (RuntimeException e) {
			// Any failure before (or in lieu of) the run being successfully handed off to the
			// background executor means executeNodes' own finally will never run to release the
			// slot - release it here so the project isn't left permanently blocked.
			WorkflowDatabaseUtility.releaseActiveRun(projectId, runId);
			throw e;
		}
	}

	// -- Core Execution ------------------------------------------------------------

	private Map<String, Object> executeNodes(String runId, String projectId,
			List<Map<String, Object>> ordered, Map<String, String> configMap,
			Map<String, String> priorOutputs) {
		return executeNodes(runId, projectId, ordered, configMap, priorOutputs,
				null, Collections.singleton(projectId));
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

				// Check cancellation between nodes - the local AtomicBoolean is a same-pod fast
				// path (set instantly by CancelWorkflowRunReactor when it lands on this pod);
				// isCancelRequested() is the cluster-safe source of truth, so a cancel request
				// that landed on a different pod than the one executing this run is still
				// honored here.
				if (cancelled.get() || WorkflowDatabaseUtility.isCancelRequested(runId)) {
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
						runId, projectId, node, scope, configMap, completedCount, ancestorProjectIds);

				String status = (String) nodeResult.get(WorkflowConstants.STATUS);
				nodeResults.add(nodeResult);

				if (WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					// Store output in scope for downstream nodes.
					// set-variable nodes write individual variables directly into scope
					// inside SetVariableNodeExecutor - skip the generic put to avoid
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
			// Release the cluster-safe active-run slot claimed in execute() (top-level runs) or
			// SubWorkflowNodeExecutor (sub-workflow runs) - covers every terminal path (success,
			// failure, cancellation) since they all return through here.
			WorkflowDatabaseUtility.releaseActiveRun(projectId, runId);
		}
	}

	private Map<String, Object> executeSingleNode(String runId, String projectId, Map<String, Object> node,
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
			// NodeDispatcher/ChildWorkflowRunner recursion callbacks - only composite executors
			// (conditional/while-loop/try-catch/switch/retry/parallel) and SubWorkflowNodeExecutor
			// actually invoke these; every other executor ignores them.
			NodeDispatcher nodeDispatcher = (innerNode, innerScope) ->
					executeSingleNode(runId, projectId, innerNode, innerScope, configMap, 0, ancestorProjectIds);
			ChildWorkflowRunner childWorkflowRunner = this::executeNodes;
			AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);

			WorkflowNodeContext ctx = new WorkflowNodeContext(runId, projectId, node, scope, configMap,
					ancestorProjectIds, this.insight, cancelFlag, nodeDispatcher, childWorkflowRunner);

			IWorkflowNodeExecutor executor = EXECUTORS.getOrDefault(type, PIXEL_EXECUTOR);
			Object rawOutput = executor.execute(ctx);

			// ForEachNodeExecutor is the one node type whose result map carries a row count for
			// the node checkpoint - detected generically by shape (does the returned Map have a
			// "totalRows" entry), not by branching on node type.
			Integer rowCount = (rawOutput instanceof Map<?, ?> rawMap && rawMap.get("totalRows") instanceof Integer count)
					? count : null;

			// WhileLoopNodeExecutor similarly marks its per-iteration-history result with a
			// "__whileResult" entry on the raw (pre-transform) Map, rather than the caller
			// string-sniffing already-serialized JSON for a magic key.
			Long whileIterationCount = (rawOutput instanceof Map<?, ?> whileMap
					&& Boolean.TRUE.equals(whileMap.get("__whileResult"))
					&& whileMap.get("iterationCount") instanceof Number n)
					? n.longValue() : null;

			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get("outputTransform");
			String transformed = WorkflowExecutionUtils.applyOutputTransform(rawOutput, transformConfig);

			long durationMs = System.currentTimeMillis() - startMs;
			String preview = whileIterationCount != null
					? whileIterationCount + " iteration" + (whileIterationCount == 1 ? "" : "s")
					: PixelExecutionUtils.generatePreview(transformed);

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

	// -- Resume Logic --------------------------------------------------------------

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

	// -- Heartbeat -----------------------------------------------------------------

	private ScheduledExecutorService startHeartbeat(String runId) {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "workflow-heartbeat-" + runId.substring(0, 8));
			t.setDaemon(true);
			return t;
		});
		// Heartbeat fires every 30 seconds - just proves liveness; per-node count updates
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

	// -- Cancellation Support ------------------------------------------------------

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

	/**
	 * Seeds the current (background executor) thread's ThreadStore with a snapshot of the
	 * caller's context. The reading getter forces lazy creation of this thread's map so the
	 * subsequent putAll has a target. Paired with {@code ThreadStore.remove()} in a finally
	 * block so pooled threads never leak context between runs.
	 */
	private static void installThreadContext(Map<String, Object> snapshot) {
		if (snapshot == null || snapshot.isEmpty()) {
			return;
		}
		ThreadStore.getInsightId(); // force creation of this thread's ThreadStore map
		ThreadStore.setThreadMapObject(snapshot);
	}

	// (resolve, getNodeTimeout, applyOutputTransform, strCfg, coerceToMap, loadWorkflowDoc,
	// topoSort moved to WorkflowExecutionUtils)

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

	private Timestamp toTimestamp(Instant instant) {
		return Utility.getSqlTimestampUTC(
				LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
	}

	// -- Result Building -----------------------------------------------------------

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
