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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.nodes.AutomationNodeContext;
import prerna.reactor.automation.nodes.AutomationNodeExecutors;
import prerna.reactor.automation.nodes.IAutomationNodeExecutor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class TriggerAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(TriggerAutomationReactor.class);

	private static final ConcurrentHashMap<String, AtomicBoolean> CANCELLATION_FLAGS = new ConcurrentHashMap<>();

	private static final ExecutorService AUTOMATION_EXECUTOR = new ThreadPoolExecutor(
			2, 20, 60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(10),
			r -> {
				Thread t = new Thread(r, "automation-run-" + System.nanoTime());
				t.setDaemon(true);
				return t;
			},
			new ThreadPoolExecutor.AbortPolicy()
	);

	public TriggerAutomationReactor() {
		this.keysToGet = new String[]{ "project" };
		this.keyRequired = new int[]{ 1 };
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

		try {
			Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);
			@SuppressWarnings("unchecked")
			Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> edges = (List<Map<String, Object>>) graph.get("edges");
			Map<String, String> configMap = AutomationExecutionUtils.loadConfig(projectId);

			List<Map<String, Object>> ordered = AutomationExecutionUtils.topoSort(nodes, edges);
			if (ordered.isEmpty()) {
				throw new IllegalArgumentException("Automation has no nodes to execute");
			}

			AutomationDatabaseUtility.insertRun(runId, projectId, AutomationConstants.DEFAULT_AUTOMATION_ID,
					AutomationConstants.TRIGGER_MANUAL, ordered.size(), userId);
			AutomationDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			Map<String, Object> parentContext = ThreadStore.getTheadMapObject();
			final Map<String, Object> contextSnapshot =
					parentContext != null ? new HashMap<>(parentContext) : null;

			try {
				AUTOMATION_EXECUTOR.submit(() -> {
					installThreadContext(contextSnapshot);
					try {
						executeNodes(runId, projectId, ordered, configMap);
					} catch (Exception e) {
						classLogger.error("Unhandled error executing automation run {}: {}", runId, e.getMessage(), e);
						AutomationDatabaseUtility.updateRunStatus(runId,
								AutomationConstants.STATUS_FAILED, null, e.getMessage());
					} finally {
						ThreadStore.remove();
					}
				});
			} catch (RejectedExecutionException e) {
				AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_FAILED,
						null, "Server is at capacity - too many concurrent automation runs");
				throw new IllegalStateException("Too many concurrent automation runs. Please try again shortly.");
			}

			Map<String, Object> result = buildRunResult(runId, projectId, AutomationConstants.STATUS_RUNNING,
					ordered.size(), 0, null, new ArrayList<>());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (RuntimeException e) {
			AutomationDatabaseUtility.releaseActiveRun(projectId, runId);
			throw e;
		}
	}

	// -- Core Execution ------------------------------------------------------------

	private Map<String, Object> executeNodes(String runId, String projectId,
			List<Map<String, Object>> ordered, Map<String, String> configMap) {

		AtomicBoolean cancelled = new AtomicBoolean(false);
		CANCELLATION_FLAGS.put(runId, cancelled);

		ScheduledExecutorService heartbeat = startHeartbeat(runId);

		Map<String, String> scope = buildInitialScope(runId);
		List<Map<String, Object>> nodeResults = new ArrayList<>();
		int completedCount = 0;

		try {
			for (Map<String, Object> node : ordered) {
				String nodeId = (String) node.get("id");
				String nodeLabel = (String) node.get("label");
				String outputVar = (String) node.get("outputVar");
				String nodeType = (String) node.get("type");

				if (cancelled.get() || AutomationDatabaseUtility.isCancelRequested(runId)) {
					AutomationDatabaseUtility.updateRunStatus(runId,
							AutomationConstants.STATUS_CANCELLED, nodeId, "Run cancelled by user");
					nodeResults.add(buildNodeResult(nodeId, nodeLabel,
							AutomationConstants.STATUS_CANCELLED, 0, null, "Run cancelled by user"));
					return buildRunResult(runId, projectId, AutomationConstants.STATUS_CANCELLED,
							ordered.size(), completedCount, nodeId, nodeResults);
				}

				Map<String, Object> nodeResult;
				try {
					nodeResult = executeSingleNode(runId, projectId, node, scope, configMap);
				} catch (AutomationCancelledException ace) {
					AutomationDatabaseUtility.updateRunStatus(runId,
							AutomationConstants.STATUS_CANCELLED, nodeId, ace.getMessage());
					nodeResults.add(buildNodeResult(nodeId, nodeLabel,
							AutomationConstants.NODE_STATUS_FAILED, 0, null, ace.getMessage()));
					return buildRunResult(runId, projectId, AutomationConstants.STATUS_CANCELLED,
							ordered.size(), completedCount, nodeId, nodeResults);
				}

				String status = (String) nodeResult.get(AutomationConstants.STATUS);
				nodeResults.add(nodeResult);

				if (AutomationConstants.NODE_STATUS_SUCCESS.equals(status)) {
					if (outputVar != null && !outputVar.isEmpty()
							&& !AutomationConstants.NODE_TRIGGER.equals(nodeType)) {
						String outputValue = (String) nodeResult.get("outputValue");
						scope.put(outputVar, outputValue != null ? outputValue : "");
					}
					completedCount++;
					AutomationDatabaseUtility.updateHeartbeat(runId, completedCount);
				} else {
					String errorMsg = (String) nodeResult.get(AutomationConstants.ERROR_MESSAGE);
					AutomationDatabaseUtility.updateRunStatus(runId,
							AutomationConstants.STATUS_FAILED, nodeId, errorMsg);
					return buildRunResult(runId, projectId, AutomationConstants.STATUS_FAILED,
							ordered.size(), completedCount, nodeId, nodeResults);
				}
			}

			AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_SUCCESS, null, null);
			return buildRunResult(runId, projectId, AutomationConstants.STATUS_SUCCESS,
					ordered.size(), completedCount, null, nodeResults);

		} finally {
			heartbeat.shutdownNow();
			CANCELLATION_FLAGS.remove(runId);
			AutomationDatabaseUtility.releaseActiveRun(projectId, runId);
		}
	}

	private Map<String, Object> executeSingleNode(String runId, String projectId, Map<String, Object> node,
			Map<String, String> scope, Map<String, String> configMap) {

		String nodeId = (String) node.get("id");
		String nodeLabel = (String) node.get("label");
		String outputVar = (String) node.get("outputVar");
		String type = (String) node.get("type");

		// Trigger node is a metadata-only node — just return success
		if (AutomationConstants.NODE_TRIGGER.equals(type)) {
			return buildNodeResult(nodeId, nodeLabel, AutomationConstants.NODE_STATUS_SUCCESS, 0,
					scope.get("triggered_at"), null);
		}

		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		Timestamp startedAt = toTimestamp(Instant.now());
		long startMs = System.currentTimeMillis();

		try {
			AtomicBoolean cancelFlag = CANCELLATION_FLAGS.get(runId);
			AutomationNodeContext ctx = new AutomationNodeContext(
					runId, projectId, node, scope, configMap, this.insight, cancelFlag);

			IAutomationNodeExecutor executor = AutomationNodeExecutors.EXECUTORS.get(type);
			if (executor == null) {
				throw new IllegalArgumentException("Unsupported node type: " + type);
			}
			Object rawOutput = executor.execute(ctx);

			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get("outputTransform");
			String transformed = AutomationExecutionUtils.applyOutputTransform(rawOutput, transformConfig);

			long durationMs = System.currentTimeMillis() - startMs;
			String preview = generatePreview(transformed);

			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, startedAt, durationMs, outputVar, transformed, preview);

			Map<String, Object> result = buildNodeResult(nodeId, nodeLabel,
					AutomationConstants.NODE_STATUS_SUCCESS, durationMs, preview, null);
			result.put("outputValue", transformed);
			return result;

		} catch (AutomationCancelledException ace) {
			long durationMs = System.currentTimeMillis() - startMs;
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, startedAt, durationMs, ace.getMessage());
			throw ace;
		} catch (Exception e) {
			long durationMs = System.currentTimeMillis() - startMs;
			String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			classLogger.error("Node {} ({}) failed: {}", nodeId, nodeLabel, errorMsg, e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, startedAt, durationMs, errorMsg);
			return buildNodeResult(nodeId, nodeLabel,
					AutomationConstants.NODE_STATUS_FAILED, durationMs, null, errorMsg);
		}
	}

	// -- Heartbeat -----------------------------------------------------------------

	private ScheduledExecutorService startHeartbeat(String runId) {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "automation-heartbeat-" + runId.substring(0, 8));
			t.setDaemon(true);
			return t;
		});
		scheduler.scheduleAtFixedRate(() -> {
			try {
				AutomationDatabaseUtility.touchHeartbeat(runId);
			} catch (Exception e) {
				classLogger.warn("Heartbeat update failed for run {}: {}", runId, e.getMessage());
			}
		}, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS,
				AutomationConstants.HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
		return scheduler;
	}

	// -- Cancellation Support ------------------------------------------------------

	public static boolean requestCancellation(String runId) {
		AtomicBoolean flag = CANCELLATION_FLAGS.get(runId);
		if (flag != null) {
			flag.set(true);
			return true;
		}
		return false;
	}

	// -- Helpers -------------------------------------------------------------------

	private static void installThreadContext(Map<String, Object> snapshot) {
		if (snapshot == null || snapshot.isEmpty()) return;
		ThreadStore.getInsightId();
		ThreadStore.setThreadMapObject(snapshot);
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
		return "system";
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
		return Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
	}

	private static String generatePreview(String s) {
		if (s == null) return null;
		return s.length() <= AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH
				? s : s.substring(0, AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH);
	}

	// -- Result Building -----------------------------------------------------------

	private Map<String, Object> buildNodeResult(String nodeId, String nodeLabel,
			String status, long durationMs, String outputPreview, String errorMessage) {
		Map<String, Object> result = new HashMap<>();
		result.put(AutomationConstants.NODE_ID, nodeId);
		result.put(AutomationConstants.NODE_LABEL, nodeLabel);
		result.put(AutomationConstants.STATUS, status);
		result.put(AutomationConstants.DURATION_MS, durationMs);
		if (outputPreview != null) result.put(AutomationConstants.OUTPUT_PREVIEW, outputPreview);
		if (errorMessage != null) result.put(AutomationConstants.ERROR_MESSAGE, errorMessage);
		return result;
	}

	private Map<String, Object> buildRunResult(String runId, String projectId, String status,
			int totalNodes, int completedNodes, String failedNodeId,
			List<Map<String, Object>> nodeResults) {
		Map<String, Object> stored = AutomationDatabaseUtility.getRunDetail(runId);
		Map<String, Object> result = new HashMap<>();
		result.put(AutomationConstants.RUN_ID, runId);
		result.put(AutomationConstants.PROJECT_ID, projectId);
		result.put(AutomationConstants.STATUS, status);
		result.put(AutomationConstants.TOTAL_NODES, totalNodes);
		result.put(AutomationConstants.COMPLETED_NODES, completedNodes);
		if (stored != null) {
			result.put(AutomationConstants.STARTED_AT, stored.get(AutomationConstants.STARTED_AT));
			result.put(AutomationConstants.COMPLETED_AT, stored.get(AutomationConstants.COMPLETED_AT));
		}
		if (failedNodeId != null) result.put(AutomationConstants.FAILED_NODE_ID, failedNodeId);
		result.put("nodeResults", nodeResults);
		return result;
	}
}
