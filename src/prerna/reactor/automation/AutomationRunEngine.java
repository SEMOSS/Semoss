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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.automation.nodes.AutomationNodeContext;
import prerna.reactor.automation.nodes.AutomationNodeExecutors;
import prerna.reactor.automation.nodes.IAutomationNodeExecutor;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.util.Utility;

/**
 * Executes an automation run synchronously. Called by {@link TriggerAutomationReactor}
 * on the virtual thread provided by the platform's {@code runPixelAsync} endpoint.
 * Iterates nodes in order, dispatches each to its {@link IAutomationNodeExecutor},
 * and writes per-node status to the DB as it goes.
 */
public final class AutomationRunEngine {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunEngine.class);

	/** In-memory cancellation flags - fast path; the DB flag is the cluster-safe source of truth. */
	static final ConcurrentHashMap<String, AtomicBoolean> CANCELLATION_FLAGS = new ConcurrentHashMap<>();

	private AutomationRunEngine() {}

	/**
	 * In-memory same-pod fast path for cancellation.
	 * Called by {@link CancelAutomationRunReactor}.
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
	 * Runs the full automation node list, blocking until all nodes complete or the run is
	 * cancelled/failed. Runs synchronously on the calling virtual thread.
	 *
	 * @param runId     the run record ID (already inserted into DB by the caller)
	 * @param projectId the owning project
	 * @param ordered   nodes in execution order
	 * @param configMap project automation config key-value pairs
	 * @param insight   the caller's insight context (propagated to each node executor)
	 * @return the run's final variable scope (trigger vars + every node's {@code outputVar}
	 *         that completed successfully) - used by the caller to resolve a per-workflow
	 *         summary message once the run finishes
	 */
	public static Map<String, String> run(String runId, String projectId,
			List<Map<String, Object>> ordered, Map<String, String> configMap, Insight insight) {

		AtomicBoolean cancelled = new AtomicBoolean(false);
		CANCELLATION_FLAGS.put(runId, cancelled);
		ScheduledExecutorService heartbeat = startHeartbeat(runId);

		// Captured once here - TriggerAutomationReactor runs on the virtual thread the platform's
		// runPixelAsync endpoint spawns, so ThreadStore carries that job's id for the whole call.
		// Used to stream per-node progress the same way HarnessToolExecutor streams tool-call
		// progress during an agent turn (see PixelJobManager#addStreamOut), so the FE can poll
		// getPixelJobStreaming(jobId) for live node status instead of inferring it from DB polls.
		String jobId = ThreadStore.getJobId();

		Map<String, String> scope = AutomationExecutionUtils.buildInitialScope(runId, insight.getUser());
		int completedCount = 0;

		try {
			for (Map<String, Object> node : ordered) {
				String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
				String nodeLabel = (String) node.get(AutomationConstants.NODE_FIELD_LABEL);
				String outputVar = (String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
				String nodeType = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);

				if (cancelled.get() || AutomationDatabaseUtility.isCancelRequested(runId)) {
					classLogger.info("Automation run {} cancelled before node {} ({})", runId, nodeId, nodeLabel);
					AutomationDatabaseUtility.updateRunStatus(runId,
							AutomationConstants.STATUS_CANCELLED, nodeId, "Run cancelled by user");
					return scope;
				}

				publishNodeEvent(jobId, nodeId, nodeLabel, AutomationConstants.NODE_STATUS_RUNNING, null, null, null);

				Map<String, Object> nodeResult;
				try {
					nodeResult = executeSingleNode(runId, projectId, node, scope, configMap, cancelled, insight);
				} catch (AutomationCancelledException ace) {
					classLogger.info("Automation run {} cancelled during node {} ({})", runId, nodeId, nodeLabel);
					AutomationDatabaseUtility.updateRunStatus(runId,
							AutomationConstants.STATUS_CANCELLED, nodeId, ace.getMessage());
					publishNodeEvent(jobId, nodeId, nodeLabel, AutomationConstants.STATUS_CANCELLED, null, null,
							ace.getMessage());
					return scope;
				}

				String status = (String) nodeResult.get(AutomationConstants.STATUS);
				Object durationMs = nodeResult.get(AutomationConstants.DURATION_MS);
				String preview = (String) nodeResult.get(AutomationConstants.OUTPUT_PREVIEW);
				String errorMsg = (String) nodeResult.get(AutomationConstants.ERROR_MESSAGE);
				publishNodeEvent(jobId, nodeId, nodeLabel, status, durationMs, preview, errorMsg);

				if (AutomationConstants.NODE_STATUS_SUCCESS.equals(status)) {
					if (outputVar != null && !outputVar.isEmpty()
							&& !AutomationConstants.NODE_TRIGGER.equals(nodeType)) {
						String outputValue = (String) nodeResult.get(AutomationConstants.RESULT_OUTPUT_VALUE);
						scope.put(outputVar, outputValue != null ? outputValue : "");
					}
					completedCount++;
					AutomationDatabaseUtility.updateHeartbeat(runId, completedCount);
				} else {
					classLogger.warn("Automation run {} failed at node {} ({}): {}", runId, nodeId, nodeLabel, errorMsg);
					AutomationDatabaseUtility.updateRunStatus(runId,
							AutomationConstants.STATUS_FAILED, nodeId, errorMsg);
					return scope;
				}
			}

			classLogger.info("Automation run {} completed successfully ({}/{} nodes)", runId, completedCount, ordered.size());
			AutomationDatabaseUtility.updateRunStatus(runId, AutomationConstants.STATUS_SUCCESS, null, null);
			return scope;

		} finally {
			heartbeat.shutdownNow();
			CANCELLATION_FLAGS.remove(runId);
			AutomationDatabaseUtility.releaseActiveRun(projectId, runId);
		}
	}

	// -- Streaming -------------------------------------------------------------------

	/**
	 * Publishes a per-node progress event onto the pixel job's stream, mirroring
	 * {@code HarnessToolExecutor.publishToolResult} - the FE polls {@code getPixelJobStreaming(jobId)}
	 * (the same mechanism playground uses for live tool-call progress) to render each node's
	 * running/success/failed transition as it happens, instead of inferring progress from DB polls.
	 * A no-op when {@code jobId} is blank (e.g. called outside a {@code runPixelAsync} job).
	 */
	private static void publishNodeEvent(String jobId, String nodeId, String nodeLabel, String status,
			Object durationMs, String preview, String errorMessage) {
		if (jobId == null || jobId.isBlank()) {
			return;
		}
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", "node-status");
		data.put(AutomationConstants.NODE_ID, nodeId);
		data.put(AutomationConstants.NODE_LABEL, nodeLabel);
		data.put(AutomationConstants.STATUS, status);
		if (durationMs != null) {
			data.put(AutomationConstants.DURATION_MS, durationMs);
		}
		if (preview != null) {
			data.put(AutomationConstants.OUTPUT_PREVIEW, preview);
		}
		if (errorMessage != null) {
			data.put(AutomationConstants.ERROR_MESSAGE, errorMessage);
		}
		data.put("timestamp", Instant.now().toString());

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", "automation");
		envelope.put("data", data);
		PixelJobManager.getManager().addStreamOut(jobId, envelope);
	}

	// -- Node execution ------------------------------------------------------------

	private static Map<String, Object> executeSingleNode(String runId, String projectId,
			Map<String, Object> node, Map<String, String> scope, Map<String, String> configMap,
			AtomicBoolean cancelFlag, Insight insight) {

		String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		String nodeLabel = (String) node.get(AutomationConstants.NODE_FIELD_LABEL);
		String outputVar = (String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);

		if (AutomationConstants.NODE_TRIGGER.equals(type)) {
			return buildNodeResult(nodeId, nodeLabel, AutomationConstants.NODE_STATUS_SUCCESS, 0,
					scope.get(AutomationConstants.SCOPE_TRIGGERED_AT), null);
		}

		classLogger.debug("Executing node {} ({}) type={} in run {}", nodeId, nodeLabel, type, runId);
		AutomationDatabaseUtility.markNodeRunning(runId, nodeId);
		Timestamp startedAt = toTimestamp(Instant.now());
		long startMs = System.currentTimeMillis();

		try {
			AutomationNodeContext ctx = new AutomationNodeContext(
					runId, projectId, node, scope, configMap, insight, cancelFlag);

			IAutomationNodeExecutor executor = AutomationNodeExecutors.EXECUTORS.get(type);
			if (executor == null) {
				throw new IllegalArgumentException("Unsupported node type: " + type);
			}
			Object rawOutput = executor.execute(ctx);

			@SuppressWarnings("unchecked")
			Map<String, Object> transformConfig = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_OUTPUT_TRANSFORM);
			String transformed = AutomationExecutionUtils.applyOutputTransform(rawOutput, transformConfig);
			long durationMs = System.currentTimeMillis() - startMs;
			String preview = AutomationExecutionUtils.generatePreview(transformed);

			AutomationDatabaseUtility.updateNodeSuccess(runId, nodeId, startedAt, durationMs, outputVar, transformed, preview);

			classLogger.debug("Node {} ({}) succeeded in {}ms in run {}", nodeId, nodeLabel, durationMs, runId);
			Map<String, Object> result = buildNodeResult(nodeId, nodeLabel,
					AutomationConstants.NODE_STATUS_SUCCESS, durationMs, transformed, null);
			result.put(AutomationConstants.RESULT_OUTPUT_VALUE, transformed);
			return result;

		} catch (AutomationCancelledException ace) {
			long durationMs = System.currentTimeMillis() - startMs;
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, startedAt, durationMs, ace.getMessage());
			throw ace;
		} catch (Exception e) {
			long durationMs = System.currentTimeMillis() - startMs;
			String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			classLogger.error("Node {} ({}) failed in run {}: {}", nodeId, nodeLabel, runId, errorMsg, e);
			AutomationDatabaseUtility.updateNodeFailed(runId, nodeId, startedAt, durationMs, errorMsg);
			return buildNodeResult(nodeId, nodeLabel,
					AutomationConstants.NODE_STATUS_FAILED, durationMs, null, errorMsg);
		}
	}

	// -- Heartbeat -----------------------------------------------------------------

	private static ScheduledExecutorService startHeartbeat(String runId) {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "automation-heartbeat-" + runId.substring(0, Math.min(8, runId.length())));
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

	// -- Helpers -------------------------------------------------------------------

	private static Map<String, Object> buildNodeResult(String nodeId, String nodeLabel,
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

	private static Timestamp toTimestamp(Instant instant) {
		return Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
	}
}
