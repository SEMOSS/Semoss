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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;

/**
 * Maintains same-pod cancellation and heartbeat state for active Python
 * Automation runs.
 *
 * <p>
 * Durable cancellation remains in the scheduler database. This registry adds
 * only the local socket interrupt fast path and periodic liveness updates for
 * runs owned by the current JVM.
 */
final class AutomationPythonRunRegistry {

	private static final Logger classLogger = LogManager.getLogger(AutomationPythonRunRegistry.class);
	private static final int MAX_RETAINED_RUN_WORKSPACES = 25;
	private static final long RUN_WORKSPACE_RETENTION_MINUTES = 5;
	private static final ConcurrentHashMap<String, ActivePythonRun> RUNS = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, RetainedInsight> RETAINED_INSIGHTS = new ConcurrentHashMap<>();
	private static final ConcurrentLinkedDeque<String> RETAINED_ORDER = new ConcurrentLinkedDeque<>();
	private static final ScheduledExecutorService HEARTBEAT_SCHEDULER = Executors
			.newSingleThreadScheduledExecutor(r -> {
				Thread thread = new Thread(r, "automation-python-heartbeat");
				thread.setDaemon(true);
				return thread;
			});
	private static final ScheduledExecutorService WORKSPACE_CLEANUP_SCHEDULER = Executors
			.newSingleThreadScheduledExecutor(r -> {
				Thread thread = new Thread(r, "automation-workspace-cleanup");
				thread.setDaemon(true);
				return thread;
			});

	private AutomationPythonRunRegistry() {
	}

	static void register(String runId, PyTranslator translator, Insight insight, String jobId) {
		register(runId, translator, insight, jobId, 0);
	}

	static void register(String runId, PyTranslator translator, Insight insight, String jobId, int completedNodes) {
		ActivePythonRun active = new ActivePythonRun(translator, insight.getInsightId(), jobId, completedNodes);
		if (RUNS.putIfAbsent(runId, active) != null) {
			throw new IllegalStateException("Python automation run is already registered: " + runId);
		}
		active.heartbeat = HEARTBEAT_SCHEDULER.scheduleAtFixedRate(() -> {
			try {
				AutomationDatabaseUtility.touchHeartbeat(runId);
			} catch (Exception e) {
				classLogger.warn("Heartbeat update failed for Python automation run {}: {}", runId, e.getMessage());
			}
		}, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS,
				TimeUnit.SECONDS);
	}

	static void unregister(String runId) {
		ActivePythonRun active = RUNS.remove(runId);
		if (active != null && active.heartbeat != null) {
			active.heartbeat.cancel(false);
		}
		RetainedInsight retained = RETAINED_INSIGHTS.remove(runId);
		if (retained != null) {
			RETAINED_ORDER.remove(runId);
			retained.cancelCleanupTask();
			retained.cleanup();
		}
	}

	/**
	 * Stops active-run tracking but keeps the run Insight available briefly for the
	 * read-only files panel. The oldest retained workspace is cleaned immediately
	 * when the bounded cache is full; every retained workspace is otherwise cleaned
	 * after the inspection window.
	 *
	 * @param runId   completed automation run identifier
	 * @param cleanup existing full Insight cleanup owned by the execution service
	 */
	static void retainInsightForInspection(String runId, Runnable cleanup) {
		ActivePythonRun active = RUNS.remove(runId);
		if (active == null) {
			cleanup.run();
			return;
		}
		if (active.heartbeat != null) {
			active.heartbeat.cancel(false);
		}

		RetainedInsight retained = new RetainedInsight(active.insightId, cleanup);
		RetainedInsight previous = RETAINED_INSIGHTS.put(runId, retained);
		RETAINED_ORDER.remove(runId);
		RETAINED_ORDER.addLast(runId);
		if (previous != null) {
			previous.cancelCleanupTask();
			previous.cleanup();
		}
		retained.cleanupTask = WORKSPACE_CLEANUP_SCHEDULER.schedule(() -> releaseRetainedInsight(runId, retained),
				RUN_WORKSPACE_RETENTION_MINUTES, TimeUnit.MINUTES);

		while (RETAINED_INSIGHTS.size() > MAX_RETAINED_RUN_WORKSPACES) {
			String oldestRunId = RETAINED_ORDER.pollFirst();
			if (oldestRunId == null) {
				break;
			}
			RetainedInsight oldest = RETAINED_INSIGHTS.remove(oldestRunId);
			if (oldest != null) {
				oldest.cancelCleanupTask();
				oldest.cleanup();
			}
		}
	}

	/**
	 * Returns the temporary Insight workspace for an active or recently completed
	 * run owned by this JVM.
	 *
	 * @param runId active automation run identifier
	 * @return Insight identifier, or {@code null} when this JVM does not own the run
	 */
	static String getInsightId(String runId) {
		ActivePythonRun active = RUNS.get(runId);
		if (active != null) {
			return active.insightId;
		}
		RetainedInsight retained = RETAINED_INSIGHTS.get(runId);
		return retained == null ? null : retained.insightId;
	}

	private static void releaseRetainedInsight(String runId, RetainedInsight retained) {
		if (RETAINED_INSIGHTS.remove(runId, retained)) {
			RETAINED_ORDER.remove(runId);
			retained.cleanup();
		}
	}

	static boolean isCancellationRequested(String runId) {
		ActivePythonRun active = RUNS.get(runId);
		return (active != null && active.cancelled.get()) || AutomationDatabaseUtility.isCancelRequested(runId);
	}

	static boolean requestCancellation(String runId) {
		ActivePythonRun active = RUNS.get(runId);
		if (active == null) {
			return false;
		}
		active.cancelled.set(true);
		if (active.jobId != null && !active.jobId.isBlank()) {
			try {
				active.translator.getSocketClient().interruptInsightJob(active.insightId, active.jobId);
			} catch (Exception e) {
				classLogger.warn("Unable to interrupt Python socket for automation run {}: {}", runId, e.getMessage());
			}
		}
		return true;
	}

	static void nodeCompleted(String runId) {
		ActivePythonRun active = RUNS.get(runId);
		if (active != null) {
			AutomationDatabaseUtility.updateHeartbeat(runId, active.completedNodes.incrementAndGet());
		}
	}

	private static final class ActivePythonRun {
		private final PyTranslator translator;
		private final String insightId;
		private final String jobId;
		private final AtomicBoolean cancelled = new AtomicBoolean();
		private final AtomicInteger completedNodes;
		private ScheduledFuture<?> heartbeat;

		private ActivePythonRun(PyTranslator translator, String insightId, String jobId, int completedNodes) {
			this.translator = translator;
			this.insightId = insightId;
			this.jobId = jobId;
			this.completedNodes = new AtomicInteger(Math.max(0, completedNodes));
		}
	}

	private static final class RetainedInsight {
		private final String insightId;
		private final Runnable cleanupAction;
		private final AtomicBoolean cleaned = new AtomicBoolean();
		private ScheduledFuture<?> cleanupTask;

		private RetainedInsight(String insightId, Runnable cleanupAction) {
			this.insightId = insightId;
			this.cleanupAction = cleanupAction;
		}

		private void cancelCleanupTask() {
			if (cleanupTask != null) {
				cleanupTask.cancel(false);
			}
		}

		private void cleanup() {
			if (!cleaned.compareAndSet(false, true)) {
				return;
			}
			try {
				cleanupAction.run();
			} catch (RuntimeException e) {
				classLogger.warn("Unable to clean retained Automation Insight '{}': {}", insightId, e.getMessage(), e);
			}
		}
	}
}
