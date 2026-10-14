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
package prerna.reactor.automation.run;

import java.util.concurrent.ConcurrentHashMap;
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
import prerna.reactor.automation.AutomationConstants;

/**
 * Maintains same-pod cancellation and heartbeat state for active Automation
 * runs.
 *
 * <p>
 * Durable cancellation remains in the scheduler database. This registry adds
 * only local Java/Python interruption and periodic liveness updates for runs
 * owned by the current JVM.
 */
final class AutomationRunRegistry {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunRegistry.class);
	private static final ConcurrentHashMap<String, ActiveRun> RUNS = new ConcurrentHashMap<>();
	private static final ScheduledExecutorService HEARTBEAT_SCHEDULER = Executors
			.newSingleThreadScheduledExecutor(r -> {
				Thread thread = new Thread(r, "automation-heartbeat");
				thread.setDaemon(true);
				return thread;
			});

	private AutomationRunRegistry() {
	}

	static void register(String runId, Insight insight, String jobId) {
		register(runId, insight, jobId, 0);
	}

	static void register(String runId, Insight insight, String jobId, int completedNodes) {
		ActiveRun active = new ActiveRun(insight.getInsightId(), jobId, completedNodes);
		active.heartbeat = HEARTBEAT_SCHEDULER.scheduleAtFixedRate(() -> {
			try {
				AutomationRunStore.touchHeartbeat(runId);
			} catch (Exception e) {
				classLogger.warn("Heartbeat update failed for Automation run '{}'", runId, e);
			}
		}, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS, AutomationConstants.HEARTBEAT_INTERVAL_SECONDS,
				TimeUnit.SECONDS);
		if (RUNS.putIfAbsent(runId, active) != null) {
			active.heartbeat.cancel(false);
			throw new IllegalStateException("Automation run is already registered: " + runId);
		}
	}

	static void attachPythonTranslator(String runId, PyTranslator translator) {
		ActiveRun active = RUNS.get(runId);
		if (active == null) {
			throw new IllegalStateException("Automation run is not registered: " + runId);
		}
		active.attachPythonTranslator(runId, translator);
	}

	static void attachJavaCancellation(String runId, Runnable cancellationSignal) {
		ActiveRun active = RUNS.get(runId);
		if (active == null) {
			throw new IllegalStateException("Automation run is not registered: " + runId);
		}
		active.attachJavaCancellation(runId, cancellationSignal);
	}

	static void detachJavaCancellation(String runId, Runnable cancellationSignal) {
		ActiveRun active = RUNS.get(runId);
		if (active != null) {
			active.detachJavaCancellation(cancellationSignal);
		}
	}

	static void unregister(String runId) {
		ActiveRun active = RUNS.remove(runId);
		if (active != null && active.heartbeat != null) {
			active.heartbeat.cancel(false);
		}
	}

	static boolean isCancellationRequested(String runId) {
		ActiveRun active = RUNS.get(runId);
		return (active != null && active.cancelled.get()) || AutomationRunStore.isCancelRequested(runId);
	}

	static boolean requestCancellation(String runId) {
		ActiveRun active = RUNS.get(runId);
		if (active == null) {
			return false;
		}
		active.cancelled.set(true);
		active.signalJavaCancellation(runId);
		active.interruptPython(runId);
		return true;
	}

	static void nodeCompleted(String runId) {
		ActiveRun active = RUNS.get(runId);
		if (active != null) {
			AutomationRunStore.updateHeartbeat(runId, active.completedNodes.incrementAndGet());
		}
	}

	private static final class ActiveRun {
		private final String insightId;
		private final String jobId;
		private final AtomicBoolean cancelled = new AtomicBoolean();
		private final AtomicInteger completedNodes;
		private volatile PyTranslator translator;
		private volatile Runnable javaCancellationSignal;
		private ScheduledFuture<?> heartbeat;

		private ActiveRun(String insightId, String jobId, int completedNodes) {
			this.insightId = insightId;
			this.jobId = jobId;
			this.completedNodes = new AtomicInteger(Math.max(0, completedNodes));
		}

		private synchronized void attachPythonTranslator(String runId, PyTranslator attachedTranslator) {
			if (attachedTranslator == null) {
				throw new IllegalArgumentException("Automation Python translator must not be null.");
			}
			if (translator != null && translator != attachedTranslator) {
				throw new IllegalStateException("Automation run has a different Python translator: " + runId);
			}
			translator = attachedTranslator;
			if (cancelled.get()) {
				interruptPython(runId);
			}
		}

		private synchronized void attachJavaCancellation(String runId, Runnable cancellationSignal) {
			if (cancellationSignal == null) {
				throw new IllegalArgumentException("Automation Java cancellation signal must not be null.");
			}
			if (javaCancellationSignal != null && javaCancellationSignal != cancellationSignal) {
				throw new IllegalStateException("Automation run already has an active Java executor: " + runId);
			}
			javaCancellationSignal = cancellationSignal;
			if (cancelled.get()) {
				signalJavaCancellation(runId);
			}
		}

		private synchronized void detachJavaCancellation(Runnable cancellationSignal) {
			if (javaCancellationSignal == cancellationSignal) {
				javaCancellationSignal = null;
			}
		}

		private void signalJavaCancellation(String runId) {
			Runnable activeSignal = javaCancellationSignal;
			if (activeSignal == null) {
				return;
			}
			try {
				activeSignal.run();
			} catch (RuntimeException e) {
				classLogger.warn("Unable to signal Java executor cancellation for Automation run '{}'", runId, e);
			}
		}

		private void interruptPython(String runId) {
			PyTranslator activeTranslator = translator;
			if (activeTranslator == null || jobId == null || jobId.isBlank()) {
				return;
			}
			try {
				activeTranslator.getSocketClient().interruptInsightJob(insightId, jobId);
			} catch (Exception e) {
				classLogger.warn("Unable to interrupt Python socket for Automation run '{}'", runId, e);
			}
		}
	}

}
