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
package prerna.reactor.agent.run;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.exceptions.AgentCancelledException;

final class AgentRunWorker {

	private static final Logger logger = LogManager.getLogger(AgentRunWorker.class);
	private static final int SCAN_LIMIT = 0;
	private static final long IDLE_WAIT_MS = 1000L;

	private final AgentRuntimeManager runtime;
	private final AgentRunStore store;
	private final AgentRunQueueCoordinator queueCoordinator;
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final Object monitor = new Object();
	private final Map<String, InsightHandle> insightsByRun = new ConcurrentHashMap<>();
	private final Map<String, Thread> activeThreadsByRun = new ConcurrentHashMap<>();
	private final Set<String> localActiveRooms = ConcurrentHashMap.newKeySet();

	AgentRunWorker(AgentRuntimeManager runtime, AgentRunStore store, AgentRunQueueCoordinator queueCoordinator) {
		this.runtime = runtime;
		this.store = store;
		this.queueCoordinator = queueCoordinator;
	}

	void rememberInsight(String runId, Insight insight) {
		if (runId == null || insight == null) {
			return;
		}
		insightsByRun.put(runId, cloneInsight(insight));
	}

	void signal() {
		start();
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

	void cancel(String runId) {
		Thread activeThread = activeThreadsByRun.get(runId);
		if (activeThread != null) {
			activeThread.interrupt();
		}
	}

	private void start() {
		if (!started.compareAndSet(false, true)) {
			return;
		}
		Thread workerThread = new Thread(this::loop, "agent-run-worker");
		workerThread.setDaemon(true);
		workerThread.start();
	}

	private void loop() {
		while (true) {
			boolean didWork = false;
			try {
				List<AgentRunRecord> records = store.getSubmittedRuns(SCAN_LIMIT, null);
				for (AgentRunRecord record : records) {
					InsightHandle insightHandle = insightsByRun.get(record.getRunId());
					if (insightHandle == null) {
						continue;
					}
					if (tryExecute(record, insightHandle)) {
						didWork = true;
					}
				}
			} catch (Exception e) {
				logger.warn("AgentRunWorker: queue scan failed: {}", e.getMessage(), e);
			}
			if (!didWork) {
				waitForSignal();
			}
		}
	}

	private boolean tryExecute(AgentRunRecord record, InsightHandle insightHandle) {
		String runId = record.getRunId();
		String roomId = record.getRoomId();
		if (!store.isOldestSubmittedRunForRoom(runId, roomId)) {
			return false;
		}
		if (!localActiveRooms.add(roomId)) {
			return false;
		}

		AgentRunQueueCoordinator.ActiveRunLease lease = null;
		try {
			lease = queueCoordinator.tryClaimTurn(runId, roomId);
			if (lease == null) {
				localActiveRooms.remove(roomId);
				return false;
			}
			String jobId = runId;
			if (!store.markRunningIfSubmitted(runId, jobId)) {
				lease.close();
				localActiveRooms.remove(roomId);
				cleanupInsight(runId, insightHandle);
				return false;
			}
			AgentRunEventBus.get().publishStatus(runId, roomId, AgentRunStatus.RUNNING, false);
			final AgentRunQueueCoordinator.ActiveRunLease claimedLease = lease;
			Thread thread = Thread.ofVirtual().name("agent-run-" + runId).unstarted(() -> {
				try {
					execute(record, insightHandle);
				} finally {
					activeThreadsByRun.remove(runId);
					claimedLease.close();
					localActiveRooms.remove(roomId);
				}
			});
			activeThreadsByRun.put(runId, thread);
			thread.start();
			return true;
		} catch (RuntimeException e) {
			if (lease != null) {
				lease.close();
			}
			localActiveRooms.remove(roomId);
			throw e;
		}
	}

	private void execute(AgentRunRecord record, InsightHandle insightHandle) {
		String runId = record.getRunId();
		String jobId = runId;
		try {
			seedThreadStore(runId, insightHandle);
			RunAgentRequest request = record.getRequest();
			AgentHarnessResult result = AgentRunner.run(request.getRoomId(), request.getInput(),
					request.getEngineIdFallback(), request.getHarnessType(), request.getMaxTurns(),
					request.getMaxReflections(), request.getParamMap(), request.getAgentParamMap(), runId,
					insightHandle.insight);
			if (result != null) {
				store.markInputMessage(runId, result.getInputMessageId());
			}

			jobId = runtime.firstNonBlank(ThreadStore.getJobId(), jobId);
			if (result != null) {
				store.markFinalOutputMessage(runId, result.getFinalOutputMessageId());
			}
			if (Thread.currentThread().isInterrupted()) {
				throw new AgentCancelledException();
			}
			store.markCompleted(runId, jobId, result != null ? result.getFinalText() : null);
			Map<String, Object> eventData = new java.util.HashMap<>();
			eventData.put("runId", runId);
			eventData.put("roomId", record.getRoomId());
			eventData.put("status", AgentRunStatus.COMPLETED.name());
			eventData.put("finalText", result != null ? result.getFinalText() : null);
			eventData.put("inputMessageId", result != null ? result.getInputMessageId() : null);
			eventData.put("finalOutputMessageId", result != null ? result.getFinalOutputMessageId() : null);
			AgentRunEventBus.get().publish(runId, "status", eventData, true);
		} catch (Exception e) {
			jobId = runtime.firstNonBlank(ThreadStore.getJobId(), jobId);
			if (runtime.isCancelled(e)) {
				store.markCancelled(runId, jobId, runtime.boundedError(e));
				AgentRunEventBus.get().publishStatus(runId, record.getRoomId(), AgentRunStatus.CANCELLED, true);
			} else {
				store.markFailed(runId, jobId, runtime.boundedError(e));
				Map<String, Object> eventData = new java.util.HashMap<>();
				eventData.put("runId", runId);
				eventData.put("roomId", record.getRoomId());
				eventData.put("status", AgentRunStatus.FAILED.name());
				eventData.put("errorMessage", runtime.boundedError(e));
				AgentRunEventBus.get().publish(runId, "status", eventData, true);
			}
			logger.warn("AgentRunWorker: runId={} failed: {}", runId, e.getMessage(), e);
		} finally {
			ThreadStore.remove();
			cleanupInsight(runId, insightHandle);
		}
	}

	private void waitForSignal() {
		synchronized (monitor) {
			try {
				monitor.wait(IDLE_WAIT_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private void cleanupInsight(String runId, InsightHandle insightHandle) {
		InsightHandle removed = insightsByRun.remove(runId);
		InsightHandle toCleanup = removed != null ? removed : insightHandle;
		if (toCleanup != null && toCleanup.insightId != null) {
			InsightStore.getInstance().remove(toCleanup.insightId);
		}
	}

	private static InsightHandle cloneInsight(Insight source) {
		Insight clone = new Insight();
		User user = source.getUser();
		if (user == null) {
			user = ThreadStore.getUser();
		}
		clone.setUser(user);
		clone.setBaseURL(source.getBaseURL());
		clone.setProjectId(source.getProjectId());
		clone.setContextProjectId(source.getContextProjectId());
		String insightId = InsightStore.getInstance().put(clone);
		return new InsightHandle(clone, insightId, ThreadStore.getSessionId(), ThreadStore.getRouteId(),
				ThreadStore.getLocalHostname(), ThreadStore.getLocalProtocol(), ThreadStore.getLocalPort());
	}

	private static void seedThreadStore(String runId, InsightHandle insightHandle) {
		ThreadStore.setJobId(runId);
		if (insightHandle == null) {
			return;
		}
		ThreadStore.setInsightId(insightHandle.insightId);
		if (insightHandle.insight != null) {
			ThreadStore.setUser(insightHandle.insight.getUser());
		}
		ThreadStore.setSessionId(insightHandle.sessionId);
		ThreadStore.setRouteId(insightHandle.routeId);
		ThreadStore.setLocalHostname(insightHandle.localHostname);
		ThreadStore.setLocalProtocol(insightHandle.localProtocol);
		ThreadStore.setLocalPort(insightHandle.localPort);
	}

	private static final class InsightHandle {
		private final Insight insight;
		private final String insightId;
		private final String sessionId;
		private final String routeId;
		private final String localHostname;
		private final String localProtocol;
		private final Integer localPort;

		private InsightHandle(Insight insight, String insightId, String sessionId, String routeId, String localHostname,
				String localProtocol, Integer localPort) {
			this.insight = insight;
			this.insightId = insightId;
			this.sessionId = sessionId;
			this.routeId = routeId;
			this.localHostname = localHostname;
			this.localProtocol = localProtocol;
			this.localPort = localPort;
		}
	}
}
