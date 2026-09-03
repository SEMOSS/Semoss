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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.logging.SemossLogUtils;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.reactor.agent.subagent.SubAgentMeta;

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
	private final Map<String, Room> automationResumeRoomsByRun = new ConcurrentHashMap<>();
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
		insightsByRun.put(runId, cloneInsight(runId, insight));
	}

	void rememberAutomationResume(String runId, Insight insight, Room ownerRoom) {
		if (runId == null || insight == null || ownerRoom == null) {
			return;
		}
		automationResumeRoomsByRun.put(runId, ownerRoom);
		rememberInsight(runId, insight);
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
			final AgentRunQueueCoordinator.ActiveRunLease claimedLease = lease;
			Thread thread = Thread.ofVirtual().name("agent-run-" + runId).unstarted(() -> {
				try (var ignored = CloseableThreadContext.putAll(insightHandle.log4jContextMap)) {
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
		String parentRunId = record.getRequest() != null ? record.getRequest().getParentRunId() : null;
		try {
			seedThreadStore(runId, insightHandle);
			publishSubagentPatch(parentRunId, runId, AgentRunStatus.RUNNING);
			RunAgentRequest request = record.getRequest();
			// Detect resume: the persisted request always has resumeMode=false on initial
			// submission, so fall back to checking for existing AGENT_RUN_ACTION rows.
			boolean resumeMode = request.isResumeMode() || new AgentRunActionStore().hasAnyActions(runId);
			Room automationResumeRoom = automationResumeRoomsByRun.remove(runId);
			AgentHarnessResult result = automationResumeRoom == null
					? AgentRunner.run(request.getRoomId(), request.getInput(), request.getEngineIdFallback(),
							request.getHarnessType(), request.getMaxTurns(), request.getMaxReflections(),
							request.getParamMap(), request.getAgentParamMap(), request.getMediaInputPaths(),
							request.getMediaUrls(), runId, insightHandle.insight, resumeMode)
					: AgentRunner.resumeAutomationRun(request.getRoomId(), request.getInput(),
							request.getEngineIdFallback(), request.getHarnessType(), request.getMaxTurns(),
							request.getMaxReflections(), request.getParamMap(), request.getAgentParamMap(),
							request.getMediaInputPaths(), request.getMediaUrls(), runId, insightHandle.insight,
							automationResumeRoom);
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
			AgentRunStreamService.get().markTerminal(runId);
			publishSubagentTerminal(parentRunId, record, runId, AgentRunStatus.COMPLETED,
					result != null ? result.getFinalText() : null, null);
		} catch (Exception e) {
			jobId = runtime.firstNonBlank(ThreadStore.getJobId(), jobId);
			if (runtime.isCancelled(e)) {
				store.markCancelled(runId, jobId, runtime.boundedError(e));
				AgentRunStreamService.get().markTerminal(runId);
				publishSubagentTerminal(parentRunId, record, runId, AgentRunStatus.CANCELLED, null,
						runtime.boundedError(e));
			} else if (e instanceof prerna.reactor.agent.exceptions.AgentInputRequiredException) {
				// Harness paused on SMSS_MCP_EXECUTION=ask tools.
				// The harness already persisted the AGENT_RUN_ACTION rows; only
				// transition the durable run status here.
				store.markInputRequired(runId, jobId);
				publishSubagentPatch(parentRunId, runId, AgentRunStatus.INPUT_REQUIRED);
				logger.info("AgentRunWorker: runId={} paused for user input", runId);
			} else {
				store.markFailed(runId, jobId, runtime.boundedError(e));
				AgentRunStreamService.get().markTerminal(runId);
				publishSubagentTerminal(parentRunId, record, runId, AgentRunStatus.FAILED, null,
						runtime.boundedError(e));
			}
			logger.warn("AgentRunWorker: runId={} failed: {}", runId, e.getMessage(), e);
		} finally {
			ThreadStore.remove();
			cleanupInsight(runId, insightHandle);
		}
	}

	private static void publishSubagentPatch(String parentRunId, String childRunId, AgentRunStatus status) {
		if (parentRunId == null || parentRunId.isBlank()) {
			return;
		}
		Map<String, Object> patch = new HashMap<>();
		patch.put("status", status.name());
		AgentRunStreamService.get().publishSubagentUpdated(parentRunId, childRunId, patch);
	}

	private static void publishSubagentTerminal(String parentRunId, AgentRunRecord record, String runId,
			AgentRunStatus status, String resultPreview, String error) {
		if (parentRunId == null || parentRunId.isBlank()) {
			return;
		}
		String alias = null;
		String workspaceId = record.getRequest() != null ? record.getRequest().getWorkspaceId() : null;
		SubAgentMeta meta = AgentSubAgentRegistry.getManager().lookup(runId);
		if (meta != null) {
			alias = meta.getAlias();
			if (meta.getWorkspaceId() != null) {
				workspaceId = meta.getWorkspaceId();
			}
		}
		Map<String, Object> item = AgentStreamItems.subagentItem(runId, alias, record.getRoomId(), workspaceId,
				status.name());
		if (resultPreview != null && !resultPreview.isBlank()) {
			item.put("resultPreview",
					AgentStreamItems.truncate(resultPreview, AgentStreamItems.MAX_RESULT_PREVIEW_CHARS));
		}
		if (error != null && !error.isBlank()) {
			item.put("error", AgentStreamItems.truncate(error, AgentStreamItems.MAX_RESULT_PREVIEW_CHARS));
		}
		AgentRunStreamService.get().publishSubagentCompleted(parentRunId, item);
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
		automationResumeRoomsByRun.remove(runId);
		InsightHandle removed = insightsByRun.remove(runId);
		InsightHandle toCleanup = removed != null ? removed : insightHandle;
		if (toCleanup != null && toCleanup.insightId != null) {
			InsightStore.getInstance().remove(toCleanup.insightId);
		}
	}

	private static InsightHandle cloneInsight(String runId, Insight source) {
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
		Map<String, String> log4jContextMap = captureLog4jContext(runId, user);
		String sessionId = ThreadStore.getSessionId();
		if (sessionId == null || sessionId.trim().isEmpty()) {
			sessionId = log4jContextMap.get(SemossLogUtils.SESSION_ID);
		}
		return new InsightHandle(clone, insightId, sessionId, ThreadStore.getRouteId(),
				ThreadStore.getLocalHostname(), ThreadStore.getLocalProtocol(), ThreadStore.getLocalPort(),
				log4jContextMap);
	}

	private static Map<String, String> captureLog4jContext(String runId, User user) {
		Map<String, String> context = new HashMap<>(ThreadContext.getImmutableContext());
		putIfBlank(context, SemossLogUtils.REQUEST_ID, runId);
		putIfBlank(context, SemossLogUtils.SESSION_ID, ThreadStore.getSessionId());

		if (user != null && user.getPrimaryLoginToken() != null) {
			var token = user.getPrimaryLoginToken();
			putIfBlank(context, SemossLogUtils.USER_ID, token.getId());
			String userName = token.getResolvedDisplayName();
			if (userName == null || userName.trim().isEmpty()) {
				userName = token.getName();
			}
			if (userName == null || userName.trim().isEmpty()) {
				userName = token.getUsername();
			}
			if (userName == null || userName.trim().isEmpty()) {
				userName = token.getEmail();
			}
			putIfBlank(context, SemossLogUtils.USER_NAME, userName);
			if (token.getProvider() != null) {
				putIfBlank(context, SemossLogUtils.USER_TYPE, token.getProvider().getLabel());
			}
		}
		return context;
	}

	private static void putIfBlank(Map<String, String> context, String key, String value) {
		if (value == null || value.trim().isEmpty()) {
			return;
		}
		String existing = context.get(key);
		if (existing == null || existing.trim().isEmpty()) {
			context.put(key, value);
		}
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
		private final Map<String, String> log4jContextMap;

		private InsightHandle(Insight insight, String insightId, String sessionId, String routeId, String localHostname,
				String localProtocol, Integer localPort, Map<String, String> log4jContextMap) {
			this.insight = insight;
			this.insightId = insightId;
			this.sessionId = sessionId;
			this.routeId = routeId;
			this.localHostname = localHostname;
			this.localProtocol = localProtocol;
			this.localPort = localPort;
			this.log4jContextMap = log4jContextMap;
		}
	}
}
