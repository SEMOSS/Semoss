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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.ClaudeCodeAgentHarness;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.reactor.agent.stream.ClaudeCodeRunActivityAdapter;
import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.reactor.agent.subagent.SubAgentMeta;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.util.Utility;

public final class AgentRuntimeManager {

	private static final Logger logger = LogManager.getLogger(AgentRuntimeManager.class);
	private static final Gson GSON = new Gson();

	private static final int MAX_ERROR_LENGTH = 8000;
	private static final String WAIT_TIMEOUT_MS = "AGENT_RUN_WAIT_TIMEOUT_MS";
	private static final long DEFAULT_WAIT_TIMEOUT_MS = 3600000L;
	private static final AgentRuntimeManager INSTANCE = new AgentRuntimeManager(new AgentRunStore());

	private final AgentRunStore store;
	private final AgentRunQueueCoordinator queueCoordinator;
	private final AgentRunWorker worker;

	public static AgentRuntimeManager get() {
		return INSTANCE;
	}

	AgentRuntimeManager(AgentRunStore store) {
		this.store = store;
		this.queueCoordinator = new AgentRunQueueCoordinator(store);
		this.worker = new AgentRunWorker(this, store, queueCoordinator);
	}

	public RunAgentResult run(RunAgentRequest request) {
		String runId = resolveRunId(request.getInsight());
		return runWithId(runId, request);
	}

	public RunAgentResult runWithId(String runId, RunAgentRequest request) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		String resolvedRunId = runId.trim();
		if (store.runExists(resolvedRunId)) {
			throw new IllegalArgumentException("AGENT_RUN already exists for runId=" + resolvedRunId);
		}
		String userId = resolveUserId(request.getInsight());
		store.insertSubmitted(resolvedRunId, request, userId);
		if (supportsCanonicalStreaming(request.getHarnessType())) {
			AgentRunStreamService.get().register(resolvedRunId);
		}
		worker.rememberInsight(resolvedRunId, request.getInsight());
		worker.signal();
		return new RunAgentResult(resolvedRunId, request.getRoomId(), AgentRunStatus.SUBMITTED);
	}

	/**
	 * Wake up the agent run worker to scan for SUBMITTED runs. Called by
	 * {@code RunMCPToolReactor} after transitioning a run from
	 * {@code INPUT_REQUIRED} back to {@code SUBMITTED}.
	 */
	public void signalWorker() {
		worker.signal();
	}

	/**
	 * Wake up the worker and remember the insight for a resumed run. Called by
	 * {@code RunMCPToolReactor} which runs on the user's HTTP request thread
	 * and has a valid Insight. This ensures the worker can resume the run on
	 * this node without needing cross-node insight reconstruction.
	 */
	public void signalWorkerForResume(String runId, prerna.om.Insight insight) {
		if (runId != null && !runId.trim().isEmpty() && insight != null) {
			worker.rememberInsight(runId, insight);
			try {
				AgentRunRecord record = store.getRun(runId, insight);
				if (record != null && record.getRequest() != null
						&& isSemossHarness(record.getRequest().getHarnessType())) {
					AgentRunStreamService.get().register(runId);
				}
			} catch (Exception e) {
				// stream re-registration is best-effort
			}
		}
		worker.signal();
	}

	/**
	 * Resume a trace-authorized Automation run using its owner room while retaining
	 * the Automation editor's insight for normal model and tool authorization.
	 * Generic agent-run resumes must continue to use {@link #signalWorkerForResume}.
	 */
	public void signalWorkerForAutomationResume(String runId, Insight insight, Room ownerRoom) {
		if (runId == null || runId.trim().isEmpty() || insight == null || ownerRoom == null) {
			throw new IllegalArgumentException("Automation resume requires runId, insight, and owner room.");
		}
		worker.rememberAutomationResume(runId, insight, ownerRoom);
		try {
			AgentRunRecord record = store.getRunForAutomation(runId, insight);
			if (record != null && record.getRequest() != null
					&& isSemossHarness(record.getRequest().getHarnessType())) {
				AgentRunStreamService.get().register(runId);
			}
		} catch (Exception e) {
			// stream re-registration is best-effort
		}
		worker.signal();
	}

	public Map<String, Object> getRun(String runId, Insight insight) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		Map<String, Object> run = store.getRunMap(runId, insight);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		populatePendingActions(run, runId);
		return run;
	}

	public Map<String, Object> getRun(String runId, Insight insight, boolean includeMessages) {
		Map<String, Object> run = getRun(runId, insight);
		return includeMessages ? attachMessages(run, runId, resolveUserId(insight)) : run;
	}

	/**
	 * Returns an Automation trace-authorized run without applying the generic
	 * owner filter. Authorization is intentionally enforced by the dedicated
	 * Automation reactors before this method is invoked.
	 */
	public Map<String, Object> getRunForAutomation(String runId, Insight insight, boolean includeMessages) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		Map<String, Object> run = store.getRunMapForAutomation(runId);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		populatePendingActions(run, runId);
		if (includeMessages) {
			attachMessages(run, runId, trimToNull(run.get("userId")));
		}
		run.remove("userId");
		return run;
	}

	/**
	 * Always present, matching getRunSnapshot's contract - populated only when the run
	 * is paused for user input to approve/decline or open portal URLs. Best-effort: a
	 * failure here still returns a readable run with an empty pending-actions list.
	 */
	private static void populatePendingActions(Map<String, Object> run, String runId) {
		run.put("pendingActions", new ArrayList<>());
		if (AgentRunStatus.INPUT_REQUIRED.name().equals(String.valueOf(run.get("status")))) {
			try {
				run.put("pendingActions", normalizePendingActions(new AgentRunActionStore().getPendingActions(runId)));
			} catch (Exception e) {
				// best-effort - don't fail the caller over pending-action lookup
			}
		}
	}

	private static Map<String, Object> attachMessages(Map<String, Object> run, String runId, String userId) {
		String roomId = trimToNull(run.get("roomId"));
		Room room = roomId != null && userId != null ? ModelInferenceLogsUtils.getRoomById(roomId, userId) : null;
		List<Map<String, Object>> messages = room == null ? new ArrayList<>() : collectRunMessages(room, runId);
		if (ClaudeCodeAgentHarness.NAME.equalsIgnoreCase(trimToNull(run.get("harnessType")))) {
			messages = ClaudeCodeRunActivityAdapter.projectMessages(run, messages);
		}
		run.put("messages", messages);
		return run;
	}

	public Map<String, Object> waitForRun(String runId, Insight insight, long timeoutMs) throws InterruptedException {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		long effectiveTimeoutMs = timeoutMs > 0 ? timeoutMs : getLongProperty(WAIT_TIMEOUT_MS, DEFAULT_WAIT_TIMEOUT_MS);
		long deadline = System.currentTimeMillis() + effectiveTimeoutMs;
		while (true) {
			Map<String, Object> run = getRun(runId, insight);
			// isTerminalStatus also returns true for INPUT_REQUIRED, the synchronous
			// wait boundary: the run pauses for user input but is not itself terminal.
			if (isTerminalStatus(String.valueOf(run.get("status")))) {
				Map<String, Object> result = getRun(runId, insight, true);
				result.put("waitTimedOut", false);
				return result;
			}
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				Map<String, Object> result = getRun(runId, insight, true);
				result.put("waitTimedOut", true);
				return result;
			}
			Thread.sleep(Math.min(1000L, remaining));
		}
	}

	public Map<String, Object> stop(String runId, Insight insight) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		AgentRunRecord record = store.getRun(runId, insight);
		if (record == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		worker.cancel(runId);
		prerna.reactor.agent.AgentCancelHook.onStop(runId);
		if (store.markCancelledIfNotTerminal(runId, runId, "Agent run cancelled")) {
			notifyStreamCancelled(runId, "Agent run cancelled");
		}
		return getRun(runId, insight);
	}

	/**
	 * Stops an Automation trace-authorized run. The corresponding Automation
	 * reactor verifies editor permission and the exact durable trace before this
	 * owner-independent operation is reachable.
	 */
	public Map<String, Object> stopForAutomation(String runId, Insight insight) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		AgentRunRecord record = store.getRunForAutomation(runId, insight);
		if (record == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		worker.cancel(runId);
		prerna.reactor.agent.AgentCancelHook.onStop(runId);
		if (store.markCancelledIfNotTerminal(runId, runId, "Agent run cancelled by an Automation project editor")) {
			notifyStreamCancelled(runId, "Agent run cancelled by an Automation project editor");
		}
		return getRunForAutomation(runId, insight, false);
	}

	public boolean cancelRun(String runId, String roomId, String reason) {
		if (runId == null || runId.trim().isEmpty()) {
			return false;
		}
		String message = reason == null || reason.trim().isEmpty() ? "Agent run cancelled" : reason.trim();
		worker.cancel(runId);
		boolean cancelled = store.markCancelledIfNotTerminal(runId, runId, message);
		if (cancelled) {
			notifyStreamCancelled(runId, message);
		}
		return cancelled;
	}

	/**
	 * Snapshot of one run shaped for the agent streaming poll contract.
	 */
	public Map<String, Object> getRunSnapshot(String runId, Insight insight) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		Map<String, Object> run = store.getRunMap(runId, insight);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		Map<String, Object> snapshot = new HashMap<>();
		snapshot.put("runId", run.get("runId"));
		snapshot.put("roomId", run.get("roomId"));
		snapshot.put("status", run.get("status"));
		snapshot.put("inputMessageId", run.get("inputMessageId"));
		snapshot.put("finalOutputMessageId", run.get("finalOutputMessageId"));
		snapshot.put("finalText", run.get("finalText"));
		snapshot.put("errorMessage", run.get("errorMessage"));
		List<Map<String, Object>> pendingActions = new ArrayList<>();
		if (AgentRunStatus.INPUT_REQUIRED.name().equals(String.valueOf(run.get("status")))) {
			try {
				pendingActions = normalizePendingActions(new AgentRunActionStore().getPendingActions(runId));
			} catch (Exception e) {
				// best-effort - snapshot still carries the run status
			}
		}
		snapshot.put("pendingActions", pendingActions);
		return snapshot;
	}

	private static boolean isSemossHarness(String harnessType) {
		return harnessType == null || harnessType.trim().isEmpty()
				|| SemossAgentHarness.NAME.equalsIgnoreCase(harnessType.trim());
	}

	private static boolean supportsCanonicalStreaming(String harnessType) {
		return isSemossHarness(harnessType)
				|| ClaudeCodeAgentHarness.NAME.equalsIgnoreCase(trimToNull(harnessType));
	}

	private static void notifyStreamCancelled(String runId, String message) {
		AgentRunStreamService streams = AgentRunStreamService.get();
		streams.markTerminal(runId);
		SubAgentMeta meta = AgentSubAgentRegistry.getManager().lookup(runId);
		if (meta != null && meta.getParentJobId() != null && !meta.getParentJobId().isBlank()) {
			Map<String, Object> item = AgentStreamItems.subagentItem(runId, meta.getAlias(), meta.getChildRoomId(),
					meta.getWorkspaceId(), AgentRunStatus.CANCELLED.name());
			item.put("error", AgentStreamItems.truncate(message, AgentStreamItems.MAX_RESULT_PREVIEW_CHARS));
			streams.publishSubagentCompleted(meta.getParentJobId(), item);
		}
	}

	private static List<Map<String, Object>> normalizePendingActions(List<Map<String, Object>> actions) {
		if (actions == null) {
			return new ArrayList<>();
		}
		for (Map<String, Object> action : actions) {
			normalizeJsonField(action, "toolArgs");
			normalizeJsonField(action, "toolMeta");
			normalizeJsonField(action, "editedArgs");
		}
		return actions;
	}

	private static void normalizeJsonField(Map<String, Object> action, String key) {
		Object value = action.get(key);
		if (!(value instanceof String)) {
			return;
		}
		String json = ((String) value).trim();
		if (json.isEmpty()) {
			action.put(key, null);
			return;
		}
		try {
			Object parsed = GSON.fromJson(json, Object.class);
			action.put(key, parsed instanceof Map ? parsed : null);
		} catch (Exception e) {
			action.put(key, null);
			logger.warn("AgentRuntimeManager: malformed {} JSON on actionId={}", key, action.get("actionId"));
		}
	}

	boolean isCancelled(Throwable t) {
		Throwable cur = t;
		while (cur != null) {
			if (cur instanceof AgentCancelledException) {
				return true;
			}
			cur = cur.getCause();
		}
		return Thread.currentThread().isInterrupted();
	}

	String boundedError(Throwable t) {
		String message = t == null ? null : t.getMessage();
		if (message == null || message.trim().isEmpty()) {
			message = t == null ? "Unknown agent run failure" : t.getClass().getName();
		}
		if (message.length() <= MAX_ERROR_LENGTH) {
			return message;
		}
		return message.substring(0, MAX_ERROR_LENGTH);
	}

	private static long getLongProperty(String key, long defaultValue) {
		String value = Utility.getDIHelperProperty(key);
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			long parsed = Long.parseLong(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	private static String resolveUserId(Insight insight) {
		if (insight == null) {
			return null;
		}
		User user = insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			return null;
		}
		return user.getPrimaryLoginToken().getId();
	}

	private static List<Map<String, Object>> collectRunMessages(Room room, String runId) {
		List<AbstractMessage> runMessages = new ArrayList<>();
		for (AbstractMessage message : room.getMessages()) {
			if (message == null) {
				continue;
			}
			Object taggedRunId = message.getOrnament(SemossAgentHarness.ORNAMENT_AGENT_RUN_ID);
			if (taggedRunId != null && runId.equals(String.valueOf(taggedRunId))) {
				runMessages.add(message);
			}
		}
		return RoomUtils.getMessagesForClient(room, runMessages);
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}

	private String resolveRunId(Insight insight) {
		String threadJobId = ThreadStore.getJobId();
		if (threadJobId != null && !threadJobId.trim().isEmpty()) {
			String candidate = threadJobId.trim();
			if (!store.runExists(candidate)) {
				return candidate;
			}
		}
		return GUID.v7().toUUID().toString();
	}

	String firstNonBlank(String first, String second) {
		if (first != null && !first.trim().isEmpty()) {
			return first;
		}
		return second;
	}

	private static boolean isTerminalStatus(String status) {
		return AgentRunStatus.COMPLETED.name().equals(status)
				|| AgentRunStatus.FAILED.name().equals(status)
				|| AgentRunStatus.CANCELLED.name().equals(status)
				|| AgentRunStatus.INPUT_REQUIRED.name().equals(status);
	}

}
