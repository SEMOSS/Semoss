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
import prerna.util.Utility;

/**
 * The entry point for agent runs. Reactors, REST resources, and subagent
 * spawning all go through this singleton rather than touching the store or the
 * queue loop directly.
 *
 * <p>
 * Submitting is asynchronous: {@link #run} and {@link #runWithId} persist an
 * {@code AGENT_RUN} row, wake the queue loop, and return an
 * {@link AgentRunHandle} immediately, so the caller's HTTP request is never
 * held open for the length of an agent run. Callers that do want to block ask
 * for it explicitly with {@link #waitForRun}, which polls until the run settles
 * or the timeout expires.
 *
 * <p>
 * {@link #stop} and {@link #cancelRun} both end a run. They differ in who is
 * asking: {@code stop} serves a user request and validates the run exists
 * first, while {@code cancelRun} serves the platform cancelling on its own
 * behalf, such as a parent run cascading a cancel to its subagents. Either way
 * the durable status is what stops a run executing on another node.
 */
public final class AgentRunService {

	private static final Logger logger = LogManager.getLogger(AgentRunService.class);
	private static final Gson GSON = new Gson();

	private static final String WAIT_TIMEOUT_MS = "AGENT_RUN_WAIT_TIMEOUT_MS";
	private static final long DEFAULT_WAIT_TIMEOUT_MS = 3600000L;
	private static final AgentRunService INSTANCE = new AgentRunService();

	private final AgentRunQueueLoop queueLoop;

	public static AgentRunService get() {
		return INSTANCE;
	}

	private AgentRunService() {
		this.queueLoop = new AgentRunQueueLoop();
	}

	public AgentRunHandle run(AgentRunRequest request) {
		String runId = resolveRunId(request.getInsight());
		return runWithId(runId, request);
	}

	public AgentRunHandle runWithId(String runId, AgentRunRequest request) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		String resolvedRunId = runId.trim();
		if (AgentRunStore.runExists(resolvedRunId)) {
			throw new IllegalArgumentException("AGENT_RUN already exists for runId=" + resolvedRunId);
		}
		String userId = resolveUserId(request.getInsight());
		AgentRunStore.insertSubmitted(resolvedRunId, request, userId);
		if (supportsCanonicalStreaming(request.getHarnessType())) {
			AgentRunStreamService.get().register(resolvedRunId);
		}
		queueLoop.rememberInsight(resolvedRunId, request.getInsight());
		queueLoop.signal();
		return new AgentRunHandle(resolvedRunId, request.getRoomId(), AgentRunStatus.SUBMITTED);
	}

	/**
	 * Wake up the queue loop to scan for SUBMITTED runs. Called by
	 * {@code RunMCPToolReactor} after transitioning a run from
	 * {@code INPUT_REQUIRED} back to {@code SUBMITTED}.
	 */
	public void signalWorker() {
		queueLoop.signal();
	}

	/**
	 * Wake up the queue loop and remember the insight for a resumed run. Called by
	 * {@code RunMCPToolReactor} which runs on the user's HTTP request thread and
	 * has a valid Insight. This ensures the queue loop can resume the run on this
	 * node without needing cross-node insight reconstruction.
	 */
	public void signalWorkerForResume(String runId, prerna.om.Insight insight) {
		if (runId != null && !runId.trim().isEmpty() && insight != null) {
			queueLoop.rememberInsight(runId, insight);
			try {
				AgentRunRecord record = AgentRunStore.getRun(runId, insight);
				if (record != null && record.request() != null && isSemossHarness(record.request().getHarnessType())) {
					AgentRunStreamService.get().register(runId);
				}
			} catch (Exception e) {
				// stream re-registration is best-effort
			}
		}
		queueLoop.signal();
	}

	public Map<String, Object> getRun(String runId, Insight insight) {
		if (runId == null || runId.trim().isEmpty()) {
			throw new IllegalArgumentException("runId is required");
		}
		Map<String, Object> run = AgentRunStore.getRunMap(runId, insight);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		// Always present, matching getRunSnapshot's contract - populated only
		// when the run is paused for user input to approve/decline or open portal URLs.
		run.put("pendingActions", new ArrayList<>());
		String status = String.valueOf(run.get("status"));
		if (AgentRunStatus.INPUT_REQUIRED.name().equals(status)) {
			try {
				List<Map<String, Object>> pendingActions = AgentRunActionStore.getPendingActions(runId);
				run.put("pendingActions", normalizePendingActions(pendingActions));
			} catch (Exception e) {
				// best-effort - don't fail the getRun call
			}
		}
		return run;
	}

	public Map<String, Object> getRun(String runId, Insight insight, boolean includeMessages) {
		Map<String, Object> run = getRun(runId, insight);
		if (!includeMessages) {
			return run;
		}

		String roomId = trimToNull(run.get("roomId"));
		String userId = resolveUserId(insight);
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
		AgentRunRecord record = AgentRunStore.getRun(runId, insight);
		if (record == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		// Frees the room even when the run's thread cannot be interrupted out of a
		// blocking call. A run queued here or executing on another node has nothing to
		// free locally; marking it cancelled below is what stops it.
		queueLoop.cancel(runId);
		prerna.reactor.agent.AgentCancelHook.onStop(runId);
		if (AgentRunStore.markCancelledIfNotTerminal(runId, runId, "Agent run cancelled")) {
			notifyStreamCancelled(runId, "Agent run cancelled");
		}
		return getRun(runId, insight);
	}

	/**
	 * Cancels a run without the caller-facing checks {@link #stop} performs. Used
	 * for cascades, where the run is being cancelled on the platform's behalf
	 * rather than a user's.
	 *
	 * @return {@code true} when this call moved the run to CANCELLED, {@code false}
	 *         when it had already settled
	 */
	public boolean cancelRun(String runId, String reason) {
		if (runId == null || runId.trim().isEmpty()) {
			return false;
		}
		String message = reason == null || reason.trim().isEmpty() ? "Agent run cancelled" : reason.trim();
		queueLoop.cancel(runId);
		boolean cancelled = AgentRunStore.markCancelledIfNotTerminal(runId, runId, message);
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
		Map<String, Object> run = AgentRunStore.getRunMap(runId, insight);
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
				pendingActions = normalizePendingActions(AgentRunActionStore.getPendingActions(runId));
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
		return isSemossHarness(harnessType) || ClaudeCodeAgentHarness.NAME.equalsIgnoreCase(trimToNull(harnessType));
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
			logger.warn("AgentRunService: malformed {} JSON on actionId={}", key, action.get("actionId"));
		}
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
			if (!AgentRunStore.runExists(candidate)) {
				return candidate;
			}
		}
		return GUID.v7().toUUID().toString();
	}

	private static boolean isTerminalStatus(String status) {
		return AgentRunStatus.COMPLETED.name().equals(status) || AgentRunStatus.FAILED.name().equals(status)
				|| AgentRunStatus.CANCELLED.name().equals(status)
				|| AgentRunStatus.INPUT_REQUIRED.name().equals(status);
	}

}
