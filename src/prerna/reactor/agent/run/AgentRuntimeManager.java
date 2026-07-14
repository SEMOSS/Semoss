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

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.util.Utility;

public final class AgentRuntimeManager {

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
		worker.rememberInsight(resolvedRunId, request.getInsight());
		worker.signal();
		return new RunAgentResult(resolvedRunId, request.getRoomId(), AgentRunStatus.SUBMITTED, null);
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
		run.put("roomRevision", store.getRoomRevision(String.valueOf(run.get("roomId")), insight));
		// Always expose a stable pendingActions collection. When the run is paused for
		// user input, populate it so clients can render approve/decline forms or URLs.
		run.put("pendingActions", new java.util.ArrayList<>());
		String status = String.valueOf(run.get("status"));
		if (AgentRunStatus.INPUT_REQUIRED.name().equals(status)) {
			try {
				AgentRunActionStore actionStore = new AgentRunActionStore();
				List<Map<String, Object>> pendingActions = actionStore.getPendingActions(runId);
				run.put("pendingActions", pendingActions);
			} catch (Exception e) {
				// best-effort — don't fail the getRun call
			}
		}
		return run;
	}

	/**
	 * Returns the current durable run snapshot and conditionally includes its
	 * ROOM.MESSAGES projection when the caller's room revision is stale.
	 */
	public Map<String, Object> getRun(String runId, Insight insight, boolean includeMessages,
			String knownRoomRevision) {
		Map<String, Object> run = getRun(runId, insight);
		if (!includeMessages) {
			return run;
		}
		String revision = normalizeRevision(run.get("roomRevision"));
		String knownRevision = normalizeRevision(knownRoomRevision);
		boolean messagesChanged = knownRevision == null || !knownRevision.equals(revision);
		run.put("messagesChanged", messagesChanged);
		if (messagesChanged) {
			run.put("messages", AgentRunMessageProjector.project(run, insight));
		}
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
			if (isWaitBoundaryStatus(String.valueOf(run.get("status")))) {
				run.put("waitTimedOut", false);
				return run;
			}
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				run.put("waitTimedOut", true);
				return run;
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
		store.markCancelledIfNotTerminal(runId, runId, "Agent run cancelled");
		return getRun(runId, insight);
	}

	public boolean cancelRun(String runId, String roomId, String reason) {
		if (runId == null || runId.trim().isEmpty()) {
			return false;
		}
		String message = reason == null || reason.trim().isEmpty() ? "Agent run cancelled" : reason.trim();
		worker.cancel(runId);
		return store.markCancelledIfNotTerminal(runId, runId, message);
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
				|| AgentRunStatus.CANCELLED.name().equals(status);
	}

	/**
	 * A synchronous waiter returns control when user input is required, while the
	 * durable run itself remains non-terminal and may resume under the same run id.
	 */
	private static boolean isWaitBoundaryStatus(String status) {
		return isTerminalStatus(status) || AgentRunStatus.INPUT_REQUIRED.name().equals(status);
	}

	private static String normalizeRevision(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() || "null".equalsIgnoreCase(text) ? null : text;
	}

}
