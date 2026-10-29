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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.AgentRunMessageContext;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.ClaudeCodeAgentHarness;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.reactor.agent.stream.AgentStreamItems;
import prerna.reactor.agent.stream.ClaudeCodeRunActivityAdapter;
import prerna.reactor.agent.subagent.AgentSubAgentRegistry;
import prerna.reactor.agent.subagent.SubAgentMeta;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Utility;
import prerna.util.ValueUtils;

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
	/** Why a run in a closed room is refused, or cancelled before it starts */
	static final String ROOM_CLOSED_MESSAGE = "Room is closed";
	private static final String ADMIN_CLOSED_MESSAGE = "Agent run cancelled: an admin closed the room";
	private static final String OWNER_CLOSED_MESSAGE = "Agent run cancelled: its user closed the room";
	/** How many reads closing a room makes at most while following its runs */
	private static final int MAX_ROOM_TREE_READS = 1000;
	private static final AgentRunService INSTANCE = new AgentRunService();

	private final AgentRunQueueLoop queueLoop;

	/**
	 * What closing a user's room covers.
	 *
	 * @param rooms each room it closes, the room itself first, with the user it
	 *              belongs to
	 * @param runs  the runs in those rooms and every run below them, the last found
	 *              first, so a child is cancelled before the run waiting on it
	 */
	record RoomTree(Map<String, String> rooms, List<Map<String, Object>> runs) {
	}

	/**
	 * What closing a user's room did.
	 *
	 * @param agentId         the agent the room's runs belong to
	 * @param closedRoomIds   the rooms closed: the room and its subagents' rooms
	 * @param cancelledRunIds the runs it cancelled; runs that had finished are left
	 *                        as they were
	 */
	public record RoomClosure(String agentId, List<String> closedRoomIds, List<String> cancelledRunIds) {
	}

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
		return submitNewRun(resolvedRunId, request, false);
	}

	private AgentRunHandle submitNewRun(String runId, AgentRunRequest request, boolean ownsUser) {
		String userId = resolveUserId(request.getInsight());
		// A guardrail, the room's user or an admin closed it; no agent runs in it
		// again.
		if (ModelInferenceLogsUtils.isRoomInActive(userId, request.getRoomId())) {
			throw new IllegalStateException(ROOM_CLOSED_MESSAGE);
		}
		AgentRunStore.insertSubmitted(runId, request, userId, resolveUserType(request.getInsight()));
		if (supportsCanonicalStreaming(request.getHarnessType())) {
			AgentRunStreamService.get().register(runId);
		}
		queueLoop.rememberInsight(runId, request.getInsight(), ownsUser);
		queueLoop.signal();
		return new AgentRunHandle(runId, request.getRoomId(), AgentRunStatus.SUBMITTED);
	}

	/**
	 * Submit deterministic server-owned work through the normal RunAgent queue
	 * path.
	 */
	boolean runBackgroundWithIdIfAbsent(String runId, AgentRunRequest request) {
		if (runId == null || runId.isBlank() || request == null || request.getInsight() == null) {
			throw new IllegalArgumentException("Background agent submission requires runId and an execution Insight");
		}
		String resolvedRunId = runId.trim();
		boolean submitted = false;
		try {
			if (!AgentRunStore.runExists(resolvedRunId)) {
				submitNewRun(resolvedRunId, request, true);
				submitted = true;
			}
		} catch (RuntimeException e) {
			// The room lease normally serializes this path. If a retry raced the first
			// insert, the deterministic run already represents the requested work.
			if (!AgentRunStore.runExists(resolvedRunId)) {
				throw e;
			}
		} finally {
			// A submitted run's handle owns the background user; otherwise release it here.
			if (!submitted) {
				request.getInsight().getUser().removeUserMemory();
			}
		}
		if (!submitted) {
			queueLoop.signal();
		}
		return submitted;
	}

	/**
	 * Creates the minimal authenticated context required by logged-out background
	 * work.
	 */
	static Insight createBackgroundExecutionInsight(String userId, AgentRunRequest request) {
		if (request == null || userId == null || userId.isBlank()) {
			throw new SecurityException("Agent run is missing its durable owner identity");
		}
		return createBackgroundExecutionInsight(userId, request.getOwnerAuthType(), request.getWorkspaceId());
	}

	static Insight createBackgroundExecutionInsight(String userId, String authType, String workspaceId) {
		if (userId == null || userId.isBlank()) {
			throw new SecurityException("Agent run is missing its durable owner identity");
		}
		if (authType == null || authType.isBlank()) {
			throw new SecurityException("Agent run is missing its durable owner authentication type");
		}
		AuthProvider provider = AuthProvider.getProviderFromString(authType);
		if (!provider.getLabel().equalsIgnoreCase(authType) && !provider.name().equalsIgnoreCase(authType)) {
			throw new SecurityException("Agent run owner authentication type is invalid");
		}
		if (!SecurityQueryUtils.isUserType(userId, provider)) {
			throw new SecurityException("Agent run owner no longer exists");
		}
		Object[] accountState = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset(userId, provider);
		if (accountState.length > 0 && Boolean.TRUE.equals(accountState[0])) {
			throw new SecurityException("Agent run owner is locked");
		}

		AccessToken token = new AccessToken();
		token.setProvider(provider);
		token.setId(userId);
		SecurityUserUtils.loadUserMetadata(token);
		User user = new User();
		user.setAccessToken(token);
		user.setPrimaryLogin(provider);

		Insight insight = new Insight();
		insight.setUser(user);
		if (workspaceId != null && !workspaceId.isBlank()) {
			insight.setProjectId(workspaceId);
			insight.setContextProjectId(workspaceId);
		}
		return insight;
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
	 * Queue one terminal child's single-message delivery behind its parent room
	 * turn.
	 */
	public void queueChildCompletion(String childRunId) {
		queueLoop.enqueueChildCompletion(childRunId);
	}

	/**
	 * Rebuild lightweight delivery work from durable child rows when a room is
	 * loaded.
	 */
	public void queueChildCompletionsForRoom(String parentRoomId, Insight insight) {
		try {
			String userId = resolveUserId(insight);
			if (userId == null) {
				return;
			}
			for (String childRunId : ChildRunCompletionService.findUndeliveredChildIdsForRoom(parentRoomId, userId)) {
				queueLoop.enqueueChildCompletion(childRunId);
			}
		} catch (Exception e) {
			logger.warn("Unable to queue child completions for roomId={}: {}", parentRoomId, e.getMessage(), e);
		}
		// Starts the loop, and with it the one-time recovery scan, on the first room
		// open.
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

	/**
	 * Resumes a trace-authorized Automation run with its persisted owner room while
	 * retaining the approving editor's insight for model and tool authorization.
	 *
	 * <p>
	 * Generic agent APIs must use {@link #signalWorkerForResume(String, Insight)}.
	 *
	 * @param runId     agent run identifier
	 * @param insight   approving Automation editor insight
	 * @param ownerRoom room loaded under the durable agent-run owner
	 */
	public void signalWorkerForAutomationResume(String runId, Insight insight, Room ownerRoom) {
		if (runId == null || runId.isBlank() || insight == null || ownerRoom == null) {
			throw new IllegalArgumentException("Automation resume requires runId, insight, and owner room.");
		}
		queueLoop.rememberAutomationResume(runId, insight, ownerRoom);
		try {
			AgentRunRecord record = AgentRunStore.getRunForAutomation(runId, insight);
			if (record != null && record.request() != null && isSemossHarness(record.request().getHarnessType())) {
				AgentRunStreamService.get().register(runId);
			}
		} catch (Exception e) {
			// Stream re-registration is best-effort.
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
		populatePendingActions(run, runId);
		return run;
	}

	public Map<String, Object> getRun(String runId, Insight insight, boolean includeMessages) {
		Map<String, Object> run = getRun(runId, insight);
		if (!includeMessages) {
			return run;
		}

		return attachMessages(run, runId, resolveUserId(insight));
	}

	/**
	 * Returns an agent run through Automation's trace-authorized access path.
	 *
	 * <p>
	 * The Automation reactor must verify project access and the exact persisted
	 * Automation run, node, and agent-run relationship before calling this method.
	 *
	 * @param runId           agent run identifier
	 * @param insight         current Automation editor insight
	 * @param includeMessages whether room messages should be included
	 * @return agent run details
	 */
	public Map<String, Object> getRunForAutomation(String runId, Insight insight, boolean includeMessages) {
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("runId is required");
		}
		Map<String, Object> run = AgentRunStore.getRunMapForAutomation(runId);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		populatePendingActions(run, runId);
		if (includeMessages) {
			attachMessages(run, runId, ValueUtils.trimToNull(run.get("userId")));
		}
		run.remove("userId");
		return run;
	}

	/**
	 * Returns any user's agent run, for the admin Agent Runs page. Its messages are
	 * read from its user's room, as the run's owner sees them.
	 *
	 * @param access          proof that the caller is an admin
	 * @param runId           agent run identifier
	 * @param includeMessages whether room messages should be included
	 * @return agent run details, with the user who ran it
	 * @throws SecurityException when {@code access} is missing
	 */
	public Map<String, Object> getRunForAdmin(AdminAccess access, String runId, boolean includeMessages) {
		AdminAccess.require(access);
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("runId is required");
		}
		String trimmed = runId.trim();
		Map<String, Object> run = AgentRunStore.getRunMapForAdmin(access, trimmed);
		if (run == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + trimmed);
		}
		populatePendingActions(run, trimmed);
		if (includeMessages) {
			attachMessages(run, trimmed, ValueUtils.trimToNull(run.get("userId")));
		}
		return run;
	}

	/**
	 * Closes any user's room as an admin, for the admin Agent Runs page, so that no
	 * agent runs in it again: every run in it and its subagents' rooms that has not
	 * finished is cancelled as a stop would, even while it executes, then the rooms
	 * are closed, then any run that started meanwhile is cancelled too. The user no
	 * longer sees the rooms in their conversations. The close is recorded in the
	 * audit trail as {@code AGENT_ROOM_CLOSE}, with the admin as its actor.
	 *
	 * @param admin    the user closing the room, who must be an admin
	 * @param roomId   the room to close
	 * @param userId   the user the room belongs to
	 * @param userType the login type of {@code userId}, or null to match the id
	 *                 under any type
	 * @return the rooms closed and the runs cancelled
	 * @throws SecurityException when {@code admin} is not an admin; nothing is read
	 *                           or closed
	 */
	public RoomClosure closeRoomForAdmin(User admin, String roomId, String userId, String userType) {
		AdminAccess.of(admin);
		RoomClosure closure = closeRoomTree(roomId, userId, userType, ADMIN_CLOSED_MESSAGE);
		UserAuditTrailUtils.recordAgentRoomClose(admin, closure.agentId(), roomId, userId, userType,
				closure.closedRoomIds(), closure.cancelledRunIds());
		return closure;
	}

	/**
	 * Closes one of the caller's own rooms so that no agent runs in it again, as an
	 * admin closing it would. The rooms and runs are the caller's own, by the id
	 * and login type of their primary login, so no one else's are touched.
	 *
	 * @param roomId  the caller's room to close
	 * @param insight the caller's insight
	 * @return the rooms closed and the runs cancelled
	 * @throws SecurityException when the caller is not signed in
	 */
	public RoomClosure closeRoom(String roomId, Insight insight) {
		String userId = resolveUserId(insight);
		if (userId == null || userId.isBlank()) {
			throw new SecurityException("Closing a room requires an authenticated user");
		}
		return closeRoomTree(roomId, userId, resolveUserType(insight), OWNER_CLOSED_MESSAGE);
	}

	/**
	 * Cancels every run of a user's room and its subagents' rooms that has not
	 * finished, closes the rooms, then cancels any run that started meanwhile. The
	 * runs are cancelled first because a run whose room closes under it fails on
	 * its next model call instead.
	 *
	 * @param roomId   the room to close
	 * @param userId   the user the room belongs to
	 * @param userType the login type of {@code userId}, or null to match the id
	 *                 under any type
	 * @param message  why the runs were cancelled, stored as their error
	 * @return the rooms closed and the runs cancelled
	 */
	private RoomClosure closeRoomTree(String roomId, String userId, String userType, String message) {
		RoomTree tree = roomTree(roomId, userId, userType);
		if (tree.runs().isEmpty()) {
			throw new IllegalArgumentException("No agent runs found in roomId=" + roomId + " for this user");
		}
		List<String> cancelledRunIds = cancelUnfinished(tree, message);
		List<String> closedRoomIds = new ArrayList<>();
		for (Map.Entry<String, String> room : tree.rooms().entrySet()) {
			if (!ModelInferenceLogsUtils.doSetRoomToInactive(room.getValue(), room.getKey())) {
				logger.warn("Unable to close roomId={}; its runs are still cancelled", room.getKey());
			} else if (ModelInferenceLogsUtils.isRoomInActive(room.getValue(), room.getKey())) {
				// a subagent's room with no messages yet has no row to close
				RoomMessageStore.evictRedisProjection(room.getKey());
				closedRoomIds.add(room.getKey());
			}
		}
		cancelledRunIds.addAll(cancelUnfinished(roomTree(roomId, userId, userType), message));
		logger.info("Closed roomId={} ({}): {} rooms closed, {} runs cancelled", roomId, message, closedRoomIds.size(),
				cancelledRunIds.size());
		return new RoomClosure(agentIdOf(roomId.trim(), tree.runs()), closedRoomIds, cancelledRunIds);
	}

	/**
	 * Cancels the runs of a room tree that have not finished.
	 *
	 * @param tree    the rooms and runs
	 * @param message why the runs were cancelled, stored as their error
	 * @return the runs this call cancelled
	 */
	private List<String> cancelUnfinished(RoomTree tree, String message) {
		List<String> cancelled = new ArrayList<>();
		for (Map<String, Object> run : tree.runs()) {
			String runId = ValueUtils.trimToNull(run.get("runId"));
			if (runId != null && isUnfinished(run)
					&& cancelAndRelease(runId, ValueUtils.trimToNull(run.get("parentRunId")), message)) {
				cancelled.add(runId);
			}
		}
		return cancelled;
	}

	/**
	 * Collects what closing a user's room covers: the runs in the room, every run
	 * below them wherever it runs, and the runs in each room they reach, such as a
	 * continuation a subagent's result started in its own room.
	 *
	 * @param roomId   the room to close
	 * @param userId   the user the room belongs to
	 * @param userType the login type of {@code userId}, or null to match the id
	 *                 under any type
	 * @return the rooms and runs it covers; no runs when the user has none in the
	 *         room
	 */
	static RoomTree roomTree(String roomId, String userId, String userType) {
		String owner = userId.trim();
		String type = ValueUtils.trimToNull(userType);
		Map<String, String> rooms = new LinkedHashMap<>();
		rooms.put(roomId.trim(), owner);
		Deque<String> roomsToRead = new ArrayDeque<>(List.of(roomId.trim()));
		Deque<String> runsToFollow = new ArrayDeque<>();
		List<Map<String, Object>> found = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		int reads = 0;
		while ((!roomsToRead.isEmpty() || !runsToFollow.isEmpty()) && reads < MAX_ROOM_TREE_READS) {
			reads++;
			List<Map<String, Object>> reached = roomsToRead.isEmpty()
					? AgentRunStore.getSubagentRunsOfAnyUser(runsToFollow.poll())
					: AgentRunStore.getUserRunsForRoom(roomsToRead.poll(), owner, type);
			for (Map<String, Object> run : reached) {
				String runId = ValueUtils.trimToNull(run.get("runId"));
				if (runId == null || !isOwnersRun(run, owner, type) || !seen.add(runId)) {
					continue;
				}
				found.add(run);
				runsToFollow.add(runId);
				String runRoomId = ValueUtils.trimToNull(run.get("roomId"));
				if (runRoomId != null && rooms.putIfAbsent(runRoomId, owner) == null) {
					roomsToRead.add(runRoomId);
				}
			}
		}
		if (reads >= MAX_ROOM_TREE_READS) {
			logger.warn("Closing roomId={} stopped following its runs after {} reads", roomId, reads);
		}
		List<Map<String, Object>> lastFoundFirst = new ArrayList<>(found);
		Collections.reverse(lastFoundFirst);
		return new RoomTree(rooms, lastFoundFirst);
	}

	/**
	 * Whether a run is the room owner's: a subagent runs as the user who started
	 * its parent, so any other user's run is never theirs to close. A run that
	 * stores no login type matches the id alone.
	 */
	private static boolean isOwnersRun(Map<String, Object> run, String owner, String type) {
		String runType = ValueUtils.trimToNull(run.get("userType"));
		return owner.equals(ValueUtils.trimToNull(run.get("userId")))
				&& (type == null || runType == null || type.equals(runType));
	}

	/**
	 * Whether a run read as a map has not finished: submitted, running or waiting
	 * for input
	 */
	private static boolean isUnfinished(Map<String, Object> run) {
		String status = ValueUtils.trimToNull(run.get("status"));
		return AgentRunStatus.SUBMITTED.name().equals(status) || AgentRunStatus.RUNNING.name().equals(status)
				|| AgentRunStatus.INPUT_REQUIRED.name().equals(status);
	}

	/**
	 * The agent of a room's runs, chosen as the admin room list chooses it: the
	 * greatest agent id of the runs in the room.
	 */
	private static String agentIdOf(String roomId, List<Map<String, Object>> runs) {
		String agentId = null;
		for (Map<String, Object> run : runs) {
			String runAgentId = ValueUtils.trimToNull(run.get("workspaceId"));
			if (runAgentId != null && roomId.equals(ValueUtils.trimToNull(run.get("roomId")))
					&& (agentId == null || runAgentId.compareTo(agentId) > 0)) {
				agentId = runAgentId;
			}
		}
		return agentId;
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
		cancelAndRelease(runId, record.request() == null ? null : record.request().getParentRunId(),
				"Agent run cancelled");
		return getRun(runId, insight);
	}

	/**
	 * Cancels a run on behalf of someone stopping it, and frees what it holds on
	 * this node.
	 *
	 * @param runId       the run to cancel
	 * @param parentRunId the run that spawned it, or null for a root run
	 * @param message     why the run was cancelled, stored as its error
	 * @return {@code true} when this call moved the run to CANCELLED, {@code false}
	 *         when it had already settled
	 */
	private boolean cancelAndRelease(String runId, String parentRunId, String message) {
		// Frees the room even when the run's thread cannot be interrupted out of a
		// blocking call. A run queued here or executing on another node has nothing to
		// free locally; marking it cancelled below is what stops it.
		queueLoop.cancel(runId);
		prerna.reactor.agent.AgentCancelHook.onStop(runId);
		if (!AgentRunStore.markCancelledIfNotTerminal(runId, runId, message)) {
			return false;
		}
		notifyStreamCancelled(runId, message);
		// Approvals the stopped run was waiting on, or the person's open request.
		AgentRunActionStore.cancelPendingForRun(runId);
		// A child cancelled while idle has no executor to report it.
		if (parentRunId != null) {
			queueChildCompletion(runId);
		}
		return true;
	}

	/**
	 * Stops a run through Automation's trace-authorized access path.
	 *
	 * <p>
	 * The Automation reactor must verify project edit access and the exact
	 * persisted trace before invoking this owner-independent operation.
	 *
	 * @param runId   agent run identifier
	 * @param insight current Automation editor insight
	 * @return terminal agent run details
	 */
	public Map<String, Object> stopForAutomation(String runId, Insight insight) {
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("runId is required");
		}
		AgentRunRecord record = AgentRunStore.getRunForAutomation(runId, insight);
		if (record == null) {
			throw new IllegalArgumentException("No AGENT_RUN found for runId=" + runId);
		}
		queueLoop.cancel(runId);
		prerna.reactor.agent.AgentCancelHook.onStop(runId);
		String message = "Agent run cancelled by an Automation project editor";
		if (AgentRunStore.markCancelledIfNotTerminal(runId, runId, message)) {
			notifyStreamCancelled(runId, message);
			AgentRunActionStore.cancelPendingForRun(runId);
		}
		return getRunForAutomation(runId, insight, false);
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
			AgentRunActionStore.cancelPendingForRun(runId);
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
		if (run.containsKey("progress")) {
			snapshot.put("progress", run.get("progress"));
		}
		for (String key : List.of("artifacts", "reviewOutcome", "warnings")) {
			if (run.containsKey(key)) {
				snapshot.put(key, run.get(key));
			}
		}
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
		return isSemossHarness(harnessType)
				|| ClaudeCodeAgentHarness.NAME.equalsIgnoreCase(ValueUtils.trimToNull(harnessType));
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

	private static void populatePendingActions(Map<String, Object> run, String runId) {
		run.put("pendingActions", new ArrayList<>());
		if (!AgentRunStatus.INPUT_REQUIRED.name().equals(String.valueOf(run.get("status")))) {
			return;
		}
		try {
			run.put("pendingActions", normalizePendingActions(AgentRunActionStore.getPendingActions(runId)));
		} catch (Exception e) {
			// Pending-action enrichment is best-effort.
		}
	}

	private static Map<String, Object> attachMessages(Map<String, Object> run, String runId, String userId) {
		String roomId = ValueUtils.trimToNull(run.get("roomId"));
		Room room = roomId != null && userId != null ? ModelInferenceLogsUtils.getRoomById(roomId, userId) : null;
		List<Map<String, Object>> messages = room == null ? new ArrayList<>() : collectRunMessages(room, runId);
		if (ClaudeCodeAgentHarness.NAME.equalsIgnoreCase(ValueUtils.trimToNull(run.get("harnessType")))) {
			messages = ClaudeCodeRunActivityAdapter.projectMessages(run, messages);
		}
		run.put("messages", messages);
		return run;
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

	/**
	 * Reads the login type of the id {@link #resolveUserId} returns, from the same
	 * primary login token.
	 *
	 * @param insight the caller's insight
	 * @return the primary login's type, or null when there is no primary login
	 */
	private static String resolveUserType(Insight insight) {
		if (insight == null || insight.getUser() == null) {
			return null;
		}
		AccessToken token = insight.getUser().getPrimaryLoginToken();
		return token == null || token.getProvider() == null ? null : token.getProvider().getLabel();
	}

	private static List<Map<String, Object>> collectRunMessages(Room room, String runId) {
		List<AbstractMessage> runMessages = new ArrayList<>();
		for (AbstractMessage message : room.getMessages()) {
			if (message == null) {
				continue;
			}
			AgentRunMessageContext agentRun = message.getAgentRun();
			if (agentRun != null && runId.equals(agentRun.getRunId())) {
				runMessages.add(message);
			}
		}
		return RoomUtils.getMessagesForClient(room, runMessages);
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
