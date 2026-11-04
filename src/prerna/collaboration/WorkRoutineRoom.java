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
package prerna.collaboration;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.reactor.agent.runtime.AgentCodeExecutionContext;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.run.AutomationRunStore;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * The owner's routine runs: their history, the latest runs for the sidebar, deleting one, and opening one completed
 * output, in the room its agent worked in or, for a routine with no agent, a private chat that starts with it.
 */
public final class WorkRoutineRoom {
	private static final int MAX_RUNS = 30;
	// the sidebar shows the routines that ran last, each with its latest runs
	private static final int RECENT_ROUTINES = 5;
	private static final int RECENT_RUNS = 2;
	private static final Set<String> FAILED = Set.of(AutomationConstants.STATUS_FAILED,
			AutomationConstants.STATUS_INTERRUPTED);

	private WorkRoutineRoom() { }

	// a routine id names its kind and its row
	private record Target(WorkRoutine.Kind kind, String customId, Pair<String, String> owner, Map<String, Object> row) {
		WorkRoutine.Slot slot() {
			return new WorkRoutine.Slot(kind, owner, customId);
		}

		// the chat one run's output opens in, from History, the sidebar or Latest
		String runRoom(String runId) {
			return CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-run-discussion",
					kind.id(), customId == null ? "" : customId, runId);
		}

		// the chat Latest opened by the row's start, before it opened the run's own
		String latestRoom(String started) {
			return customId == null
					? CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-discussion",
							kind.id(), started)
					: CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-discussion",
							kind.id(), customId, started);
		}
	}

	private static Target target(Insight insight, String routineId) {
		WorkRoutine.Kind kind;
		// any other id names one of the owner's own routines
		String customId = null;
		if (WorkInboxRoutine.ID.equals(routineId)) {
			kind = WorkInboxRoutine.KIND;
		} else if (WorkAgendaRoutine.ID.equals(routineId)) {
			kind = WorkAgendaRoutine.KIND;
		} else if (WorkEventPlanningRoutine.ID.equals(routineId)) {
			kind = WorkEventPlanningRoutine.KIND;
		} else if (WorkExpenseRoutine.ID.equals(routineId)) {
			kind = WorkExpenseRoutine.KIND;
		} else if (routineId != null && !routineId.isBlank()) {
			kind = WorkCustomRoutine.KIND;
			customId = routineId.trim();
		} else {
			throw new IllegalArgumentException("Unknown routine");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		Map<String, Object> row = WorkRoutine.stored(new WorkRoutine.Slot(kind, owner, customId));
		if (row == null && customId != null) {
			throw new IllegalArgumentException("Unknown routine");
		}
		return new Target(kind, customId, owner, row);
	}

	/** The routine's past runs for the signed-in owner, newest first, from the scheduler history. */
	public static List<Map<String, Object>> runs(Insight insight, String routineId) {
		WorkRoutine.Slot slot = target(insight, routineId).slot();
		List<Map<String, Object>> runs = new ArrayList<>();
		for (Map<String, Object> run : AutomationRunStore.getRunsForAutomation(
				AutomationProjectService.inlineRunScope(insight.getUser()), slot.automationId(), MAX_RUNS)) {
			Timestamp started = (Timestamp) run.get(AutomationConstants.STARTED_AT);
			Timestamp finished = (Timestamp) run.get(AutomationConstants.COMPLETED_AT);
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("runId", run.get(AutomationConstants.RUN_ID));
			item.put("startedAt", CollaborationDbUtils.toIso(started));
			item.put("finishedAt", CollaborationDbUtils.toIso(finished));
			item.put("status", status(run));
			item.put("trigger", AutomationConstants.TRIGGER_MANUAL.equals(run.get(AutomationConstants.TRIGGER_TYPE))
					? "manual" : "scheduled");
			item.put("durationMs", started == null || finished == null ? null : finished.getTime() - started.getTime());
			// a run that failed after its result node still has output to discuss, so the history checks the node
			item.put("hasOutput", output(slot, (String) run.get(AutomationConstants.RUN_ID)) != null);
			runs.add(item);
		}
		return runs;
	}

	/**
	 * The owner's routines that ran last, newest first, each with its latest runs: the routine id, a custom routine's
	 * name (null for the system routines, which the client names), and each run's id, status and times. A running run
	 * of the owner's own routine also names its room, where it can be watched.
	 */
	public static List<Map<String, Object>> recentRuns(Insight insight) {
		User user = insight.getUser();
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		// the rows say when each routine last started, so only the routines shown read their run history
		List<Last> last = new ArrayList<>();
		for (WorkRoutine.Slot slot : slots(owner)) {
			Map<String, Object> row = WorkRoutine.stored(slot);
			if (row != null && row.get("startedAt") instanceof String startedAt) {
				last.add(new Last(slot, row, Instant.parse(startedAt)));
			}
		}
		last.sort(Comparator.comparing(Last::startedAt).reversed());
		String scope = AutomationProjectService.inlineRunScope(user);
		List<Map<String, Object>> recent = new ArrayList<>();
		for (Last routine : last.subList(0, Math.min(RECENT_ROUTINES, last.size()))) {
			WorkRoutine.Slot slot = routine.slot();
			if ("running".equals(routine.row().get("status"))) {
				// a run that ended or went stale reads as it does on Routines, never as running forever
				WorkRoutine.settle(slot);
			}
			List<Map<String, Object>> runs = new ArrayList<>();
			for (Map<String, Object> run : AutomationRunStore.getRunsForAutomation(scope, slot.automationId(),
					RECENT_RUNS)) {
				Timestamp started = (Timestamp) run.get(AutomationConstants.STARTED_AT);
				if (started == null) {
					continue;
				}
				String runId = (String) run.get(AutomationConstants.RUN_ID);
				String status = status(run);
				Map<String, Object> item = new LinkedHashMap<>();
				item.put("runId", runId);
				item.put("status", status);
				item.put("startedAt", CollaborationDbUtils.toIso(started));
				item.put("finishedAt", CollaborationDbUtils.toIso((Timestamp) run.get(AutomationConstants.COMPLETED_AT)));
				// only the owner's own routines run in a room Collaboration opens
				item.put("roomId", "running".equals(status) && slot.routineId() != null ? roomOf(runId) : null);
				runs.add(item);
			}
			if (runs.isEmpty()) {
				continue;
			}
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("routineId", slot.routineId() == null ? slot.kind().id() : slot.routineId());
			item.put("name", slot.routineId() != null && routine.row().get("settings") instanceof Map<?, ?> settings
					&& settings.get("name") instanceof String name && !name.isBlank() ? name : null);
			item.put("runs", runs);
			recent.add(item);
		}
		return recent;
	}

	// one routine's row, ranked by when the routine last started
	private record Last(WorkRoutine.Slot slot, Map<String, Object> row, Instant startedAt) { }

	/** An automation run's status in the words Routines uses. */
	private static String status(Map<String, Object> run) {
		String status = String.valueOf(run.get(AutomationConstants.STATUS));
		return AutomationConstants.STATUS_SUCCESS.equals(status) ? "completed"
				: AutomationConstants.STATUS_CANCELLED.equals(status) ? "stopped"
				: FAILED.contains(status) ? "failed" : "running";
	}

	/** Stops the owner's running run of the routine, so it can be run again. */
	public static void stopRun(Insight insight, String routineId) {
		WorkRoutine.stop(target(insight, routineId).slot());
	}

	/** The room a run's agent works in, from its first node that names one; null until the agent starts. */
	private static String roomOf(String runId) {
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if (node.get(AutomationConstants.PARENT_NODE_ID) == null
					&& node.get(AutomationConstants.ROOM_ID) instanceof String roomId && !roomId.isBlank()) {
				return roomId;
			}
		}
		return null;
	}

	/**
	 * Deletes one finished run of the owner's routine from its history, with the chats opened from it and the rooms its
	 * agent worked in. When it was the routine's latest run, Routines stops showing its result too.
	 *
	 * @return the rooms deleted, so a client showing one can leave it
	 */
	public static List<String> deleteRun(Insight insight, String routineId, String runId) {
		Target target = target(insight, routineId);
		WorkRoutine.Slot slot = target.slot();
		User user = insight.getUser();
		String id = runId == null ? "" : runId.trim();
		Map<String, Object> run = id.isEmpty() ? null : ownRun(slot, user, id);
		if (run == null) {
			throw new IllegalArgumentException("Unknown run");
		}
		if ("running".equals(status(run))) {
			throw new IllegalArgumentException("This run is still going. Delete it once it has finished.");
		}
		Set<String> roomIds = rooms(target, id);
		// the chats go first, so after a failure deleting the run again finishes the job
		deleteRooms(user, target, roomIds);
		AutomationRunStore.deleteRun(AutomationProjectService.inlineRunScope(user), slot.automationId(), id);
		// the row keeps its start, which orders the sidebar, and the older runs stay in the history
		CollaborationDbUtils.update("UPDATE " + slot.kind().table() + " SET RUN_STATUS = NULL, FINISHED_AT = NULL, "
				+ "ROOM_ID = NULL, RUN_ID = NULL, SUMMARY = NULL, ERROR = NULL, ERROR_CAUSE = NULL" + slot.where()
				+ " AND RUN_ID = ?", slot.args(new Object[0], id));
		return List.copyOf(roomIds);
	}

	/**
	 * Deletes every run of all the owner's routines, with the chats opened from them and the rooms their agents worked
	 * in, and removes each routine's schedule. Reset calls it before it deletes the routine rows, which name the
	 * routines; while one is running it refuses before deleting anything.
	 *
	 * @return the runs deleted
	 */
	static int deleteAll(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<WorkRoutine.Slot> slots = slots(owner);
		for (WorkRoutine.Slot slot : slots) {
			WorkRoutine.settle(slot);
			Map<String, Object> row = WorkRoutine.stored(slot);
			if (row != null && "running".equals(row.get("status"))) {
				throw new IllegalStateException("A routine is running; wait for it to finish or stop it, then reset");
			}
		}
		String scope = AutomationProjectService.inlineRunScope(user);
		int deleted = 0;
		for (WorkRoutine.Slot slot : slots) {
			Target target = new Target(slot.kind(), slot.routineId(), owner, WorkRoutine.stored(slot));
			for (Map<String, Object> run : AutomationRunStore.getRunsForAutomation(scope, slot.automationId(), -1)) {
				deleteRooms(user, target, rooms(target, String.valueOf(run.get(AutomationConstants.RUN_ID))));
			}
			deleted += AutomationRunStore.deleteRuns(scope, slot.automationId());
			WorkRoutine.unschedule(slot);
		}
		return deleted;
	}

	// the four system routines and each of the owner's own
	private static List<WorkRoutine.Slot> slots(Pair<String, String> owner) {
		List<WorkRoutine.Slot> slots = new ArrayList<>();
		for (WorkRoutine.Kind kind : List.of(WorkInboxRoutine.KIND, WorkAgendaRoutine.KIND, WorkEventPlanningRoutine.KIND,
				WorkExpenseRoutine.KIND)) {
			slots.add(new WorkRoutine.Slot(kind, owner, null));
		}
		for (String routineId : WorkRoutine.routineIds(WorkCustomRoutine.KIND, owner)) {
			slots.add(new WorkRoutine.Slot(WorkCustomRoutine.KIND, owner, routineId));
		}
		return slots;
	}

	// every run's agent gets rooms of its own, and only the run's history names them; plus the chats opened from it
	private static Set<String> rooms(Target target, String runId) {
		Set<String> roomIds = new LinkedHashSet<>();
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if (node.get(AutomationConstants.ROOM_ID) instanceof String roomId && !roomId.isBlank()) {
				roomIds.add(roomId);
			}
		}
		roomIds.add(target.runRoom(runId));
		Map<String, Object> row = target.row();
		if (row != null && runId.equals(row.get("runId")) && row.get("startedAt") instanceof String started) {
			roomIds.add(target.latestRoom(started));
		}
		return roomIds;
	}

	// as deleting a chat does
	private static void deleteRooms(User user, Target target, Set<String> roomIds) {
		for (String roomId : roomIds) {
			if (!ModelInferenceLogsUtils.doSetRoomToInactive(target.owner().getValue0(), roomId)) {
				throw new IllegalStateException("The run's chats could not be deleted. Try again.");
			}
			AgentCodeExecutionContext.removeForRoom(user, roomId);
			RoomMessageStore.evictRedisProjection(roomId);
		}
	}

	/** The run, only when it is this owner's run of this routine; the client id is never trusted alone. */
	private static Map<String, Object> ownRun(WorkRoutine.Slot slot, User user, String runId) {
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		return run != null && AutomationProjectService.inlineRunScope(user).equals(run.get(AutomationConstants.PROJECT_ID))
				&& slot.automationId().equals(run.get(AutomationConstants.AUTOMATION_ID)) ? run : null;
	}

	/** One run's result text, only when the run is this owner's run of this routine; null when it has none. */
	private static String runOutput(WorkRoutine.Slot slot, User user, String runId) {
		return ownRun(slot, user, runId) == null ? null : output(slot, runId);
	}

	private static String output(WorkRoutine.Slot slot, String runId) {
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if (slot.kind().outputNode().equals(node.get(AutomationConstants.NODE_ID))
					&& node.get(AutomationConstants.PARENT_NODE_ID) == null
					&& AutomationConstants.NODE_STATUS_SUCCESS.equals(node.get(AutomationConstants.STATUS))
					&& node.get(AutomationConstants.OUTPUT_VALUE) instanceof String output) {
				try {
					Object text = AutomationRuntimeUtils.GSON.fromJson(output, Object.class);
					return text instanceof String summary && !summary.isBlank() ? summary : null;
				} catch (RuntimeException e) {
					return null;
				}
			}
		}
		return null;
	}

	public static Map<String, Object> open(Insight insight, String routineId, String startedAt) {
		return open(insight, routineId, startedAt, null);
	}

	/**
	 * Opens the latest output, or with a run id that past run's output. A routine whose answer is its agent's opens the
	 * room that agent worked in, tool calls and all; the others get a chat that starts with the output.
	 */
	public static Map<String, Object> open(Insight insight, String routineId, String startedAt, String runId) {
		Target target = target(insight, routineId);
		WorkRoutine.Kind kind = target.kind();
		Pair<String, String> owner = target.owner();
		Map<String, Object> row = target.row();
		String summary;
		String runStarted;
		String roomId;
		if (runId != null && !runId.isBlank()) {
			// the run must be this owner's run of this routine; the client id is never trusted alone
			summary = runOutput(target.slot(), insight.getUser(), runId.trim());
			Map<String, Object> run = summary == null ? null : AutomationRunStore.getRunDetail(runId.trim());
			if (run == null) {
				throw new IllegalArgumentException("This run has no output to discuss");
			}
			Object started = run.get(AutomationConstants.STARTED_AT);
			runStarted = started == null ? "" : String.valueOf(started);
			roomId = target.runRoom(runId.trim());
		} else {
			if (row == null || !"completed".equals(row.get("status"))
					|| !(row.get("summary") instanceof String latest) || latest.isBlank()
					|| !(row.get("startedAt") instanceof String latestStarted)) {
				throw new IllegalArgumentException("This routine has no completed output to discuss");
			}
			if (startedAt != null && !latestStarted.equals(startedAt)) {
				throw new IllegalArgumentException("The routine output has changed. Refresh Routines and open the latest output.");
			}
			summary = latest;
			runStarted = latestStarted;
			// the run's own chat, the one History opens, so deleting the run finds it; a chat opened earlier stays
			String earlier = target.latestRoom(latestStarted);
			roomId = row.get("runId") instanceof String latestRun && !ModelInferenceLogsUtils.doCheckRoomExists(earlier)
					? target.runRoom(latestRun)
					: earlier;
		}
		// past the same checks as a chat: the owner's own run, with output, and still the one shown
		if (kind.agent() != null) {
			return Map.of("roomId", agentRoom(target, runId));
		}
		String messageId = CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), roomId, "output");
		String modelId = BrainTopicModel.engine(insight.getUser());
		Map<String, Object> options = modelId == null ? Map.of() : Map.of("modelId", modelId);
		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(roomId)) {
			String date = runStarted.length() >= 10 ? " - " + runStarted.substring(0, 10) : "";
			RoomUtils.createRoomIfNotExists(roomId, insight, null, kind.jobName() + date, null, options, null,
					CollaborationUtils.COLLABORATION_PROJECT_ID, null);
			// A deterministic message makes retries safe even if the room was created before persistence failed.
			RoomMessageStore.appendPlatformMessageIfAbsent(roomId, insight.getUser().getPrimaryLoginToken().getId(),
					messageId, summary, null);
		}
		return Map.of("roomId", roomId);
	}

	/**
	 * The room the run's agent answered in, which already holds the output after its tool calls and the reference
	 * documents the run read: the past run's, else the latest run's from the routine's row.
	 */
	private static String agentRoom(Target target, String runId) {
		Object roomId;
		if (runId != null && !runId.isBlank()) {
			Map<String, Object> result = WorkRoutine.resultNode(target.kind(), runId.trim());
			roomId = result == null ? null : result.get(AutomationConstants.ROOM_ID);
		} else {
			roomId = target.row().get("roomId");
		}
		if (!(roomId instanceof String id) || id.isBlank()) {
			throw new IllegalArgumentException("This run has no room to open");
		}
		return id;
	}
}
