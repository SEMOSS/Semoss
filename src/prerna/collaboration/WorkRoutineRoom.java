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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.run.AutomationRunStore;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Opens a private discussion of one completed routine output without exposing its automation history. */
public final class WorkRoutineRoom {
	private static final int MAX_RUNS = 30;
	private static final Set<String> FAILED = Set.of(AutomationConstants.STATUS_FAILED,
			AutomationConstants.STATUS_INTERRUPTED, AutomationConstants.STATUS_CANCELLED);

	private WorkRoutineRoom() { }

	// a routine id names its kind, its row and the documents its runs read
	private record Target(WorkRoutine.Kind kind, String customId, String documentsId, Pair<String, String> owner,
			Map<String, Object> row) {
		WorkRoutine.Slot slot() {
			return new WorkRoutine.Slot(kind, owner, customId);
		}
	}

	private static Target target(Insight insight, String routineId) {
		WorkRoutine.Kind kind;
		// any other id names one of the owner's own routines
		String customId = null;
		// the reference documents the routine's runs read; the inbox and agenda routines have none
		String documentsId = null;
		if (WorkInboxRoutine.ID.equals(routineId)) {
			kind = WorkInboxRoutine.KIND;
		} else if (WorkAgendaRoutine.ID.equals(routineId)) {
			kind = WorkAgendaRoutine.KIND;
		} else if (WorkEventPlanningRoutine.ID.equals(routineId)) {
			kind = WorkEventPlanningRoutine.KIND;
		} else if (WorkExpenseRoutine.ID.equals(routineId)) {
			kind = WorkExpenseRoutine.KIND;
			documentsId = WorkExpenseRoutine.DOCUMENTS_ID;
		} else if (routineId != null && !routineId.isBlank()) {
			kind = WorkCustomRoutine.KIND;
			customId = routineId.trim();
			documentsId = customId;
		} else {
			throw new IllegalArgumentException("Unknown routine");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		Map<String, Object> row = WorkRoutine.stored(new WorkRoutine.Slot(kind, owner, customId));
		if (row == null && customId != null) {
			throw new IllegalArgumentException("Unknown routine");
		}
		return new Target(kind, customId, documentsId, owner, row);
	}

	/** The routine's past runs for the signed-in owner, newest first, from the scheduler history. */
	public static List<Map<String, Object>> runs(Insight insight, String routineId) {
		WorkRoutine.Slot slot = target(insight, routineId).slot();
		List<Map<String, Object>> runs = new ArrayList<>();
		for (Map<String, Object> run : AutomationRunStore.getRunsForAutomation(
				AutomationProjectService.inlineRunScope(insight.getUser()), slot.automationId(), MAX_RUNS)) {
			String status = String.valueOf(run.get(AutomationConstants.STATUS));
			Timestamp started = (Timestamp) run.get(AutomationConstants.STARTED_AT);
			Timestamp finished = (Timestamp) run.get(AutomationConstants.COMPLETED_AT);
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("runId", run.get(AutomationConstants.RUN_ID));
			item.put("startedAt", CollaborationDbUtils.toIso(started));
			item.put("finishedAt", CollaborationDbUtils.toIso(finished));
			item.put("status", AutomationConstants.STATUS_SUCCESS.equals(status) ? "completed"
					: FAILED.contains(status) ? "failed" : "running");
			item.put("trigger", AutomationConstants.TRIGGER_MANUAL.equals(run.get(AutomationConstants.TRIGGER_TYPE))
					? "manual" : "scheduled");
			item.put("durationMs", started == null || finished == null ? null : finished.getTime() - started.getTime());
			// a run that failed after its result node still has output to discuss, so the history checks the node
			item.put("hasOutput", output(slot, (String) run.get(AutomationConstants.RUN_ID)) != null);
			runs.add(item);
		}
		return runs;
	}

	/** One run's result text, only when the run is this owner's run of this routine; null when it has none. */
	private static String runOutput(WorkRoutine.Slot slot, User user, String runId) {
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		if (run == null || !AutomationProjectService.inlineRunScope(user).equals(run.get(AutomationConstants.PROJECT_ID))
				|| !slot.automationId().equals(run.get(AutomationConstants.AUTOMATION_ID))) {
			return null;
		}
		return output(slot, runId);
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

	/** Opens the latest output, or with a run id that past run's output, as a chat. */
	public static Map<String, Object> open(Insight insight, String routineId, String startedAt, String runId) {
		Target target = target(insight, routineId);
		WorkRoutine.Kind kind = target.kind();
		String customId = target.customId();
		String documentsId = target.documentsId();
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
			roomId = CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-run-discussion",
					kind.id(), customId == null ? "" : customId, runId.trim());
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
			roomId = customId == null
					? CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-discussion",
							kind.id(), runStarted)
					: CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-discussion",
							kind.id(), customId, runStarted);
		}
		String title = customId != null && row.get("settings") instanceof Map<?, ?> settings
				&& settings.get("name") instanceof String name && !name.isBlank() ? name : kind.jobName();
		String messageId = CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), roomId, "output");
		String modelId = BrainTopicModel.engine(insight.getUser());
		Map<String, Object> options = modelId == null ? Map.of() : Map.of("modelId", modelId);
		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(roomId)) {
			String date = runStarted.length() >= 10 ? " - " + runStarted.substring(0, 10) : "";
			RoomUtils.createRoomIfNotExists(roomId, insight, null, title + date, null, options, null,
					CollaborationUtils.COLLABORATION_PROJECT_ID, null);
			// the same copy a run gets, refreshed on every open so a document uploaded since is there too
			List<String> documents = List.of();
			if (documentsId != null) {
				try {
					documents = WorkRoutineDocuments.copyInto(insight.getUser(), documentsId, Room.roomFolderPath(roomId));
				} catch (IOException e) {
					throw new IllegalStateException("Could not copy the routine's reference documents into the chat", e);
				}
			}
			// A deterministic message makes retries safe even if the room was created before persistence failed.
			RoomMessageStore.appendPlatformMessageIfAbsent(roomId, insight.getUser().getPrimaryLoginToken().getId(),
					messageId, withDocuments(summary, documents), null);
		}
		return Map.of("roomId", roomId);
	}

	// shortcut: the note names the documents present at first open; a later upload is copied on reopen but stays
	// unnamed here, so add a per-turn runtime note if owners add documents mid-chat
	static String withDocuments(String summary, List<String> documents) {
		if (documents.isEmpty()) {
			return summary;
		}
		return summary + "\n\n---\n\nThe routine's reference documents are in the \"" + WorkRoutineDocuments.FOLDER
				+ "\" folder of this chat's working folder: " + String.join(", ", documents)
				+ ". Read the ones a question needs with the file tools, or the document reader for Office files and PDFs.";
	}
}
