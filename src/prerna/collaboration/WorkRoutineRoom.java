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
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;

/** Opens a private discussion of one completed routine output without exposing its automation history. */
public final class WorkRoutineRoom {
	private WorkRoutineRoom() { }

	public static Map<String, Object> open(Insight insight, String routineId, String startedAt) {
		WorkRoutine.Kind kind;
		// any other id names one of the owner's own routines
		String customId = null;
		// the reference documents the routine's runs read; the inbox and agenda routines have none
		String documentsId = null;
		if (WorkInboxRoutine.ID.equals(routineId)) {
			kind = WorkInboxRoutine.KIND;
		} else if (WorkAgendaRoutine.ID.equals(routineId)) {
			kind = WorkAgendaRoutine.KIND;
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
		if (row == null || !"completed".equals(row.get("status"))
				|| !(row.get("summary") instanceof String summary) || summary.isBlank()
				|| !(row.get("startedAt") instanceof String runStarted)) {
			throw new IllegalArgumentException("This routine has no completed output to discuss");
		}
		if (startedAt != null && !runStarted.equals(startedAt)) {
			throw new IllegalArgumentException("The routine output has changed. Refresh Routines and open the latest output.");
		}
		String roomId = customId == null
				? CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-discussion",
						kind.id(), runStarted)
				: CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), "routine-discussion",
						kind.id(), customId, runStarted);
		String title = customId != null && row.get("settings") instanceof Map<?, ?> settings
				&& settings.get("name") instanceof String name && !name.isBlank() ? name : kind.jobName();
		String messageId = CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), roomId, "output");
		String modelId = BrainTopicModel.engine(insight.getUser());
		Map<String, Object> options = modelId == null ? Map.of() : Map.of("modelId", modelId);
		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(roomId)) {
			RoomUtils.createRoomIfNotExists(roomId, insight, null, title + " - " + runStarted.substring(0, 10),
					null, options, null, CollaborationUtils.COLLABORATION_PROJECT_ID, null);
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
