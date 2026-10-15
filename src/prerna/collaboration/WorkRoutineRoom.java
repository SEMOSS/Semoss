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

import java.util.Map;

import org.javatuples.Pair;

import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;

/** Opens a private discussion of one completed routine output without exposing its automation history. */
public final class WorkRoutineRoom {
	private WorkRoutineRoom() { }

	public static Map<String, Object> open(Insight insight, String routineId, String startedAt) {
		WorkRoutine.Kind kind;
		if (WorkInboxRoutine.ID.equals(routineId)) {
			kind = WorkInboxRoutine.KIND;
		} else if (WorkAgendaRoutine.ID.equals(routineId)) {
			kind = WorkAgendaRoutine.KIND;
		} else {
			throw new IllegalArgumentException("Unknown routine");
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		Map<String, Object> row = WorkRoutine.stored(kind, owner);
		if (row == null || !"completed".equals(row.get("status"))
				|| !(row.get("summary") instanceof String summary) || summary.isBlank()
				|| !(row.get("startedAt") instanceof String runStarted)) {
			throw new IllegalArgumentException("This routine has no completed output to discuss");
		}
		if (startedAt != null && !runStarted.equals(startedAt)) {
			throw new IllegalArgumentException("The routine output has changed. Refresh Routines and open the latest output.");
		}
		String roomId = CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(),
				"routine-discussion", kind.id(), runStarted);
		String messageId = CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), roomId, "output");
		String modelId = BrainTopicModel.engine(insight.getUser());
		Map<String, Object> options = modelId == null ? Map.of() : Map.of("modelId", modelId);
		try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(roomId)) {
			RoomUtils.createRoomIfNotExists(roomId, insight, null, kind.jobName() + " - " + runStarted.substring(0, 10),
					null, options, null, CollaborationUtils.COLLABORATION_PROJECT_ID, null);
			// A deterministic message makes retries safe even if the room was created before persistence failed.
			RoomMessageStore.appendPlatformMessageIfAbsent(roomId, insight.getUser().getPrimaryLoginToken().getId(),
					messageId, summary, null);
		}
		return Map.of("roomId", roomId);
	}
}
