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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.User;

// Work open tabs (WORK_OPEN_ROOM) and thread goals. The room itself lives in the inference logs database
// (ROOM-01); this only records which thread rooms the owner has open.
public final class WorkRoomUtils {

	private static final String LOCK = "room";

	private WorkRoomUtils() {

	}

	// most recently active first, pinned tabs on top
	public static Map<String, Object> listOpenRooms(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<Map<String, Object>> items = CollaborationDbUtils.query("SELECT o.THREAD_ID, o.ROOM_ID, o.PINNED, "
				+ "o.LAST_ACTIVE_AT, t.SUBJECT FROM WORK_OPEN_ROOM o JOIN BRAIN_THREAD t ON t.OWNER_ID = o.OWNER_ID "
				+ "AND t.OWNER_TYPE = o.OWNER_TYPE AND t.THREAD_ID = o.THREAD_ID "
				+ "WHERE o.OWNER_ID = ? AND o.OWNER_TYPE = ? "
				+ "ORDER BY CASE WHEN o.PINNED = ? THEN 0 ELSE 1 END, o.LAST_ACTIVE_AT DESC, o.THREAD_ID", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("threadId", CollaborationDbUtils.getString(rs, "THREAD_ID"));
					row.put("roomId", CollaborationDbUtils.getString(rs, "ROOM_ID"));
					row.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
					row.put("lastAt", CollaborationDbUtils.getTimestamp(rs, "LAST_ACTIVE_AT"));
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "PINNED"))) {
						row.put("pinned", true);
					}
					return row;
				}, owner.getValue0(), owner.getValue1(), true);
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", items.size());
		return page;
	}

	// the owner opened (or came back to) a thread's workspace: one tab per thread; a later room id replaces the
	// first, a null one keeps it
	public static Map<String, Object> openRoom(User user, String threadId, String roomId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		Timestamp now = CollaborationDbUtils.now();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			int updated = CollaborationDbUtils.update("UPDATE WORK_OPEN_ROOM SET ROOM_ID = COALESCE(?, ROOM_ID), "
					+ "LAST_ACTIVE_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", roomId, now, ownerId,
					ownerType, threadId);
			if (updated == 0) {
				CollaborationDbUtils.update("INSERT INTO WORK_OPEN_ROOM (OWNER_ID, OWNER_TYPE, THREAD_ID, ROOM_ID, "
						+ "OPENED_AT, LAST_ACTIVE_AT, PINNED) VALUES (?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType,
						threadId, roomId, now, now, false);
			}
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("threadId", threadId);
		result.put("roomId", roomId);
		result.put("lastAt", CollaborationDbUtils.toIso(now));
		return result;
	}

	// removes the tab only; the room and its messages stay
	public static Map<String, Object> closeRoom(User user, String threadId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			CollaborationDbUtils.update("DELETE FROM WORK_OPEN_ROOM WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ?", ownerId, ownerType, threadId);
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("threadId", threadId);
		result.put("closed", true);
		return result;
	}

	// a blank goal clears it
	public static Map<String, Object> setThreadGoal(User user, String threadId, String goal) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		String value = goal == null || goal.isBlank() ? null : goal.trim();
		CollaborationDbUtils.update("UPDATE BRAIN_THREAD SET GOAL = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND THREAD_ID = ?", value, ownerId, ownerType, threadId);
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("threadId", threadId);
		result.put("goal", value);
		return result;
	}
}
