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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

/**
 * A chat's topics (BRAIN_TOPIC_ROOM). The owner picks them, or the assistant tags them while the owner works:
 * linked when it is confident, a soft tag (suggested) otherwise. Only linked topics apply to the chat. A topic
 * the owner removes or dismisses stays dismissed, so it is not suggested again in that chat.
 */
public final class BrainTopicRoomUtils {

	public static final String LINKED = "linked";
	public static final String SUGGESTED = "suggested";
	public static final String DISMISSED = "dismissed";
	static final String AGENT = "agent";
	static final String THREAD = "thread";

	private static final String LOCK = "topic-room";
	private static final String OWNED_ROOM = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ROOM_ID = ?";

	/** The owner's own collaboration chat and the thread it was opened from (null for a plain chat). */
	record Chat(String roomId, String threadId) {
	}

	private BrainTopicRoomUtils() {
	}

	// ---- reading ----

	/** The chat's linked, suggested and dismissed topics, with names, for its header. */
	public static Map<String, Object> listRoomTopics(User user, String roomId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Chat chat = requireChat(user, roomId);
		seedFromThread(owner.getValue0(), owner.getValue1(), chat);
		List<Map<String, Object>> topics = CollaborationDbUtils.query("SELECT r.TOPIC_ID, r.STATE, r.ORIGIN, "
				+ "r.CHANGED_AT, t.NAME, t.COLOR FROM BRAIN_TOPIC_ROOM r JOIN BRAIN_TOPIC t ON t.OWNER_ID = r.OWNER_ID "
				+ "AND t.OWNER_TYPE = r.OWNER_TYPE AND t.TOPIC_ID = r.TOPIC_ID "
				+ "WHERE r.OWNER_ID = ? AND r.OWNER_TYPE = ? AND r.ROOM_ID = ? ORDER BY r.CHANGED_AT DESC, r.TOPIC_ID",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("topicId", CollaborationDbUtils.getString(rs, "TOPIC_ID"));
					row.put("name", CollaborationDbUtils.getString(rs, "NAME"));
					row.put("color", CollaborationDbUtils.getString(rs, "COLOR"));
					row.put("state", CollaborationDbUtils.getString(rs, "STATE"));
					row.put("origin", CollaborationDbUtils.getString(rs, "ORIGIN"));
					row.put("changedAt", CollaborationDbUtils.getTimestamp(rs, "CHANGED_AT"));
					return row;
				}, owner.getValue0(), owner.getValue1(), chat.roomId());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("roomId", chat.roomId());
		result.put("threadId", chat.threadId());
		result.put("topics", topics);
		return result;
	}

	/**
	 * Topic ids that apply to this room: its linked topics. Empty for any room that is not the owner's own
	 * assistant chat.
	 */
	public static List<String> topicsOf(User user, Room room) {
		if (user == null || !CollaborationUtils.isAssistantRoom(room)) {
			return List.of();
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		seedFromThread(owner.getValue0(), owner.getValue1(),
				new Chat(room.getId(), CollaborationUtils.threadIdOf(room)));
		return CollaborationDbUtils.query(
				"SELECT TOPIC_ID FROM BRAIN_TOPIC_ROOM" + OWNED_ROOM + " AND STATE = ? ORDER BY TOPIC_ID",
				rs -> rs.getString("TOPIC_ID"), owner.getValue0(), owner.getValue1(), room.getId(), LINKED);
	}

	/** A topic's linked chats, most recently active first; closed chats are left out. */
	public static Map<String, Object> listTopicRooms(User user, String topicId, int limit, int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		BrainTopicUtils.requireTopic(owner.getValue0(), owner.getValue1(), topicId);
		Map<String, Map<String, Object>> links = new LinkedHashMap<>();
		CollaborationDbUtils.query("SELECT ROOM_ID, ORIGIN, CHANGED_AT FROM BRAIN_TOPIC_ROOM WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND TOPIC_ID = ? AND STATE = ?", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("roomId", rs.getString("ROOM_ID"));
					row.put("origin", CollaborationDbUtils.getString(rs, "ORIGIN"));
					row.put("linkedAt", CollaborationDbUtils.getTimestamp(rs, "CHANGED_AT"));
					links.put(rs.getString("ROOM_ID"), row);
					return row;
				}, owner.getValue0(), owner.getValue1(), topicId, LINKED);

		List<Map<String, Object>> rooms = new ArrayList<>();
		Map<String, Timestamp> activeAt = new HashMap<>();
		for (Map<String, Object> room : activeRooms(userId(user), new ArrayList<>(links.keySet()))) {
			Map<String, Object> row = links.get(room.get("roomId"));
			row.put("name", room.get("name"));
			row.put("lastAt", CollaborationDbUtils.toIso((Timestamp) room.get("updatedAt")));
			activeAt.put((String) room.get("roomId"), (Timestamp) room.get("updatedAt"));
			rooms.add(row);
		}
		rooms.sort((a, b) -> {
			Timestamp x = activeAt.get(a.get("roomId"));
			Timestamp y = activeAt.get(b.get("roomId"));
			int byTime = x == null || y == null ? (x == null ? (y == null ? 0 : 1) : -1) : y.compareTo(x);
			return byTime != 0 ? byTime : String.valueOf(a.get("roomId")).compareTo(String.valueOf(b.get("roomId")));
		});
		int from = Math.min(Math.max(offset, 0), rooms.size());
		int to = limit > 0 ? Math.min(from + limit, rooms.size()) : rooms.size();
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("topicId", topicId);
		page.put("items", new ArrayList<>(rooms.subList(from, to)));
		page.put("total", rooms.size());
		return page;
	}

	// ---- changing ----

	/** The owner sets or removes one of the chat's topics; a removed topic stays dismissed. */
	public static Map<String, Object> linkRoomTopic(User user, String roomId, String topicId, String topicName,
			boolean remove) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Chat chat = requireChat(user, roomId);
		String topic = BrainThreadFinder.resolveTopic(ownerId, ownerType, topicId, topicName);
		if (!remove) {
			requireOpenTopic(ownerId, ownerType, topic);
		}
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			seedFromThread(ownerId, ownerType, chat);
			put(ownerId, ownerType, chat.roomId(), topic, remove ? DISMISSED : LINKED, BrainProfileUtils.YOU);
		}
		return listRoomTopics(user, chat.roomId());
	}

	/**
	 * The assistant tags the chat it is in: linked when confident, a soft tag otherwise. Never overrides the
	 * owner: a dismissed topic is not tagged again and a linked one is left as it is.
	 */
	public static Map<String, Object> tagRoomTopic(User user, String roomId, String topicRef, boolean confident) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Chat chat = requireChat(user, roomId);
		if (topicRef == null || topicRef.isBlank()) {
			throw new IllegalArgumentException("Pass the topic's id or name");
		}
		String topic = BrainAgentEdits.topic(ownerId, ownerType, topicRef);
		requireOpenTopic(ownerId, ownerType, topic);
		String outcome;
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			seedFromThread(ownerId, ownerType, chat);
			String state = CollaborationDbUtils.queryOne(
					"SELECT STATE FROM BRAIN_TOPIC_ROOM" + OWNED_ROOM + " AND TOPIC_ID = ?",
					rs -> CollaborationDbUtils.getString(rs, "STATE"), ownerId, ownerType, chat.roomId(), topic);
			if (DISMISSED.equals(state)) {
				outcome = "The owner removed this topic from this chat. Do not tag it again.";
			} else if (LINKED.equals(state)) {
				outcome = "This chat already has this topic.";
			} else if (SUGGESTED.equals(state) && !confident) {
				outcome = "This topic is already suggested; the owner has not answered yet.";
			} else {
				put(ownerId, ownerType, chat.roomId(), topic, confident ? LINKED : SUGGESTED, AGENT);
				outcome = confident ? "Tagged. The topic applies to this chat now; the owner can undo it."
						: "Suggested. It applies once the owner accepts it.";
			}
		}
		Map<String, Object> result = listRoomTopics(user, chat.roomId());
		result.put("topicId", topic);
		result.put("outcome", outcome);
		return result;
	}

	// ---- topic lifecycle, inside the caller's transaction ----

	// merge: the source's chats move to the target; a chat on both keeps one row, linked if either was
	static void moveRooms(Connection conn, String ownerId, String ownerType, String sourceTopicId,
			String targetTopicId, Timestamp now) throws SQLException {
		String onSource = " AND ROOM_ID IN (SELECT ROOM_ID FROM BRAIN_TOPIC_ROOM WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND TOPIC_ID = ? AND STATE = ?)";
		String onTarget = " AND ROOM_ID IN (SELECT ROOM_ID FROM BRAIN_TOPIC_ROOM WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND TOPIC_ID = ?)";
		String owned = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?";
		CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC_ROOM SET STATE = ?, CHANGED_AT = ?" + owned + onSource,
				LINKED, now, ownerId, ownerType, targetTopicId, ownerId, ownerType, sourceTopicId, LINKED);
		CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_TOPIC_ROOM" + owned + onTarget, ownerId, ownerType,
				sourceTopicId, ownerId, ownerType, targetTopicId);
		CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC_ROOM SET TOPIC_ID = ?" + owned, targetTopicId, ownerId,
				ownerType, sourceTopicId);
	}

	// archive and delete: the topic leaves every chat; restoring it does not bring the links back
	static void removeRooms(Connection conn, String ownerId, String ownerType, String topicId) throws SQLException {
		CollaborationDbUtils.update(conn,
				"DELETE FROM BRAIN_TOPIC_ROOM WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", ownerId,
				ownerType, topicId);
	}

	// ---- helpers ----

	// a thread chat with no topic rows yet takes its thread's confirmed topics as linked
	private static void seedFromThread(String ownerId, String ownerType, Chat chat) {
		if (chat.threadId() == null || CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_ROOM" + OWNED_ROOM,
				ownerId, ownerType, chat.roomId())) {
			return;
		}
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_ROOM" + OWNED_ROOM, ownerId, ownerType,
					chat.roomId())) {
				return;
			}
			List<String> topics = CollaborationDbUtils.query("SELECT l.TOPIC_ID FROM BRAIN_THREAD_TOPIC l "
					+ "JOIN BRAIN_TOPIC t ON t.OWNER_ID = l.OWNER_ID AND t.OWNER_TYPE = l.OWNER_TYPE "
					+ "AND t.TOPIC_ID = l.TOPIC_ID WHERE l.OWNER_ID = ? AND l.OWNER_TYPE = ? AND l.THREAD_ID = ? "
					+ "AND l.SOURCE <> ? AND t.STATUS <> ? ORDER BY l.TOPIC_ID", rs -> rs.getString("TOPIC_ID"),
					ownerId, ownerType, chat.threadId(), BrainTopicUtils.SUGGESTED, BrainTopicUtils.ARCHIVED);
			Timestamp now = CollaborationDbUtils.now();
			for (String topic : topics) {
				CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, "
						+ "STATE, ORIGIN, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, chat.roomId(),
						topic, LINKED, THREAD, now);
			}
		}
	}

	// one row per chat and topic
	private static void put(String ownerId, String ownerType, String roomId, String topicId, String state,
			String origin) {
		Timestamp now = CollaborationDbUtils.now();
		int updated = CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_ROOM SET STATE = ?, ORIGIN = ?, CHANGED_AT = ?"
				+ OWNED_ROOM + " AND TOPIC_ID = ?", state, origin, now, ownerId, ownerType, roomId, topicId);
		if (updated == 0) {
			CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, "
					+ "STATE, ORIGIN, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, roomId, topicId,
					state, origin, now);
		}
	}

	private static void requireOpenTopic(String ownerId, String ownerType, String topicId) {
		String status = BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		if (BrainTopicUtils.ARCHIVED.equals(status)) {
			throw new IllegalArgumentException("That topic is archived; restore it first");
		}
		if (BrainTopicUtils.SUGGESTED.equals(status)) {
			throw new IllegalArgumentException("That topic is only suggested; accept it first");
		}
	}

	// the signed-in user's own open collaboration chat, not a delegation room; "Chat not found" otherwise
	static Chat requireChat(User user, String roomId) {
		if (roomId == null || roomId.isBlank()) {
			throw new IllegalArgumentException("A roomId is required");
		}
		List<Map<String, Object>> found = ModelInferenceLogsUtils.getActiveRoomSummaries(userId(user), List.of(roomId));
		Map<String, Object> room = found.isEmpty() ? null : found.get(0);
		if (room == null || !CollaborationUtils.COLLABORATION_PROJECT_ID.equals(room.get("PROJECT_ID"))) {
			throw new IllegalArgumentException("Chat not found");
		}
		String json = (String) room.get("OPTIONS");
		Map<String, Object> options = json == null || json.isBlank() ? Map.of() : CollaborationDbUtils.parseMap(json);
		Object delegation = options.get(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID);
		if (delegation != null && !String.valueOf(delegation).isBlank()) {
			throw new IllegalArgumentException("Chat not found");
		}
		return new Chat(roomId, CollaborationUtils.threadIdOf(options));
	}

	// the user's open rooms among roomIds: roomId, name, updatedAt
	private static List<Map<String, Object>> activeRooms(String userId, List<String> roomIds) {
		List<Map<String, Object>> rooms = new ArrayList<>();
		for (Map<String, Object> room : ModelInferenceLogsUtils.getActiveRoomSummaries(userId, roomIds)) {
			Map<String, Object> row = new HashMap<>();
			row.put("roomId", room.get("ROOM_ID"));
			row.put("name", room.get("ROOM_NAME"));
			row.put("updatedAt", room.get("UPDATED_AT"));
			rooms.add(row);
		}
		return rooms;
	}

	private static String userId(User user) {
		return user.getPrimaryLoginToken().getId();
	}
}
