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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.collaboration.BrainThreadMessages.Shown;
import prerna.collaboration.BrainTopicRoomUtils.Chat;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

/**
 * The email threads a chat includes (BRAIN_THREAD_ROOM). A chat opened from a thread includes it; the owner adds and
 * removes others, and a removed thread stays out until the owner adds it again. SEEN_REF is the newest email of the
 * thread the chat's assistant has been given, so the emails after it are new to the chat.
 */
public final class BrainThreadRoomUtils {

	public static final String LINKED = "linked";
	public static final String REMOVED = "removed";
	// origin of the thread a chat was opened from; a thread the owner added is BrainProfileUtils.YOU
	static final String THREAD = "thread";

	private static final String LOCK = "thread-room";
	private static final String OWNED_ROOM = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ROOM_ID = ?";
	// owners whose chats from before BRAIN_THREAD_ROOM were seeded on this server
	private static final Set<String> SEEDED_OWNERS = ConcurrentHashMap.newKeySet();

	private BrainThreadRoomUtils() {
	}

	// ---- reading ----

	/**
	 * The chat's threads, most recent mail first, each with how many of its emails are new to the chat's assistant
	 * (all of them for a thread it has not been given yet). withIds adds the ids of the emails the chat shows, the
	 * ones a reply can answer.
	 */
	public static Map<String, Object> listRoomThreads(User user, String roomId, boolean withIds) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Chat chat = BrainTopicRoomUtils.requireChat(user, roomId);
		seedFromSource(ownerId, ownerType, chat.roomId(), chat.options());
		List<Map<String, Object>> threads = CollaborationDbUtils.query("SELECT l.THREAD_ID, l.ORIGIN, l.SEEN_AT, "
				+ "t.SUBJECT, t.SOURCE, t.MESSAGE_COUNT, t.LAST_MESSAGE_AT FROM BRAIN_THREAD_ROOM l "
				+ "JOIN BRAIN_THREAD t ON t.OWNER_ID = l.OWNER_ID AND t.OWNER_TYPE = l.OWNER_TYPE "
				+ "AND t.THREAD_ID = l.THREAD_ID WHERE l.OWNER_ID = ? AND l.OWNER_TYPE = ? AND l.ROOM_ID = ? "
				+ "AND l.STATE = ? ORDER BY t.LAST_MESSAGE_AT DESC, l.THREAD_ID", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("threadId", rs.getString("THREAD_ID"));
					row.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
					row.put("channel", CollaborationDbUtils.getString(rs, "SOURCE"));
					row.put("origin", CollaborationDbUtils.getString(rs, "ORIGIN"));
					row.put("messageCount", CollaborationDbUtils.getInteger(rs, "MESSAGE_COUNT"));
					row.put("lastMessageAt", CollaborationDbUtils.getTimestamp(rs, "LAST_MESSAGE_AT"));
					row.put("seenAt", CollaborationDbUtils.getTimestamp(rs, "SEEN_AT"));
					return row;
				}, ownerId, ownerType, chat.roomId(), LINKED);
		for (Map<String, Object> thread : threads) {
			String seenAt = (String) thread.get("seenAt");
			List<Shown> shown = BrainThreadMessages.shown(ownerId, ownerType, (String) thread.get("threadId"));
			thread.put("newCount", newCount(shown, seenAt));
			thread.put("added", seenAt == null);
			if (withIds) {
				thread.put("emailIds", shown.stream().map(Shown::graphId).toList());
			}
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("roomId", chat.roomId());
		result.put("threads", threads);
		return result;
	}

	/**
	 * The chats that include a thread, most recently active first; closed chats are left out. Origin "thread" marks a
	 * chat that was opened from it.
	 */
	public static Map<String, Object> listThreadRooms(User user, String threadId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireThreadId(ownerId, ownerType, threadId);
		seedOlderChats(user, ownerId, ownerType);
		Map<String, String> origins = new HashMap<>();
		CollaborationDbUtils.query("SELECT ROOM_ID, ORIGIN FROM BRAIN_THREAD_ROOM WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND THREAD_ID = ? AND STATE = ?", rs -> origins.put(rs.getString("ROOM_ID"), rs.getString("ORIGIN")),
				ownerId, ownerType, threadId, LINKED);
		List<Map<String, Object>> rooms = new ArrayList<>(
				ModelInferenceLogsUtils.getActiveRoomSummaries(userId(user), new ArrayList<>(origins.keySet())));
		rooms.removeIf(room -> !CollaborationUtils.COLLABORATION_PROJECT_ID.equals(room.get("PROJECT_ID")));
		rooms.sort(Comparator
				.comparing((Map<String, Object> room) -> (Timestamp) room.get("UPDATED_AT"),
						Comparator.nullsLast(Comparator.reverseOrder()))
				.thenComparing(room -> String.valueOf(room.get("ROOM_ID"))));
		List<Map<String, Object>> items = new ArrayList<>();
		for (Map<String, Object> room : rooms) {
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("roomId", room.get("ROOM_ID"));
			item.put("name", room.get("ROOM_NAME"));
			item.put("origin", origins.get(room.get("ROOM_ID")));
			item.put("lastAt", CollaborationDbUtils.toIso((Timestamp) room.get("UPDATED_AT")));
			items.add(item);
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("threadId", threadId);
		result.put("items", items);
		result.put("total", items.size());
		return result;
	}

	// ---- changing ----

	/**
	 * The owner adds a thread to the chat or removes one. A thread new to the chat has no SEEN_REF, so the assistant
	 * gets its latest emails once; a thread added back picks up where the chat left it.
	 */
	public static Map<String, Object> linkRoomThread(User user, String roomId, String threadId, boolean remove) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Chat chat = BrainTopicRoomUtils.requireChat(user, roomId);
		requireThreadId(ownerId, ownerType, threadId);
		String state = remove ? REMOVED : LINKED;
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			// first, so removing the thread the chat was opened from sticks
			seedFromSource(ownerId, ownerType, chat.roomId(), chat.options());
			int updated = CollaborationDbUtils.update("UPDATE BRAIN_THREAD_ROOM SET STATE = ?, CHANGED_AT = ?"
					+ OWNED_ROOM + " AND THREAD_ID = ?", state, CollaborationDbUtils.now(), ownerId, ownerType,
					chat.roomId(), threadId);
			if (updated == 0) {
				insert(ownerId, ownerType, chat.roomId(), threadId, state,
						threadId.equals(chat.threadId()) ? THREAD : BrainProfileUtils.YOU, null, null);
			}
		}
		return listRoomThreads(user, chat.roomId(), false);
	}

	// ---- helpers ----

	// shown emails (newest first) after the newest one the assistant was given; all of them when it was given none
	static int newCount(List<Shown> shown, String seenAt) {
		if (seenAt == null) {
			return shown.size();
		}
		Instant seen = Instant.parse(seenAt);
		int count = 0;
		for (Shown email : shown) {
			if (email.at() == null || !Instant.parse(email.at()).isAfter(seen)) {
				break;
			}
			count++;
		}
		return count;
	}

	// a chat opened from a thread includes it, given up to the newest email its import held
	private static void seedFromSource(String ownerId, String ownerType, String roomId, Map<String, Object> options) {
		String threadId = CollaborationUtils.threadIdOf(options);
		String linkSql = "SELECT 1 FROM BRAIN_THREAD_ROOM" + OWNED_ROOM + " AND THREAD_ID = ?";
		if (threadId == null || CollaborationDbUtils.exists(linkSql, ownerId, ownerType, roomId, threadId)) {
			return;
		}
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			if (CollaborationDbUtils.exists(linkSql, ownerId, ownerType, roomId, threadId)) {
				return;
			}
			// not a Brain thread (an Outlook email or a calendar event opened as a chat): nothing to follow
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID = ?", ownerId, ownerType, threadId)) {
				return;
			}
			Set<String> imported = importedIds(options);
			String seenRef = null;
			Timestamp seenAt = null;
			if (!imported.isEmpty()) {
				for (Object[] email : CollaborationDbUtils.query("SELECT GRAPH_ID, RECEIVED_AT FROM BRAIN_MESSAGE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND GRAPH_ID IS NOT NULL "
						+ "ORDER BY RECEIVED_AT DESC, MESSAGE_KEY",
						rs -> new Object[] { rs.getString("GRAPH_ID"), rs.getTimestamp("RECEIVED_AT") }, ownerId,
						ownerType, threadId)) {
					if (imported.contains(email[0])) {
						seenRef = (String) email[0];
						seenAt = (Timestamp) email[1];
						break;
					}
				}
			}
			insert(ownerId, ownerType, roomId, threadId, LINKED, THREAD, seenRef, seenAt);
		}
	}

	// the email ids a chat's import held (room option source.messages)
	private static Set<String> importedIds(Map<String, Object> options) {
		Set<String> ids = new HashSet<>();
		if (options != null && options.get(CollaborationUtils.ROOM_OPTION_SOURCE) instanceof Map<?, ?> source
				&& source.get("messages") instanceof List<?> messages) {
			for (Object message : messages) {
				if (message instanceof Map<?, ?> email && email.get("id") != null) {
					ids.add(String.valueOf(email.get("id")));
				}
			}
		}
		return ids;
	}

	// chats opened from a thread before BRAIN_THREAD_ROOM existed, once per owner on this server
	private static void seedOlderChats(User user, String ownerId, String ownerType) {
		String key = ownerType + ":" + ownerId;
		if (SEEDED_OWNERS.contains(key)) {
			return;
		}
		for (Map<String, Object> room : ModelInferenceLogsUtils.getActiveRoomOptions(userId(user),
				CollaborationUtils.COLLABORATION_PROJECT_ID)) {
			Map<String, Object> options;
			try {
				options = CollaborationDbUtils.parseMap((String) room.get("OPTIONS"));
			} catch (RuntimeException e) {
				continue;
			}
			Object delegation = options == null ? null : options.get(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID);
			if (options != null && (delegation == null || String.valueOf(delegation).isBlank())) {
				seedFromSource(ownerId, ownerType, (String) room.get("ROOM_ID"), options);
			}
		}
		SEEDED_OWNERS.add(key);
	}

	private static void requireThreadId(String ownerId, String ownerType, String threadId) {
		if (threadId == null || threadId.isBlank()) {
			throw new IllegalArgumentException("A threadId is required");
		}
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
	}

	private static void insert(String ownerId, String ownerType, String roomId, String threadId, String state,
			String origin, String seenRef, Timestamp seenAt) {
		CollaborationDbUtils.update("INSERT INTO BRAIN_THREAD_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, THREAD_ID, STATE, "
				+ "ORIGIN, SEEN_REF, SEEN_AT, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, roomId,
				threadId, state, origin, seenRef, seenAt, CollaborationDbUtils.now());
	}

	private static String userId(User user) {
		return user.getPrimaryLoginToken().getId();
	}
}
