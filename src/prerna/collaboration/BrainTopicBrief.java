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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.collaboration.BrainRulesGate.Rule;
import prerna.collaboration.BrainTopicRoomUtils.Chat;
import prerna.engine.impl.model.Room;

/**
 * What the owner's assistant is told about the chat's topics on every turn: each topic's description and open
 * work items. It rides in the per-turn runtime note, not the system prompt, because work items
 * change mid-chat and the system prompt has to stay the same for caching.
 */
public final class BrainTopicBrief {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicBrief.class);

	static final int MAX_ITEMS = 10;
	private static final int MAX_DESCRIPTION = 400;

	private BrainTopicBrief() {
	}

	/** The chat's own topics and its included threads' confirmed topics, after the owner's exclusions. */
	public static List<String> chatTopics(User user, Room room) {
		if (user == null || !CollaborationUtils.isAssistantRoom(room)) {
			return List.of();
		}
		return chatTopics(user, new Chat(room.getId(), CollaborationUtils.threadIdOf(room), room.getOptionsMap()));
	}

	/** The same for an owned chat id; rejects inaccessible and delegation rooms. */
	public static List<String> chatTopics(User user, String roomId) {
		return chatTopics(user, BrainTopicRoomUtils.requireChat(user, roomId));
	}

	static List<String> chatTopics(User user, Chat chat) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		BrainThreadRoomUtils.seedFromSource(owner.getValue0(), owner.getValue1(), chat.roomId(), chat.options());
		// A topic inherited from a thread applies only while an included thread still carries it.
		List<String> topics = CollaborationDbUtils.query("SELECT t.TOPIC_ID FROM BRAIN_TOPIC t "
				+ "LEFT JOIN BRAIN_TOPIC_ROOM r ON r.OWNER_ID = t.OWNER_ID AND r.OWNER_TYPE = t.OWNER_TYPE "
				+ "AND r.TOPIC_ID = t.TOPIC_ID AND r.ROOM_ID = ? WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? "
				+ "AND t.STATUS IN (?, ?) AND (r.STATE IS NULL OR r.STATE = ?) "
				+ "AND ((r.STATE = ? AND COALESCE(r.ORIGIN, '') <> ?) OR EXISTS ("
				+ "SELECT 1 FROM BRAIN_THREAD_ROOM tr JOIN BRAIN_THREAD_TOPIC tt ON tt.OWNER_ID = tr.OWNER_ID "
				+ "AND tt.OWNER_TYPE = tr.OWNER_TYPE AND tt.THREAD_ID = tr.THREAD_ID "
				+ "JOIN BRAIN_THREAD th ON th.OWNER_ID = tr.OWNER_ID AND th.OWNER_TYPE = tr.OWNER_TYPE "
				+ "AND th.THREAD_ID = tr.THREAD_ID WHERE tr.OWNER_ID = t.OWNER_ID AND tr.OWNER_TYPE = t.OWNER_TYPE "
				+ "AND tr.ROOM_ID = ? AND tr.STATE = ? AND tt.TOPIC_ID = t.TOPIC_ID AND tt.SOURCE <> ?)) "
				+ "ORDER BY t.TOPIC_ID", rs -> rs.getString("TOPIC_ID"), chat.roomId(), owner.getValue0(),
				owner.getValue1(), BrainTopicUtils.ACTIVE, BrainTopicUtils.DORMANT, BrainTopicRoomUtils.LINKED,
				BrainTopicRoomUtils.LINKED, BrainTopicRoomUtils.THREAD, chat.roomId(), BrainThreadRoomUtils.LINKED,
				BrainTopicUtils.SUGGESTED);
		return applying(user, topics);
	}

	// linked topics minus the ones a topic-wide ignore keeps out
	static List<String> applying(User user, List<String> linked) {
		if (linked.isEmpty()) {
			return linked;
		}
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<Rule> rules = BrainRulesGate.activeRules(owner.getValue0(), owner.getValue1());
		List<String> topics = new ArrayList<>();
		for (String topicId : linked) {
			if (!keptOut(rules, topicId)) {
				topics.add(topicId);
			}
		}
		return topics;
	}

	// a topic rule with no person keeps the topic out of the assistant
	static boolean keptOut(List<Rule> rules, String topicId) {
		for (Rule rule : rules) {
			String ruleTopic = rule.topicId() != null ? rule.topicId() : rule.value();
			if (rule.personId() == null && "exclude_topic".equals(rule.kind()) && topicId.equals(ruleTopic)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * The chat-topics section of the runtime note, or null for a room with no topics or anything but the owner's
	 * own assistant chat. Never throws: a failure only costs the turn its brief.
	 */
	public static String runtimeNote(User user, Room room) {
		if (user == null || !CollaborationUtils.isAssistantRoom(room)) {
			return null;
		}
		try {
			List<String> topics = chatTopics(user, room);
			if (topics.isEmpty()) {
				return null;
			}
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			StringBuilder out = new StringBuilder("[Chat topics]\nThis chat is about the topics below. Their open "
					+ "work items are as of this turn; use ListMyWork or GetWork for more.");
			for (String topicId : topics) {
				out.append("\n\n").append(topic(user, owner.getValue0(), owner.getValue1(), topicId));
			}
			return out.append("\n[/Chat topics]").toString();
		} catch (RuntimeException e) {
			classLogger.warn("Could not build the topic brief for room {}; the turn goes on without it", room.getId(),
					e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private static String topic(User user, String ownerId, String ownerType, String topicId) {
		Map<String, String> row = CollaborationDbUtils.queryOne(
				"SELECT NAME, DESCRIPTION FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?",
				rs -> {
					Map<String, String> topic = new LinkedHashMap<>();
					topic.put("name", CollaborationDbUtils.getString(rs, "NAME"));
					topic.put("description", CollaborationDbUtils.getString(rs, "DESCRIPTION"));
					return topic;
				},
				ownerId, ownerType, topicId);
		StringBuilder out = new StringBuilder("Topic: ").append(row == null ? topicId : row.get("name"))
				.append(" (id ").append(topicId).append(")");
		String description = row == null ? null : row.get("description");
		if (description != null && !description.isBlank()) {
			out.append("\nDescription: ").append(clip(description.trim(), MAX_DESCRIPTION));
		}
		try {
			BrainThreadFinder.TopicThreads threads = BrainThreadFinder.topicThreads(ownerId, ownerType, topicId);
			out.append("\nEmail threads: ").append(threads.confirmed());
			if (threads.suggested() > 0) {
				out.append(" (and ").append(threads.suggested()).append(" more only suggested)");
			}
			if (threads.newest() != null) {
				out.append(", the newest with mail on ").append(CollaborationDbUtils.toIso(threads.newest()), 0, 10);
			}
			if (threads.confirmed() + threads.suggested() > 0) {
				out.append(". SearchMail looks inside this topic's threads by default, in their message text as well; "
						+ "ReadThread reads one.");
			}
		} catch (RuntimeException e) {
			classLogger.warn("Could not count the threads of topic {} for the brief", topicId, e);
		}
		out.append(BrainTopicFiles.brief(ownerId, ownerType, topicId));
		out.append(linkedFoldersBrief(ownerId, ownerType, topicId));
		// the topic's work items, each with the owner's next step or who they wait on
		List<Map<String, Object>> items = new ArrayList<>();
		for (Object o : (List<?>) WorkTaskUtils.listMyWork(user, Map.of("topicId", topicId)).get("workItems")) {
			if (o instanceof Map<?, ?> m) {
				items.add((Map<String, Object>) m);
			}
		}
		out.append("\nOpen work items:").append(items.isEmpty() ? " none" : "");
		for (Map<String, Object> item : items.subList(0, Math.min(items.size(), MAX_ITEMS))) {
			out.append("\n- ").append(item.get("title")).append(" (id ").append(item.get("id"));
			if ("proposed".equals(item.get("state"))) {
				out.append(", not reviewed yet");
			}
			out.append(")");
			String next = nextStep(item);
			if (next != null) {
				out.append(": ").append(next);
			}
		}
		if (items.size() > MAX_ITEMS) {
			out.append("\n").append(items.size() - MAX_ITEMS).append(" more; use ListMyWork with this topic.");
		}
		return out.toString();
	}

	// the owner's next step, else the first one someone else owes
	@SuppressWarnings("unchecked")
	private static String nextStep(Map<String, Object> item) {
		List<Map<String, Object>> steps = item.get("openSteps") instanceof List<?> l ? (List<Map<String, Object>>) l
				: List.of();
		for (Map<String, Object> st : steps) {
			if (st.get("assigneeId") == null) {
				return "next " + st.get("text");
			}
		}
		for (Map<String, Object> st : steps) {
			Object who = st.get("assigneeName");
			return "waiting on " + (who == null ? "someone" : who) + " to " + st.get("text");
		}
		return null;
	}

	/** The topic's open work items: title, status, due, whether the owner reviewed it, and the next step. */
	static List<Map<String, Object>> openWork(String ownerId, String ownerType, String topicId, int limit) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Object o : (List<?>) WorkTaskUtils.listMyWork(ownerId, ownerType, Map.of("topicId", topicId)).get("workItems")) {
			if (!(o instanceof Map<?, ?> m) || out.size() >= limit) {
				continue;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> item = (Map<String, Object>) m;
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", item.get("id"));
			row.put("title", item.get("title"));
			row.put("status", item.get("status"));
			row.put("due", item.get("due"));
			row.put("reviewed", !WorkTaskUtils.PROPOSED.equals(item.get("state")));
			row.put("next", nextStep(item));
			out.add(row);
		}
		return out;
	}

	private static String clip(String text, int max) {
		return text.length() <= max ? text : text.substring(0, max - 3) + "...";
	}

	private static String linkedFoldersBrief(String ownerId, String ownerType, String topicId) {
		List<Map<String, Object>> links = BrainTopicLinks.ofTopic(ownerId, ownerType, topicId);
		if (links.isEmpty()) {
			return "";
		}
		StringBuilder out = new StringBuilder(
				"\nLinked Microsoft 365 folders: the owner linked these to this topic because they contain "
						+ "relevant files for this topic's work. The agent lists and reads files from these by default. "
						+ "To list files, call ListDriveFiles for a OneDrive link, or ListChannelFiles for Teams. "
						+ "Availability is marked 'ok' or 'unavailable' (access lost):");
		for (Map<String, Object> link : links.subList(0, Math.min(links.size(), MAX_ITEMS))) {
			out.append("\n- ").append(BrainThreadBrief.neutral(String.valueOf(link.get("displayName"))))
					.append(" (");
			String state = String.valueOf(link.get("state"));
			out.append(state);
			if ("ok".equals(state)) {
				String kind = String.valueOf(link.get("kind"));
				if ("drive_folder".equals(kind)) {
					out.append(", driveId ").append(link.get("driveId")).append(", itemId ")
							.append(link.get("itemId"));
				} else if ("channel_files".equals(kind)) {
					out.append(", teamId ").append(link.get("teamId")).append(", channelId ")
							.append(link.get("channelId"));
				}
			}
			out.append(")");
		}
		if (links.size() > MAX_ITEMS) {
			out.append("\nAnd ").append(links.size() - MAX_ITEMS).append(" more, listed on the topic's page.");
		}
		return out.toString();
	}
}
