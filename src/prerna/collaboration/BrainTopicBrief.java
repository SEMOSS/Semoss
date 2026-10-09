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
import prerna.engine.impl.model.Room;

/**
 * What the owner's assistant is told about the chat's topics on every turn: each topic's description, open goals
 * and open action items. It rides in the per-turn runtime note, not the system prompt, because goals and items
 * change mid-chat and the system prompt has to stay the same for caching.
 */
public final class BrainTopicBrief {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicBrief.class);

	static final int MAX_GOALS = 10;
	static final int MAX_ITEMS = 10;
	private static final int MAX_DESCRIPTION = 400;

	private BrainTopicBrief() {
	}

	/** The chat's topics that apply: linked, and not kept out by a topic-wide ignore. */
	public static List<String> chatTopics(User user, Room room) {
		return applying(user, BrainTopicRoomUtils.topicsOf(user, room));
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
			StringBuilder out = new StringBuilder("[Chat topics]\nThis chat is about the topics below. Their goals "
					+ "and open action items are as of this turn; use ListTopics with a topic for more.");
			for (String topicId : topics) {
				out.append("\n\n").append(topic(owner.getValue0(), owner.getValue1(), topicId));
			}
			return out.append("\n[/Chat topics]").toString();
		} catch (RuntimeException e) {
			classLogger.warn("Could not build the topic brief for room {}; the turn goes on without it", room.getId(),
					e);
			return null;
		}
	}

	private static String topic(String ownerId, String ownerType, String topicId) {
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
		List<String> goals = new ArrayList<>();
		for (Map<String, Object> goal : BrainTopicUtils.getGoals(ownerId, ownerType, topicId)) {
			if ("open".equals(goal.get("status")) && goals.size() < MAX_GOALS) {
				goals.add(String.valueOf(goal.get("text")));
			}
		}
		out.append("\nOpen goals:").append(goals.isEmpty() ? " none" : "");
		for (String goal : goals) {
			out.append("\n- ").append(goal);
		}
		List<Map<String, Object>> items = openItems(ownerId, ownerType, topicId, MAX_ITEMS);
		out.append("\nOpen action items:").append(items.isEmpty() ? " none" : "");
		for (Map<String, Object> item : items) {
			out.append("\n- ").append(item.get("title"));
			if (item.get("dueAt") != null) {
				out.append(" (due ").append(String.valueOf(item.get("dueAt")), 0, 10).append(")");
			}
		}
		return out.toString();
	}

	/**
	 * Open Work items on the topic's confirmed threads or linked to the topic, most important first. Until the
	 * action item model settles (SEMOSS/Semoss#3109) these are the topic's action items.
	 */
	static List<Map<String, Object>> openItems(String ownerId, String ownerType, String topicId, int limit) {
		return CollaborationDbUtils.query(CollaborationDbUtils.page("SELECT w.ITEM_ID, w.TITLE, w.THREAD_ID, "
				+ "w.DUE_AT, w.PRIORITY FROM WORK_ITEM w WHERE w.OWNER_ID = ? AND w.OWNER_TYPE = ? AND w.STATUS = ? "
				+ "AND (w.LINK_TOPIC_ID = ? OR w.THREAD_ID IN (SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND SOURCE <> ?)) "
				+ "ORDER BY COALESCE(w.SCORE, 0) DESC, w.DUE_AT, w.ITEM_ID", limit, 0), rs -> {
					Map<String, Object> item = new LinkedHashMap<>();
					item.put("itemId", rs.getString("ITEM_ID"));
					item.put("title", CollaborationDbUtils.getString(rs, "TITLE"));
					item.put("threadId", CollaborationDbUtils.getString(rs, "THREAD_ID"));
					Timestamp due = rs.getTimestamp("DUE_AT");
					item.put("dueAt", due == null ? null : CollaborationDbUtils.toIso(due));
					item.put("priority", CollaborationDbUtils.getString(rs, "PRIORITY"));
					return item;
				}, ownerId, ownerType, "open", topicId, ownerId, ownerType, topicId, BrainTopicUtils.SUGGESTED);
	}

	private static String clip(String text, int max) {
		return text.length() <= max ? text : text.substring(0, max - 3) + "...";
	}
}
