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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;

// Brain threads: BRAIN_THREAD with its topic links and participants
public final class BrainThreadUtils {

	public static final String MUTED = "muted";
	public static final String NEEDS_TOPIC_CHOICE = "needs_topic_choice";
	public static final Set<String> FILTERS = Set.of(MUTED, NEEDS_TOPIC_CHOICE);
	public static final Set<String> CHANNELS = Set.of("email", "teams", "calendar");

	public static final String RULE = "rule";

	// a participant with several roles shows the first of these
	private static final List<String> ROLE_ORDER = List.of("organizer", "from", "to", "cc", "required", "attendee",
			"member");

	private static final String THREAD_COLUMNS = "t.THREAD_ID, t.SOURCE, t.SUBJECT, t.MUTED, t.AUTOMATED, "
			+ "t.MESSAGE_COUNT, t.LAST_MESSAGE_AT, t.ROOM_ID";

	// an open topic_choice review means two candidate topics were close
	private static final String OPEN_TOPIC_CHOICE = "EXISTS (SELECT 1 FROM BRAIN_REVIEW r "
			+ "WHERE r.OWNER_ID = t.OWNER_ID AND r.OWNER_TYPE = t.OWNER_TYPE AND r.KIND = 'topic_choice' "
			+ "AND r.REF_TYPE = 'thread' AND r.REF_ID = t.THREAD_ID AND r.STATUS = 'open')";

	private BrainThreadUtils() {

	}

	// ---- read ----

	public static Map<String, Object> listThreads(User user, String filter, String topicId, String channel,
			int limit, int offset) {
		return listThreads(user, filter, topicId, channel, limit, offset, false);
	}

	// detail adds each thread's participants and summary, one query each for the page
	public static Map<String, Object> listThreads(User user, String filter, String topicId, String channel,
			int limit, int offset, boolean detail) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();

		StringBuilder where = new StringBuilder(" WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ?");
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		if (filter != null) {
			if (!FILTERS.contains(filter)) {
				throw new IllegalArgumentException("Thread filter must be one of " + FILTERS);
			}
			where.append(MUTED.equals(filter) ? " AND t.MUTED = ?" : " AND " + OPEN_TOPIC_CHOICE);
			if (MUTED.equals(filter)) {
				params.add(true);
			}
		}
		if (topicId != null) {
			where.append(" AND EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC l WHERE l.OWNER_ID = t.OWNER_ID "
					+ "AND l.OWNER_TYPE = t.OWNER_TYPE AND l.THREAD_ID = t.THREAD_ID AND l.TOPIC_ID = ?)");
			params.add(topicId);
		}
		if (channel != null) {
			checkChannel(channel);
			where.append(" AND t.SOURCE = ?");
			params.add(channel);
		}

		List<Map<String, Object>> items = CollaborationDbUtils.query(
				CollaborationDbUtils.page("SELECT " + THREAD_COLUMNS + ", t.SUMMARY, " + OPEN_TOPIC_CHOICE
						+ " AS NEEDS_CHOICE FROM BRAIN_THREAD t" + where
						+ " ORDER BY COALESCE(t.LAST_MESSAGE_AT, t.CREATED_AT) DESC, t.THREAD_ID", limit, offset),
				rs -> {
					Map<String, Object> row = mapThread(rs);
					if (detail) {
						row.put("summary", CollaborationDbUtils.getString(rs, "SUMMARY"));
					}
					return row;
				}, params.toArray());
		addLinks(ownerId, ownerType, items);
		if (detail) {
			addParticipants(ownerId, ownerType, items);
		}

		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_THREAD t" + where, params.toArray()));
		return page;
	}

	public static Map<String, Object> getThread(User user, String threadId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> thread = CollaborationDbUtils.queryOne("SELECT " + THREAD_COLUMNS + ", t.SUMMARY, "
				+ OPEN_TOPIC_CHOICE + " AS NEEDS_CHOICE FROM BRAIN_THREAD t "
				+ "WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND t.THREAD_ID = ?", rs -> {
					Map<String, Object> row = mapThread(rs);
					row.put("summary", CollaborationDbUtils.getString(rs, "SUMMARY"));
					return row;
				}, ownerId, ownerType, threadId);
		if (thread == null) {
			throw new IllegalArgumentException("Thread not found");
		}
		addLinks(ownerId, ownerType, List.of(thread));
		thread.put("participants", CollaborationDbUtils.query(
				"SELECT PERSON_ID, ROLES_JSON, INCLUDED, EXCLUDED_BY, EXCLUDED_AT, HIDDEN_COUNT "
						+ "FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? "
						+ "ORDER BY FIRST_SEEN_AT, PERSON_ID",
				BrainThreadUtils::mapParticipant, ownerId, ownerType, threadId));

		// keep summary last to match the contract's field order
		thread.put("summary", thread.remove("summary"));
		return thread;
	}

	static List<Map<String, Object>> getLinks(String ownerId, String ownerType, String threadId) {
		return CollaborationDbUtils.query("SELECT TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? ORDER BY IS_PRIMARY DESC, CONFIDENCE DESC, "
				+ "TOPIC_ID", BrainThreadUtils::mapLink, ownerId, ownerType, threadId);
	}

	// ---- write ----

	// the owner links a topic (source you, confidence 100); one primary per thread, the first link included
	public static List<Map<String, Object>> linkThreadTopic(User user, String threadId, String topicId,
			boolean primary, boolean remove) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireThread(ownerId, ownerType, threadId);
		BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);

		List<Map<String, Object>> links = getLinks(ownerId, ownerType, threadId);
		Map<String, Object> current = null;
		for (Map<String, Object> link : links) {
			if (topicId.equals(link.get("topicId"))) {
				current = link;
			}
		}
		Map<String, Object> existing = current;
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			if (remove) {
				if (existing == null) {
					return;
				}
				CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND TOPIC_ID = ?", ownerId,
						ownerType, threadId, topicId);
				// dropping the primary promotes the strongest remaining link
				if (Boolean.TRUE.equals(existing.get("primary"))) {
					for (Map<String, Object> link : links) {
						if (link != existing) {
							setPrimary(conn, ownerId, ownerType, threadId, (String) link.get("topicId"));
							break;
						}
					}
				}
				return;
			}
			boolean makePrimary = primary || links.isEmpty();
			if (makePrimary) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET IS_PRIMARY = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", false, ownerId, ownerType, threadId);
			}
			if (existing == null) {
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, "
						+ "TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY, CHANGED_BY, CHANGED_AT) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, threadId, topicId,
						BrainProfileUtils.YOU, 100, makePrimary, BrainProfileUtils.YOU, now);
			} else {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET SOURCE = ?, CONFIDENCE = ?, "
						+ "IS_PRIMARY = ?, CHANGED_BY = ?, CHANGED_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND TOPIC_ID = ?",
						BrainProfileUtils.YOU, 100, makePrimary || Boolean.TRUE.equals(existing.get("primary")),
						BrainProfileUtils.YOU, now, ownerId, ownerType, threadId, topicId);
			}
		});
		return getLinks(ownerId, ownerType, threadId);
	}

	// excluding is by hand; a rule-driven exclusion is undone by changing the rule, not here
	public static Map<String, Object> setThreadParticipant(User user, String threadId, String personId,
			boolean included) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireThread(ownerId, ownerType, threadId);
		Map<String, Object> current = CollaborationDbUtils.queryOne("SELECT INCLUDED, EXCLUDED_BY, EXCLUDED_RULE_ID "
				+ "FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? "
				+ "AND PERSON_ID = ?", rs -> {
					Map<String, Object> row = new HashMap<>();
					row.put("included", !Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "INCLUDED")));
					row.put("excludedBy", CollaborationDbUtils.getString(rs, "EXCLUDED_BY"));
					row.put("ruleId", CollaborationDbUtils.getString(rs, "EXCLUDED_RULE_ID"));
					return row;
				}, ownerId, ownerType, threadId, personId);
		if (current == null) {
			throw new IllegalArgumentException("Person is not on this thread");
		}

		boolean wasIncluded = (Boolean) current.get("included");
		if (included && !wasIncluded && RULE.equals(current.get("excludedBy"))
				&& isActiveRule(ownerId, ownerType, (String) current.get("ruleId"))) {
			throw new IllegalArgumentException("Excluded by rule " + current.get("ruleId")
					+ "; change the rule to include this person");
		}
		if (included != wasIncluded) {
			CollaborationDbUtils.update("UPDATE BRAIN_THREAD_PARTICIPANT SET INCLUDED = ?, EXCLUDED_BY = ?, "
					+ "EXCLUDED_RULE_ID = NULL, EXCLUDED_AT = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?", included,
					included ? null : BrainProfileUtils.YOU, included ? null : CollaborationDbUtils.now(), ownerId,
					ownerType, threadId, personId);
		}
		return CollaborationDbUtils.queryOne("SELECT PERSON_ID, ROLES_JSON, INCLUDED, EXCLUDED_BY, EXCLUDED_AT, "
				+ "HIDDEN_COUNT FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? "
				+ "AND PERSON_ID = ?", BrainThreadUtils::mapParticipant, ownerId, ownerType, threadId, personId);
	}

	public static Map<String, Object> setThreadMuted(User user, String threadId, boolean muted) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireThread(ownerId, ownerType, threadId);
		CollaborationDbUtils.update("UPDATE BRAIN_THREAD SET MUTED = ? "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", muted, ownerId, ownerType, threadId);
		Map<String, Object> thread = CollaborationDbUtils.queryOne("SELECT " + THREAD_COLUMNS + ", "
				+ OPEN_TOPIC_CHOICE + " AS NEEDS_CHOICE FROM BRAIN_THREAD t "
				+ "WHERE t.OWNER_ID = ? AND t.OWNER_TYPE = ? AND t.THREAD_ID = ?", BrainThreadUtils::mapThread,
				ownerId, ownerType, threadId);
		addLinks(ownerId, ownerType, List.of(thread));
		return thread;
	}

	// ---- helpers ----

	static void requireThread(String ownerId, String ownerType, String threadId) {
		if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND THREAD_ID = ?", ownerId, ownerType, threadId)) {
			throw new IllegalArgumentException("Thread not found");
		}
	}

	private static boolean isActiveRule(String ownerId, String ownerType, String ruleId) {
		return ruleId != null && CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_RULE WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND RULE_ID = ? AND DISABLED_AT IS NULL", ownerId, ownerType, ruleId);
	}

	private static void setPrimary(Connection conn, String ownerId, String ownerType, String threadId,
			String topicId) throws SQLException {
		CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET IS_PRIMARY = ? "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND TOPIC_ID = ?", true, ownerId, ownerType,
				threadId, topicId);
	}

	// one query for every thread on the page
	private static void addLinks(String ownerId, String ownerType, List<Map<String, Object>> threads) {
		if (threads.isEmpty()) {
			return;
		}
		Map<String, List<Map<String, Object>>> byThread = new HashMap<>();
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		Set<String> ids = new HashSet<>();
		for (Map<String, Object> thread : threads) {
			ids.add((String) thread.get("id"));
		}
		params.addAll(ids);
		for (Pair<String, Map<String, Object>> row : CollaborationDbUtils.query(
				"SELECT THREAD_ID, TOPIC_ID, SOURCE, CONFIDENCE, IS_PRIMARY FROM BRAIN_THREAD_TOPIC "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IN ("
						+ CollaborationDbUtils.placeholders(ids.size()) + ") "
						+ "ORDER BY IS_PRIMARY DESC, CONFIDENCE DESC, TOPIC_ID",
				rs -> Pair.with(rs.getString("THREAD_ID"), mapLink(rs)), params.toArray())) {
			byThread.computeIfAbsent(row.getValue0(), k -> new ArrayList<>()).add(row.getValue1());
		}
		for (Map<String, Object> thread : threads) {
			thread.put("topicLinks", byThread.getOrDefault(thread.get("id"), new ArrayList<>()));
			// keep topicLinks right after subject, as in the contract
			for (String key : List.of("muted", "automated", "messageCount", "lastAt", "roomId", "needsTopicChoice")) {
				if (thread.containsKey(key)) {
					thread.put(key, thread.remove(key));
				}
			}
		}
	}

	private static void addParticipants(String ownerId, String ownerType, List<Map<String, Object>> threads) {
		if (threads.isEmpty()) {
			return;
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		for (Map<String, Object> thread : threads) {
			params.add(thread.get("id"));
			thread.put("participants", new ArrayList<Map<String, Object>>());
		}
		Map<String, List<Map<String, Object>>> byThread = new HashMap<>();
		for (Pair<String, Map<String, Object>> row : CollaborationDbUtils.query(
				"SELECT THREAD_ID, PERSON_ID, ROLES_JSON, INCLUDED, EXCLUDED_BY, EXCLUDED_AT, HIDDEN_COUNT "
						+ "FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IN ("
						+ CollaborationDbUtils.placeholders(threads.size()) + ") ORDER BY FIRST_SEEN_AT, PERSON_ID",
				rs -> Pair.with(rs.getString("THREAD_ID"), mapParticipant(rs)), params.toArray())) {
			byThread.computeIfAbsent(row.getValue0(), k -> new ArrayList<>()).add(row.getValue1());
		}
		for (Map<String, Object> thread : threads) {
			thread.put("participants", byThread.getOrDefault(thread.get("id"), new ArrayList<>()));
			if (thread.containsKey("summary")) {
				thread.put("summary", thread.remove("summary"));
			}
		}
	}

	static void checkChannel(String channel) {
		if (!CHANNELS.contains(channel)) {
			throw new IllegalArgumentException("Thread channel must be one of " + CHANNELS);
		}
	}

	// ---- mapping ----

	private static Map<String, Object> mapThread(ResultSet rs) throws SQLException {
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("id", CollaborationDbUtils.getString(rs, "THREAD_ID"));
		row.put("channel", CollaborationDbUtils.getString(rs, "SOURCE"));
		row.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
		row.put("muted", Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MUTED")));
		if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "AUTOMATED"))) {
			row.put("automated", true);
		}
		Integer count = CollaborationDbUtils.getInteger(rs, "MESSAGE_COUNT");
		row.put("messageCount", count == null ? 0 : count);
		row.put("lastAt", CollaborationDbUtils.getTimestamp(rs, "LAST_MESSAGE_AT"));
		row.put("roomId", CollaborationDbUtils.getString(rs, "ROOM_ID"));
		if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "NEEDS_CHOICE"))) {
			row.put("needsTopicChoice", true);
		}
		return row;
	}

	private static Map<String, Object> mapLink(ResultSet rs) throws SQLException {
		Map<String, Object> link = new LinkedHashMap<>();
		link.put("topicId", CollaborationDbUtils.getString(rs, "TOPIC_ID"));
		link.put("source", CollaborationDbUtils.getString(rs, "SOURCE"));
		Integer confidence = CollaborationDbUtils.getInteger(rs, "CONFIDENCE");
		link.put("confidence", confidence == null ? 0 : confidence);
		link.put("primary", Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_PRIMARY")));
		return link;
	}

	private static Map<String, Object> mapParticipant(ResultSet rs) throws SQLException {
		Map<String, Object> participant = new LinkedHashMap<>();
		participant.put("personId", CollaborationDbUtils.getString(rs, "PERSON_ID"));
		participant.put("role", firstRole(CollaborationDbUtils.getString(rs, "ROLES_JSON")));
		// a null flag counts as included
		boolean included = !Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "INCLUDED"));
		participant.put("included", included);
		if (!included) {
			participant.put("excludedBy", CollaborationDbUtils.getString(rs, "EXCLUDED_BY"));
			String at = CollaborationDbUtils.getTimestamp(rs, "EXCLUDED_AT");
			participant.put("excludedOn", at == null ? null : at.substring(0, 10));
			Integer hidden = CollaborationDbUtils.getInteger(rs, "HIDDEN_COUNT");
			participant.put("hiddenCount", hidden == null ? 0 : hidden);
		}
		return participant;
	}

	private static String firstRole(String rolesJson) {
		List<Object> roles = CollaborationDbUtils.parseList(rolesJson);
		for (String role : ROLE_ORDER) {
			if (roles.contains(role)) {
				return role;
			}
		}
		return roles.isEmpty() ? null : String.valueOf(roles.get(0));
	}
}
