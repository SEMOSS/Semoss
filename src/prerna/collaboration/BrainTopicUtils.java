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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// Brain topics: BRAIN_TOPIC with its notes, goals, and people
public final class BrainTopicUtils {

	public static final String SUGGESTED = "suggested";
	public static final String ACTIVE = "active";
	public static final String DORMANT = "dormant";
	public static final String ARCHIVED = "archived";
	public static final Set<String> STATUSES = Set.of(SUGGESTED, ACTIVE, DORMANT, ARCHIVED);

	// BRAIN_TOPIC_PERSON.STATE
	public static final String MEMBER = "member";
	public static final String REMOVED = "removed";
	public static final List<String> PERSON_STATES = List.of(MEMBER, SUGGESTED, REMOVED);

	public static final Set<String> KINDS = Set.of("client", "internal", "event", "personal");

	// BRAIN_TOPIC_NOTE.KIND and the states each kind allows
	public static final String GOAL = "goal";
	public static final String NOTE = "note";
	private static final Map<String, List<String>> NOTE_STATES = Map.of(GOAL, List.of("open", "done"), NOTE,
			List.of("draft", "confirmed"));

	// TW-Q-BRAIN-011: an active topic with no activity and no owner edit for this long goes dormant
	static final int DORMANT_AFTER_DAYS = 30;

	public static final int DEFAULT_LIMIT = 30;

	private static final String SUMMARY_COLUMNS = "TOPIC_ID, NAME, SHORT_NAME, ACCOUNT_ID, KIND, COLOR, STATUS, "
			+ "LAST_ACTIVITY_AT";

	private BrainTopicUtils() {

	}

	// ---- read ----

	public static Map<String, Object> listTopics(User user, List<String> statuses, String accountId, int limit,
			int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		for (String status : statuses) {
			checkStatus(status);
		}
		markDormant(ownerId, ownerType);

		StringBuilder where = new StringBuilder(" WHERE OWNER_ID = ? AND OWNER_TYPE = ?");
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		if (!statuses.isEmpty()) {
			where.append(" AND STATUS IN (").append(CollaborationDbUtils.placeholders(statuses.size())).append(")");
			params.addAll(statuses);
		}
		if (accountId != null) {
			where.append(" AND ACCOUNT_ID = ?");
			params.add(accountId);
		}

		Map<String, Integer> threads = countThreadsByTopic(ownerId, ownerType);
		Map<String, Integer> openItems = countOpenItemsByTopic(ownerId, ownerType);
		List<Map<String, Object>> items = CollaborationDbUtils.query(
				CollaborationDbUtils.page("SELECT " + SUMMARY_COLUMNS + " FROM BRAIN_TOPIC" + where
						+ " ORDER BY NAME, TOPIC_ID", limit, offset),
				rs -> mapSummary(rs, threads, openItems), params.toArray());

		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_TOPIC" + where, params.toArray()));
		return page;
	}

	public static Map<String, Object> getTopic(User user, String topicId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		markDormant(owner.getValue0(), owner.getValue1());
		return getTopic(owner.getValue0(), owner.getValue1(), topicId);
	}

	public static Map<String, Object> getTopic(String ownerId, String ownerType, String topicId) {
		Map<String, Integer> threads = Map.of(topicId, countThreads(ownerId, ownerType, topicId));
		Map<String, Integer> openItems = Map.of(topicId, countOpenItems(ownerId, ownerType, topicId));
		Map<String, Object> topic = CollaborationDbUtils.queryOne("SELECT " + SUMMARY_COLUMNS
				+ ", DESCRIPTION, KEYWORDS_JSON, CALENDAR_SERIES_JSON FROM BRAIN_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", rs -> {
					Map<String, Object> row = mapSummary(rs, threads, openItems);
					row.put("description", CollaborationDbUtils.getString(rs, "DESCRIPTION"));
					row.put("keywords", CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "KEYWORDS_JSON")));
					row.put("calendarSeries",
							CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "CALENDAR_SERIES_JSON")));
					return row;
				}, ownerId, ownerType, topicId);
		if (topic == null) {
			throw new IllegalArgumentException("Topic not found");
		}

		// goals and notes share BRAIN_TOPIC_NOTE, split by KIND
		List<Map<String, Object>> goals = new ArrayList<>();
		List<Map<String, Object>> notes = new ArrayList<>();
		for (Map<String, Object> note : getNotes(ownerId, ownerType, topicId)) {
			if (GOAL.equals(note.get("kind"))) {
				Map<String, Object> goal = new LinkedHashMap<>();
				goal.put("noteId", note.get("noteId"));
				goal.put("text", note.get("text"));
				goal.put("status", note.get("status"));
				goals.add(goal);
			} else {
				notes.add(note);
			}
		}
		topic.put("goals", goals);
		topic.put("notes", notes);
		topic.put("people", getPeople(ownerId, ownerType, topicId));

		// keep stats last to match the contract's field order
		topic.put("stats", topic.remove("stats"));
		return topic;
	}

	static List<Map<String, Object>> getNotes(String ownerId, String ownerType, String topicId) {
		return CollaborationDbUtils.query(
				"SELECT NOTE_ID, KIND, TEXT, STATE, ORIGIN, SOURCE_REF, CREATED_AT FROM BRAIN_TOPIC_NOTE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? ORDER BY CREATED_AT, NOTE_ID",
				BrainTopicUtils::mapNote, ownerId, ownerType, topicId);
	}

	static List<Map<String, Object>> getPeople(String ownerId, String ownerType, String topicId) {
		return CollaborationDbUtils.query(
				"SELECT PERSON_ID, ROLE_LABEL, ENGAGEMENT, STATE, ORIGIN, REASON FROM BRAIN_TOPIC_PERSON "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? ORDER BY PERSON_ID",
				BrainTopicUtils::mapPerson, ownerId, ownerType, topicId);
	}

	// ---- write ----

	// partial Topic: no id creates, an id edits; saving a suggested topic accepts it
	@SuppressWarnings("unchecked")
	public static Map<String, Object> saveTopic(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String topicId = CollaborationDbUtils.asString(changes.get("id"));
		String name = CollaborationDbUtils.asString(changes.get("name"));
		if ((topicId == null || changes.containsKey("name")) && (name == null || name.isBlank())) {
			throw new IllegalArgumentException("Topic name is required");
		}
		if (changes.containsKey("kind")) {
			checkKind(CollaborationDbUtils.asString(changes.get("kind")));
		}
		if (changes.containsKey("status")) {
			checkStatus(CollaborationDbUtils.asString(changes.get("status")));
		}

		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		CollaborationDbUtils.setIfPresent(changes, "name", "NAME", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "short", "SHORT_NAME", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "accountId", "ACCOUNT_ID", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "kind", "KIND", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "color", "COLOR", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "description", "DESCRIPTION", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "status", "STATUS", sets, params);
		for (String[] json : new String[][] { { "keywords", "KEYWORDS_JSON" },
				{ "calendarSeries", "CALENDAR_SERIES_JSON" } }) {
			if (changes.containsKey(json[0])) {
				Object value = changes.get(json[0]);
				CollaborationDbUtils.addSet(sets, params, json[1], value instanceof List
						? CollaborationDbUtils.toJson(CollaborationDbUtils.toStringList((List<Object>) value))
						: null);
			}
		}

		Timestamp now = CollaborationDbUtils.now();
		if (topicId == null) {
			topicId = UUID.randomUUID().toString();
			List<String> columns = new ArrayList<>();
			for (String set : sets) {
				columns.add(set.substring(0, set.indexOf(' ')));
			}
			if (!changes.containsKey("short")) {
				columns.add("SHORT_NAME");
				params.add(name);
			}
			if (!changes.containsKey("status")) {
				columns.add("STATUS");
				params.add(ACTIVE);
			}
			columns.addAll(List.of("OWNER_ID", "OWNER_TYPE", "TOPIC_ID", "ORIGIN", "LAST_ACTIVITY_AT", "CREATED_AT",
					"UPDATED_AT"));
			params.addAll(List.of(ownerId, ownerType, topicId, BrainProfileUtils.YOU, now, now, now));
			CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC (" + String.join(", ", columns) + ") VALUES ("
					+ CollaborationDbUtils.placeholders(columns.size()) + ")", params.toArray());
			return getTopic(ownerId, ownerType, topicId);
		}

		String current = requireTopic(ownerId, ownerType, topicId);
		if (SUGGESTED.equals(current) && !changes.containsKey("status")) {
			CollaborationDbUtils.addSet(sets, params, "STATUS", ACTIVE);
		}
		CollaborationDbUtils.addSet(sets, params, "UPDATED_AT", now);
		params.addAll(List.of(ownerId, ownerType, topicId));
		CollaborationDbUtils.update("UPDATE BRAIN_TOPIC SET " + String.join(", ", sets)
				+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", params.toArray());
		return getTopic(ownerId, ownerType, topicId);
	}

	public static Map<String, Object> setTopicStatus(User user, String topicId, String status) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		checkStatus(status);
		if (SUGGESTED.equals(status)) {
			throw new IllegalArgumentException("A topic cannot be set back to suggested");
		}
		requireTopic(ownerId, ownerType, topicId);
		CollaborationDbUtils.update("UPDATE BRAIN_TOPIC SET STATUS = ?, UPDATED_AT = ? "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", status, CollaborationDbUtils.now(), ownerId,
				ownerType, topicId);
		return getTopic(ownerId, ownerType, topicId);
	}

	// goals take open|done, notes take draft|confirmed; no noteId creates
	public static Map<String, Object> saveTopicNote(User user, String topicId, String noteId, String kind,
			String text, String state) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (kind == null || !NOTE_STATES.containsKey(kind)) {
			throw new IllegalArgumentException("Note kind must be goal or note");
		}
		if (state == null || !NOTE_STATES.get(kind).contains(state)) {
			throw new IllegalArgumentException("A " + kind + " state must be one of " + NOTE_STATES.get(kind));
		}
		if (text == null || text.isBlank()) {
			throw new IllegalArgumentException("Note text is required");
		}
		requireTopic(ownerId, ownerType, topicId);

		Timestamp now = CollaborationDbUtils.now();
		String id = noteId == null ? UUID.randomUUID().toString() : noteId;
		if (noteId != null && !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC_NOTE "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND NOTE_ID = ?", ownerId, ownerType, topicId,
				noteId)) {
			throw new IllegalArgumentException("Note not found");
		}
		CollaborationDbUtils.inTransaction(conn -> {
			if (noteId == null) {
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC_NOTE (OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, "
						+ "KIND, TEXT, STATE, ORIGIN, CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, id, topicId, kind, text, state, BrainProfileUtils.YOU, now, now);
			} else {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC_NOTE SET KIND = ?, TEXT = ?, STATE = ?, "
						+ "UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND NOTE_ID = ?",
						kind, text, state, now, ownerId, ownerType, topicId, id);
			}
			touchTopic(conn, ownerId, ownerType, topicId, now);
		});
		return CollaborationDbUtils.queryOne(
				"SELECT NOTE_ID, KIND, TEXT, STATE, ORIGIN, SOURCE_REF, CREATED_AT FROM BRAIN_TOPIC_NOTE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND NOTE_ID = ?",
				BrainTopicUtils::mapNote, ownerId, ownerType, id);
	}

	public static Map<String, Object> deleteTopicNote(User user, String topicId, String noteId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireTopic(ownerId, ownerType, topicId);
		Timestamp now = CollaborationDbUtils.now();
		int[] deleted = new int[1];
		CollaborationDbUtils.inTransaction(conn -> {
			deleted[0] = CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_TOPIC_NOTE "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND NOTE_ID = ?", ownerId, ownerType,
					topicId, noteId);
			touchTopic(conn, ownerId, ownerType, topicId, now);
		});
		if (deleted[0] == 0) {
			throw new IllegalArgumentException("Note not found");
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("topicId", topicId);
		result.put("noteId", noteId);
		return result;
	}

	// ---- people ----

	// owner sets member, suggested (undo), or removed; a removed row stays so Brain never re-suggests it
	public static Map<String, Object> setTopicPerson(User user, String topicId, String personId, String state,
			String role) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (state == null || !PERSON_STATES.contains(state)) {
			throw new IllegalArgumentException("Person state must be one of " + PERSON_STATES);
		}
		requireTopic(ownerId, ownerType, topicId);
		Map<String, Object> person = CollaborationDbUtils.queryOne("SELECT JOB_TITLE FROM BRAIN_PERSON "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", rs -> {
					Map<String, Object> row = new HashMap<>();
					row.put("jobTitle", CollaborationDbUtils.getString(rs, "JOB_TITLE"));
					return row;
				}, ownerId, ownerType, personId);
		if (person == null) {
			throw new IllegalArgumentException("Person not found");
		}
		Map<String, Object> current = getTopicPerson(ownerId, ownerType, topicId, personId);

		// accepting a Brain suggestion keeps origin brain; anything else is the owner's call
		String origin = current != null && SUGGESTED.equals(current.get("state")) && MEMBER.equals(state)
				? (String) current.get("origin")
				: BrainProfileUtils.YOU;
		String reason = REMOVED.equals(state) ? "Removed by you" : null;
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			if (current == null) {
				// a new member's role defaults to their job title
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC_PERSON (OWNER_ID, OWNER_TYPE, TOPIC_ID, "
						+ "PERSON_ID, STATE, ORIGIN, ROLE_LABEL, REASON, CHANGED_BY, CHANGED_AT) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, topicId, personId, state, origin,
						role != null ? role : person.get("jobTitle"), reason, BrainProfileUtils.YOU, now);
			} else {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC_PERSON SET STATE = ?, ORIGIN = ?, "
						+ "ROLE_LABEL = COALESCE(?, ROLE_LABEL), REASON = ?, CHANGED_BY = ?, CHANGED_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND PERSON_ID = ?", state, origin,
						role, reason, BrainProfileUtils.YOU, now, ownerId, ownerType, topicId, personId);
			}
			touchTopic(conn, ownerId, ownerType, topicId, now);
		});
		return getTopicPerson(ownerId, ownerType, topicId, personId);
	}

	private static Map<String, Object> getTopicPerson(String ownerId, String ownerType, String topicId,
			String personId) {
		return CollaborationDbUtils.queryOne(
				"SELECT PERSON_ID, ROLE_LABEL, ENGAGEMENT, STATE, ORIGIN, REASON FROM BRAIN_TOPIC_PERSON "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND PERSON_ID = ?",
				BrainTopicUtils::mapPerson, ownerId, ownerType, topicId, personId);
	}

	// ---- merge ----

	// moves thread links, people, notes, keywords, rules, and work-item links into the target, then drops the source
	public static Map<String, Object> mergeTopics(User user, String sourceTopicId, String targetTopicId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (sourceTopicId.equals(targetTopicId)) {
			throw new IllegalArgumentException("Cannot merge a topic into itself");
		}
		requireTopic(ownerId, ownerType, sourceTopicId);
		requireTopic(ownerId, ownerType, targetTopicId);

		List<String> keywords = new ArrayList<>();
		for (String topicId : List.of(targetTopicId, sourceTopicId)) {
			for (Object keyword : getKeywords(ownerId, ownerType, topicId)) {
				if (keyword != null && !keywords.contains(String.valueOf(keyword))) {
					keywords.add(String.valueOf(keyword));
				}
			}
		}
		int threadsMoved = countThreads(ownerId, ownerType, sourceTopicId);
		Timestamp now = CollaborationDbUtils.now();
		String owned = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?";
		String threadOnTopic = " AND THREAD_ID IN (SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?";
		String personOnTarget = " AND PERSON_ID IN (SELECT PERSON_ID FROM BRAIN_TOPIC_PERSON "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?)";

		CollaborationDbUtils.inTransaction(conn -> {
			// threads on both topics: the target link inherits primary, the source link goes
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET IS_PRIMARY = ?, CHANGED_BY = ?, "
					+ "CHANGED_AT = ?" + owned + threadOnTopic + " AND IS_PRIMARY = ?)", true, BrainProfileUtils.YOU,
					now, ownerId, ownerType, targetTopicId, ownerId, ownerType, sourceTopicId, true);
			CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_THREAD_TOPIC" + owned + threadOnTopic + ")",
					ownerId, ownerType, sourceTopicId, ownerId, ownerType, targetTopicId);
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET TOPIC_ID = ?, CHANGED_BY = ?, "
					+ "CHANGED_AT = ?" + owned, targetTopicId, BrainProfileUtils.YOU, now, ownerId, ownerType,
					sourceTopicId);

			// people already on the target keep the target's row
			CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_TOPIC_PERSON" + owned + personOnTarget, ownerId,
					ownerType, sourceTopicId, ownerId, ownerType, targetTopicId);
			for (String table : new String[] { "BRAIN_TOPIC_PERSON", "BRAIN_TOPIC_NOTE", "BRAIN_RULE" }) {
				CollaborationDbUtils.update(conn, "UPDATE " + table + " SET TOPIC_ID = ?" + owned, targetTopicId,
						ownerId, ownerType, sourceTopicId);
			}
			CollaborationDbUtils.update(conn, "UPDATE WORK_ITEM SET LINK_TOPIC_ID = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND LINK_TOPIC_ID = ?", targetTopicId, ownerId, ownerType,
					sourceTopicId);
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC SET MERGE_CANDIDATE_ID = NULL "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MERGE_CANDIDATE_ID = ?", ownerId, ownerType,
					sourceTopicId);

			CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC SET KEYWORDS_JSON = ?, UPDATED_AT = ?" + owned,
					CollaborationDbUtils.toJson(keywords), now, ownerId, ownerType, targetTopicId);
			CollaborationDbUtils.update(conn, "DELETE FROM BRAIN_TOPIC" + owned, ownerId, ownerType, sourceTopicId);
		});

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("mergedInto", targetTopicId);
		result.put("threadsMoved", threadsMoved);
		return result;
	}

	private static List<Object> getKeywords(String ownerId, String ownerType, String topicId) {
		return CollaborationDbUtils.parseList(CollaborationDbUtils.queryOne("SELECT KEYWORDS_JSON FROM BRAIN_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?",
				rs -> CollaborationDbUtils.getString(rs, "KEYWORDS_JSON"), ownerId, ownerType, topicId));
	}

	// ---- delete ----

	// removes the topic with its notes, people, thread links, and topic rules; a thread that loses
	// its primary link gets its next most confident link as primary
	public static Map<String, Object> deleteTopic(User user, String topicId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireTopic(ownerId, ownerType, topicId);

		int threadsUnlinked = countThreads(ownerId, ownerType, topicId);
		List<Pair<String, String>> newPrimaries = CollaborationDbUtils.query("SELECT tt.THREAD_ID, "
				+ "(SELECT o.TOPIC_ID FROM BRAIN_THREAD_TOPIC o WHERE o.OWNER_ID = tt.OWNER_ID "
				+ "AND o.OWNER_TYPE = tt.OWNER_TYPE AND o.THREAD_ID = tt.THREAD_ID AND o.TOPIC_ID <> tt.TOPIC_ID "
				+ "ORDER BY o.CONFIDENCE DESC, o.TOPIC_ID FETCH FIRST 1 ROWS ONLY) AS NEXT_TOPIC_ID "
				+ "FROM BRAIN_THREAD_TOPIC tt WHERE tt.OWNER_ID = ? AND tt.OWNER_TYPE = ? AND tt.TOPIC_ID = ? "
				+ "AND tt.IS_PRIMARY = ?",
				rs -> Pair.with(rs.getString("THREAD_ID"), rs.getString("NEXT_TOPIC_ID")), ownerId, ownerType,
				topicId, true);
		Timestamp now = CollaborationDbUtils.now();
		String owned = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?";

		CollaborationDbUtils.inTransaction(conn -> {
			for (Pair<String, String> thread : newPrimaries) {
				if (thread.getValue1() != null) {
					CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_TOPIC SET IS_PRIMARY = ?, CHANGED_BY = ?, "
							+ "CHANGED_AT = ?" + owned + " AND THREAD_ID = ?", true, BrainProfileUtils.YOU, now,
							ownerId, ownerType, thread.getValue1(), thread.getValue0());
				}
			}
			for (String table : new String[] { "BRAIN_THREAD_TOPIC", "BRAIN_TOPIC_PERSON", "BRAIN_TOPIC_NOTE",
					"BRAIN_RULE", "BRAIN_TOPIC" }) {
				CollaborationDbUtils.update(conn, "DELETE FROM " + table + owned, ownerId, ownerType, topicId);
			}
			CollaborationDbUtils.update(conn, "UPDATE WORK_ITEM SET LINK_TOPIC_ID = NULL "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND LINK_TOPIC_ID = ?", ownerId, ownerType, topicId);
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC SET MERGE_CANDIDATE_ID = NULL "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MERGE_CANDIDATE_ID = ?", ownerId, ownerType, topicId);
			// an open review about this topic (e.g. a new-topic suggestion) no longer applies
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_REVIEW SET STATUS = ?, RESOLVED_AT = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REF_ID = ? AND STATUS = ?", "dismissed", now, ownerId,
					ownerType, topicId, "open");
		});

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("topicId", topicId);
		result.put("threadsUnlinked", threadsUnlinked);
		return result;
	}

	// an owner edit counts as activity for the dormant rule
	private static void touchTopic(Connection conn, String ownerId, String ownerType, String topicId, Timestamp now)
			throws SQLException {
		CollaborationDbUtils.update(conn, "UPDATE BRAIN_TOPIC SET UPDATED_AT = ? "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", now, ownerId, ownerType, topicId);
	}

	// current status, or "Topic not found"
	static String requireTopic(String ownerId, String ownerType, String topicId) {
		String status = CollaborationDbUtils.queryOne("SELECT STATUS FROM BRAIN_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?",
				rs -> CollaborationDbUtils.getString(rs, "STATUS"), ownerId, ownerType, topicId);
		if (status == null) {
			throw new IllegalArgumentException("Topic not found");
		}
		return status;
	}

	// ---- dormant ----

	// flips stale active topics to dormant; an owner edit (UPDATED_AT) counts as activity
	static void markDormant(String ownerId, String ownerType) {
		Timestamp now = CollaborationDbUtils.now();
		Timestamp cutoff = Timestamp.valueOf(now.toLocalDateTime().minusDays(DORMANT_AFTER_DAYS));
		CollaborationDbUtils.update("UPDATE BRAIN_TOPIC SET STATUS = ?, UPDATED_AT = ? "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = ? "
				+ "AND COALESCE(LAST_ACTIVITY_AT, CREATED_AT) < ? AND (UPDATED_AT IS NULL OR UPDATED_AT < ?)",
				DORMANT, now, ownerId, ownerType, ACTIVE, cutoff, cutoff);
	}

	// ---- stats ----

	private static int countThreads(String ownerId, String ownerType, String topicId) {
		return CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", ownerId, ownerType, topicId);
	}

	private static Map<String, Integer> countThreadsByTopic(String ownerId, String ownerType) {
		return countByTopic("SELECT TOPIC_ID, COUNT(*) FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? GROUP BY TOPIC_ID", ownerId, ownerType);
	}

	// open work items on threads linked to the topic
	private static final String OPEN_ITEMS_FROM = " FROM WORK_ITEM w JOIN BRAIN_THREAD_TOPIC tt "
			+ "ON tt.OWNER_ID = w.OWNER_ID AND tt.OWNER_TYPE = w.OWNER_TYPE AND tt.THREAD_ID = w.THREAD_ID "
			+ "WHERE w.OWNER_ID = ? AND w.OWNER_TYPE = ? AND w.STATUS = ?";

	private static int countOpenItems(String ownerId, String ownerType, String topicId) {
		return CollaborationDbUtils.count("SELECT COUNT(DISTINCT w.ITEM_ID)" + OPEN_ITEMS_FROM + " AND tt.TOPIC_ID = ?",
				ownerId, ownerType, "open", topicId);
	}

	private static Map<String, Integer> countOpenItemsByTopic(String ownerId, String ownerType) {
		return countByTopic("SELECT tt.TOPIC_ID, COUNT(DISTINCT w.ITEM_ID)" + OPEN_ITEMS_FROM + " GROUP BY tt.TOPIC_ID",
				ownerId, ownerType, "open");
	}

	private static Map<String, Integer> countByTopic(String sql, Object... params) {
		Map<String, Integer> counts = new HashMap<>();
		for (Pair<String, Integer> row : CollaborationDbUtils.query(sql,
				rs -> Pair.with(rs.getString(1), rs.getInt(2)), params)) {
			counts.put(row.getValue0(), row.getValue1());
		}
		return counts;
	}

	// ---- mapping ----

	private static Map<String, Object> mapSummary(ResultSet rs, Map<String, Integer> threads,
			Map<String, Integer> openItems) throws SQLException {
		String topicId = CollaborationDbUtils.getString(rs, "TOPIC_ID");
		Map<String, Object> stats = new LinkedHashMap<>();
		stats.put("threads", threads.getOrDefault(topicId, 0));
		stats.put("openItems", openItems.getOrDefault(topicId, 0));
		stats.put("lastActivity", CollaborationDbUtils.getTimestamp(rs, "LAST_ACTIVITY_AT"));

		Map<String, Object> row = new LinkedHashMap<>();
		row.put("id", topicId);
		row.put("name", CollaborationDbUtils.getString(rs, "NAME"));
		row.put("short", CollaborationDbUtils.getString(rs, "SHORT_NAME"));
		row.put("accountId", CollaborationDbUtils.getString(rs, "ACCOUNT_ID"));
		row.put("kind", CollaborationDbUtils.getString(rs, "KIND"));
		row.put("color", CollaborationDbUtils.getString(rs, "COLOR"));
		row.put("status", CollaborationDbUtils.getString(rs, "STATUS"));
		row.put("stats", stats);
		return row;
	}

	private static Map<String, Object> mapNote(ResultSet rs) throws SQLException {
		Map<String, Object> note = new LinkedHashMap<>();
		note.put("noteId", CollaborationDbUtils.getString(rs, "NOTE_ID"));
		note.put("kind", CollaborationDbUtils.getString(rs, "KIND"));
		note.put("text", CollaborationDbUtils.getString(rs, "TEXT"));
		note.put("status", CollaborationDbUtils.getString(rs, "STATE"));
		note.put("by", CollaborationDbUtils.getString(rs, "ORIGIN"));
		note.put("date", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
		note.put("source", CollaborationDbUtils.getString(rs, "SOURCE_REF"));
		return note;
	}

	private static Map<String, Object> mapPerson(ResultSet rs) throws SQLException {
		Map<String, Object> person = new LinkedHashMap<>();
		person.put("personId", CollaborationDbUtils.getString(rs, "PERSON_ID"));
		person.put("role", CollaborationDbUtils.getString(rs, "ROLE_LABEL"));
		person.put("engagement", CollaborationDbUtils.getInteger(rs, "ENGAGEMENT"));
		person.put("state", CollaborationDbUtils.getString(rs, "STATE"));
		person.put("origin", CollaborationDbUtils.getString(rs, "ORIGIN"));
		person.put("reason", CollaborationDbUtils.getString(rs, "REASON"));
		return person;
	}

	static void checkKind(String kind) {
		if (kind == null || !KINDS.contains(kind)) {
			throw new IllegalArgumentException("Unknown topic kind: " + kind);
		}
	}

	static void checkStatus(String status) {
		if (status == null || !STATUSES.contains(status)) {
			throw new IllegalArgumentException("Unknown topic status: " + status);
		}
	}
}
