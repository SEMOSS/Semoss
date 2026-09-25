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
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// Work items (WORK_ITEM) and their change history (WORK_ITEM_HISTORY) with undo.
// Scoring, the queue views, and the preview payload build on this (WORK-02, WORK-03, WORK-05).
public final class WorkItemUtils {

	public static final Set<String> ASK_TYPES = Set.of("reply", "approve", "attend", "review", "waiting_on", "errand",
			"fyi");
	public static final Set<String> STATUSES = Set.of("open", "waiting", "done", "dismissed", "snoozed");
	public static final Set<String> PRIORITIES = Set.of("P0", "P1", "P2", "P3");
	public static final Set<String> CHANNELS = Set.of("email", "teams", "calendar", "room", "task");
	public static final Set<String> CLOSED_REASONS = Set.of("replied", "responded", "deleted", "by_owner", "by_agent",
			"expired");

	public static final String YOU = BrainProfileUtils.YOU;
	public static final String BRAIN = "brain";
	public static final String ASSISTANT = "assistant";
	public static final Set<String> ACTORS = Set.of(YOU, BRAIN, ASSISTANT);

	public static final String OPEN = "open";
	public static final String WAITING = "waiting";
	public static final String SNOOZED = "snoozed";
	public static final String DONE = "done";
	public static final String DISMISSED = "dismissed";

	public static final String SORT_PRIORITY = "priority";
	public static final String SORT_RECEIVED = "received";
	public static final String SORT_CLOSED = "closed";

	private static final String LOCK = "work";
	// history FIELD of the row written when an item is created; never undone
	private static final String CREATED = "created";
	private static final String UNDO = "undo";

	// field -> column for everything an update can change, in history order
	private static final Map<String, String> COLUMNS = new LinkedHashMap<>();
	static {
		COLUMNS.put("status", "STATUS");
		COLUMNS.put("snoozeUntil", "SNOOZE_UNTIL");
		COLUMNS.put("closedReason", "CLOSED_REASON");
		COLUMNS.put("closedAt", "CLOSED_AT");
		COLUMNS.put("priority", "PRIORITY");
		COLUMNS.put("title", "TITLE");
		COLUMNS.put("dueAt", "DUE_AT");
		COLUMNS.put("assignee", "ASSIGNEE_PERSON_ID");
		COLUMNS.put("linkTopicId", "LINK_TOPIC_ID");
		COLUMNS.put("roomId", "ROOM_ID");
		COLUMNS.put("suggested", "SUGGESTED");
	}
	private static final Set<String> TIME_FIELDS = Set.of("snoozeUntil", "closedAt", "dueAt");
	private static final String BOOLEAN_FIELD = "suggested";
	// closedAt follows status, so callers do not set it
	public static final Set<String> EDITABLE = Set.of("status", "snoozeUntil", "closedReason", "priority", "title",
			"dueAt", "assignee", "linkTopicId", "roomId", "suggested");

	public static final Set<String> FILTER_KEYS = Set.of("statuses", "askTypes", "notAskTypes", "channel", "topicId",
			"threadId", "suggested", "assigned", "closedSince", "sort", "waitingOnOthers", "hideMuted");

	public static final Set<String> VIEWS = Set.of("needs_me", "waiting", "suggested", "done_today", "all");

	private static final String ITEM_COLUMNS = "w.ITEM_ID, w.THREAD_ID, w.SOURCE, w.ACTOR_TYPE, w.ACTOR_ID, "
			+ "w.ACTOR_NAME, w.TITLE, w.ASK_TYPE, w.ORIGIN, w.PRIORITY, w.SCORE, w.REASONS_JSON, w.DUE_AT, "
			+ "w.RECEIVED_AT, w.STATUS, w.CLOSED_REASON, w.CLOSED_AT, w.SNOOZE_UNTIL, w.ROOM_ID, "
			+ "w.ASSIGNEE_PERSON_ID, w.LINK_TOPIC_ID, w.SUGGESTED";

	private WorkItemUtils() {

	}

	// ---- read ----

	// filter keys (all optional): statuses, askTypes, notAskTypes (lists), channel, topicId (thread link or
	// linked topic), threadId, suggested, assigned (true: someone else's), closedSince (ISO), sort
	// (priority | received | closed). Snoozed items whose time has come reopen first.
	public static Map<String, Object> listItems(User user, Map<String, Object> filter, int limit, int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		wakeSnoozed(ownerId, ownerType);

		List<Object> params = new ArrayList<>();
		String where = where(ownerId, ownerType, filter == null ? Map.of() : filter, params);
		String sort = filter == null ? null : CollaborationDbUtils.asString(filter.get("sort"));
		String order;
		if (sort == null || SORT_PRIORITY.equals(sort)) {
			order = " ORDER BY COALESCE(w.PRIORITY, 'P9'), COALESCE(w.SCORE, -1) DESC, w.RECEIVED_AT DESC, w.ITEM_ID";
		} else if (SORT_RECEIVED.equals(sort)) {
			order = " ORDER BY w.RECEIVED_AT DESC, w.ITEM_ID";
		} else if (SORT_CLOSED.equals(sort)) {
			order = " ORDER BY w.CLOSED_AT DESC, w.ITEM_ID";
		} else {
			throw new IllegalArgumentException("Work item sort must be priority, received, or closed");
		}

		List<Map<String, Object>> items = CollaborationDbUtils.query(
				CollaborationDbUtils.page("SELECT " + ITEM_COLUMNS + " FROM WORK_ITEM w" + where + order, limit,
						offset),
				WorkItemUtils::mapItem, params.toArray());
		addTopicIds(ownerId, ownerType, items);

		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_ITEM w" + where, params.toArray()));
		return page;
	}

	// WorkListItems: one queue view (all = every item, for a client that filters itself), plus the collapsed
	// FYI count and the automated threads left out of the queue
	public static Map<String, Object> listView(User user, String view, String topicId, String channel, String sort,
			int limit, int offset) {
		String v = view == null ? "needs_me" : check(VIEWS, view, "view");
		Map<String, Object> filter = new LinkedHashMap<>();
		if (!"all".equals(v)) {
			filter.put("hideMuted", true);
		}
		switch (v) {
		case "needs_me" -> {
			filter.put("statuses", List.of(OPEN));
			filter.put("notAskTypes", List.of("fyi", "waiting_on"));
			filter.put("suggested", false);
			filter.put("assigned", false);
		}
		case "waiting" -> filter.put("waitingOnOthers", true);
		case "suggested" -> {
			filter.put("statuses", List.of(OPEN));
			filter.put("suggested", true);
		}
		case "done_today" -> {
			filter.put("statuses", List.of(DONE));
			filter.put("closedSince", startOfToday(user));
			filter.put("sort", SORT_CLOSED);
		}
		default -> {
		}
		}
		if (topicId != null) {
			filter.put("topicId", topicId);
		}
		if (channel != null) {
			filter.put("channel", channel);
		}
		if (sort != null) {
			filter.put("sort", sort);
		}
		Map<String, Object> page = listItems(user, filter, limit, offset);

		Map<String, Object> fyi = new LinkedHashMap<>();
		fyi.put("statuses", List.of(OPEN));
		fyi.put("askTypes", List.of("fyi"));
		fyi.put("hideMuted", true);
		page.put("fyiCount", countItems(user, fyi));
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		page.put("automatedSkippedCount", CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_THREAD "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND AUTOMATED = ?", owner.getValue0(), owner.getValue1(),
				true));
		return page;
	}

	// WorkGetItem: the item with its thread's summary, topic check, and people
	public static Map<String, Object> getItemPreview(User user, String itemId) {
		Map<String, Object> item = getItem(user, itemId);
		Map<String, Object> thread = BrainThreadUtils.getThread(user, (String) item.get("threadId"));
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> people = (List<Map<String, Object>>) thread.get("participants");
		int hidden = 0;
		for (Map<String, Object> person : people) {
			Object count = person.get("hiddenCount");
			hidden += count instanceof Number n ? n.intValue() : 0;
		}
		Map<String, Object> preview = new LinkedHashMap<>();
		preview.put("item", item);
		preview.put("threadSummary", thread.get("summary"));
		preview.put("hiddenCount", hidden);
		preview.put("topicCheck", thread.get("topicLinks"));
		preview.put("people", people);
		preview.put("assistantQuestions", new ArrayList<>());
		return preview;
	}

	// midnight in the owner's timezone (UTC when unset), as ISO
	private static String startOfToday(User user) {
		Object tz = BrainProfileUtils.getProfile(user).get("timezone");
		ZoneId zone = ZoneOffset.UTC;
		try {
			if (tz != null) {
				zone = ZoneId.of(String.valueOf(tz));
			}
		} catch (DateTimeException e) {
			zone = ZoneOffset.UTC;
		}
		LocalDateTime midnight = LocalDate.now(zone).atStartOfDay(zone).withZoneSameInstant(ZoneOffset.UTC)
				.toLocalDateTime();
		return CollaborationDbUtils.toIso(Timestamp.valueOf(midnight));
	}

	// same filter as listItems, count only (e.g. the collapsed FYI row)
	public static int countItems(User user, Map<String, Object> filter) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<Object> params = new ArrayList<>();
		String where = where(owner.getValue0(), owner.getValue1(), filter == null ? Map.of() : filter, params);
		return CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_ITEM w" + where, params.toArray());
	}

	public static Map<String, Object> getItem(User user, String itemId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> item = getItem(owner.getValue0(), owner.getValue1(), itemId);
		if (item == null) {
			throw new IllegalArgumentException("Work item not found");
		}
		return item;
	}

	// newest first, one entry per change
	public static List<Map<String, Object>> getHistory(User user, String itemId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireItem(ownerId, ownerType, itemId);
		List<Map<String, Object>> changes = new ArrayList<>();
		Map<String, Map<String, Object>> byId = new LinkedHashMap<>();
		for (Map<String, Object> row : CollaborationDbUtils.query("SELECT CHANGE_ID, CHANGED_BY, CHANGED_AT, FIELD, "
				+ "OLD_VALUE, NEW_VALUE, REASON, UNDO_OF FROM WORK_ITEM_HISTORY WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND ITEM_ID = ? ORDER BY CHANGED_AT DESC, CHANGE_ID, HISTORY_ID", rs -> {
					Map<String, Object> r = new LinkedHashMap<>();
					r.put("changeId", CollaborationDbUtils.getString(rs, "CHANGE_ID"));
					r.put("by", CollaborationDbUtils.getString(rs, "CHANGED_BY"));
					r.put("at", CollaborationDbUtils.getTimestamp(rs, "CHANGED_AT"));
					r.put("field", CollaborationDbUtils.getString(rs, "FIELD"));
					r.put("from", CollaborationDbUtils.getString(rs, "OLD_VALUE"));
					r.put("to", CollaborationDbUtils.getString(rs, "NEW_VALUE"));
					r.put("reason", CollaborationDbUtils.getString(rs, "REASON"));
					r.put("undoOf", CollaborationDbUtils.getString(rs, "UNDO_OF"));
					return r;
				}, ownerId, ownerType, itemId)) {
			Map<String, Object> change = byId.computeIfAbsent((String) row.get("changeId"), id -> {
				Map<String, Object> c = new LinkedHashMap<>();
				c.put("changeId", id);
				c.put("by", row.get("by"));
				c.put("at", row.get("at"));
				c.put("reason", row.get("reason"));
				c.put("undoOf", row.get("undoOf"));
				c.put("fields", new ArrayList<Map<String, Object>>());
				changes.add(c);
				return c;
			});
			Map<String, Object> field = new LinkedHashMap<>();
			field.put("field", row.get("field"));
			field.put("from", row.get("from"));
			field.put("to", row.get("to"));
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> fields = (List<Map<String, Object>>) change.get("fields");
			fields.add(field);
		}
		return changes;
	}

	// ---- create ----

	// ingest: one item per dedupeKey, so a retried delivery returns the first item; a muted thread gets none.
	// required: threadId, channel, title, askType, receivedAt, dedupeKey. optional: sourceRef, actorType,
	// actorId, actorName, priority, score, reasons, dueAt, linkTopicId, classifierVersion, status (open or
	// waiting), origin (brain or assistant), reason
	public static Map<String, Object> createFromIngest(String ownerId, String ownerType, Map<String, Object> item) {
		String dedupeKey = required(item, "dedupeKey");
		String threadId = required(item, "threadId");
		String channel = check(CHANNELS, required(item, "channel"), "channel");
		String title = title(required(item, "title"));
		String askType = check(ASK_TYPES, required(item, "askType"), "askType");
		Timestamp receivedAt = CollaborationDbUtils.toTimestamp(required(item, "receivedAt"), "receivedAt");
		String status = CollaborationDbUtils.asString(item.getOrDefault("status", OPEN));
		if (!OPEN.equals(status) && !WAITING.equals(status)) {
			throw new IllegalArgumentException("A new work item is open or waiting");
		}
		String origin = CollaborationDbUtils.asString(item.getOrDefault("origin", BRAIN));
		if (!BRAIN.equals(origin) && !ASSISTANT.equals(origin)) {
			throw new IllegalArgumentException("Ingest items come from brain or assistant");
		}
		String priority = optional(PRIORITIES, item.get("priority"), "priority");
		Integer score = item.get("score") == null ? null : CollaborationDbUtils.toInt(item.get("score"), "score");
		String actorType = CollaborationDbUtils.asString(item.get("actorType"));
		String actorId = CollaborationDbUtils.asString(item.get("actorId"));
		String linkTopicId = CollaborationDbUtils.asString(item.get("linkTopicId"));

		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Map<String, Object> result = new LinkedHashMap<>();
			String existing = CollaborationDbUtils.queryOne("SELECT ITEM_ID FROM WORK_ITEM WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND DEDUPE_KEY = ?", rs -> rs.getString(1), ownerId, ownerType, dedupeKey);
			if (existing != null) {
				result.put("created", false);
				result.put("item", getItem(ownerId, ownerType, existing));
				return result;
			}
			Boolean muted = CollaborationDbUtils.queryOne("SELECT MUTED FROM BRAIN_THREAD WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> CollaborationDbUtils.getBoolean(rs, "MUTED"),
					ownerId, ownerType, threadId);
			if (muted == null) {
				throw new IllegalArgumentException("Thread not found");
			}
			if (muted) {
				result.put("created", false);
				result.put("skipped", BrainRulesGate.MUTED);
				return result;
			}
			if ("person".equals(actorType)) {
				BrainPeopleUtils.requirePerson(ownerId, ownerType, actorId);
			}
			if (linkTopicId != null) {
				BrainTopicUtils.requireTopic(ownerId, ownerType, linkTopicId);
			}

			String itemId = CollaborationDbUtils.deterministicId(ownerId, ownerType, "work_item", dedupeKey);
			insert(ownerId, ownerType, itemId, threadId, channel, CollaborationDbUtils.asString(item.get("sourceRef")),
					actorType, actorId, CollaborationDbUtils.asString(item.get("actorName")), title, askType, origin,
					priority, score, reasons(item.get("reasons")), CollaborationDbUtils.toTimestamp(item.get("dueAt"),
							"dueAt"),
					receivedAt, status, null, linkTopicId,
					CollaborationDbUtils.asString(item.get("classifierVersion")), dedupeKey, ASSISTANT.equals(origin),
					origin, CollaborationDbUtils.asString(item.getOrDefault("reason", "ingest")));
			result.put("created", true);
			result.put("item", getItem(ownerId, ownerType, itemId));
			return result;
		}
	}

	// WorkCreateItem: the owner's own item (priority P2), or an assistant suggestion that stays suggested until
	// the owner accepts it (suggested=false)
	public static Map<String, Object> createItem(User user, String threadId, String title, String askType,
			String assignee, String dueAt, boolean byAssistant) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		BrainPeopleUtils.requirePerson(ownerId, ownerType, assignee);
		check(ASK_TYPES, askType, "askType");
		String origin = byAssistant ? ASSISTANT : YOU;
		String itemId = UUID.randomUUID().toString();
		Timestamp now = CollaborationDbUtils.now();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			insert(ownerId, ownerType, itemId, threadId, "task", null, byAssistant ? ASSISTANT : YOU,
					byAssistant ? ASSISTANT : null, null, title(title), askType, origin, "P2", null,
					byAssistant ? List.of("Suggested by assistant") : List.of(),
					CollaborationDbUtils.toTimestamp(dueAt, "dueAt"), now, OPEN, assignee, null, null, null,
					byAssistant, origin, null);
		}
		return getItem(ownerId, ownerType, itemId);
	}

	// ---- update ----

	// WorkUpdateItem from the owner
	public static Map<String, Object> updateItem(User user, String itemId, Map<String, Object> changes,
			String reason) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return updateItem(owner.getValue0(), owner.getValue1(), itemId, changes, YOU, reason);
	}

	// changes: any of EDITABLE. snoozeUntil alone means snoozed; leaving snoozed clears it; done or dismissed
	// sets closedAt and closedReason (by_owner or by_agent unless given); reopening clears both.
	// Returns the item with the changeId to pass to undo; no change writes no history.
	public static Map<String, Object> updateItem(String ownerId, String ownerType, String itemId,
			Map<String, Object> changes, String actor, String reason) {
		check(ACTORS, actor, "actor");
		if (changes == null || changes.isEmpty()) {
			throw new IllegalArgumentException("Nothing to update");
		}
		for (String key : changes.keySet()) {
			if (!EDITABLE.contains(key)) {
				throw new IllegalArgumentException("Unknown work item field: " + key);
			}
		}
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Map<String, String> current = currentValues(ownerId, ownerType, itemId);
			Map<String, String> next = next(ownerId, ownerType, current, changes, actor);
			String changeId = apply(ownerId, ownerType, itemId, current, next, actor, reason, null);
			Map<String, Object> item = getItem(ownerId, ownerType, itemId);
			if (changeId != null) {
				item.put("changeId", changeId);
			}
			return item;
		}
	}

	// reverts one change (default: the actor's latest that is not undone). Refused when a later change touched
	// the same fields. The undo is itself a history entry.
	public static Map<String, Object> undo(User user, String itemId, String changeId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return undo(owner.getValue0(), owner.getValue1(), itemId, changeId, YOU);
	}

	public static Map<String, Object> undo(String ownerId, String ownerType, String itemId, String changeId,
			String actor) {
		check(ACTORS, actor, "actor");
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Map<String, String> current = currentValues(ownerId, ownerType, itemId);
			String target = changeId;
			if (target == null) {
				target = CollaborationDbUtils.queryOne("SELECT h.CHANGE_ID FROM WORK_ITEM_HISTORY h "
						+ "WHERE h.OWNER_ID = ? AND h.OWNER_TYPE = ? AND h.ITEM_ID = ? AND h.CHANGED_BY = ? "
						+ "AND h.FIELD <> ? AND h.UNDO_OF IS NULL AND NOT EXISTS (SELECT 1 FROM WORK_ITEM_HISTORY u "
						+ "WHERE u.OWNER_ID = h.OWNER_ID AND u.OWNER_TYPE = h.OWNER_TYPE AND u.ITEM_ID = h.ITEM_ID "
						+ "AND u.UNDO_OF = h.CHANGE_ID) ORDER BY h.CHANGED_AT DESC, h.CHANGE_ID FETCH FIRST 1 ROWS ONLY",
						rs -> rs.getString(1), ownerId, ownerType, itemId, actor, CREATED);
				if (target == null) {
					throw new IllegalArgumentException("Nothing to undo");
				}
			}
			List<String[]> rows = CollaborationDbUtils.query("SELECT FIELD, OLD_VALUE, NEW_VALUE, UNDO_OF "
					+ "FROM WORK_ITEM_HISTORY WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ITEM_ID = ? AND CHANGE_ID = ?",
					rs -> new String[] { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4) }, ownerId,
					ownerType, itemId, target);
			if (rows.isEmpty()) {
				throw new IllegalArgumentException("Change not found");
			}
			if (CollaborationDbUtils.exists("SELECT 1 FROM WORK_ITEM_HISTORY WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND ITEM_ID = ? AND UNDO_OF = ?", ownerId, ownerType, itemId, target)) {
				throw new IllegalArgumentException("Change already undone");
			}
			Map<String, String> next = new LinkedHashMap<>(current);
			for (String[] row : rows) {
				if (CREATED.equals(row[0]) || row[3] != null) {
					throw new IllegalArgumentException("This change cannot be undone");
				}
				if (!Objects.equals(current.get(row[0]), row[2])) {
					throw new IllegalArgumentException("The item changed since; undo not applied");
				}
				next.put(row[0], row[1]);
			}
			String undoId = apply(ownerId, ownerType, itemId, current, next, actor, UNDO, target);
			Map<String, Object> item = getItem(ownerId, ownerType, itemId);
			item.put("changeId", undoId);
			return item;
		}
	}

	// ---- ingest events ----

	// the owner replied on the thread: close open reply items received at or before the send
	public static List<String> closeOnReply(String ownerId, String ownerType, String threadId, String sentAt) {
		return closeWhere(ownerId, ownerType, " AND THREAD_ID = ? AND ASK_TYPE = ? AND RECEIVED_AT <= ?",
				List.of(threadId, "reply", CollaborationDbUtils.toTimestamp(sentAt, "sentAt")), DONE, "replied",
				"owner replied");
	}

	// the source message was deleted: dismiss the items it raised
	public static List<String> dismissForMessage(String ownerId, String ownerType, String sourceRef) {
		return closeWhere(ownerId, ownerType, " AND SOURCE_REF = ?", List.of(sourceRef), DISMISSED, "deleted",
				"message deleted");
	}

	// snoozed items whose time has come go back to open
	static void wakeSnoozed(String ownerId, String ownerType) {
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			for (String itemId : CollaborationDbUtils.query("SELECT ITEM_ID FROM WORK_ITEM WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND STATUS = ? AND SNOOZE_UNTIL <= ?", rs -> rs.getString(1), ownerId,
					ownerType, SNOOZED, CollaborationDbUtils.now())) {
				Map<String, String> current = currentValues(ownerId, ownerType, itemId);
				Map<String, String> next = new LinkedHashMap<>(current);
				next.put("status", OPEN);
				next.put("snoozeUntil", null);
				apply(ownerId, ownerType, itemId, current, next, BRAIN, "snooze ended", null);
			}
		}
	}

	private static List<String> closeWhere(String ownerId, String ownerType, String and, List<Object> andParams,
			String status, String closedReason, String reason) {
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType, OPEN, WAITING, SNOOZED));
		params.addAll(andParams);
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			List<String> ids = CollaborationDbUtils.query("SELECT ITEM_ID FROM WORK_ITEM WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND STATUS IN (?, ?, ?)" + and + " ORDER BY ITEM_ID", rs -> rs.getString(1),
					params.toArray());
			for (String itemId : ids) {
				Map<String, String> current = currentValues(ownerId, ownerType, itemId);
				Map<String, Object> changes = new LinkedHashMap<>();
				changes.put("status", status);
				changes.put("closedReason", closedReason);
				apply(ownerId, ownerType, itemId, current, next(ownerId, ownerType, current, changes, BRAIN), BRAIN,
						reason, null);
			}
			return ids;
		}
	}

	// ---- change mechanics ----

	// the item's changeable fields as history strings: ISO times, "true"/"false", ids
	private static Map<String, String> currentValues(String ownerId, String ownerType, String itemId) {
		Map<String, String> values = CollaborationDbUtils.queryOne("SELECT " + String.join(", ", COLUMNS.values())
				+ " FROM WORK_ITEM WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ITEM_ID = ?", rs -> {
					Map<String, String> v = new LinkedHashMap<>();
					for (Map.Entry<String, String> e : COLUMNS.entrySet()) {
						if (TIME_FIELDS.contains(e.getKey())) {
							v.put(e.getKey(), CollaborationDbUtils.getTimestamp(rs, e.getValue()));
						} else if (BOOLEAN_FIELD.equals(e.getKey())) {
							v.put(e.getKey(), String.valueOf(Boolean.TRUE.equals(
									CollaborationDbUtils.getBoolean(rs, e.getValue()))));
						} else {
							v.put(e.getKey(), CollaborationDbUtils.getString(rs, e.getValue()));
						}
					}
					return v;
				}, ownerId, ownerType, itemId);
		if (values == null) {
			throw new IllegalArgumentException("Work item not found");
		}
		return values;
	}

	// validated next state for the requested changes, with the status rules applied
	private static Map<String, String> next(String ownerId, String ownerType, Map<String, String> current,
			Map<String, Object> changes, String actor) {
		Map<String, String> next = new LinkedHashMap<>(current);
		for (Map.Entry<String, Object> e : changes.entrySet()) {
			String key = e.getKey();
			String value = CollaborationDbUtils.asString(e.getValue());
			switch (key) {
			case "status" -> next.put(key, check(STATUSES, value, "status"));
			case "priority" -> next.put(key, optional(PRIORITIES, value, "priority"));
			case "closedReason" -> next.put(key, optional(CLOSED_REASONS, value, "closedReason"));
			case "title" -> next.put(key, title(value));
			case "snoozeUntil", "dueAt" -> next.put(key, CollaborationDbUtils.toIso(
					CollaborationDbUtils.toTimestamp(value, key)));
			case "assignee" -> {
				BrainPeopleUtils.requirePerson(ownerId, ownerType, value);
				next.put(key, value);
			}
			case "linkTopicId" -> {
				if (value != null) {
					BrainTopicUtils.requireTopic(ownerId, ownerType, value);
				}
				next.put(key, value);
			}
			case "suggested" -> next.put(key, String.valueOf(value != null && Boolean.parseBoolean(value.trim())));
			default -> next.put(key, value);
			}
		}

		if (changes.containsKey("snoozeUntil") && !changes.containsKey("status") && next.get("snoozeUntil") != null) {
			next.put("status", SNOOZED);
		}
		String status = next.get("status");
		if (SNOOZED.equals(status)) {
			Timestamp until = CollaborationDbUtils.toTimestamp(next.get("snoozeUntil"), "snoozeUntil");
			if (until == null || !until.after(CollaborationDbUtils.now())) {
				throw new IllegalArgumentException("Snoozing needs a snoozeUntil in the future");
			}
		} else {
			next.put("snoozeUntil", null);
		}
		boolean closing = DONE.equals(status) || DISMISSED.equals(status);
		boolean wasClosed = DONE.equals(current.get("status")) || DISMISSED.equals(current.get("status"));
		if (closing) {
			if (!wasClosed) {
				next.put("closedAt", CollaborationDbUtils.toIso(CollaborationDbUtils.now()));
			}
			if (!changes.containsKey("closedReason") && (!wasClosed || next.get("closedReason") == null)) {
				next.put("closedReason", YOU.equals(actor) ? "by_owner" : "by_agent");
			}
		} else {
			if (changes.get("closedReason") != null) {
				throw new IllegalArgumentException("closedReason goes with done or dismissed");
			}
			next.put("closedAt", null);
			next.put("closedReason", null);
		}
		return next;
	}

	// writes the differing fields and one history row each under a new change id; null when nothing differs
	private static String apply(String ownerId, String ownerType, String itemId, Map<String, String> current,
			Map<String, String> next, String actor, String reason, String undoOf) {
		List<String> changed = new ArrayList<>();
		for (String field : COLUMNS.keySet()) {
			if (!Objects.equals(current.get(field), next.get(field))) {
				changed.add(field);
			}
		}
		if (changed.isEmpty()) {
			return null;
		}
		String changeId = UUID.randomUUID().toString();
		Timestamp now = CollaborationDbUtils.now();
		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		for (String field : changed) {
			CollaborationDbUtils.addSet(sets, params, COLUMNS.get(field), toColumnValue(field, next.get(field)));
		}
		CollaborationDbUtils.addSet(sets, params, "UPDATED_AT", now);
		params.addAll(List.of(ownerId, ownerType, itemId));
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "UPDATE WORK_ITEM SET " + String.join(", ", sets)
					+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ITEM_ID = ?", params.toArray());
			for (String field : changed) {
				insertHistory(conn, ownerId, ownerType, itemId, changeId, actor, now, field, current.get(field),
						next.get(field), reason, undoOf);
			}
		});
		return changeId;
	}

	private static Object toColumnValue(String field, String value) {
		if (value == null) {
			return null;
		}
		if (TIME_FIELDS.contains(field)) {
			return CollaborationDbUtils.toTimestamp(value, field);
		}
		if (BOOLEAN_FIELD.equals(field)) {
			return Boolean.valueOf(value);
		}
		return value;
	}

	private static void insert(String ownerId, String ownerType, String itemId, String threadId, String channel,
			String sourceRef, String actorType, String actorId, String actorName, String title, String askType,
			String origin, String priority, Integer score, List<String> reasons, Timestamp dueAt, Timestamp receivedAt,
			String status, String assignee, String linkTopicId, String classifierVersion, String dedupeKey,
			boolean suggested, String actor, String reason) {
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "INSERT INTO WORK_ITEM (OWNER_ID, OWNER_TYPE, ITEM_ID, THREAD_ID, "
					+ "SOURCE, SOURCE_REF, ACTOR_TYPE, ACTOR_ID, ACTOR_NAME, TITLE, ASK_TYPE, ORIGIN, PRIORITY, SCORE, "
					+ "REASONS_JSON, DUE_AT, RECEIVED_AT, PROCESSED_AT, STATUS, ASSIGNEE_PERSON_ID, LINK_TOPIC_ID, "
					+ "CLASSIFIER_VERSION, DEDUPE_KEY, SUGGESTED, CREATED_AT, UPDATED_AT) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId,
					ownerType, itemId, threadId, channel, sourceRef, actorType, actorId, actorName, title, askType,
					origin, priority, score, CollaborationDbUtils.toJson(reasons), dueAt, receivedAt, now, status,
					assignee, linkTopicId, classifierVersion, dedupeKey, suggested, now, now);
			insertHistory(conn, ownerId, ownerType, itemId, UUID.randomUUID().toString(), actor, now, CREATED, null,
					status, reason, null);
		});
	}

	private static void insertHistory(Connection conn, String ownerId, String ownerType, String itemId,
			String changeId, String actor, Timestamp at, String field, String oldValue, String newValue,
			String reason, String undoOf) throws SQLException {
		CollaborationDbUtils.update(conn, "INSERT INTO WORK_ITEM_HISTORY (OWNER_ID, OWNER_TYPE, HISTORY_ID, ITEM_ID, "
				+ "CHANGED_BY, CHANGED_AT, FIELD, OLD_VALUE, NEW_VALUE, REASON, CHANGE_ID, UNDO_OF) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, UUID.randomUUID().toString(),
				itemId, actor, at, field, oldValue, newValue, reason, changeId, undoOf);
	}

	// ---- helpers ----

	static Map<String, Object> getItem(String ownerId, String ownerType, String itemId) {
		Map<String, Object> item = CollaborationDbUtils.queryOne("SELECT " + ITEM_COLUMNS + " FROM WORK_ITEM w "
				+ "WHERE w.OWNER_ID = ? AND w.OWNER_TYPE = ? AND w.ITEM_ID = ?", WorkItemUtils::mapItem, ownerId,
				ownerType, itemId);
		if (item != null) {
			addTopicIds(ownerId, ownerType, List.of(item));
		}
		return item;
	}

	static void requireItem(String ownerId, String ownerType, String itemId) {
		if (!CollaborationDbUtils.exists("SELECT 1 FROM WORK_ITEM WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND ITEM_ID = ?", ownerId, ownerType, itemId)) {
			throw new IllegalArgumentException("Work item not found");
		}
	}

	private static String where(String ownerId, String ownerType, Map<String, Object> filter, List<Object> params) {
		for (String key : filter.keySet()) {
			if (!FILTER_KEYS.contains(key)) {
				throw new IllegalArgumentException("Unknown work item filter: " + key);
			}
		}
		StringBuilder where = new StringBuilder(" WHERE w.OWNER_ID = ? AND w.OWNER_TYPE = ?");
		params.add(ownerId);
		params.add(ownerType);
		inList(where, params, "w.STATUS", false, filter.get("statuses"), STATUSES, "status");
		inList(where, params, "w.ASK_TYPE", false, filter.get("askTypes"), ASK_TYPES, "askType");
		inList(where, params, "w.ASK_TYPE", true, filter.get("notAskTypes"), ASK_TYPES, "askType");
		String channel = CollaborationDbUtils.asString(filter.get("channel"));
		if (channel != null) {
			where.append(" AND w.SOURCE = ?");
			params.add(check(CHANNELS, channel, "channel"));
		}
		String threadId = CollaborationDbUtils.asString(filter.get("threadId"));
		if (threadId != null) {
			where.append(" AND w.THREAD_ID = ?");
			params.add(threadId);
		}
		String topicId = CollaborationDbUtils.asString(filter.get("topicId"));
		if (topicId != null) {
			where.append(" AND (w.LINK_TOPIC_ID = ? OR EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC l "
					+ "WHERE l.OWNER_ID = w.OWNER_ID AND l.OWNER_TYPE = w.OWNER_TYPE AND l.THREAD_ID = w.THREAD_ID "
					+ "AND l.TOPIC_ID = ?))");
			params.add(topicId);
			params.add(topicId);
		}
		Object suggested = filter.get("suggested");
		if (suggested != null) {
			boolean want = Boolean.parseBoolean(String.valueOf(suggested));
			where.append(want ? " AND w.SUGGESTED = ?" : " AND (w.SUGGESTED IS NULL OR w.SUGGESTED = ?)");
			params.add(want);
		}
		Object assigned = filter.get("assigned");
		if (assigned != null) {
			where.append(Boolean.parseBoolean(String.valueOf(assigned)) ? " AND w.ASSIGNEE_PERSON_ID IS NOT NULL"
					: " AND w.ASSIGNEE_PERSON_ID IS NULL");
		}
		if (Boolean.parseBoolean(String.valueOf(filter.get("waitingOnOthers")))) {
			where.append(" AND w.STATUS IN (?, ?) AND (w.STATUS = ? OR w.ASK_TYPE = ? OR w.ASSIGNEE_PERSON_ID IS NOT NULL)");
			params.addAll(List.of(OPEN, WAITING, WAITING, "waiting_on"));
		}
		if (Boolean.parseBoolean(String.valueOf(filter.get("hideMuted")))) {
			where.append(" AND NOT EXISTS (SELECT 1 FROM BRAIN_THREAD mt WHERE mt.OWNER_ID = w.OWNER_ID "
					+ "AND mt.OWNER_TYPE = w.OWNER_TYPE AND mt.THREAD_ID = w.THREAD_ID AND mt.MUTED = ?)");
			params.add(true);
		}
		Timestamp closedSince = CollaborationDbUtils.toTimestamp(filter.get("closedSince"), "closedSince");
		if (closedSince != null) {
			where.append(" AND w.CLOSED_AT >= ?");
			params.add(closedSince);
		}
		return where.toString();
	}

	@SuppressWarnings("unchecked")
	private static void inList(StringBuilder where, List<Object> params, String column, boolean not, Object value,
			Set<String> allowed, String name) {
		if (value == null) {
			return;
		}
		List<String> values = value instanceof List<?> list ? CollaborationDbUtils.toStringList((List<Object>) list)
				: List.of(String.valueOf(value));
		if (values.isEmpty()) {
			return;
		}
		for (String v : values) {
			check(allowed, v, name);
		}
		where.append(" AND ").append(column).append(not ? " NOT IN (" : " IN (")
				.append(CollaborationDbUtils.placeholders(values.size())).append(")");
		params.addAll(values);
	}

	// the thread's topics, primary first; one query for the page
	private static void addTopicIds(String ownerId, String ownerType, List<Map<String, Object>> items) {
		if (items.isEmpty()) {
			return;
		}
		List<String> threadIds = new ArrayList<>();
		for (Map<String, Object> item : items) {
			item.put("topicIds", new ArrayList<String>());
			if (!threadIds.contains(item.get("threadId"))) {
				threadIds.add((String) item.get("threadId"));
			}
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(threadIds);
		Map<String, List<String>> byThread = new LinkedHashMap<>();
		for (Pair<String, String> link : CollaborationDbUtils.query("SELECT THREAD_ID, TOPIC_ID FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IN ("
				+ CollaborationDbUtils.placeholders(threadIds.size()) + ") ORDER BY IS_PRIMARY DESC, CONFIDENCE DESC, "
				+ "TOPIC_ID", rs -> Pair.with(rs.getString(1), rs.getString(2)), params.toArray())) {
			byThread.computeIfAbsent(link.getValue0(), k -> new ArrayList<>()).add(link.getValue1());
		}
		for (Map<String, Object> item : items) {
			item.put("topicIds", byThread.getOrDefault(item.get("threadId"), new ArrayList<>()));
		}
	}

	private static Map<String, Object> mapItem(ResultSet rs) throws SQLException {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("id", CollaborationDbUtils.getString(rs, "ITEM_ID"));
		item.put("threadId", CollaborationDbUtils.getString(rs, "THREAD_ID"));
		item.put("channel", CollaborationDbUtils.getString(rs, "SOURCE"));
		item.put("actorId", CollaborationDbUtils.getString(rs, "ACTOR_ID"));
		item.put("actorName", CollaborationDbUtils.getString(rs, "ACTOR_NAME"));
		item.put("title", CollaborationDbUtils.getString(rs, "TITLE"));
		item.put("askType", CollaborationDbUtils.getString(rs, "ASK_TYPE"));
		item.put("priority", CollaborationDbUtils.getString(rs, "PRIORITY"));
		item.put("score", CollaborationDbUtils.getInteger(rs, "SCORE"));
		item.put("reasons", CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "REASONS_JSON")));
		item.put("due", CollaborationDbUtils.getTimestamp(rs, "DUE_AT"));
		item.put("received", CollaborationDbUtils.getTimestamp(rs, "RECEIVED_AT"));
		item.put("status", CollaborationDbUtils.getString(rs, "STATUS"));
		if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "SUGGESTED"))) {
			item.put("suggested", true);
		}
		item.put("origin", CollaborationDbUtils.getString(rs, "ORIGIN"));
		item.put("snoozeUntil", CollaborationDbUtils.getTimestamp(rs, "SNOOZE_UNTIL"));
		item.put("closedReason", CollaborationDbUtils.getString(rs, "CLOSED_REASON"));
		item.put("closedAt", CollaborationDbUtils.getTimestamp(rs, "CLOSED_AT"));
		item.put("assignee", CollaborationDbUtils.getString(rs, "ASSIGNEE_PERSON_ID"));
		item.put("linkTopicId", CollaborationDbUtils.getString(rs, "LINK_TOPIC_ID"));
		item.put("roomId", CollaborationDbUtils.getString(rs, "ROOM_ID"));
		return item;
	}

	@SuppressWarnings("unchecked")
	private static List<String> reasons(Object value) {
		if (value == null) {
			return List.of();
		}
		if (value instanceof List<?> list) {
			return CollaborationDbUtils.toStringList((List<Object>) list);
		}
		throw new IllegalArgumentException("reasons must be a list");
	}

	private static String title(String value) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("A work item needs a title");
		}
		String title = value.trim();
		if (title.length() > 255) {
			throw new IllegalArgumentException("A work item title is at most 255 characters");
		}
		return title;
	}

	private static String required(Map<String, Object> map, String key) {
		String value = CollaborationDbUtils.asString(map.get(key));
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Work item needs " + key);
		}
		return value;
	}

	private static String check(Set<String> allowed, String value, String name) {
		if (value == null || !allowed.contains(value)) {
			throw new IllegalArgumentException("Unknown " + name + ": " + value);
		}
		return value;
	}

	private static String optional(Set<String> allowed, Object value, String name) {
		String text = CollaborationDbUtils.asString(value);
		return text == null ? null : check(allowed, text, name);
	}
}
