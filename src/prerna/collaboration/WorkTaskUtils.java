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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// Work items (WORK_TASK): an outcome the owner is driving.
// Steps live in WORK_THREAD_STEP with TASK_ID set; a step with no task is a loose step under its topic.
// Plan: AGENTS_TASKS/teamwork/wiki/work-items-plan-2026-11-02.md
public final class WorkTaskUtils {

	public static final String WORK = "work";
	// discussion lists are parked (owner, 2026-11-02); only work items for now
	public static final Set<String> KINDS = Set.of(WORK);

	public static final String ACTIVE = "active";
	public static final String WAITING = "waiting";
	public static final String BLOCKED = "blocked";
	public static final String DONE = "done";
	public static final String DROPPED = "dropped";
	public static final Set<String> STATUSES = Set.of(ACTIVE, WAITING, BLOCKED, DONE, DROPPED);
	private static final Set<String> OPEN_STATUSES = Set.of(ACTIVE, WAITING, BLOCKED);
	private static final Set<String> CLOSED_STATUSES = Set.of(DONE, DROPPED);

	public static final Set<String> PRIORITIES = Set.of("high", "medium", "low");
	public static final Set<String> UPDATE_KINDS = Set.of("update", "decision", "ask", "date", "status", "note");

	// who made a row: the owner by hand, the assistant in chat, or Brain from mail
	public static final String OWNER = "owner";
	public static final String ASSISTANT = "assistant";
	public static final String BRAIN = WorkItemUtils.BRAIN;

	public static final String PROPOSED = "proposed";
	public static final String ACCEPTED = "accepted";

	static final Set<String> STEP_OPEN = Set.of("open", "waiting");

	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";
	private static final String TASK_COLUMNS = "TASK_ID, KIND, TITLE, DESCRIPTION, STATUS, ONGOING, NEXT_STEP, "
			+ "WAITING_ON_PERSON_ID, PERSON_ID, DUE_AT, PRIORITY, ORIGIN, STATE, CREATED_AT, UPDATED_AT, CLOSED_AT, SORT_ORDER, ROOM_ID";
	private static final String STEP_COLUMNS = "STEP_ID, THREAD_ID, TASK_ID, TEXT, KIND, STATUS, STEP_OWNER_ID, "
			+ "DUE_AT, LINK_TOPIC_ID, ORIGIN, NOTE, CLOSED_BY, CREATED_AT, UPDATED_AT";
	private static final int MAX_TITLE = 255;
	private static final int LIST_LIMIT = 100;
	private static final int LOOSE_LIMIT = 50;

	private WorkTaskUtils() {
	}

	// ---- reading ----

	/**
	 * The owner's work: open work items with their open steps, and loose steps (no
	 * work item) that are the owner's. Brain's own loose steps from mail are left
	 * out unless includeSuggested, since most are noise until reconcile groups
	 * them. filter: status (open, done, all), kind, topicId, personId,
	 * includeSuggested.
	 */
	public static Map<String, Object> listMyWork(User user, Map<String, Object> filter) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String self = selfId(ownerId, ownerType);
		String status = lower(filter.get("status"));
		String kind = checked(lower(filter.get("kind")), KINDS, "kind");
		String topicId = blankToNull(filter.get("topicId"));
		String personId = blankToNull(filter.get("personId"));
		boolean includeSuggested = Boolean.TRUE.equals(filter.get("includeSuggested"));

		StringBuilder sql = new StringBuilder("SELECT " + TASK_COLUMNS + " FROM WORK_TASK t" + OWNED);
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		Set<String> statuses = status == null || "open".equals(status) ? OPEN_STATUSES
				: "done".equals(status) || "closed".equals(status) ? CLOSED_STATUSES : null;
		if (statuses != null) {
			sql.append(" AND STATUS IN (").append(CollaborationDbUtils.placeholders(statuses.size())).append(")");
			params.addAll(statuses);
		}
		if (kind != null) {
			sql.append(" AND KIND = ?");
			params.add(kind);
		}
		if (personId != null) {
			sql.append(" AND PERSON_ID = ?");
			params.add(personId);
		}
		if (topicId != null) {
			sql.append(" AND TASK_ID IN (SELECT TASK_ID FROM WORK_TASK_TOPIC" + OWNED + " AND TOPIC_ID = ?)");
			params.addAll(List.of(ownerId, ownerType, topicId));
		}
		// the owner's order first; the rest newest first
		sql.append(" ORDER BY CASE WHEN SORT_ORDER IS NULL THEN 1 ELSE 0 END, SORT_ORDER, UPDATED_AT DESC");
		List<Map<String, Object>> tasks = CollaborationDbUtils.query(sql.toString(), WorkTaskUtils::mapTask,
				params.toArray());
		if (tasks.size() > LIST_LIMIT) {
			tasks = new ArrayList<>(tasks.subList(0, LIST_LIMIT));
		}
		for (Map<String, Object> task : tasks) {
			String taskId = (String) task.get("id");
			task.put("topics", topics(ownerId, ownerType, taskId));
			List<Map<String, Object>> steps = steps(ownerId, ownerType, taskId);
			task.put("stepCount", steps.size());
			steps.removeIf(step -> !STEP_OPEN.contains(step.get("status")));
			task.put("openSteps", steps);
			// the owner's when they have an open step; the rest someone else is driving
			// a step nobody was named for (a group ask) counts as the owner's
			task.put("mine", steps.stream()
					.anyMatch(step -> step.get("assigneeId") == null || step.get("assigneeId").equals(self)));
		}

		// loose steps: the owner's open ones with no work item
		StringBuilder loose = new StringBuilder("SELECT " + STEP_COLUMNS + " FROM WORK_THREAD_STEP" + OWNED
				+ " AND TASK_ID IS NULL AND STATUS = ? AND (STEP_OWNER_ID IS NULL OR STEP_OWNER_ID = ?)");
		List<Object> looseParams = new ArrayList<>(List.of(ownerId, ownerType, "open", self == null ? "" : self));
		if (!includeSuggested) {
			loose.append(" AND (ORIGIN IS NULL OR ORIGIN <> ?)");
			looseParams.add(BRAIN);
		}
		if (topicId != null) {
			loose.append(" AND LINK_TOPIC_ID = ?");
			looseParams.add(topicId);
		}
		loose.append(" ORDER BY DUE_AT, CREATED_AT DESC");
		List<Map<String, Object>> looseSteps = kind != null || personId != null ? List.of()
				: CollaborationDbUtils.query(loose.toString(), WorkTaskUtils::mapStep, looseParams.toArray());
		if (looseSteps.size() > LOOSE_LIMIT) {
			looseSteps = new ArrayList<>(looseSteps.subList(0, LOOSE_LIMIT));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("workItems", tasks);
		out.put("looseSteps", looseSteps);
		putIfNotNull(out, "selfId", self);
		if (!includeSuggested) {
			out.put("suggestedStepCount",
					CollaborationDbUtils.queryOne("SELECT COUNT(*) AS N FROM WORK_THREAD_STEP" + OWNED
							+ " AND TASK_ID IS NULL AND STATUS = ? AND ORIGIN = ? AND (STEP_OWNER_ID IS NULL OR "
							+ "STEP_OWNER_ID = ?)", rs -> rs.getInt("N"), ownerId, ownerType, "open", BRAIN,
							self == null ? "" : self));
		}
		return out;
	}

	/**
	 * Open steps (open or waiting) for reconcile to match repeats and done claims
	 * against: any on threadIds, and the loose ones under topicId when it is given.
	 * Newest first, at most max.
	 */
	static List<Map<String, Object>> openSteps(String ownerId, String ownerType, String topicId,
			Collection<String> threadIds, int max) {
		List<String> where = new ArrayList<>();
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType, "open", "waiting"));
		if (!threadIds.isEmpty()) {
			where.add("THREAD_ID IN (" + CollaborationDbUtils.placeholders(threadIds.size()) + ")");
			params.addAll(threadIds);
		}
		if (topicId != null) {
			where.add("(TASK_ID IS NULL AND LINK_TOPIC_ID = ?)");
			params.add(topicId);
		}
		if (where.isEmpty()) {
			return new ArrayList<>();
		}
		return CollaborationDbUtils.query(CollaborationDbUtils.page("SELECT " + STEP_COLUMNS + " FROM WORK_THREAD_STEP"
				+ OWNED + " AND STATUS IN (?, ?) AND (" + String.join(" OR ", where) + ") ORDER BY CREATED_AT DESC",
				max, 0), WorkTaskUtils::mapStep, params.toArray());
	}

	/** One work item in full: steps, timeline, threads and topics. */
	public static Map<String, Object> getWork(User user, String taskId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> task = requireTask(ownerId, ownerType, taskId);
		task.put("topics", topics(ownerId, ownerType, taskId));
		task.put("steps", steps(ownerId, ownerType, taskId));
		task.put("timeline",
				CollaborationDbUtils.query(
						"SELECT UPDATE_ID, KIND, TEXT, THREAD_ID, SOURCE_REF, "
								+ "MESSAGE_AT, ORIGIN, CREATED_AT FROM WORK_TASK_UPDATE" + OWNED
								+ " AND TASK_ID = ? AND " + "(STATE IS NULL OR STATE <> ?) ORDER BY CREATED_AT DESC",
						rs -> {
							Map<String, Object> u = new LinkedHashMap<>();
							u.put("id", rs.getString("UPDATE_ID"));
							u.put("kind", rs.getString("KIND"));
							u.put("text", CollaborationDbUtils.getString(rs, "TEXT"));
							putIfNotNull(u, "threadId", rs.getString("THREAD_ID"));
							putIfNotNull(u, "messageAt", CollaborationDbUtils.getTimestamp(rs, "MESSAGE_AT"));
							u.put("origin", rs.getString("ORIGIN"));
							u.put("at", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
							return u;
						}, ownerId, ownerType, taskId, "rejected"));
		task.put("threads", CollaborationDbUtils.query("SELECT l.THREAD_ID, th.SUBJECT, th.SOURCE, "
				+ "th.LAST_MESSAGE_AT FROM WORK_TASK_THREAD l LEFT JOIN BRAIN_THREAD th ON th.OWNER_ID = l.OWNER_ID "
				+ "AND th.OWNER_TYPE = l.OWNER_TYPE AND th.THREAD_ID = l.THREAD_ID WHERE l.OWNER_ID = ? AND "
				+ "l.OWNER_TYPE = ? AND l.TASK_ID = ? AND (l.STATE IS NULL OR l.STATE <> ?)", rs -> {
					Map<String, Object> t = new LinkedHashMap<>();
					t.put("threadId", rs.getString("THREAD_ID"));
					t.put("subject", CollaborationDbUtils.getString(rs, "SUBJECT"));
					t.put("source", rs.getString("SOURCE"));
					t.put("lastMessageAt", CollaborationDbUtils.getTimestamp(rs, "LAST_MESSAGE_AT"));
					return t;
				}, ownerId, ownerType, taskId, "rejected"));
		return task;
	}

	// ---- writing ----

	/**
	 * Creates a work item (no id) or changes one. args: id, title, kind,
	 * description, status, ongoing, nextStep, waitingOnPersonId, personId, due,
	 * priority, topicIds, threadIds, note (a timeline line). origin is who made a
	 * new one.
	 */
	public static Map<String, Object> saveWork(User user, Map<String, Object> args, String origin) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String taskId = blankToNull(args.get("id"));
		String title = trimmed(args.get("title"));
		String kind = checked(lower(args.get("kind")), KINDS, "kind");
		String status = checked(lower(args.get("status")), STATUSES, "status");
		String priority = checked(lower(args.get("priority")), PRIORITIES, "priority");
		String personId = blankToNull(args.get("personId"));
		String waitingOn = blankToNull(args.get("waitingOnPersonId"));
		BrainPeopleUtils.requirePerson(ownerId, ownerType, personId);
		BrainPeopleUtils.requirePerson(ownerId, ownerType, waitingOn);
		List<String> topicIds = strings(args.get("topicIds"));
		for (String topicId : topicIds) {
			BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		}
		List<String> threadIds = strings(args.get("threadIds"));
		for (String threadId : threadIds) {
			BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		}
		Timestamp due = CollaborationDbUtils.toTimestamp(args.get("due"), "due");
		Timestamp now = CollaborationDbUtils.now();
		String note = trimmed(args.get("note"));
		String[] id = { taskId };

		CollaborationDbUtils.inTransaction(conn -> {
			if (id[0] == null) {
				if (title == null) {
					throw new IllegalArgumentException("A work item needs a title");
				}
				String newKind = kind == null ? WORK : kind;
				id[0] = UUID.randomUUID().toString();
				CollaborationDbUtils.update(conn, "INSERT INTO WORK_TASK (OWNER_ID, OWNER_TYPE, TASK_ID, KIND, TITLE, "
						+ "DESCRIPTION, STATUS, ONGOING, NEXT_STEP, WAITING_ON_PERSON_ID, PERSON_ID, DUE_AT, PRIORITY, "
						+ "ORIGIN, STATE, CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, id[0], newKind, clip(title, MAX_TITLE), trimmed(args.get("description")),
						status == null ? ACTIVE : status, Boolean.TRUE.equals(args.get("ongoing")),
						trimmed(args.get("nextStep")), waitingOn, personId, due, priority,
						origin == null ? OWNER : origin, BRAIN.equals(origin) ? PROPOSED : ACCEPTED, now, now);
			} else {
				Map<String, Object> before = requireTask(ownerId, ownerType, id[0]);
				List<String> sets = new ArrayList<>();
				List<Object> values = new ArrayList<>();
				if (title != null) {
					CollaborationDbUtils.addSet(sets, values, "TITLE", clip(title, MAX_TITLE));
				}
				if (args.containsKey("description")) {
					CollaborationDbUtils.addSet(sets, values, "DESCRIPTION", trimmed(args.get("description")));
				}
				if (args.containsKey("nextStep")) {
					CollaborationDbUtils.addSet(sets, values, "NEXT_STEP", trimmed(args.get("nextStep")));
				}
				if (args.containsKey("waitingOnPersonId")) {
					CollaborationDbUtils.addSet(sets, values, "WAITING_ON_PERSON_ID", waitingOn);
				}
				if (args.containsKey("due")) {
					CollaborationDbUtils.addSet(sets, values, "DUE_AT", due);
				}
				if (priority != null) {
					CollaborationDbUtils.addSet(sets, values, "PRIORITY", priority);
				}
				if (args.containsKey("ongoing")) {
					CollaborationDbUtils.addSet(sets, values, "ONGOING", Boolean.TRUE.equals(args.get("ongoing")));
				}
				if (status != null && !status.equals(before.get("status"))) {
					CollaborationDbUtils.addSet(sets, values, "STATUS", status);
					CollaborationDbUtils.addSet(sets, values, "CLOSED_AT",
							CLOSED_STATUSES.contains(status) ? now : null);
					insertUpdate(conn, ownerId, ownerType, id[0], "status",
							"Status: " + before.get("status") + " -> " + status, null, origin, now);
				}
				// the owner accepting one Brain proposed
				if (Boolean.TRUE.equals(args.get("accept"))) {
					CollaborationDbUtils.addSet(sets, values, "STATE", ACCEPTED);
				}
				CollaborationDbUtils.addSet(sets, values, "UPDATED_AT", now);
				values.addAll(List.of(ownerId, ownerType, id[0]));
				CollaborationDbUtils.update(conn,
						"UPDATE WORK_TASK SET " + String.join(", ", sets) + OWNED + " AND TASK_ID = ?",
						values.toArray());
			}
			linkTopics(conn, ownerId, ownerType, id[0], topicIds, now);
			for (String threadId : threadIds) {
				linkThread(conn, ownerId, ownerType, id[0], threadId, origin, now);
			}
			if (note != null) {
				insertUpdate(conn, ownerId, ownerType, id[0], "note", note, null, origin, now);
			}
		});
		return getWork(user, id[0]);
	}

	/**
	 * Adds a step: under a work item (taskId), or loose under a topic. args: text,
	 * taskId, threadId, topicId, due, assigneeId (a person; none is the owner),
	 * kind (task, waiting_on).
	 */
	public static Map<String, Object> addStep(User user, Map<String, Object> args, String origin) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String text = trimmed(args.get("text"));
		if (text == null) {
			throw new IllegalArgumentException("Step text is required");
		}
		String taskId = blankToNull(args.get("taskId"));
		String threadId = blankToNull(args.get("threadId"));
		String topicId = blankToNull(args.get("topicId"));
		String assignee = blankToNull(args.get("assigneeId"));
		String kind = lower(args.get("kind"));
		// waiting with no assignee: a group's step
		String wanted = lower(args.get("status"));
		boolean waits = assignee != null || "waiting".equals(wanted);
		if (kind == null) {
			kind = waits ? "waiting_on" : "task";
		}
		if (!WorkWorkspaceUtils.STEP_KINDS.contains(kind)) {
			throw new IllegalArgumentException("kind must be one of " + WorkWorkspaceUtils.STEP_KINDS);
		}
		if (taskId != null) {
			requireTask(ownerId, ownerType, taskId);
			if (topicId == null) {
				topicId = primaryTopic(ownerId, ownerType, taskId);
			}
		}
		if (threadId != null) {
			BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		}
		if (topicId != null) {
			BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		}
		BrainPeopleUtils.requirePerson(ownerId, ownerType, assignee);
		Timestamp due = CollaborationDbUtils.toTimestamp(args.get("due"), "due");
		Timestamp now = CollaborationDbUtils.now();
		String stepId = UUID.randomUUID().toString();
		String status = waits ? "waiting" : "open";
		String finalTopic = topicId;
		String finalKind = kind;
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "INSERT INTO WORK_THREAD_STEP (OWNER_ID, OWNER_TYPE, STEP_ID, THREAD_ID, "
					+ "TASK_ID, TEXT, KIND, STATUS, STEP_OWNER_ID, DUE_AT, LINK_TOPIC_ID, ORIGIN, NOTE, CREATED_AT, "
					+ "UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, stepId,
					threadId, taskId, text, finalKind, status, assignee, due, finalTopic,
					origin == null ? OWNER : origin, trimmed(args.get("note")), now, now);
			if (taskId != null) {
				if (threadId != null) {
					linkThread(conn, ownerId, ownerType, taskId, threadId, origin, now);
				}
				touch(conn, ownerId, ownerType, taskId, now);
			}
		});
		return requireStep(ownerId, ownerType, stepId);
	}

	/**
	 * Changes a step: status (open, waiting, done, dropped), text, due, note,
	 * taskId (move it), assigneeId. Finishing one records the owner as what closed
	 * it.
	 */
	public static Map<String, Object> updateStep(User user, String stepId, Map<String, Object> args, String origin) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Map<String, Object> before = requireStep(ownerId, ownerType, stepId);
		String status = lower(args.get("status"));
		if (status != null && !WorkWorkspaceUtils.STEP_STATUSES.contains(status)) {
			throw new IllegalArgumentException("status must be one of " + WorkWorkspaceUtils.STEP_STATUSES);
		}
		String taskId = blankToNull(args.get("taskId"));
		if (taskId != null) {
			requireTask(ownerId, ownerType, taskId);
		}
		String assignee = blankToNull(args.get("assigneeId"));
		BrainPeopleUtils.requirePerson(ownerId, ownerType, assignee);
		Timestamp now = CollaborationDbUtils.now();
		List<String> sets = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		if (args.containsKey("text")) {
			String text = trimmed(args.get("text"));
			if (text == null) {
				throw new IllegalArgumentException("Step text is required");
			}
			CollaborationDbUtils.addSet(sets, values, "TEXT", text);
			CollaborationDbUtils.addSet(sets, values, "EDITED", true);
		}
		if (status != null) {
			CollaborationDbUtils.addSet(sets, values, "STATUS", status);
			String closedBy = blankToNull(args.get("closedBy"));
			CollaborationDbUtils.addSet(sets, values, "CLOSED_BY",
					STEP_OPEN.contains(status) ? null : closedBy == null ? OWNER : closedBy);
		}
		if (args.containsKey("due")) {
			CollaborationDbUtils.addSet(sets, values, "DUE_AT",
					CollaborationDbUtils.toTimestamp(args.get("due"), "due"));
		}
		if (args.containsKey("note")) {
			CollaborationDbUtils.addSet(sets, values, "NOTE", trimmed(args.get("note")));
		}
		if (taskId != null) {
			CollaborationDbUtils.addSet(sets, values, "TASK_ID", taskId);
		}
		if (args.containsKey("assigneeId")) {
			CollaborationDbUtils.addSet(sets, values, "STEP_OWNER_ID", assignee);
		}
		if (sets.isEmpty()) {
			return before;
		}
		CollaborationDbUtils.addSet(sets, values, "UPDATED_AT", now);
		values.addAll(List.of(ownerId, ownerType, stepId));
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn,
					"UPDATE WORK_THREAD_STEP SET " + String.join(", ", sets) + OWNED + " AND STEP_ID = ?",
					values.toArray());
			String task = taskId != null ? taskId : (String) before.get("taskId");
			if (task != null) {
				touch(conn, ownerId, ownerType, task, now);
			}
		});
		return requireStep(ownerId, ownerType, stepId);
	}

	/**
	 * A timeline entry on a work item from a message: kind update, decision, ask,
	 * date, status or note.
	 */
	/** The owner's order on the Work page: the given items first, in this order. */
	public static Map<String, Object> reorder(User user, List<String> taskIds) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			for (int i = 0; i < taskIds.size(); i++) {
				CollaborationDbUtils.update(conn,
						"UPDATE WORK_TASK SET SORT_ORDER = ?, UPDATED_AT = UPDATED_AT" + OWNED + " AND TASK_ID = ?", i,
						ownerId, ownerType, taskIds.get(i));
			}
		});
		return Map.of("ordered", taskIds.size(), "at", CollaborationDbUtils.toIso(now));
	}

	/**
	 * Moves one item's steps, threads, topics and updates onto another and closes
	 * the first.
	 */
	public static Map<String, Object> mergeWork(User user, String fromId, String intoId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (fromId == null || fromId.equals(intoId)) {
			throw new IllegalArgumentException("Pick a different work item to merge into");
		}
		Map<String, Object> from = requireTask(ownerId, ownerType, fromId);
		requireTask(ownerId, ownerType, intoId);
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn,
					"UPDATE WORK_THREAD_STEP SET TASK_ID = ?, UPDATED_AT = ?" + OWNED + " AND TASK_ID = ?", intoId, now,
					ownerId, ownerType, fromId);
			CollaborationDbUtils.update(conn, "UPDATE WORK_TASK_UPDATE SET TASK_ID = ?" + OWNED + " AND TASK_ID = ?",
					intoId, ownerId, ownerType, fromId);
			// the target keeps its primary topic
			CollaborationDbUtils.update(conn, "UPDATE WORK_TASK_TOPIC SET IS_PRIMARY = ?" + OWNED + " AND TASK_ID = ?",
					false, ownerId, ownerType, fromId);
			// links the target lacks move over; duplicates go
			for (String[] link : new String[][] { { "WORK_TASK_THREAD", "THREAD_ID" },
					{ "WORK_TASK_TOPIC", "TOPIC_ID" } }) {
				CollaborationDbUtils.update(conn,
						"UPDATE " + link[0] + " SET TASK_ID = ?" + OWNED + " AND TASK_ID = ? " + "AND " + link[1]
								+ " NOT IN (SELECT " + link[1] + " FROM " + link[0] + OWNED + " AND TASK_ID = ?)",
						intoId, ownerId, ownerType, fromId, ownerId, ownerType, intoId);
				CollaborationDbUtils.update(conn, "DELETE FROM " + link[0] + OWNED + " AND TASK_ID = ?", ownerId,
						ownerType, fromId);
			}
			CollaborationDbUtils.update(conn,
					"UPDATE WORK_TASK SET STATUS = ?, CLOSED_AT = ?, UPDATED_AT = ?" + OWNED + " AND TASK_ID = ?",
					DROPPED, now, now, ownerId, ownerType, fromId);
			CollaborationDbUtils.update(conn, "UPDATE WORK_TASK SET UPDATED_AT = ?" + OWNED + " AND TASK_ID = ?", now,
					ownerId, ownerType, intoId);
		});
		addUpdate(user, intoId, "note", "Merged in \"" + from.get("title") + "\"", null, null, OWNER);
		return getWork(user, intoId);
	}

	/**
	 * Folds duplicate steps of one item into keepId: their evidence moves to it,
	 * they close as dropped, and the kept step takes the new text. Only steps Brain
	 * made and the owner never edited are folded or reworded.
	 */
	static int mergeSteps(String ownerId, String ownerType, String taskId, String keepId, String text,
			List<String> mergeIds) {
		Timestamp now = CollaborationDbUtils.now();
		int[] merged = { 0 };
		// the kept step is the owner's to do when any folded step was
		boolean ownerOpen = !mergeIds.isEmpty() && CollaborationDbUtils.queryOne(
				"SELECT COUNT(*) AS N FROM " + "WORK_THREAD_STEP" + OWNED
						+ " AND TASK_ID = ? AND STATUS = ? AND STEP_OWNER_ID IS NULL AND ORIGIN = ? "
						+ "AND (EDITED IS NULL OR EDITED = ?) AND STEP_ID IN ("
						+ CollaborationDbUtils.placeholders(mergeIds.size()) + ")",
				rs -> rs.getInt("N"), concat(List.of(ownerId, ownerType, taskId, "open", BRAIN, false), mergeIds)) > 0;
		CollaborationDbUtils.inTransaction(conn -> {
			for (String id : mergeIds) {
				int n = CollaborationDbUtils.update(conn,
						"UPDATE WORK_THREAD_STEP SET STATUS = ?, CLOSED_BY = ?, " + "UPDATED_AT = ?" + OWNED
								+ " AND STEP_ID = ? AND TASK_ID = ? AND STATUS IN (?, ?) AND ORIGIN = ? "
								+ "AND (EDITED IS NULL OR EDITED = ?)",
						DROPPED, keepId, now, ownerId, ownerType, id, taskId, "open", "waiting", BRAIN, false);
				if (n == 0) {
					continue;
				}
				merged[0]++;
				CollaborationDbUtils.update(conn,
						"UPDATE WORK_STEP_EVIDENCE SET STEP_ID = ?" + OWNED + " AND STEP_ID = ?", keepId, ownerId,
						ownerType, id);
			}
			if (merged[0] > 0 && ownerOpen) {
				CollaborationDbUtils.update(conn,
						"UPDATE WORK_THREAD_STEP SET STATUS = ?" + OWNED + " AND STEP_ID = ? "
								+ "AND STATUS = ? AND STEP_OWNER_ID IS NULL",
						"open", ownerId, ownerType, keepId, "waiting");
			}
			if (merged[0] > 0 && text != null) {
				CollaborationDbUtils.update(conn,
						"UPDATE WORK_THREAD_STEP SET TEXT = ?, UPDATED_AT = ?" + OWNED
								+ " AND STEP_ID = ? AND ORIGIN = ? AND (EDITED IS NULL OR EDITED = ?)",
						clip(text, 500), now, ownerId, ownerType, keepId, BRAIN, false);
			}
		});
		return merged[0];
	}

	/**
	 * The room the owner works an item in; the room must be their own assistant
	 * chat.
	 */
	public static void setRoom(User user, String taskId, String roomId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		requireTask(owner.getValue0(), owner.getValue1(), taskId);
		BrainTopicRoomUtils.requireChat(user, roomId);
		CollaborationDbUtils.update("UPDATE WORK_TASK SET ROOM_ID = ?" + OWNED + " AND TASK_ID = ?", roomId,
				owner.getValue0(), owner.getValue1(), taskId);
	}

	public static void addUpdate(User user, String taskId, String kind, String text, String threadId, String messageAt,
			String origin) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		requireTask(ownerId, ownerType, taskId);
		String body = trimmed(text);
		if (body == null) {
			return;
		}
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "INSERT INTO WORK_TASK_UPDATE (OWNER_ID, OWNER_TYPE, UPDATE_ID, TASK_ID, "
					+ "KIND, TEXT, THREAD_ID, MESSAGE_AT, ORIGIN, STATE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, UUID.randomUUID().toString(), taskId,
					UPDATE_KINDS.contains(kind) ? kind : "update", body, threadId,
					CollaborationDbUtils.toTimestamp(messageAt, "messageAt"), origin == null ? OWNER : origin,
					BRAIN.equals(origin) ? PROPOSED : ACCEPTED, now);
			touch(conn, ownerId, ownerType, taskId, now);
		});
	}

	// ---- helpers ----

	static Map<String, Object> requireTask(String ownerId, String ownerType, String taskId) {
		Map<String, Object> task = taskId == null ? null
				: CollaborationDbUtils.queryOne(
						"SELECT " + TASK_COLUMNS + " FROM WORK_TASK" + OWNED + " AND TASK_ID = ?",
						WorkTaskUtils::mapTask, ownerId, ownerType, taskId);
		if (task == null) {
			throw new IllegalArgumentException("Work item not found");
		}
		return task;
	}

	private static Map<String, Object> requireStep(String ownerId, String ownerType, String stepId) {
		Map<String, Object> step = stepId == null ? null
				: CollaborationDbUtils.queryOne(
						"SELECT " + STEP_COLUMNS + " FROM WORK_THREAD_STEP" + OWNED + " AND STEP_ID = ?",
						WorkTaskUtils::mapStep, ownerId, ownerType, stepId);
		if (step == null) {
			throw new IllegalArgumentException("Step not found");
		}
		return step;
	}

	private static List<Map<String, Object>> steps(String ownerId, String ownerType, String taskId) {
		// with the assignee's name, so the UI need not load people
		return CollaborationDbUtils.query("SELECT " + STEP_COLUMNS + ", (SELECT p.DISPLAY_NAME FROM BRAIN_PERSON p "
				+ "WHERE p.OWNER_ID = WORK_THREAD_STEP.OWNER_ID AND p.OWNER_TYPE = WORK_THREAD_STEP.OWNER_TYPE AND "
				+ "p.PERSON_ID = WORK_THREAD_STEP.STEP_OWNER_ID) AS ASSIGNEE_NAME FROM WORK_THREAD_STEP" + OWNED
				+ " AND TASK_ID = ? AND (STATUS IS NULL OR STATUS <> ?) ORDER BY CREATED_AT", rs -> {
					Map<String, Object> step = mapStep(rs);
					putIfNotNull(step, "assigneeName", CollaborationDbUtils.getString(rs, "ASSIGNEE_NAME"));
					return step;
				}, ownerId, ownerType, taskId, WorkWorkspaceUtils.DISMISSED);
	}

	private static List<Map<String, Object>> topics(String ownerId, String ownerType, String taskId) {
		return CollaborationDbUtils.query("SELECT l.TOPIC_ID, l.IS_PRIMARY, t.NAME FROM WORK_TASK_TOPIC l LEFT JOIN "
				+ "BRAIN_TOPIC t ON t.OWNER_ID = l.OWNER_ID AND t.OWNER_TYPE = l.OWNER_TYPE AND t.TOPIC_ID = l.TOPIC_ID "
				+ "WHERE l.OWNER_ID = ? AND l.OWNER_TYPE = ? AND l.TASK_ID = ? ORDER BY l.IS_PRIMARY DESC, t.NAME",
				rs -> {
					Map<String, Object> t = new LinkedHashMap<>();
					t.put("id", rs.getString("TOPIC_ID"));
					t.put("name", CollaborationDbUtils.getString(rs, "NAME"));
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_PRIMARY"))) {
						t.put("primary", true);
					}
					return t;
				}, ownerId, ownerType, taskId);
	}

	private static String primaryTopic(String ownerId, String ownerType, String taskId) {
		return CollaborationDbUtils.queryOne(
				"SELECT TOPIC_ID FROM WORK_TASK_TOPIC" + OWNED + " AND TASK_ID = ? " + "ORDER BY IS_PRIMARY DESC",
				rs -> rs.getString("TOPIC_ID"), ownerId, ownerType, taskId);
	}

	// adds the topics not linked yet; the first link a work item gets is its
	// primary
	private static void linkTopics(java.sql.Connection conn, String ownerId, String ownerType, String taskId,
			List<String> topicIds, Timestamp now) throws SQLException {
		if (topicIds.isEmpty()) {
			return;
		}
		boolean hasPrimary = CollaborationDbUtils.exists(
				"SELECT 1 FROM WORK_TASK_TOPIC" + OWNED + " AND TASK_ID = ? AND IS_PRIMARY = ?", ownerId, ownerType,
				taskId, true);
		for (String topicId : topicIds) {
			if (CollaborationDbUtils.exists(
					"SELECT 1 FROM WORK_TASK_TOPIC" + OWNED + " AND TASK_ID = ? AND TOPIC_ID = ?", ownerId, ownerType,
					taskId, topicId)) {
				continue;
			}
			CollaborationDbUtils.update(conn,
					"INSERT INTO WORK_TASK_TOPIC (OWNER_ID, OWNER_TYPE, TASK_ID, TOPIC_ID, "
							+ "IS_PRIMARY, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, taskId, topicId, !hasPrimary, now);
			hasPrimary = true;
		}
	}

	private static void linkThread(java.sql.Connection conn, String ownerId, String ownerType, String taskId,
			String threadId, String origin, Timestamp now) throws SQLException {
		if (CollaborationDbUtils.exists("SELECT 1 FROM WORK_TASK_THREAD" + OWNED + " AND TASK_ID = ? AND THREAD_ID = ?",
				ownerId, ownerType, taskId, threadId)) {
			return;
		}
		CollaborationDbUtils.update(conn,
				"INSERT INTO WORK_TASK_THREAD (OWNER_ID, OWNER_TYPE, TASK_ID, THREAD_ID, "
						+ "ORIGIN, STATE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, taskId, threadId, origin == null ? OWNER : origin,
				BRAIN.equals(origin) ? PROPOSED : ACCEPTED, now);
	}

	private static void insertUpdate(java.sql.Connection conn, String ownerId, String ownerType, String taskId,
			String kind, String text, String threadId, String origin, Timestamp now) throws SQLException {
		CollaborationDbUtils.update(conn,
				"INSERT INTO WORK_TASK_UPDATE (OWNER_ID, OWNER_TYPE, UPDATE_ID, TASK_ID, "
						+ "KIND, TEXT, THREAD_ID, ORIGIN, STATE, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				ownerId, ownerType, UUID.randomUUID().toString(), taskId, kind, text, threadId,
				origin == null ? OWNER : origin, BRAIN.equals(origin) ? PROPOSED : ACCEPTED, now);
	}

	private static void touch(java.sql.Connection conn, String ownerId, String ownerType, String taskId, Timestamp now)
			throws SQLException {
		CollaborationDbUtils.update(conn, "UPDATE WORK_TASK SET UPDATED_AT = ?" + OWNED + " AND TASK_ID = ?", now,
				ownerId, ownerType, taskId);
	}

	private static String selfId(String ownerId, String ownerType) {
		return CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON" + OWNED + " AND RELATIONSHIP = ?",
				rs -> rs.getString("PERSON_ID"), ownerId, ownerType, "self");
	}

	private static Map<String, Object> mapTask(ResultSet rs) throws SQLException {
		Map<String, Object> t = new LinkedHashMap<>();
		t.put("id", rs.getString("TASK_ID"));
		t.put("kind", rs.getString("KIND"));
		t.put("title", CollaborationDbUtils.getString(rs, "TITLE"));
		putIfNotNull(t, "description", CollaborationDbUtils.getString(rs, "DESCRIPTION"));
		t.put("status", rs.getString("STATUS"));
		if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "ONGOING"))) {
			t.put("ongoing", true);
		}
		putIfNotNull(t, "nextStep", CollaborationDbUtils.getString(rs, "NEXT_STEP"));
		putIfNotNull(t, "waitingOnPersonId", rs.getString("WAITING_ON_PERSON_ID"));
		putIfNotNull(t, "personId", rs.getString("PERSON_ID"));
		putIfNotNull(t, "due", CollaborationDbUtils.getTimestamp(rs, "DUE_AT"));
		putIfNotNull(t, "priority", rs.getString("PRIORITY"));
		t.put("origin", rs.getString("ORIGIN"));
		t.put("state", rs.getString("STATE"));
		t.put("createdAt", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
		t.put("updatedAt", CollaborationDbUtils.getTimestamp(rs, "UPDATED_AT"));
		putIfNotNull(t, "closedAt", CollaborationDbUtils.getTimestamp(rs, "CLOSED_AT"));
		int order = rs.getInt("SORT_ORDER");
		if (!rs.wasNull()) {
			t.put("sortOrder", order);
		}
		putIfNotNull(t, "roomId", rs.getString("ROOM_ID"));
		return t;
	}

	private static Map<String, Object> mapStep(ResultSet rs) throws SQLException {
		Map<String, Object> s = new LinkedHashMap<>();
		s.put("id", rs.getString("STEP_ID"));
		s.put("text", CollaborationDbUtils.getString(rs, "TEXT"));
		s.put("kind", rs.getString("KIND"));
		s.put("status", rs.getString("STATUS"));
		putIfNotNull(s, "taskId", rs.getString("TASK_ID"));
		putIfNotNull(s, "threadId", rs.getString("THREAD_ID"));
		putIfNotNull(s, "assigneeId", rs.getString("STEP_OWNER_ID"));
		putIfNotNull(s, "due", CollaborationDbUtils.getTimestamp(rs, "DUE_AT"));
		putIfNotNull(s, "topicId", rs.getString("LINK_TOPIC_ID"));
		putIfNotNull(s, "note", CollaborationDbUtils.getString(rs, "NOTE"));
		putIfNotNull(s, "closedBy", rs.getString("CLOSED_BY"));
		s.put("origin", rs.getString("ORIGIN"));
		s.put("createdAt", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
		return s;
	}

	private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
		if (value != null) {
			map.put(key, value);
		}
	}

	private static String checked(String value, Set<String> allowed, String name) {
		if (value != null && !allowed.contains(value)) {
			throw new IllegalArgumentException(name + " must be one of " + allowed);
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private static List<String> strings(Object value) {
		List<String> out = new ArrayList<>();
		if (value instanceof List<?> list) {
			for (Object o : list) {
				String s = blankToNull(o);
				if (s != null && !out.contains(s)) {
					out.add(s);
				}
			}
		} else {
			String s = blankToNull(value);
			if (s != null) {
				out.add(s);
			}
		}
		return out;
	}

	private static String lower(Object value) {
		String s = blankToNull(value);
		return s == null ? null : s.toLowerCase(Locale.ROOT);
	}

	private static String trimmed(Object value) {
		return blankToNull(value);
	}

	private static String blankToNull(Object value) {
		if (value == null) {
			return null;
		}
		String s = String.valueOf(value).trim();
		return s.isEmpty() ? null : s;
	}

	private static Object[] concat(List<Object> head, List<String> tail) {
		List<Object> all = new ArrayList<>(head);
		all.addAll(tail);
		return all.toArray();
	}

	private static String clip(String text, int max) {
		return text.length() <= max ? text : text.substring(0, max);
	}
}
