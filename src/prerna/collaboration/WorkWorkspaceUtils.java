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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// A thread workspace's steps (WORK_THREAD_STEP); the goal is on BRAIN_THREAD, and its facts are Brain memories
// linked to the thread (BrainMemoryUtils)
public final class WorkWorkspaceUtils {

	public static final Set<String> STEP_KINDS = Set.of("reply", "task", "waiting_on", "errand", "approve");
	public static final Set<String> STEP_STATUSES = Set.of("open", "waiting", "done", "suggested", "draft_ready");
	// a generated step the owner deleted: kept, unlisted, so a later summary does not add it again
	static final String DISMISSED = "dismissed";
	// a step changed in any of these is the owner's: thread insights no longer rewrite or drop it
	private static final Set<String> CONTENT_KEYS = Set.of("text", "kind", "ownerId", "due");

	private static final String STEP_COLUMNS = "STEP_ID, THREAD_ID, TEXT, KIND, STATUS, STEP_OWNER_ID, DUE_AT, "
			+ "ITEM_ID, LINK_TOPIC_ID, ORIGIN";
	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";

	private WorkWorkspaceUtils() {
	}

	// every thread with a goal or step (or just the one thread), oldest steps first
	public static Map<String, Object> listWorkspaces(User user, String threadId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String oneThread = threadId == null ? "" : " AND THREAD_ID = ?";
		Object[] params = threadId == null ? new Object[] { ownerId, ownerType }
				: new Object[] { ownerId, ownerType, threadId };

		Map<String, Map<String, Object>> workspaces = new LinkedHashMap<>();
		for (Map<String, Object> row : CollaborationDbUtils.query("SELECT THREAD_ID, GOAL FROM BRAIN_THREAD" + OWNED
				+ " AND GOAL IS NOT NULL" + oneThread + " ORDER BY THREAD_ID", rs -> {
					Map<String, Object> r = new LinkedHashMap<>();
					r.put("threadId", CollaborationDbUtils.getString(rs, "THREAD_ID"));
					r.put("goal", CollaborationDbUtils.getString(rs, "GOAL"));
					return r;
				}, params)) {
			workspace(workspaces, (String) row.get("threadId")).put("goal", row.get("goal"));
		}
		for (Map<String, Object> step : CollaborationDbUtils.query("SELECT " + STEP_COLUMNS + " FROM WORK_THREAD_STEP"
				+ OWNED + oneThread + " AND (STATUS IS NULL OR STATUS <> '" + DISMISSED + "') ORDER BY CREATED_AT, STEP_ID",
				WorkWorkspaceUtils::mapStep, params)) {
			list(workspace(workspaces, (String) step.remove("threadId")), "steps").add(step);
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("items", new ArrayList<>(workspaces.values()));
		result.put("total", workspaces.size());
		return result;
	}

	// creates a step when step has no id; otherwise changes only the keys passed
	public static Map<String, Object> saveStep(User user, String threadId, Map<String, Object> step) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		String stepId = CollaborationDbUtils.asString(step.get("id"));
		String text = CollaborationDbUtils.asString(step.get("text"));
		if ((stepId == null || step.containsKey("text")) && (text == null || text.isBlank())) {
			throw new IllegalArgumentException("Step text is required");
		}
		String kind = checked(step, "kind", STEP_KINDS);
		String status = checked(step, "status", STEP_STATUSES);
		String itemId = blankToNull(step.get("itemId"));
		if (itemId != null && !CollaborationDbUtils.exists("SELECT 1 FROM WORK_ITEM" + OWNED + " AND ITEM_ID = ?",
				ownerId, ownerType, itemId)) {
			throw new IllegalArgumentException("Work item not found");
		}
		String topicId = blankToNull(step.get("linkTopicId"));
		if (topicId != null) {
			BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		}
		Timestamp due = CollaborationDbUtils.toTimestamp(step.get("due"), "due");
		Timestamp now = CollaborationDbUtils.now();

		if (stepId == null) {
			stepId = UUID.randomUUID().toString();
			CollaborationDbUtils.update(
					"INSERT INTO WORK_THREAD_STEP (OWNER_ID, OWNER_TYPE, STEP_ID, THREAD_ID, TEXT, "
							+ "KIND, STATUS, STEP_OWNER_ID, DUE_AT, ITEM_ID, LINK_TOPIC_ID, CREATED_AT, UPDATED_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, stepId, threadId, text.trim(), kind == null ? "task" : kind,
					status == null ? "open" : status, blankToNull(step.get("ownerId")), due, itemId, topicId, now, now);
		} else {
			List<String> sets = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			if (step.containsKey("text")) {
				CollaborationDbUtils.addSet(sets, values, "TEXT", text.trim());
			}
			if (kind != null) {
				CollaborationDbUtils.addSet(sets, values, "KIND", kind);
			}
			if (status != null) {
				CollaborationDbUtils.addSet(sets, values, "STATUS", status);
			}
			if (step.containsKey("ownerId")) {
				CollaborationDbUtils.addSet(sets, values, "STEP_OWNER_ID", blankToNull(step.get("ownerId")));
			}
			if (step.containsKey("due")) {
				CollaborationDbUtils.addSet(sets, values, "DUE_AT", due);
			}
			if (step.containsKey("itemId")) {
				CollaborationDbUtils.addSet(sets, values, "ITEM_ID", itemId);
			}
			if (step.containsKey("linkTopicId")) {
				CollaborationDbUtils.addSet(sets, values, "LINK_TOPIC_ID", topicId);
			}
			if (CONTENT_KEYS.stream().anyMatch(step::containsKey)) {
				CollaborationDbUtils.addSet(sets, values, "EDITED", true);
			}
			CollaborationDbUtils.addSet(sets, values, "UPDATED_AT", now);
			values.addAll(List.of(ownerId, ownerType, threadId, stepId));
			if (CollaborationDbUtils.update("UPDATE WORK_THREAD_STEP SET " + String.join(", ", sets) + OWNED
					+ " AND THREAD_ID = ? AND STEP_ID = ?", values.toArray()) == 0) {
				throw new IllegalArgumentException("Step not found");
			}
		}
		return CollaborationDbUtils.queryOne(
				"SELECT " + STEP_COLUMNS + " FROM WORK_THREAD_STEP" + OWNED + " AND STEP_ID = ?",
				WorkWorkspaceUtils::mapStep, ownerId, ownerType, stepId);
	}

	// a generated step stays behind as dismissed, so the next summary of the thread knows the owner dropped it
	public static Map<String, Object> deleteStep(User user, String threadId, String stepId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		if (CollaborationDbUtils.update("UPDATE WORK_THREAD_STEP SET STATUS = ?, UPDATED_AT = ?" + OWNED
				+ " AND THREAD_ID = ? AND STEP_ID = ? AND ORIGIN = ?", DISMISSED, CollaborationDbUtils.now(),
				owner.getValue0(), owner.getValue1(), threadId, stepId, WorkThreadInsights.BRAIN) > 0) {
			Map<String, Object> result = new LinkedHashMap<>();
			result.put("id", stepId);
			result.put("deleted", true);
			return result;
		}
		return delete(user, "WORK_THREAD_STEP", "STEP_ID", "Step", threadId, stepId);
	}

	private static Map<String, Object> delete(User user, String table, String idColumn, String label, String threadId,
			String id) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		if (CollaborationDbUtils.update("DELETE FROM " + table + OWNED + " AND THREAD_ID = ? AND " + idColumn + " = ?",
				owner.getValue0(), owner.getValue1(), threadId, id) == 0) {
			throw new IllegalArgumentException(label + " not found");
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("id", id);
		result.put("deleted", true);
		return result;
	}

	private static Map<String, Object> mapStep(ResultSet rs) throws SQLException {
		Map<String, Object> step = new LinkedHashMap<>();
		step.put("id", CollaborationDbUtils.getString(rs, "STEP_ID"));
		step.put("threadId", CollaborationDbUtils.getString(rs, "THREAD_ID"));
		step.put("text", CollaborationDbUtils.getString(rs, "TEXT"));
		step.put("kind", CollaborationDbUtils.getString(rs, "KIND"));
		step.put("status", CollaborationDbUtils.getString(rs, "STATUS"));
		step.put("ownerId", CollaborationDbUtils.getString(rs, "STEP_OWNER_ID"));
		step.put("due", CollaborationDbUtils.getTimestamp(rs, "DUE_AT"));
		step.put("itemId", CollaborationDbUtils.getString(rs, "ITEM_ID"));
		step.put("linkTopicId", CollaborationDbUtils.getString(rs, "LINK_TOPIC_ID"));
		String origin = CollaborationDbUtils.getString(rs, "ORIGIN");
		if (origin != null) {
			step.put("origin", origin);
		}
		return step;
	}

	private static Map<String, Object> workspace(Map<String, Map<String, Object>> workspaces, String threadId) {
		return workspaces.computeIfAbsent(threadId, id -> {
			Map<String, Object> w = new LinkedHashMap<>();
			w.put("threadId", id);
			w.put("goal", null);
			w.put("steps", new ArrayList<>());
			return w;
		});
	}

	@SuppressWarnings("unchecked")
	private static List<Object> list(Map<String, Object> workspace, String key) {
		return (List<Object>) workspace.get(key);
	}

	// the value when it is one of allowed, null when the key is absent
	private static String checked(Map<String, Object> values, String key, Set<String> allowed) {
		String value = CollaborationDbUtils.asString(values.get(key));
		if (value != null && !allowed.contains(value)) {
			throw new IllegalArgumentException(key + " must be one of " + allowed);
		}
		return value;
	}

	private static String blankToNull(Object value) {
		String text = CollaborationDbUtils.asString(value);
		return text == null || text.isBlank() ? null : text;
	}
}
