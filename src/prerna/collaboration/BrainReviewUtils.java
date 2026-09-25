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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;

// Brain review queue (BRAIN_REVIEW): questions Brain asks the owner. Resolving here records the answer;
// applying it to topics, people, and links is BRAIN-05.
public final class BrainReviewUtils {

	public static final String NEW_TOPIC = "new_topic";
	public static final String TOPIC_CHOICE = "topic_choice";
	public static final String ADD_PERSON = "add_person";
	public static final String UNASSIGNED = "unassigned";
	public static final Set<String> KINDS = Set.of(NEW_TOPIC, TOPIC_CHOICE, ADD_PERSON, UNASSIGNED);

	public static final String OPEN = "open";
	public static final String ACCEPTED = "accepted";
	public static final String DISMISSED = "dismissed";
	public static final Set<String> STATUSES = Set.of(OPEN, ACCEPTED, DISMISSED);

	public static final Set<String> REF_TYPES = Set.of("thread", "person", "topic");

	// a client may answer with a decision instead of a button label
	public static final String DISMISS = "dismiss";
	public static final Set<String> DECISIONS = Set.of("accept", DISMISS, "both", "choose", "merge");

	private static final String LOCK = "review";
	// DETAIL_JSON keys shown as the entry's own fields; the rest is returned as data
	private static final Set<String> DISPLAY_KEYS = Set.of("text", "detail", "actions");

	private static final String REVIEW_COLUMNS = "REVIEW_ID, KIND, REF_TYPE, REF_ID, DETAIL_JSON, STATUS, CREATED_AT, "
			+ "RESOLVED_AT, RESOLVED_BY, RESOLUTION_JSON";

	private BrainReviewUtils() {

	}

	// ---- read ----

	// open: newest first; resolved: most recently resolved first
	public static Map<String, Object> listReview(User user, String status, int limit, int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String want = status == null ? OPEN : check(STATUSES, status, "review status");
		String where = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND STATUS = ?";
		String order = OPEN.equals(want) ? " ORDER BY CREATED_AT DESC, REVIEW_ID"
				: " ORDER BY RESOLVED_AT DESC, REVIEW_ID";
		List<Map<String, Object>> items = CollaborationDbUtils.query(CollaborationDbUtils.page(
				"SELECT " + REVIEW_COLUMNS + " FROM BRAIN_REVIEW" + where + order, limit, offset),
				rs -> mapReview(rs, ownerId, ownerType), ownerId, ownerType, want);
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_REVIEW" + where, ownerId, ownerType,
				want));
		return page;
	}

	public static Map<String, Object> getReview(User user, String reviewId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return requireReview(owner.getValue0(), owner.getValue1(), reviewId);
	}

	// ---- write ----

	// Brain asks once per kind, ref, and key: a repeat returns the first entry, even one the owner already
	// answered. detail may carry text, detail, actions (display) and anything else (returned as data).
	public static Map<String, Object> addReview(String ownerId, String ownerType, String kind, String refType,
			String refId, String key, Map<String, Object> detail) {
		check(KINDS, kind, "review kind");
		check(REF_TYPES, refType, "review refType");
		requireRef(ownerId, ownerType, refType, refId);
		String reviewId = CollaborationDbUtils.deterministicId(ownerId, ownerType, "review", kind, refType, refId, key);
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Map<String, Object> result = new LinkedHashMap<>();
			Map<String, Object> existing = findReview(ownerId, ownerType, reviewId);
			if (existing == null) {
				CollaborationDbUtils.update("INSERT INTO BRAIN_REVIEW (OWNER_ID, OWNER_TYPE, REVIEW_ID, KIND, REF_TYPE, "
						+ "REF_ID, DETAIL_JSON, STATUS, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId,
						ownerType, reviewId, kind, refType, refId,
						CollaborationDbUtils.toJson(detail == null ? Map.of() : detail), OPEN,
						CollaborationDbUtils.now());
			}
			result.put("created", existing == null);
			result.put("review", existing == null ? findReview(ownerId, ownerType, reviewId) : existing);
			return result;
		}
	}

	// BrainResolveReview from the owner
	public static Map<String, Object> resolveReview(User user, String reviewId, String action,
			Map<String, Object> paramValues) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return resolveReview(owner.getValue0(), owner.getValue1(), reviewId, action, paramValues,
				BrainProfileUtils.YOU);
	}

	// records the answer: a label from actions or a decision; dismissed for "dismiss" or the last label (its
	// "Ignore"), accepted otherwise
	public static Map<String, Object> resolveReview(String ownerId, String ownerType, String reviewId, String action,
			Map<String, Object> paramValues, String actor) {
		if (action == null || action.isBlank()) {
			throw new IllegalArgumentException("Resolving a review needs an action");
		}
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Map<String, Object> review = requireReview(ownerId, ownerType, reviewId);
			if (!OPEN.equals(review.get("status"))) {
				throw new IllegalArgumentException("Review already resolved");
			}
			@SuppressWarnings("unchecked")
			List<Object> actions = (List<Object>) review.get("actions");
			if (!actions.contains(action) && !DECISIONS.contains(action)) {
				throw new IllegalArgumentException("Review action must be one of " + actions + " or " + DECISIONS);
			}
			String status = DISMISS.equals(action) || action.equals(actions.get(actions.size() - 1)) ? DISMISSED
					: ACCEPTED;
			Map<String, Object> resolution = new LinkedHashMap<>();
			resolution.put("action", action);
			if (paramValues != null && !paramValues.isEmpty()) {
				resolution.put("paramValues", paramValues);
			}
			CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = ?, RESOLVED_AT = ?, RESOLVED_BY = ?, "
					+ "RESOLUTION_JSON = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ?", status,
					CollaborationDbUtils.now(), actor, CollaborationDbUtils.toJson(resolution), ownerId, ownerType,
					reviewId);
			return findReview(ownerId, ownerType, reviewId);
		}
	}

	// undo: the entry goes back to open; the answer it had is returned as undone so the caller can reverse
	// what it applied
	public static Map<String, Object> reopenReview(User user, String reviewId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			Map<String, Object> review = requireReview(ownerId, ownerType, reviewId);
			if (OPEN.equals(review.get("status"))) {
				throw new IllegalArgumentException("Review is already open");
			}
			CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = ?, RESOLVED_AT = NULL, RESOLVED_BY = NULL, "
					+ "RESOLUTION_JSON = NULL WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ?", OPEN, ownerId,
					ownerType, reviewId);
			Map<String, Object> reopened = findReview(ownerId, ownerType, reviewId);
			reopened.put("undone", review.get("resolution"));
			return reopened;
		}
	}

	// open questions about a ref that went away (a thread, person, or topic) no longer apply
	public static int dismissForRef(String ownerId, String ownerType, String refType, String refId) {
		check(REF_TYPES, refType, "review refType");
		synchronized (CollaborationDbUtils.ownerLock(LOCK, ownerId, ownerType)) {
			return CollaborationDbUtils.update("UPDATE BRAIN_REVIEW SET STATUS = ?, RESOLVED_AT = ?, RESOLVED_BY = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REF_TYPE = ? AND REF_ID = ? AND STATUS = ?",
					DISMISSED, CollaborationDbUtils.now(), WorkItemUtils.BRAIN, ownerId, ownerType, refType, refId,
					OPEN);
		}
	}

	// ---- helpers ----

	private static Map<String, Object> findReview(String ownerId, String ownerType, String reviewId) {
		return CollaborationDbUtils.queryOne("SELECT " + REVIEW_COLUMNS + " FROM BRAIN_REVIEW "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND REVIEW_ID = ?", rs -> mapReview(rs, ownerId, ownerType),
				ownerId, ownerType, reviewId);
	}

	private static Map<String, Object> requireReview(String ownerId, String ownerType, String reviewId) {
		Map<String, Object> review = findReview(ownerId, ownerType, reviewId);
		if (review == null) {
			throw new IllegalArgumentException("Review not found");
		}
		return review;
	}

	private static void requireRef(String ownerId, String ownerType, String refType, String refId) {
		if (refId == null || refId.isBlank()) {
			throw new IllegalArgumentException("A review needs a refId");
		}
		switch (refType) {
		case "thread" -> BrainThreadUtils.requireThread(ownerId, ownerType, refId);
		case "person" -> BrainPeopleUtils.requirePerson(ownerId, ownerType, refId);
		default -> BrainTopicUtils.requireTopic(ownerId, ownerType, refId);
		}
	}

	private static Map<String, Object> mapReview(ResultSet rs, String ownerId, String ownerType) throws SQLException {
		String kind = CollaborationDbUtils.getString(rs, "KIND");
		String refType = CollaborationDbUtils.getString(rs, "REF_TYPE");
		String refId = CollaborationDbUtils.getString(rs, "REF_ID");
		Map<String, Object> detail = CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "DETAIL_JSON"));
		if (detail == null) {
			detail = new LinkedHashMap<>();
		}
		Map<String, Object> data = new LinkedHashMap<>();
		detail.forEach((k, v) -> {
			if (!DISPLAY_KEYS.contains(k)) {
				data.put(k, v);
			}
		});

		Map<String, Object> review = new LinkedHashMap<>();
		review.put("id", CollaborationDbUtils.getString(rs, "REVIEW_ID"));
		review.put("kind", kind);
		review.put("text", detail.containsKey("text") ? CollaborationDbUtils.asString(detail.get("text"))
				: defaultText(ownerId, ownerType, kind, refId, data));
		review.put("detail", CollaborationDbUtils.asString(detail.get("detail")));
		review.put("refType", refType);
		review.put("refId", refId);
		review.put("status", CollaborationDbUtils.getString(rs, "STATUS"));
		review.put("actions", detail.get("actions") instanceof List<?> list ? new ArrayList<>(list)
				: defaultActions(ownerId, ownerType, kind, data));
		review.put("data", data);
		review.put("createdAt", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
		review.put("resolvedAt", CollaborationDbUtils.getTimestamp(rs, "RESOLVED_AT"));
		review.put("resolvedBy", CollaborationDbUtils.getString(rs, "RESOLVED_BY"));
		review.put("resolution", CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "RESOLUTION_JSON")));
		return review;
	}

	// used when Brain did not store its own text
	private static String defaultText(String ownerId, String ownerType, String kind, String refId,
			Map<String, Object> data) {
		return switch (kind) {
		case NEW_TOPIC -> "New topic suggested: " + topicName(ownerId, ownerType, data.get("candidate"));
		case TOPIC_CHOICE -> threadSubject(ownerId, ownerType, refId) + " fits more than one topic";
		case ADD_PERSON -> "Add " + personName(ownerId, ownerType, refId) + " to "
				+ topicName(ownerId, ownerType, data.get("suggestedTopic")) + "?";
		default -> threadSubject(ownerId, ownerType, refId) + " has no topic";
		};
	}

	// the last action dismisses the entry
	private static List<Object> defaultActions(String ownerId, String ownerType, String kind,
			Map<String, Object> data) {
		List<Object> actions = new ArrayList<>();
		switch (kind) {
		case NEW_TOPIC -> actions.add("Accept");
		case TOPIC_CHOICE -> {
			actions.add("Both");
			if (data.get("candidates") instanceof List<?> candidates) {
				for (Object candidate : candidates) {
					actions.add(topicName(ownerId, ownerType, candidate));
				}
			}
		}
		case ADD_PERSON -> actions.add("Add");
		default -> actions.add("Pick a topic");
		}
		actions.add("Ignore");
		return actions;
	}

	private static String topicName(String ownerId, String ownerType, Object topicId) {
		String name = topicId == null ? null : CollaborationDbUtils.queryOne("SELECT NAME FROM BRAIN_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?", rs -> rs.getString(1), ownerId, ownerType,
				String.valueOf(topicId));
		return name != null ? name : topicId == null ? "a topic" : String.valueOf(topicId);
	}

	private static String threadSubject(String ownerId, String ownerType, String threadId) {
		String subject = CollaborationDbUtils.queryOne("SELECT SUBJECT FROM BRAIN_THREAD WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> rs.getString(1), ownerId, ownerType, threadId);
		return subject == null || subject.isBlank() ? "A thread" : "\"" + subject + "\"";
	}

	private static String personName(String ownerId, String ownerType, String personId) {
		String name = CollaborationDbUtils.queryOne("SELECT DISPLAY_NAME FROM BRAIN_PERSON WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND PERSON_ID = ?", rs -> rs.getString(1), ownerId, ownerType, personId);
		return name == null ? "this person" : name;
	}

	private static String check(Set<String> allowed, String value, String name) {
		if (value == null || !allowed.contains(value)) {
			throw new IllegalArgumentException("Unknown " + name + ": " + value);
		}
		return value;
	}
}
