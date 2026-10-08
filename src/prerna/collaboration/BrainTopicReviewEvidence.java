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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Bounded, read-only imported metadata for the review. Message bodies use BrainThreadMessages on demand. */
final class BrainTopicReviewEvidence {
	static final int MAX_THREADS = 1000;

	private BrainTopicReviewEvidence() {
	}

	static Map<String, Object> list(String ownerId, String ownerType, Map<String, Object> review, String topicKey,
			String query, int offset, int limit) {
		Map<String, Object> topic = topic(review, topicKey);
		Set<String> ids = ids(ownerId, ownerType, topic, review);
		boolean limited = ids.size() > MAX_THREADS;
		List<String> bounded = ids.stream().limit(MAX_THREADS).toList();
		List<Map<String, Object>> rows = headers(ownerId, ownerType, bounded);
		Set<String> allowed = eligible(ownerId, ownerType, rows);
		String search = Objects.toString(query, "").trim().toLowerCase(Locale.ROOT);
		if (search.length() > 200) {
			throw new IllegalArgumentException("Search at most 200 characters");
		}
		List<Map<String, Object>> visible = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			String threadId = (String) row.get("id");
			if (!allowed.contains(threadId)) {
				continue;
			}
			if (!search.isEmpty() && !Objects.toString(row.get("subject"), "").toLowerCase(Locale.ROOT).contains(search)) {
				continue;
			}
			visible.add(row);
		}
		int start = Math.min(Math.max(0, offset), visible.size());
		int end = Math.min(visible.size(), start + Math.max(1, Math.min(50, limit)));
		List<Map<String, Object>> page = new ArrayList<>(visible.subList(start, end));
		for (String threadId : page.stream().map(row -> (String) row.get("id")).sorted().toList()) {
			BrainThreadTopicDecisions.lockThread(ownerId, ownerType, threadId);
		}
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		Map<String, String> names = new LinkedHashMap<>();
		CollaborationDbUtils.query("SELECT TOPIC_ID, NAME FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> names.put(rs.getString(1), rs.getString(2)), ownerId, ownerType);
		for (Map<String, Object> row : page) {
			String threadId = (String) row.get("id");
			List<Map<String, Object>> links = BrainThreadUtils.getLinks(ownerId, ownerType, threadId);
			links.forEach(link -> link.put("name", Objects.toString(names.get(link.get("topicId")), "Saved topic")));
			row.put("links", links);
			row.put("people", people(ownerId, ownerType, threadId, rules));
			row.put("rejectedTopicIds", BrainThreadTopicDecisions.rejected(ownerId, ownerType, threadId).stream().sorted().toList());
			row.put("canCorrect", "email".equals(row.get("source")));
			row.put("version", version(ownerId, ownerType, threadId));
		}
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("reviewId", review.get("id"));
		out.put("revision", review.get("revision"));
		out.put("topicKey", topicKey);
		out.put("items", page);
		out.put("total", visible.size());
		out.put("offset", start);
		out.put("hasMore", end < visible.size());
		out.put("hiddenOrUnavailable", rows.size() - allowed.size() + bounded.size() - rows.size());
		out.put("limited", limited);
		out.put("scope", "Imported conversation headers and existing topic links");
		return out;
	}

	static Map<String, Object> topic(Map<String, Object> review, String key) {
		for (Map<String, Object> topic : maps(map(review.get("draft")).get("topics"))) {
			if (Objects.equals(key, topic.get("key"))) {
				return topic;
			}
		}
		throw new IllegalArgumentException("Draft topic not found");
	}

	static Set<String> ids(String ownerId, String ownerType, Map<String, Object> topic, Map<String, Object> review) {
		Set<String> ids = new LinkedHashSet<>(strings(topic.get("threadIds")));
		for (Map<String, Object> correction : maps(map(review.get("draft")).get("corrections"))) {
			if (Objects.equals(topic.get("key"), correction.get("topicKey")) && "include".equals(correction.get("state"))) {
				ids.add((String) correction.get("threadId"));
			}
		}
		if (topic.get("id") instanceof String id) {
			ids.addAll(CollaborationDbUtils.query("SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND TOPIC_ID = ? ORDER BY THREAD_ID", rs -> rs.getString(1), ownerId, ownerType, id));
		}
		return ids;
	}

	static void requireEmail(String ownerId, String ownerType, String threadId) {
		List<Map<String, Object>> rows = headers(ownerId, ownerType, List.of(threadId));
		if (rows.isEmpty() || !"email".equals(rows.get(0).get("source"))
				|| !eligible(ownerId, ownerType, rows).contains(threadId)) {
			throw new IllegalArgumentException("This conversation is no longer available for email topic review. Refresh its examples.");
		}
	}

	/** Metadata signature for optimistic relationship checks, including explicit rejections. */
	static String version(String ownerId, String ownerType, String threadId) {
		List<String> links = BrainThreadUtils.getLinks(ownerId, ownerType, threadId).stream()
				.map(link -> link.get("topicId") + ":" + link.get("source") + ":"
						+ ((Number) link.get("confidence")).intValue() + ":" + link.get("primary"))
				.sorted().toList();
		return CollaborationDbUtils.toJson(Map.of("links", links, "rejected",
				BrainThreadTopicDecisions.rejected(ownerId, ownerType, threadId).stream().sorted().toList()));
	}

	private static List<Map<String, Object>> headers(String ownerId, String ownerType, List<String> ids) {
		if (ids.isEmpty()) {
			return List.of();
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(ids);
		return CollaborationDbUtils.query("SELECT THREAD_ID, SOURCE, SUBJECT, LAST_MESSAGE_AT, MESSAGE_COUNT, MUTED "
				+ "FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IN ("
				+ CollaborationDbUtils.placeholders(ids.size()) + ") ORDER BY LAST_MESSAGE_AT DESC, THREAD_ID", rs -> {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", rs.getString("THREAD_ID"));
			row.put("source", rs.getString("SOURCE"));
			row.put("subject", Objects.toString(rs.getString("SUBJECT"), "Untitled conversation"));
			row.put("lastMessageAt", CollaborationDbUtils.getTimestamp(rs, "LAST_MESSAGE_AT"));
			row.put("messageCount", rs.getInt("MESSAGE_COUNT"));
			row.put("muted", Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MUTED")));
			return row;
		}, params.toArray());
	}

	private static Set<String> eligible(String ownerId, String ownerType, List<Map<String, Object>> rows) {
		if (rows.isEmpty()) {
			return Set.of();
		}
		Set<String> sources = new HashSet<>(CollaborationDbUtils.query("SELECT SOURCE FROM SOURCE_CONNECTION "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ENABLED = ?", rs -> rs.getString(1), ownerId, ownerType, true));
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		Set<String> candidates = new HashSet<>();
		for (Map<String, Object> row : rows) {
			if (sources.contains(row.get("source")) && !Boolean.TRUE.equals(row.get("muted"))
					&& BrainRulesGate.keywordRule(rules, (String) row.get("subject"), "") == null) {
				candidates.add((String) row.get("id"));
			}
		}
		if (candidates.isEmpty()) {
			return Set.of();
		}
		Set<String> allowed = new HashSet<>();
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(candidates);
		CollaborationDbUtils.query("SELECT m.THREAD_ID, m.DECISION, m.SENDER_PERSON_ID, m.FOLDER, p.EMAIL_NORM "
				+ "FROM BRAIN_MESSAGE m LEFT JOIN BRAIN_PERSON p ON p.OWNER_ID = m.OWNER_ID AND p.OWNER_TYPE = m.OWNER_TYPE "
				+ "AND p.PERSON_ID = m.SENDER_PERSON_ID WHERE m.OWNER_ID = ? AND m.OWNER_TYPE = ? AND m.THREAD_ID IN ("
				+ CollaborationDbUtils.placeholders(candidates.size()) + ")", rs -> {
			String decision = rs.getString("DECISION");
			if ((BrainRulesGate.INGESTED.equals(decision) || BrainRulesGate.EXCLUDED.equals(decision))
					&& BrainRulesGate.neverRule(rules, Objects.toString(rs.getString("EMAIL_NORM"), ""),
							rs.getString("SENDER_PERSON_ID"), rs.getString("FOLDER")) == null) {
				allowed.add(rs.getString("THREAD_ID"));
			}
			return null;
		}, params.toArray());
		return allowed;
	}

	private static List<Map<String, Object>> people(String ownerId, String ownerType, String threadId,
			List<BrainRulesGate.Rule> rules) {
		List<Map<String, Object>> people = CollaborationDbUtils.query("SELECT p.PERSON_ID, p.DISPLAY_NAME, p.EMAIL_NORM "
				+ "FROM BRAIN_THREAD_PARTICIPANT tp JOIN BRAIN_PERSON p ON p.OWNER_ID = tp.OWNER_ID AND p.OWNER_TYPE = tp.OWNER_TYPE "
				+ "AND p.PERSON_ID = tp.PERSON_ID WHERE tp.OWNER_ID = ? AND tp.OWNER_TYPE = ? AND tp.THREAD_ID = ? "
				+ "AND (tp.INCLUDED IS NULL OR tp.INCLUDED = ?) ORDER BY p.DISPLAY_NAME, p.PERSON_ID", rs -> {
			String id = rs.getString("PERSON_ID");
			String email = Objects.toString(rs.getString("EMAIL_NORM"), "");
			return BrainRulesGate.neverRule(rules, email, id, null) == null
					? Map.<String, Object>of("id", id, "name", Objects.toString(rs.getString("DISPLAY_NAME"), email), "email", email)
					: null;
		}, ownerId, ownerType, threadId, true);
		return people.stream().filter(Objects::nonNull).toList();
	}

	@SuppressWarnings("unchecked")
	static Map<String, Object> map(Object value) {
		return value instanceof Map<?, ?> ? new LinkedHashMap<>((Map<String, Object>) value) : new LinkedHashMap<>();
	}

	static List<Map<String, Object>> maps(Object value) {
		return value instanceof List<?> list ? list.stream().map(BrainTopicReviewEvidence::map).toList() : List.of();
	}

	static List<String> strings(Object value) {
		return value instanceof List<?> list ? list.stream().filter(String.class::isInstance).map(String.class::cast).toList() : List.of();
	}
}
