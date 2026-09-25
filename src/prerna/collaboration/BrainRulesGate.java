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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

// GATE-01: decides from headers and owner rules alone, before any body fetch or model call.
// Never reads or logs the subject. One owner's messages go through one at a time (see lockFor).
public final class BrainRulesGate {

	public static final String INGESTED = "ingested";
	public static final String NEVER = "never";
	public static final String EXCLUDED = "excluded";
	public static final String MUTED = "muted";
	public static final String OFF = "off";
	private static final String NEVER_KEYWORD = "never_keyword";

	// never-ingest kinds in the order they are checked
	private static final List<String> NEVER_KINDS = List.of("never_sender", "exclude_everywhere", "never_domain",
			"never_folder");

	record Rule(String id, String kind, String value, String topicId, String personId, String channel) {
	}

	private BrainRulesGate() {

	}

	// headers: source, messageId, graphId, conversationId, folderId, from, receivedAt (ISO-8601 UTC)
	public static Map<String, Object> check(String ownerId, String ownerType, Map<String, String> headers) {
		synchronized (lockFor(ownerId, ownerType)) {
			return checkLocked(ownerId, ownerType, headers);
		}
	}

	private static Map<String, Object> checkLocked(String ownerId, String ownerType, Map<String, String> headers) {
		String source = required(headers, "source");
		String messageId = required(headers, "messageId");
		String from = norm(required(headers, "from"));
		String conversationId = headers.get("conversationId");
		String folderId = headers.get("folderId");
		Timestamp receivedAt = toTimestamp(headers.get("receivedAt"));
		String messageKey = CollaborationDbUtils.deterministicId(ownerId, ownerType, source, messageId);

		// replay: the first decision stands and nothing is bumped twice
		Map<String, Object> replay = CollaborationDbUtils.queryOne(
				"SELECT DECISION, RULE_ID, THREAD_ID, SENDER_PERSON_ID FROM BRAIN_MESSAGE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?",
				rs -> result(CollaborationDbUtils.getString(rs, "DECISION"), CollaborationDbUtils.getString(rs, "RULE_ID"),
						CollaborationDbUtils.getString(rs, "THREAD_ID"),
						CollaborationDbUtils.getString(rs, "SENDER_PERSON_ID")),
				ownerId, ownerType, messageKey);
		if (replay != null) {
			return replay;
		}

		// source off (or never connected): drop without writing anything
		Boolean enabled = CollaborationDbUtils.queryOne(
				"SELECT ENABLED FROM SOURCE_CONNECTION WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ?",
				rs -> CollaborationDbUtils.getBoolean(rs, "ENABLED"), ownerId, ownerType, source);
		if (!Boolean.TRUE.equals(enabled)) {
			return result(OFF, null, null, null);
		}

		String personId = findPerson(ownerId, ownerType, from);
		String[] thread = conversationId == null ? null
				: CollaborationDbUtils.queryOne(
						"SELECT THREAD_ID, MUTED FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_KEY = ?",
						rs -> new String[] { rs.getString("THREAD_ID"),
								String.valueOf(Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MUTED"))) },
						ownerId, ownerType, source + ":" + conversationId);
		String threadId = thread == null ? null : thread[0];
		List<Rule> rules = activeRules(ownerId, ownerType);

		// never-ingest: counted, not listed, so no thread on the row
		Rule never = neverRule(rules, from, personId, folderId);
		if (never != null) {
			return record(ownerId, ownerType, messageKey, headers, null, personId, receivedAt, NEVER, never.id(), null);
		}

		// excluded on this thread, a linked topic, or this channel
		if (personId != null) {
			Boolean included = null;
			String excludedRuleId = null;
			List<String> topicIds = List.of();
			if (threadId != null) {
				String[] participant = CollaborationDbUtils.queryOne(
						"SELECT INCLUDED, EXCLUDED_RULE_ID FROM BRAIN_THREAD_PARTICIPANT "
								+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
						rs -> new String[] { String.valueOf(CollaborationDbUtils.getBoolean(rs, "INCLUDED")),
								rs.getString("EXCLUDED_RULE_ID") },
						ownerId, ownerType, threadId, personId);
				if (participant != null) {
					included = !"false".equals(participant[0]);
					excludedRuleId = participant[1];
				}
				topicIds = CollaborationDbUtils.query(
						"SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
						rs -> rs.getString("TOPIC_ID"), ownerId, ownerType, threadId);
			}
			if (Boolean.FALSE.equals(included)) {
				return record(ownerId, ownerType, messageKey, headers, threadId, personId, receivedAt, EXCLUDED,
						excludedRuleId, included);
			}
			Rule exclusion = exclusionRule(rules, topicIds, source, personId);
			if (exclusion != null) {
				return record(ownerId, ownerType, messageKey, headers, threadId, personId, receivedAt, EXCLUDED,
						exclusion.id(), included);
			}
		}

		// muted thread, then muted sender
		if (thread != null && Boolean.parseBoolean(thread[1])) {
			return record(ownerId, ownerType, messageKey, headers, threadId, personId, receivedAt, MUTED, null, null);
		}
		for (Rule rule : rules) {
			if ("mute_sender".equals(rule.kind()) && matchesSender(rule, from, personId)) {
				return record(ownerId, ownerType, messageKey, headers, threadId, personId, receivedAt, MUTED, rule.id(),
						null);
			}
		}

		return record(ownerId, ownerType, messageKey, headers, threadId, personId, receivedAt, INGESTED, null, null);
	}

	// GATE-02: after a passing message's body is fetched and cleaned (BrainMessageText), a never_keyword rule
	// still stops it. The text is matched in memory, never stored or logged.
	public static Map<String, Object> checkText(String ownerId, String ownerType, String source, String messageId,
			String subject, String body) {
		synchronized (lockFor(ownerId, ownerType)) {
			return checkTextLocked(ownerId, ownerType, source, messageId, subject, body);
		}
	}

	private static Map<String, Object> checkTextLocked(String ownerId, String ownerType, String source,
			String messageId, String subject, String body) {
		String messageKey = CollaborationDbUtils.deterministicId(ownerId, ownerType, source, messageId);
		String[] row = CollaborationDbUtils.queryOne("SELECT DECISION, RULE_ID, THREAD_ID, SENDER_PERSON_ID "
				+ "FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?",
				rs -> new String[] { rs.getString("DECISION"), rs.getString("RULE_ID"), rs.getString("THREAD_ID"),
						rs.getString("SENDER_PERSON_ID") },
				ownerId, ownerType, messageKey);
		if (row == null) {
			throw new IllegalArgumentException("Run the header gate on this message first");
		}
		String decision = row[0];
		String threadId = row[2];
		String personId = row[3];
		if (!INGESTED.equals(decision) && !EXCLUDED.equals(decision)) {
			return result(decision, row[1], threadId, personId);
		}
		Rule keyword = keywordRule(activeRules(ownerId, ownerType), subject, body);
		if (keyword == null) {
			return result(decision, row[1], threadId, personId);
		}
		// now a never-ingest stop: counted, not listed, and an exclusion's hidden-count bump is taken back
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_MESSAGE SET DECISION = ?, RULE_ID = ?, THREAD_ID = NULL "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?", NEVER, keyword.id(), ownerId,
					ownerType, messageKey);
			if (EXCLUDED.equals(decision) && threadId != null && personId != null) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_PARTICIPANT SET HIDDEN_COUNT = CASE WHEN "
						+ "HIDDEN_COUNT > 0 THEN HIDDEN_COUNT - 1 ELSE 0 END WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND THREAD_ID = ? AND PERSON_ID = ?", ownerId, ownerType, threadId, personId);
			}
		});
		return result(NEVER, keyword.id(), null, personId);
	}

	// the first never_keyword rule found in the clean subject or body
	static Rule keywordRule(List<Rule> rules, String subject, String body) {
		String text = (subject == null ? "" : subject) + "\n" + (body == null ? "" : body);
		for (Rule rule : rules) {
			if (NEVER_KEYWORD.equals(rule.kind()) && rule.value() != null && !rule.value().isBlank()
					&& keywordPattern(rule.value()).matcher(text).find()) {
				return rule;
			}
		}
		return null;
	}

	// case-insensitive whole words; any whitespace in the keyword matches any run of whitespace
	static Pattern keywordPattern(String keyword) {
		StringBuilder regex = new StringBuilder("(?<![\\p{L}\\p{N}])");
		String[] words = keyword.trim().split("\\s+");
		for (int i = 0; i < words.length; i++) {
			regex.append(i == 0 ? "" : "\\s+").append(Pattern.quote(words[i]));
		}
		regex.append("(?![\\p{L}\\p{N}])");
		return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
	}

	// writes the BRAIN_MESSAGE row; an exclusion on a known thread also bumps the sender's hidden count.
	// included is the sender's current participant flag, null when there is no participant row yet
	private static Map<String, Object> record(String ownerId, String ownerType, String messageKey,
			Map<String, String> headers, String threadId, String personId, Timestamp receivedAt, String decision,
			String ruleId, Boolean included) {
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn,
					"INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, THREAD_ID, GRAPH_ID, SENDER_PERSON_ID, "
							+ "FOLDER, RECEIVED_AT, DECISION, RULE_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, messageKey, threadId, headers.get("graphId"), personId, headers.get("folderId"),
					receivedAt, decision, ruleId);
			if (EXCLUDED.equals(decision) && threadId != null) {
				hide(conn, ownerId, ownerType, threadId, personId, receivedAt, ruleId, included);
			}
		});
		return result(decision, ruleId, threadId, personId);
	}

	private static void hide(Connection conn, String ownerId, String ownerType, String threadId, String personId,
			Timestamp receivedAt, String ruleId, Boolean included) throws SQLException {
		if (included == null) {
			CollaborationDbUtils.update(conn,
					"INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, PERSON_ID, ROLES_JSON, INCLUDED, "
							+ "EXCLUDED_BY, EXCLUDED_RULE_ID, EXCLUDED_AT, HIDDEN_COUNT, FIRST_SEEN_AT, LAST_SEEN_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, threadId, personId, CollaborationDbUtils.toJson(List.of("from")), false, "rule",
					ruleId, CollaborationDbUtils.now(), 1, receivedAt, receivedAt);
		} else if (included) {
			// a rule now covers someone who was included on this thread
			CollaborationDbUtils.update(conn,
					"UPDATE BRAIN_THREAD_PARTICIPANT SET HIDDEN_COUNT = COALESCE(HIDDEN_COUNT, 0) + 1, LAST_SEEN_AT = ?, "
							+ "INCLUDED = ?, EXCLUDED_BY = ?, EXCLUDED_RULE_ID = ?, EXCLUDED_AT = ? "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
					receivedAt, false, "rule", ruleId, CollaborationDbUtils.now(), ownerId, ownerType, threadId, personId);
		} else {
			CollaborationDbUtils.update(conn,
					"UPDATE BRAIN_THREAD_PARTICIPANT SET HIDDEN_COUNT = COALESCE(HIDDEN_COUNT, 0) + 1, LAST_SEEN_AT = ? "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
					receivedAt, ownerId, ownerType, threadId, personId);
		}
	}

	// webhook, delta sync, and backfill can deliver the same message at once, so the replay check and the
	// write run under one lock per owner. Covers one server only.
	private static Object lockFor(String ownerId, String ownerType) {
		return CollaborationDbUtils.ownerLock("gate", ownerId, ownerType);
	}

	// active rules, oldest first; the gate and the thread read share them
	static List<Rule> activeRules(String ownerId, String ownerType) {
		return CollaborationDbUtils.query(
				"SELECT RULE_ID, KIND, VALUE, TOPIC_ID, PERSON_ID, CHANNEL FROM BRAIN_RULE "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND DISABLED_AT IS NULL ORDER BY CREATED_AT, RULE_ID",
				rs -> new Rule(rs.getString("RULE_ID"), rs.getString("KIND"), rs.getString("VALUE"),
						rs.getString("TOPIC_ID"), rs.getString("PERSON_ID"), rs.getString("CHANNEL")),
				ownerId, ownerType);
	}

	// the first never-ingest rule for this sender or folder, in NEVER_KINDS order; from is normalized
	static Rule neverRule(List<Rule> rules, String from, String personId, String folderId) {
		for (String kind : NEVER_KINDS) {
			for (Rule rule : rules) {
				if (kind.equals(rule.kind()) && matchesNever(rule, from, personId, folderId)) {
					return rule;
				}
			}
		}
		return null;
	}

	// an exclude_topic or exclude_channel rule covering this person on a thread
	static Rule exclusionRule(List<Rule> rules, List<String> topicIds, String source, String personId) {
		for (Rule rule : rules) {
			boolean onTopic = "exclude_topic".equals(rule.kind()) && topicIds.contains(rule.topicId());
			boolean onChannel = "exclude_channel".equals(rule.kind()) && source.equals(rule.channel());
			if ((onTopic || onChannel) && personId != null && personId.equals(rule.personId())) {
				return rule;
			}
		}
		return null;
	}

	private static boolean matchesNever(Rule rule, String from, String personId, String folderId) {
		switch (rule.kind()) {
		case "never_sender":
			return matchesSender(rule, from, personId);
		case "exclude_everywhere":
			return personId != null && personId.equals(rule.personId());
		case "never_domain":
			String domain = from.substring(from.lastIndexOf('@') + 1);
			String value = norm(rule.value());
			return value != null && (domain.equals(value) || domain.endsWith("." + value));
		case "never_folder":
			// folder ids are opaque and case-sensitive
			return folderId != null && folderId.equals(rule.value());
		default:
			return false;
		}
	}

	private static boolean matchesSender(Rule rule, String from, String personId) {
		return from.equals(norm(rule.value())) || (personId != null && personId.equals(rule.personId()));
	}

	// address book first, then the person's primary address
	private static String findPerson(String ownerId, String ownerType, String address) {
		String personId = CollaborationDbUtils.queryOne(
				"SELECT PERSON_ID FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND VALUE_NORM = ? "
						+ "ORDER BY PERSON_ID",
				rs -> rs.getString("PERSON_ID"), ownerId, ownerType, address);
		if (personId != null) {
			return personId;
		}
		return CollaborationDbUtils.queryOne(
				"SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND EMAIL_NORM = ? "
						+ "ORDER BY PERSON_ID",
				rs -> rs.getString("PERSON_ID"), ownerId, ownerType, address);
	}

	private static Map<String, Object> result(String decision, String ruleId, String threadId, String personId) {
		Map<String, Object> result = new LinkedHashMap<>();
		// excluded still flows to ingest: read and classified, but no items or alerts
		result.put("pass", INGESTED.equals(decision) || EXCLUDED.equals(decision));
		result.put("decision", decision);
		result.put("ruleId", ruleId);
		result.put("threadId", threadId);
		result.put("personId", personId);
		return result;
	}

	private static String required(Map<String, String> headers, String key) {
		String value = headers.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Gate header " + key + " is required");
		}
		return value;
	}

	static String norm(String value) {
		return value == null ? null : value.trim().toLowerCase();
	}

	// UTC wall time, like every other Collaboration timestamp
	private static Timestamp toTimestamp(String iso) {
		if (iso == null || iso.isBlank()) {
			return CollaborationDbUtils.now();
		}
		try {
			return Timestamp.valueOf(LocalDateTime.ofInstant(Instant.parse(iso), ZoneOffset.UTC));
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException("Gate header receivedAt must be ISO-8601: " + iso);
		}
	}
}
