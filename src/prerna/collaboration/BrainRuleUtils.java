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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// Brain rules: BRAIN_RULE, the only thing the rules gate reads
public final class BrainRuleUtils {

	public static final String NEVER_SENDER = "never_sender";
	public static final String NEVER_DOMAIN = "never_domain";
	public static final String NEVER_FOLDER = "never_folder";
	public static final String NEVER_KEYWORD = "never_keyword";
	public static final String EXCLUDE_TOPIC = "exclude_topic";
	public static final String EXCLUDE_CHANNEL = "exclude_channel";
	public static final String EXCLUDE_EVERYWHERE = "exclude_everywhere";
	public static final String MUTE_SENDER = "mute_sender";
	public static final Set<String> KINDS = Set.of(NEVER_SENDER, NEVER_DOMAIN, NEVER_FOLDER, NEVER_KEYWORD,
			EXCLUDE_TOPIC, EXCLUDE_CHANNEL, EXCLUDE_EVERYWHERE, MUTE_SENDER);
	public static final Set<String> RULE_CHANNELS = Set.of("email", "teams");

	private static final String RULE_COLUMNS = "RULE_ID, KIND, VALUE, TOPIC_ID, PERSON_ID, CHANNEL, NOTE, "
			+ "CREATED_BY, CREATED_AT, DISABLED_AT";

	private BrainRuleUtils() {

	}

	// ---- read ----

	// active rules only; a deleted rule keeps its row with DISABLED_AT
	public static Map<String, Object> listRules(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<Map<String, Object>> items = CollaborationDbUtils.query("SELECT " + RULE_COLUMNS + " FROM BRAIN_RULE "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND DISABLED_AT IS NULL ORDER BY CREATED_AT, RULE_ID",
				BrainRuleUtils::mapRule, owner.getValue0(), owner.getValue1());
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", items.size());
		return page;
	}

	static Map<String, Object> getRule(String ownerId, String ownerType, String ruleId) {
		Map<String, Object> rule = CollaborationDbUtils.queryOne("SELECT " + RULE_COLUMNS + " FROM BRAIN_RULE "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RULE_ID = ?", BrainRuleUtils::mapRule, ownerId, ownerType,
				ruleId);
		if (rule == null) {
			throw new IllegalArgumentException("Rule not found");
		}
		return rule;
	}

	// ---- write ----

	// no id creates; an id edits the note or value only (delete and re-create to change the rest)
	public static Map<String, Object> saveRule(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String ruleId = CollaborationDbUtils.asString(changes.get("id"));
		if (ruleId != null) {
			Map<String, Object> current = getRule(ownerId, ownerType, ruleId);
			for (String key : List.of("kind", "topicId", "personId", "channel")) {
				if (changes.containsKey(key) && !Objects.equals(changes.get(key), current.get(key))) {
					throw new IllegalArgumentException("A rule's " + key + " cannot change; delete it and add a new one");
				}
			}
			String kind = (String) current.get("kind");
			String value = changes.containsKey("value")
					? normValue(kind, CollaborationDbUtils.asString(changes.get("value")))
					: (String) current.get("value");
			checkRule(ownerId, ownerType, kind, value, (String) current.get("topicId"),
					(String) current.get("personId"), (String) current.get("channel"));
			CollaborationDbUtils.update("UPDATE BRAIN_RULE SET VALUE = ?, NOTE = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RULE_ID = ?", value,
					changes.containsKey("note") ? CollaborationDbUtils.asString(changes.get("note")) : current.get("note"),
					ownerId, ownerType, ruleId);
			return getRule(ownerId, ownerType, ruleId);
		}

		String kind = CollaborationDbUtils.asString(changes.get("kind"));
		String value = normValue(kind, CollaborationDbUtils.asString(changes.get("value")));
		String topicId = CollaborationDbUtils.asString(changes.get("topicId"));
		String personId = CollaborationDbUtils.asString(changes.get("personId"));
		String channel = CollaborationDbUtils.asString(changes.get("channel"));
		return addRule(ownerId, ownerType, kind, value, topicId, personId, channel,
				CollaborationDbUtils.asString(changes.get("note")));
	}

	// the same active rule twice returns the first one
	static Map<String, Object> addRule(String ownerId, String ownerType, String kind, String value, String topicId,
			String personId, String channel, String note) {
		checkRule(ownerId, ownerType, kind, value, topicId, personId, channel);
		String existing = findActive(ownerId, ownerType, kind, value, topicId, personId, channel);
		if (existing != null) {
			return getRule(ownerId, ownerType, existing);
		}
		String ruleId = UUID.randomUUID().toString();
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, VALUE, "
					+ "TOPIC_ID, PERSON_ID, CHANNEL, NOTE, CREATED_BY, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, ruleId, kind, value, topicId, personId, channel, note, BrainProfileUtils.YOU, now);
			// exclusions cover past messages too: flip the person's current rows on matching threads
			if (EXCLUDE_TOPIC.equals(kind) || EXCLUDE_CHANNEL.equals(kind)) {
				String threads = EXCLUDE_TOPIC.equals(kind)
						? "SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ?"
						: "SELECT THREAD_ID FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SOURCE = ?";
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_PARTICIPANT SET INCLUDED = ?, EXCLUDED_BY = ?, "
						+ "EXCLUDED_RULE_ID = ?, EXCLUDED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ? "
						+ "AND (INCLUDED IS NULL OR INCLUDED = ?) AND THREAD_ID IN (" + threads + ")", false,
						BrainThreadUtils.RULE, ruleId, now, ownerId, ownerType, personId, true, ownerId, ownerType,
						EXCLUDE_TOPIC.equals(kind) ? topicId : channel);
			}
		});
		return getRule(ownerId, ownerType, ruleId);
	}

	// soft delete; people this rule excluded on a thread are included again
	public static Map<String, Object> deleteRule(User user, String ruleId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		getRule(ownerId, ownerType, ruleId);
		disable(ownerId, ownerType, ruleId);
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("deleted", true);
		return result;
	}

	static void disable(String ownerId, String ownerType, String ruleId) {
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.inTransaction(conn -> {
			CollaborationDbUtils.update(conn, "UPDATE BRAIN_RULE SET DISABLED_AT = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RULE_ID = ? AND DISABLED_AT IS NULL", now, ownerId,
					ownerType, ruleId);
			reinclude(conn, ownerId, ownerType, ruleId);
		});
	}

	private static void reinclude(Connection conn, String ownerId, String ownerType, String ruleId)
			throws SQLException {
		CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD_PARTICIPANT SET INCLUDED = ?, EXCLUDED_BY = NULL, "
				+ "EXCLUDED_RULE_ID = NULL, EXCLUDED_AT = NULL WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND EXCLUDED_BY = ? AND EXCLUDED_RULE_ID = ?", true, ownerId, ownerType, BrainThreadUtils.RULE, ruleId);
	}

	// active person-scoped rules of these kinds, for the person drawer's flags
	static List<String> activeForPerson(String ownerId, String ownerType, String personId, List<String> kinds,
			String channel) {
		String sql = "SELECT RULE_ID FROM BRAIN_RULE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ? "
				+ "AND DISABLED_AT IS NULL AND KIND IN (" + CollaborationDbUtils.placeholders(kinds.size()) + ")"
				+ (channel == null ? "" : " AND CHANNEL = ?");
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType, personId));
		params.addAll(kinds);
		if (channel != null) {
			params.add(channel);
		}
		return CollaborationDbUtils.query(sql, rs -> rs.getString(1), params.toArray());
	}

	// ---- checks ----

	private static void checkRule(String ownerId, String ownerType, String kind, String value, String topicId,
			String personId, String channel) {
		if (kind == null || !KINDS.contains(kind)) {
			throw new IllegalArgumentException("Rule kind must be one of " + KINDS);
		}
		boolean needsValue = Set.of(NEVER_DOMAIN, NEVER_FOLDER, NEVER_KEYWORD).contains(kind);
		boolean needsPerson = Set.of(EXCLUDE_TOPIC, EXCLUDE_CHANNEL, EXCLUDE_EVERYWHERE).contains(kind);
		if (needsValue && (value == null || value.isBlank())) {
			throw new IllegalArgumentException(kind + " needs a value");
		}
		if (needsPerson && personId == null) {
			throw new IllegalArgumentException(kind + " needs a personId");
		}
		// a sender rule matches an address or a person
		if ((NEVER_SENDER.equals(kind) || MUTE_SENDER.equals(kind)) && (value == null || value.isBlank())
				&& personId == null) {
			throw new IllegalArgumentException(kind + " needs a value or a personId");
		}
		if (EXCLUDE_TOPIC.equals(kind)) {
			if (topicId == null) {
				throw new IllegalArgumentException("exclude_topic needs a topicId");
			}
			BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
		}
		if (EXCLUDE_CHANNEL.equals(kind) && (channel == null || !RULE_CHANNELS.contains(channel))) {
			throw new IllegalArgumentException("exclude_channel needs a channel, one of " + RULE_CHANNELS);
		}
		if (personId != null && !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_PERSON WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND PERSON_ID = ?", ownerId, ownerType, personId)) {
			throw new IllegalArgumentException("Person not found");
		}
	}

	// addresses and domains compare lower-cased; folder ids are opaque and kept as given
	private static String normValue(String kind, String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		String v = value.trim();
		if (NEVER_DOMAIN.equals(kind)) {
			return v.toLowerCase().replaceFirst("^@", "");
		}
		if (NEVER_SENDER.equals(kind) || MUTE_SENDER.equals(kind)) {
			return v.toLowerCase();
		}
		return v;
	}

	private static String findActive(String ownerId, String ownerType, String kind, String value, String topicId,
			String personId, String channel) {
		return CollaborationDbUtils.queryOne("SELECT RULE_ID FROM BRAIN_RULE WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND KIND = ? AND DISABLED_AT IS NULL AND COALESCE(VALUE, '') = ? AND COALESCE(TOPIC_ID, '') = ? "
				+ "AND COALESCE(PERSON_ID, '') = ? AND COALESCE(CHANNEL, '') = ? ORDER BY CREATED_AT, RULE_ID",
				rs -> rs.getString(1), ownerId, ownerType, kind, value == null ? "" : value,
				topicId == null ? "" : topicId, personId == null ? "" : personId, channel == null ? "" : channel);
	}

	// ---- mapping ----

	private static Map<String, Object> mapRule(ResultSet rs) throws SQLException {
		Map<String, Object> rule = new LinkedHashMap<>();
		rule.put("id", CollaborationDbUtils.getString(rs, "RULE_ID"));
		rule.put("kind", CollaborationDbUtils.getString(rs, "KIND"));
		rule.put("value", CollaborationDbUtils.getString(rs, "VALUE"));
		rule.put("topicId", CollaborationDbUtils.getString(rs, "TOPIC_ID"));
		rule.put("personId", CollaborationDbUtils.getString(rs, "PERSON_ID"));
		rule.put("channel", CollaborationDbUtils.getString(rs, "CHANNEL"));
		rule.put("note", CollaborationDbUtils.getString(rs, "NOTE"));
		rule.put("createdBy", CollaborationDbUtils.getString(rs, "CREATED_BY"));
		rule.put("createdAt", CollaborationDbUtils.getTimestamp(rs, "CREATED_AT"));
		rule.put("disabledAt", CollaborationDbUtils.getTimestamp(rs, "DISABLED_AT"));
		return rule;
	}
}
