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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;

import prerna.auth.User;

// Brain people and accounts: BRAIN_PERSON with addresses, BRAIN_ACCOUNT
public final class BrainPeopleUtils {

	public static final Set<String> ACCOUNT_KINDS = Set.of("client", "internal", "other");

	private static final String PERSON_COLUMNS = "p.PERSON_ID, p.EMAIL_NORM, p.DISPLAY_NAME, p.JOB_TITLE, "
			+ "p.ACCOUNT_ID, p.RELATIONSHIP, p.IS_VIP, p.STRENGTH, p.LAST_CONTACT_AT, a.COLOR";

	private static final String PERSON_FROM = " FROM BRAIN_PERSON p LEFT JOIN BRAIN_ACCOUNT a "
			+ "ON a.OWNER_ID = p.OWNER_ID AND a.OWNER_TYPE = p.OWNER_TYPE AND a.ACCOUNT_ID = p.ACCOUNT_ID";

	// marks a never-ingest person rule among the channel names
	private static final String NEVER = "never";

	private static final String ACCOUNT_COLUMNS = "ACCOUNT_ID, NAME, KIND, DOMAINS_JSON, COLOR";

	private BrainPeopleUtils() {

	}

	// ---- people ----

	public static Map<String, Object> listPeople(User user, String query, String accountId, String topicId,
			int limit, int offset) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();

		StringBuilder where = new StringBuilder(" WHERE p.OWNER_ID = ? AND p.OWNER_TYPE = ?");
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		if (query != null && !query.isBlank()) {
			String like = "%" + query.trim().toLowerCase() + "%";
			where.append(" AND (LOWER(p.DISPLAY_NAME) LIKE ? OR p.EMAIL_NORM LIKE ?)");
			params.addAll(List.of(like, like));
		}
		if (accountId != null) {
			where.append(" AND p.ACCOUNT_ID = ?");
			params.add(accountId);
		}
		if (topicId != null) {
			where.append(" AND EXISTS (SELECT 1 FROM BRAIN_TOPIC_PERSON tp WHERE tp.OWNER_ID = p.OWNER_ID "
					+ "AND tp.OWNER_TYPE = p.OWNER_TYPE AND tp.PERSON_ID = p.PERSON_ID AND tp.TOPIC_ID = ? "
					+ "AND tp.STATE = ?)");
			params.addAll(List.of(topicId, BrainTopicUtils.MEMBER));
		}

		List<Map<String, Object>> items = CollaborationDbUtils.query(
				CollaborationDbUtils.page("SELECT " + PERSON_COLUMNS + PERSON_FROM + where
						+ " ORDER BY COALESCE(p.STRENGTH, 0) DESC, p.DISPLAY_NAME, p.PERSON_ID", limit, offset),
				BrainPeopleUtils::mapPerson, params.toArray());
		addCountsAndTopics(ownerId, ownerType, items);

		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", CollaborationDbUtils.count("SELECT COUNT(*)" + PERSON_FROM + where, params.toArray()));
		return page;
	}

	public static Map<String, Object> getPerson(User user, String personId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return getPerson(owner.getValue0(), owner.getValue1(), personId);
	}

	static Map<String, Object> getPerson(String ownerId, String ownerType, String personId) {
		Map<String, Object> person = CollaborationDbUtils.queryOne("SELECT " + PERSON_COLUMNS + PERSON_FROM
				+ " WHERE p.OWNER_ID = ? AND p.OWNER_TYPE = ? AND p.PERSON_ID = ?", BrainPeopleUtils::mapPerson,
				ownerId, ownerType, personId);
		if (person == null) {
			throw new IllegalArgumentException("Person not found");
		}
		addCountsAndTopics(ownerId, ownerType, List.of(person));
		person.put("topicMembership", CollaborationDbUtils.query("SELECT TOPIC_ID, ROLE_LABEL, STATE "
				+ "FROM BRAIN_TOPIC_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ? ORDER BY TOPIC_ID",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("topicId", CollaborationDbUtils.getString(rs, "TOPIC_ID"));
					row.put("role", CollaborationDbUtils.getString(rs, "ROLE_LABEL"));
					row.put("state", CollaborationDbUtils.getString(rs, "STATE"));
					return row;
				}, ownerId, ownerType, personId));
		person.put("threadInclusion", CollaborationDbUtils.query("SELECT THREAD_ID, INCLUDED "
				+ "FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ? "
				+ "ORDER BY LAST_SEEN_AT DESC, THREAD_ID", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("threadId", CollaborationDbUtils.getString(rs, "THREAD_ID"));
					row.put("included", !Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "INCLUDED")));
					return row;
				}, ownerId, ownerType, personId));
		return person;
	}

	// partial Person: relationship, vip, accountId, neverIngest, channelScope; the flags save only as rules
	@SuppressWarnings("unchecked")
	public static Map<String, Object> savePerson(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String personId = CollaborationDbUtils.asString(changes.get("id"));
		if (personId == null) {
			throw new IllegalArgumentException("Person id is required");
		}
		Map<String, Object> current = getPerson(ownerId, ownerType, personId);
		requireAccount(ownerId, ownerType, CollaborationDbUtils.asString(changes.get("accountId")));
		Map<String, Object> scope = new LinkedHashMap<>((Map<String, Object>) current.get("channelScope"));
		if (changes.get("channelScope") instanceof Map<?, ?> wanted) {
			for (String channel : BrainRuleUtils.RULE_CHANNELS) {
				if (wanted.containsKey(channel)) {
					scope.put(channel, Boolean.parseBoolean(String.valueOf(wanted.get(channel))));
				}
			}
		}

		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		CollaborationDbUtils.setIfPresent(changes, "relationship", "RELATIONSHIP", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "accountId", "ACCOUNT_ID", sets, params);
		if (changes.containsKey("vip")) {
			CollaborationDbUtils.addSet(sets, params, "IS_VIP", Boolean.parseBoolean(String.valueOf(changes.get("vip"))));
		}
		Boolean neverIngest = changes.containsKey("neverIngest")
				? Boolean.parseBoolean(String.valueOf(changes.get("neverIngest")))
				: null;
		CollaborationDbUtils.addSet(sets, params, "UPDATED_AT", CollaborationDbUtils.now());
		params.addAll(List.of(ownerId, ownerType, personId));
		CollaborationDbUtils.update("UPDATE BRAIN_PERSON SET " + String.join(", ", sets)
				+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", params.toArray());

		// the Blocked list: never ingest is an exclude_everywhere rule; clearing it drops any person-level never rule
		if (Boolean.TRUE.equals(neverIngest)) {
			BrainRuleUtils.addRule(ownerId, ownerType, BrainRuleUtils.EXCLUDE_EVERYWHERE, null, null, personId, null,
					"Never ingest");
		} else if (Boolean.FALSE.equals(neverIngest)) {
			for (String ruleId : BrainRuleUtils.activeForPerson(ownerId, ownerType, personId,
					List.of(BrainRuleUtils.EXCLUDE_EVERYWHERE, BrainRuleUtils.NEVER_SENDER), null)) {
				BrainRuleUtils.disable(ownerId, ownerType, ruleId);
			}
		}
		for (String channel : BrainRuleUtils.RULE_CHANNELS) {
			List<String> active = BrainRuleUtils.activeForPerson(ownerId, ownerType, personId,
					List.of(BrainRuleUtils.EXCLUDE_CHANNEL), channel);
			if (Boolean.FALSE.equals(scope.get(channel)) && active.isEmpty()) {
				BrainRuleUtils.addRule(ownerId, ownerType, BrainRuleUtils.EXCLUDE_CHANNEL, null, null, personId,
						channel, "Not on " + channel);
			} else if (Boolean.TRUE.equals(scope.get(channel))) {
				for (String ruleId : active) {
					BrainRuleUtils.disable(ownerId, ownerType, ruleId);
				}
			}
		}
		return getPerson(ownerId, ownerType, personId);
	}

	// ---- accounts ----

	public static Map<String, Object> listAccounts(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		List<Map<String, Object>> items = CollaborationDbUtils.query("SELECT " + ACCOUNT_COLUMNS
				+ " FROM BRAIN_ACCOUNT WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY NAME, ACCOUNT_ID",
				BrainPeopleUtils::mapAccount, owner.getValue0(), owner.getValue1());
		Map<String, Object> page = new LinkedHashMap<>();
		page.put("items", items);
		page.put("total", items.size());
		return page;
	}

	// partial Account: no id creates, an id edits
	public static Map<String, Object> saveAccount(User user, Map<String, Object> changes) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		String accountId = CollaborationDbUtils.asString(changes.get("id"));
		String name = CollaborationDbUtils.asString(changes.get("name"));
		if ((accountId == null || changes.containsKey("name")) && (name == null || name.isBlank())) {
			throw new IllegalArgumentException("Account name is required");
		}
		String kind = CollaborationDbUtils.asString(changes.get("kind"));
		if (changes.containsKey("kind") && !ACCOUNT_KINDS.contains(kind)) {
			throw new IllegalArgumentException("Account kind must be one of " + ACCOUNT_KINDS);
		}

		List<String> sets = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		CollaborationDbUtils.setIfPresent(changes, "name", "NAME", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "kind", "KIND", sets, params);
		CollaborationDbUtils.setIfPresent(changes, "color", "COLOR", sets, params);
		if (changes.containsKey("domain")) {
			String domain = CollaborationDbUtils.asString(changes.get("domain"));
			CollaborationDbUtils.addSet(sets, params, "DOMAINS_JSON", domain == null || domain.isBlank() ? null
					: CollaborationDbUtils.toJson(List.of(domain.trim().toLowerCase().replaceFirst("^@", ""))));
		}

		Timestamp now = CollaborationDbUtils.now();
		if (accountId == null) {
			accountId = UUID.randomUUID().toString();
			List<String> columns = new ArrayList<>();
			for (String set : sets) {
				columns.add(set.substring(0, set.indexOf(' ')));
			}
			if (!changes.containsKey("kind")) {
				columns.add("KIND");
				params.add("other");
			}
			columns.addAll(List.of("OWNER_ID", "OWNER_TYPE", "ACCOUNT_ID", "STATUS", "CREATED_AT", "UPDATED_AT"));
			params.addAll(List.of(ownerId, ownerType, accountId, BrainTopicUtils.ACTIVE, now, now));
			CollaborationDbUtils.update("INSERT INTO BRAIN_ACCOUNT (" + String.join(", ", columns) + ") VALUES ("
					+ CollaborationDbUtils.placeholders(columns.size()) + ")", params.toArray());
		} else {
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_ACCOUNT WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND ACCOUNT_ID = ?", ownerId, ownerType, accountId)) {
				throw new IllegalArgumentException("Account not found");
			}
			CollaborationDbUtils.addSet(sets, params, "UPDATED_AT", now);
			params.addAll(List.of(ownerId, ownerType, accountId));
			CollaborationDbUtils.update("UPDATE BRAIN_ACCOUNT SET " + String.join(", ", sets)
					+ " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ACCOUNT_ID = ?", params.toArray());
		}
		return CollaborationDbUtils.queryOne("SELECT " + ACCOUNT_COLUMNS + " FROM BRAIN_ACCOUNT "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND ACCOUNT_ID = ?", BrainPeopleUtils::mapAccount, ownerId,
				ownerType, accountId);
	}

	// ---- helpers ----

	// an accountId from the caller must be one of the owner's accounts; null is allowed
	static void requireAccount(String ownerId, String ownerType, String accountId) {
		if (accountId != null && !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_ACCOUNT WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND ACCOUNT_ID = ?", ownerId, ownerType, accountId)) {
			throw new IllegalArgumentException("Account not found");
		}
	}

	// a personId from the caller must be one of the owner's people; null is allowed
	static void requirePerson(String ownerId, String ownerType, String personId) {
		if (personId != null && !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_PERSON WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND PERSON_ID = ?", ownerId, ownerType, personId)) {
			throw new IllegalArgumentException("Person not found");
		}
	}

	// rule-derived flags, message counts per channel, calendar threads as meetings, and member topics, one query
	// each for the page
	private static void addCountsAndTopics(String ownerId, String ownerType, List<Map<String, Object>> people) {
		if (people.isEmpty()) {
			return;
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		for (Map<String, Object> person : people) {
			params.add(person.get("id"));
		}
		String in = CollaborationDbUtils.placeholders(people.size());
		String threadJoin = " JOIN BRAIN_THREAD t ON t.OWNER_ID = x.OWNER_ID AND t.OWNER_TYPE = x.OWNER_TYPE "
				+ "AND t.THREAD_ID = x.THREAD_ID";

		Map<String, Map<String, Integer>> counts = new HashMap<>();
		for (Object[] row : CollaborationDbUtils.query("SELECT x.SENDER_PERSON_ID, t.SOURCE, COUNT(*) FROM BRAIN_MESSAGE x"
				+ threadJoin + " WHERE x.OWNER_ID = ? AND x.OWNER_TYPE = ? AND x.SENDER_PERSON_ID IN (" + in + ") "
				+ "GROUP BY x.SENDER_PERSON_ID, t.SOURCE", rs -> new Object[] { rs.getString(1), rs.getString(2),
						rs.getInt(3) }, params.toArray())) {
			counts.computeIfAbsent((String) row[0], k -> new HashMap<>()).put((String) row[1], (Integer) row[2]);
		}
		for (Object[] row : CollaborationDbUtils.query("SELECT x.PERSON_ID, COUNT(*) FROM BRAIN_THREAD_PARTICIPANT x"
				+ threadJoin + " WHERE x.OWNER_ID = ? AND x.OWNER_TYPE = ? AND x.PERSON_ID IN (" + in + ") "
				+ "AND t.SOURCE = 'calendar' GROUP BY x.PERSON_ID", rs -> new Object[] { rs.getString(1),
						rs.getInt(2) }, params.toArray())) {
			counts.computeIfAbsent((String) row[0], k -> new HashMap<>()).put("meetings", (Integer) row[1]);
		}

		List<Object> topicParams = new ArrayList<>(params);
		topicParams.add(BrainTopicUtils.MEMBER);
		Map<String, List<String>> topics = new HashMap<>();
		for (Pair<String, String> row : CollaborationDbUtils.query("SELECT PERSON_ID, TOPIC_ID FROM BRAIN_TOPIC_PERSON "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID IN (" + in + ") AND STATE = ? "
				+ "ORDER BY TOPIC_ID", rs -> Pair.with(rs.getString(1), rs.getString(2)), topicParams.toArray())) {
			topics.computeIfAbsent(row.getValue0(), k -> new ArrayList<>()).add(row.getValue1());
		}

		// the gate reads rules only, so the drawer's flags come from active person rules, never a copy
		List<Object> ruleParams = new ArrayList<>(params);
		ruleParams.addAll(List.of(BrainRuleUtils.EXCLUDE_EVERYWHERE, BrainRuleUtils.NEVER_SENDER,
				BrainRuleUtils.EXCLUDE_CHANNEL));
		Map<String, Set<String>> blocked = new HashMap<>();
		for (Pair<String, String> row : CollaborationDbUtils.query("SELECT PERSON_ID, KIND, CHANNEL FROM BRAIN_RULE "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID IN (" + in + ") AND DISABLED_AT IS NULL "
				+ "AND KIND IN (?, ?, ?)", rs -> Pair.with(rs.getString(1), BrainRuleUtils.EXCLUDE_CHANNEL
						.equals(rs.getString(2)) ? rs.getString(3) : NEVER), ruleParams.toArray())) {
			blocked.computeIfAbsent(row.getValue0(), k -> new HashSet<>()).add(row.getValue1());
		}

		for (Map<String, Object> person : people) {
			Map<String, Integer> mine = counts.getOrDefault(person.get("id"), Map.of());
			Set<String> off = blocked.getOrDefault(person.get("id"), Set.of());
			Map<String, Object> channels = new LinkedHashMap<>();
			channels.put("email", mine.getOrDefault("email", 0));
			channels.put("teams", mine.getOrDefault("teams", 0));
			channels.put("meetings", mine.getOrDefault("meetings", 0));
			Map<String, Object> scope = new LinkedHashMap<>();
			for (String channel : List.of("email", "teams")) {
				scope.put(channel, !off.contains(channel));
			}
			// contract order: vip, neverIngest, strength, lastContact, channels, then scope and topics
			Object strength = person.remove("strength");
			Object lastContact = person.remove("lastContact");
			person.put("neverIngest", off.contains(NEVER));
			person.put("strength", strength);
			person.put("lastContact", lastContact);
			person.put("channels", channels);
			person.put("channelScope", scope);
			person.put("topics", topics.getOrDefault(person.get("id"), new ArrayList<>()));
		}
	}

	// ---- mapping ----

	private static Map<String, Object> mapPerson(ResultSet rs) throws SQLException {
		String name = CollaborationDbUtils.getString(rs, "DISPLAY_NAME");
		String email = CollaborationDbUtils.getString(rs, "EMAIL_NORM");
		Map<String, Object> person = new LinkedHashMap<>();
		person.put("id", CollaborationDbUtils.getString(rs, "PERSON_ID"));
		person.put("name", name);
		person.put("initials", initials(name, email));
		person.put("email", email);
		person.put("accountId", CollaborationDbUtils.getString(rs, "ACCOUNT_ID"));
		person.put("title", CollaborationDbUtils.getString(rs, "JOB_TITLE"));
		person.put("relationship", CollaborationDbUtils.getString(rs, "RELATIONSHIP"));
		// people take their account's color
		person.put("color", CollaborationDbUtils.getString(rs, "COLOR"));
		person.put("vip", Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_VIP")));
		Integer strength = CollaborationDbUtils.getInteger(rs, "STRENGTH");
		person.put("strength", strength == null ? 0 : strength);
		person.put("lastContact", CollaborationDbUtils.getTimestamp(rs, "LAST_CONTACT_AT"));
		return person;
	}

	private static Map<String, Object> mapAccount(ResultSet rs) throws SQLException {
		List<Object> domains = CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "DOMAINS_JSON"));
		Map<String, Object> account = new LinkedHashMap<>();
		account.put("id", CollaborationDbUtils.getString(rs, "ACCOUNT_ID"));
		account.put("name", CollaborationDbUtils.getString(rs, "NAME"));
		account.put("domain", domains.isEmpty() ? null : String.valueOf(domains.get(0)));
		account.put("kind", CollaborationDbUtils.getString(rs, "KIND"));
		account.put("color", CollaborationDbUtils.getString(rs, "COLOR"));
		return account;
	}

	static String initials(String name, String email) {
		String source = name != null && !name.isBlank() ? name.trim() : email;
		if (source == null || source.isBlank()) {
			return null;
		}
		String[] words = source.split("\\s+");
		String first = words[0].substring(0, 1);
		String last = words.length > 1 ? words[words.length - 1].substring(0, 1) : "";
		return (first + last).toUpperCase();
	}
}
