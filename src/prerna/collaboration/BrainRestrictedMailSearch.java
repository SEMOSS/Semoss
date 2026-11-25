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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;

/**
 * Finds mailbox search hits kept out of Brain, without returning their bodies or
 * the subject and sender of never-ingest mail. This does not change the gate.
 */
final class BrainRestrictedMailSearch {
	private static final Logger LOGGER = LogManager.getLogger(BrainRestrictedMailSearch.class);
	private static final int MAX_SEARCH = 100;

	private BrainRestrictedMailSearch() {
	}

	static Map<String, Object> search(User user, String ownerId, String ownerType, List<String> topics,
			String query, String person, Timestamp after, Timestamp before) {
		List<String> words = BrainThreadFinder.bodyTokens(query);
		if (words.isEmpty() && (person == null || person.isBlank())) {
			return Map.of("status", "not_searched", "items", List.of(),
					"note", "Enter search words or a sender to check for restricted mail.");
		}
		List<Map<String, Object>> messages;
		try {
			List<String> searchWords = words.isEmpty() ? List.of("from:" + person.trim())
					: words.size() > 6 ? words.subList(0, 6) : words;
			messages = BrainMessageSource.current().search(user, searchWords, true, instant(after), instant(before),
					MAX_SEARCH);
			if (messages != null && messages.isEmpty() && searchWords.size() > 1) {
				messages = BrainMessageSource.current().search(user, searchWords, false, instant(after),
						instant(before), MAX_SEARCH);
			}
		} catch (Exception e) {
			LOGGER.warn("Could not search the mailbox for restricted mail", e);
			return Map.of("status", "unavailable", "items", List.of(),
					"note", "Restricted mail could not be checked. Check the mailbox connection and try again.");
		}
		if (messages == null) {
			return Map.of("status", "unavailable", "items", List.of(),
					"note", "This mailbox does not support searching for restricted mail.");
		}
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		BrainMailFolders.Tree folders = BrainMailFolders.load(ownerId, ownerType);
		List<Map<String, Object>> items = new ArrayList<>();
		for (Map<String, Object> message : messages) {
			Timestamp received = BrainTopicBodySearch.timestamp(message.get("receivedDateTime"));
			if ((after != null && (received == null || received.before(after)))
					|| (before != null && (received == null || !received.before(before)))
					|| Boolean.TRUE.equals(message.get("isDraft"))
					|| folders.skipped(CollaborationDbUtils.asString(message.get("parentFolderId")))) {
				continue;
			}
			Map<String, Object> item = diagnose(ownerId, ownerType, topics, person, rules, message);
			if (item != null) {
				items.add(item);
			}
		}
		return Map.of("status", messages.size() >= MAX_SEARCH ? "partial" : "searched", "items", items,
				"note", "Mailbox matches are checked against known rules; results may be incomplete or match only "
						+ "some search words. Unlinked messages cannot be attributed to a topic. Unrecorded "
						+ "keyword blocks cannot be confirmed without reading the body. The person filter checks "
						+ "only the sender. Message bodies and never-ingest details are not shown.");
	}

	private static Instant instant(Timestamp value) {
		return value == null ? null : value.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC);
	}

	private static Map<String, Object> diagnose(String ownerId, String ownerType, List<String> topics, String personFilter,
			List<BrainRulesGate.Rule> rules, Map<String, Object> message) {
		String graphId = CollaborationDbUtils.asString(message.get("id"));
		if (graphId == null) {
			return null;
		}
		String from = BrainMailImport.address(message.get("from"));
		if (personFilter != null && !personFilter.isBlank()) {
			String name = null;
			if (message.get("from") instanceof Map<?, ?> sender
					&& sender.get("emailAddress") instanceof Map<?, ?> address) {
				name = BrainRulesGate.norm(CollaborationDbUtils.asString(address.get("name")));
			}
			String needle = BrainRulesGate.norm(personFilter);
			if ((from == null || !from.contains(needle)) && (name == null || !name.contains(needle))) {
				return null;
			}
		}
		String person = from == null ? null : BrainRulesGate.findPerson(ownerId, ownerType, from);
		String[] stored = CollaborationDbUtils.queryOne("SELECT THREAD_ID, SENDER_PERSON_ID, FOLDER, DECISION, RULE_ID "
				+ "FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND GRAPH_ID = ?",
				rs -> new String[] { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
						rs.getString(5) }, ownerId, ownerType, graphId);
		String thread = stored == null ? null : stored[0];
		if (person == null && stored != null) {
			person = stored[1];
		}
		String folder = CollaborationDbUtils.asString(message.get("parentFolderId"));
		if (folder == null && stored != null) {
			folder = stored[2];
		}
		if ((from == null && rules.stream().anyMatch(rule -> BrainRuleUtils.NEVER_SENDER.equals(rule.kind())
				|| BrainRuleUtils.NEVER_DOMAIN.equals(rule.kind()) || "exclude_everywhere".equals(rule.kind())))
				|| (folder == null && rules.stream().anyMatch(rule -> BrainRuleUtils.NEVER_FOLDER.equals(rule.kind())))) {
			return null;
		}
		String conversation = CollaborationDbUtils.asString(message.get("conversationId"));
		if (thread == null && conversation != null) {
			thread = CollaborationDbUtils.queryOne("SELECT THREAD_ID FROM BRAIN_THREAD WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND THREAD_KEY = ?", rs -> rs.getString(1), ownerId, ownerType,
					"email:" + conversation);
			if (thread == null) {
				thread = CollaborationDbUtils.queryOne("SELECT THREAD_ID FROM BRAIN_THREAD_ALIAS WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND THREAD_KEY = ?", rs -> rs.getString(1), ownerId, ownerType,
						"email:" + conversation);
			}
		}
		List<String> linked = thread == null ? List.of() : CollaborationDbUtils.query(
				"SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
				rs -> rs.getString(1), ownerId, ownerType, thread);
		boolean inScope = topics == null || linked.stream().anyMatch(topics::contains);
		if (topics != null && !linked.isEmpty() && !inScope) {
			return null;
		}
		List<String> relevantTopics = topics == null ? linked : topics;
		String decision = stored == null ? null : stored[3];
		String ruleId = stored == null ? null : stored[4];
		BrainRulesGate.Rule never = BrainRulesGate.neverRule(rules, from == null ? "" : from, person, folder);
		String code = null;
		if (never != null || (BrainRulesGate.NEVER.equals(decision) && ruleId != null)) {
			code = "NEVER_INGEST";
		} else if (BrainRulesGate.NEVER.equals(decision)) {
			return null;
		} else if (thread != null && Boolean.TRUE.equals(CollaborationDbUtils.queryOne(
				"SELECT MUTED FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
				rs -> CollaborationDbUtils.getBoolean(rs, "MUTED"), ownerId, ownerType, thread))) {
			code = "THREAD_MUTED";
		} else {
			for (BrainRulesGate.Rule rule : rules) {
				if (BrainRuleUtils.MUTE_SENDER.equals(rule.kind())
						&& BrainRulesGate.matchesSender(rule, from == null ? "" : from, person)) {
					code = "SENDER_MUTED";
					break;
				}
			}
			if (code == null && BrainRulesGate.exclusionRule(rules, relevantTopics, "email", person) != null) {
				code = "IGNORED";
			}
			if (code == null && BrainRulesGate.EXCLUDED.equals(decision) && inScope) {
				code = "IGNORED";
			}
			if (code == null && thread != null && person != null && inScope
					&& Boolean.FALSE.equals(CollaborationDbUtils.queryOne(
							"SELECT INCLUDED FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
									+ "AND THREAD_ID = ? AND PERSON_ID = ?",
							rs -> CollaborationDbUtils.getBoolean(rs, "INCLUDED"), ownerId, ownerType, thread, person))) {
				code = "IGNORED";
			}
			if (code == null && BrainRulesGate.MUTED.equals(decision)) {
				code = "MUTED";
			}
		}
		if (code == null) {
			return null;
		}
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("messageId", graphId);
		item.put("code", code);
		item.put("message", switch (code) {
		case "NEVER_INGEST" -> "This email is or was blocked by a global never-ingest rule.";
		case "THREAD_MUTED" -> "This email belongs to a muted thread.";
		case "SENDER_MUTED" -> "This email was muted by a sender rule.";
		case "MUTED" -> "This email was recorded as muted; the original cause is not available.";
		default -> inScope ? "This email is or was ignored by a rule or participant setting."
				: "An ignore rule applies to this sender, but this email's topic is unknown.";
		});
		item.put("topicMatch", topics == null ? "not_scoped" : inScope ? "linked" : "unknown");
		if (!"NEVER_INGEST".equals(code)
				&& rules.stream().noneMatch(rule -> BrainRuleUtils.NEVER_KEYWORD.equals(rule.kind()))) {
			item.put("subject", message.get("subject"));
		}
		if (!"NEVER_INGEST".equals(code) && thread != null) {
			item.put("threadId", thread);
		}
		return item;
	}
}
