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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.collaboration.BrainRulesGate.Rule;

/** Applies the owner's explicit Brain exclusions without importing or classifying inbox mail. */
final class InboxBriefFilter {

	private record ThreadScope(boolean muted, List<String> topics, Set<String> excludedPeople) {
	}

	private InboxBriefFilter() {
	}

	static List<Map<String, Object>> filter(Pair<String, String> owner, List<Map<String, Object>> mail) {
		if (mail.isEmpty()) {
			return List.of();
		}
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		List<Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		Set<String> neverIngest = BrainMemoryUtils.neverIngest(ownerId, ownerType);
		Map<String, String> people = new HashMap<>();
		Map<String, ThreadScope> threads = new HashMap<>();
		List<Map<String, Object>> allowed = new ArrayList<>();
		for (Map<String, Object> message : mail) {
			String from = BrainMailImport.address(message.get("from"));
			from = from == null ? "" : from;
			if (!people.containsKey(from)) {
				people.put(from, BrainRulesGate.findPerson(ownerId, ownerType, from));
			}
			String personId = people.get(from);
			String folder = CollaborationDbUtils.asString(message.get("parentFolderId"));
			if (neverIngest.contains(personId) || BrainRulesGate.neverRule(rules, from, personId, folder) != null) {
				continue;
			}
			boolean mutedSender = false;
			for (Rule rule : rules) {
				if ("mute_sender".equals(rule.kind()) && BrainRulesGate.matchesSender(rule, from, personId)) {
					mutedSender = true;
					break;
				}
			}
			if (mutedSender) {
				continue;
			}
			String conversation = CollaborationDbUtils.asString(message.get("conversationId"));
			if (conversation == null) {
				conversation = BrainMailImport.messageId(message);
			}
			ThreadScope thread = conversation == null ? new ThreadScope(false, List.of(), Set.of())
					: threads.computeIfAbsent(conversation, key -> thread(ownerId, ownerType, key));
			if (thread.muted() || (personId != null && thread.excludedPeople().contains(personId))
					|| BrainMemoryRecall.keptOut(rules, "email", thread.topics())
					|| BrainMemoryRecall.keptOut(rules, "outlook", thread.topics())
					|| BrainRulesGate.exclusionRule(rules, thread.topics(), "email", personId) != null
					|| BrainRulesGate.exclusionRule(rules, thread.topics(), "outlook", personId) != null) {
				continue;
			}
			Map<String, Object> clean = clean(message);
			String subject = (String) clean.get("subject");
			String body = (String) clean.get("body");
			if (BrainRulesGate.keywordRule(rules, subject, body) != null) {
				continue;
			}
			Map<String, Object> copy = new LinkedHashMap<>(message);
			copy.put("subject", subject);
			copy.put("body", Map.of("contentType", "text", "content", body));
			// Do not let the prompt's preview fallback restore a signature or quoted history.
			copy.put("bodyPreview", body);
			allowed.add(copy);
		}
		return allowed;
	}

	private static ThreadScope thread(String ownerId, String ownerType, String conversation) {
		String key = "email:" + conversation;
		String legacyKey = "outlook:" + conversation;
		String[] thread = CollaborationDbUtils.queryOne(
				"SELECT THREAD_ID, MUTED FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_KEY IN (?, ?) ORDER BY THREAD_KEY",
				rs -> new String[] { rs.getString("THREAD_ID"), String.valueOf(CollaborationDbUtils.getBoolean(rs, "MUTED")) },
				ownerId, ownerType, key, legacyKey);
		if (thread == null) {
			thread = CollaborationDbUtils.queryOne("SELECT t.THREAD_ID, t.MUTED FROM BRAIN_THREAD_ALIAS a "
					+ "JOIN BRAIN_THREAD t ON t.OWNER_ID = a.OWNER_ID AND t.OWNER_TYPE = a.OWNER_TYPE "
					+ "AND t.THREAD_ID = a.THREAD_ID WHERE a.OWNER_ID = ? AND a.OWNER_TYPE = ? AND a.THREAD_KEY IN (?, ?) ORDER BY a.THREAD_KEY",
					rs -> new String[] { rs.getString("THREAD_ID"), String.valueOf(CollaborationDbUtils.getBoolean(rs, "MUTED")) },
					ownerId, ownerType, key, legacyKey);
		}
		if (thread == null) {
			return new ThreadScope(false, List.of(), Set.of());
		}
		String threadId = thread[0];
		List<String> topics = CollaborationDbUtils.query(
				"SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
				rs -> rs.getString("TOPIC_ID"), ownerId, ownerType, threadId);
		Set<String> excluded = new HashSet<>(CollaborationDbUtils.query(
				"SELECT PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND INCLUDED = ?",
				rs -> rs.getString("PERSON_ID"), ownerId, ownerType, threadId, false));
		return new ThreadScope(Boolean.parseBoolean(thread[1]), topics, excluded);
	}

	private static Map<String, Object> clean(Map<String, Object> message) {
		Map<?, ?> unique = message.get("uniqueBody") instanceof Map<?, ?> content ? content : Map.of();
		Map<?, ?> body = message.get("body") instanceof Map<?, ?> content ? content : Map.of();
		String text = CollaborationDbUtils.asString(body.get("content"));
		boolean preview = text == null || text.isBlank();
		return BrainMessageText.extract(CollaborationDbUtils.asString(message.get("subject")),
				CollaborationDbUtils.asString(unique.get("content")), CollaborationDbUtils.asString(unique.get("contentType")),
				preview ? CollaborationDbUtils.asString(message.get("bodyPreview")) : text,
				preview ? "text" : CollaborationDbUtils.asString(body.get("contentType")));
	}
}
