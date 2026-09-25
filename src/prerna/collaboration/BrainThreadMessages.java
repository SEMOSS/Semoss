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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

// TOOL-02: the filtered thread read behind brain_get_thread. Today's rules run before any body is
// fetched; bodies come from the source at call time, go through BrainMessageText, and are never stored.
public final class BrainThreadMessages {

	private static final int DEFAULT_LIMIT = 20;
	private static final int MAX_LIMIT = 100;

	private record Row(String graphId, String personId, String folder, String at, String decision) {
	}

	private BrainThreadMessages() {

	}

	public static Map<String, Object> read(User user, String threadId, Integer limit) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return read(user, owner.getValue0(), owner.getValue1(), threadId, limit, BrainMessageSource.current());
	}

	static Map<String, Object> read(User user, String ownerId, String ownerType, String threadId, Integer limit,
			BrainMessageSource messages) {
		String[] thread = CollaborationDbUtils.queryOne("SELECT SOURCE, THREAD_KEY, MUTED FROM BRAIN_THREAD "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
				rs -> new String[] { rs.getString("SOURCE"), rs.getString("THREAD_KEY"),
						String.valueOf(Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MUTED"))) },
				ownerId, ownerType, threadId);
		if (thread == null) {
			throw new IllegalArgumentException("Thread not found");
		}
		String source = thread[0];
		String conversationId = thread[1] != null && thread[1].startsWith(source + ":")
				? thread[1].substring(source.length() + 1)
				: null;
		int max = limit == null ? DEFAULT_LIMIT : Math.max(1, Math.min(MAX_LIMIT, limit));

		// newest first, so the limit keeps the latest messages
		List<Row> rows = CollaborationDbUtils.query("SELECT GRAPH_ID, SENDER_PERSON_ID, FOLDER, RECEIVED_AT, DECISION "
				+ "FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? "
				+ "ORDER BY RECEIVED_AT DESC, MESSAGE_KEY",
				rs -> new Row(rs.getString("GRAPH_ID"), rs.getString("SENDER_PERSON_ID"), rs.getString("FOLDER"),
						CollaborationDbUtils.getTimestamp(rs, "RECEIVED_AT"), rs.getString("DECISION")),
				ownerId, ownerType, threadId);
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		List<String> topicIds = CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> rs.getString("TOPIC_ID"), ownerId,
				ownerType, threadId);
		Map<String, Boolean> included = new HashMap<>();
		CollaborationDbUtils.query("SELECT PERSON_ID, INCLUDED FROM BRAIN_THREAD_PARTICIPANT "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
				rs -> included.put(rs.getString("PERSON_ID"), CollaborationDbUtils.getBoolean(rs, "INCLUDED")), ownerId,
				ownerType, threadId);
		Map<String, List<String>> addresses = addresses(ownerId, ownerType, rows);

		// pass 1, no fetch: drop what today's never-ingest rules cover
		int hidden = 0;
		List<Row> candidates = new ArrayList<>();
		for (Row row : rows) {
			if (BrainRulesGate.NEVER.equals(row.decision()) || BrainRulesGate.OFF.equals(row.decision())
					|| isNever(rules, addresses.getOrDefault(row.personId(), List.of()), row)) {
				hidden++;
			} else {
				candidates.add(row);
			}
		}

		// pass 2: fetch newest first until the limit, re-checking the sender address and keyword rules
		List<Map<String, Object>> out = new ArrayList<>();
		int unavailable = 0;
		int next = 0;
		Exception firstError = null;
		for (; next < candidates.size() && out.size() < max; next++) {
			Row row = candidates.get(next);
			Map<String, Object> message;
			try {
				message = messages.fetch(user, source, conversationId, row.graphId());
			} catch (SemossPixelException e) {
				// login required: pass it through untouched so the UI can prompt, and stop fetching
				throw e;
			} catch (Exception e) {
				firstError = firstError == null ? e : firstError;
				unavailable++;
				continue;
			}
			// gone, or no longer in this conversation
			Object messageConversation = message == null ? null : message.get("conversationId");
			if (message == null || (messageConversation != null && conversationId != null
					&& !conversationId.equals(messageConversation))) {
				unavailable++;
				continue;
			}
			Map<String, Object> sender = sender(message);
			String from = BrainRulesGate.norm(CollaborationDbUtils.asString(sender.get("address")));
			if (from != null && BrainRulesGate.neverRule(rules, from, row.personId(), row.folder()) != null) {
				hidden++;
				continue;
			}
			Map<String, Object> clean = clean(message);
			if (BrainRulesGate.keywordRule(rules, (String) clean.get("subject"), (String) clean.get("body")) != null) {
				hidden++;
				continue;
			}
			out.add(entry(row, clean, sender, rules, topicIds, source, included));
		}
		// every fetch failed: surface why (for example no Microsoft login) instead of an empty thread
		if (out.isEmpty() && firstError != null) {
			throw new IllegalStateException("Could not read this thread's messages: " + firstError.getMessage(),
					firstError);
		}
		Collections.reverse(out);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("threadId", threadId);
		result.put("source", source);
		result.put("muted", Boolean.parseBoolean(thread[2]));
		result.put("messages", out);
		result.put("hiddenCount", hidden);
		result.put("unavailableCount", unavailable);
		result.put("hasMore", next < candidates.size());
		return result;
	}

	private static boolean isNever(List<BrainRulesGate.Rule> rules, List<String> known, Row row) {
		// an unknown sender has no address yet; the folder rule still applies and the address is checked after fetch
		for (String address : known.isEmpty() ? List.of("") : known) {
			if (BrainRulesGate.neverRule(rules, address, row.personId(), row.folder()) != null) {
				return true;
			}
		}
		return false;
	}

	private static Map<String, Object> clean(Map<String, Object> message) {
		Map<String, Object> unique = content(message.get("uniqueBody"));
		Map<String, Object> body = content(message.get("body"));
		return BrainMessageText.extract(CollaborationDbUtils.asString(message.get("subject")),
				(String) unique.get("content"), (String) unique.get("contentType"), (String) body.get("content"),
				(String) body.get("contentType"));
	}

	private static Map<String, Object> entry(Row row, Map<String, Object> clean, Map<String, Object> sender,
			List<BrainRulesGate.Rule> rules, List<String> topicIds, String source, Map<String, Boolean> included) {
		// excluded and muted people are still read, only flagged: exclusion is about attention, not privacy
		boolean excluded = BrainRulesGate.EXCLUDED.equals(row.decision())
				|| Boolean.FALSE.equals(included.get(row.personId()))
				|| BrainRulesGate.exclusionRule(rules, topicIds, source, row.personId()) != null;
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put("id", row.graphId());
		entry.put("fromId", row.personId());
		entry.put("fromName", sender.get("name"));
		entry.put("at", row.at());
		entry.put("subject", clean.get("subject"));
		entry.put("text", clean.get("body"));
		entry.put("excluded", excluded);
		entry.put("muted", BrainRulesGate.MUTED.equals(row.decision()));
		return entry;
	}

	// every known address of each sender, lowercased, for the never-ingest check before fetch
	private static Map<String, List<String>> addresses(String ownerId, String ownerType, List<Row> rows) {
		Set<String> people = new LinkedHashSet<>();
		for (Row row : rows) {
			if (row.personId() != null) {
				people.add(row.personId());
			}
		}
		Map<String, List<String>> out = new HashMap<>();
		if (people.isEmpty()) {
			return out;
		}
		List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
		params.addAll(people);
		params.add(ownerId);
		params.add(ownerType);
		params.addAll(people);
		String in = CollaborationDbUtils.placeholders(people.size());
		CollaborationDbUtils.query("SELECT PERSON_ID, EMAIL_NORM AS ADDRESS FROM BRAIN_PERSON WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND PERSON_ID IN (" + in + ") UNION SELECT PERSON_ID, VALUE_NORM AS ADDRESS "
				+ "FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID IN (" + in + ")",
				rs -> {
					String address = BrainRulesGate.norm(rs.getString("ADDRESS"));
					if (address != null) {
						out.computeIfAbsent(rs.getString("PERSON_ID"), k -> new ArrayList<>()).add(address);
					}
					return null;
				}, params.toArray());
		return out;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> sender(Map<String, Object> message) {
		Object from = message.get("from");
		Object address = from instanceof Map<?, ?> f ? f.get("emailAddress") : null;
		return address instanceof Map<?, ?> a ? (Map<String, Object>) a : Map.of();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> content(Object value) {
		return value instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
	}
}
