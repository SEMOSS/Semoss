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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * Finds the topic's email threads whose message text holds the search words. The mailbox's own search does the
 * reading (Graph indexes bodies), and only what lands in the topic's own conversations is kept, so the cost does not
 * grow with the topic. Each match then goes through the same rules as reading a thread: a message from a never-ingest
 * sender, one the owner kept out, a person the owner excluded from the thread, or a keyword rule is dropped before
 * anything reaches the assistant. When the mailbox cannot search, the newest few messages of the topic are read
 * instead, and the answer says it covers only those.
 */
final class BrainTopicBodySearch {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicBodySearch.class);

	// Graph's own ceiling for one search is far above this; a topic rarely needs more to be found in
	static final int MAX_FOUND = 250;
	// messages read for the fallback and for snippets
	static final int MAX_SCAN = 40;
	static final int MAX_SNIPPET_MESSAGES = 15;
	private static final int SNIPPET_BEFORE = 80;
	private static final int SNIPPET_AFTER = 140;

	static final String SEARCHED = "searched";
	static final String PARTIAL = "partial";
	static final String SCANNED_RECENT = "scanned_recent";
	static final String UNAVAILABLE = "unavailable";

	private BrainTopicBodySearch() {
	}

	/** One thread whose text matched: what was found, where, and how many messages matched. */
	record Hit(String threadId, String snippet, String messageId, Timestamp at, String from, int matches) {
	}

	/** The matches by thread, and how far the search reached: searched, partial, scanned_recent or unavailable. */
	record Result(Map<String, Hit> hits, String status, String note) {
	}

	private record Row(String threadId, String graphId, String personId, String folder, Timestamp at,
			String decision) {
	}

	static Result search(User user, String ownerId, String ownerType, Collection<String> threadIds, List<String> terms,
			String from, String to, BrainThreadFinder.Privacy privacy) {
		if (threadIds.isEmpty() || terms.isEmpty()) {
			return new Result(Map.of(), SEARCHED, "");
		}
		List<Row> rows = new ArrayList<>();
		BrainThreadFinder.byIds("SELECT THREAD_ID, GRAPH_ID, SENDER_PERSON_ID, FOLDER, RECEIVED_AT, DECISION FROM "
				+ "BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IN (", ")", threadIds, rs -> {
					rows.add(new Row(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
							rs.getTimestamp(5), rs.getString(6)));
					return null;
				}, ownerId, ownerType);
		Map<String, String> byConversation = conversations(ownerId, ownerType, threadIds);
		Map<String, Row> byGraphId = new HashMap<>();
		rows.forEach(row -> {
			if (row.graphId() != null) {
				byGraphId.put(row.graphId(), row);
			}
		});
		Gate gate = Gate.load(ownerId, ownerType, threadIds, privacy);
		List<String> words = terms.size() > 6 ? terms.subList(0, 6) : terms;
		BrainMessageSource source = BrainMessageSource.current();

		List<Map<String, Object>> found;
		boolean partial = false;
		try {
			Instant[] window = window(rows, from, to);
			found = source.search(user, words, true, window[0], window[1], MAX_FOUND);
			if (found != null && found.isEmpty() && words.size() > 1) {
				found = source.search(user, words, false, window[0], window[1], MAX_FOUND);
			}
			partial = found != null && found.size() >= MAX_FOUND;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.warn("The mailbox search failed; reading the topic's newest messages instead", e);
			found = null;
		}

		List<Map<String, Object>> matched = new ArrayList<>();
		String status = partial ? PARTIAL : SEARCHED;
		String note = partial ? "The mailbox search hit its limit, so older matches may be missing. Narrow the words, "
				+ "a person, or the dates." : "";
		if (found == null) {
			status = SCANNED_RECENT;
			boolean more = rows.size() > MAX_SCAN;
			note = "The mailbox could not search message text, so only the topic's newest " + MAX_SCAN
					+ " messages were read." + (more ? " Older messages were not checked." : "");
			rows.sort((a, b) -> compare(b.at(), a.at()));
			for (Row row : rows.subList(0, Math.min(rows.size(), MAX_SCAN))) {
				if (row.graphId() != null) {
					Map<String, Object> candidate = new LinkedHashMap<>();
					candidate.put("id", row.graphId());
					matched.add(candidate);
				}
			}
		} else {
			matched.addAll(found);
		}

		// keep only the topic's own messages that the rules let through, newest first
		List<Map<String, Object>> allowed = new ArrayList<>();
		Map<String, String> threadOf = new HashMap<>();
		for (Map<String, Object> message : matched) {
			String graphId = CollaborationDbUtils.asString(message.get("id"));
			Row row = graphId == null ? null : byGraphId.get(graphId);
			String threadId = row != null ? row.threadId()
					: byConversation.get(CollaborationDbUtils.asString(message.get("conversationId")));
			if (threadId == null || !privacy.visible(threadId)
					|| !gate.allows(threadId, row, address(message), CollaborationDbUtils.asString(message.get("parentFolderId")))) {
				continue;
			}
			threadOf.put(graphId, threadId);
			allowed.add(message);
			if (allowed.size() >= MAX_SNIPPET_MESSAGES * 4) {
				break;
			}
		}
		Map<String, Hit> hits = read(user, source, allowed, threadOf, words, gate, byGraphId);
		return new Result(hits, status, note);
	}

	// each candidate is read once (a Graph $batch), checked against the keyword rules, and must really hold a word
	private static Map<String, Hit> read(User user, BrainMessageSource source, List<Map<String, Object>> allowed,
			Map<String, String> threadOf, List<String> words, Gate gate, Map<String, Row> byGraphId) {
		Map<String, Hit> hits = new LinkedHashMap<>();
		List<Map<String, Object>> toRead = allowed.size() > MAX_SNIPPET_MESSAGES * 4 ? allowed.subList(0, MAX_SNIPPET_MESSAGES * 4)
				: allowed;
		List<String> ids = toRead.stream().map(m -> CollaborationDbUtils.asString(m.get("id"))).toList();
		List<BrainMessageSource.Fetched> fetched = source.fetchAll(user, "email", null, ids);
		Map<String, Integer> counts = new HashMap<>();
		for (int i = 0; i < fetched.size(); i++) {
			Map<String, Object> message = fetched.get(i).message();
			String graphId = ids.get(i);
			String threadId = threadOf.get(graphId);
			if (message == null || threadId == null) {
				continue;
			}
			String subject = CollaborationDbUtils.asString(message.get("subject"));
			Map<String, Object> unique = content(message.get("uniqueBody"));
			Map<String, Object> body = content(message.get("body"));
			Map<String, Object> clean = BrainMessageText.extract(subject, (String) unique.get("content"),
					(String) unique.get("contentType"), (String) body.get("content"), (String) body.get("contentType"),
					false);
			String text = CollaborationDbUtils.asString(clean.get("body"));
			if (text == null || text.isBlank()) {
				text = plain((String) body.get("content"));
			}
			if (gate.keyword(subject, text)) {
				continue;
			}
			String snippet = snippet(text, words);
			if (snippet == null) {
				snippet = snippet(plain((String) body.get("content")), words);
			}
			if (snippet == null) {
				continue;
			}
			counts.merge(threadId, 1, Integer::sum);
			Row row = byGraphId.get(graphId);
			Timestamp at = row != null ? row.at() : timestamp(message.get("receivedDateTime"));
			Hit previous = hits.get(threadId);
			// the newest matching message speaks for the thread
			if (previous == null || compare(at, previous.at()) > 0) {
				hits.put(threadId, new Hit(threadId, snippet, graphId, at, sender(message), 0));
			}
		}
		Map<String, Hit> out = new LinkedHashMap<>();
		hits.forEach((id, hit) -> out.put(id,
				new Hit(id, hit.snippet(), hit.messageId(), hit.at(), hit.from(), counts.getOrDefault(id, 1))));
		return out;
	}

	// ---- the rules ----

	/** What keeps a message out of search results, decided once for the topic's threads. */
	private static final class Gate {
		private final List<BrainRulesGate.Rule> rules;
		private final BrainThreadFinder.Privacy privacy;
		private final Map<String, String> personByAddress = new HashMap<>();
		// thread|person the owner dropped from that thread
		private final Set<String> excluded = new HashSet<>();
		// the topics each thread is filed under, for the owner's topic-wide ignore rules
		private final Map<String, List<String>> topicsOf = new HashMap<>();

		private Gate(List<BrainRulesGate.Rule> rules, BrainThreadFinder.Privacy privacy) {
			this.rules = rules;
			this.privacy = privacy;
		}

		static Gate load(String ownerId, String ownerType, Collection<String> threadIds,
				BrainThreadFinder.Privacy privacy) {
			Gate gate = new Gate(BrainRulesGate.activeRules(ownerId, ownerType), privacy);
			Set<String> personIds = new HashSet<>();
			BrainThreadFinder.byIds("SELECT THREAD_ID, PERSON_ID, INCLUDED FROM BRAIN_THREAD_PARTICIPANT WHERE "
					+ "OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IN (", ")", threadIds, rs -> {
						personIds.add(rs.getString(2));
						if (Boolean.FALSE.equals(CollaborationDbUtils.getBoolean(rs, "INCLUDED"))) {
							gate.excluded.add(rs.getString(1) + "|" + rs.getString(2));
						}
						return null;
					}, ownerId, ownerType);
			BrainThreadFinder.byIds("SELECT THREAD_ID, TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND "
					+ "OWNER_TYPE = ? AND THREAD_ID IN (", ")", threadIds, rs -> {
						gate.topicsOf.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(rs.getString(2));
						return null;
					}, ownerId, ownerType);
			if (!personIds.isEmpty()) {
				BrainThreadFinder.byIds("SELECT PERSON_ID, VALUE_NORM FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND PERSON_ID IN (", ")", personIds, rs -> {
							gate.personByAddress.put(rs.getString(2), rs.getString(1));
							return null;
						}, ownerId, ownerType);
			}
			return gate;
		}

		// Reading a thread keeps a message out for four reasons, and so does this: a never-ingest standing (sender,
		// address, folder), a stored excluded or off decision, a person the owner dropped from the thread, and a
		// topic or channel ignore rule. BrainRulesGate.standing decides the rule part for the gate, the classifier
		// and memory alike. A message Brain already stored uses its own sender and folder; a newer one is judged by
		// its address
		boolean allows(String threadId, Row row, String address, String folder) {
			String norm = BrainRulesGate.norm(address);
			String person = row != null && row.personId() != null ? row.personId() : norm == null ? null : personByAddress.get(norm);
			if (row != null && (BrainRulesGate.NEVER.equals(row.decision()) || BrainRulesGate.OFF.equals(row.decision())
					|| BrainRulesGate.EXCLUDED.equals(row.decision()))) {
				return false;
			}
			if (person != null && (privacy.hiddenPerson(person) || excluded.contains(threadId + "|" + person))) {
				return false;
			}
			String where = row != null && row.folder() != null ? row.folder() : folder;
			return !BrainRulesGate.standing(rules, norm == null ? "" : norm, person, where, "email",
					topicsOf.getOrDefault(threadId, List.of()), Set.of(), false).keptOut();
		}

		boolean keyword(String subject, String text) {
			return BrainRulesGate.keywordRule(rules, subject, text) != null;
		}
	}

	// ---- helpers ----

	private static Map<String, String> conversations(String ownerId, String ownerType, Collection<String> threadIds) {
		Map<String, String> out = new HashMap<>();
		BrainThreadFinder.byIds("SELECT THREAD_ID, THREAD_KEY FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND SOURCE = 'email' AND THREAD_ID IN (", ")", threadIds, rs -> {
					key(out, rs.getString(1), rs.getString(2));
					return null;
				}, ownerId, ownerType);
		BrainThreadFinder.byIds("SELECT THREAD_ID, THREAD_KEY FROM BRAIN_THREAD_ALIAS WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND THREAD_ID IN (", ")", threadIds, rs -> {
					key(out, rs.getString(1), rs.getString(2));
					return null;
				}, ownerId, ownerType);
		return out;
	}

	private static void key(Map<String, String> out, String threadId, String key) {
		if (key != null && key.startsWith("email:")) {
			out.put(key.substring("email:".length()), threadId);
		}
	}

	// the dates of the topic's mail, narrowed by the caller's own from and to; null leaves a side open
	private static Instant[] window(List<Row> rows, String from, String to) {
		Timestamp min = null;
		Timestamp max = null;
		for (Row row : rows) {
			if (row.at() != null) {
				min = min == null || row.at().before(min) ? row.at() : min;
				max = max == null || row.at().after(max) ? row.at() : max;
			}
		}
		if (min == null || max == null) {
			return new Instant[] { null, null };
		}
		Instant start = min.toLocalDateTime().minusDays(1).toInstant(ZoneOffset.UTC);
		Instant end = max.toLocalDateTime().plusDays(1).toInstant(ZoneOffset.UTC);
		Timestamp after = CollaborationDbUtils.toTimestamp(from, "from");
		Timestamp before = CollaborationDbUtils.toTimestamp(to, "to");
		if (after != null && after.toLocalDateTime().toInstant(ZoneOffset.UTC).isAfter(start)) {
			start = after.toLocalDateTime().toInstant(ZoneOffset.UTC);
		}
		if (before != null && before.toLocalDateTime().toInstant(ZoneOffset.UTC).isBefore(end)) {
			end = before.toLocalDateTime().plusDays(1).toInstant(ZoneOffset.UTC);
		}
		return new Instant[] { start, end };
	}

	private static String address(Map<String, Object> message) {
		return message.get("from") instanceof Map<?, ?> from && from.get("emailAddress") instanceof Map<?, ?> a
				? CollaborationDbUtils.asString(a.get("address"))
				: null;
	}

	private static String sender(Map<String, Object> message) {
		if (message.get("from") instanceof Map<?, ?> from && from.get("emailAddress") instanceof Map<?, ?> a) {
			String name = CollaborationDbUtils.asString(a.get("name"));
			return name != null && !name.isBlank() ? name : CollaborationDbUtils.asString(a.get("address"));
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> content(Object value) {
		return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
	}

	private static String plain(String html) {
		return html == null ? "" : html.replaceAll("(?s)<(script|style)[^>]*>.*?</\\1>", " ").replaceAll("<[^>]*>", " ")
				.replace("&nbsp;", " ").replace("&amp;", "&").replaceAll("\\s+", " ").trim();
	}

	// a short excerpt around the first word found, or null when none is there
	static String snippet(String text, List<String> words) {
		if (text == null) {
			return null;
		}
		String flat = text.replaceAll("\\s+", " ").trim();
		String lower = flat.toLowerCase(Locale.ROOT);
		int at = -1;
		for (String word : words) {
			int index = lower.indexOf(word.toLowerCase(Locale.ROOT));
			if (index >= 0 && (at < 0 || index < at)) {
				at = index;
			}
		}
		if (at < 0) {
			return null;
		}
		int start = Math.max(0, at - SNIPPET_BEFORE);
		int end = Math.min(flat.length(), at + SNIPPET_AFTER);
		return (start > 0 ? "..." : "") + flat.substring(start, end).trim() + (end < flat.length() ? "..." : "");
	}

	private static Timestamp timestamp(Object value) {
		try {
			// stored times are UTC wall-clock, so these compare with them
			return value == null ? null
					: Timestamp.valueOf(java.time.LocalDateTime.ofInstant(Instant.parse(value.toString()), ZoneOffset.UTC));
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static int compare(Timestamp a, Timestamp b) {
		return a == null ? (b == null ? 0 : -1) : b == null ? 1 : a.compareTo(b);
	}
}
