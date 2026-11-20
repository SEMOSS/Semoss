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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;

// Finds threads for the owner from what Brain stores (subject, summary, people, topics, dates), never from message
// bodies: the threads related to one thread, and a search inside one topic. Both go through the same privacy
// check: muted and automated threads, never-ingest people, and threads whose every message was kept out are left out.
public final class BrainThreadFinder {

	private static final int DEFAULT_LIMIT = 10;
	private static final int MAX_LIMIT = 25;
	// threads looked at before ranking; a topic or a person with more than this is cut off by recency of link
	private static final int CANDIDATE_CAP = 1500;
	private static final int CHUNK = 400;
	private static final int SNIPPET_BEFORE = 60;
	private static final int SNIPPET_AFTER = 100;
	private static final int PEOPLE_SHOWN = 5;
	private static final Set<String> STOP_WORDS = Set.of("the", "and", "for", "with", "from", "about", "that", "this",
			"have", "has", "was", "were", "are", "you", "your", "any", "all", "email", "emails", "mail", "thread",
			"threads", "message", "messages", "find", "search", "show", "get", "into", "out");

	private BrainThreadFinder() {
	}

	// ---- related to one thread ----

	public static Map<String, Object> related(User user, String threadId, Integer limit) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		BrainThreadUtils.requireThread(ownerId, ownerType, threadId);
		int max = clamp(limit);
		String selfId = selfPersonId(ownerId, ownerType);

		// the thread's own topics (primary flagged) and the people on it
		Map<String, Boolean> myTopics = new LinkedHashMap<>();
		// a link still waiting for the owner is a guess, so it relates nothing
		CollaborationDbUtils.query("SELECT TOPIC_ID, IS_PRIMARY FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID = ? AND (SOURCE IS NULL OR SOURCE <> 'suggested')", rs -> {
					myTopics.put(rs.getString(1), Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_PRIMARY")));
					return null;
				}, ownerId, ownerType, threadId);
		Privacy mine = Privacy.load(ownerId, ownerType, List.of(threadId));
		Set<String> myPeople = new LinkedHashSet<>(mine.shownPeople(threadId, selfId));

		// candidates: threads that share a topic, a person, or a (non-internal) account
		Map<String, Set<String>> viaTopic = new LinkedHashMap<>();
		Map<String, Set<String>> viaPeople = new LinkedHashMap<>();
		Map<String, Set<String>> viaAccount = new LinkedHashMap<>();
		if (!myTopics.isEmpty()) {
			byIds("SELECT THREAD_ID, TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID <> ? AND (SOURCE IS NULL OR SOURCE <> 'suggested') AND TOPIC_ID IN (", ")",
					myTopics.keySet(), rs -> {
						viaTopic.computeIfAbsent(rs.getString(1), k -> new LinkedHashSet<>()).add(rs.getString(2));
						return null;
					}, ownerId, ownerType, threadId);
		}
		if (!myPeople.isEmpty()) {
			byIds("SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND THREAD_ID <> ? AND (INCLUDED IS NULL OR INCLUDED = true) AND PERSON_ID IN (", ")", myPeople,
					rs -> {
						viaPeople.computeIfAbsent(rs.getString(1), k -> new LinkedHashSet<>()).add(rs.getString(2));
						return null;
					}, ownerId, ownerType, threadId);
		}
		Map<String, String> accountNames = new HashMap<>();
		Map<String, String> accountOfPerson = new HashMap<>();
		if (!myPeople.isEmpty()) {
			byIds("SELECT p.PERSON_ID, p.ACCOUNT_ID, a.NAME FROM BRAIN_PERSON p JOIN BRAIN_ACCOUNT a ON "
					+ "a.OWNER_ID = p.OWNER_ID AND a.OWNER_TYPE = p.OWNER_TYPE AND a.ACCOUNT_ID = p.ACCOUNT_ID "
					+ "WHERE p.OWNER_ID = ? AND p.OWNER_TYPE = ? AND a.KIND <> 'internal' AND p.PERSON_ID IN (", ")",
					myPeople, rs -> {
						accountOfPerson.put(rs.getString(1), rs.getString(2));
						accountNames.put(rs.getString(2), rs.getString(3));
						return null;
					}, ownerId, ownerType);
		}
		if (!accountNames.isEmpty()) {
			Map<String, String> accountByMember = new HashMap<>();
			byIds("SELECT PERSON_ID, ACCOUNT_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND ACCOUNT_ID IN (", ")", accountNames.keySet(), rs -> {
						accountByMember.put(rs.getString(1), rs.getString(2));
						return null;
					}, ownerId, ownerType);
			if (!accountByMember.isEmpty()) {
				byIds("SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND THREAD_ID <> ? AND (INCLUDED IS NULL OR INCLUDED = true) AND PERSON_ID IN (", ")",
						accountByMember.keySet(), rs -> {
							viaAccount.computeIfAbsent(rs.getString(1), k -> new LinkedHashSet<>())
									.add(accountByMember.get(rs.getString(2)));
							return null;
						}, ownerId, ownerType, threadId);
			}
		}
		Set<String> candidateIds = new LinkedHashSet<>();
		candidateIds.addAll(viaTopic.keySet());
		candidateIds.addAll(viaPeople.keySet());
		candidateIds.addAll(viaAccount.keySet());
		candidateIds = cap(candidateIds);

		List<ThreadRow> rows = loadThreads(ownerId, ownerType, candidateIds);
		Privacy privacy = Privacy.load(ownerId, ownerType, ids(rows));
		Map<String, String> topicNames = topicNames(ownerId, ownerType, myTopics.keySet());

		List<Map<String, Object>> scored = new ArrayList<>();
		Instant now = Instant.now();
		for (ThreadRow row : rows) {
			if (!privacy.visible(row.id())) {
				continue;
			}
			double score = 0;
			List<String> reasons = new ArrayList<>();
			for (String topic : viaTopic.getOrDefault(row.id(), Set.of())) {
				score += Boolean.TRUE.equals(myTopics.get(topic)) ? 5 : 3;
				reasons.add("Same topic: " + topicNames.getOrDefault(topic, "topic"));
			}
			List<String> sharedNames = new ArrayList<>();
			for (String person : viaPeople.getOrDefault(row.id(), Set.of())) {
				if (privacy.hiddenPerson(person)) {
					continue;
				}
				sharedNames.add(privacy.label(person));
			}
			if (!sharedNames.isEmpty()) {
				score += Math.min(6, 2 * sharedNames.size());
				reasons.add("Same people: " + String.join(", ", sharedNames.subList(0, Math.min(3, sharedNames.size()))));
			}
			Set<String> accounts = new LinkedHashSet<>();
			for (String account : viaAccount.getOrDefault(row.id(), Set.of())) {
				if (account != null && accountNames.containsKey(account)) {
					accounts.add(accountNames.get(account));
				}
			}
			for (String account : accounts) {
				score += 1;
				reasons.add("Same account: " + account);
			}
			// a reason can vanish after the privacy check (a hidden person was the only shared one)
			if (reasons.isEmpty()) {
				continue;
			}
			if (row.lastAt() != null) {
				// stored as UTC wall time
				long days = Duration.between(row.lastAt().toLocalDateTime().toInstant(ZoneOffset.UTC), now).toDays();
				score += days <= 7 ? 1 : days <= 30 ? 0.5 : 0;
			}
			Map<String, Object> item = item(row, privacy, selfId);
			item.put("reasons", reasons);
			item.put("score", Math.round(score * 10) / 10.0);
			scored.add(item);
		}
		scored.sort(Comparator.<Map<String, Object>>comparingDouble(i -> -((Number) i.get("score")).doubleValue())
				.thenComparing(i -> String.valueOf(i.get("lastAt")), Comparator.reverseOrder()));

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("threadId", threadId);
		out.put("total", scored.size());
		out.put("items", scored.size() > max ? new ArrayList<>(scored.subList(0, max)) : scored);
		// files attached to the thread's topics arrive with the topic documents work
		out.put("documents", List.of());
		return out;
	}

	// ---- search inside one topic ----

	public static Map<String, Object> searchTopic(User user, String topicId, String topicName, String query,
			String person, String from, String to, Integer limit) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		return search(ownerId, ownerType, resolveTopic(ownerId, ownerType, topicId, topicName), query, person, from, to,
				limit);
	}

	// the same search across all of the owner's threads, newest first up to the candidate cap
	public static Map<String, Object> searchAll(User user, String query, String person, String from, String to,
			Integer limit) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		return search(owner.getValue0(), owner.getValue1(), null, query, person, from, to, limit);
	}

	// topic null means every thread
	private static Map<String, Object> search(String ownerId, String ownerType, String topic, String query,
			String person, String from, String to, Integer limit) {
		boolean noPerson = person == null || person.isBlank();
		Timestamp after = CollaborationDbUtils.toTimestamp(from, "from");
		Timestamp before = CollaborationDbUtils.toTimestamp(to, "to");
		// a plain end date means the whole day
		if (before != null && to.trim().length() == 10) {
			before = Timestamp.valueOf(before.toLocalDateTime().plusDays(1));
		}
		int max = clamp(limit);
		String selfId = selfPersonId(ownerId, ownerType);

		// every thread linked to the topic (a link still waiting for the owner counts, and says so)
		Map<String, String> linkSource = new LinkedHashMap<>();
		Collection<String> candidates;
		if (topic != null) {
			CollaborationDbUtils.query("SELECT THREAD_ID, SOURCE FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND TOPIC_ID = ?", rs -> {
						linkSource.put(rs.getString(1), rs.getString(2));
						return null;
					}, ownerId, ownerType, topic);
			candidates = cap(linkSource.keySet());
		} else {
			candidates = CollaborationDbUtils.query(CollaborationDbUtils.page("SELECT THREAD_ID FROM BRAIN_THREAD WHERE "
					+ "OWNER_ID = ? AND OWNER_TYPE = ? AND (MUTED IS NULL OR MUTED = ?) AND (AUTOMATED IS NULL OR "
					+ "AUTOMATED = ?) ORDER BY COALESCE(LAST_MESSAGE_AT, CREATED_AT) DESC, THREAD_ID", CANDIDATE_CAP, 0),
					rs -> rs.getString(1), ownerId, ownerType, false, false);
		}
		List<ThreadRow> rows = loadThreads(ownerId, ownerType, candidates);
		Privacy privacy = Privacy.load(ownerId, ownerType, ids(rows));

		List<String> tokens = tokens(query);
		String personNeedle = noPerson ? null : person.trim().toLowerCase(Locale.ROOT);
		List<Map<String, Object>> hits = new ArrayList<>();
		for (ThreadRow row : rows) {
			if (!privacy.visible(row.id())) {
				continue;
			}
			if ((after != null && (row.lastAt() == null || row.lastAt().before(after)))
					|| (before != null && (row.lastAt() == null || !row.lastAt().before(before)))) {
				continue;
			}
			String people = privacy.peopleText(row.id(), selfId);
			if (personNeedle != null && !people.contains(personNeedle)) {
				continue;
			}
			String subject = lower(row.subject());
			String summary = lower(row.summary());
			List<String> matched = new ArrayList<>();
			double score = 0;
			if (!tokens.isEmpty()) {
				int inSubject = hits(subject, tokens);
				int inSummary = hits(summary, tokens);
				int inPeople = hits(people, tokens);
				if (inSubject + inSummary + inPeople == 0) {
					continue;
				}
				score = 3 * inSubject + inSummary + 2 * inPeople;
				if (inSubject > 0) {
					matched.add("subject");
				}
				if (inSummary > 0) {
					matched.add("summary");
				}
				if (inPeople > 0) {
					matched.add("people");
				}
			}
			Map<String, Object> item = item(row, privacy, selfId);
			item.put("score", score);
			if (!matched.isEmpty()) {
				item.put("matchedOn", matched);
			}
			String snippet = snippet(row.summary(), summary, tokens);
			if (snippet != null) {
				item.put("snippet", snippet);
			}
			if ("suggested".equals(linkSource.get(row.id()))) {
				item.put("topicLink", "suggested");
			}
			hits.add(item);
		}
		hits.sort(Comparator.<Map<String, Object>>comparingDouble(i -> -((Number) i.get("score")).doubleValue())
				.thenComparing(i -> String.valueOf(i.get("lastAt")), Comparator.reverseOrder()));

		List<Map<String, Object>> page = hits.size() > max ? new ArrayList<>(hits.subList(0, max)) : hits;
		addTopicNames(ownerId, ownerType, page);
		Map<String, Object> out = new LinkedHashMap<>();
		if (topic != null) {
			out.put("topicId", topic);
		}
		out.put("total", hits.size());
		out.put("items", page);
		// so the model knows what a miss means: bodies are not searched
		out.put("searchedOn", List.of("subject", "summary", "people", "dates"));
		out.put("note", "Matches the thread subject, summary, and people only, not message text. "
				+ "Read a thread to see its messages.");
		return out;
	}

	// ---- shared pieces ----

	private record ThreadRow(String id, String source, String subject, String summary, Timestamp lastAt,
			Integer messageCount) {
	}

	private static int clamp(Integer limit) {
		return limit == null ? DEFAULT_LIMIT : Math.max(1, Math.min(MAX_LIMIT, limit));
	}

	private static Set<String> cap(Collection<String> ids) {
		Set<String> out = new LinkedHashSet<>();
		for (String id : ids) {
			if (out.size() >= CANDIDATE_CAP) {
				break;
			}
			out.add(id);
		}
		return out;
	}

	private static List<String> ids(List<ThreadRow> rows) {
		List<String> ids = new ArrayList<>();
		for (ThreadRow row : rows) {
			ids.add(row.id());
		}
		return ids;
	}

	private static String selfPersonId(String ownerId, String ownerType) {
		return CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType, "self");
	}

	// a topic by id, or by name when no id is given; the name must pick exactly one
	public static String resolveTopic(String ownerId, String ownerType, String topicId, String topicName) {
		if (topicId != null && !topicId.isBlank()) {
			BrainTopicUtils.requireTopic(ownerId, ownerType, topicId);
			return topicId;
		}
		if (topicName == null || topicName.isBlank()) {
			throw new IllegalArgumentException("Pass a topicId or a topic name");
		}
		String needle = topicName.trim().toLowerCase(Locale.ROOT);
		List<String[]> topics = CollaborationDbUtils.query("SELECT TOPIC_ID, NAME, SHORT_NAME FROM BRAIN_TOPIC WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ?", rs -> new String[] { rs.getString(1),
						CollaborationDbUtils.getString(rs, "NAME"), CollaborationDbUtils.getString(rs, "SHORT_NAME") },
				ownerId, ownerType);
		List<String[]> exact = new ArrayList<>();
		List<String[]> partial = new ArrayList<>();
		for (String[] t : topics) {
			String name = lower(t[1]);
			String shortName = lower(t[2]);
			if (name.equals(needle) || shortName.equals(needle)) {
				exact.add(t);
			} else if (name.contains(needle)) {
				partial.add(t);
			}
		}
		List<String[]> found = exact.isEmpty() ? partial : exact;
		if (found.isEmpty()) {
			throw new IllegalArgumentException("No topic is named " + topicName);
		}
		if (found.size() > 1) {
			List<String> names = new ArrayList<>();
			for (String[] t : found) {
				names.add(t[1] + " (" + t[0] + ")");
			}
			throw new IllegalArgumentException("More than one topic matches " + topicName + ": " + String.join("; ", names)
					+ ". Pass the topicId.");
		}
		return found.get(0)[0];
	}

	// the names of the topics each result belongs to (links the owner has not confirmed are left out)
	private static void addTopicNames(String ownerId, String ownerType, List<Map<String, Object>> items) {
		if (items.isEmpty()) {
			return;
		}
		List<String> threadIds = new ArrayList<>();
		for (Map<String, Object> item : items) {
			threadIds.add((String) item.get("threadId"));
		}
		Map<String, List<String>> topicsOf = new HashMap<>();
		Set<String> topicIds = new LinkedHashSet<>();
		byIds("SELECT THREAD_ID, TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND (SOURCE IS NULL OR SOURCE <> 'suggested') AND THREAD_ID IN (", ")", threadIds, rs -> {
					topicsOf.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(rs.getString(2));
					topicIds.add(rs.getString(2));
					return null;
				}, ownerId, ownerType);
		Map<String, String> names = topicNames(ownerId, ownerType, topicIds);
		for (Map<String, Object> item : items) {
			List<String> found = new ArrayList<>();
			for (String id : topicsOf.getOrDefault((String) item.get("threadId"), List.of())) {
				if (names.get(id) != null) {
					found.add(names.get(id));
				}
			}
			item.put("topics", found);
		}
	}

	private static Map<String, String> topicNames(String ownerId, String ownerType, Collection<String> topicIds) {
		Map<String, String> names = new HashMap<>();
		if (topicIds.isEmpty()) {
			return names;
		}
		byIds("SELECT TOPIC_ID, NAME FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID IN (", ")",
				topicIds, rs -> {
					names.put(rs.getString(1), CollaborationDbUtils.getString(rs, "NAME"));
					return null;
				}, ownerId, ownerType);
		return names;
	}

	// threads that are not muted and not marked automated
	private static List<ThreadRow> loadThreads(String ownerId, String ownerType, Collection<String> ids) {
		List<ThreadRow> rows = new ArrayList<>();
		if (ids.isEmpty()) {
			return rows;
		}
		byIds("SELECT THREAD_ID, SOURCE, SUBJECT, SUMMARY, LAST_MESSAGE_AT, MESSAGE_COUNT FROM BRAIN_THREAD WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ? AND (MUTED IS NULL OR MUTED = ?) AND (AUTOMATED IS NULL OR AUTOMATED = ?) "
				+ "AND THREAD_ID IN (", ")", ids, rs -> {
					rows.add(new ThreadRow(rs.getString("THREAD_ID"), rs.getString("SOURCE"),
							CollaborationDbUtils.getString(rs, "SUBJECT"), CollaborationDbUtils.getString(rs, "SUMMARY"),
							rs.getTimestamp("LAST_MESSAGE_AT"), CollaborationDbUtils.getInteger(rs, "MESSAGE_COUNT")));
					return null;
				}, ownerId, ownerType, false, false);
		return rows;
	}

	// runs prefix + ?,?,... + suffix once per chunk of ids; the fixed params come before the ids, the ids last
	private static void byIds(String prefix, String suffix, Collection<String> ids, CollaborationDbUtils.RowMapper<Void> mapper,
			Object... fixed) {
		// the muted and automated flags in loadThreads are fixed params that follow the owner, so they are
		// passed in the caller's order; the id list is always the last placeholder group
		List<String> all = new ArrayList<>(ids);
		for (int i = 0; i < all.size(); i += CHUNK) {
			List<String> part = all.subList(i, Math.min(all.size(), i + CHUNK));
			List<Object> params = new ArrayList<>(List.of(fixed));
			params.addAll(part);
			CollaborationDbUtils.query(prefix + CollaborationDbUtils.placeholders(part.size()) + suffix, mapper,
					params.toArray());
		}
	}

	private static Map<String, Object> item(ThreadRow row, Privacy privacy, String selfId) {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("threadId", row.id());
		item.put("type", "teams".equals(row.source()) ? "chat" : "calendar".equals(row.source()) ? "meeting" : "email");
		item.put("subject", row.subject() == null ? "(no subject)" : row.subject());
		item.put("lastAt", CollaborationDbUtils.toIso(row.lastAt()));
		item.put("messageCount", row.messageCount() == null ? 0 : row.messageCount());
		List<String> names = new ArrayList<>();
		for (String person : privacy.shownPeople(row.id(), selfId)) {
			names.add(privacy.label(person));
			if (names.size() >= PEOPLE_SHOWN) {
				break;
			}
		}
		item.put("people", names);
		return item;
	}

	// ---- text matching ----

	private static String lower(String text) {
		return text == null ? "" : text.toLowerCase(Locale.ROOT);
	}

	// lower-case words of two letters or more, without filler words, each once
	static List<String> tokens(String query) {
		Set<String> out = new LinkedHashSet<>();
		if (query != null) {
			for (String word : query.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}]+")) {
				if (word.length() >= 2 && !STOP_WORDS.contains(word)) {
					out.add(word);
				}
			}
		}
		return new ArrayList<>(out);
	}

	// how many different words appear in the text
	private static int hits(String text, List<String> tokens) {
		int count = 0;
		for (String token : tokens) {
			if (text.contains(token)) {
				count++;
			}
		}
		return count;
	}

	// a short stretch of the summary around the first word that matches
	private static String snippet(String summary, String lowerSummary, List<String> tokens) {
		if (summary == null || summary.isBlank() || tokens.isEmpty()) {
			return null;
		}
		int at = -1;
		for (String token : tokens) {
			int i = lowerSummary.indexOf(token);
			if (i >= 0 && (at < 0 || i < at)) {
				at = i;
			}
		}
		if (at < 0) {
			return null;
		}
		int start = Math.max(0, at - SNIPPET_BEFORE);
		int end = Math.min(summary.length(), at + SNIPPET_AFTER);
		String text = summary.substring(start, end).replaceAll("\\s+", " ").trim();
		return (start > 0 ? "..." : "") + text + (end < summary.length() ? "..." : "");
	}

	// ---- privacy ----

	// What may be shown for a set of threads: who is on them and whether they are visible at all. A person is hidden
	// when flagged never-ingest or covered by a never-ingest rule today; a thread is visible when at least one of its
	// messages was not kept out, or (with no messages stored, such as a meeting) at least one person on it is not hidden.
	private static final class Privacy {
		private final Map<String, List<String>> participants = new HashMap<>();
		private final Map<String, List<String[]>> messages = new HashMap<>();
		private final Map<String, String[]> people = new HashMap<>();
		private final Set<String> hidden = new HashSet<>();
		private final Set<String> automated = new HashSet<>();
		private final List<BrainRulesGate.Rule> rules;

		private Privacy(List<BrainRulesGate.Rule> rules) {
			this.rules = rules;
		}

		static Privacy load(String ownerId, String ownerType, Collection<String> threadIds) {
			Privacy p = new Privacy(BrainRulesGate.activeRules(ownerId, ownerType));
			if (threadIds.isEmpty()) {
				return p;
			}
			Set<String> personIds = new HashSet<>();
			// the people on a thread, left out when the owner dropped them from it
			byIds("SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND (INCLUDED IS NULL OR INCLUDED = true) AND THREAD_ID IN (", ")", threadIds, rs -> {
						p.participants.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(rs.getString(2));
						personIds.add(rs.getString(2));
						return null;
					}, ownerId, ownerType);
			byIds("SELECT THREAD_ID, SENDER_PERSON_ID, FOLDER, DECISION FROM BRAIN_MESSAGE WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND THREAD_ID IN (", ")", threadIds, rs -> {
						p.messages.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(new String[] {
								rs.getString(2), rs.getString(3), rs.getString(4) });
						if (rs.getString(2) != null) {
							personIds.add(rs.getString(2));
						}
						return null;
					}, ownerId, ownerType);
			if (personIds.isEmpty()) {
				return p;
			}
			Map<String, List<String>> addresses = new HashMap<>();
			byIds("SELECT PERSON_ID, VALUE_NORM FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND PERSON_ID IN (", ")", personIds, rs -> {
						addresses.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(rs.getString(2));
						return null;
					}, ownerId, ownerType);
			byIds("SELECT PERSON_ID, DISPLAY_NAME, EMAIL_NORM, NEVER_INGEST, RELATIONSHIP FROM BRAIN_PERSON WHERE "
					+ "OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID IN (", ")", personIds, rs -> {
						String id = rs.getString(1);
						p.people.put(id, new String[] { CollaborationDbUtils.getString(rs, "DISPLAY_NAME"),
								CollaborationDbUtils.getString(rs, "EMAIL_NORM") });
						List<String> known = addresses.getOrDefault(id, List.of());
						boolean never = Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "NEVER_INGEST"));
						for (String address : known.isEmpty() ? List.of("") : known) {
							never |= BrainRulesGate.neverRule(p.rules, address, id, null) != null;
						}
						if (never) {
							p.hidden.add(id);
						}
						if (BrainSenderTyping.AUTOMATED.equals(rs.getString("RELATIONSHIP"))) {
							p.automated.add(id);
						}
						return null;
					}, ownerId, ownerType);
			return p;
		}

		boolean hiddenPerson(String personId) {
			return hidden.contains(personId);
		}

		boolean visible(String threadId) {
			List<String[]> sent = messages.get(threadId);
			if (sent != null && !sent.isEmpty()) {
				for (String[] m : sent) {
					boolean kept = BrainRulesGate.NEVER.equals(m[2]) || BrainRulesGate.OFF.equals(m[2]);
					if (kept || (m[0] != null && hidden.contains(m[0]))
							|| BrainRulesGate.neverRule(rules, "", m[0], m[1]) != null) {
						continue;
					}
					return true;
				}
				return false;
			}
			for (String person : participants.getOrDefault(threadId, List.of())) {
				if (!hidden.contains(person)) {
					return true;
				}
			}
			return false;
		}

		// the people shown for a thread: not the owner, not hidden, not an automated sender
		List<String> shownPeople(String threadId, String selfId) {
			List<String> out = new ArrayList<>();
			for (String person : participants.getOrDefault(threadId, List.of())) {
				if (!person.equals(selfId) && !hidden.contains(person) && !automated.contains(person)
						&& !out.contains(person)) {
					out.add(person);
				}
			}
			return out;
		}

		String label(String personId) {
			String[] p = people.get(personId);
			if (p == null) {
				return "someone";
			}
			return p[0] != null && !p[0].isBlank() ? p[0] : p[1] != null ? p[1] : "someone";
		}

		// names and addresses of the shown people, lower case, for matching
		String peopleText(String threadId, String selfId) {
			StringBuilder text = new StringBuilder();
			for (String person : shownPeople(threadId, selfId)) {
				String[] p = people.get(person);
				if (p != null) {
					text.append(' ').append(lower(p[0])).append(' ').append(lower(p[1]));
				}
			}
			return text.toString();
		}
	}
}
