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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// Judges the sender, not each thread, for someone the owner never writes with who alone writes on a few threads.
// With five or more verdicts on file (earlier runs, machine-sent headers, saved scores), 80% leaning automated
// types them automated. Otherwise the automated question goes to up to five threads (newest, different subjects
// first): three yes and no no, or four yes, is automated; two no keeps them a person.
final class BrainSenderVote {

	private static final Logger classLogger = LogManager.getLogger(BrainSenderVote.class);

	static final int MIN_THREADS = 3;
	private static final int MAX_VOTES = 5;
	private static final int YES_CLEAN = 3;
	private static final int YES_ANY = 4;
	private static final int NO_STOP = 2;
	// leaning automated is a yes; one thread on its own still needs the classifier cutoff
	static final double LEAN = 0.5;
	private static final double SHARE = 0.8;
	private static final String SUGGESTED = "suggested";

	private BrainSenderVote() {
	}

	@FunctionalInterface
	interface Scorer {
		// the model's automated score for a thread, or null when it could not be read
		Double automated(String threadId) throws Exception;
	}

	/** automated: every automated sender now; typed: new this run; calls: model calls the vote made. */
	record Outcome(Set<String> automated, int typed, int calls) {
	}

	static Outcome run(String ownerId, String ownerType, String selfId, Set<String> pending, Scorer scorer,
			ExecutorService pool) {
		Set<String> automated = ConcurrentHashMap.newKeySet();
		Map<String, String> states = new HashMap<>();
		CollaborationDbUtils.query("SELECT PERSON_ID, RELATIONSHIP, RELATIONSHIP_STATE FROM BRAIN_PERSON WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					if (BrainSenderTyping.AUTOMATED.equals(rs.getString(2))) {
						automated.add(rs.getString(1));
					}
					states.put(rs.getString(1), rs.getString(3) == null ? SUGGESTED : rs.getString(3));
					return null;
				}, ownerId, ownerType);

		// who wrote on each thread, and whether all of a sender's messages there say machine-sent
		Map<String, Set<String>> senders = new HashMap<>();
		Map<String, Boolean> machineSent = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SENDER_PERSON_ID, AUTO FROM BRAIN_MESSAGE WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID IS NOT NULL AND SENDER_PERSON_ID IS NOT NULL", rs -> {
					senders.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2));
					machineSent.merge(rs.getString(1) + "|" + rs.getString(2),
							Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "AUTO")), Boolean::logicalAnd);
					return null;
				}, ownerId, ownerType);
		Set<String> ownerThreads = new HashSet<>();
		senders.forEach((thread, who) -> {
			if (who.contains(selfId)) {
				ownerThreads.add(thread);
			}
		});
		// anyone on a thread the owner wrote on is someone the owner writes with
		Set<String> writesWith = new HashSet<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ?", rs -> {
					if (ownerThreads.contains(rs.getString(1))) {
						writesWith.add(rs.getString(2));
					}
					return null;
				}, ownerId, ownerType);
		Map<String, List<String>> solo = new HashMap<>();
		senders.forEach((thread, who) -> {
			if (!ownerThreads.contains(thread) && who.size() == 1) {
				solo.computeIfAbsent(who.iterator().next(), k -> new ArrayList<>()).add(thread);
			}
		});

		// per-thread evidence already on file, and the newest-first order
		Set<String> automatedThreads = new HashSet<>();
		Map<String, String> subjects = new HashMap<>();
		List<String> newestFirst = CollaborationDbUtils.query("SELECT THREAD_ID, SUBJECT, AUTOMATED FROM BRAIN_THREAD "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY LAST_MESSAGE_AT DESC", rs -> {
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "AUTOMATED"))) {
						automatedThreads.add(rs.getString(1));
					}
					String subject = CollaborationDbUtils.getString(rs, "SUBJECT");
					subjects.put(rs.getString(1), subject == null ? "" : subject.trim().toLowerCase(Locale.ROOT));
					return rs.getString(1);
				}, ownerId, ownerType);
		Map<String, Double> saved = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SIGNALS_JSON FROM WORK_ITEM WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND SIGNALS_JSON IS NOT NULL", rs -> {
					Map<String, Object> signals = CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "SIGNALS_JSON"));
					if (rs.getString(1) != null && signals != null && signals.get("automated") instanceof Number n) {
						saved.put(rs.getString(1), n.doubleValue());
					}
					return null;
				}, ownerId, ownerType);
		Map<String, Integer> rank = new HashMap<>();
		for (int i = 0; i < newestFirst.size(); i++) {
			rank.put(newestFirst.get(i), i);
		}

		AtomicInteger calls = new AtomicInteger();
		Map<String, Future<Boolean>> votes = new HashMap<>();
		for (Map.Entry<String, List<String>> e : solo.entrySet()) {
			String id = e.getKey();
			if (id.equals(selfId) || !states.containsKey(id) || automated.contains(id) || writesWith.contains(id)
					|| e.getValue().size() < MIN_THREADS || !SUGGESTED.equals(states.get(id))) {
				continue;
			}
			List<String> order = order(e.getValue(), rank, subjects);
			// what is on file already, across all their threads
			int onFile = 0;
			int onFileYes = 0;
			for (String thread : order) {
				Boolean vote = onFile(thread, id, automatedThreads, machineSent, saved);
				onFile += vote == null ? 0 : 1;
				onFileYes += Boolean.TRUE.equals(vote) ? 1 : 0;
			}
			if (onFile >= MAX_VOTES) {
				votes.put(id, CompletableFuture.completedFuture(onFileYes >= SHARE * onFile));
				continue;
			}
			votes.put(id, pool.submit(() -> {
				int yes = 0;
				int no = 0;
				for (String thread : order) {
					Boolean vote = onFile(thread, id, automatedThreads, machineSent, saved);
					if (vote == null && pending.contains(thread)) {
						calls.incrementAndGet();
						Double score = scorer.automated(thread);
						vote = score == null ? null : score >= LEAN;
					}
					if (vote != null) {
						yes += vote ? 1 : 0;
						no += vote ? 0 : 1;
					}
					if ((yes >= YES_CLEAN && no == 0) || yes >= YES_ANY || no >= NO_STOP || yes + no >= MAX_VOTES) {
						break;
					}
				}
				return (yes >= YES_CLEAN && no == 0) || yes >= YES_ANY;
			}));
		}

		List<String> typed = new ArrayList<>();
		for (Map.Entry<String, Future<Boolean>> v : votes.entrySet()) {
			try {
				if (Boolean.TRUE.equals(v.getValue().get())) {
					typed.add(v.getKey());
				}
			} catch (Exception e) {
				classLogger.warn("Sender vote failed for {}: {}", v.getKey(), e.getMessage());
			}
		}
		List<String> threads = new ArrayList<>();
		for (String id : typed) {
			threads.addAll(solo.get(id));
		}
		CollaborationDbUtils.batch(conn -> {
			for (String id : typed) {
				CollaborationDbUtils.update(conn,
						"UPDATE BRAIN_PERSON SET RELATIONSHIP = ?, RELATIONSHIP_STATE = ?, UPDATED_AT = ? WHERE "
								+ "OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
						BrainSenderTyping.AUTOMATED, SUGGESTED, CollaborationDbUtils.now(), ownerId, ownerType, id);
			}
			for (String thread : threads) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD SET AUTOMATED = ? WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND THREAD_ID = ?", true, ownerId, ownerType, thread);
			}
		});
		WorkItemUtils.dismissAutomated(ownerId, ownerType, threads);
		automated.addAll(typed);
		return new Outcome(automated, typed.size(), calls.get());
	}

	// a verdict already on file for this sender's thread, or null
	private static Boolean onFile(String thread, String sender, Set<String> automatedThreads,
			Map<String, Boolean> machineSent, Map<String, Double> saved) {
		if (automatedThreads.contains(thread) || Boolean.TRUE.equals(machineSent.get(thread + "|" + sender))) {
			return true;
		}
		return saved.containsKey(thread) ? saved.get(thread) >= LEAN : null;
	}

	// newest first, one per subject before any repeat, so five votes are not five copies of one alert
	private static List<String> order(List<String> threads, Map<String, Integer> rank, Map<String, String> subjects) {
		List<String> byTime = new ArrayList<>(threads);
		byTime.sort((a, b) -> Integer.compare(rank.getOrDefault(a, Integer.MAX_VALUE),
				rank.getOrDefault(b, Integer.MAX_VALUE)));
		List<String> first = new ArrayList<>();
		List<String> repeats = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		for (String t : byTime) {
			(seen.add(subjects.getOrDefault(t, "")) ? first : repeats).add(t);
		}
		first.addAll(repeats);
		return first;
	}
}
