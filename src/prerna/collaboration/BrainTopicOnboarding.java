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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.util.Utility;

// Adapts the completed import into header discovery and model votes; returns candidates without publishing topics.
final class BrainTopicOnboarding {

	// Retain the persisted ID namespace and recognize links created by earlier
	// onboarding runs.
	static final String VERSION = "candidate_a1";

	record Result(List<BrainTopicSuggest.Candidate> candidates, Map<String, Object> diagnostics) {
	}

	private BrainTopicOnboarding() {
	}

	static Result propose(User user, String engineId, String ownerId, String ownerType,
			Map<String, BrainTopicSuggest.Thread> eligible, String self, Map<String, String> emails, Set<String> vips,
			BrainOrgDomains.Org ownOrg, Set<String> takenNames, CollaborationJobUtils.Job job) {
		Map<String, Object> notes = new LinkedHashMap<>();
		if (job != null) {
			job.step("grouping", 10);
		}
		// stored headers after a sort: per-message To/Cc only fed the engagement gate, which WIDE does not use
		BrainTopicStructure.Prepared prepared = snapshot(ownerId, ownerType, eligible.keySet(), self, emails, vips,
				ownOrg, notes);
		if (job != null) {
			job.count("conversations", prepared.pool().size());
			job.count("groups", prepared.seeds().size());
			job.step("naming", 30);
		}
		BrainTopicVotes.Result votes = BrainTopicVotes.run(prepared, settings(ownerId, ownerType),
				caller(user, engineId));
		Map<String, Object> diagnostics = new LinkedHashMap<>();
		diagnostics.putAll(notes);
		diagnostics.put("status", votes.status());
		diagnostics.put("poolThreads", prepared.pool().size());
		diagnostics.put("gatedThreads", prepared.gated());
		diagnostics.put("seedTopics", prepared.seeds().size());
		diagnostics.put("droppedTopics", votes.dropped().size());
		diagnostics.put("modelCalls", votes.calls());
		diagnostics.put("failedVotes", votes.failedVotes());
		if ("vote_failed".equals(votes.status())) {
			diagnostics.put("modelError", "Topic voting did not produce three complete answers; retry later");
			return new Result(List.of(), diagnostics);
		}
		List<BrainTopicSuggest.Candidate> candidates = new ArrayList<>();
		Set<String> names = new HashSet<>(takenNames);
		Set<Integer> grouped = new HashSet<>();
		// Apply votes after discovery assignment so rejected groups do not spill their
		// mail into kept groups.
		for (int i = 0; i < prepared.seeds().size(); i++) {
			if (votes.dropped().contains(i)) {
				continue;
			}
			final int topic = i;
			List<BrainTopicSuggest.Thread> members = prepared.filed().entrySet().stream()
					.filter(e -> e.getValue() == topic).map(e -> {
						grouped.add(e.getKey());
						return eligible.get(prepared.pool().get(e.getKey()).id());
					}).toList();
			String name = uniqueName(votes.names().get(i), names);
			List<String> ids = members.stream().map(BrainTopicSuggest.Thread::id).sorted().toList();
			String key = VERSION + ":"
					+ CollaborationDbUtils.deterministicId(ownerId, ownerType, ids.toArray(String[]::new));
			// the card shows threads, people and your part; how it was grouped stays in
			// diagnostics
			// no name-word keywords: a rename would leave the old words behind for the
			// classifier
			candidates.add(new BrainTopicSuggest.Candidate(key, name, members, List.of(), null, votes.abouts().get(i),
					BrainTopicStructure.terms(prepared, i, 8)));
		}
		diagnostics.put("groupedThreads", grouped.size());
		diagnostics.put("unsortedThreads", prepared.pool().size() - grouped.size());
		if (job != null) {
			job.count("found", candidates.stream().map(BrainTopicSuggest.Candidate::name).limit(40).toList());
			job.step("saving", 70);
		}
		return new Result(candidates, diagnostics);
	}

	// one model call as the owner; each call gets its own insight
	static BrainTopicVotes.Caller caller(User user, String engineId) {
		IModelEngine model = Utility.getModel(engineId);
		if (model == null) {
			throw new IllegalArgumentException("The topic model could not be loaded");
		}
		return (prompt, instructions, params) -> {
			Insight insight = new Insight();
			insight.setUser(user);
			return model.ask(prompt, instructions, insight, new LinkedHashMap<>(params)).getStringResponse();
		};
	}

	// Completed-job history selects pool breadth, not an admin strategy flag. A
	// skipped sort uses the engaged pool.
	static BrainTopicStructure.Settings settings(String ownerId, String ownerType) {
		boolean sorted = CollaborationDbUtils.exists(
				"SELECT 1 FROM COLLAB_JOB c WHERE c.OWNER_ID = ? AND c.OWNER_TYPE = ? "
						+ "AND c.KIND = ? AND c.STATUS = ? AND c.STARTED_AT >= (SELECT MAX(i.STARTED_AT) FROM COLLAB_JOB i WHERE "
						+ "i.OWNER_ID = c.OWNER_ID AND i.OWNER_TYPE = c.OWNER_TYPE AND i.KIND = ? AND i.STATUS = ?)",
				ownerId, ownerType, BrainThreadClassifier.JOB_KIND, CollaborationJobUtils.DONE, BrainMailImport.KIND,
				CollaborationJobUtils.DONE);
		return sorted ? BrainTopicStructure.WIDE : BrainTopicStructure.A1;
	}

	private static String uniqueName(String proposed, Set<String> names) {
		String name = proposed;
		for (int suffix = 2; !names.add(name.toLowerCase(Locale.ROOT)); suffix++) {
			String[] words = proposed.split("\\s+");
			String stem = String.join(" ", java.util.Arrays.copyOf(words, Math.min(4, words.length)));
			String number = " " + suffix;
			name = stem.substring(0, Math.min(stem.length(), 60 - number.length())).trim() + number;
		}
		return name;
	}

	// The imported headers as stored: sender, time and bulk per message, the thread's included people as its
	// recipients. Email always; Teams chats with a real name when Teams is on (chats named only by who is in them
	// never reach here). No mailbox calls.
	private static BrainTopicStructure.Prepared snapshot(String ownerId, String ownerType, Set<String> eligible,
			String self, Map<String, String> emails, Set<String> vips, BrainOrgDomains.Org ownOrg,
			Map<String, Object> notes) {
		if (self == null || emails.get(self) == null) {
			throw new IllegalStateException("Import the mailbox before proposing topics");
		}
		Map<String, Boolean> enabled = CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType);
		if (!Boolean.TRUE.equals(enabled.get("email"))) {
			throw new IllegalStateException("Email is disabled for this owner");
		}
		if (CollaborationJobUtils.anyRunning(ownerId, ownerType, BrainTopicReviewUtils.MAP_KIND)) {
			throw new IllegalStateException("Wait for the current mailbox job to finish before proposing topics");
		}
		Map<String, Object> window = CollaborationDbUtils.queryOne(CollaborationDbUtils.page(
				"SELECT PARAMS_JSON, STARTED_AT, FINISHED_AT FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND KIND = ? AND STATUS = ? ORDER BY STARTED_AT DESC",
				1, 0), rs -> {
					Map<String, Object> row = new HashMap<>();
					row.put("params", CollaborationDbUtils.parseMap(rs.getString(1)));
					row.put("start", rs.getTimestamp(2));
					row.put("end", rs.getTimestamp(3));
					return row;
				}, ownerId, ownerType, BrainMailImport.KIND, CollaborationJobUtils.DONE);
		if (window == null || !(window.get("params") instanceof Map<?, ?> params)
				|| !(params.get("days") instanceof Number days) || days.intValue() < 1 || days.intValue() > 180
				|| !(window.get("start") instanceof Timestamp start) || !(window.get("end") instanceof Timestamp end)) {
			throw new IllegalStateException("A completed mailbox import with a known window is required");
		}
		// job times are stored as UTC wall time (CollaborationDbUtils.now), not JVM
		// local time
		Instant since = start.toLocalDateTime().toInstant(ZoneOffset.UTC).minus(Duration.ofDays(days.intValue()));
		boolean teams = Boolean.TRUE.equals(enabled.get("teams"));
		List<BrainRulesGate.Rule> rules = BrainRulesGate.activeRules(ownerId, ownerType);
		Map<String, String> sources = new HashMap<>();
		Map<String, String> subjects = new HashMap<>();
		CollaborationDbUtils.query(
				"SELECT THREAD_ID, SOURCE, SUBJECT FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND (MUTED IS NULL OR MUTED = ?)",
				rs -> {
					String thread = rs.getString(1);
					String source = rs.getString(2);
					String subject = rs.getString(3);
					if (eligible.contains(thread) && ("email".equals(source) || (teams && "teams".equals(source)))
							&& BrainRulesGate.keywordRule(rules, subject, null) == null) {
						sources.put(thread, source);
						subjects.put(thread, subject == null ? "" : subject);
					}
					return null;
				}, ownerId, ownerType, false);
		Map<String, Set<String>> included = new HashMap<>();
		CollaborationDbUtils.query(
				"SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND (INCLUDED IS NULL OR INCLUDED = ?)",
				rs -> included.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2)), ownerId,
				ownerType, true);
		Map<String, List<String>> topicIds = new HashMap<>();
		CollaborationDbUtils.query(
				"SELECT THREAD_ID, TOPIC_ID FROM BRAIN_THREAD_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> topicIds.computeIfAbsent(rs.getString(1), k -> new ArrayList<>()).add(rs.getString(2)), ownerId,
				ownerType);
		Set<String> vipEmails = new HashSet<>();
		vips.forEach(p -> {
			if (emails.get(p) != null) {
				vipEmails.add(emails.get(p));
			}
		});
		String me = emails.get(self);
		Map<String, List<BrainTopicStructure.Message>> byThread = new LinkedHashMap<>();
		Set<String> sentHistory = new HashSet<>();
		int[] read = { 0 };
		CollaborationDbUtils.query("SELECT THREAD_ID, SENDER_PERSON_ID, RECEIVED_AT, BULK, FOLDER FROM BRAIN_MESSAGE "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND DECISION = ? AND RECEIVED_AT >= ? AND RECEIVED_AT <= ? "
				+ "ORDER BY RECEIVED_AT", rs -> {
					String thread = rs.getString(1);
					String source = sources.get(thread);
					Timestamp at = rs.getTimestamp(3);
					if (source == null || at == null) {
						return null;
					}
					String folder = rs.getString(5);
					String from = permitted(rs.getString(2), thread, folder, source, emails, included, topicIds, rules);
					if (from == null) {
						return null;
					}
					List<String> to = new ArrayList<>();
					for (String person : included.getOrDefault(thread, Set.of())) {
						String address = person.equals(rs.getString(2)) ? null
								: permitted(person, thread, folder, source, emails, included, topicIds, rules);
						if (address != null) {
							to.add(address);
						}
					}
					if (me.equals(from)) {
						sentHistory.addAll(to);
					}
					read[0]++;
					byThread.computeIfAbsent(thread, k -> new ArrayList<>())
							.add(new BrainTopicStructure.Message(subjects.get(thread),
									at.toLocalDateTime().toInstant(ZoneOffset.UTC).toString(), from, to, List.of(),
									Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "BULK"))));
					return null;
				}, ownerId, ownerType, BrainRulesGate.INGESTED,
				Timestamp.valueOf(LocalDateTime.ofInstant(since, ZoneOffset.UTC)), end);
		notes.put("messages", read[0]);
		notes.put("chats", byThread.keySet().stream().filter(t -> "teams".equals(sources.get(t))).count());
		List<BrainTopicStructure.MailThread> input = byThread.entrySet().stream()
				.map(e -> new BrainTopicStructure.MailThread(e.getKey(), e.getValue())).toList();
		BrainTopicStructure.Settings settings = settings(ownerId, ownerType);
		notes.put("wide", settings.wide());
		return BrainTopicStructure.prepare(input, me, ownOrg::isMine, vipEmails, settings, sentHistory);
	}

	// the person's address when they are an included participant no keep-out rule covers here
	private static String permitted(String person, String thread, String folder, String source,
			Map<String, String> emails, Map<String, Set<String>> included, Map<String, List<String>> topicIds,
			List<BrainRulesGate.Rule> rules) {
		String address = person == null ? null : emails.get(person);
		if (address == null || !included.getOrDefault(thread, Set.of()).contains(person)
				|| BrainRulesGate.neverRule(rules, address, person, folder) != null
				|| BrainRulesGate.exclusionRule(rules, topicIds.getOrDefault(thread, List.of()), source,
						person) != null) {
			return null;
		}
		return address;
	}
}
