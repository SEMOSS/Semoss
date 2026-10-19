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
import java.util.LinkedHashSet;
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
		BrainTopicVotes.Caller caller = caller(user, engineId);
		BrainTopicStructure.Prepared prepared = snapshot(ownerId, ownerType, eligible.keySet(), self, emails, vips,
				ownOrg, notes, caller);
		if (job != null) {
			job.count("conversations", prepared.pool().size());
			job.count("groups", prepared.seeds().size());
			job.step("naming", 30);
		}
		BrainTopicVotes.Result votes = BrainTopicVotes.run(prepared, settings(ownerId, ownerType), caller);
		Map<String, Object> diagnostics = new LinkedHashMap<>();
		diagnostics.putAll(notes);
		diagnostics.put("status", votes.status());
		diagnostics.put("poolThreads", prepared.pool().size());
		diagnostics.put("gatedThreads", prepared.gated());
		diagnostics.put("seedTopics", prepared.seeds().size());
		diagnostics.put("folderTopics", prepared.folders().stream().filter(java.util.Objects::nonNull).count());
		diagnostics.put("droppedTopics", votes.dropped().size());
		diagnostics.put("modelCalls", votes.calls());
		diagnostics.put("failedVotes", votes.failedVotes());
		if ("vote_failed".equals(votes.status())) {
			diagnostics.put("modelError", "Topic voting did not produce three complete answers; retry later");
			return new Result(List.of(), diagnostics);
		}
		List<BrainTopicSuggest.Candidate> candidates = new ArrayList<>();
		BrainMailFolders.Tree folderTree = BrainMailFolders.load(ownerId, ownerType);
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
			// filing from the folder is offered only for a project folder; any other folder is a naming hint
			String folder = prepared.folders().get(i);
			String path = folder == null ? null : (String) prepared.cards().get(i).get("outlookFolder");
			candidates.add(new BrainTopicSuggest.Candidate(key, name, members, List.of(), null, votes.abouts().get(i),
					BrainTopicStructure.terms(prepared, i, 8),
					BrainFolderRoles.PROJECT.equals(folderTree.role(folder)) ? folder : null, path));
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
			Map<String, Object> notes, BrainTopicVotes.Caller caller) {
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
		// the owner's own folders by id, with their paths; system folders say nothing about the work
		BrainMailFolders.Tree tree = BrainMailFolders.load(ownerId, ownerType);
		Map<String, String> folderPaths = new HashMap<>();
		Map<String, Set<String>> folderThreads = new HashMap<>();
		Map<String, Set<String>> folderWrote = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SENDER_PERSON_ID, RECEIVED_AT, BULK, FOLDER, CATEGORIES_JSON FROM BRAIN_MESSAGE "
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
					String path = tree.custom(folder);
					if (path != null) {
						folderPaths.put(folder, path);
						folderThreads.computeIfAbsent(folder, k -> new LinkedHashSet<>()).add(thread);
						if (me.equals(from)) {
							folderWrote.computeIfAbsent(folder, k -> new HashSet<>()).add(thread);
						}
					}
					byThread.computeIfAbsent(thread, k -> new ArrayList<>())
							.add(new BrainTopicStructure.Message(subjects.get(thread),
									at.toLocalDateTime().toInstant(ZoneOffset.UTC).toString(), from, to, List.of(),
									Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "BULK")),
									path == null ? null : folder, CollaborationDbUtils.toStringList(CollaborationDbUtils
											.parseList(CollaborationDbUtils.getString(rs, "CATEGORIES_JSON")))));
					return null;
				}, ownerId, ownerType, BrainRulesGate.INGESTED,
				Timestamp.valueOf(LocalDateTime.ofInstant(since, ZoneOffset.UTC)), end);
		notes.put("messages", read[0]);
		notes.put("chats", byThread.keySet().stream().filter(t -> "teams".equals(sources.get(t))).count());
		// what each folder the owner made is for; only project, area and other folders steer grouping, and mail
		// in a folder kept for automated mail counts as bulk. No folders: the plain discovery, no model call
		Map<String, String> roles = folderRoles(caller, tree, folderThreads, folderWrote, byThread, subjects, me);
		BrainFolderRoles.save(ownerId, ownerType, roles);
		Map<String, String> guides = new HashMap<>();
		folderPaths.forEach((id, path) -> {
			if (BrainFolderRoles.guides(roles.getOrDefault(id, BrainFolderRoles.OTHER))) {
				guides.put(id, path);
			}
		});
		List<BrainTopicStructure.MailThread> input = byThread.entrySet().stream()
				.map(e -> new BrainTopicStructure.MailThread(e.getKey(), e.getValue().stream()
						.map(m -> new BrainTopicStructure.Message(m.subject(), m.at(), m.sender(), m.to(), m.cc(),
								m.bulk() || BrainFolderRoles.AUTOMATED.equals(roles.get(m.folder())),
								guides.containsKey(m.folder()) ? m.folder() : null, m.categories()))
						.toList()))
				.toList();
		BrainTopicStructure.Settings settings = settings(ownerId, ownerType);
		notes.put("wide", settings.wide());
		notes.put("folders", folderPaths.size());
		notes.put("guideFolders", guides.size());
		notes.put("folderRoles", roles.values().stream()
				.collect(java.util.stream.Collectors.groupingBy(r -> r, LinkedHashMap::new,
						java.util.stream.Collectors.counting())));
		return BrainTopicStructure.prepare(input, me, ownOrg::isMine, vipEmails, settings, sentHistory, guides);
	}

	// one card per folder with two or more conversations in the window, judged in one model call
	private static Map<String, String> folderRoles(BrainTopicVotes.Caller caller, BrainMailFolders.Tree tree,
			Map<String, Set<String>> folderThreads, Map<String, Set<String>> folderWrote,
			Map<String, List<BrainTopicStructure.Message>> byThread, Map<String, String> subjects, String me) {
		List<Map<String, Object>> cards = new ArrayList<>();
		Map<String, String> byCard = new HashMap<>();
		folderThreads.entrySet().stream().filter(e -> e.getValue().size() >= 2)
				.sorted(java.util.Comparator.<Map.Entry<String, Set<String>>>comparingInt(e -> e.getValue().size())
						.reversed())
				.forEach(e -> {
					String folder = e.getKey();
					Map<String, Integer> people = new HashMap<>();
					int bulk = 0;
					for (String thread : e.getValue()) {
						List<BrainTopicStructure.Message> messages = byThread.getOrDefault(thread, List.of());
						bulk += messages.stream().anyMatch(BrainTopicStructure.Message::bulk) ? 1 : 0;
						Set<String> seen = new HashSet<>();
						messages.forEach(m -> seen.add(m.sender()));
						seen.remove(me);
						seen.forEach(p -> people.merge(p, 1, Integer::sum));
					}
					int children = (int) tree.byId().values().stream().filter(f -> folder.equals(f.parentId()))
							.count();
					String id = "f" + (cards.size() + 1);
					byCard.put(id, folder);
					cards.add(BrainFolderRoles.card(id, tree.path(folder), children, e.getValue().size(),
							folderWrote.getOrDefault(folder, Set.of()).size(), bulk,
							people.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
									.limit(3).map(Map.Entry::getKey).toList(),
							e.getValue().stream().map(subjects::get).filter(x -> x != null && !x.isBlank()).distinct()
									.limit(8).toList()));
				});
		Map<String, String> roles = new HashMap<>();
		if (!cards.isEmpty()) {
			BrainFolderRoles.judge(caller, cards).forEach((card, role) -> roles.put(byCard.get(card), role));
		}
		return roles;
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
