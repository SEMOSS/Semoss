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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.util.Constants;

// onboarding: accounts from outside domains (returned, the owner saves them), and structured topic groups judged
// and named by the platform text model (COLLAB_LLM_ENGINE_ID), written as STATUS suggested, ORIGIN brain, with suggested
// members. No text model means no topic suggestions. The classifier files only against accepted topics.
public final class BrainTopicSuggest {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicSuggest.class);

	private static final int ACCOUNT_MIN_PEOPLE = 2;
	private static final int ACCOUNT_MIN_THREADS = 3;
	private static final int MEMBERS = 6;
	private static final int DESCRIBE = 5;
	private static final Set<String> FREEMAIL = Set.of("gmail.com", "googlemail.com", "outlook.com", "hotmail.com",
			"live.com", "msn.com", "yahoo.com", "icloud.com", "me.com", "aol.com", "proton.me", "protonmail.com");

	private BrainTopicSuggest() {
	}

	record Thread(String id, String subject, Set<String> people) {
	}

	/**
	 * Account suggestions only; writes nothing. Run before topics so topics can
	 * link to saved accounts.
	 */
	public static Map<String, Object> accounts(User user) {
		return suggest(user, false, false);
	}

	/**
	 * Writes suggested topics (linked to saved accounts) and returns them with any
	 * unsaved account suggestions.
	 */
	public static Map<String, Object> topics(User user) {
		return topics(user, false);
	}

	public static Map<String, Object> topics(User user, boolean dryRun) {
		var owner = CollaborationDbUtils.ownerOf(user);
		synchronized (CollaborationDbUtils.ownerLock("topic-onboarding", owner.getValue0(), owner.getValue1())) {
			return suggest(user, true, dryRun);
		}
	}

	private static Map<String, Object> suggest(User user, boolean writeTopics, boolean dryRun) {
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();

		String self = CollaborationDbUtils.queryOne(
				"SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND " + "OWNER_TYPE = ? AND RELATIONSHIP = ?",
				rs -> rs.getString(1), ownerId, ownerType, "self");
		String myDomain = BrainMailImport.domain(CollaborationDbUtils.queryOne(
				"SELECT EMAIL_NORM FROM BRAIN_PERSON " + "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
				rs -> rs.getString(1), ownerId, ownerType, self));
		BrainOrgDomains.Org ownOrg = BrainOrgDomains.load(ownerId, ownerType, myDomain);
		Map<String, String> emails = new HashMap<>();
		Map<String, String> names = new HashMap<>();
		CollaborationDbUtils.query(
				"SELECT PERSON_ID, EMAIL_NORM, DISPLAY_NAME FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					emails.put(rs.getString(1), rs.getString(2));
					if (rs.getString(3) != null) {
						names.put(rs.getString(1), rs.getString(3));
					}
					return null;
				}, ownerId, ownerType);

		Map<String, Thread> threads = new LinkedHashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SUBJECT FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND (MUTED IS NULL OR MUTED = ?) AND (AUTOMATED IS NULL OR AUTOMATED = ?) "
				// a chat named only by who is in it says nothing about a topic; the classifier
				// files it from its text
				+ "AND NOT (SOURCE = ? AND SUBJECT LIKE ?) ORDER BY LAST_MESSAGE_AT DESC",
				rs -> threads.put(rs.getString(1),
						new Thread(rs.getString(1), CollaborationDbUtils.getString(rs, "SUBJECT"),
								new LinkedHashSet<>())),
				ownerId, ownerType, false, false, "teams", BrainGraphHeaderSource.CHAT_PREFIX + "%");
		// automated and list senders, VIPs, and who wrote on each thread
		Set<String> automated = new HashSet<>();
		Set<String> vips = new HashSet<>();
		CollaborationDbUtils.query(
				"SELECT PERSON_ID, RELATIONSHIP, IS_VIP FROM BRAIN_PERSON WHERE OWNER_ID = ? AND " + "OWNER_TYPE = ?",
				rs -> {
					if (BrainSenderTyping.AUTOMATED.equals(rs.getString(2))) {
						automated.add(rs.getString(1));
					}
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_VIP"))) {
						vips.add(rs.getString(1));
					}
					return null;
				}, ownerId, ownerType);
		// accepting or cancelling a meeting is not writing on a thread
		Map<String, Set<String>> writers = new HashMap<>();
		Set<String> mailThreads = new HashSet<>();
		CollaborationDbUtils.query(
				"SELECT THREAD_ID, SENDER_PERSON_ID, MEETING FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND THREAD_ID IS NOT NULL",
				rs -> {
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MEETING"))) {
						return null;
					}
					mailThreads.add(rs.getString(1));
					if (rs.getString(2) != null) {
						writers.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2));
					}
					return null;
				}, ownerId, ownerType);
		CollaborationDbUtils.query("SELECT THREAD_ID, PERSON_ID FROM BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND (INCLUDED IS NULL OR INCLUDED = ?)", rs -> {
					Thread t = threads.get(rs.getString(1));
					if (t != null && !rs.getString(2).equals(self) && !automated.contains(rs.getString(2))) {
						t.people().add(rs.getString(2));
					}
					return null;
				}, ownerId, ownerType, true);

		// existing accounts by domain, existing topic names
		Map<String, String> accountByDomain = new HashMap<>();
		CollaborationDbUtils.query(
				"SELECT ACCOUNT_ID, NAME, DOMAINS_JSON FROM BRAIN_ACCOUNT WHERE OWNER_ID = ? AND " + "OWNER_TYPE = ?",
				rs -> {
					for (String d : CollaborationDbUtils.toStringList(
							CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "DOMAINS_JSON")))) {
						accountByDomain.put(d, rs.getString(1));
					}
					return null;
				}, ownerId, ownerType);
		Set<String> topicNames = new HashSet<>(CollaborationDbUtils.query("SELECT LOWER(NAME) FROM BRAIN_TOPIC WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ? AND (STATUS IS NULL OR ORIGIN IS NULL OR STATUS <> ? OR ORIGIN <> ?)", rs -> rs.getString(1),
				ownerId, ownerType, BrainTopicUtils.SUGGESTED, "brain"));
		Set<String> neverDomains = BrainRulesGate.activeRules(ownerId, ownerType).stream()
				.filter(r -> BrainRuleUtils.NEVER_DOMAIN.equals(r.kind()) && r.value() != null)
				.map(r -> BrainRulesGate.norm(r.value())).collect(Collectors.toSet());

		// accounts: outside organisations by registrable domain
		Map<String, Set<String>> domainPeople = new HashMap<>();
		Map<String, Set<String>> domainThreads = new HashMap<>();
		for (Thread t : threads.values()) {
			for (String p : t.people()) {
				String d = org(BrainMailImport.domain(emails.get(p)));
				if (d == null || ownOrg.isMine(d) || FREEMAIL.contains(d) || neverDomains.contains(d)) {
					continue;
				}
				domainPeople.computeIfAbsent(d, k -> new HashSet<>()).add(p);
				domainThreads.computeIfAbsent(d, k -> new HashSet<>()).add(t.id());
			}
		}
		List<Map<String, Object>> accounts = new ArrayList<>();
		for (String d : domainPeople.keySet()) {
			int people = domainPeople.get(d).size();
			int count = domainThreads.get(d).size();
			if (accountByDomain.containsKey(d) || (people < ACCOUNT_MIN_PEOPLE && count < ACCOUNT_MIN_THREADS)) {
				continue;
			}
			// threads with them the owner wrote on, and VIPs there
			int twoWay = (int) domainThreads.get(d).stream()
					.filter(id -> self != null && writers.getOrDefault(id, Set.of()).contains(self)).count();
			int vipCount = (int) domainPeople.get(d).stream().filter(vips::contains).count();
			Map<String, Object> a = new LinkedHashMap<>();
			a.put("name", label(d));
			a.put("domain", d);
			a.put("kind", "client");
			a.put("people", people);
			a.put("threads", count);
			a.put("twoWayThreads", twoWay);
			a.put("vips", vipCount);
			// pre-checked on screen only when you write to them or a VIP is there; volume alone is not work
			a.put("suggested", twoWay > 0 || vipCount > 0);
			accounts.add(a);
		}
		accounts.sort((x, y) -> {
			int byVip = (Integer) y.get("vips") - (Integer) x.get("vips");
			int byTwoWay = (Integer) y.get("twoWayThreads") - (Integer) x.get("twoWayThreads");
			return byVip != 0 ? byVip
					: byTwoWay != 0 ? byTwoWay : (Integer) y.get("threads") - (Integer) x.get("threads");
		});

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("accounts", accounts);
		if (!writeTopics) {
			return out;
		}
		out.put("dryRun", dryRun);

		List<Candidate> candidates;
		try {
			String engineId = BrainTopicModel.engine(user);
			if (engineId == null) {
				out.put("topics", List.of());
				out.put("modelError",
						"No topic model is set up (" + Constants.COLLAB_LLM_ENGINE_ID + "); ask an admin to set one");
				return out;
			}
			BrainTopicOnboarding.Result result = BrainTopicOnboarding.propose(user, engineId, ownerId, ownerType,
					threads, self, emails, vips, ownOrg, topicNames);
			out.putAll(result.diagnostics());
			if (out.containsKey("modelError")) {
				out.put("topics", List.of());
				return out;
			}
			candidates = new ArrayList<>(result.candidates());
		} catch (RuntimeException e) {
			classLogger.warn("Topic model failed", e);
			out.put("topics", List.of());
			out.put("modelError", e.getMessage());
			return out;
		}

		// threads with a VIP or the owner on them rank a topic first
		java.util.function.ToIntFunction<Candidate> weight = c -> (int) c.threads().stream()
				.filter(t -> t.people().stream().anyMatch(vips::contains)
						|| writers.getOrDefault(t.id(), Set.of()).contains(self))
				.count();
		candidates.sort(
				(x, y) -> weight.applyAsInt(y) != weight.applyAsInt(x) ? weight.applyAsInt(y) - weight.applyAsInt(x)
						: y.threads().size() - x.threads().size());

		List<Map<String, Object>> written = new ArrayList<>();
		Timestamp now = CollaborationDbUtils.now();
		List<Candidate> proposals = candidates;
		CollaborationDbUtils.TransactionWork replace = conn -> {
			if (!dryRun) {
				// Replace only after a complete generation, in the same transaction as the inserts.
				for (String id : CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND STATUS = ? AND ORIGIN = ? FOR UPDATE", rs -> rs.getString(1), ownerId, ownerType,
						BrainTopicUtils.SUGGESTED, "brain")) {
					BrainTopicUtils.deleteTopic(user, id);
				}
			}
			for (Candidate c : proposals) {
				if (written.size() >= BrainTopicStructure.A1.topics() || topicNames.contains(c.name().toLowerCase(Locale.ROOT))) {
					continue;
				}
				String topicId = "t-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "suggest", c.key());
				if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
						+ "TOPIC_ID = ? AND (STATUS IS NULL OR ORIGIN IS NULL OR STATUS <> ? OR ORIGIN <> ?)", ownerId, ownerType, topicId,
						BrainTopicUtils.SUGGESTED, "brain")) {
					continue;
				}
				// an account (or outside at all) only when its people are on at least half the threads, so
				// one outside person copied on a few threads does not claim the topic
				Map<String, Integer> byAccount = new HashMap<>();
				Map<String, Integer> byPerson = new HashMap<>();
				int outsideThreads = 0;
				for (Thread t : c.threads()) {
					Set<String> accountsHere = new HashSet<>();
					boolean outsideHere = false;
					for (String p : t.people()) {
						byPerson.merge(p, 1, Integer::sum);
						String d = org(BrainMailImport.domain(emails.get(p)));
						String a = accountByDomain.get(d);
						if (a != null) {
							accountsHere.add(a);
						}
						outsideHere |= d != null && !ownOrg.isMine(d);
					}
					accountsHere.forEach(a -> byAccount.merge(a, 1, Integer::sum));
					outsideThreads += outsideHere ? 1 : 0;
				}
				int half = (c.threads().size() + 1) / 2;
				String accountId = byAccount.entrySet().stream().filter(e -> e.getValue() >= half)
						.max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse(null);
				boolean outside = accountId != null || outsideThreads >= half;
				List<String> members = byPerson.entrySet().stream()
						.filter(e -> e.getValue() >= Math.max(2, c.threads().size() / 3)
								&& !automated.contains(e.getKey()))
						.sorted((x, y) -> y.getValue() - x.getValue()).limit(MEMBERS).map(Map.Entry::getKey)
						.collect(Collectors.toList());
				String kind = outside ? "client" : "internal";
				if (!dryRun) {
					CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, SHORT_NAME, "
							+ "DESCRIPTION, KIND, ACCOUNT_ID, KEYWORDS_JSON, STATUS, ORIGIN, SUGGEST_REASON, LAST_ACTIVITY_AT, "
							+ "CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType,
							topicId, c.name(), c.name(), c.about() != null ? c.about() : description(samples(c, mailThreads, writers, self)), kind, accountId, CollaborationDbUtils.toJson(c.keywords()),
							BrainTopicUtils.SUGGESTED, "brain", c.reason(), now, now, now);
					for (String p : members) {
						CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC_PERSON (OWNER_ID, OWNER_TYPE, TOPIC_ID, "
								+ "PERSON_ID, STATE, ORIGIN, REASON, CHANGED_BY, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
								ownerId, ownerType, topicId, p, "suggested", "brain", "On " + byPerson.get(p) + " of its threads",
								"brain", now);
					}
				}
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("id", topicId);
				row.put("name", c.name());
				row.put("kind", kind);
				row.put("accountId", accountId);
				row.put("reason", c.reason());
				row.put("threadIds", c.threads().stream().map(Thread::id).collect(Collectors.toList()));
				row.put("memberIds", members);
				// the card's profile: key people by name, and the outside domains on half its threads or more
				row.put("people", members.stream().map(p -> Map.of("id", p, "name", names.getOrDefault(p, emails.getOrDefault(p, p))))
						.collect(Collectors.toList()));
				Map<String, Integer> byDomain = new HashMap<>();
				for (Thread t : c.threads()) {
					t.people().stream().map(p -> org(BrainMailImport.domain(emails.get(p))))
							.filter(d -> d != null && !ownOrg.isMine(d) && !FREEMAIL.contains(d)).distinct()
							.forEach(d -> byDomain.merge(d, 1, Integer::sum));
				}
				row.put("domains", byDomain.entrySet().stream().filter(e -> e.getValue() >= half)
						.sorted((x, y) -> y.getValue() - x.getValue()).map(Map.Entry::getKey).limit(3)
						.collect(Collectors.toList()));
				row.put("about", c.about());
				row.put("sampleSubjects", samples(c, mailThreads, writers, self).stream().limit(3)
						.collect(Collectors.toList()));
				int mine = (int) c.threads().stream().filter(t -> writers.getOrDefault(t.id(), Set.of()).contains(self)).count();
				int withVip = (int) c.threads().stream().filter(t -> t.people().stream().anyMatch(vips::contains)).count();
				row.put("youWrote", mine);
				row.put("vipThreads", withVip);
				// pre-checked on screen: you took part, or a VIP is on it
				row.put("suggested", mine > 0 || withVip > 0);
				written.add(row);
			}
		};
		if (dryRun) {
			try {
				replace.run(null);
			} catch (java.sql.SQLException e) {
				throw new IllegalStateException("Topic preview failed", e);
			}
		} else {
			CollaborationDbUtils.batch(replace);
		}

		out.put("topics", written);
		return out;
	}

	// what the owner sees and the classifier matches threads against: a few of its subjects
	private static String description(List<String> subjects) {
		return subjects.isEmpty() ? null
				: "Threads such as: " + String.join("; ", subjects.subList(0, Math.min(DESCRIBE, subjects.size()))) + ".";
	}

	// subjects of real mail first (threads the owner wrote on, then others), calendar-only threads last
	private static List<String> samples(Candidate c, Set<String> mailThreads, Map<String, Set<String>> writers,
			String self) {
		java.util.function.ToIntFunction<Thread> rank = t -> !mailThreads.contains(t.id()) ? 2
				: writers.getOrDefault(t.id(), Set.of()).contains(self) ? 0 : 1;
		return c.threads().stream().sorted(java.util.Comparator.comparingInt(rank)).map(Thread::subject)
				.filter(x -> x != null && !x.isBlank()).distinct().collect(Collectors.toList());
	}

	// about: the topic model's one line on what the topic covers, stored as its description
	record Candidate(String key, String name, List<Thread> threads, List<String> keywords, String reason, String about) {
	}

	// the organisation's domain: mail.adatum.example is adatum.example
	static String org(String domain) {
		return BrainOrgDomains.org(domain);
	}

	// "northwind.example" to "Northwind"
	static String label(String domain) {
		String first = domain.contains(".") ? domain.substring(0, domain.indexOf('.')) : domain;
		// a short label reads as an acronym; the owner renames anything else
		return first.isEmpty() ? domain
				: first.length() <= 3 ? first.toUpperCase(Locale.ROOT)
						: Character.toUpperCase(first.charAt(0)) + first.substring(1);
	}
}
