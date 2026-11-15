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
import prerna.util.Utility;

// onboarding: accounts from outside domains (returned, the owner saves them), and structured topic groups judged
// and named by the platform text model (COLLAB_LLM_ENGINE_ID), written as STATUS suggested, ORIGIN brain, with suggested
// members. No text model means no topic suggestions. The classifier files only against accepted topics.
public final class BrainTopicSuggest {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicSuggest.class);

	private static final int ACCOUNT_MIN_PEOPLE = 2;
	private static final int ACCOUNT_MIN_THREADS = 3;
	private static final int MEMBERS = 6;
	private static final int DESCRIBE = 5;
	// new topics as mail comes in: the job, how far back unfiled mail is looked at, how much of it, the least worth
	// a look, how many questions one look may raise, and how often a sync may start one
	public static final String FRESH_KIND = "topic_watch";
	private static final int FRESH_DAYS = 45;
	private static final int FRESH_POOL = 300;
	private static final int FRESH_MIN_POOL = 6;
	private static final int FRESH_MIN_THREADS = 3;
	private static final int FRESH_MAX = 3;
	private static final long FRESH_EVERY_HOURS = 24;
	static final Set<String> FREEMAIL = Set.of("gmail.com", "googlemail.com", "outlook.com", "hotmail.com",
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
		return suggest(user, false, false, null, false);
	}

	/**
	 * Writes suggested topics (linked to saved accounts) and returns them with any
	 * unsaved account suggestions.
	 */
	public static Map<String, Object> topics(User user) {
		return topics(user, false);
	}

	public static Map<String, Object> topics(User user, boolean dryRun) {
		return topics(user, dryRun, null);
	}

	// a job (null outside one) hears how grouping and naming are going
	static Map<String, Object> topics(User user, boolean dryRun, CollaborationJobUtils.Job job) {
		var owner = CollaborationDbUtils.ownerOf(user);
		// Serialize generation through replacement for this owner; accepted topics are never replaced.
		synchronized (CollaborationDbUtils.ownerLock("topic-suggest", owner.getValue0(), owner.getValue1())) {
			return suggest(user, true, dryRun, job, false);
		}
	}

	/**
	 * New topics as mail comes in: a new project or event shows up as recent conversations no topic holds that
	 * belong together. The same grouping and naming as onboarding, over that mail only. A group the owner takes
	 * part in is saved as a suggested topic with a question (BrainReviewUtils.NEW_TOPIC); the topics and questions
	 * already there stay. Accepting the question makes the topic active and files the group's conversations under
	 * it (settle). A group asked about before, whatever the answer was, is not asked about again.
	 */
	public static Map<String, Object> fresh(User user, boolean dryRun) {
		return fresh(user, dryRun, null);
	}

	static Map<String, Object> fresh(User user, boolean dryRun, CollaborationJobUtils.Job job) {
		var owner = CollaborationDbUtils.ownerOf(user);
		synchronized (CollaborationDbUtils.ownerLock("topic-suggest", owner.getValue0(), owner.getValue1())) {
			return suggest(user, true, dryRun, job, true);
		}
	}

	/**
	 * After a sync: looks for new topics when setup is over, no look ran in the last day, and enough unfiled
	 * conversations are there with some of them newer than the last look. Runs as its own job so the sync does
	 * not wait for it. Returns whether a look was started.
	 */
	// hours between looks: COLLAB_TOPIC_WATCH_HOURS, a day when unset or unreadable, never when 0
	static long everyHours() {
		String set = Utility.getDIHelperProperty(Constants.COLLAB_TOPIC_WATCH_HOURS);
		try {
			return set == null || set.isBlank() ? FRESH_EVERY_HOURS : Long.parseLong(set.trim());
		} catch (NumberFormatException e) {
			return FRESH_EVERY_HOURS;
		}
	}

	public static boolean watch(User user, String ownerId, String ownerType) {
		if (BrainTopicModel.engine(user) == null || !CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_SETTINGS WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ? AND SETUP_DONE_AT IS NOT NULL", ownerId, ownerType)) {
			return false;
		}
		Map<String, Object> last = CollaborationJobUtils.latest(ownerId, ownerType, FRESH_KIND);
		Timestamp lastAt = last == null ? null : (Timestamp) last.get("startedAt");
		Timestamp now = CollaborationDbUtils.now();
		long every = everyHours();
		if (every <= 0 || (lastAt != null && now.getTime() - lastAt.getTime() < every * 3_600_000L)) {
			return false;
		}
		List<Timestamp> unfiled = CollaborationDbUtils.query(UNFILED.replace("THREAD_ID FROM", "LAST_MESSAGE_AT FROM"),
				rs -> rs.getTimestamp(1), unfiledParams(ownerId, ownerType, now));
		long newer = unfiled.stream().filter(at -> lastAt == null || (at != null && at.after(lastAt))).count();
		if (unfiled.size() < FRESH_MIN_POOL || newer < FRESH_MIN_THREADS) {
			return false;
		}
		CollaborationJobUtils.start(ownerId, ownerType, FRESH_KIND, Map.of(), job -> {
			Map<String, Object> out = fresh(user, false, job);
			job.count("asked", out.get("topics") instanceof List<?> topics ? topics.size() : 0);
			job.count("pool", out.get("freshPool"));
		});
		return true;
	}

	// conversations of the last FRESH_DAYS days that a person wrote and none of the owner's topics holds
	private static final String UNFILED = "SELECT t.THREAD_ID FROM BRAIN_THREAD t WHERE t.OWNER_ID = ? AND "
			+ "t.OWNER_TYPE = ? AND (t.MUTED IS NULL OR t.MUTED = ?) AND (t.AUTOMATED IS NULL OR t.AUTOMATED = ?) AND "
			+ "t.LAST_MESSAGE_AT >= ? AND NOT EXISTS (SELECT 1 FROM BRAIN_THREAD_TOPIC l JOIN BRAIN_TOPIC tp ON "
			+ "tp.OWNER_ID = l.OWNER_ID AND tp.OWNER_TYPE = l.OWNER_TYPE AND tp.TOPIC_ID = l.TOPIC_ID WHERE "
			+ "l.OWNER_ID = t.OWNER_ID AND l.OWNER_TYPE = t.OWNER_TYPE AND l.THREAD_ID = t.THREAD_ID AND "
			+ "(tp.STATUS IS NULL OR tp.ORIGIN IS NULL OR tp.STATUS <> ? OR tp.ORIGIN <> ?)) "
			+ "ORDER BY t.LAST_MESSAGE_AT DESC";

	private static Object[] unfiledParams(String ownerId, String ownerType, Timestamp now) {
		return new Object[] { ownerId, ownerType, false, false,
				new Timestamp(now.getTime() - FRESH_DAYS * 86_400_000L), BrainTopicUtils.SUGGESTED, "brain" };
	}

	/**
	 * The owner answered a new-topic question. Accepted: the suggested topic becomes active, the people suggested
	 * for it become its members, and the group's conversations are filed under it, except where the owner filed
	 * or refused one themselves. Dismissed: the suggested topic is removed; the question stays, answered, so the
	 * same group is not asked about again.
	 */
	static void settle(String ownerId, String ownerType, String topicId, List<String> threadIds, boolean accepted) {
		Timestamp now = CollaborationDbUtils.now();
		String suggested = " WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TOPIC_ID = ? AND STATUS = ? AND ORIGIN = ?";
		if (!accepted) {
			if (CollaborationDbUtils.update("DELETE FROM BRAIN_TOPIC" + suggested, ownerId, ownerType, topicId,
					BrainTopicUtils.SUGGESTED, "brain") > 0) {
				CollaborationDbUtils.update("DELETE FROM BRAIN_TOPIC_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
						+ "TOPIC_ID = ?", ownerId, ownerType, topicId);
			}
			return;
		}
		if (CollaborationDbUtils.update("UPDATE BRAIN_TOPIC SET STATUS = ?, UPDATED_AT = ?" + suggested,
				BrainTopicUtils.ACTIVE, now, ownerId, ownerType, topicId, BrainTopicUtils.SUGGESTED, "brain") == 0) {
			// already kept, or gone: nothing of the proposal to apply
			return;
		}
		CollaborationDbUtils.update("UPDATE BRAIN_TOPIC_PERSON SET STATE = ?, CHANGED_AT = ? WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND TOPIC_ID = ? AND STATE = ?", BrainTopicUtils.MEMBER, now, ownerId, ownerType, topicId,
				"suggested");
		for (String threadId : threadIds) {
			try {
				BrainThreadClassifier.linkKept(ownerId, ownerType, threadId, topicId, Map.of("proposed", true),
						BrainTopicOnboarding.VERSION);
			} catch (RuntimeException e) {
				// a conversation deleted since the question was asked
				classLogger.warn("Could not file {} under new topic {}", threadId, topicId, e);
			}
		}
	}

	private static Map<String, Object> suggest(User user, boolean writeTopics, boolean dryRun,
			CollaborationJobUtils.Job job, boolean fresh) {
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
		Map<String, String> accountOfPerson = new HashMap<>();
		CollaborationDbUtils.query(
				"SELECT PERSON_ID, EMAIL_NORM, DISPLAY_NAME, ACCOUNT_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					emails.put(rs.getString(1), rs.getString(2));
					if (rs.getString(3) != null) {
						names.put(rs.getString(1), rs.getString(3));
					}
					if (rs.getString(4) != null) {
						accountOfPerson.put(rs.getString(1), rs.getString(4));
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
		// what was asked about before, answered or not: a group made mostly of it is not asked about again
		Set<String> asked = new HashSet<>();
		if (fresh) {
			List<String> pool = CollaborationDbUtils.query(UNFILED, rs -> rs.getString(1),
					unfiledParams(ownerId, ownerType, CollaborationDbUtils.now()));
			threads.keySet().retainAll(new HashSet<>(pool.subList(0, Math.min(FRESH_POOL, pool.size()))));
			CollaborationDbUtils.query("SELECT DETAIL_JSON FROM BRAIN_REVIEW WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
					+ "KIND = ?", rs -> {
						Map<String, Object> detail = CollaborationDbUtils
								.parseMap(CollaborationDbUtils.getString(rs, "DETAIL_JSON"));
						if (detail != null && detail.get("threadIds") instanceof List<?> ids) {
							ids.forEach(id -> asked.add(String.valueOf(id)));
						}
						return null;
					}, ownerId, ownerType, BrainReviewUtils.NEW_TOPIC);
		}
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
		if (fresh) {
			out.put("freshPool", threads.size());
			if (threads.size() < FRESH_MIN_POOL) {
				out.put("topics", List.of());
				return out;
			}
		}

		List<Candidate> candidates;
		try {
			String engineId = BrainTopicModel.engine(user);
			if (engineId == null) {
				out.put("topics", List.of());
				out.put("modelError",
						"No topic model is set up (" + Constants.COLLAB_LLM_ENGINE_ID + "); ask an admin to set one");
				return out;
			}
			// The only topic proposer: snapshot -> deterministic groups -> complete model votes, with no writes yet.
			BrainTopicOnboarding.Result result = BrainTopicOnboarding.propose(user, engineId, ownerId, ownerType,
					threads, self, emails, vips, ownOrg, topicNames, job);
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
		// the questions to raise once the suggested topics are saved
		List<Object[]> questions = new ArrayList<>();
		CollaborationDbUtils.TransactionWork replace = conn -> {
			if (!dryRun && !fresh) {
				// Replace only after a complete generation, in the same transaction as the inserts.
				for (String id : CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND STATUS = ? AND ORIGIN = ? FOR UPDATE", rs -> rs.getString(1), ownerId, ownerType,
						BrainTopicUtils.SUGGESTED, "brain")) {
					BrainTopicUtils.deleteTopic(user, id);
				}
			}
			for (Candidate c : proposals) {
				if (written.size() >= (fresh ? FRESH_MAX : BrainTopicStructure.A1.topics())
						|| topicNames.contains(c.name().toLowerCase(Locale.ROOT))) {
					continue;
				}
				String topicId = "t-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "suggest", c.key());
				if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
						+ "TOPIC_ID = ? AND (STATUS IS NULL OR ORIGIN IS NULL OR STATUS <> ? OR ORIGIN <> ?)", ownerId, ownerType, topicId,
						BrainTopicUtils.SUGGESTED, "brain")) {
					continue;
				}
				int mine = (int) c.threads().stream().filter(t -> writers.getOrDefault(t.id(), Set.of()).contains(self)).count();
				int withVip = (int) c.threads().stream().filter(t -> t.people().stream().anyMatch(vips::contains)).count();
				// pre-checked on screen: you wrote on a third of its conversations or more, or a VIP is on it. One
				// reply among thirty threads of recruiters or vendors is not taking part: on the fixture worlds every
				// real topic had the owner writing on 44% or more, and half of the other groups under a third
				boolean takesPart = (mine > 0 && 3 * mine >= c.threads().size()) || withVip > 0;
				if (fresh) {
					// a question interrupts: only a group the owner takes part in, asked about once
					long before = c.threads().stream().filter(t -> asked.contains(t.id())).count();
					if (!takesPart || c.threads().size() < FRESH_MIN_THREADS || 2 * before >= c.threads().size()
							|| CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND "
									+ "OWNER_TYPE = ? AND TOPIC_ID = ?", ownerId, ownerType, topicId)) {
						continue;
					}
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
						// the owner's own link (one outside contact kept by email) beats the domain's account
						String a = accountOfPerson.getOrDefault(p, accountByDomain.get(d));
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
				// the Outlook folder most of its mail is in, shown on its card; filing new mail from a project folder
				// is offered and on by default, and made only when the owner keeps the topic
				row.put("folders", c.folderPath() == null ? List.of() : List.of(c.folderPath()));
				row.put("folderIds", c.folderId() == null ? List.of() : List.of(c.folderId()));
				row.put("fileFromFolder", c.folderId() != null);
				// optional clues the owner can turn on; never saved as keywords on their own
				row.put("suggestedTerms", c.suggestedTerms());
				row.put("sampleSubjects", samples(c, mailThreads, writers, self).stream().limit(3)
						.collect(Collectors.toList()));
				row.put("youWrote", mine);
				row.put("vipThreads", withVip);
				row.put("suggested", takesPart);
				written.add(row);
				if (fresh && !dryRun) {
					List<String> subjects = samples(c, mailThreads, writers, self);
					Map<String, Object> detail = new LinkedHashMap<>();
					detail.put("text", "New topic suggested: " + c.name());
					detail.put("detail", c.threads().size() + " recent conversations no topic holds"
							+ (subjects.isEmpty() ? "" : ", such as: "
									+ String.join("; ", subjects.subList(0, Math.min(3, subjects.size())))));
					detail.put("candidate", topicId);
					detail.put("threadIds", row.get("threadIds"));
					questions.add(new Object[] { topicId, c.key(), detail });
				}
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
		for (Object[] q : questions) {
			@SuppressWarnings("unchecked")
			Map<String, Object> detail = (Map<String, Object>) q[2];
			BrainReviewUtils.addReview(ownerId, ownerType, BrainReviewUtils.NEW_TOPIC, "topic", (String) q[0],
					(String) q[1], detail);
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
	// folderPath: the Outlook folder most of its mail is in; folderId only when that is a project folder, which the
	// owner may then file new mail from
	record Candidate(String key, String name, List<Thread> threads, List<String> keywords, String reason, String about,
			List<String> suggestedTerms, String folderId, String folderPath) {
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
