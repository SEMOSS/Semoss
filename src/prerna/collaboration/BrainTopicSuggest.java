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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;

// onboarding topics: the platform text model when one is set (BrainTopicModel), else rules. The classifier picks
// among topics, it does not name them. Rules:
// accounts from outside domains (returned, the owner saves them) and topics from recurring subject prefixes and
// account threads (written as STATUS suggested, ORIGIN brain, with suggested members). The classifier files only
// against accepted topics.
public final class BrainTopicSuggest {

	private static final Logger classLogger = LogManager.getLogger(BrainTopicSuggest.class);

	private static final int MAX_TOPICS = 12;
	private static final int MIN_THREADS = 2;
	private static final int ACCOUNT_MIN_PEOPLE = 2;
	private static final int ACCOUNT_MIN_THREADS = 3;
	private static final int ACCOUNT_TOPIC_MIN_THREADS = 3;
	private static final int MEMBERS = 6;
	private static final Set<String> FREEMAIL = Set.of("gmail.com", "googlemail.com", "outlook.com", "hotmail.com",
			"live.com", "msn.com", "yahoo.com", "icloud.com", "me.com", "aol.com", "proton.me", "protonmail.com");
	// subject prefixes that are mail machinery, not work
	private static final Set<String> STOP_PREFIXES = Set.of("accepted", "declined", "tentative", "tentatively accepted",
			"canceled", "cancelled", "updated invitation", "invitation", "updated", "reminder", "action required",
			"action needed", "fyi", "urgent", "important", "automatic reply", "out of office", "undeliverable", "read",
			"new", "update", "request", "approval required", "approved", "rejected", "notice", "notification", "alert",
			"total time", "response required", "sample", "test");
	// [EXTERNAL], [CAUTION] and similar banner tags; reply and forward prefixes
	private static final Pattern TAG = Pattern.compile("^\\s*\\[(external|caution|ext|warning|spam|secure)\\]\\s*",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern REPLY = Pattern.compile("^\\s*(re|fw|fwd|aw|wg)\\s*:\\s*", Pattern.CASE_INSENSITIVE);
	private static final Pattern BRACKET = Pattern.compile("^\\s*\\[([^\\]]{2,40})\\]");
	private static final Pattern PREFIX = Pattern.compile("^([^:|]{2,40}?)\\s*(:|\\s-\\s|\\s\\|\\s)");

	private BrainTopicSuggest() {
	}

	record Thread(String id, String subject, Set<String> people) {
	}

	/** Account suggestions only; writes nothing. Run before topics so topics can link to saved accounts. */
	public static Map<String, Object> accounts(User user) {
		return suggest(user, false);
	}

	/** Writes suggested topics (linked to saved accounts) and returns them with any unsaved account suggestions. */
	public static Map<String, Object> topics(User user) {
		return suggest(user, true);
	}

	private static Map<String, Object> suggest(User user, boolean writeTopics) {
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (writeTopics) {
			// each run replaces the last run's undecided suggestions, so coming back to the step shows a fresh set
			for (String id : CollaborationDbUtils.query("SELECT TOPIC_ID FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND "
					+ "OWNER_TYPE = ? AND STATUS = ? AND ORIGIN = ?", rs -> rs.getString(1), ownerId, ownerType,
					BrainTopicUtils.SUGGESTED, "brain")) {
				BrainTopicUtils.deleteTopic(user, id);
			}
		}

		String self = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType, "self");
		String myDomain = BrainMailImport.domain(CollaborationDbUtils.queryOne("SELECT EMAIL_NORM FROM BRAIN_PERSON "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", rs -> rs.getString(1), ownerId, ownerType,
				self));
		BrainOrgDomains.Org ownOrg = BrainOrgDomains.load(ownerId, ownerType, myDomain);
		Map<String, String> emails = new HashMap<>();
		CollaborationDbUtils.query("SELECT PERSON_ID, EMAIL_NORM FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> emails.put(rs.getString(1), rs.getString(2)), ownerId, ownerType);

		Map<String, Thread> threads = new LinkedHashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SUBJECT FROM BRAIN_THREAD WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
				+ "AND (MUTED IS NULL OR MUTED = ?) AND (AUTOMATED IS NULL OR AUTOMATED = ?) "
				// a chat named only by who is in it says nothing about a topic; the classifier files it from its text
				+ "AND NOT (SOURCE = ? AND SUBJECT LIKE ?) ORDER BY LAST_MESSAGE_AT DESC",
				rs -> threads.put(rs.getString(1), new Thread(rs.getString(1), CollaborationDbUtils.getString(rs, "SUBJECT"),
						new LinkedHashSet<>())),
				ownerId, ownerType, false, false, "teams", BrainGraphHeaderSource.CHAT_PREFIX + "%");
		// automated and list senders, VIPs, and who wrote on each thread
		Set<String> automated = new HashSet<>();
		Set<String> vips = new HashSet<>();
		CollaborationDbUtils.query("SELECT PERSON_ID, RELATIONSHIP, IS_VIP FROM BRAIN_PERSON WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ?", rs -> {
					if (BrainSenderTyping.AUTOMATED.equals(rs.getString(2))) {
						automated.add(rs.getString(1));
					}
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_VIP"))) {
						vips.add(rs.getString(1));
					}
					return null;
				}, ownerId, ownerType);
		Map<String, Set<String>> writers = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SENDER_PERSON_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND THREAD_ID IS NOT NULL AND SENDER_PERSON_ID IS NOT NULL", rs -> writers
						.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2)), ownerId, ownerType);
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
		Map<String, String> accountNames = new HashMap<>();
		CollaborationDbUtils.query("SELECT ACCOUNT_ID, NAME, DOMAINS_JSON FROM BRAIN_ACCOUNT WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ?", rs -> {
					accountNames.put(rs.getString(1), rs.getString(2));
					for (String d : CollaborationDbUtils.toStringList(
							CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "DOMAINS_JSON")))) {
						accountByDomain.put(d, rs.getString(1));
					}
					return null;
				}, ownerId, ownerType);
		Set<String> topicNames = new HashSet<>(CollaborationDbUtils.query("SELECT LOWER(NAME) FROM BRAIN_TOPIC WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ?", rs -> rs.getString(1), ownerId, ownerType));
		Set<String> neverDomains = BrainRulesGate.activeRules(ownerId, ownerType).stream()
				.filter(r -> BrainRuleUtils.NEVER_DOMAIN.equals(r.kind()) && r.value() != null)
				.map(r -> BrainRulesGate.norm(r.value())).collect(Collectors.toSet());

		// accounts: outside organisations by registrable domain
		Map<String, Set<String>> domainPeople = new HashMap<>();
		Map<String, Set<String>> domainThreads = new HashMap<>();
		for (Thread t : threads.values()) {
			for (String p : t.people()) {
				String d = org(BrainMailImport.domain(emails.get(p)));
				if (d == null || ownOrg.isMine(d) || FREEMAIL.contains(d) || neverDomains.contains(d)
						|| BrainSenderTyping.automatedAddress(emails.get(p))) {
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
			// pre-checked on screen: you write to them, a VIP is there, or several people
			a.put("suggested", twoWay > 0 || vipCount > 0 || people >= 3);
			accounts.add(a);
		}
		accounts.sort((x, y) -> {
			int byVip = (Integer) y.get("vips") - (Integer) x.get("vips");
			int byTwoWay = (Integer) y.get("twoWayThreads") - (Integer) x.get("twoWayThreads");
			return byVip != 0 ? byVip : byTwoWay != 0 ? byTwoWay : (Integer) y.get("threads") - (Integer) x.get("threads");
		});

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("accounts", accounts);
		if (!writeTopics) {
			return out;
		}

		// topics: threads sharing a subject prefix, then leftover threads of one account
		Map<String, List<Thread>> byPrefix = new LinkedHashMap<>();
		Map<String, Map<String, Integer>> casing = new HashMap<>();
		for (Thread t : threads.values()) {
			String prefix = prefix(t.subject());
			if (prefix != null) {
				String key = prefix.toLowerCase();
				byPrefix.computeIfAbsent(key, k -> new ArrayList<>()).add(t);
				casing.computeIfAbsent(key, k -> new HashMap<>()).merge(prefix, 1, Integer::sum);
			}
		}
		List<Candidate> candidates = new ArrayList<>();
		Set<String> covered = new HashSet<>();
		Set<String> firstNames = new HashSet<>();
		CollaborationDbUtils.query("SELECT DISPLAY_NAME FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
			String n = CollaborationDbUtils.getString(rs, "DISPLAY_NAME");
			if (n != null) {
				// "Doe, Jane" and "Jane Doe" both give jane
				String first = n.contains(",") ? n.substring(n.indexOf(',') + 1).trim() : n.trim();
				first = first.split("\\s+")[0].toLowerCase();
				if (!first.isEmpty()) {
					firstNames.add(first);
				}
			}
			return null;
		}, ownerId, ownerType);
		for (Map.Entry<String, List<Thread>> e : byPrefix.entrySet()) {
			if (STOP_PREFIXES.contains(e.getKey()) || firstNames.contains(e.getKey())) {
				continue;
			}
			// a conversation: the owner wrote on one, or two or more people wrote
			Set<String> wrote = new HashSet<>();
			e.getValue().forEach(t -> wrote.addAll(writers.getOrDefault(t.id(), Set.of())));
			wrote.removeAll(automated);
			if (!wrote.contains(self) && wrote.size() < 2) {
				continue;
			}
			if (e.getValue().size() >= MIN_THREADS) {
				String name = casing.get(e.getKey()).entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
				candidates.add(new Candidate("prefix:" + e.getKey(), name, e.getValue(), List.of(e.getKey()),
						e.getValue().size() + " threads start with \"" + name + "\""));
				e.getValue().forEach(t -> covered.add(t.id()));
			}
		}
		for (String d : domainThreads.keySet()) {
			List<Thread> rest = domainThreads.get(d).stream().filter(id -> !covered.contains(id)).map(threads::get)
					.collect(Collectors.toList());
			if (rest.size() >= ACCOUNT_TOPIC_MIN_THREADS) {
				String account = accountByDomain.containsKey(d) ? accountNames.get(accountByDomain.get(d)) : label(d);
				candidates.add(new Candidate("account:" + d, account, rest, List.of(d.substring(0, d.indexOf('.') < 0
						? d.length() : d.indexOf('.'))), rest.size() + " other threads with people from " + d));
			}
		}
		// the text model groups and names topics when one is set; rules are the fallback
		String source = "rules";
		try {
			String engineId = BrainTopicModel.engine(user);
			if (engineId != null) {
				List<Candidate> proposed = modelCandidates(user, engineId, ownerId, ownerType, threads, writers, vips,
						emails, accountByDomain, accountNames, ownOrg, self, topicNames);
				if (!proposed.isEmpty()) {
					candidates = proposed;
					source = "model";
				}
			}
		} catch (RuntimeException e) {
			classLogger.warn("Topic model failed; using rules", e);
			out.put("modelError", e.getMessage());
		}
		out.put("source", source);

		// subject topics first; an account catch-all only when that account has no subject topic
		// threads with a VIP or the owner on them rank a topic first; subject topics before account catch-alls
		java.util.function.ToIntFunction<Candidate> weight = c -> (int) c.threads().stream()
				.filter(t -> t.people().stream().anyMatch(vips::contains)
						|| writers.getOrDefault(t.id(), Set.of()).contains(self))
				.count();
		candidates.sort((x, y) -> x.key().startsWith("prefix:") != y.key().startsWith("prefix:")
				? (x.key().startsWith("prefix:") ? -1 : 1)
				: weight.applyAsInt(y) != weight.applyAsInt(x) ? weight.applyAsInt(y) - weight.applyAsInt(x)
						: y.threads().size() - x.threads().size());
		Set<String> accountsWithTopic = new HashSet<>();

		List<Map<String, Object>> written = new ArrayList<>();
		Timestamp now = CollaborationDbUtils.now();
		for (Candidate c : candidates) {
			if (written.size() >= MAX_TOPICS || topicNames.contains(c.name().toLowerCase())) {
				continue;
			}
			String topicId = "t-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "suggest", c.key());
			if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_TOPIC WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND "
					+ "TOPIC_ID = ?", ownerId, ownerType, topicId)) {
				continue;
			}
			// the account most of its outside people belong to; internal when none
			Map<String, Integer> byAccount = new HashMap<>();
			Map<String, Integer> byPerson = new HashMap<>();
			for (Thread t : c.threads()) {
				for (String p : t.people()) {
					byPerson.merge(p, 1, Integer::sum);
					String a = accountByDomain.get(org(BrainMailImport.domain(emails.get(p))));
					if (a != null) {
						byAccount.merge(a, 1, Integer::sum);
					}
				}
			}
			String accountId = byAccount.entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey)
					.orElse(null);
			boolean outside = c.threads().stream().flatMap(t -> t.people().stream())
					.map(p -> org(BrainMailImport.domain(emails.get(p))))
					.anyMatch(d -> d != null && !ownOrg.isMine(d));
			List<String> members = byPerson.entrySet().stream()
					.filter(e -> e.getValue() >= Math.max(2, c.threads().size() / 3)
							&& !automated.contains(e.getKey()) && !BrainSenderTyping.automatedAddress(emails.get(e.getKey())))
					.sorted((x, y) -> y.getValue() - x.getValue()).limit(MEMBERS).map(Map.Entry::getKey)
					.collect(Collectors.toList());
			if (c.key().startsWith("account:") && accountId != null && accountsWithTopic.contains(accountId)) {
				continue;
			}
			if (accountId != null) {
				accountsWithTopic.add(accountId);
			}
			String kind = outside ? "client" : "internal";
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, SHORT_NAME, "
						+ "DESCRIPTION, KIND, ACCOUNT_ID, KEYWORDS_JSON, STATUS, ORIGIN, SUGGEST_REASON, LAST_ACTIVITY_AT, "
						+ "CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType,
						topicId, c.name(), c.name(), description(c), kind, accountId, CollaborationDbUtils.toJson(c.keywords()),
						BrainTopicUtils.SUGGESTED, "brain", c.reason(), now, now, now);
				for (String p : members) {
					CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_TOPIC_PERSON (OWNER_ID, OWNER_TYPE, TOPIC_ID, "
							+ "PERSON_ID, STATE, ORIGIN, REASON, CHANGED_BY, CHANGED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
							ownerId, ownerType, topicId, p, "suggested", "brain", "On " + byPerson.get(p) + " of its threads",
							"brain", now);
				}
			});
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", topicId);
			row.put("name", c.name());
			row.put("kind", kind);
			row.put("accountId", accountId);
			row.put("reason", c.reason());
			row.put("threadIds", c.threads().stream().map(Thread::id).collect(Collectors.toList()));
			row.put("memberIds", members);
			row.put("sampleSubjects", c.threads().stream().map(Thread::subject).distinct().limit(3)
					.collect(Collectors.toList()));
			int mine = (int) c.threads().stream().filter(t -> writers.getOrDefault(t.id(), Set.of()).contains(self)).count();
			int withVip = (int) c.threads().stream().filter(t -> t.people().stream().anyMatch(vips::contains)).count();
			row.put("youWrote", mine);
			row.put("vipThreads", withVip);
			// pre-checked on screen: you took part, or a VIP is on it
			row.put("suggested", mine > 0 || withVip > 0);
			written.add(row);
		}

		out.put("topics", written);
		return out;
	}

	// what the classifier matches threads against: why the topic was suggested and a few of its subjects
	private static String description(Candidate c) {
		String examples = c.threads().stream().map(Thread::subject).filter(x -> x != null && !x.isBlank()).distinct()
				.limit(3).collect(Collectors.joining("; "));
		String why = c.reason() == null ? "" : c.reason().trim();
		return (why + (examples.isEmpty() ? "" : " Threads such as: " + examples + ".")).trim();
	}

	record Candidate(String key, String name, List<Thread> threads, List<String> keywords, String reason) {
	}

	// working threads first (a VIP on it, the owner wrote, more messages), then recent; to the model and back
	private static List<Candidate> modelCandidates(User user, String engineId, String ownerId, String ownerType,
			Map<String, Thread> threads, Map<String, Set<String>> writers, Set<String> vips, Map<String, String> emails,
			Map<String, String> accountByDomain, Map<String, String> accountNames, BrainOrgDomains.Org ownOrg, String self,
			Set<String> takenNames) {
		Map<String, Integer> counts = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, MESSAGE_COUNT FROM BRAIN_THREAD WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ?", rs -> counts.put(rs.getString(1), rs.getInt(2)), ownerId, ownerType);
		Set<String> organisations = new LinkedHashSet<>();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Thread t : threads.values()) {
			Set<String> orgs = new LinkedHashSet<>();
			for (String p : t.people()) {
				String d = org(BrainMailImport.domain(emails.get(p)));
				if (d != null && !ownOrg.isMine(d) && !FREEMAIL.contains(d)) {
					orgs.add(accountByDomain.containsKey(d) ? accountNames.get(accountByDomain.get(d)) : label(d));
				}
			}
			organisations.addAll(orgs);
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", t.id());
			row.put("subject", t.subject());
			row.put("orgs", new ArrayList<>(orgs));
			row.put("messages", counts.getOrDefault(t.id(), 1));
			row.put("you", self != null && writers.getOrDefault(t.id(), Set.of()).contains(self));
			row.put("vip", t.people().stream().anyMatch(vips::contains));
			rows.add(row);
		}
		// stable: recency stays the tie-break
		rows.sort((x, y) -> {
			int a = (Boolean.TRUE.equals(x.get("vip")) ? 2 : 0) + (Boolean.TRUE.equals(x.get("you")) ? 1 : 0);
			int b = (Boolean.TRUE.equals(y.get("vip")) ? 2 : 0) + (Boolean.TRUE.equals(y.get("you")) ? 1 : 0);
			return a != b ? b - a : (Integer) y.get("messages") - (Integer) x.get("messages");
		});
		List<Candidate> out = new ArrayList<>();
		for (BrainTopicModel.Proposal p : BrainTopicModel.propose(user, engineId, rows, new ArrayList<>(organisations),
				takenNames)) {
			List<Thread> list = p.threadIds().stream().map(threads::get).collect(Collectors.toList());
			List<String> keywords = List.of(p.name().toLowerCase().split("\\s+"));
			out.add(new Candidate("model:" + p.name().toLowerCase(), p.name(), list, keywords,
					p.why().isEmpty() ? list.size() + " threads grouped by the topic model" : p.why()));
		}
		return out;
	}

	// "Northwind Migration" from "[EXTERNAL] RE: Northwind Migration: the cutover plan"; "[Proj X] ..." too
	static String prefix(String subject) {
		if (subject == null) {
			return null;
		}
		String s = subject;
		for (int i = 0; i < 4; i++) {
			String before = s;
			s = REPLY.matcher(TAG.matcher(s).replaceFirst("")).replaceFirst("");
			if (s.equals(before)) {
				break;
			}
		}
		Matcher bracket = BRACKET.matcher(s);
		String prefix = bracket.find() ? bracket.group(1) : null;
		if (prefix == null) {
			Matcher m = PREFIX.matcher(s);
			prefix = m.find() ? m.group(1) : null;
		}
		if (prefix == null) {
			return null;
		}
		prefix = prefix.trim();
		int words = prefix.split("\\s+").length;
		return words >= 1 && words <= 5 && !REPLY.matcher(prefix + ":").find() ? prefix : null;
	}

	// the organisation's domain: mail.adatum.example is adatum.example
	static String org(String domain) {
		return BrainOrgDomains.org(domain);
	}

	// "northwind.example" to "Northwind"
	static String label(String domain) {
		String first = domain.contains(".") ? domain.substring(0, domain.indexOf('.')) : domain;
		return first.isEmpty() ? domain : Character.toUpperCase(first.charAt(0)) + first.substring(1);
	}
}
