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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;

// onboarding and Refresh: Inbox and Sent headers into people, threads, participants, and gated message rows.
// Every message goes through BrainRulesGate first; never-ingest senders and recipients leave no person or thread.
// Re-runnable: a message already linked to a thread is skipped.
public final class BrainMailImport {

	public static final String KIND = "import";
	private static final Logger classLogger = LogManager.getLogger(BrainMailImport.class);
	public static final int MAX_DAYS = 180;
	private static final int MAX_PER_FOLDER = 5000;
	private static final int BATCH = 25;
	// more people than this on To and Cc is a mailing: no new people from it
	private static final int MAX_RECIPIENTS = 25;
	private static final String SOURCE = "email";
	private static final String TEAMS = "teams";
	private static final int MAX_CHATS = 300;
	private static final int MAX_PER_CHAT = 200;
	// which source a header came from; mail headers carry none
	private static final String SOURCE_KEY = "_source";
	private static final Pattern REPLY_PREFIX = Pattern.compile("^\\s*((re|fw|fwd|aw|wg)\\s*:\\s*)+",
			Pattern.CASE_INSENSITIVE);
	private static final Set<String> LINKED = Set.of(BrainRulesGate.INGESTED, BrainRulesGate.EXCLUDED,
			BrainRulesGate.MUTED);

	private BrainMailImport() {
	}

	/** Starts (or returns the running) import job for the last days of mail. */
	/**
	 * Also Teams chats: true turns them on, false off, null keeps the owner's
	 * setting.
	 */
	public static Map<String, Object> start(User user, int days, Boolean teams) {
		if (days < 1 || days > MAX_DAYS) {
			throw new IllegalArgumentException("days must be 1 to " + MAX_DAYS);
		}
		var owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> input = new LinkedHashMap<>();
		input.put("days", days);
		if (teams != null) {
			input.put("teams", teams);
		}
		return CollaborationJobUtils.start(owner.getValue0(), owner.getValue1(), KIND, input,
				job -> run(user, job, days, teams));
	}

	static void run(User user, CollaborationJobUtils.Job job, int days, Boolean teams) throws Exception {
		String ownerId = job.ownerId();
		String ownerType = job.ownerType();
		BrainMailHeaderSource source = BrainMailHeaderSource.current();

		job.step("mailbox", 2);
		Map<String, Object> me = source.me(user);
		String myAddress = BrainRulesGate.norm(first(me, "mail", "userPrincipalName"));
		if (myAddress == null) {
			throw new IllegalStateException("The signed-in mailbox has no address");
		}
		CollaborationSourceUtils.saveOwner(ownerId, ownerType, (String) me.get("id"),
				(String) me.get("userPrincipalName"), "active");
		// the owner starting an import is the consent to read email headers
		CollaborationSourceUtils.setSourceEnabled(ownerId, ownerType, SOURCE, true);
		// Teams chats only when the owner said so, now or earlier in Settings
		if (teams != null) {
			CollaborationSourceUtils.setSourceEnabled(ownerId, ownerType, TEAMS, teams);
		}
		boolean withTeams = Boolean.TRUE
				.equals(CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType).get(TEAMS));

		Run run = new Run(ownerId, ownerType);
		String selfId = run.ensureSelf(myAddress, (String) me.get("displayName"), (String) me.get("userPrincipalName"),
				source.aliases(user));

		Instant since = Instant.now().minus(Duration.ofDays(days));
		List<Map<String, Object>> headers = new ArrayList<>();
		for (String folder : BrainMailHeaderSource.FOLDERS) {
			job.step("reading " + folder, 5 + 10 * BrainMailHeaderSource.FOLDERS.indexOf(folder));
			headers.addAll(source.list(user, folder, since, MAX_PER_FOLDER));
		}
		// a chat failure (no Chat.Read yet, throttled) leaves the mail import whole
		String teamsError = null;
		if (withTeams) {
			job.step("reading Teams chats", 22);
			try {
				List<Map<String, Object>> chats = source.chats(user, since, MAX_CHATS, MAX_PER_CHAT);
				for (Map<String, Object> chat : chats) {
					chat.put(SOURCE_KEY, TEAMS);
				}
				headers.addAll(chats);
				job.count("teamsMessages", chats.size());
			} catch (Exception e) {
				teamsError = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
				classLogger.warn("Teams chats were not read for the import: {}", teamsError);
				job.count("teamsError", teamsError);
				CollaborationSourceUtils.recordSourceError(ownerId, ownerType, TEAMS, teamsError,
						teamsError.contains("403") || teamsError.contains("401"));
			}
		}
		// oldest first so a thread takes its first subject; a message sent to yourself
		// is in both folders once
		headers.sort(Comparator.comparing(h -> String.valueOf(h.get("receivedDateTime"))));
		Map<String, Map<String, Object>> unique = new LinkedHashMap<>();
		for (Map<String, Object> h : headers) {
			unique.putIfAbsent(messageId(h), h);
		}
		job.count("messages", unique.size());

		// one connection and one commit per batch; a failed batch rolls back and a
		// re-run picks it up
		List<Map<String, Object>> all = new ArrayList<>(unique.values());
		for (int start = 0; start < all.size(); start += BATCH) {
			List<Map<String, Object>> part = all.subList(start, Math.min(all.size(), start + BATCH));
			CollaborationDbUtils.batch(conn -> {
				for (Map<String, Object> header : part) {
					run.importOne(header);
				}
			});
			job.count("imported", run.imported);
			job.count("threads", run.threads.size());
			job.count("newPeople", run.newPeople);
			job.step("importing", 25 + 60 * (start + part.size()) / Math.max(1, all.size()));
		}
		job.count("imported", run.imported);
		job.count("alreadyImported", run.skipped);
		job.count("keptOut", run.keptOut);
		// calendar mail seen; 0 on a real mailbox means Graph gave no @odata.type
		job.count("meetingMessages", run.meetings);
		job.count("threads", run.threads.size());
		job.count("newPeople", run.newPeople);

		job.step("threads", 87);
		run.refreshThreads();

		// the directory first: who is a colleague, a shared mailbox or a list is a
		// fact, not a guess
		job.step("directory", 88);
		BrainOrgDomains.save(ownerId, ownerType, source.organization(user));
		Map<String, Object> manager = source.manager(user);
		String managerAddress = manager == null ? null
				: BrainRulesGate.norm(first(manager, "mail", "userPrincipalName"));
		String managerPersonId = managerAddress == null ? null
				: run.ensurePerson(managerAddress, (String) manager.get("displayName"));
		Map<String, String> chart = source.orgChart(user, manager == null ? null : (String) manager.get("id"));
		Set<String> check = new HashSet<>(chart.keySet());
		if (managerAddress != null) {
			check.add(managerAddress);
		}
		Map<String, Object> directory = BrainPeopleDirectory.apply(user, source, ownerId, ownerType, selfId, check);
		directory.forEach(job::count);

		job.step("people", 91);
		// everyone the directory did not settle is ranked; the classifier later marks
		// automated senders
		BrainPeopleRanking.rank(ownerId, ownerType, selfId, domain(myAddress));
		if (managerPersonId != null) {
			job.count("managerPersonId", managerPersonId);
		}
		// people to follow: manager, reports, peers, and two-way mail
		Map<String, String> org = new HashMap<>();
		if (managerPersonId != null) {
			org.put(managerPersonId, "Your manager");
		}
		chart.forEach((address, role) -> {
			String id = run.people.get(address);
			if (id != null && !id.equals(selfId)) {
				org.putIfAbsent(id, "report".equals(role) ? "Reports to you" : "Same manager");
			}
		});
		job.count("followSuggested", BrainFollow.suggest(ownerId, ownerType, selfId, org));
		CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, SOURCE);
		if (withTeams && teamsError == null) {
			CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, TEAMS);
		}
	}

	// per-run state, loaded once so a message costs only its own writes
	private static final class Run {
		final String ownerId;
		final String ownerType;
		final List<BrainRulesGate.Rule> rules;
		// address to person, thread key to [id, muted], message key to gate result,
		// "thread|person" to roles
		final Map<String, String> people = new HashMap<>();
		final Map<String, String[]> threadsByKey = new HashMap<>();
		final Map<String, Map<String, Object>> messages = new HashMap<>();
		final Map<String, Set<String>> participants = new HashMap<>();
		// the gate's lookups per source; only whether the source is on differs
		final Map<String, BrainRulesGate.Known> known = new HashMap<>();
		final Set<String> threads = new LinkedHashSet<>();
		final Set<String> selfAddresses = new HashSet<>();
		String selfId;
		String selfName;
		int imported;
		int skipped;
		int keptOut;
		int meetings;
		int newPeople;

		Run(String ownerId, String ownerType) {
			this.ownerId = ownerId;
			this.ownerType = ownerType;
			this.rules = BrainRulesGate.activeRules(ownerId, ownerType);
			CollaborationDbUtils.query(
					"SELECT PERSON_ID, EMAIL_NORM FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
							+ "AND EMAIL_NORM IS NOT NULL",
					rs -> people.putIfAbsent(rs.getString(2), rs.getString(1)), ownerId, ownerType);
			// a known address wins over a main email, as in the gate
			CollaborationDbUtils.query(
					"SELECT PERSON_ID, VALUE_NORM FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND "
							+ "OWNER_TYPE = ? ORDER BY PERSON_ID DESC",
					rs -> people.put(rs.getString(2), rs.getString(1)), ownerId, ownerType);
			CollaborationDbUtils.query(
					"SELECT THREAD_KEY, THREAD_ID, MUTED FROM BRAIN_THREAD WHERE OWNER_ID = ? AND " + "OWNER_TYPE = ?",
					rs -> threadsByKey.put(rs.getString(1),
							new String[] { rs.getString(2),
									String.valueOf(
											Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MUTED"))) }),
					ownerId, ownerType);
			CollaborationDbUtils.query(
					"SELECT MESSAGE_KEY, DECISION, RULE_ID, THREAD_ID, SENDER_PERSON_ID FROM "
							+ "BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					rs -> messages.put(rs.getString(1),
							BrainRulesGate.result(rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5))),
					ownerId, ownerType);
			CollaborationDbUtils.query("SELECT THREAD_ID, PERSON_ID, ROLES_JSON FROM BRAIN_THREAD_PARTICIPANT WHERE "
					+ "OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
						Set<String> roles = new LinkedHashSet<>();
						for (Object role : CollaborationDbUtils
								.parseList(CollaborationDbUtils.getString(rs, "ROLES_JSON"))) {
							roles.add(String.valueOf(role));
						}
						return participants.put(rs.getString(1) + "|" + rs.getString(2), roles);
					}, ownerId, ownerType);
		}

		BrainRulesGate.Known known(String source) {
			return known.computeIfAbsent(source, s -> {
				Boolean enabled = CollaborationDbUtils.queryOne(
						"SELECT ENABLED FROM SOURCE_CONNECTION WHERE OWNER_ID = ? "
								+ "AND OWNER_TYPE = ? AND SOURCE = ?",
						rs -> CollaborationDbUtils.getBoolean(rs, "ENABLED"), ownerId, ownerType, s);
				return new BrainRulesGate.Known(rules, Boolean.TRUE.equals(enabled), messages::get, people::get,
						threadsByKey::get);
			});
		}

		void importOne(Map<String, Object> header) {
			String from = address(header.get("from"));
			if (from == null) {
				return;
			}
			String source = header.get(SOURCE_KEY) instanceof String s ? s : SOURCE;
			String messageId = messageId(header);
			String messageKey = CollaborationDbUtils.deterministicId(ownerId, ownerType, source, messageId);
			Map<String, Object> prior = messages.get(messageKey);
			if (prior != null && prior.get("threadId") != null) {
				skipped++;
				return;
			}
			String conversationId = (String) header.get("conversationId");
			Map<String, String> gate = new HashMap<>();
			gate.put("source", source);
			gate.put("messageId", messageId);
			gate.put("graphId", (String) header.get("id"));
			gate.put("conversationId", conversationId);
			gate.put("folderId", (String) header.get("parentFolderId"));
			gate.put("from", from);
			gate.put("receivedAt", (String) header.get("receivedDateTime"));
			Map<String, Object> decision = BrainRulesGate.check(ownerId, ownerType, gate, known(source));
			messages.put(messageKey, decision);
			// an exclusion on a known thread wrote the sender's participant row
			if (BrainRulesGate.EXCLUDED.equals(decision.get("decision")) && decision.get("threadId") != null
					&& decision.get("personId") != null) {
				participants.putIfAbsent(decision.get("threadId") + "|" + decision.get("personId"),
						new LinkedHashSet<>(List.of("from")));
			}
			if (!LINKED.contains(decision.get("decision"))) {
				keptOut++;
				return;
			}

			Timestamp at = CollaborationDbUtils.toTimestamp(header.get("receivedDateTime"), "receivedDateTime");
			String threadKey = source + ":" + (conversationId != null ? conversationId : messageId);
			String threadId = ensureThread(threadKey, source, (String) header.get("subject"), at);
			boolean senderHidden = BrainRulesGate.EXCLUDED.equals(decision.get("decision"));

			// who was on it, by role; never-ingest recipients are left out entirely
			Map<String, Set<String>> roles = new LinkedHashMap<>();
			Map<String, String> names = new HashMap<>();
			addRole(roles, names, header.get("from"), "from");
			for (String field : List.of("toRecipients", "ccRecipients")) {
				if (header.get(field) instanceof List<?> list) {
					for (Object r : list) {
						addRole(roles, names, r, field.startsWith("to") ? "to" : "cc");
					}
				}
			}
			boolean fromMe = isSelf(from, names.get(from));
			// a big mailing adds no new people, only the sender and people already known
			boolean broadcast = roles.size() > MAX_RECIPIENTS;
			boolean toMe = false;
			String senderId = null;
			for (Map.Entry<String, Set<String>> entry : roles.entrySet()) {
				String address = entry.getKey();
				boolean isSender = address.equals(from);
				boolean self = isSelf(address, names.get(address));
				if (!isSender && self) {
					toMe = true;
				}
				if (isSender && senderHidden) {
					senderId = (String) decision.get("personId");
					continue;
				}
				if (!isSender && BrainRulesGate.neverRule(rules, address, people.get(address), null) != null) {
					continue;
				}
				if (broadcast && !isSender && !self && people.get(address) == null) {
					continue;
				}
				String personId = self ? selfId : ensurePerson(address, names.get(address));
				if (isSender) {
					senderId = personId;
				}
				upsertParticipant(threadId, personId, entry.getValue(), at);
			}
			// Focused Inbox "Other", sent on behalf of another mailbox, or a big mailing
			String sender = address(header.get("sender"));
			boolean bulk = "other".equals(header.get("inferenceClassification"))
					|| (sender != null && !sender.equals(from)) || broadcast;
			CollaborationDbUtils.update(
					"UPDATE BRAIN_MESSAGE SET THREAD_ID = ?, SENDER_PERSON_ID = ?, TO_ME = ?, BULK = ?, MEETING = ? "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?",
					threadId, senderId, fromMe ? null : toMe, bulk, meeting(header), ownerId, ownerType, messageKey);
			messages.put(messageKey, BrainRulesGate.result((String) decision.get("decision"),
					(String) decision.get("ruleId"), threadId, senderId));
			threads.add(threadId);
			imported++;
			if (meeting(header)) {
				meetings++;
			}
		}

		// the owner by any of their addresses, or by their exact display name on an
		// alias we do not know
		boolean isSelf(String address, String name) {
			return selfAddresses.contains(address)
					|| (selfName != null && name != null && selfName.equalsIgnoreCase(name.trim()));
		}

		String ensureSelf(String address, String name, String upn, List<String> aliases) {
			String existing = CollaborationDbUtils.queryOne(
					"SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? "
							+ "AND OWNER_TYPE = ? AND RELATIONSHIP = ?",
					rs -> rs.getString(1), ownerId, ownerType, "self");
			int before = newPeople;
			String personId = existing != null ? existing : ensurePerson(address, name);
			// the owner is not a new person
			newPeople = before;
			if (existing == null) {
				CollaborationDbUtils.update(
						"UPDATE BRAIN_PERSON SET RELATIONSHIP = ?, RELATIONSHIP_STATE = ? "
								+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
						"self", "confirmed", ownerId, ownerType, personId);
			}
			selfId = personId;
			selfName = name == null || name.isBlank() ? null : name.trim();
			List<String> all = new ArrayList<>(List.of(address));
			String upnNorm = BrainRulesGate.norm(upn);
			if (upnNorm != null) {
				all.add(upnNorm);
			}
			for (String alias : aliases) {
				String a = BrainRulesGate.norm(alias);
				// an alias already someone else's stays theirs
				if (a != null && (people.get(a) == null || personId.equals(people.get(a)))) {
					all.add(a);
				}
			}
			for (String a : all) {
				addAddress(personId, a);
				people.put(a, personId);
				selfAddresses.add(a);
			}
			return personId;
		}

		String ensurePerson(String address, String name) {
			String personId = people.get(address);
			if (personId != null) {
				return personId;
			}
			String id = "p-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "person", address);
			Timestamp now = CollaborationDbUtils.now();
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn,
						"INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, EMAIL_NORM, "
								+ "DISPLAY_NAME, IS_VIP, NEVER_INGEST, STRENGTH, CREATED_AT, UPDATED_AT) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, id, address, name == null || name.isBlank() ? address : name.trim(), false,
						false, 0, now, now);
				CollaborationDbUtils.update(conn,
						"INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, "
								+ "KIND, VALUE_NORM, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, id, "email", address, now);
			});
			people.put(address, id);
			newPeople++;
			return id;
		}

		private void addAddress(String personId, String address) {
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND PERSON_ID = ? AND VALUE_NORM = ?", ownerId, ownerType, personId, address)) {
				CollaborationDbUtils.update(
						"INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, KIND, "
								+ "VALUE_NORM, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, personId, "email", address, CollaborationDbUtils.now());
			}
		}

		private String ensureThread(String threadKey, String source, String subject, Timestamp at) {
			String[] existing = threadsByKey.get(threadKey);
			if (existing != null) {
				return existing[0];
			}
			String threadId = "th-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "thread", threadKey);
			CollaborationDbUtils.update(
					"INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, "
							+ "SUBJECT, MUTED, AUTOMATED, MESSAGE_COUNT, LAST_MESSAGE_AT, CREATED_AT) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					ownerId, ownerType, threadId, threadKey, source, cleanSubject(subject), false, false, 0, at, at);
			threadsByKey.put(threadKey, new String[] { threadId, "false" });
			return threadId;
		}

		private void upsertParticipant(String threadId, String personId, Set<String> newRoles, Timestamp at) {
			String key = threadId + "|" + personId;
			Set<String> roles = participants.get(key);
			if (roles == null) {
				CollaborationDbUtils.update(
						"INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, "
								+ "PERSON_ID, ROLES_JSON, INCLUDED, HIDDEN_COUNT, FIRST_SEEN_AT, LAST_SEEN_AT) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, threadId, personId, CollaborationDbUtils.toJson(new ArrayList<>(newRoles)),
						true, 0, at, at);
				participants.put(key, new LinkedHashSet<>(newRoles));
				return;
			}
			roles.addAll(newRoles);
			CollaborationDbUtils.update(
					"UPDATE BRAIN_THREAD_PARTICIPANT SET ROLES_JSON = ?, LAST_SEEN_AT = ? "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
					CollaborationDbUtils.toJson(new ArrayList<>(roles)), at, ownerId, ownerType, threadId, personId);
		}

		// counts and latest time from the linked message rows, in one query
		void refreshThreads() {
			Map<String, Object[]> agg = new HashMap<>();
			CollaborationDbUtils.query(
					"SELECT THREAD_ID, COUNT(*) AS N, MAX(RECEIVED_AT) AS LAST FROM BRAIN_MESSAGE "
							+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IS NOT NULL GROUP BY THREAD_ID",
					rs -> agg.put(rs.getString(1), new Object[] { rs.getInt("N"), rs.getTimestamp("LAST") }), ownerId,
					ownerType);
			CollaborationDbUtils.batch(conn -> {
				for (String threadId : threads) {
					Object[] a = agg.get(threadId);
					if (a != null) {
						CollaborationDbUtils.update(conn,
								"UPDATE BRAIN_THREAD SET MESSAGE_COUNT = ?, LAST_MESSAGE_AT = ? "
										+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?",
								a[0], a[1], ownerId, ownerType, threadId);
					}
				}
			});
		}
	}

	@SuppressWarnings("unchecked")
	private static void addRole(Map<String, Set<String>> roles, Map<String, String> names, Object recipient,
			String role) {
		String address = address(recipient);
		if (address == null) {
			return;
		}
		roles.computeIfAbsent(address, k -> new LinkedHashSet<>()).add(role);
		if (recipient instanceof Map<?, ?> r && r.get("emailAddress") instanceof Map<?, ?> e) {
			names.putIfAbsent(address, (String) ((Map<String, Object>) e).get("name"));
		}
	}

	// Graph recipient {emailAddress: {name, address}} to a normalized address
	static String address(Object recipient) {
		if (recipient instanceof Map<?, ?> r && r.get("emailAddress") instanceof Map<?, ?> e
				&& e.get("address") instanceof String a && !a.isBlank()) {
			return BrainRulesGate.norm(a);
		}
		return null;
	}

	// Graph types calendar mail as eventMessage (request, response, cancellation)
	static boolean meeting(Map<String, Object> message) {
		return message != null && message.get("@odata.type") instanceof String type
				&& type.startsWith("#microsoft.graph.eventMessage");
	}

	static String messageId(Map<String, Object> header) {
		Object id = header.get("internetMessageId");
		return id instanceof String s && !s.isBlank() ? s : (String) header.get("id");
	}

	static String cleanSubject(String subject) {
		if (subject == null) {
			return "(no subject)";
		}
		String clean = REPLY_PREFIX.matcher(subject).replaceFirst("").trim();
		clean = clean.isEmpty() ? "(no subject)" : clean;
		return clean.length() > 255 ? clean.substring(0, 255) : clean;
	}

	static String domain(String address) {
		int at = address == null ? -1 : address.lastIndexOf('@');
		return at < 0 ? null : address.substring(at + 1);
	}

	private static String first(Map<String, Object> map, String... keys) {
		for (String key : keys) {
			if (map != null && map.get(key) instanceof String s && !s.isBlank()) {
				return s;
			}
		}
		return null;
	}
}
