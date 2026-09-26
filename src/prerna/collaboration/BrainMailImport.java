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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import prerna.auth.User;

// onboarding and Refresh: Inbox and Sent headers into people, threads, participants, and gated message rows.
// Every message goes through BrainRulesGate first; never-ingest senders and recipients leave no person or thread.
// Re-runnable: a message already linked to a thread is skipped.
public final class BrainMailImport {

	public static final String KIND = "import";
	public static final int MAX_DAYS = 180;
	private static final int MAX_PER_FOLDER = 5000;
	private static final String SOURCE = "email";
	private static final Pattern REPLY_PREFIX = Pattern.compile("^\\s*((re|fw|fwd|aw|wg)\\s*:\\s*)+",
			Pattern.CASE_INSENSITIVE);
	private static final Set<String> LINKED = Set.of(BrainRulesGate.INGESTED, BrainRulesGate.EXCLUDED,
			BrainRulesGate.MUTED);

	private BrainMailImport() {
	}

	/** Starts (or returns the running) import job for the last days of mail. */
	public static Map<String, Object> start(User user, int days) {
		if (days < 1 || days > MAX_DAYS) {
			throw new IllegalArgumentException("days must be 1 to " + MAX_DAYS);
		}
		var owner = CollaborationDbUtils.ownerOf(user);
		return CollaborationJobUtils.start(owner.getValue0(), owner.getValue1(), KIND, Map.of("days", days),
				job -> run(user, job, days));
	}

	static void run(User user, CollaborationJobUtils.Job job, int days) throws Exception {
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

		Run run = new Run(ownerId, ownerType);
		String selfId = run.ensureSelf(myAddress, (String) me.get("displayName"), (String) me.get("userPrincipalName"));

		Instant since = Instant.now().minus(Duration.ofDays(days));
		List<Map<String, Object>> headers = new ArrayList<>();
		for (String folder : BrainMailHeaderSource.FOLDERS) {
			job.step("reading " + folder, 5 + 10 * BrainMailHeaderSource.FOLDERS.indexOf(folder));
			headers.addAll(source.list(user, folder, since, MAX_PER_FOLDER));
		}
		// oldest first so a thread takes its first subject; a message sent to yourself is in both folders once
		headers.sort(Comparator.comparing(h -> String.valueOf(h.get("receivedDateTime"))));
		Map<String, Map<String, Object>> unique = new LinkedHashMap<>();
		for (Map<String, Object> h : headers) {
			unique.putIfAbsent(messageId(h), h);
		}
		job.count("messages", unique.size());

		int i = 0;
		for (Map<String, Object> header : unique.values()) {
			run.importOne(header);
			if (++i % 25 == 0) {
				job.count("imported", run.imported);
				job.step("importing", 25 + 60 * i / Math.max(1, unique.size()));
			}
		}
		job.count("imported", run.imported);
		job.count("alreadyImported", run.skipped);
		job.count("keptOut", run.keptOut);
		job.count("threads", run.threads.size());
		job.count("newPeople", run.newPeople);

		job.step("threads", 88);
		run.refreshThreads();
		job.step("people", 92);
		BrainPeopleRanking.rank(ownerId, ownerType, selfId, domain(myAddress));
		Map<String, Object> manager = source.manager(user);
		String managerAddress = manager == null ? null : BrainRulesGate.norm(first(manager, "mail", "userPrincipalName"));
		if (managerAddress != null) {
			job.count("managerPersonId", run.ensurePerson(managerAddress, (String) manager.get("displayName")));
		}
		CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, SOURCE);
	}

	// per-run state and caches
	private static final class Run {
		final String ownerId;
		final String ownerType;
		final List<BrainRulesGate.Rule> rules;
		final Map<String, String> people = new HashMap<>();
		final Set<String> threads = new LinkedHashSet<>();
		int imported;
		int skipped;
		int keptOut;
		int newPeople;

		Run(String ownerId, String ownerType) {
			this.ownerId = ownerId;
			this.ownerType = ownerType;
			this.rules = BrainRulesGate.activeRules(ownerId, ownerType);
		}

		@SuppressWarnings("unchecked")
		void importOne(Map<String, Object> header) {
			String from = address(header.get("from"));
			if (from == null) {
				return;
			}
			String messageId = messageId(header);
			String messageKey = CollaborationDbUtils.deterministicId(ownerId, ownerType, SOURCE, messageId);
			if (CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND MESSAGE_KEY = ? AND THREAD_ID IS NOT NULL", ownerId, ownerType, messageKey)) {
				skipped++;
				return;
			}
			String conversationId = (String) header.get("conversationId");
			Map<String, String> gate = new HashMap<>();
			gate.put("source", SOURCE);
			gate.put("messageId", messageId);
			gate.put("graphId", (String) header.get("id"));
			gate.put("conversationId", conversationId);
			gate.put("folderId", (String) header.get("parentFolderId"));
			gate.put("from", from);
			gate.put("receivedAt", (String) header.get("receivedDateTime"));
			Map<String, Object> decision = BrainRulesGate.check(ownerId, ownerType, gate);
			if (!LINKED.contains(decision.get("decision"))) {
				keptOut++;
				return;
			}

			Timestamp at = CollaborationDbUtils.toTimestamp(header.get("receivedDateTime"), "receivedDateTime");
			String threadKey = SOURCE + ":" + (conversationId != null ? conversationId : messageId);
			String threadId = ensureThread(threadKey, (String) header.get("subject"), at);
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
			String senderId = null;
			for (Map.Entry<String, Set<String>> entry : roles.entrySet()) {
				String address = entry.getKey();
				boolean isSender = address.equals(from);
				if (isSender && senderHidden) {
					senderId = (String) decision.get("personId");
					continue;
				}
				if (!isSender && BrainRulesGate.neverRule(rules, address, lookup(address), null) != null) {
					continue;
				}
				String personId = ensurePerson(address, names.get(address));
				if (isSender) {
					senderId = personId;
				}
				upsertParticipant(threadId, personId, entry.getValue(), at);
			}
			CollaborationDbUtils.update("UPDATE BRAIN_MESSAGE SET THREAD_ID = ?, SENDER_PERSON_ID = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?", threadId, senderId, ownerId, ownerType,
					messageKey);
			threads.add(threadId);
			imported++;
		}

		String ensureSelf(String address, String name, String upn) {
			String existing = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType, "self");
			String personId = existing != null ? existing : ensurePerson(address, name);
			if (existing == null) {
				CollaborationDbUtils.update("UPDATE BRAIN_PERSON SET RELATIONSHIP = ?, RELATIONSHIP_STATE = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", "self", "confirmed", ownerId,
						ownerType, personId);
			}
			addAddress(personId, address);
			String upnNorm = BrainRulesGate.norm(upn);
			if (upnNorm != null) {
				addAddress(personId, upnNorm);
				people.put(upnNorm, personId);
			}
			people.put(address, personId);
			return personId;
		}

		String ensurePerson(String address, String name) {
			String personId = lookup(address);
			if (personId != null) {
				return personId;
			}
			String id = "p-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "person", address);
			Timestamp now = CollaborationDbUtils.now();
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, EMAIL_NORM, "
						+ "DISPLAY_NAME, IS_VIP, NEVER_INGEST, STRENGTH, CREATED_AT, UPDATED_AT) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, id, address,
						name == null || name.isBlank() ? address : name.trim(), false, false, 0, now, now);
				CollaborationDbUtils.update(conn, "INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, "
						+ "KIND, VALUE_NORM, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)", ownerId, ownerType, id, "email",
						address, now);
			});
			people.put(address, id);
			newPeople++;
			return id;
		}

		// by any known address, then by the person's main email
		String lookup(String address) {
			String cached = people.get(address);
			if (cached != null) {
				return cached;
			}
			String id = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND VALUE_NORM = ?", rs -> rs.getString(1), ownerId, ownerType, address);
			if (id == null) {
				id = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND EMAIL_NORM = ?", rs -> rs.getString(1), ownerId, ownerType, address);
			}
			if (id != null) {
				people.put(address, id);
			}
			return id;
		}

		private void addAddress(String personId, String address) {
			if (!CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_PERSON_ADDRESS WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND PERSON_ID = ? AND VALUE_NORM = ?", ownerId, ownerType, personId, address)) {
				CollaborationDbUtils.update("INSERT INTO BRAIN_PERSON_ADDRESS (OWNER_ID, OWNER_TYPE, PERSON_ID, KIND, "
						+ "VALUE_NORM, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?)", ownerId, ownerType, personId, "email",
						address, CollaborationDbUtils.now());
			}
		}

		private String ensureThread(String threadKey, String subject, Timestamp at) {
			String existing = CollaborationDbUtils.queryOne("SELECT THREAD_ID FROM BRAIN_THREAD WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND THREAD_KEY = ?", rs -> rs.getString(1), ownerId, ownerType, threadKey);
			if (existing != null) {
				return existing;
			}
			String threadId = "th-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "thread", threadKey);
			CollaborationDbUtils.update("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, "
					+ "SUBJECT, MUTED, AUTOMATED, MESSAGE_COUNT, LAST_MESSAGE_AT, CREATED_AT) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, threadId, threadKey, SOURCE,
					cleanSubject(subject), false, false, 0, at, at);
			return threadId;
		}

		private void upsertParticipant(String threadId, String personId, Set<String> newRoles, Timestamp at) {
			Map<String, Object> row = CollaborationDbUtils.queryOne("SELECT ROLES_JSON, FIRST_SEEN_AT FROM "
					+ "BRAIN_THREAD_PARTICIPANT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
					rs -> {
						Map<String, Object> r = new HashMap<>();
						r.put("roles", CollaborationDbUtils.parseList(CollaborationDbUtils.getString(rs, "ROLES_JSON")));
						return r;
					}, ownerId, ownerType, threadId, personId);
			if (row == null) {
				CollaborationDbUtils.update("INSERT INTO BRAIN_THREAD_PARTICIPANT (OWNER_ID, OWNER_TYPE, THREAD_ID, "
						+ "PERSON_ID, ROLES_JSON, INCLUDED, HIDDEN_COUNT, FIRST_SEEN_AT, LAST_SEEN_AT) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, threadId, personId,
						CollaborationDbUtils.toJson(new ArrayList<>(newRoles)), true, 0, at, at);
				return;
			}
			Set<String> roles = new LinkedHashSet<>();
			for (Object role : (List<Object>) row.get("roles")) {
				roles.add(String.valueOf(role));
			}
			roles.addAll(newRoles);
			CollaborationDbUtils.update("UPDATE BRAIN_THREAD_PARTICIPANT SET ROLES_JSON = ?, LAST_SEEN_AT = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ? AND PERSON_ID = ?",
					CollaborationDbUtils.toJson(new ArrayList<>(roles)), at, ownerId, ownerType, threadId, personId);
		}

		// counts and latest time from the linked message rows
		void refreshThreads() {
			for (String threadId : threads) {
				Map<String, Object> agg = CollaborationDbUtils.queryOne("SELECT COUNT(*) AS N, MAX(RECEIVED_AT) AS LAST "
						+ "FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", rs -> {
							Map<String, Object> r = new HashMap<>();
							r.put("n", rs.getInt("N"));
							r.put("last", rs.getTimestamp("LAST"));
							return r;
						}, ownerId, ownerType, threadId);
				CollaborationDbUtils.update("UPDATE BRAIN_THREAD SET MESSAGE_COUNT = ?, LAST_MESSAGE_AT = ? "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?", agg.get("n"), agg.get("last"),
						ownerId, ownerType, threadId);
			}
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
