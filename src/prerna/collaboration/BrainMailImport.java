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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.regex.Matcher;
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
	// one read across every folder
	private static final int MAX_MAIL = 10000;
	private static final int BATCH = 25;
	// more people than this on To and Cc is a mailing: no new people from it
	private static final int MAX_RECIPIENTS = 25;
	private static final String SOURCE = "email";
	// a system routine's dated title, as each deliver node writes it (older runs wrote other cases): how results sent
	// before Collaboration recorded the mail it sends (BrainOwnMail) are known
	static final Pattern SYSTEM_RESULT_SUBJECT = Pattern.compile(
			"(?:Morning Work Briefing|Weekly Meeting Agendas|Event Planning) - \\d{4}-\\d{2}-\\d{2}",
			Pattern.CASE_INSENSITIVE);
	// the cleanup of Work raised before the keep-out also knows Expense Review results (and custom routines' names)
	static final Pattern ROUTINE_SUBJECT = Pattern.compile(SYSTEM_RESULT_SUBJECT.pattern() + "|Expense Review: .+",
			Pattern.CASE_INSENSITIVE);

	// a custom routine mails its result under the routine's name
	static Set<String> customRoutineNames(String ownerId, String ownerType) {
		Set<String> names = new HashSet<>();
		CollaborationDbUtils.query("SELECT SETTINGS_JSON FROM WORK_CUSTOM_ROUTINE WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> {
					try {
						Map<String, Object> settings = CollaborationDbUtils
								.parseMap(CollaborationDbUtils.getString(rs, "SETTINGS_JSON"));
						return settings != null && settings.get("name") instanceof String name && !name.isBlank()
								&& names.add(name.trim());
					} catch (RuntimeException e) {
						// one unreadable routine must not stop the import; its results are imported as before
						return false;
					}
				}, ownerId, ownerType);
		return names;
	}

	/**
	 * Dismisses the open Work Brain raised from routine results before imports kept them out: the generated steps,
	 * the asks, and the work items left with nothing to do, on a thread under a routine's subject that no one but its
	 * owner is on. Returns how many steps, asks and work items. Re-runnable; once done, nothing matches.
	 */
	static int[] dismissRoutineResultWork() {
		Map<String, String[]> threads = new LinkedHashMap<>();
		String thread = "SELECT t.OWNER_ID, t.OWNER_TYPE, t.THREAD_ID, t.SUBJECT FROM BRAIN_THREAD t JOIN ";
		String on = " ON w.OWNER_ID = t.OWNER_ID AND w.OWNER_TYPE = t.OWNER_TYPE AND w.THREAD_ID = t.THREAD_ID WHERE ";
		CollaborationDbUtils.RowMapper<String[]> row = rs -> {
			String[] t = { rs.getString(1), rs.getString(2), rs.getString(3), CollaborationDbUtils.getString(rs, "SUBJECT") };
			threads.putIfAbsent(t[0] + "|" + t[1] + "|" + t[2], t);
			return t;
		};
		CollaborationDbUtils.query(thread + "WORK_THREAD_STEP w" + on + "w.ORIGIN = ? AND w.STATUS IN (?, ?)", row,
				WorkItemUtils.BRAIN, WorkItemUtils.OPEN, WorkItemUtils.WAITING);
		CollaborationDbUtils.query(thread + "WORK_ITEM w" + on + "w.ORIGIN = ? AND w.STATUS IN (?, ?, ?)", row,
				WorkItemUtils.BRAIN, WorkItemUtils.OPEN, WorkItemUtils.WAITING, WorkItemUtils.SNOOZED);
		// work items Brain made from such steps, also those a cleanup before this one left open
		CollaborationDbUtils.query(thread + "WORK_THREAD_STEP w" + on + "w.ORIGIN = ? AND w.TASK_ID IN (SELECT "
				+ "k.TASK_ID FROM WORK_TASK k WHERE k.OWNER_ID = w.OWNER_ID AND k.OWNER_TYPE = w.OWNER_TYPE AND "
				+ "k.ORIGIN = ? AND k.STATUS IN (?, ?, ?))", row, WorkItemUtils.BRAIN, WorkItemUtils.BRAIN,
				WorkTaskUtils.ACTIVE, WorkTaskUtils.WAITING, WorkTaskUtils.BLOCKED);
		Map<String, Set<String>> names = new HashMap<>();
		int steps = 0;
		int items = 0;
		int tasks = 0;
		for (String[] t : threads.values()) {
			String subject = t[3] == null ? "" : t[3].trim();
			if (!ROUTINE_SUBJECT.matcher(subject).matches()
					&& !names.computeIfAbsent(t[0] + "|" + t[1], k -> customRoutineNames(t[0], t[1])).contains(subject)) {
				continue;
			}
			// anyone else on it makes it a conversation, whatever its subject
			if (CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_THREAD_PARTICIPANT p WHERE p.OWNER_ID = ? "
					+ "AND p.OWNER_TYPE = ? AND p.THREAD_ID = ? AND p.PERSON_ID NOT IN (SELECT PERSON_ID FROM BRAIN_PERSON "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND RELATIONSHIP = ?)", t[0], t[1], t[2], t[0], t[1],
					"self") > 0) {
				continue;
			}
			// as the owner deleting a generated step: kept, unlisted, never offered again
			steps += CollaborationDbUtils.update("UPDATE WORK_THREAD_STEP SET STATUS = ?, UPDATED_AT = ? WHERE OWNER_ID = ? "
					+ "AND OWNER_TYPE = ? AND THREAD_ID = ? AND ORIGIN = ? AND STATUS IN (?, ?)",
					WorkWorkspaceUtils.DISMISSED, CollaborationDbUtils.now(), t[0], t[1], t[2], WorkItemUtils.BRAIN,
					WorkItemUtils.OPEN, WorkItemUtils.WAITING);
			items += WorkItemUtils.dismissForRoutineResult(t[0], t[1], t[2]).size();
			// and a work item Brain grouped them under, once nothing on it is left to do
			Timestamp now = CollaborationDbUtils.now();
			tasks += CollaborationDbUtils.update("UPDATE WORK_TASK SET STATUS = ?, CLOSED_AT = ?, UPDATED_AT = ? WHERE "
					+ "OWNER_ID = ? AND OWNER_TYPE = ? AND ORIGIN = ? AND STATUS IN (?, ?, ?) AND TASK_ID IN (SELECT "
					+ "TASK_ID FROM WORK_THREAD_STEP WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID = ?) AND TASK_ID "
					+ "NOT IN (SELECT TASK_ID FROM WORK_THREAD_STEP WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND TASK_ID IS "
					+ "NOT NULL AND STATUS IN (?, ?, ?, ?))", WorkTaskUtils.DROPPED, now, now, t[0], t[1],
					WorkItemUtils.BRAIN, WorkTaskUtils.ACTIVE, WorkTaskUtils.WAITING, WorkTaskUtils.BLOCKED, t[0], t[1],
					t[2], t[0], t[1], WorkItemUtils.OPEN, WorkItemUtils.WAITING, "suggested", "draft_ready");
		}
		if (steps + items + tasks > 0) {
			classLogger.info("Dismissed {} steps, {} asks and {} work items raised from routine results", steps, items,
					tasks);
		}
		return new int[] { steps, items, tasks };
	}
	// BRAIN_PERSON.DISPLAY_NAME is VARCHAR(255)
	private static final int DISPLAY_NAME_CHARS = 255;
	// a run with more messages it could not save than this has a problem of its own, not one bad message
	private static final int MAX_UNSAVED = 5;
	private static final String TEAMS = "teams";
	private static final int MAX_CHATS = 300;
	private static final int MAX_PER_CHAT = 200;
	// which source a header came from; mail headers carry none
	private static final String SOURCE_KEY = "_source";
	// reply and forward prefixes, after any tags a mail gateway puts in front ("[EXT] Re: ..."), which stay
	private static final Pattern REPLY_PREFIX = Pattern.compile("^(\\s*(\\[[^\\]]*\\]\\s*)*)((re|fw|fwd|aw|wg)\\s*:\\s*)+",
			Pattern.CASE_INSENSITIVE);
	// one message id in a reply header: <local@domain>
	private static final Pattern MESSAGE_ID = Pattern.compile("<[^<>\\s]+>");
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
		// new mail arrives by itself once the import is done; in the background, skipped when Graph cannot reach us
		BrainMailWebhook.ensureSubscribed(user);
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

		List<String> aliases = source.aliases(user);
		String teamsError = null;
		Instant since = Instant.now().minus(Duration.ofDays(days));
		// Graph is read before the ingest lock is taken: a Send from a chat waits on it. Webhooks and chat pulls skip
		// while this job runs; the lock covers one that began just before it
		Mail mail = readMail(user, source, job, ownerId, ownerType, since, false, 5, 16);
		Run run;
		String selfId;
		String managerPersonId;
		synchronized (CollaborationDbUtils.ownerLock(BrainSync.INGEST_LOCK, ownerId, ownerType)) {
			run = new Run(ownerId, ownerType, false);
			selfId = run.ensureSelf(myAddress, (String) me.get("displayName"), (String) me.get("userPrincipalName"),
					aliases);
			// mail first, then who matters; the owner can go on to People while Teams chats are read
			importMail(job, run, mail, 16, 40);
			job.step("threads", 42);
			run.refreshThreads();
			managerPersonId = people(user, source, job, run, selfId, myAddress);
		}
		found(user, job, ownerId, ownerType, selfId, managerPersonId);
		job.count("mailReady", true);
		if (withTeams) {
			Chats chats = readTeams(user, source, job, ownerId, ownerType, since, 50, 83);
			teamsError = chats.error();
			synchronized (CollaborationDbUtils.ownerLock(BrainSync.INGEST_LOCK, ownerId, ownerType)) {
				// what a Send or the owner wrote while the chats were read
				run.load(false);
				importHeaders(job, run, chats.headers(), 83, 95);
				job.step("ranking", 96);
				run.refreshThreads();
				BrainPeopleRanking.rank(ownerId, ownerType, selfId, domain(myAddress));
			}
		}
		CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, SOURCE);
		if (withTeams && teamsError == null) {
			CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, TEAMS);
		}
	}

	// directory facts, then ranking and who to follow: manager, reports, peers, and two-way mail
	private static String people(User user, BrainMailHeaderSource source, CollaborationJobUtils.Job job, Run run,
			String selfId, String myAddress) throws Exception {
		String ownerId = run.ownerId;
		String ownerType = run.ownerType;
		// the directory first: who is a colleague, a shared mailbox or a list is a
		// fact, not a guess
		job.step("directory", 44);
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

		job.step("people", 47);
		// everyone the directory did not settle is ranked; the classifier later marks
		// automated senders
		BrainPeopleRanking.rank(ownerId, ownerType, selfId, domain(myAddress));
		if (managerPersonId != null) {
			job.count("managerPersonId", managerPersonId);
		}
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
		return managerPersonId;
	}

	// what the import found, for the owner to read while Teams chats are still coming in
	private static void found(User user, CollaborationJobUtils.Job job, String ownerId, String ownerType, String selfId,
			String managerPersonId) {
		if (managerPersonId != null) {
			job.count("managerName", CollaborationDbUtils.queryOne("SELECT COALESCE(DISPLAY_NAME, EMAIL_NORM) FROM "
					+ "BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", rs -> rs.getString(1),
					ownerId, ownerType, managerPersonId));
		}
		List<Map<String, Object>> top = CollaborationDbUtils.query(CollaborationDbUtils.page(
				"SELECT PERSON_ID, DISPLAY_NAME, EMAIL_NORM FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND PERSON_ID <> ? AND COALESCE(RELATIONSHIP, '') <> ? AND COALESCE(STRENGTH, 0) > 0 "
						+ "ORDER BY STRENGTH DESC, PERSON_ID",
				8, 0), rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("id", rs.getString(1));
					String email = Objects.toString(rs.getString(3), "");
					row.put("name", rs.getString(2) == null || rs.getString(2).isBlank() ? email : rs.getString(2));
					return row;
				}, ownerId, ownerType, selfId, BrainSenderTyping.AUTOMATED);
		job.count("topPeople", top);
		List<Map<String, Object>> outside = new ArrayList<>();
		try {
			Object accounts = BrainTopicSuggest.accounts(user).get("accounts");
			for (Object item : accounts instanceof List<?> list ? list : List.of()) {
				if (!(item instanceof Map<?, ?> account)) {
					continue;
				}
				if (outside.size() >= 4) {
					break;
				}
				outside.add(Map.of("name", account.get("name"), "people", account.get("people"), "threads",
						account.get("threads")));
			}
		} catch (RuntimeException e) {
			// the feed is a nicety; the import goes on without it
			classLogger.warn("Could not list outside organizations for the import feed", e);
		}
		job.count("outsideOrgs", outside);
	}

	// mail read for an import, before the ingest lock is taken (a Send and the webhook wait on it); cutoff, for a sync,
	// keeps out older mail Brain does not hold, which only the import can tell
	record Mail(List<Map<String, Object>> headers, BrainMailFolders.Tree tree, Instant cutoff) {
	}

	// Teams chat messages read for an import, and why some chats could not be read (null when all were)
	record Chats(List<Map<String, Object>> headers, String error) {
	}

	/**
	 * Every folder but the skipped ones (Deleted Items, Junk, Drafts, Outbox...) in one read, when the mailbox has
	 * folders; otherwise Inbox and Sent. changed (a sync) reads what was received or changed since: mail it already
	 * holds gets its folder, flag and categories brought up to date, older mail it does not hold stays out.
	 */
	static Mail readMail(User user, BrainMailHeaderSource source, CollaborationJobUtils.Job job, String ownerId,
			String ownerType, Instant since, boolean changed, int from, int to) throws Exception {
		job.step("reading folders", from);
		BrainMailFolders.Tree tree = changed ? BrainMailFolders.current(user, source, ownerId, ownerType)
				: java.util.Objects.requireNonNullElseGet(BrainMailFolders.refresh(user, source, ownerId, ownerType),
						() -> BrainMailFolders.load(ownerId, ownerType));
		if (!tree.isEmpty()) {
			job.step("reading mail", from + 1);
			List<Map<String, Object>> read;
			try {
				read = source.allMail(user, since, MAX_MAIL, changed);
			} catch (Exception e) {
				// a mailbox that will not filter on change time (Graph answers 400) still gives what arrived. Any other
				// failure (throttled, a reset) fails the sync, so the next one reads this window's moves and flags again
				if (!changed || e instanceof InterruptedException || !filterRejected(e)) {
					throw e;
				}
				classLogger.warn("Reading changed mail failed; reading new mail only: {}", e.getMessage());
				read = source.allMail(user, since, MAX_MAIL, false);
			}
			tree = BrainMailFolders.covering(user, source, ownerId, ownerType, tree, read);
			Instant cutoff = null;
			if (changed) {
				// older than what Brain took stays out; newer is mail moved out of a folder Brain skips (Not junk)
				Instant window = importWindow(ownerId, ownerType);
				cutoff = window != null && window.isBefore(since) ? window : since;
			}
			return new Mail(BrainMailFolders.keep(tree, read), tree, cutoff);
		}
		List<Map<String, Object>> headers = new ArrayList<>();
		int reading = (to - from) / BrainMailHeaderSource.FOLDERS.size();
		for (String folder : BrainMailHeaderSource.FOLDERS) {
			job.step("reading " + folder, from + reading * BrainMailHeaderSource.FOLDERS.indexOf(folder));
			// received mail carries the sender's own machine-sent headers; Sent is the owner's
			List<Map<String, Object>> read = BrainMailHeaderSource.SENT.equals(folder)
					? source.list(user, folder, since, MAX_PER_FOLDER)
					: source.topicHeaders(user, folder, since, MAX_PER_FOLDER);
			headers.addAll(read);
			job.count(BrainMailHeaderSource.SENT.equals(folder) ? "sentMessages" : "inboxMessages", read.size());
		}
		return new Mail(headers, tree, null);
	}

	// what readMail read, through the gate in batches; the caller holds the ingest lock
	static void importMail(CollaborationJobUtils.Job job, Run run, Mail mail, int from, int to) throws Exception {
		List<Map<String, Object>> headers = mail.headers();
		if (!mail.tree().isEmpty()) {
			if (mail.cutoff() != null) {
				headers.removeIf(h -> !run.holds(h) && !received(h, mail.cutoff()));
			}
			int inbox = 0;
			int sent = 0;
			Set<String> custom = new HashSet<>();
			for (Map<String, Object> h : headers) {
				String folder = (String) h.get("parentFolderId");
				if (mail.tree().sent(folder)) {
					sent++;
				} else if (mail.tree().custom(folder) != null) {
					custom.add(folder);
				} else {
					inbox++;
				}
			}
			job.count("inboxMessages", inbox);
			job.count("sentMessages", sent);
			job.count("folderMessages", headers.size() - inbox - sent);
			job.count("folders", custom.size());
		}
		importHeaders(job, run, headers, from, to);
	}

	// Teams chats since a time, counted as each chat is read. Keeps readable chats when individual chats fail; partial
	// Teams reads must be retried
	static Chats readTeams(User user, BrainMailHeaderSource source, CollaborationJobUtils.Job job, String ownerId,
			String ownerType, Instant teamsSince, int from, int to) throws Exception {
		String teamsError = null;
		job.count("teamsSince", teamsSince.toString());
		job.step("reading Teams chats", from);
		boolean reauthNeeded = false;
		List<Map<String, Object>> headers = new ArrayList<>();
		try {
			BrainMailHeaderSource.ChatImport chats = source.importChats(user, teamsSince, MAX_CHATS, MAX_PER_CHAT,
					(done, total) -> {
						job.count("teamsChatsRead", done);
						job.count("teamsChats", total);
						// a step write per few chats keeps the count live without a write per chat
						if (done == total || done % 5 == 0) {
							job.step("reading Teams chats", from + (to - from) * done / Math.max(1, total));
						}
					});
			for (Map<String, Object> chat : chats.messages()) {
				chat.put(SOURCE_KEY, TEAMS);
			}
			headers.addAll(chats.messages());
			job.count("teamsMessages", chats.messages().size());
			job.count("teamsChatsSkipped", chats.skippedChats());
			job.count("teamsChatsUnreadable", chats.unreadableChats());
			reauthNeeded = chats.reauthNeeded();
			if (chats.skippedChats() > 0) {
				teamsError = chats.skippedChats() + " Teams chat" + (chats.skippedChats() == 1 ? "" : "s")
						+ " could not be read. Readable chats were kept; failed chats will be retried on the next import.";
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		} catch (CancellationException e) {
			throw e;
		} catch (Exception e) {
			teamsError = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
			reauthNeeded = BrainMailHeaderSource.needsReauth(e);
		}
		job.count("teamsReauthNeeded", reauthNeeded);
		if (teamsError != null) {
			classLogger.warn("Teams import was incomplete: {}", teamsError);
			job.count("teamsError", teamsError);
			CollaborationSourceUtils.recordSourceError(ownerId, ownerType, TEAMS, teamsError, reauthNeeded);
		}
		return new Chats(headers, teamsError);
	}

	// oldest first so a thread takes its first subject; a message sent to yourself is in both folders once.
	// One connection and one commit per batch. A message that cannot be saved is undone alone and the batch keeps the
	// rest; a run with more of them than MAX_UNSAVED fails, its batch rolls back, and a re-run picks it up
	static void importHeaders(CollaborationJobUtils.Job job, Run run, List<Map<String, Object>> headers, int from,
			int to) throws Exception {
		headers.sort(Comparator.comparing(h -> String.valueOf(h.get("receivedDateTime"))));
		Map<String, Map<String, Object>> unique = new LinkedHashMap<>();
		for (Map<String, Object> h : headers) {
			unique.putIfAbsent(messageId(h), h);
		}
		run.seen += unique.size();
		job.count("messages", run.seen);
		List<Map<String, Object>> all = new ArrayList<>(unique.values());
		for (int start = 0; start < all.size(); start += BATCH) {
			List<Map<String, Object>> part = all.subList(start, Math.min(all.size(), start + BATCH));
			CollaborationDbUtils.batch(conn -> {
				for (Map<String, Object> header : part) {
					run.importIsolated(conn, header);
				}
			});
			job.count("imported", run.imported);
			job.count("threads", run.threads.size());
			job.count("newPeople", run.newPeople);
			job.step("importing", from + (to - from) * (start + part.size()) / Math.max(1, all.size()));
		}
		job.count("imported", run.imported);
		job.count("unsaved", run.unsaved);
		job.count("alreadyImported", run.skipped);
		job.count("keptOut", run.keptOut);
		job.count("ownMail", run.ownMail);
		// calendar mail seen; 0 on a real mailbox means Graph gave no @odata.type
		job.count("meetingMessages", run.meetings);
		job.count("threads", run.threads.size());
		job.count("newPeople", run.newPeople);
	}

	// per-run state, loaded once so a message costs only its own writes
	static final class Run {
		final String ownerId;
		final String ownerType;
		final List<BrainRulesGate.Rule> rules;
		// address to person, thread key to [id, muted], message key to gate result,
		// "thread|person" to roles
		final Map<String, String> people = new HashMap<>();
		final Map<String, String[]> threadsByKey = new HashMap<>();
		final Map<String, Map<String, Object>> messages = new HashMap<>();
		// message key to its stored folder, categories and flag, to see what changed in Outlook since
		final Map<String, String[]> mailbox = new HashMap<>();
		// threads with mail the owner moved to another folder since it was read
		final Set<String> refiled = new LinkedHashSet<>();
		final Map<String, Set<String>> participants = new HashMap<>();
		// the gate's lookups per source; only whether the source is on differs
		final Map<String, BrainRulesGate.Known> known = new HashMap<>();
		final Set<String> threads = new LinkedHashSet<>();
		// [threadId, sent time] of each owed message (new, or pending from a run cut off) the owner sent, so a sync can
		// close answered work
		final List<String[]> ownerSent = new ArrayList<>();
		// [messageKey, threadId, subject] of each owed message to the owner, from someone else or a note they sent
		// themselves, for the routines mail sets off
		final List<String[]> inbound = new ArrayList<>();
		// a sync, a chat's pull or the webhook, whose messages are owed the after-import steps (not the onboarding
		// import): each is marked SYNC_PENDING with its import, until BrainSync takes it
		final boolean followUps;
		// keys still marked by a run that was cut off: read again, such a message is owed its steps once more
		final Set<String> pending = new HashSet<>();
		// keys of the messages this run owes the after-import steps, new and pending ones
		final List<String> owed = new ArrayList<>();
		final Set<String> selfAddresses = new HashSet<>();
		String selfId;
		int imported;
		int skipped;
		int keptOut;
		// mail Collaboration sent the owner, never read back
		int ownMail;
		// keys of that mail when the run started; a message sent during the run is looked up
		final Set<String> sentByUs;
		int meetings;
		int newPeople;
		// unique headers read so far, mail and Teams
		int seen;
		// messages whose writes failed and were undone alone
		int unsaved;

		Run(String ownerId, String ownerType, boolean followUps) {
			this.ownerId = ownerId;
			this.ownerType = ownerType;
			this.followUps = followUps;
			this.rules = BrainRulesGate.activeRules(ownerId, ownerType);
			this.sentByUs = BrainOwnMail.keys(ownerId, ownerType);
			load(true);
		}

		// what the owner's Brain holds, read again after one message's writes are undone; the cut-off keys this run
		// already took stay taken, so none is owed its steps twice
		private void load(boolean withPending) {
			people.clear();
			threadsByKey.clear();
			messages.clear();
			mailbox.clear();
			participants.clear();
			known.clear();
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
			// conversations that joined a thread (joinReply)
			CollaborationDbUtils.query("SELECT a.THREAD_KEY, a.THREAD_ID, t.MUTED FROM BRAIN_THREAD_ALIAS a "
					+ "JOIN BRAIN_THREAD t ON t.OWNER_ID = a.OWNER_ID AND t.OWNER_TYPE = a.OWNER_TYPE "
					+ "AND t.THREAD_ID = a.THREAD_ID WHERE a.OWNER_ID = ? AND a.OWNER_TYPE = ?",
					rs -> threadsByKey.putIfAbsent(rs.getString(1),
							new String[] { rs.getString(2),
									String.valueOf(
											Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "MUTED"))) }),
					ownerId, ownerType);
			CollaborationDbUtils.query(
					"SELECT MESSAGE_KEY, DECISION, RULE_ID, THREAD_ID, SENDER_PERSON_ID, FOLDER, CATEGORIES_JSON, "
							+ "FLAGGED, SYNC_PENDING, GRAPH_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
					rs -> {
						mailbox.put(rs.getString(1), new String[] { rs.getString(6),
								CollaborationDbUtils.getString(rs, "CATEGORIES_JSON"),
								String.valueOf(Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "FLAGGED"))),
								rs.getString("GRAPH_ID") });
						if (withPending && Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "SYNC_PENDING"))) {
							pending.add(rs.getString(1));
						}
						return messages.put(rs.getString(1), BrainRulesGate.result(rs.getString(2), rs.getString(3),
								rs.getString(4), rs.getString(5)));
					}, ownerId, ownerType);
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

		/**
		 * One header inside the batch's transaction. When its writes fail on its own data they are undone alone, with
		 * what this run noted for it, and the batch keeps the rest; more than MAX_UNSAVED such messages in one run is
		 * not one bad message, and any other failure is the database's, so the run fails and its window is read again.
		 */
		void importIsolated(Connection conn, Map<String, Object> header) throws SQLException {
			Savepoint savepoint = conn.setSavepoint();
			int[] sizes = { ownerSent.size(), inbound.size(), owed.size(), threads.size(), refiled.size() };
			int[] counts = { imported, skipped, keptOut, meetings, newPeople };
			try {
				importOne(header);
			} catch (RuntimeException e) {
				conn.rollback(savepoint);
				// a deadlock or a lock timeout says nothing about this message: the run fails, so its window is read again
				if (!dataError(e) || ++unsaved > MAX_UNSAVED) {
					throw e;
				}
				classLogger.warn("Skipped message {}: it could not be saved, and the rest of its batch was kept",
						messageId(header), e);
				ownerSent.subList(sizes[0], ownerSent.size()).clear();
				inbound.subList(sizes[1], inbound.size()).clear();
				owed.subList(sizes[2], owed.size()).clear();
				keepFirst(threads, sizes[3]);
				keepFirst(refiled, sizes[4]);
				imported = counts[0];
				skipped = counts[1];
				keptOut = counts[2];
				meetings = counts[3];
				newPeople = counts[4];
				// the caches may name rows the rollback took back
				load(false);
			}
		}

		// the message's own fault (a value that does not fit, a clash with a row: SQLState classes 22 and 23, or no
		// database error at all) rather than the database's
		static boolean dataError(Throwable e) {
			for (Throwable cause = e; cause != null; cause = cause.getCause()) {
				if (cause instanceof SQLException sql) {
					String state = sql.getSQLState();
					return state != null && (state.startsWith("22") || state.startsWith("23"));
				}
			}
			return true;
		}

		// insertion order: what was added after the first size entries goes
		private static void keepFirst(Set<String> set, int size) {
			Iterator<String> it = set.iterator();
			for (int i = 0; it.hasNext(); i++) {
				it.next();
				if (i >= size) {
					it.remove();
				}
			}
		}

		// a mail header this run already holds a message for
		boolean holds(Map<String, Object> header) {
			String messageId = messageId(header);
			return messageId != null && messages
					.containsKey(CollaborationDbUtils.deterministicId(ownerId, ownerType, SOURCE, messageId));
		}

		// mail read before: its folder (and Graph id, which a move changes), categories and flag as Outlook has them now
		// mail older than what Work already read on its thread (a late webhook, mail moved out of Junk) moves the
		// thread's WORK_REF back to just before it; WorkPipeline.extract reads what is after WORK_REF
		private void lowerWorkRef(String threadId, String receivedAt) {
			Instant at;
			try {
				at = receivedAt == null ? null : Instant.parse(receivedAt);
			} catch (java.time.format.DateTimeParseException e) {
				return;
			}
			if (at != null) {
				// WORK_REF is a message's RECEIVED_AT as CollaborationDbUtils.toIso writes it, compared as text
				CollaborationDbUtils.update("UPDATE BRAIN_THREAD SET WORK_REF = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND THREAD_ID = ? AND WORK_REF >= ?", at.minusSeconds(1).toString(), ownerId, ownerType,
						threadId, at.toString());
			}
		}

		private void refresh(String messageKey, String threadId, Map<String, Object> header) {
			String[] stored = mailbox.get(messageKey);
			String folder = header.get("parentFolderId") instanceof String f ? f : null;
			String categories = BrainMailFolders.categories(header);
			boolean flagged = BrainMailFolders.flagged(header);
			// the id too: a message moved away and back between two syncs has a new one, and the old one answers 404
			if (stored == null || folder == null || (folder.equals(stored[0]) && Objects.equals(categories, stored[1])
					&& String.valueOf(flagged).equals(stored[2]) && Objects.equals(header.get("id"), stored[3]))) {
				return;
			}
			CollaborationDbUtils.update("UPDATE BRAIN_MESSAGE SET FOLDER = ?, GRAPH_ID = ?, CATEGORIES_JSON = ?, "
					+ "FLAGGED = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?", folder, header.get("id"),
					categories, flagged, ownerId, ownerType, messageKey);
			if (!folder.equals(stored[0])) {
				refiled.add(threadId);
			}
			mailbox.put(messageKey, new String[] { folder, categories, String.valueOf(flagged), (String) header.get("id") });
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
				if (SOURCE.equals(source)) {
					refresh(messageKey, (String) prior.get("threadId"), header);
				}
				// imported before these headers were read: fill them in once
				Boolean auto = autoSent(header);
				if (auto != null) {
					CollaborationDbUtils.update("UPDATE BRAIN_MESSAGE SET AUTO = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
							+ "AND MESSAGE_KEY = ? AND AUTO IS NULL", auto, ownerId, ownerType, messageKey);
				}
				skipped++;
				// imported by a run cut off before its after-import steps: this run owes them instead
				if (followUps && pending.remove(messageKey)) {
					Map<String, String> names = new HashMap<>();
					Map<String, Set<String>> roles = roles(header, names);
					owe(messageKey, (String) prior.get("threadId"), header, isSelf(from), toMe(roles));
				}
				return;
			}
			// mail Collaboration sent the owner: no thread, work, topic or routine trigger comes from it
			if (SOURCE.equals(source) && (sentByUs.contains(messageKey) || BrainOwnMail.marked(header)
					|| (selfAddresses.contains(from) && !BrainOwnMail.keys(ownerId, ownerType, messageKey).isEmpty()))) {
				ownMail++;
				return;
			}
			String conversationId = (String) header.get("conversationId");
			String threadKey = source + ":" + (conversationId != null ? conversationId : messageId);
			// before the gate, so it judges a reply on the thread the reply joins
			joinReply(threadKey, source, header);
			Map<String, String> gate = new HashMap<>();
			gate.put("source", source);
			gate.put("messageId", messageId);
			gate.put("graphId", (String) header.get("id"));
			gate.put("conversationId", conversationId);
			gate.put("folderId", (String) header.get("parentFolderId"));
			gate.put("from", from);
			gate.put("receivedAt", (String) header.get("receivedDateTime"));
			// ingested, each brief's own list came back as new Work, and the next brief listed it again
			if (routineResult(header)) {
				gate.put(BrainRulesGate.ROUTINE_RESULT, "true");
			}
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
			String threadId = ensureThread(threadKey, source, (String) header.get("subject"), at);
			boolean senderHidden = BrainRulesGate.EXCLUDED.equals(decision.get("decision"));

			// who was on it, by role; never-ingest recipients are left out entirely
			Map<String, String> names = new HashMap<>();
			Map<String, Set<String>> roles = roles(header, names);
			boolean fromMe = isSelf(from);
			// a big mailing adds no new people, only the sender and people already known
			boolean broadcast = roles.size() > MAX_RECIPIENTS;
			boolean toMe = toMe(roles);
			String senderId = null;
			for (Map.Entry<String, Set<String>> entry : roles.entrySet()) {
				String address = entry.getKey();
				boolean isSender = address.equals(from);
				boolean self = isSelf(address);
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
					|| (sender != null && !sender.equals(from)) || broadcast || toList(header);
			// marked in the same batch as the message, so a run cut off after it leaves its steps to the next sync
			CollaborationDbUtils.update(
					"UPDATE BRAIN_MESSAGE SET THREAD_ID = ?, SENDER_PERSON_ID = ?, TO_ME = ?, BULK = ?, MEETING = ?, "
							+ "AUTO = ?, CATEGORIES_JSON = ?, FLAGGED = ?, IMPORTANCE = ?, ATTACHMENTS = ?, "
							+ "SYNC_PENDING = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?",
					threadId, senderId, fromMe ? null : toMe, bulk, meeting(header), autoSent(header),
					BrainMailFolders.categories(header), BrainMailFolders.flagged(header),
					BrainMailFolders.importance(header), BrainMailFolders.attachments(header),
					followUps ? Boolean.TRUE : null, ownerId, ownerType, messageKey);
			lowerWorkRef(threadId, (String) header.get("receivedDateTime"));
			mailbox.put(messageKey, new String[] { (String) header.get("parentFolderId"),
					BrainMailFolders.categories(header), String.valueOf(BrainMailFolders.flagged(header)),
					(String) header.get("id") });
			messages.put(messageKey, BrainRulesGate.result((String) decision.get("decision"),
					(String) decision.get("ruleId"), threadId, senderId));
			owe(messageKey, threadId, header, fromMe, toMe);
			imported++;
			if (meeting(header)) {
				meetings++;
			}
		}

		// a result one of the owner's routines mailed them before Collaboration recorded the mail it sends: under a
		// system routine's dated title, to the owner alone, Bcc included. A subject alone never decides: a note to
		// self can carry a routine's name
		boolean routineResult(Map<String, Object> header) {
			String from = address(header.get("from"));
			if (from == null || !selfAddresses.contains(from)) {
				return false;
			}
			String subject = header.get("subject") instanceof String s ? s.trim() : "";
			if (!SYSTEM_RESULT_SUBJECT.matcher(subject).matches()) {
				return false;
			}
			for (String field : List.of("toRecipients", "ccRecipients", "bccRecipients")) {
				if (header.get(field) instanceof List<?> list) {
					for (Object recipient : list) {
						String address = address(recipient);
						if (address == null || !selfAddresses.contains(address)) {
							return false;
						}
					}
				}
			}
			return true;
		}

		// the owner by any of their addresses (ensureSelf adds the aliases), never by display name: a colleague with
		// the owner's name would close the owner's reply items, and their asks would be filed as the owner's
		boolean isSelf(String address) {
			return selfAddresses.contains(address);
		}

		// the owner is on To or Cc, so a note they sent themselves is to them too
		private boolean toMe(Map<String, Set<String>> roles) {
			for (Map.Entry<String, Set<String>> entry : roles.entrySet()) {
				if ((entry.getValue().contains("to") || entry.getValue().contains("cc")) && isSelf(entry.getKey())) {
					return true;
				}
			}
			return false;
		}

		// what a message brings to the after-import steps: its thread, the owner's reply, mail to the owner
		private void owe(String messageKey, String threadId, Map<String, Object> header, boolean fromMe,
				boolean toMe) {
			threads.add(threadId);
			if (followUps) {
				owed.add(messageKey);
			}
			if (fromMe) {
				ownerSent.add(new String[] { threadId, (String) header.get("receivedDateTime") });
			}
			if (!fromMe || toMe) {
				inbound.add(new String[] { messageKey, threadId, (String) header.get("subject") });
			}
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
			String displayName = displayName(name == null || name.isBlank() ? address : name.trim());
			CollaborationDbUtils.inTransaction(conn -> {
				CollaborationDbUtils.update(conn,
						"INSERT INTO BRAIN_PERSON (OWNER_ID, OWNER_TYPE, PERSON_ID, EMAIL_NORM, "
								+ "DISPLAY_NAME, IS_VIP, NEVER_INGEST, STRENGTH, CREATED_AT, UPDATED_AT) "
								+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						ownerId, ownerType, id, address, displayName, false, false, 0, now, now);
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

		// Outlook starts a new conversation for some replies (an outside mail client that leaves out its thread
		// header, a subject a mail gateway tagged): such a reply joins the thread of the email it answers, found by
		// its reply headers, and so does the rest of its conversation
		private void joinReply(String threadKey, String source, Map<String, Object> header) {
			if (threadsByKey.containsKey(threadKey)) {
				return;
			}
			for (String answered : repliesTo(header)) {
				Map<String, Object> message = messages
						.get(CollaborationDbUtils.deterministicId(ownerId, ownerType, source, answered));
				Object threadId = message == null ? null : message.get("threadId");
				if (threadId == null) {
					// A quick reply can reach the webhook before the sent copy has been imported.
					String[] outgoing = threadsByKey.get("message:" + source + ":"
							+ CollaborationDbUtils.deterministicId(ownerId, ownerType, source, answered));
					threadId = outgoing == null ? null : outgoing[0];
				}
				if (threadId == null) {
					continue;
				}
				for (String[] thread : threadsByKey.values()) {
					if (thread[0].equals(threadId)) {
						CollaborationDbUtils.update("INSERT INTO BRAIN_THREAD_ALIAS (OWNER_ID, OWNER_TYPE, THREAD_KEY, "
								+ "THREAD_ID, CREATED_AT) VALUES (?, ?, ?, ?, ?)", ownerId, ownerType, threadKey,
								thread[0], CollaborationDbUtils.now());
						threadsByKey.put(threadKey, thread);
						return;
					}
				}
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
			// a run cut off before this point left its threads uncounted, and the next one skips their mail as imported
			Set<String> counted = new LinkedHashSet<>(threads);
			counted.addAll(CollaborationDbUtils.query("SELECT THREAD_ID FROM BRAIN_THREAD WHERE OWNER_ID = ? AND "
					+ "OWNER_TYPE = ? AND (MESSAGE_COUNT IS NULL OR MESSAGE_COUNT = 0)", rs -> rs.getString(1), ownerId,
					ownerType));
			CollaborationDbUtils.batch(conn -> {
				for (String threadId : counted) {
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

	// who was on a message, by role (from, to, cc), and the names the header gives them
	private static Map<String, Set<String>> roles(Map<String, Object> header, Map<String, String> names) {
		Map<String, Set<String>> roles = new LinkedHashMap<>();
		addRole(roles, names, header.get("from"), "from");
		for (String field : List.of("toRecipients", "ccRecipients")) {
			if (header.get(field) instanceof List<?> list) {
				for (Object r : list) {
					addRole(roles, names, r, field.startsWith("to") ? "to" : "cc");
				}
			}
		}
		return roles;
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
	// the sending system says a machine sent it: Auto-Submitted (RFC 3834) other than "no", a List-Unsubscribe
	// link, or Precedence bulk/junk; null when the headers were not read
	static Boolean autoSent(Map<String, Object> header) {
		if (!(header.get("internetMessageHeaders") instanceof List<?> list)) {
			return null;
		}
		for (Object o : list) {
			String name = headerName(o);
			String value = headerValue(o);
			if (("auto-submitted".equals(name) && !value.isEmpty() && !"no".equals(value))
					|| "list-unsubscribe".equals(name)
					|| ("precedence".equals(name) && ("bulk".equals(value) || "junk".equals(value)))) {
				return true;
			}
		}
		return false;
	}

	// posted to a mailing list (List-Id, Precedence list): a hint only, people write to lists too
	static boolean toList(Map<String, Object> header) {
		if (header.get("internetMessageHeaders") instanceof List<?> list) {
			for (Object o : list) {
				if ("list-id".equals(headerName(o)) || ("precedence".equals(headerName(o)) && "list".equals(headerValue(o)))) {
					return true;
				}
			}
		}
		return false;
	}

	private static String headerName(Object h) {
		return h instanceof Map<?, ?> m && m.get("name") instanceof String n ? n.trim().toLowerCase(Locale.ROOT) : "";
	}

	private static String headerValue(Object h) {
		return h instanceof Map<?, ?> m && m.get("value") instanceof String v ? v.trim().toLowerCase(Locale.ROOT) : "";
	}

	static boolean meeting(Map<String, Object> message) {
		return message != null && message.get("@odata.type") instanceof String type
				&& type.startsWith("#microsoft.graph.eventMessage");
	}

	// the message ids an email answers, nearest first: In-Reply-To, then References from the newest
	static List<String> repliesTo(Map<String, Object> header) {
		String inReplyTo = null;
		String references = null;
		if (header.get("internetMessageHeaders") instanceof List<?> list) {
			for (Object h : list) {
				// as written: message ids are case sensitive, so not headerValue
				String value = h instanceof Map<?, ?> m && m.get("value") instanceof String v ? v : null;
				if ("in-reply-to".equals(headerName(h))) {
					inReplyTo = value;
				} else if ("references".equals(headerName(h))) {
					references = value;
				}
			}
		}
		Set<String> ids = new LinkedHashSet<>(messageIds(inReplyTo));
		List<String> referenced = messageIds(references);
		Collections.reverse(referenced);
		ids.addAll(referenced);
		return new ArrayList<>(ids);
	}

	private static List<String> messageIds(String header) {
		List<String> ids = new ArrayList<>();
		Matcher id = header == null ? null : MESSAGE_ID.matcher(header);
		while (id != null && id.find()) {
			ids.add(id.group());
		}
		return ids;
	}

	// received at or after since; a header without a time counts as new
	// Graph would not take the request as asked: HttpHelperUtility reports a status as "... returned HTTP 400"
	static boolean filterRejected(Exception e) {
		return e.getMessage() != null && e.getMessage().contains(" returned HTTP 400");
	}

	// the oldest mail Brain's import took (the latest finished import's start less its days), so mail moved out of
	// Junk or Deleted Items after a sync's window is still imported; null when no import says
	static Instant importWindow(String ownerId, String ownerType) {
		return CollaborationDbUtils.queryOne(CollaborationDbUtils.page("SELECT PARAMS_JSON, STARTED_AT FROM COLLAB_JOB "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = ? AND STATUS = ? ORDER BY STARTED_AT DESC", 1, 0),
				rs -> {
					Map<String, Object> params = CollaborationDbUtils
							.parseMap(CollaborationDbUtils.getString(rs, "PARAMS_JSON"));
					java.sql.Timestamp started = rs.getTimestamp("STARTED_AT");
					if (started == null || params == null || !(params.get("days") instanceof Number days)) {
						return null;
					}
					// stored times are UTC wall time
					return started.toLocalDateTime().toInstant(java.time.ZoneOffset.UTC)
							.minus(Duration.ofDays(days.longValue()));
				}, ownerId, ownerType, KIND, CollaborationJobUtils.DONE);
	}

	private static boolean received(Map<String, Object> header, Instant since) {
		try {
			return !(header.get("receivedDateTime") instanceof String at) || !Instant.parse(at).isBefore(since);
		} catch (java.time.format.DateTimeParseException e) {
			return true;
		}
	}

	static String messageId(Map<String, Object> header) {
		Object id = header.get("internetMessageId");
		return id instanceof String s && !s.isBlank() ? s : (String) header.get("id");
	}

	static String cleanSubject(String subject) {
		if (subject == null) {
			return "(no subject)";
		}
		String clean = subject;
		// "RE: [EXT] Re: x" takes two passes
		for (String before = null; !clean.equals(before);) {
			before = clean;
			clean = REPLY_PREFIX.matcher(clean).replaceFirst("$1");
		}
		clean = clean.trim();
		clean = clean.isEmpty() ? "(no subject)" : clean;
		return clean.length() > 255 ? clean.substring(0, 255) : clean;
	}

	// a sender picks their own display name (a directory allows 256 characters): one longer than DISPLAY_NAME's 255
	// would roll back its batch, and every later sync of the same window, so it is cut, never a pair split
	static String displayName(String name) {
		if (name.length() <= DISPLAY_NAME_CHARS) {
			return name;
		}
		return name.substring(0,
				Character.isHighSurrogate(name.charAt(DISPLAY_NAME_CHARS - 1)) ? DISPLAY_NAME_CHARS - 1 : DISPLAY_NAME_CHARS);
	}

	static String domain(String address) {
		int at = address == null ? -1 : address.lastIndexOf('@');
		return at < 0 ? null : address.substring(at + 1);
	}

	static String first(Map<String, Object> map, String... keys) {
		for (String key : keys) {
			if (map != null && map.get(key) instanceof String s && !s.isBlank()) {
				return s;
			}
		}
		return null;
	}
}
