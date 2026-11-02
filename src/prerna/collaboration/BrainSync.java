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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;

// A sync: mail and Teams since the last finished import or sync, through the same gate and classifier as onboarding,
// without its setup steps, then queues the summaries and action items of the threads with new mail
// (WorkThreadInsights). Refresh and the open page (every minute) start one, and so does the schedule here, for each
// owner who saved background sign-in, as often as they chose on the Connections page (15 minutes unless they changed
// it, or never), so mail is filed while their browser is closed. The Collaboration webhook comes later. One sync job
// per owner at a time.
public final class BrainSync {

	private static final Logger classLogger = LogManager.getLogger(BrainSync.class);

	public static final String KIND = "sync";
	// re-read a little before the last run started; linked messages are skipped and the gate replays its decisions
	private static final Duration OVERLAP = Duration.ofMinutes(15);
	private static final String EMAIL = "email";
	private static final String TEAMS = "teams";
	private static final String NEW = "new";
	private static final String UPDATED = "updated";
	private static final String CLEARED = "cleared";
	private static final String AUTOMATED = "automated";
	private static final String QUIET = "quiet";
	// threads listed on the job for the page; counts cover all of them
	private static final int MAX_CHANGES = 50;
	// one import into an owner's Brain at a time on this server (a sync, the onboarding import, a webhook, a chat's
	// pull): a Run dedupes only against what it loaded when it was built, and the tables have no unique keys
	static final String INGEST_LOCK = "ingest";
	// a chat's pull of new mail: at most this many headers a folder, once every PULL_EVERY an owner and purpose, and
	// no further back than PULL_REACH; when each purpose last tried, and per owner when a pull last read the mailbox
	private static final int PULL_MAX = 200;
	// under the chat's 30 s check, so request jitter does not skip every other one
	private static final Duration PULL_EVERY = Duration.ofSeconds(25);
	private static final Duration PULL_REACH = Duration.ofDays(2);
	private static final Map<String, Instant> PULL_TRIED = new ConcurrentHashMap<>();
	private static final Map<String, Instant> PULL_READ = new ConcurrentHashMap<>();
	// purposes paced apart, so a chat's polling never uses up the pull of the owner's next turn
	static final String PULL_FOR_RUN = "run";
	static final String PULL_FOR_READ = "read";
	// the schedule looks every TICK for owners with a saved background sign-in whose own interval
	// (BRAIN_SETTINGS.SYNC_EVERY_MINUTES) has passed since their last sync of any kind; an open page syncs every
	// minute, so in practice it syncs them while their browser is closed
	private static final Duration TICK = Duration.ofMinutes(5);
	private static final Duration FIRST = Duration.ofMinutes(5);
	// one scheduled sync at a time leaves the two job threads to imports and Refresh; a longer one goes on alone
	private static final Duration WAIT_AT_MOST = Duration.ofMinutes(30);
	private static final long WAIT_STEP_MS = 5_000;
	static final String SIGN_IN_EXPIRED = "Microsoft sign-in expired. Sign in again to keep mail syncing.";
	private static final AtomicBoolean SCHEDULED = new AtomicBoolean();
	private static final ScheduledExecutorService SCHEDULE = Executors.newSingleThreadScheduledExecutor(runnable -> {
		Thread thread = new Thread(runnable, "collaboration-sync-schedule");
		thread.setDaemon(true);
		return thread;
	});

	private BrainSync() {
	}

	/** Starts (or returns the running) sync job; fails when onboarding has not imported mail yet. */
	public static Map<String, Object> start(User user) {
		var owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		if (!Boolean.TRUE.equals(CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType).get(EMAIL))) {
			throw new IllegalArgumentException("Mail is not connected yet; finish the onboarding import first");
		}
		Map<String, Object> importing = CollaborationJobUtils.latest(ownerId, ownerType, BrainMailImport.KIND);
		if (importing != null && CollaborationJobUtils.RUNNING.equals(importing.get("status"))) {
			throw new IllegalStateException("An import is running; refresh when it finishes");
		}
		Timestamp last = lastRun(ownerId, ownerType);
		if (last == null) {
			throw new IllegalArgumentException("Nothing has been imported yet; finish the onboarding import first");
		}
		// fail here, not inside the job, when no model is set or the caller cannot use it
		BrainThreadClassifier.requireEngine(user);
		// from then on new mail also arrives by itself; in the background, skipped when Graph cannot reach this server
		BrainMailWebhook.ensureSubscribed(user);
		keepBackgroundCurrent(user);
		return CollaborationJobUtils.start(ownerId, ownerType, KIND, Map.of(), job -> {
			try {
				run(user, job, last);
			} catch (Exception e) {
				// the mail source says so (Needs sign-in); the failed run leaves lastRun, so its window is read again
				if (signInExpired(e)) {
					CollaborationSourceUtils.recordSourceError(ownerId, ownerType, EMAIL, SIGN_IN_EXPIRED, true);
				}
				throw e;
			}
		});
	}

	static void run(User user, CollaborationJobUtils.Job job, Timestamp last) throws Exception {
		String ownerId = job.ownerId();
		String ownerType = job.ownerType();
		BrainMailHeaderSource source = BrainMailHeaderSource.current();
		Timestamp startedAt = CollaborationDbUtils.now();

		job.step("mailbox", 2);
		Map<String, Object> me = source.me(user);
		String myAddress = BrainRulesGate.norm(BrainMailImport.first(me, "mail", "userPrincipalName"));
		if (myAddress == null) {
			throw new IllegalStateException("The signed-in mailbox has no address");
		}
		List<String> aliases = source.aliases(user);

		Instant floor = Instant.now().minus(Duration.ofDays(BrainMailImport.MAX_DAYS));
		Instant since = later(toInstant(last).minus(OVERLAP), floor);
		boolean withTeams = Boolean.TRUE
				.equals(CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType).get(TEAMS));
		job.count("since", since.toString());
		BrainMailImport.Run run;
		String teamsError;
		synchronized (CollaborationDbUtils.ownerLock(INGEST_LOCK, ownerId, ownerType)) {
			run = new BrainMailImport.Run(ownerId, ownerType, true);
			run.ensureSelf(myAddress, (String) me.get("displayName"), (String) me.get("userPrincipalName"), aliases);
			teamsError = BrainMailImport.importSince(user, source, job, run, since,
					withTeams ? teamsSince(ownerId, ownerType, since, floor) : null);
			job.step("threads", 86);
			run.refreshThreads();
		}

		// mail the owner moved into a folder a topic stands for is filed under that topic
		job.count("filedByFolder", BrainThreadClassifier.fileByFolder(ownerId, ownerType, run.refiled));

		// the owner answered: reply items received before the send are done
		job.step("replies", 88);
		Set<String> closed = new LinkedHashSet<>();
		for (String[] sent : run.ownerSent) {
			closed.addAll(WorkItemUtils.closeOnReply(ownerId, ownerType, sent[0], sent[1]));
		}
		job.count("closedByReply", closed.size());

		// every thread with mail since the last finished run, so one cut off before classifying is picked up here
		Set<String> touched = new LinkedHashSet<>(run.threads);
		touched.addAll(threadsSince(ownerId, ownerType, last));
		job.count("toClassify", touched.size());
		Map<String, String> verdicts = new HashMap<>();
		if (!touched.isEmpty()) {
			job.step("classifying", 90);
			// own insight so the run does not depend on the page that started it
			Insight insight = new Insight();
			insight.setUser(user);
			Map<String, Object> summary = BrainThreadClassifier.classify(user, insight, new ArrayList<>(touched),
					false, (done, total) -> job.step("classifying", 90 + 9 * done / Math.max(1, total)));
			job.count("work", summary.get("work"));
			job.count("topics", summary.get("topics"));
			job.count("classifyErrors", summary.get("errors"));
			if (summary.get("results") instanceof List<?> rows) {
				for (Object row : rows) {
					if (row instanceof Map<?, ?> r && r.get("threadId") instanceof String id
							&& r.get("work") instanceof String work) {
						verdicts.put(id, work);
					}
				}
			}
		}
		outcomes(job, ownerId, ownerType, touched, verdicts, startedAt);

		// summaries and action items for the threads with new mail, on their own pool: the sync does not wait
		Set<String> summarize = new LinkedHashSet<>(touched);
		summarize.removeIf(id -> AUTOMATED.equals(verdicts.get(id)) || "skipped".equals(verdicts.get(id)));
		WorkThreadInsights.queue(user, ownerId, ownerType, summarize);
		job.count("summarizing", summarize.size());
		job.count("resumed", run.owed.size() - run.imported);
		finishFollowUps(ownerId, ownerType, run);

		CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, EMAIL);
		if (withTeams && teamsError == null) {
			CollaborationSourceUtils.recordSourceEvent(ownerId, ownerType, TEAMS);
		}
	}

	/**
	 * New mail since Brain last read the mailbox, straight from Graph, so a chat's assistant sees what came after the
	 * last sync: Inbox and Sent through the same gate as a sync, where a reply Outlook filed in a conversation of its
	 * own still joins the thread it answers. At most once every 25 seconds an owner and purpose (PULL_FOR_RUN,
	 * PULL_FOR_READ), never while a sync runs (it brings the mail in itself), and no further back than two days (older
	 * mail waits for the next sync, whose window this does not move). Returns the threads that got mail.
	 */
	public static Set<String> pullNewMail(User user) throws Exception {
		var owner = CollaborationDbUtils.ownerOf(user);
		return pullNewMail(user, owner.getValue0(), owner.getValue1(), BrainMailHeaderSource.current(), Instant.now(),
				PULL_FOR_READ);
	}

	static Set<String> pullNewMail(User user, String ownerId, String ownerType, BrainMailHeaderSource source,
			Instant now, String purpose) throws Exception {
		String owner = ownerType + ":" + ownerId;
		if (!pullDue(purpose, ownerId, ownerType, now) || !canIngest(ownerId, ownerType)) {
			return Set.of();
		}
		// a sync holds the ingest lock for minutes; not marked as tried, so the first pull after it runs
		Map<String, Object> syncing = CollaborationJobUtils.latest(ownerId, ownerType, KIND);
		if (syncing != null && CollaborationJobUtils.RUNNING.equals(syncing.get("status"))) {
			return Set.of();
		}
		PULL_TRIED.put(purpose + "|" + owner, now);
		// from the last pull that read the mailbox, else the last import or sync
		Instant read = PULL_READ.get(owner);
		Instant last = toInstant(lastRun(ownerId, ownerType));
		Instant since = later((read != null && read.isAfter(last) ? read : last).minus(OVERLAP),
				now.minus(PULL_REACH));
		List<Map<String, Object>> headers;
		if (BrainMailFolders.load(ownerId, ownerType).isEmpty()) {
			headers = new ArrayList<>(source.topicHeaders(user, BrainMailHeaderSource.INBOX, since, PULL_MAX));
			headers.addAll(source.list(user, BrainMailHeaderSource.SENT, since, PULL_MAX));
		} else {
			// every folder the owner's Outlook rules may have filed new mail in
			headers = source.allMail(user, since, 2 * PULL_MAX, false);
		}
		Set<String> threads = importHeaders(user, ownerId, ownerType, source, headers);
		PULL_READ.put(owner, now);
		return threads;
	}

	// whether a pull for this purpose may read the mailbox now; callers check it before handing one to a pool
	static boolean pullDue(String purpose, String ownerId, String ownerType, Instant now) {
		Instant tried = PULL_TRIED.get(purpose + "|" + ownerType + ":" + ownerId);
		return tried == null || !tried.plus(PULL_EVERY).isAfter(now);
	}

	/**
	 * Imports the headers Brain does not hold yet, as a sync would: the gate's rules, people and threads; the
	 * owner's own replies close their reply items and the threads' summaries are queued. Nothing happens before the
	 * onboarding import or while one runs. The next sync classifies these threads. Returns the threads that got mail.
	 */
	static Set<String> importHeaders(User user, String ownerId, String ownerType, BrainMailHeaderSource source,
			List<Map<String, Object>> headers) throws Exception {
		Map<String, Map<String, Object>> unseen = new LinkedHashMap<>();
		for (Map<String, Object> header : headers) {
			String messageId = BrainMailImport.messageId(header);
			if (messageId != null) {
				unseen.putIfAbsent(CollaborationDbUtils.deterministicId(ownerId, ownerType, EMAIL, messageId), header);
			}
		}
		// usually nothing is new: one query and no Run, which loads the owner's whole Brain
		if (!unseen.isEmpty()) {
			List<Object> params = new ArrayList<>(List.of(ownerId, ownerType));
			params.addAll(unseen.keySet());
			CollaborationDbUtils.query("SELECT MESSAGE_KEY FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
					+ "AND MESSAGE_KEY IN (" + CollaborationDbUtils.placeholders(unseen.size()) + ")",
					rs -> unseen.remove(rs.getString("MESSAGE_KEY")), params.toArray());
		}
		if (unseen.isEmpty() || !canIngest(ownerId, ownerType)) {
			return Set.of();
		}
		// no drafts, Deleted Items or Junk: a webhook on all mail hears about those too
		BrainMailFolders.Tree tree = BrainMailFolders.current(user, source, ownerId, ownerType);
		tree = BrainMailFolders.covering(user, source, ownerId, ownerType, tree, new ArrayList<>(unseen.values()));
		List<Map<String, Object>> kept = BrainMailFolders.keep(tree, new ArrayList<>(unseen.values()));
		if (kept.isEmpty()) {
			return Set.of();
		}
		Map<String, Object> me = source.me(user);
		String myAddress = BrainRulesGate.norm(BrainMailImport.first(me, "mail", "userPrincipalName"));
		if (myAddress == null) {
			throw new IllegalStateException("The signed-in mailbox has no address");
		}
		List<String> aliases = source.aliases(user);
		List<Map<String, Object>> fresh = new ArrayList<>(kept);
		// oldest first, so a new thread takes its first subject
		fresh.sort(Comparator.comparing(header -> String.valueOf(header.get("receivedDateTime"))));
		BrainMailImport.Run run;
		synchronized (CollaborationDbUtils.ownerLock(INGEST_LOCK, ownerId, ownerType)) {
			run = new BrainMailImport.Run(ownerId, ownerType, true);
			run.ensureSelf(myAddress, (String) me.get("displayName"), (String) me.get("userPrincipalName"), aliases);
			CollaborationDbUtils.batch(conn -> {
				for (Map<String, Object> header : fresh) {
					run.importOne(header);
				}
			});
			run.refreshThreads();
		}
		for (String[] sent : run.ownerSent) {
			WorkItemUtils.closeOnReply(ownerId, ownerType, sent[0], sent[1]);
		}
		WorkThreadInsights.queue(user, ownerId, ownerType, run.threads);
		finishFollowUps(ownerId, ownerType, run);
		return run.threads;
	}

	/**
	 * The last after-import step, since it is the one that must not run twice: the run takes its messages off the
	 * pending list one at a time, so a sync and a webhook holding the same message cannot both act on it, then an
	 * approval request among those it took starts the owner's expense review. A run cut off before this leaves its
	 * messages pending; the next sync reads them again and runs every step once more, the others being safe to repeat.
	 */
	// shortcut: a server stopped between the take and the trigger loses that review; add a trigger outbox if it matters
	static void finishFollowUps(String ownerId, String ownerType, BrainMailImport.Run run) {
		Set<String> taken = take(ownerId, ownerType, run.owed);
		List<String[]> requests = new ArrayList<>(run.inbound);
		requests.removeIf(message -> !taken.contains(message[0]));
		WorkExpenseRoutine.onNewMail(ownerId, ownerType, requests);
	}

	// clears SYNC_PENDING on each key still marked, in one commit; returns the keys this call cleared
	static Set<String> take(String ownerId, String ownerType, Collection<String> keys) {
		Set<String> taken = new HashSet<>();
		if (keys.isEmpty()) {
			return taken;
		}
		CollaborationDbUtils.batch(conn -> {
			for (String key : keys) {
				if (CollaborationDbUtils.update(conn, "UPDATE BRAIN_MESSAGE SET SYNC_PENDING = NULL WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND MESSAGE_KEY = ? AND SYNC_PENDING = ?", ownerId, ownerType, key, true) > 0) {
					taken.add(key);
				}
			}
		});
		return taken;
	}

	// ---- schedule ----

	/** Looks every 5 minutes for enrolled owners due a sync; once per server, and not where the scheduler is off. */
	public static void startSchedule() {
		if (!Utility.schedulerForceDisable() && SCHEDULED.compareAndSet(false, true)) {
			SCHEDULE.scheduleWithFixedDelay(BrainSync::syncAll, FIRST.toMinutes(), TICK.toMinutes(), TimeUnit.MINUTES);
		}
	}

	// shortcut: per server, like the owner locks, the jobs' ALIVE set and BrainMailWebhook.renewAll; before running
	// more than one server, move this tick onto the clustered SEMOSS scheduler and give COLLAB_JOB a heartbeat
	static void syncAll() {
		try {
			if (!CollaborationDbUtils.isInitalized()) {
				return;
			}
			for (String[] owner : CollaborationDbUtils.query(
					"SELECT OWNER_ID, OWNER_TYPE, MS_USER_ID FROM COLLAB_OWNER WHERE MS_USER_ID IS NOT NULL",
					rs -> new String[] { rs.getString(1), rs.getString(2), rs.getString(3) })) {
				try {
					String outcome = syncInBackground(owner[0], owner[1], owner[2], Instant.now());
					classLogger.debug("Scheduled sync of {} {}: {}", owner[1], owner[0], outcome);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				} catch (Exception e) {
					classLogger.warn("Scheduled sync of {} {} failed", owner[1], owner[0], e);
				}
			}
		} catch (RuntimeException e) {
			// thrown out of the tick it would end the schedule
			classLogger.error("Scheduled sync failed; trying again on the next tick", e);
		}
	}

	/**
	 * One owner's scheduled sync, as their Refresh would run it but with their saved background sign-in, and waited
	 * for. Skipped when the owner turned it off or a sync of theirs started within the interval they chose, while
	 * mail is not ready or anything of theirs runs, and for owners with no background sign-in or a locked account. A
	 * sign-in that cannot be renewed is recorded on the mail source (Needs sign-in) and no job is started, so nothing
	 * reports the owner as synced. Returns what happened, for the log.
	 */
	static String syncInBackground(String ownerId, String ownerType, String msUserId, Instant now) throws Exception {
		int every = BrainProfileUtils.syncEveryMinutes(ownerId, ownerType);
		if (every == 0) {
			return "background sync off";
		}
		if (!canIngest(ownerId, ownerType)) {
			return "mail not ready";
		}
		if (CollaborationJobUtils.anyRunning(ownerId, ownerType)) {
			return "busy";
		}
		Timestamp synced = CollaborationDbUtils.queryOne(
				"SELECT MAX(STARTED_AT) FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = ?",
				rs -> rs.getTimestamp(1), ownerId, ownerType, KIND);
		if (synced != null && toInstant(synced).plus(Duration.ofMinutes(every)).isAfter(now)) {
			return "synced lately";
		}
		if (MicrosoftGraphCredentialStore.get(msUserId) == null) {
			return "no background sign-in";
		}
		if (SecurityQueryUtils.getUnlockedUser(ownerId, ownerType) == null) {
			return "account locked";
		}
		AccessToken token;
		try {
			// renewed and saved first, so the sync starts with a token that lasts
			MicrosoftGraphCredentialStore.accessToken(msUserId);
			token = MicrosoftGraphCredentialStore.get(msUserId);
		} catch (Exception e) {
			// shortcut: HttpHelperUtility answers a refused refresh and an unreachable token endpoint alike (null), so
			// an outage also reads as an expired sign-in until the next good sync clears it
			classLogger.info("Background sign-in of {} {} could not be renewed: {}", ownerType, ownerId, e.getMessage());
			CollaborationSourceUtils.recordSourceError(ownerId, ownerType, EMAIL, SIGN_IN_EXPIRED, true);
			return "sign-in expired";
		}
		// turned off in Settings meanwhile
		if (token == null) {
			return "no background sign-in";
		}
		User user = BrainMailWebhook.ownerUser(ownerId, ownerType, token);
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		if (!ownerId.equals(owner.getValue0()) || !ownerType.equals(owner.getValue1())) {
			return "background sign-in belongs to " + owner.getValue1() + " " + owner.getValue0();
		}
		Map<String, Object> job;
		try {
			job = start(user);
		} catch (IllegalArgumentException | IllegalStateException e) {
			// no model set, or an import began meanwhile
			return "not started: " + e.getMessage();
		}
		Instant until = Instant.now().plus(WAIT_AT_MOST);
		Object status = job.get("status");
		while (CollaborationJobUtils.RUNNING.equals(status) && Instant.now().isBefore(until)) {
			Thread.sleep(WAIT_STEP_MS);
			Map<String, Object> current = CollaborationJobUtils.get(ownerId, ownerType, (String) job.get("id"));
			status = current == null ? null : current.get("status");
		}
		// a Graph call during the sync may have renewed the token; keep the new pair for the next one
		AccessToken current = user.getAccessToken(AuthProvider.MICROSOFT);
		if (current != null && !Objects.equals(current.getAccess_token(), token.getAccess_token())
				&& MicrosoftGraphCredentialStore.isRefreshable(current) && msUserId.equals(current.getId())) {
			MicrosoftGraphCredentialStore.save(current);
		}
		return String.valueOf(status);
	}

	// Graph refused the token (401), or MicrosoftLoginUtils could not renew it and asks for a sign-in
	static boolean signInExpired(Throwable error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			if (cause instanceof SemossPixelException pixel && pixel.getNoun() != null
					&& pixel.getNoun().getOpType().contains(PixelOperationType.LOGGIN_REQUIRED_ERROR)) {
				return true;
			}
		}
		return BrainMailHeaderSource.needsReauth(error);
	}

	// The owner signed in again: when they saved background sign-in for this Microsoft account (their consent), a
	// renewable token that lasts longer replaces it, so background sync comes back. Never blocks the sync.
	static void keepBackgroundCurrent(User user) {
		AccessToken session = user.getAccessToken(AuthProvider.MICROSOFT);
		if (!MicrosoftGraphCredentialStore.isRefreshable(session)) {
			return;
		}
		try {
			AccessToken saved = MicrosoftGraphCredentialStore.get(session.getId());
			if (saved != null && expiresAt(session) > expiresAt(saved)) {
				MicrosoftGraphCredentialStore.save(session);
			}
		} catch (RuntimeException e) {
			classLogger.warn("Could not keep the background sign-in of {} current", session.getId(), e);
		}
	}

	private static long expiresAt(AccessToken token) {
		return token.getStartTime() > 0 && token.getExpires_in() > 0
				? token.getStartTime() + token.getExpires_in() * 1000L
				: 0;
	}

	// mail is connected, the onboarding import has finished, and none is running
	private static boolean canIngest(String ownerId, String ownerType) {
		if (!Boolean.TRUE.equals(CollaborationSourceUtils.getSourcesEnabled(ownerId, ownerType).get(EMAIL))) {
			return false;
		}
		Map<String, Object> importing = CollaborationJobUtils.latest(ownerId, ownerType, BrainMailImport.KIND);
		return (importing == null || !CollaborationJobUtils.RUNNING.equals(importing.get("status")))
				&& lastRun(ownerId, ownerType) != null;
	}

	// where each thread with new mail went, so the page can say more than "N new messages": a new card, an
	// updated card, a cleared card (answered), automated, or into Brain with nothing to do
	private static void outcomes(CollaborationJobUtils.Job job, String ownerId, String ownerType, Set<String> touched,
			Map<String, String> verdicts, Timestamp startedAt) {
		Map<String, String> outcome = new LinkedHashMap<>();
		CollaborationDbUtils.query("SELECT w.THREAD_ID, h.FIELD, h.NEW_VALUE FROM WORK_ITEM_HISTORY h JOIN WORK_ITEM w "
				+ "ON w.OWNER_ID = h.OWNER_ID AND w.OWNER_TYPE = h.OWNER_TYPE AND w.ITEM_ID = h.ITEM_ID "
				+ "WHERE h.OWNER_ID = ? AND h.OWNER_TYPE = ? AND h.CHANGED_BY = ? AND h.CHANGED_AT >= ?", rs -> {
					String field = rs.getString(2);
					String value = rs.getString(3);
					String kind = "created".equals(field) ? NEW
							: "status".equals(field) && WorkItemUtils.DONE.equals(value) ? CLEARED
									: "sourceRef".equals(field) ? UPDATED : null;
					if (kind != null) {
						outcome.merge(rs.getString(1), kind, BrainSync::stronger);
					}
					return null;
				}, ownerId, ownerType, WorkItemUtils.BRAIN, startedAt);
		Map<String, Integer> counts = new LinkedHashMap<>();
		List<Map<String, String>> changes = new ArrayList<>();
		Set<String> all = new LinkedHashSet<>(outcome.keySet());
		all.addAll(touched);
		for (String threadId : all) {
			String kind = outcome.getOrDefault(threadId,
					"automated".equals(verdicts.get(threadId)) ? AUTOMATED : QUIET);
			counts.merge(kind, 1, Integer::sum);
			if (changes.size() < MAX_CHANGES) {
				changes.add(Map.of("threadId", threadId, "outcome", kind));
			}
		}
		job.count("outcomes", counts);
		job.count("changes", changes);
	}

	// one outcome per thread: a new card says more than a clear, a clear more than an update
	private static String stronger(String a, String b) {
		List<String> order = List.of(NEW, CLEARED, UPDATED);
		return order.indexOf(a) <= order.indexOf(b) ? a : b;
	}

	// start of the newest finished import or sync; a failed run does not move it, so its window is read again
	static Timestamp lastRun(String ownerId, String ownerType) {
		return CollaborationDbUtils.queryOne(
				"SELECT MAX(STARTED_AT) FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND IN (?, ?) "
						+ "AND STATUS = ?",
				rs -> rs.getTimestamp(1), ownerId, ownerType, BrainMailImport.KIND, KIND, CollaborationJobUtils.DONE);
	}

	// Teams reaches back further when its last good read is older than the mail window (a chat read failed)
	private static Instant teamsSince(String ownerId, String ownerType, Instant since, Instant floor) {
		for (Map<String, Object> row : CollaborationSourceUtils.getSources(ownerId, ownerType)) {
			if (TEAMS.equals(row.get("id")) && row.get("lastEventAt") instanceof String at) {
				Instant teamsLast = Instant.parse(at).minus(OVERLAP);
				return teamsLast.isBefore(since) ? later(teamsLast, floor) : since;
			}
		}
		// When the first Teams import was partial there is no successful Teams checkpoint yet.
		// Retry its selected window instead of using mail's newer checkpoint and losing skipped chats.
		Map<String, Object> imported = CollaborationDbUtils.queryOne(CollaborationDbUtils.page(
				"SELECT COUNTS_JSON FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = ? "
						+ "AND STATUS = ? ORDER BY STARTED_AT DESC", 1, 0),
				rs -> CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "COUNTS_JSON")),
				ownerId, ownerType, BrainMailImport.KIND, CollaborationJobUtils.DONE);
		return retryTeamsSince(since, floor, imported);
	}

	static Instant retryTeamsSince(Instant since, Instant floor, Map<String, Object> imported) {
		if (imported != null && imported.get("teamsSince") instanceof String at) {
			Instant firstWindow = Instant.parse(at);
			return firstWindow.isBefore(since) ? later(firstWindow, floor) : since;
		}
		return since;
	}

	private static List<String> threadsSince(String ownerId, String ownerType, Timestamp last) {
		return CollaborationDbUtils.query(
				"SELECT DISTINCT THREAD_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? "
						+ "AND THREAD_ID IS NOT NULL AND RECEIVED_AT >= ?",
				rs -> rs.getString(1), ownerId, ownerType, last);
	}

	// stored times are UTC wall time
	private static Instant toInstant(Timestamp at) {
		return at.toLocalDateTime().toInstant(ZoneOffset.UTC);
	}

	private static Instant later(Instant a, Instant b) {
		return a.isAfter(b) ? a : b;
	}
}
