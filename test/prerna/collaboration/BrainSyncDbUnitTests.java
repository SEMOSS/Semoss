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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * A sync cut off after its import and the next one picking up, and the
 * schedule's turn for an owner, against an in-memory H2 Collaboration schema:
 * no message imported twice, the after-import steps owed once more, one expense
 * review per request, and an owner whose Microsoft sign-in expired.
 */
class BrainSyncDbUnitTests {

	private static final Instant NOW = Instant.parse("2026-10-29T13:00:00Z");
	private static final String MS_ID = "ms-owner";

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<WorkThreadInsights> insights;
	private MockedStatic<WorkExpenseRoutine> expenses;
	// the message keys each expense review was started for
	private final List<List<String>> reviews = new ArrayList<>();
	private User user;
	private String ownerId;
	private String ownerType;
	private final List<Map<String, Object>> mailbox = new ArrayList<>();
	private final BrainMailHeaderSource headers = new BrainMailHeaderSource() {
		@Override
		public Map<String, Object> me(User user) {
			return Map.of("id", MS_ID, "displayName", "Owner", "mail", "owner@test.com", "userPrincipalName",
					"owner@test.com");
		}

		@Override
		public Map<String, Object> manager(User user) {
			return null;
		}

		@Override
		public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
			return List.of();
		}
	};

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_sync_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		// summaries need a model; queued work is not under test here
		insights = mockStatic(WorkThreadInsights.class);
		// the review itself runs on the scheduler; what is under test is how often it
		// is asked for
		expenses = mockStatic(WorkExpenseRoutine.class);
		expenses.when(() -> WorkExpenseRoutine.onNewMail(anyString(), anyString(), any())).thenAnswer(call -> {
			List<String> keys = new ArrayList<>();
			for (String[] message : (List<String[]>) call.getArgument(2)) {
				keys.add(message[0]);
			}
			if (!keys.isEmpty()) {
				reviews.add(keys);
			}
			return null;
		});
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		// a new owner per test: jobs and locks are per owner on a server
		AccessToken token = new AccessToken();
		token.setId("owner-" + UUID.randomUUID());
		token.setName("Owner");
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = User.getPrimaryUserIdAndTypePair(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		sql("INSERT INTO COLLAB_OWNER (OWNER_ID, OWNER_TYPE, MS_USER_ID, MS_UPN, STATUS) "
				+ "VALUES (?, ?, ?, 'owner@test.com', 'active')", ownerId, ownerType, MS_ID);
		sql("INSERT INTO SOURCE_CONNECTION (OWNER_ID, OWNER_TYPE, SOURCE, ENABLED, REAUTH_NEEDED) "
				+ "VALUES (?, ?, 'email', TRUE, FALSE)", ownerId, ownerType);
		sql("INSERT INTO COLLAB_JOB (OWNER_ID, OWNER_TYPE, JOB_ID, KIND, STATUS, STARTED_AT, FINISHED_AT) "
				+ "VALUES (?, ?, ?, 'import', 'done', ?, ?)", ownerId, ownerType, UUID.randomUUID().toString(),
				utc(NOW.minus(Duration.ofHours(4))), utc(NOW.minus(Duration.ofHours(4))));
		// an approval request to the owner, and the owner's own answer on another
		// thread
		mailbox.add(
				email("G1", "conv-1", "Meeting Approval: OSLT In Person (102783)", "mes@test.com", "owner@test.com"));
		mailbox.add(email("G2", "conv-2", "RE: Aldercrest proposal", "owner@test.com", "kyle@aldercrest.com"));
	}

	@AfterEach
	void tearDown() throws Exception {
		expenses.close();
		insights.close();
		registry.close();
		connection.close();
	}

	@Test
	void aSyncCutOffAfterItsImportLeavesItsStepsToTheNextSyncOnce() throws Exception {
		BrainMailImport.Run cutOff = read(true);
		assertEquals(2, cutOff.imported);
		assertEquals(2, pending());

		// the next sync reads the same window again: nothing imports twice, and both
		// messages are owed their steps
		BrainMailImport.Run next = read(true);
		assertEquals(0, next.imported);
		assertEquals(2, count("BRAIN_MESSAGE"));
		assertEquals(2, next.owed.size());
		assertEquals(1, next.ownerSent.size());
		assertEquals(1, next.inbound.size());
		BrainSync.finishFollowUps(ownerId, ownerType, next);
		assertEquals(0, pending());
		assertEquals(List.of(List.of(next.inbound.get(0)[0])), reviews);

		// and the one after it owes nothing
		BrainMailImport.Run after = read(true);
		assertTrue(after.owed.isEmpty());
		assertTrue(after.ownerSent.isEmpty());
		assertTrue(after.inbound.isEmpty());
		BrainSync.finishFollowUps(ownerId, ownerType, after);
		assertEquals(1, reviews.size());
	}

	@Test
	void aResultTheOwnersRoutineMailedThemIsNeverImportedAsWork() throws Exception {
		sql("INSERT INTO WORK_CUSTOM_ROUTINE (OWNER_ID, OWNER_TYPE, ROUTINE_ID, SETTINGS_JSON) VALUES (?, ?, 'r1', ?)",
				ownerId, ownerType, "{\"name\":\"NVIDIA Follow-up Digest\"}");
		mailbox.clear();
		// sent before Collaboration recorded its mail: a system routine's dated title, to the owner alone
		mailbox.add(email("B1", "conv-b1", "Morning Work Briefing - 2026-11-04", "owner@test.com", "owner@test.com"));
		mailbox.add(email("B5", "conv-b5", "Event planning - 2026-10-29", "owner@test.com", "owner@test.com"));
		// sent through BrainOwnMail: the copy in Sent is known by its recorded id, the copy that arrived by its
		// header, whoever else it went to
		mailbox.add(email("B2", "conv-b2", "NVIDIA Follow-up Digest", "owner@test.com", "owner@test.com"));
		BrainOwnMail.record(ownerId, ownerType, "<B2@mail.example>", "routine");
		Map<String, Object> arrived = email("B6", "conv-b6", "Expense Review: Q3 travel", "owner@test.com",
				"owner@test.com");
		arrived.put("ccRecipients", List.of(Map.of("emailAddress", Map.of("address", "dana@test.com"))));
		arrived.put("internetMessageHeaders", List.of(Map.of("name", BrainOwnMail.HEADER, "value", "routine")));
		mailbox.add(arrived);
		// the same subjects to or from anyone else are ordinary mail
		mailbox.add(email("B3", "conv-b3", "Morning Work Briefing - 2026-11-04", "owner@test.com", "kyle@test.com"));
		mailbox.add(email("B4", "conv-b4", "NVIDIA Follow-up Digest", "kyle@test.com", "owner@test.com"));
		// a subject alone never decides: a note to self named like a routine, and a brief's title Bcc'd to the team
		mailbox.add(email("B7", "conv-b7", "NVIDIA Follow-up Digest", "owner@test.com", "owner@test.com"));
		Map<String, Object> bcc = email("B8", "conv-b8", "Morning Work Briefing - 2026-11-05", "owner@test.com",
				"owner@test.com");
		bcc.put("bccRecipients", List.of(Map.of("emailAddress", Map.of("address", "team@test.com"))));
		mailbox.add(bcc);
		BrainMailImport.Run run = read(true);
		assertEquals(4, run.imported);
		// what Collaboration sent leaves no row at all; the older results are decided once, and a sync leaves them be
		assertEquals(2, run.ownMail);
		assertEquals(2, run.keptOut);
		assertEquals(4, count("BRAIN_THREAD"));
		assertEquals(2, ((Number) value("SELECT COUNT(*) FROM BRAIN_MESSAGE WHERE DECISION = 'never' "
				+ "AND THREAD_ID IS NULL")).intValue());
		assertEquals(0, read(true).imported);
		assertEquals(4, count("BRAIN_THREAD"));
		for (String kept : List.of("conv-b3", "conv-b4", "conv-b7", "conv-b8")) {
			assertEquals(1, ((Number) value("SELECT COUNT(*) FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:" + kept + "'"))
					.intValue(), kept);
		}
	}

	@Test
	void workRaisedFromARoutineResultImportedBeforeTheKeepOutIsDismissedOnce() throws Exception {
		// a note to self later given a brief's subject, as imports before the keep-out left one; and a real thread
		mailbox.clear();
		mailbox.add(email("B1", "conv-b1", "Notes", "owner@test.com", "owner@test.com"));
		mailbox.add(email("B2", "conv-b2", "Morning Work Briefing - 2026-10-30", "kyle@test.com", "owner@test.com"));
		mailbox.add(email("B3", "conv-b3", "More notes", "owner@test.com", "owner@test.com"));
		read(true);
		String brief = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-b1'");
		String reply = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-b2'");
		String earlier = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-b3'");
		sql("UPDATE BRAIN_THREAD SET SUBJECT = 'Morning Work Briefing - 2026-10-30' WHERE THREAD_ID = ?", brief);
		sql("UPDATE BRAIN_THREAD SET SUBJECT = 'Morning Work Briefing - 2026-10-29' WHERE THREAD_ID = ?", earlier);
		String step = "INSERT INTO WORK_THREAD_STEP (OWNER_ID, OWNER_TYPE, STEP_ID, THREAD_ID, TEXT, KIND, STATUS, ORIGIN) "
				+ "VALUES (?, ?, ?, ?, 'Handle WBSs (P1)', 'task', 'open', ?)";
		sql(step, ownerId, ownerType, "copied", brief, "brain");
		sql(step, ownerId, ownerType, "mine", brief, "you");
		sql(step, ownerId, ownerType, "real", reply, "brain");
		sql("INSERT INTO WORK_ITEM (OWNER_ID, OWNER_TYPE, ITEM_ID, THREAD_ID, SOURCE, TITLE, ASK_TYPE, STATUS, ORIGIN) "
				+ "VALUES (?, ?, 'waiting', ?, 'email', 'Morning Work Briefing - 2026-10-30', 'waiting_on', 'waiting', "
				+ "'brain')", ownerId, ownerType, brief);
		sql("INSERT INTO WORK_ITEM (OWNER_ID, OWNER_TYPE, ITEM_ID, THREAD_ID, SOURCE, TITLE, ASK_TYPE, STATUS, ORIGIN) "
				+ "VALUES (?, ?, 'own', ?, 'task', 'Book the offsite', 'task', 'open', 'you')", ownerId, ownerType, brief);
		// the work items Brain grouped those steps under; one an earlier cleanup left open with its step dismissed
		sql("INSERT INTO WORK_THREAD_STEP (OWNER_ID, OWNER_TYPE, STEP_ID, THREAD_ID, TEXT, KIND, STATUS, ORIGIN) VALUES "
				+ "(?, ?, 'old', ?, 'Handle WBSs (P2)', 'task', 'dismissed', 'brain')", ownerId, ownerType, earlier);
		String task = "INSERT INTO WORK_TASK (OWNER_ID, OWNER_TYPE, TASK_ID, KIND, TITLE, STATUS, ORIGIN) VALUES (?, ?, ?, "
				+ "'work', 'Handle WBSs', 'active', 'brain')";
		for (String[] grouped : new String[][] { { "brief-task", "copied" }, { "owner-task", "mine" },
				{ "real-task", "real" }, { "left-task", "old" } }) {
			sql(task, ownerId, ownerType, grouped[0]);
			sql("UPDATE WORK_THREAD_STEP SET TASK_ID = ? WHERE STEP_ID = ?", grouped[0], grouped[1]);
		}

		assertArrayEquals(new int[] { 1, 1, 2 }, BrainMailImport.dismissRoutineResultWork());
		assertEquals("dismissed", value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = 'copied'"));
		assertEquals("dismissed", value("SELECT STATUS FROM WORK_ITEM WHERE ITEM_ID = 'waiting'"));
		// the owner's own step and item, and a thread someone else wrote on, keep theirs
		assertEquals("open", value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = 'mine'"));
		assertEquals("open", value("SELECT STATUS FROM WORK_ITEM WHERE ITEM_ID = 'own'"));
		assertEquals("open", value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = 'real'"));
		// a work item with nothing left to do goes; one with the owner's step on it, or real mail, stays
		assertEquals("dropped", value("SELECT STATUS FROM WORK_TASK WHERE TASK_ID = 'brief-task'"));
		assertEquals("dropped", value("SELECT STATUS FROM WORK_TASK WHERE TASK_ID = 'left-task'"));
		assertEquals("active", value("SELECT STATUS FROM WORK_TASK WHERE TASK_ID = 'owner-task'"));
		assertEquals("active", value("SELECT STATUS FROM WORK_TASK WHERE TASK_ID = 'real-task'"));
		// the next boot finds nothing left
		assertArrayEquals(new int[] { 0, 0, 0 }, BrainMailImport.dismissRoutineResultWork());
	}

	@Test
	void anAskMarkedDoneDoesNotComeBackWhenItsEmailIsMovedAndReadAgain() throws Exception {
		read(true);
		String thread = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-1'");
		Map<String, Object> verdict = new HashMap<>(Map.of("dedupeKey", "classifier:" + thread + ":G1", "threadId",
				thread, "channel", "email", "title", "Meeting Approval", "askType", "approve", "receivedAt",
				"2026-10-29T11:00:00Z", "sourceRef", "G1"));
		assertEquals(true, WorkItemUtils.ingestOnThread(ownerId, ownerType, verdict).get("created"));
		sql("UPDATE WORK_ITEM SET STATUS = 'done', CLOSED_REASON = 'manual' WHERE THREAD_ID = ?", thread);
		// moved to another folder, the message has a new Graph id, and so a new key
		verdict.put("dedupeKey", "classifier:" + thread + ":G1-moved");
		verdict.put("sourceRef", "G1-moved");
		assertEquals(false, WorkItemUtils.ingestOnThread(ownerId, ownerType, verdict).get("created"));
		assertEquals(1, count("WORK_ITEM"));
		// a later ask on the thread is new work
		verdict.put("dedupeKey", "classifier:" + thread + ":G9");
		verdict.put("receivedAt", "2026-10-29T12:30:00Z");
		assertEquals(true, WorkItemUtils.ingestOnThread(ownerId, ownerType, verdict).get("created"));
	}

	@Test
	void aMessageCutOffByALockTimeoutFailsTheRunSoItsWindowIsReadAgain() {
		BrainMailImport.Run run = spy(new BrainMailImport.Run(ownerId, ownerType, true));
		Map<String, Object> header = mailbox.get(0);
		doThrow(new IllegalStateException("Collaboration update failed",
				new SQLException("Timeout trying to lock table", "HYT00"))).when(run).importOne(header);
		assertThrows(IllegalStateException.class,
				() -> CollaborationDbUtils.batch(conn -> run.importIsolated(conn, header)));
		assertEquals(0, run.unsaved);
	}

	@Test
	void aMessageThatClashesWithARowIsStillSkippedAlone() throws Exception {
		BrainMailImport.Run run = spy(new BrainMailImport.Run(ownerId, ownerType, true));
		Map<String, Object> header = mailbox.get(0);
		doThrow(new IllegalStateException("Collaboration update failed",
				new SQLException("Unique index or primary key violation", "23505"))).when(run).importOne(header);
		CollaborationDbUtils.batch(conn -> run.importIsolated(conn, header));
		assertEquals(1, run.unsaved);
	}

	@Test
	void twoRunsOwingTheSameMessageStartOneReview() throws Exception {
		read(true);
		BrainMailImport.Run sync = read(true);
		BrainMailImport.Run webhook = read(true);
		assertEquals(sync.owed, webhook.owed);

		BrainSync.finishFollowUps(ownerId, ownerType, sync);
		BrainSync.finishFollowUps(ownerId, ownerType, webhook);
		assertEquals(1, reviews.size());
	}

	@Test
	void theOnboardingImportOwesNoSteps() throws Exception {
		BrainMailImport.Run onboarding = read(false);
		assertEquals(2, onboarding.imported);
		assertTrue(onboarding.owed.isEmpty());
		assertEquals(0, pending());
		// so the first sync after it does not review its mail
		assertTrue(read(true).owed.isEmpty());
	}

	@Test
	void aWebhookImportTakesItsMessagesOnceItsStepsAreDone() throws Exception {
		BrainSync.importHeaders(user, ownerId, ownerType, headers, new ArrayList<>(mailbox));
		assertEquals(2, count("BRAIN_MESSAGE"));
		assertEquals(0, pending());
		assertEquals(1, reviews.size());
	}

	@Test
	void aWebhookImportLeavesWorkToTheSyncThatClassifiesItsThreads() throws Exception {
		try (MockedStatic<WorkPipeline> pipeline = mockStatic(WorkPipeline.class)) {
			BrainSync.importHeaders(user, ownerId, ownerType, headers, new ArrayList<>(mailbox));
			pipeline.verify(() -> WorkPipeline.queue(any(), anyString(), anyString(), any()), never());
		}
	}

	@Test
	void aWebhookImportUnderwayWhenResetTakesTheJobLockFinishesInsteadOfDeadlocking() throws Exception {
		CountDownLatch reading = new CountDownLatch(1);
		CountDownLatch resetHoldsJobLock = new CountDownLatch(1);
		// past the import's first check and reading Graph when the owner presses Reset
		BrainMailHeaderSource slow = new BrainMailHeaderSource() {
			@Override
			public Map<String, Object> me(User user) throws Exception {
				reading.countDown();
				try {
					resetHoldsJobLock.await(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return headers.me(user);
			}

			@Override
			public Map<String, Object> manager(User user) {
				return null;
			}

			@Override
			public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
				return List.of();
			}
		};
		CountDownLatch imported = new CountDownLatch(1);
		ExecutorService pool = Executors.newSingleThreadExecutor();
		try {
			// Reset's order: the job lock, held while it deletes routines, then the ingest lock. The import runs on
			// this thread, where the database mocks live
			Future<Boolean> reset = pool.submit(() -> {
				reading.await(10, TimeUnit.SECONDS);
				synchronized (CollaborationDbUtils.ownerLock("job", ownerId, ownerType)) {
					resetHoldsJobLock.countDown();
					if (!imported.await(5, TimeUnit.SECONDS)) {
						// the import is waiting on this lock: let go, so the test fails instead of deadlocking
						return false;
					}
					synchronized (CollaborationDbUtils.ownerLock(BrainSync.INGEST_LOCK, ownerId, ownerType)) {
						return true;
					}
				}
			});
			Set<String> threads = BrainSync.importHeaders(user, ownerId, ownerType, slow, new ArrayList<>(mailbox));
			imported.countDown();
			assertEquals(2, threads.size());
			assertTrue(reset.get(15, TimeUnit.SECONDS), "the import waited on the job lock Reset held");
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	void aSendFromAChatIsNotHeldUpWhileASyncReadsGraph() throws Exception {
		List<Boolean> waited = new ArrayList<>();
		BrainMailHeaderSource reading = inboxSource(changed -> {
			waited.add(aSendWaits());
			return List.of();
		});
		try (MockedStatic<BrainMailHeaderSource> source = mockStatic(BrainMailHeaderSource.class);
				MockedStatic<WorkPipeline> pipeline = mockStatic(WorkPipeline.class)) {
			source.when(BrainMailHeaderSource::current).thenReturn(reading);
			BrainSync.run(user, job(), utc(NOW.minus(Duration.ofHours(1))));
		}
		assertEquals(List.of(false), waited);
	}

	@Test
	void aSendFromAChatIsNotHeldUpWhileTheOnboardingImportReadsMailAndTeams() throws Exception {
		List<Boolean> waited = new ArrayList<>();
		// a mailbox without folders: Inbox and Sent are read, then the Teams chats
		BrainMailHeaderSource reading = new BrainMailHeaderSource() {
			@Override
			public Map<String, Object> me(User user) throws Exception {
				return headers.me(user);
			}

			@Override
			public Map<String, Object> manager(User user) {
				return null;
			}

			@Override
			public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
				waited.add(aSendWaits());
				return List.of();
			}

			@Override
			public List<Map<String, Object>> chats(User user, Instant since, int maxChats, int maxPerChat) {
				waited.add(aSendWaits());
				return List.of();
			}
		};
		try (MockedStatic<BrainMailHeaderSource> source = mockStatic(BrainMailHeaderSource.class)) {
			source.when(BrainMailHeaderSource::current).thenReturn(reading);
			BrainMailImport.run(user, job(), 30, true);
		}
		assertEquals(List.of(false, false, false), waited);
	}

	// this owner's job, for the progress and counts nobody reads here
	private CollaborationJobUtils.Job job() {
		CollaborationJobUtils.Job job = mock(CollaborationJobUtils.Job.class);
		when(job.ownerId()).thenReturn(ownerId);
		when(job.ownerType()).thenReturn(ownerType);
		return job;
	}

	// whether a Send from a chat, which takes the owner's ingest lock to track its thread, has to wait for it now
	private boolean aSendWaits() {
		ExecutorService pool = Executors.newSingleThreadExecutor();
		try {
			pool.submit(() -> {
				synchronized (CollaborationDbUtils.ownerLock(BrainSync.INGEST_LOCK, ownerId, ownerType)) {
					return null;
				}
			}).get(3, TimeUnit.SECONDS);
			return false;
		} catch (java.util.concurrent.TimeoutException e) {
			return true;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			// a Send still waiting takes the lock once the import lets go of it
			pool.shutdown();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void aWorkPlanPartTheServerRefusesIsSavedAloneInsteadOfFailingItsTopicEveryRun() throws Exception {
		read(true);
		String thread = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-1'");
		String candidate = "INSERT INTO WORK_CANDIDATE (OWNER_ID, OWNER_TYPE, CANDIDATE_ID, THREAD_ID, KIND, TEXT, "
				+ "ASSIGNEE, TOPIC_ID, MESSAGE_AT) VALUES (?, ?, ?, ?, 'task', ?, ?, ?, ?)";
		// a name the model made up, under a topic merged away since
		sql(candidate, ownerId, ownerType, "bob", thread, "Send the deck", "Bob", "merged-topic", utc(NOW));
		sql(candidate, ownerId, ownerType, "mine", thread, "Review the budget", "me", null, utc(NOW.plusSeconds(1)));
		List<Object> plannedTopics = new ArrayList<>();
		try (MockedStatic<WorkReconcile> reconcile = mockStatic(WorkReconcile.class)) {
			reconcile.when(() -> WorkReconcile.plan(any(), anyString(), anyString(), any(), any(), any()))
					.thenAnswer(call -> {
						for (Map<String, Object> c : (List<Map<String, Object>>) call.getArgument(4)) {
							plannedTopics.add(c.get("topicId"));
						}
						// the model's item has no title, which the server refuses
						((Consumer<Map<String, Object>>) call.getArgument(5)).accept(Map.of("topicId",
								WorkReconcile.NO_TOPIC, "newItems",
								List.of(Map.of("title", "", "steps",
										List.of(Map.of("text", "Send the deck", "evidence", List.of("c1"))))),
								"looseSteps", List.of(
										Map.of("text", "Review the budget", "assignee", "me", "evidence", List.of("c2")))));
						return Map.of();
					});
			WorkPipeline.reconcile(user, ownerId, ownerType, null);
		}
		assertEquals(Arrays.asList(null, null), plannedTopics);
		assertEquals(2, count("WORK_THREAD_STEP"));
		assertEquals(0, ((Number) value("SELECT COUNT(*) FROM WORK_THREAD_STEP WHERE STEP_OWNER_ID IS NOT NULL"))
				.intValue());
		assertEquals(2, ((Number) value("SELECT COUNT(*) FROM WORK_CANDIDATE WHERE RESULT = 'step'")).intValue());
	}

	@Test
	void aColleagueWithTheOwnersNameIsNotTheOwner() throws Exception {
		mailbox.clear();
		Map<String, Object> namesake = email("N1", "conv-n1", "RE: Budget", "owner@other.com", "owner@test.com");
		namesake.put("from", Map.of("emailAddress", Map.of("name", "Owner", "address", "owner@other.com")));
		mailbox.add(namesake);
		BrainMailImport.Run run = read(true);
		// their reply closes none of the owner's items, and their asks are theirs
		assertTrue(run.ownerSent.isEmpty());
		assertEquals(1, run.inbound.size());
	}

	@Test
	void mailMovedOutOfJunkAfterTheSyncWindowIsImportedButMailOlderThanTheImportIsNot() throws Exception {
		// the onboarding import took the 30 days before it
		sql("UPDATE COLLAB_JOB SET PARAMS_JSON = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND KIND = 'import'",
				"{\"days\":30}", ownerId, ownerType);
		Map<String, Object> notJunk = email("J1", "conv-j1", "Can you approve the PO?", "vendor@test.com",
				"owner@test.com");
		notJunk.put("receivedDateTime", "2026-10-26T11:00:00Z");
		Map<String, Object> ancient = email("J2", "conv-j2", "Old thread", "vendor@test.com", "owner@test.com");
		ancient.put("receivedDateTime", "2026-08-01T11:00:00Z");
		BrainMailHeaderSource changedMail = new BrainMailHeaderSource() {
			@Override
			public Map<String, Object> me(User user) throws Exception {
				return headers.me(user);
			}

			@Override
			public Map<String, Object> manager(User user) {
				return null;
			}

			@Override
			public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
				return List.of();
			}

			@Override
			public List<Map<String, Object>> folders(User user) {
				return List.of(Map.of("id", "inbox-id", "displayName", "Inbox", "wellKnownName", "inbox",
						"totalItemCount", 2), Map.of("id", "sent-id", "displayName", "Sent Items", "wellKnownName",
								"sentitems", "totalItemCount", 0));
			}

			@Override
			public List<Map<String, Object>> allMail(User user, Instant since, int max, boolean changed) {
				return List.of(notJunk, ancient);
			}
		};
		BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType, true);
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		// a sync an hour after the last: the mail came out of Junk days after it arrived
		CollaborationJobUtils.Job job = mock(CollaborationJobUtils.Job.class);
		BrainMailImport.importMail(job, run, BrainMailImport.readMail(user, changedMail, job, ownerId, ownerType,
				NOW.minus(Duration.ofHours(1)), true, 0, 0), 0, 100);
		assertEquals(1, run.imported);
		assertEquals(1, ((Number) value("SELECT COUNT(*) FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-j1'"))
				.intValue());
		assertEquals(0, ((Number) value("SELECT COUNT(*) FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-j2'"))
				.intValue());
	}

	// a mailbox with Inbox and Sent whose all-mail read is the given one (changed is whether it filters on change time)
	private BrainMailHeaderSource inboxSource(java.util.function.Function<Boolean, List<Map<String, Object>>> allMail) {
		return new BrainMailHeaderSource() {
			@Override
			public Map<String, Object> me(User user) throws Exception {
				return headers.me(user);
			}

			@Override
			public Map<String, Object> manager(User user) {
				return null;
			}

			@Override
			public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
				return List.of();
			}

			@Override
			public List<Map<String, Object>> folders(User user) {
				return List.of(Map.of("id", "inbox-id", "displayName", "Inbox", "wellKnownName", "inbox",
						"totalItemCount", 2), Map.of("id", "sent-id", "displayName", "Sent Items", "wellKnownName",
								"sentitems", "totalItemCount", 0));
			}

			@Override
			public List<Map<String, Object>> allMail(User user, Instant since, int max, boolean changed) {
				return allMail.apply(changed);
			}
		};
	}

	@Test
	void aThrottledChangedMailReadFailsTheSyncSoItsWindowIsReadAgain() {
		// the new-mail read would work, so falling back to it would quietly lose this window's moves and flags
		BrainMailHeaderSource throttled = inboxSource(changed -> {
			if (changed) {
				throw new IllegalArgumentException("GET request to https://graph.microsoft.com/v1.0/me/messages "
						+ "returned HTTP 429. Response body: TooManyRequests");
			}
			return List.of();
		});
		assertThrows(IllegalArgumentException.class, () -> BrainMailImport.readMail(user, throttled,
				mock(CollaborationJobUtils.Job.class), ownerId, ownerType, NOW.minus(Duration.ofHours(1)), true, 0, 0));
	}

	@Test
	void aMailboxThatRejectsTheChangeFilterStillGetsItsNewMail() throws Exception {
		BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType, true);
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		Map<String, Object> arrived = email("R1", "conv-r1", "Can you review?", "dana@test.com", "owner@test.com");
		arrived.put("receivedDateTime", "2026-10-29T12:30:00Z");
		BrainMailHeaderSource strict = inboxSource(changed -> {
			if (changed) {
				throw new IllegalArgumentException("GET request to https://graph.microsoft.com/v1.0/me/messages "
						+ "returned HTTP 400. Response body: ErrorInvalidProperty");
			}
			return List.of(arrived);
		});
		CollaborationJobUtils.Job job = mock(CollaborationJobUtils.Job.class);
		BrainMailImport.importMail(job, run, BrainMailImport.readMail(user, strict, job, ownerId, ownerType,
				NOW.minus(Duration.ofHours(1)), true, 0, 0), 0, 100);
		assertEquals(1, run.imported);
	}

	@Test
	void aMessageMovedAwayAndBackKeepsItsNewGraphId() throws Exception {
		read(true);
		// the same folder, categories and flag as before; only Graph's id for it changed
		mailbox.get(0).put("id", "G1-moved");
		read(true);
		assertEquals("G1-moved", value("SELECT GRAPH_ID FROM BRAIN_MESSAGE WHERE GRAPH_ID LIKE 'G1%'"));
	}

	@Test
	void mailOlderThanWhatWorkReadOnItsThreadMovesTheThreadsWorkRefBack() throws Exception {
		read(true);
		sql("UPDATE BRAIN_THREAD SET WORK_REF = '2026-10-29T12:00:00Z' WHERE THREAD_KEY = 'email:conv-1'");
		mailbox.clear();
		// a webhook lost, the next sync brings the 10:30 mail; and mail newer than what Work read changes nothing
		Map<String, Object> late = email("G9", "conv-1", "RE: Meeting Approval", "kyle@test.com", "owner@test.com");
		late.put("receivedDateTime", "2026-10-29T10:30:00Z");
		mailbox.add(late);
		read(true);
		assertEquals("2026-10-29T10:29:59Z", value("SELECT WORK_REF FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-1'"));
		mailbox.clear();
		Map<String, Object> newer = email("G10", "conv-1", "RE: Meeting Approval", "kyle@test.com", "owner@test.com");
		newer.put("receivedDateTime", "2026-10-29T12:30:00Z");
		mailbox.add(newer);
		read(true);
		assertEquals("2026-10-29T10:29:59Z", value("SELECT WORK_REF FROM BRAIN_THREAD WHERE THREAD_KEY = 'email:conv-1'"));
	}

	@Test
	void anAssigneeTheModelMadeUpIsUnclearNotAPersonId() {
		Map<String, String> people = Map.of("p1", "person-1");
		assertEquals("person-1", WorkPipeline.assignee("p1", people));
		assertEquals("unclear", WorkPipeline.assignee("Bob", people));
		assertEquals("unclear", WorkPipeline.assignee("p9", people));
		assertEquals("me", WorkPipeline.assignee("me", people));
		assertEquals("group", WorkPipeline.assignee("group", people));
		assertNull(WorkPipeline.assignee("", people));
	}

	@Test
	void aMessageThatCannotBeSavedIsUndoneAloneAndTheRestOfItsBatchIsKept() throws Exception {
		// a conversation longer than THREAD_KEY holds: its thread insert fails after the gate saved its message row
		mailbox.add(1, email("G3", "c".repeat(300), "Budget", "dana@test.com", "owner@test.com"));
		BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType, true);
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		CollaborationDbUtils.batch(conn -> {
			for (Map<String, Object> header : mailbox) {
				run.importIsolated(conn, header);
			}
		});
		assertEquals(2, count("BRAIN_MESSAGE"));
		assertEquals(2, count("BRAIN_THREAD"));
		assertEquals(2, pending());
		assertEquals(2, run.imported);
		assertEquals(1, run.unsaved);
		// what the run noted for the bad message went with its rows
		assertEquals(2, run.owed.size());
		assertEquals(2, run.threads.size());
		assertFalse(run.holds(mailbox.get(1)));
		assertTrue(run.holds(mailbox.get(2)));
	}

	@Test
	void aMessageThatFailsAfterItsStepsWereNotedLeavesNoStepsBehind() throws Exception {
		BrainMailImport.Run run = spy(new BrainMailImport.Run(ownerId, ownerType, true));
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		// the owner's own answer gets all the way through, then fails
		Map<String, Object> answer = mailbox.get(1);
		doAnswer(call -> {
			call.callRealMethod();
			throw new IllegalStateException("failed after its steps were noted");
		}).when(run).importOne(answer);
		CollaborationDbUtils.batch(conn -> {
			for (Map<String, Object> header : mailbox) {
				run.importIsolated(conn, header);
			}
		});
		assertEquals(1, count("BRAIN_MESSAGE"));
		assertEquals(1, run.imported);
		assertEquals(1, run.unsaved);
		assertEquals(1, run.owed.size());
		assertEquals(1, run.threads.size());
		assertEquals(1, run.inbound.size());
		assertTrue(run.ownerSent.isEmpty());
		assertFalse(run.holds(answer));
	}

	@Test
	void aRunWithMoreUnsavableMessagesThanOneBadOneFailsAndKeepsNothing() throws Exception {
		for (int i = 0; i < 6; i++) {
			mailbox.add(email("B" + i, i + "c".repeat(300), "Budget", "dana@test.com", "owner@test.com"));
		}
		BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType, true);
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		assertThrows(IllegalStateException.class, () -> CollaborationDbUtils.batch(conn -> {
			for (Map<String, Object> header : mailbox) {
				run.importIsolated(conn, header);
			}
		}));
		assertEquals(0, count("BRAIN_MESSAGE"));
	}

	@Test
	void theAssistantPutsEmailsBrainHasNotReadInATopicThroughTheSyncsImport() throws Exception {
		// G1 is in Brain already; G2 and an Uber receipt are only in the mailbox
		BrainSync.importHeaders(user, ownerId, ownerType, headers, List.of(mailbox.get(0)));
		mailbox.add(
				email("G3", "conv-3", "Your Thursday morning trip with Uber", "noreply@uber.com", "owner@test.com"));
		mailbox.add(email("G4", "conv-4", "Your Friday trip with Uber", "noreply@uber.com", "owner@test.com"));
		BrainMailHeaderSource outlook = new BrainMailHeaderSource() {
			@Override
			public Map<String, Object> me(User user) throws Exception {
				return headers.me(user);
			}

			@Override
			public Map<String, Object> manager(User user) {
				return null;
			}

			@Override
			public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
				return List.of();
			}

			@Override
			public Map<String, Object> message(User user, String messageId) {
				return mailbox.stream().filter(header -> messageId.equals(header.get("id"))).findFirst().orElse(null);
			}
		};
		try (MockedStatic<BrainMailHeaderSource> source = mockStatic(BrainMailHeaderSource.class)) {
			source.when(BrainMailHeaderSource::current).thenReturn(outlook);
			Map<String, Object> result = BrainAgentEdits.editTopic(user, null, null, "Uber", null, null, null, null,
					null, null, List.of("G1", "G2", "G3"));
			assertEquals(List.of("created Uber", "added 3 conversations"), result.get("changes"));
			assertEquals(3, count("BRAIN_MESSAGE"));
			assertEquals(3, count("BRAIN_THREAD_TOPIC"));

			// an id the mailbox does not have, or mail that cannot come in, stops the call
			// before the topic is made
			assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, null, null, "Lyft", null,
					null, null, null, null, null, List.of("G9")));
			sql("UPDATE SOURCE_CONNECTION SET ENABLED = FALSE WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId,
					ownerType);
			assertTrue(assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, null, null,
					"Lyft", null, null, null, null, null, null, List.of("G4"))).getMessage()
							.contains("cannot be added"));
		}
		assertEquals(1, count("BRAIN_TOPIC"));
		assertEquals(3, count("BRAIN_MESSAGE"));
	}

	@Test
	void anOwnerWhoseSignInExpiredIsSkippedAndNotReportedAsSynced() throws Exception {
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(MicrosoftGraphCredentialStore.class);
				MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			credentials.when(() -> MicrosoftGraphCredentialStore.get(MS_ID)).thenReturn(new AccessToken());
			credentials.when(() -> MicrosoftGraphCredentialStore.accessToken(MS_ID))
					.thenThrow(new IllegalArgumentException("Reconnect Microsoft to enable background inbox access."));
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser(ownerId, ownerType))
					.thenReturn(Map.of("id", ownerId));

			assertEquals("sign-in expired", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));
		}
		// no job was started, and the mail source says to sign in again without a newer
		// read
		assertEquals(1, count("COLLAB_JOB"));
		Map<String, Object> mail = CollaborationSourceUtils.getSources(ownerId, ownerType).get(0);
		assertEquals("email", mail.get("id"));
		assertEquals(Boolean.TRUE, mail.get("reauthNeeded"));
		assertEquals(BrainSync.SIGN_IN_EXPIRED, mail.get("lastError"));
		assertNull(mail.get("lastEventAt"));
		assertEquals("needs_permission",
				CollaborationSourceUtils.getSourceStatuses(ownerId, ownerType).get(0).get("status"));
	}

	@Test
	void theScheduleSkipsOwnersItShouldLeaveAlone() throws Exception {
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(MicrosoftGraphCredentialStore.class);
				MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			// never saved background sign-in: not enrolled
			assertEquals("no background sign-in", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));

			credentials.when(() -> MicrosoftGraphCredentialStore.get(MS_ID)).thenReturn(new AccessToken());
			// a locked or removed account has no unlocked user
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser(ownerId, ownerType)).thenReturn(null);
			assertEquals("account locked", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));

			// a sync started within the last 15 minutes, such as the open page's
			sql("INSERT INTO COLLAB_JOB (OWNER_ID, OWNER_TYPE, JOB_ID, KIND, STATUS, STARTED_AT, FINISHED_AT) "
					+ "VALUES (?, ?, ?, 'sync', 'done', ?, ?)", ownerId, ownerType, UUID.randomUUID().toString(),
					utc(NOW.minus(Duration.ofMinutes(5))), utc(NOW.minus(Duration.ofMinutes(4))));
			assertEquals("synced lately", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));

			sql("UPDATE SOURCE_CONNECTION SET ENABLED = FALSE WHERE SOURCE = 'email'");
			assertEquals("mail not ready",
					BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW.plus(Duration.ofHours(1))));
			credentials.verify(() -> MicrosoftGraphCredentialStore.accessToken(anyString()), never());
		}
		assertEquals(Boolean.FALSE, value("SELECT REAUTH_NEEDED FROM SOURCE_CONNECTION WHERE SOURCE = 'email'"));
	}

	@Test
	void anOwnerWhoTurnedBackgroundSyncOffIsLeftAlone() throws Exception {
		sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, SYNC_EVERY_MINUTES) "
				+ "VALUES (?, ?, 85, 40, 1, 0)", ownerId, ownerType);
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(
				MicrosoftGraphCredentialStore.class)) {
			assertEquals("background sync off", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));
			credentials.verifyNoInteractions();
		}
	}

	@Test
	void theOwnersIntervalDecidesWhenTheyAreDue() throws Exception {
		sql("INSERT INTO BRAIN_SETTINGS (OWNER_ID, OWNER_TYPE, FILE_AT, ASK_AT, VERSION, SYNC_EVERY_MINUTES) "
				+ "VALUES (?, ?, 85, 40, 1, 60)", ownerId, ownerType);
		sql("INSERT INTO COLLAB_JOB (OWNER_ID, OWNER_TYPE, JOB_ID, KIND, STATUS, STARTED_AT, FINISHED_AT) "
				+ "VALUES (?, ?, ?, 'sync', 'done', ?, ?)", ownerId, ownerType, UUID.randomUUID().toString(),
				utc(NOW.minus(Duration.ofMinutes(20))), utc(NOW.minus(Duration.ofMinutes(19))));
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(
				MicrosoftGraphCredentialStore.class)) {
			// every hour: 20 minutes after the last sync is too soon, though the default 15
			// would be due
			assertEquals("synced lately", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));
			// 70 minutes after it they are due, and go on to the next check
			assertEquals("no background sign-in",
					BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW.plus(Duration.ofMinutes(50))));
		}
	}

	@Test
	void theIntervalIsSavedFromTheOfferedChoicesOnly() throws Exception {
		// the settings read names the platform classifier, which comes from RDF_Map
		try (MockedStatic<BrainThreadClassifier> classifier = mockStatic(BrainThreadClassifier.class)) {
			assertEquals(15, BrainProfileUtils.getSettings(ownerId, ownerType).get("syncEveryMinutes"));
			assertEquals(60,
					BrainProfileUtils.saveSettings(user, Map.of("syncEveryMinutes", 60)).get("syncEveryMinutes"));
			assertEquals(60, BrainProfileUtils.syncEveryMinutes(ownerId, ownerType));
			assertThrows(IllegalArgumentException.class,
					() -> BrainProfileUtils.saveSettings(user, Map.of("syncEveryMinutes", 45)));
			assertEquals(0,
					BrainProfileUtils.saveSettings(user, Map.of("syncEveryMinutes", 0)).get("syncEveryMinutes"));
		}
	}

	@Test
	void aSyncAlreadyRunningIsJoinedAndTheScheduleWaitsItsTurn() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		Map<String, Object> running = CollaborationJobUtils.start(ownerId, ownerType, BrainSync.KIND, Map.of(),
				job -> release.await(30, TimeUnit.SECONDS));
		try {
			// Refresh during a scheduled run gets that run
			Map<String, Object> refresh = CollaborationJobUtils.start(ownerId, ownerType, BrainSync.KIND, Map.of(),
					job -> {
						throw new AssertionError("a second sync started");
					});
			assertEquals(running.get("id"), refresh.get("id"));
			assertEquals("busy", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));
		} finally {
			// the job's own thread cannot see this test's database (static mocks are per
			// thread), so it is only let go
			release.countDown();
		}
	}

	@Test
	void aRefusedTokenOrALoginPromptReadsAsAnExpiredSignIn() {
		Exception login = assertThrows(Exception.class,
				() -> MicrosoftLoginUtils.throwLoginError(Map.of("type", "MICROSOFT")));
		assertTrue(BrainSync.signInExpired(login));
		assertTrue(BrainSync.signInExpired(new IllegalStateException("Graph read failed",
				new IllegalStateException("GET https://graph.microsoft.com/v1.0/me returned HTTP 401"))));
		assertFalse(BrainSync.signInExpired(new IllegalStateException("The directory is not readable (403)")));
	}

	@Test
	void signingInAgainRenewsTheSavedBackgroundSignInOnly() {
		AccessToken session = token("session", NOW.plus(Duration.ofMinutes(50)));
		User signedIn = new User();
		signedIn.setAccessToken(session);
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(
				MicrosoftGraphCredentialStore.class)) {
			credentials.when(() -> MicrosoftGraphCredentialStore.isRefreshable(any())).thenCallRealMethod();
			// nothing saved: the owner never turned background sign-in on, so nothing is
			// kept
			BrainSync.keepBackgroundCurrent(signedIn);
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), never());

			credentials.when(() -> MicrosoftGraphCredentialStore.get(MS_ID))
					.thenReturn(token("expired", NOW.minus(Duration.ofHours(1))));
			BrainSync.keepBackgroundCurrent(signedIn);
			// the user hands out a read-only copy of the token, so the save is matched by
			// what it holds
			credentials.verify(() -> MicrosoftGraphCredentialStore
					.save(argThat(saved -> "session".equals(saved.getAccess_token()))));

			// a saved token that lasts longer stays
			credentials.when(() -> MicrosoftGraphCredentialStore.get(MS_ID))
					.thenReturn(token("renewed", NOW.plus(Duration.ofHours(2))));
			BrainSync.keepBackgroundCurrent(signedIn);
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), times(1));
		}
	}

	// what a sync's import leaves when the server stops right after it: the
	// messages, and none of the later steps
	private BrainMailImport.Run read(boolean followUps) {
		BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType, followUps);
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		CollaborationDbUtils.batch(conn -> {
			for (Map<String, Object> header : mailbox) {
				run.importOne(header);
			}
		});
		return run;
	}

	private static Map<String, Object> email(String graphId, String conversation, String subject, String from,
			String to) {
		Map<String, Object> header = new HashMap<>();
		header.put("id", graphId);
		header.put("internetMessageId", "<" + graphId + "@mail.example>");
		header.put("conversationId", conversation);
		header.put("subject", subject);
		header.put("from", Map.of("emailAddress", Map.of("address", from)));
		header.put("toRecipients", List.of(Map.of("emailAddress", Map.of("address", to))));
		header.put("receivedDateTime", "2026-10-29T11:00:00Z");
		header.put("parentFolderId", "inbox-id");
		return header;
	}

	private static AccessToken token(String accessToken, Instant expires) {
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.MICROSOFT);
		token.setId(MS_ID);
		token.setAccess_token(accessToken);
		token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, "refresh-" + accessToken);
		token.setExpires_in(3600);
		token.setStartTime(expires.toEpochMilli() - 3_600_000L);
		return token;
	}

	// stored times are UTC wall time
	private static Timestamp utc(Instant at) {
		return Timestamp.valueOf(LocalDateTime.ofInstant(at, ZoneOffset.UTC));
	}

	private int pending() throws Exception {
		return ((Number) value("SELECT COUNT(*) FROM BRAIN_MESSAGE WHERE SYNC_PENDING = TRUE")).intValue();
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}

	private int count(String table) throws Exception {
		return ((Number) value("SELECT COUNT(*) FROM " + table)).intValue();
	}

	private Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return rs.next() ? rs.getObject(1) : null;
		}
	}
}
