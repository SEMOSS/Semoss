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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntFunction;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.common.util.concurrent.MoreExecutors;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/**
 * Work's cursor and reconcile against an in-memory H2 Collaboration schema, with the model and the mail source
 * stubbed: mail left unread holds WORK_REF back, mail that always fails is moved past, a thread behind on its mail is
 * taken again, and a done claim closes a loose step. Also the prompts' date, Tidy, a rebuild that replaces old steps,
 * the owners' lanes on the Work pool, and the first build.
 */
class WorkPipelineDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<BrainMessageSource> source;
	private User user;
	private String ownerId;
	private String ownerType;
	private final IModelEngine model = mock(IModelEngine.class);
	// what each model call was given; a null reply is a model that fails
	private final List<String> prompts = new ArrayList<>();
	private String reply = "{\"candidates\": []}";
	// runs once, during the next model call
	private Runnable onAsk;
	private final Map<String, String> bodies = new HashMap<>();
	private final Set<String> failing = new HashSet<>();
	private Executor pool;
	private IntFunction<ExecutorService> readPool;

	@BeforeEach
	void setUp() throws Exception {
		// the tries below come seconds apart; one test checks the real floor
		WorkPipeline.giveUpAfter = Duration.ZERO;
		// lane jobs and their thread reads run on the test thread, where the static mocks are
		pool = WorkPipeline.pool;
		WorkPipeline.pool = Runnable::run;
		readPool = WorkPipeline.readPool;
		WorkPipeline.readPool = size -> MoreExecutors.newDirectExecutorService();
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_work_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
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
		// a new owner per test: unfinished tries are counted per owner on a server
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
		when(model.ask(anyString(), anyString(), any(), anyMap())).thenAnswer(call -> {
			prompts.add(call.getArgument(0));
			Runnable during = onAsk;
			onAsk = null;
			if (during != null) {
				during.run();
			}
			if (reply == null) {
				throw new IllegalStateException("429 Too Many Requests");
			}
			return new AskStringModelEngineResponse(reply, 0, 0);
		});
		BrainMessageSource mail = (u, channel, conversation, graphId) -> {
			if (failing.contains(graphId)) {
				throw new IllegalStateException("Graph returned 503");
			}
			return Map.of("subject", "Deck", "body", Map.of("content", bodies.get(graphId), "contentType", "text"),
					"from", Map.of("emailAddress", Map.of("name", "Kira", "address", "kira@test.com")));
		};
		source = mockStatic(BrainMessageSource.class);
		source.when(BrainMessageSource::current).thenReturn(mail);
	}

	@AfterEach
	void tearDown() throws Exception {
		WorkPipeline.giveUpAfter = Duration.ofHours(6);
		WorkPipeline.pool = pool;
		WorkPipeline.readPool = readPool;
		source.close();
		registry.close();
		connection.close();
	}

	@Test
	void anOutageOfAFewMinutesNeverMovesTheCursorPastUnreadMail() throws Exception {
		WorkPipeline.giveUpAfter = Duration.ofHours(6);
		thread("th-1", at("2026-11-05T10:00:00"), null);
		email("th-1", "g1", at("2026-11-05T10:00:00"), "Can you send the deck?");
		reply = null;
		for (int i = 0; i < 5; i++) {
			assertThrows(IllegalStateException.class, () -> extract("th-1"));
		}
		// five quick failures, as a model outage gives: the ask is still there to read once it is over
		assertNull(workRef("th-1"));
	}

	@Test
	void mailWhoseFetchFailedHoldsTheCursorUntilThreeTriesInARowFailToReadIt() throws Exception {
		thread("th-1", at("2026-11-05T10:02:00"), null);
		email("th-1", "g1", at("2026-11-05T10:00:00"), "Can you send the deck?");
		email("th-1", "g2", at("2026-11-05T10:01:00"), "Here is the agenda.");
		email("th-1", "g3", at("2026-11-05T10:02:00"), "Thanks.");
		failing.add("g2");
		extract("th-1");
		// g2 was not read: the next run starts just before it, not after g3
		assertEquals("2026-11-05T10:00:59Z", workRef("th-1"));
		// it keeps failing: tries that end where they started count, and the third in a row moves past it
		for (int i = 0; i < 2; i++) {
			extract("th-1");
			assertEquals("2026-11-05T10:00:59Z", workRef("th-1"));
		}
		extract("th-1");
		assertEquals("2026-11-05T10:02:00Z", workRef("th-1"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void mailPastTheCharacterBudgetIsReadByTheNextRunInsteadOfSkipped() throws Exception {
		thread("th-1", at("2026-11-05T10:11:00"), null);
		for (int i = 0; i < 12; i++) {
			String n = String.format("%02d", i);
			// ten of these fill the budget
			email("th-1", "g" + n, at("2026-11-05T10:" + n + ":00"), ("Part " + n + " " + "x".repeat(4000)).substring(0,
					4000));
		}
		extract("th-1");
		assertEquals("2026-11-05T10:09:59Z", workRef("th-1"));
		extract("th-1");
		assertEquals("2026-11-05T10:11:00Z", workRef("th-1"));
		// the first ten, then the last two: none read twice, none skipped
		assertEquals(List.of(10, 2), prompts.stream()
				.map(p -> ((List<Object>) CollaborationDbUtils.firstJsonObject(p).get("messages")).size()).toList());
	}

	@Test
	void mailHiddenOnPurposeDoesNotHoldTheCursorBack() throws Exception {
		thread("th-1", at("2026-11-05T10:01:00"), null);
		email("th-1", "g1", at("2026-11-05T10:00:00"), "Can you send the deck?");
		email("th-1", "g2", at("2026-11-05T10:01:00"), "Weekly newsletter");
		sql("UPDATE BRAIN_MESSAGE SET DECISION = ? WHERE GRAPH_ID = 'g2'", BrainRulesGate.OFF);
		extract("th-1");
		// the cursor reaches the thread's newest mail, so the thread is not behind on it
		assertEquals("2026-11-05T10:01:00Z", workRef("th-1"));
	}

	@Test
	void anExtractThatKeepsFailingMovesPastItsMailOnTheThirdTryInARow() throws Exception {
		thread("th-1", at("2026-11-05T10:00:00"), null);
		email("th-1", "g1", at("2026-11-05T10:00:00"), "Can you send the deck?");
		reply = null;
		for (int i = 0; i < 2; i++) {
			assertThrows(IllegalStateException.class, () -> extract("th-1"));
			assertNull(workRef("th-1"));
		}
		assertThrows(IllegalStateException.class, () -> extract("th-1"));
		assertEquals("2026-11-05T10:00:00Z", workRef("th-1"));
	}

	@Test
	void eachSyncRunAlsoTakesThreadsBehindOnTheirMailButNotTheBacklog() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		thread("behind", now.minusHours(1), now.minusHours(2));
		thread("current", now.minusHours(1), now.minusHours(1));
		thread("muted", now.minusHours(1), now.minusHours(2));
		sql("UPDATE BRAIN_THREAD SET MUTED = TRUE WHERE THREAD_ID = 'muted'");
		thread("automated", now.minusHours(1), now.minusHours(2));
		sql("UPDATE BRAIN_THREAD SET AUTOMATED = TRUE WHERE THREAD_ID = 'automated'");
		thread("synced", now.minusMinutes(5), now.minusHours(2));
		// never read by Work: only recent mail counts
		thread("never-read", now.minusDays(2), null);
		thread("never-read-old", now.minusDays(10), null);
		thread("quiet-since-before-the-build", now.minusDays(40), now.minusDays(41));
		assertEquals(List.of("behind", "never-read"), WorkPipeline.lagging(ownerId, ownerType, List.of("synced")));
		for (int i = 0; i < 12; i++) {
			thread("more-" + i, now.minusMinutes(30 + i), now.minusHours(3));
		}
		assertEquals(10, WorkPipeline.lagging(ownerId, ownerType, List.of()).size());
	}

	@Test
	void aDoneClaimClosesALooseStepOnItsThreadAndOneUnderItsTopic() throws Exception {
		LocalDateTime at = at("2026-11-05T10:00:00");
		// Kira's ask on an unfiled thread became a loose step; the owner's reply is a done claim
		thread("unfiled", at, null);
		step("kira-step", "unfiled", null, "Send Kira the deck");
		candidate("reply", "unfiled", null, "Sent Kira the deck", at.plusMinutes(1));
		// a loose step under a topic, finished on another of the topic's threads
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) VALUES (?, ?, 'budget', 'Budget', "
				+ "'active')", ownerId, ownerType);
		thread("ask", at, null);
		thread("answer", at, null);
		for (String threadId : List.of("ask", "answer")) {
			sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE, IS_PRIMARY) VALUES "
					+ "(?, ?, ?, 'budget', 'you', TRUE)", ownerId, ownerType, threadId);
		}
		step("budget-step", "ask", "budget", "Review the budget");
		candidate("reviewed", "answer", "budget", "Reviewed the budget", at.plusMinutes(2));
		// in each topic's batch the loose step is s1
		reply = "{\"decisions\": [{\"candidate\": \"c1\", \"action\": \"closes\", \"ref\": \"s1\", \"reason\": \"\"}, "
				+ "{\"candidate\": \"c2\", \"action\": \"closes\", \"ref\": \"s1\", \"reason\": \"\"}], "
				+ "\"newItems\": [], \"loose\": []}";
		WorkPipeline.reconcile(user, ownerId, ownerType, model);
		assertEquals("done", value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = 'kira-step'"));
		assertEquals("done", value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = 'budget-step'"));
		assertEquals(2, ((Number) value("SELECT COUNT(*) FROM WORK_CANDIDATE WHERE RESULT = 'closes' AND "
				+ "DROP_REASON IS NULL")).intValue());
	}

	@Test
	void aDoneClaimThatNamesARepeatedAskClosesTheStepItRepeats() throws Exception {
		LocalDateTime at = at("2026-11-05T10:00:00");
		thread("deck", at, null);
		step("kira-step", "deck", null, "Send Kira the deck");
		// Kira asks again and the owner's reply finishes it; the close names the repeat (c1), not the step (s1)
		candidate("again", "deck", null, "Send Kira the deck", at.plusMinutes(1));
		sql("UPDATE WORK_CANDIDATE SET KIND = 'task', ASSIGNEE = 'me' WHERE CANDIDATE_ID = 'again'");
		candidate("reply", "deck", null, "Sent Kira the deck", at.plusMinutes(2));
		reply = "{\"decisions\": [{\"candidate\": \"c1\", \"action\": \"same_step\", \"ref\": \"s1\", \"reason\": \"\"}, "
				+ "{\"candidate\": \"c2\", \"action\": \"closes\", \"ref\": \"c1\", \"reason\": \"\"}], "
				+ "\"newItems\": [], \"loose\": []}";
		WorkPipeline.reconcile(user, ownerId, ownerType, model);
		assertEquals("done", value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = 'kira-step'"));
		assertEquals("kira-step", value("SELECT RESULT_REF FROM WORK_CANDIDATE WHERE CANDIDATE_ID = 'reply'"));
	}

	@Test
	void tidyNeverFoldsOrRewordsAStepTheOwnerMade() throws Exception {
		sql("INSERT INTO WORK_TASK (OWNER_ID, OWNER_TYPE, TASK_ID, KIND, TITLE, STATUS, ORIGIN, STATE) VALUES (?, ?, "
				+ "'bill', 'work', 'Answer Bill', 'active', 'brain', 'proposed')", ownerId, ownerType);
		itemStep("brain", "waiting", "brain", "Send Bill the response");
		itemStep("mine", "open", "owner", "Reply to Bill with the response");
		// added on a thread's workspace, which records no origin
		itemStep("workspace", "open", null, "Email Bill the response");
		itemStep("brain-2", "waiting", "brain", "Get Bill the response");
		// only Brain's own duplicate folds, and the owner's open steps that stay do not make the kept step theirs
		assertEquals(1, WorkTaskUtils.mergeSteps(ownerId, ownerType, "bill", "brain", "Send Bill the final response",
				List.of("mine", "workspace", "brain-2")));
		assertEquals(List.of("waiting", "open", "open", "dropped"),
				List.of(stepStatus("brain"), stepStatus("mine"), stepStatus("workspace"), stepStatus("brain-2")));
		// Brain's step folds into the owner's, which keeps the owner's words
		assertEquals(1, WorkTaskUtils.mergeSteps(ownerId, ownerType, "bill", "mine", "Send Bill the response",
				List.of("brain")));
		assertEquals("dropped", stepStatus("brain"));
		assertEquals("Reply to Bill with the response", CollaborationDbUtils.queryOne("SELECT TEXT FROM "
				+ "WORK_THREAD_STEP WHERE STEP_ID = ?", rs -> CollaborationDbUtils.getString(rs, "TEXT"), "mine"));
	}

	@Test
	void theExtractAndReconcilePromptsGiveTheOwnersDateAndWeekday() throws Exception {
		// 26 hours apart, so one of them is on another date than the server's
		ZoneId zone = ZoneId.of("Pacific/Kiritimati");
		if (LocalDate.now(zone).equals(LocalDate.now())) {
			zone = ZoneId.of("Etc/GMT+12");
		}
		sql("INSERT INTO BRAIN_PROFILE (OWNER_ID, OWNER_TYPE, TIMEZONE) VALUES (?, ?, ?)", ownerId, ownerType,
				zone.getId());
		LocalDate day = LocalDate.now(zone);
		String today = day + " (" + day.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH) + ")";
		LocalDateTime at = at("2026-11-05T10:00:00");
		thread("th-1", at, null);
		email("th-1", "g1", at, "Can you send the deck tomorrow?");
		extract("th-1");
		candidate("sent", "th-1", null, "Sent the deck", at);
		reply = "{\"decisions\": [], \"newItems\": [], \"loose\": []}";
		WorkPipeline.reconcile(user, ownerId, ownerType, model);
		assertEquals(List.of(today, today),
				prompts.stream().map(p -> CollaborationDbUtils.firstJsonObject(p).get("today")).toList());
	}

	@Test
	void replacingOldStepsRetiresAndRereadsOnlyTheThreadsInTheBuildsWindow() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		// both read before, each with a loose step Brain made
		thread("recent", now.minusDays(1), now.minusDays(1));
		email("recent", "g-recent", now.minusDays(1), "Can you send the deck?");
		step("recent-step", "recent", null, "Send the deck");
		thread("older", now.minusDays(20), now.minusDays(20));
		step("older-step", "older", null, "Book the venue");
		Map<String, Object> out;
		try (AutoCloseable models = models(model)) {
			out = WorkPipeline.backfill(user, 7, true);
		}
		assertEquals(1, out.get("retiredSteps"));
		assertEquals(WorkTaskUtils.DROPPED, stepStatus("recent-step"));
		// the window's thread was read again from the start
		assertEquals(1, prompts.size());
		// one quiet since before the window keeps its step and its cursor
		assertEquals("open", stepStatus("older-step"));
		assertEquals(now.minusDays(20).toInstant(ZoneOffset.UTC).toString(), workRef("older"));
	}

	@Test
	void aLongLaneGivesTheOnlyPoolThreadToAnotherOwnerBetweenItsJobs() throws Exception {
		ExecutorService one = Executors.newSingleThreadExecutor();
		WorkPipeline.pool = one;
		try {
			List<String> order = Collections.synchronizedList(new ArrayList<>());
			CountDownLatch go = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(4);
			String a = "owner-a-" + UUID.randomUUID();
			String b = "owner-b-" + UUID.randomUUID();
			// A's first job holds the only thread until two more of A's jobs and one of B's are waiting
			WorkPipeline.submit(WorkPipeline.lane(a, ownerType), job(order, "a1", go, done));
			WorkPipeline.submit(WorkPipeline.lane(a, ownerType), job(order, "a2", null, done));
			WorkPipeline.submit(WorkPipeline.lane(a, ownerType), job(order, "a3", null, done));
			WorkPipeline.submit(WorkPipeline.lane(b, ownerType), job(order, "b1", null, done));
			go.countDown();
			assertTrue(done.await(10, TimeUnit.SECONDS));
			assertEquals(List.of("a1", "b1", "a2", "a3"), order);
		} finally {
			one.shutdownNow();
		}
	}

	@Test
	void anOwnersJobsStillRunOneAtATimeAndInOrderOnABiggerPool() throws Exception {
		ExecutorService three = Executors.newFixedThreadPool(3);
		WorkPipeline.pool = three;
		try {
			List<String> order = Collections.synchronizedList(new ArrayList<>());
			List<String> expected = new ArrayList<>();
			AtomicInteger running = new AtomicInteger();
			AtomicBoolean overlapped = new AtomicBoolean();
			CountDownLatch done = new CountDownLatch(40);
			String a = "owner-a-" + UUID.randomUUID();
			String b = "owner-b-" + UUID.randomUUID();
			for (int i = 0; i < 20; i++) {
				String name = "a" + i;
				expected.add(name);
				WorkPipeline.submit(WorkPipeline.lane(a, ownerType), () -> {
					if (running.incrementAndGet() > 1) {
						overlapped.set(true);
					}
					LockSupport.parkNanos(1_000_000);
					order.add(name);
					running.decrementAndGet();
					done.countDown();
				});
				WorkPipeline.submit(WorkPipeline.lane(b, ownerType), done::countDown);
			}
			assertTrue(done.await(10, TimeUnit.SECONDS));
			assertFalse(overlapped.get());
			assertEquals(expected, order);
		} finally {
			three.shutdownNow();
		}
	}

	@Test
	void workFoundWhileTopicsWereReviewedDoesNotStopANewOwnersFirstBuild() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		job(BrainMailImport.KIND, "done", "{\"days\": 30}");
		job(BrainThreadClassifier.JOB_KIND, "done", "{}");
		backlog(now, 2);
		// a sync while the owner reviewed topics found an ask on new mail
		thread("new", now.minusHours(1), now.minusHours(1));
		candidate("found", "new", null, "Send Kira the deck", now.minusHours(1));
		sql("UPDATE WORK_CANDIDATE SET RESULT = 'step' WHERE CANDIDATE_ID = 'found'");
		// the filing job this build ends
		job(BrainThreadClassifier.JOB_KIND, "running", "{\"mode\": \"topics\"}");
		try (AutoCloseable models = models(model)) {
			WorkPipeline.firstBuild(user);
		}
		assertEquals(2, backlogRead());
		assertEquals("done", buildState());
	}

	@Test
	void anOwnerWhoseTopicsWereFiledBeforeTheMarkerIsNotBuiltAgain() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		job(BrainMailImport.KIND, "done", "{\"days\": 30}");
		// the old first build ran when this filing finished; it found nothing, so no candidate shows it ran
		job(BrainThreadClassifier.JOB_KIND, "done", "{\"mode\": \"topics\"}");
		backlog(now, 2);
		try (AutoCloseable models = models(model)) {
			WorkPipeline.firstBuild(user);
		}
		assertEquals(0, backlogRead());
		assertTrue(prompts.isEmpty());
	}

	@Test
	void aFirstBuildCutOffMidwayIsFinishedByTheNextSyncRunsAPartAtATime() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		int part = WorkPipeline.BUILD_PART;
		backlog(now, 2 * part + 5);
		// the model is gone after the first part, which cuts the build off as a restart would
		try (AutoCloseable models = models(model, null, model)) {
			WorkPipeline.firstBuild(user);
			assertEquals(part, backlogRead());
			assertEquals("building", buildState());
			thread("new", now.minusMinutes(5), null);
			email("new", "g-new", now.minusMinutes(5), "Can you send the deck?");
			WorkPipeline.queue(user, ownerId, ownerType, List.of("new"));
			// one more part, besides the sync's own mail
			assertEquals(2 * part, backlogRead());
			WorkPipeline.queue(user, ownerId, ownerType, List.of("new"));
			assertEquals(2 * part + 5, backlogRead());
		}
		assertEquals("done", buildState());
	}

	@Test
	void aFirstBuildAskedForWhileARunIsGoingIsQueuedBehindIt() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		backlog(now, 2);
		thread("new", now.minusMinutes(5), null);
		email("new", "g-new", now.minusMinutes(5), "Can you send the deck?");
		// topic filing finishes while a sync run is reading the new mail
		onAsk = () -> WorkPipeline.firstBuild(user);
		try (AutoCloseable models = models(model)) {
			WorkPipeline.queue(user, ownerId, ownerType, List.of("new"));
		}
		assertEquals(2, backlogRead());
		assertEquals("done", buildState());
	}

	@Test
	void aFirstBuildAskedForBeforeTheWorkModelIsSetIsBuiltBySyncRunsOnceItIs() throws Exception {
		LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
		backlog(now, 2);
		// topic filing has a model of its own; the admin sets the Work model later
		try (MockedStatic<BrainTopicModel> none = mockStatic(BrainTopicModel.class)) {
			WorkPipeline.firstBuild(user);
		}
		assertEquals(0, backlogRead());
		assertEquals("building", buildState());
		thread("new", now.minusMinutes(5), null);
		email("new", "g-new", now.minusMinutes(5), "Can you send the deck?");
		try (AutoCloseable models = models(model)) {
			WorkPipeline.queue(user, ownerId, ownerType, List.of("new"));
		}
		assertEquals(2, backlogRead());
		assertEquals("done", buildState());
	}

	// notes its name once go (if any) opens
	private static Runnable job(List<String> order, String name, CountDownLatch go, CountDownLatch done) {
		return () -> {
			try {
				if (go != null) {
					go.await(10, TimeUnit.SECONDS);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			order.add(name);
			done.countDown();
		};
	}

	// the owner's Work model, and what each load of it gives in turn (null: it cannot be loaded)
	private static AutoCloseable models(IModelEngine first, IModelEngine... then) {
		MockedStatic<BrainTopicModel> engine = mockStatic(BrainTopicModel.class);
		engine.when(() -> BrainTopicModel.engine(any())).thenReturn("work-model");
		MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
		utility.when(() -> Utility.getModel("work-model")).thenReturn(first, then);
		return () -> {
			utility.close();
			engine.close();
		};
	}

	// threads with an ask each, older than a sync run takes up on its own (they were never read)
	private void backlog(LocalDateTime now, int count) throws Exception {
		for (int i = 0; i < count; i++) {
			String threadId = String.format("old-%02d", i);
			LocalDateTime at = now.minusDays(10).minusMinutes(i);
			thread(threadId, at, null);
			email(threadId, "g-" + threadId, at, "Can you review part " + i + "?");
		}
	}

	private int backlogRead() throws Exception {
		return ((Number) value("SELECT COUNT(*) FROM BRAIN_THREAD WHERE THREAD_ID LIKE 'old-%' AND WORK_REF IS NOT "
				+ "NULL")).intValue();
	}

	private String buildState() {
		Map<String, Object> build = CollaborationDbUtils.parseMap(CollaborationDbUtils.queryOne("SELECT WORK_BUILD_JSON "
				+ "FROM BRAIN_SETTINGS WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> CollaborationDbUtils.getString(rs, "WORK_BUILD_JSON"), ownerId, ownerType));
		return build == null ? null : (String) build.get("state");
	}

	private void job(String kind, String status, String params) throws Exception {
		sql("INSERT INTO COLLAB_JOB (OWNER_ID, OWNER_TYPE, JOB_ID, KIND, STATUS, PARAMS_JSON, STARTED_AT) VALUES (?, ?, "
				+ "?, ?, ?, ?, ?)", ownerId, ownerType, UUID.randomUUID().toString(), kind, status, params,
				Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC)));
	}

	private void extract(String threadId) {
		WorkPipeline.extract(user, ownerId, ownerType, threadId, model, null);
	}

	private void thread(String threadId, LocalDateTime lastMessageAt, LocalDateTime workRef) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, THREAD_KEY, SOURCE, SUBJECT, MUTED, AUTOMATED, "
				+ "MESSAGE_COUNT, LAST_MESSAGE_AT, WORK_REF, CREATED_AT) VALUES (?, ?, ?, ?, 'email', ?, FALSE, FALSE, 0, "
				+ "?, ?, ?)", ownerId, ownerType, threadId, "email:conv-" + threadId, "About " + threadId,
				Timestamp.valueOf(lastMessageAt), workRef == null ? null : workRef.toInstant(ZoneOffset.UTC).toString(),
				Timestamp.valueOf(lastMessageAt));
	}

	private void email(String threadId, String graphId, LocalDateTime receivedAt, String body) throws Exception {
		sql("INSERT INTO BRAIN_MESSAGE (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, THREAD_ID, GRAPH_ID, FOLDER, RECEIVED_AT, "
				+ "DECISION) VALUES (?, ?, ?, ?, ?, 'inbox', ?, 'include')", ownerId, ownerType, "key-" + graphId,
				threadId, graphId, Timestamp.valueOf(receivedAt));
		bodies.put(graphId, body);
	}

	private void step(String stepId, String threadId, String topicId, String text) throws Exception {
		sql("INSERT INTO WORK_THREAD_STEP (OWNER_ID, OWNER_TYPE, STEP_ID, THREAD_ID, TEXT, KIND, STATUS, LINK_TOPIC_ID, "
				+ "ORIGIN, CREATED_AT, UPDATED_AT) VALUES (?, ?, ?, ?, ?, 'task', 'open', ?, 'brain', ?, ?)", ownerId,
				ownerType, stepId, threadId, text, topicId, Timestamp.valueOf("2026-11-04 09:00:00"),
				Timestamp.valueOf("2026-11-04 09:00:00"));
	}

	// a step of the work item 'bill'
	private void itemStep(String stepId, String status, String origin, String text) throws Exception {
		sql("INSERT INTO WORK_THREAD_STEP (OWNER_ID, OWNER_TYPE, STEP_ID, TASK_ID, TEXT, KIND, STATUS, ORIGIN, CREATED_AT, "
				+ "UPDATED_AT) VALUES (?, ?, ?, 'bill', ?, 'task', ?, ?, ?, ?)", ownerId, ownerType, stepId, text, status,
				origin, Timestamp.valueOf("2026-11-04 09:00:00"), Timestamp.valueOf("2026-11-04 09:00:00"));
	}

	private Object stepStatus(String stepId) throws Exception {
		return value("SELECT STATUS FROM WORK_THREAD_STEP WHERE STEP_ID = '" + stepId + "'");
	}

	private void candidate(String candidateId, String threadId, String topicId, String text, LocalDateTime at)
			throws Exception {
		sql("INSERT INTO WORK_CANDIDATE (OWNER_ID, OWNER_TYPE, CANDIDATE_ID, THREAD_ID, KIND, TEXT, TOPIC_ID, MESSAGE_AT) "
				+ "VALUES (?, ?, ?, ?, 'done_claim', ?, ?, ?)", ownerId, ownerType, candidateId, threadId, text, topicId,
				Timestamp.valueOf(at));
	}

	private String workRef(String threadId) throws Exception {
		return (String) value("SELECT WORK_REF FROM BRAIN_THREAD WHERE THREAD_ID = '" + threadId + "'");
	}

	private static LocalDateTime at(String text) {
		return LocalDateTime.parse(text);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}

	private Object value(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
			return rs.next() ? rs.getObject(1) : null;
		}
	}
}
