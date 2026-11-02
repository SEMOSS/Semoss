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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
 * A sync cut off after its import and the next one picking up, and the schedule's turn for an owner, against an
 * in-memory H2 Collaboration schema: no message imported twice, the after-import steps owed once more, one expense
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
		// the review itself runs on the scheduler; what is under test is how often it is asked for
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
					columns.add(column.getValue0() + " " + column.getValue1());
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
		// an approval request to the owner, and the owner's own answer on another thread
		mailbox.add(email("G1", "conv-1", "Meeting Approval: OSLT In Person (102783)", "mes@test.com",
				"owner@test.com"));
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

		// the next sync reads the same window again: nothing imports twice, and both messages are owed their steps
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
	void theAssistantPutsEmailsBrainHasNotReadInATopicThroughTheSyncsImport() throws Exception {
		// G1 is in Brain already; G2 and an Uber receipt are only in the mailbox
		BrainSync.importHeaders(user, ownerId, ownerType, headers, List.of(mailbox.get(0)));
		mailbox.add(email("G3", "conv-3", "Your Thursday morning trip with Uber", "noreply@uber.com",
				"owner@test.com"));
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
					null, null, null, null, null, List.of("G1", "G2", "G3"));
			assertEquals(List.of("created Uber", "added 3 conversations"), result.get("changes"));
			assertEquals(3, count("BRAIN_MESSAGE"));
			assertEquals(3, count("BRAIN_THREAD_TOPIC"));

			// an id the mailbox does not have, or mail that cannot come in, stops the call before the topic is made
			assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, null, null, "Lyft",
					null, null, null, null, null, null, null, null, null, List.of("G9")));
			sql("UPDATE SOURCE_CONNECTION SET ENABLED = FALSE WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId,
					ownerType);
			assertTrue(assertThrows(IllegalArgumentException.class, () -> BrainAgentEdits.editTopic(user, null, null,
					"Lyft", null, null, null, null, null, null, null, null, null, List.of("G4"))).getMessage()
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
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser(ownerId, ownerType)).thenReturn(Map.of("id", ownerId));

			assertEquals("sign-in expired", BrainSync.syncInBackground(ownerId, ownerType, MS_ID, NOW));
		}
		// no job was started, and the mail source says to sign in again without a newer read
		assertEquals(1, count("COLLAB_JOB"));
		Map<String, Object> mail = CollaborationSourceUtils.getSources(ownerId, ownerType).get(0);
		assertEquals("email", mail.get("id"));
		assertEquals(Boolean.TRUE, mail.get("reauthNeeded"));
		assertEquals(BrainSync.SIGN_IN_EXPIRED, mail.get("lastError"));
		assertNull(mail.get("lastEventAt"));
		assertEquals("needs_permission", CollaborationSourceUtils.getSourceStatuses(ownerId, ownerType).get(0).get("status"));
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
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(MicrosoftGraphCredentialStore.class)) {
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
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(MicrosoftGraphCredentialStore.class)) {
			// every hour: 20 minutes after the last sync is too soon, though the default 15 would be due
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
			assertEquals(60, BrainProfileUtils.saveSettings(user, Map.of("syncEveryMinutes", 60)).get("syncEveryMinutes"));
			assertEquals(60, BrainProfileUtils.syncEveryMinutes(ownerId, ownerType));
			assertThrows(IllegalArgumentException.class,
					() -> BrainProfileUtils.saveSettings(user, Map.of("syncEveryMinutes", 45)));
			assertEquals(0, BrainProfileUtils.saveSettings(user, Map.of("syncEveryMinutes", 0)).get("syncEveryMinutes"));
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
			// the job's own thread cannot see this test's database (static mocks are per thread), so it is only let go
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
		try (MockedStatic<MicrosoftGraphCredentialStore> credentials = mockStatic(MicrosoftGraphCredentialStore.class)) {
			credentials.when(() -> MicrosoftGraphCredentialStore.isRefreshable(any())).thenCallRealMethod();
			// nothing saved: the owner never turned background sign-in on, so nothing is kept
			BrainSync.keepBackgroundCurrent(signedIn);
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), never());

			credentials.when(() -> MicrosoftGraphCredentialStore.get(MS_ID))
					.thenReturn(token("expired", NOW.minus(Duration.ofHours(1))));
			BrainSync.keepBackgroundCurrent(signedIn);
			// the user hands out a read-only copy of the token, so the save is matched by what it holds
			credentials.verify(() -> MicrosoftGraphCredentialStore
					.save(argThat(saved -> "session".equals(saved.getAccess_token()))));

			// a saved token that lasts longer stays
			credentials.when(() -> MicrosoftGraphCredentialStore.get(MS_ID))
					.thenReturn(token("renewed", NOW.plus(Duration.ofHours(2))));
			BrainSync.keepBackgroundCurrent(signedIn);
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), times(1));
		}
	}

	// what a sync's import leaves when the server stops right after it: the messages, and none of the later steps
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
