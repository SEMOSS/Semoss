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
package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.inferencetracking.RouterEvidenceStore;
import prerna.om.Insight;
import prerna.reactor.agent.run.AgentRunService;
import prerna.util.SystemEngineRegistry;

/**
 * The pooling rule of router evidence, run against a real (in-memory H2) copy of the logs tables: the owner of a
 * router reads the runs and cost rows of all its users, an ordinary user only their own, and a caller who does not
 * own the router cannot use the owner-only read of another user's run.
 */
class RouterEvidencePoolingUnitTests {

	private static final String URL = "jdbc:h2:mem:pooling;DB_CLOSE_DELAY=-1";
	private static final String UTC_NOW = "CAST(FORMATDATETIME(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS', 'en', 'UTC') AS TIMESTAMP)";
	private Object previous;

	private static AccessToken login(String id) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setProvider(AuthProvider.NATIVE);
		return token;
	}

	@BeforeEach
	void logsDatabase() throws Exception {
		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			s.execute("DROP ALL OBJECTS");
			s.execute("CREATE TABLE AGENT_RUN (RUN_ID VARCHAR(50), USER_ID VARCHAR(50), USER_TYPE VARCHAR(50), "
					+ "MODEL_ID VARCHAR(50), DATE_CREATED TIMESTAMP, ROOM_ID VARCHAR(50))");
			s.execute("CREATE TABLE ROOM (ROOM_ID VARCHAR(50), INSIGHT_ID VARCHAR(50))");
			s.execute("CREATE TABLE MESSAGE (ROOM_ID VARCHAR(50), USER_ID VARCHAR(50), USER_TYPE VARCHAR(50), TRANSACTION_ID VARCHAR(50), "
					+ "MESSAGE_ID VARCHAR(50), MESSAGE_TYPE VARCHAR(20), MESSAGE_METHOD VARCHAR(50), COST_MICROS BIGINT, INPUT_TOKENS INT, OUTPUT_TOKENS INT, CACHE_READ_TOKENS INT, "
					+ "CACHE_CREATION_TOKENS INT, THINKING_TOKENS INT)");
			s.execute("INSERT INTO AGENT_RUN VALUES ('runA', 'alice', 'NATIVE', 'router-1', " + UTC_NOW + ", 'roomA')");
			s.execute("INSERT INTO AGENT_RUN VALUES ('runB', 'bob', 'NATIVE', 'router-1', " + UTC_NOW + ", 'roomA')");
			s.execute("INSERT INTO AGENT_RUN VALUES ('runC', 'bob', 'NATIVE', 'other-router', " + UTC_NOW + ", 'roomC')");
			s.execute("INSERT INTO ROOM VALUES ('roomA', 'ordinary-insight')");
			s.execute("INSERT INTO ROOM VALUES ('roomC', 'other-insight')");
			s.execute("INSERT INTO MESSAGE VALUES ('roomA', 'alice', 'NATIVE', 'txA', 'mA', 'RESPONSE', 'ask', 11, 7, 3, NULL, NULL, NULL)");
			s.execute("INSERT INTO MESSAGE VALUES ('roomB', 'bob', 'NATIVE', 'txB', 'mB', 'RESPONSE', 'ask', 22, NULL, NULL, NULL, NULL, NULL)");
		}
		Field holder = SystemEngineRegistry.class.getDeclaredField("modelInferenceLogsDbHolder");
		holder.setAccessible(true);
		previous = holder.get(null);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenAnswer(call -> DriverManager.getConnection(URL));
		when(engine.isConnectionPooling()).thenReturn(false);
		when(engine.getQueryUtil()).thenReturn(mock(prerna.util.sql.AbstractSqlQueryUtil.class));
		holder.set(null, (Supplier<IRDBMSEngine>) () -> engine);
	}

	@AfterEach
	void restore() throws Exception {
		Field holder = SystemEngineRegistry.class.getDeclaredField("modelInferenceLogsDbHolder");
		holder.setAccessible(true);
		holder.set(null, previous);
	}

	private static Set<String> runs(String user, boolean pooled) throws Exception {
		return Set.copyOf(RouterEvidenceStore.runIds(login(user), "router-1", Instant.now().minusSeconds(3600), 10, pooled));
	}

	private static long cost(String room, String user, boolean pooled) throws Exception {
		return RouterEvidenceStore.callRows(room, login(user), pooled).stream().mapToLong(r -> ((Number) r.get("COST_MICROS")).longValue()).sum();
	}

	@Test
	void theOwnerReadsTheRunsAndCostsOfEveryUserOfTheRouterButNotOfAnotherRouter() throws Exception {
		assertEquals(Set.of("runA", "runB"), runs("owner", true));
		assertEquals(11, cost("roomA", "owner", true));
		assertEquals(22, cost("roomB", "owner", true));
	}

	@Test
	void anOrdinaryUsersReadsStayLimitedToTheirOwnRuns() throws Exception {
		assertEquals(Set.of("runA"), runs("alice", false));
		assertEquals(Set.of("runB"), runs("bob", false));
		assertEquals(Set.of(), runs("owner", false));
		assertEquals(11, cost("roomA", "alice", false));
		assertEquals(0, cost("roomB", "alice", false));
		assertEquals(0, cost("roomA", "bob", false));
	}

	@Test
	void routedCostUsesPricedResponsesAndFailsClosedWhenAResponseChargeIsMissing() throws Exception {
		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			s.execute("INSERT INTO MESSAGE VALUES ('roomR', 'alice', 'NATIVE', 'routerTx', 'sharedInput', 'INPUT', 'route_ask', NULL, NULL, NULL, NULL, NULL, NULL)");
			s.execute("INSERT INTO MESSAGE VALUES ('roomR', 'alice', 'NATIVE', 'servedTx', 'sharedInput', 'INPUT', 'ask', NULL, NULL, NULL, NULL, NULL, NULL)");
			s.execute("INSERT INTO MESSAGE VALUES ('roomR', 'alice', 'NATIVE', 'routerTx', 'routerResponse', 'RESPONSE', 'route_ask', NULL, NULL, NULL, NULL, NULL, NULL)");
			s.execute("INSERT INTO MESSAGE VALUES ('roomR', 'alice', 'NATIVE', 'servedTx', 'servedResponse', 'RESPONSE', 'ask', 709, NULL, 39, NULL, NULL, NULL)");
			s.execute("INSERT INTO MESSAGE VALUES ('roomMissing', 'alice', 'NATIVE', 'txMissing', 'inputMissing', 'INPUT', 'ask', NULL, NULL, NULL, NULL, NULL, NULL)");
			s.execute("INSERT INTO MESSAGE VALUES ('roomMissing', 'alice', 'NATIVE', 'txMissing', 'responseMissing', 'RESPONSE', 'ask', NULL, NULL, 39, NULL, NULL, NULL)");
		}

		var pricedRows = RouterEvidenceStore.callRows("roomR", login("alice"), false);
		assertEquals(1, pricedRows.size(), "unpriced inputs and router bookkeeping rows are not charges");
		assertEquals("routerTx", pricedRows.get(0).get("TRANSACTION_ID"), "the served response is attributed to the router transaction");
		assertEquals(709L, RouterEvidenceStore.totalCostMicros(pricedRows));

		var unreadableRows = RouterEvidenceStore.callRows("roomMissing", login("alice"), false);
		assertEquals(1, unreadableRows.size(), "a response with unknown cost must not be silently dropped");
		assertNull(unreadableRows.get(0).get("COST_MICROS"));
		IllegalStateException error = assertThrows(IllegalStateException.class, () -> RouterEvidenceStore.totalCostMicros(unreadableRows));
		assertTrue(error.getMessage().contains("no numeric COST_MICROS"));
	}

	@Test
	void newRunsAreCountedAfterTheNewestRunOfTheLearningRoomUnderThePoolingRule() throws Exception {
		// no attempt yet: every run of the router counts
		assertEquals(2, RouterEvidenceStore.runsSinceLastAttempt(login("owner"), "router-1", List.of(), true));
		assertEquals(1, RouterEvidenceStore.runsSinceLastAttempt(login("alice"), "router-1", List.of(), false));
		List<String> attemptRoomIds = List.of("rl-router-1");
		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			s.execute("INSERT INTO AGENT_RUN VALUES ('attempt', 'owner', 'NATIVE', 'judge', DATEADD('SECOND', 1, " + UTC_NOW + "), 'rl-router-1')");
			s.execute("INSERT INTO ROOM VALUES ('rl-router-1', 'optimizer-insight')");
			s.execute("INSERT INTO AGENT_RUN VALUES ('runD', 'bob', 'NATIVE', 'router-1', DATEADD('SECOND', 5, " + UTC_NOW + "), 'roomD')");
			s.execute("INSERT INTO ROOM VALUES ('roomD', 'ordinary-during-learning')");
		}
		assertEquals(1, RouterEvidenceStore.runsSinceLastAttempt(login("owner"), "router-1", attemptRoomIds, true));
		assertEquals(0, RouterEvidenceStore.runsSinceLastAttempt(login("alice"), "router-1", attemptRoomIds, false));
	}

	@Test
	void optimizerReplayTrafficDoesNotTriggerAnotherAttemptOrEnterIndependentEvidence() throws Exception {
		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			s.execute("INSERT INTO AGENT_RUN VALUES ('attempt', 'owner', 'NATIVE', 'judge', DATEADD('SECOND', 1, " + UTC_NOW + "), 'rl-router-1-cycle')");
			s.execute("INSERT INTO ROOM VALUES ('rl-router-1-cycle', 'optimizer-insight')");
			s.execute("INSERT INTO AGENT_RUN VALUES ('legacyAttempt', 'owner', 'NATIVE', 'judge', DATEADD('SECOND', 1, " + UTC_NOW + "), 'rl-router-1')");
			s.execute("INSERT INTO ROOM VALUES ('rl-router-1', 'legacy-optimizer-insight')");
			s.execute("INSERT INTO ROOM VALUES ('legacyReplayRoom', 'legacy-optimizer-insight')");
			s.execute("INSERT INTO AGENT_RUN VALUES ('legacyOptimizerReplay', 'owner', 'NATIVE', 'router-1', "
					+ "DATEADD('SECOND', 2, " + UTC_NOW + "), 'legacyReplayRoom')");
			for (int i = 0; i < 100; i++) {
				s.execute("INSERT INTO ROOM VALUES ('replayRoom" + i + "', 'optimizer-insight')");
				s.execute("INSERT INTO AGENT_RUN VALUES ('replay" + i + "', 'owner', 'NATIVE', 'router-1', "
						+ "DATEADD('SECOND', 2, " + UTC_NOW + "), 'replayRoom" + i + "')");
			}
		}

		List<String> attemptRoomIds = List.of("rl-router-1-cycle", "rl-router-1");
		assertEquals(0, RouterEvidenceStore.runsSinceLastAttempt(login("owner"), "router-1", attemptRoomIds, true),
				"evaluator replays sharing current or legacy optimizer Insights are not new user tasks");

		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			for (int i = 0; i < 60; i++) {
				s.execute("INSERT INTO ROOM VALUES ('ordinaryRoom" + i + "', 'ordinary-insight-" + i + "')");
				s.execute("INSERT INTO AGENT_RUN VALUES ('ordinary" + i + "', 'owner', 'NATIVE', 'router-1', "
						+ "DATEADD('SECOND', 3, " + UTC_NOW + "), 'ordinaryRoom" + i + "')");
			}
		}
		assertEquals(60, RouterEvidenceStore.runsSinceLastAttempt(login("owner"), "router-1", attemptRoomIds, true),
				"genuine ordinary tasks created during the cycle still count toward the next trigger");

		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			// Missing legacy ROOM metadata is retained rather than classified as optimizer-owned.
			s.execute("INSERT INTO AGENT_RUN VALUES ('legacyOrdinary', 'owner', 'NATIVE', 'router-1', "
					+ "DATEADD('SECOND', 4, " + UTC_NOW + "), 'legacyOrdinaryRoom')");
		}
		assertEquals(61, RouterEvidenceStore.runsSinceLastAttempt(login("owner"), "router-1", attemptRoomIds, true));

		Instant since = Instant.now().minusSeconds(60);
		Instant through = Instant.now().plusSeconds(60);
		var allRuns = RouterEvidenceStore.runIds(login("owner"), "router-1", since, through, 500, 0, true);
		var independentRuns = RouterEvidenceStore.runIdsExcludingLearningReplays(login("owner"), "router-1", since, through, 500, 0, true,
				attemptRoomIds);
		assertTrue(allRuns.contains("replay0"));
		assertTrue(allRuns.contains("legacyOptimizerReplay"));
		assertFalse(independentRuns.contains("replay0"), "optimizer replays cannot be selected as independent golden/watch evidence");
		assertFalse(independentRuns.contains("legacyOptimizerReplay"), "the legacy optimizer room uses the same replay exclusion");
		assertTrue(independentRuns.contains("ordinary0"));
		assertTrue(independentRuns.contains("legacyOrdinary"));
	}

	@Test
	void onlySuppliedRoomIdsIdentifyAttemptsAndTheirReplays() throws Exception {
		List<String> attemptRoomIds = List.of("recorded'room");
		try (Connection c = DriverManager.getConnection(URL); Statement s = c.createStatement()) {
			s.execute("INSERT INTO ROOM VALUES ('recorded''room', 'optimizer-insight'), ('replayRoom', 'optimizer-insight'), "
					+ "('rl-router-1-misleading', 'ordinary-prefix'), ('rl-router-1', 'ordinary-legacy')");
			String afterStart = "DATEADD('SECOND', 1, " + UTC_NOW + ")";
			String afterAttempt = "DATEADD('SECOND', 2, " + UTC_NOW + ")";
			s.execute("INSERT INTO AGENT_RUN VALUES ('attempt', 'owner', 'NATIVE', 'judge', " + afterStart + ", 'recorded''room'), "
					+ "('replay', 'alice', 'NATIVE', 'router-1', " + afterAttempt + ", 'replayRoom'), "
					+ "('ordinaryPrefix', 'alice', 'NATIVE', 'router-1', " + afterAttempt + ", 'rl-router-1-misleading'), "
					+ "('ordinaryLegacy', 'alice', 'MICROSOFT', 'router-1', " + afterAttempt + ", 'rl-router-1')");
			s.execute("ALTER TABLE AGENT_RUN ADD STATUS VARCHAR(20)");
			s.execute("UPDATE AGENT_RUN SET STATUS = 'RUNNING' WHERE RUN_ID IN ('ordinaryPrefix', 'ordinaryLegacy')");
			assertFalse(RouterEvidenceStore.attemptRunning(attemptRoomIds));
			s.execute("UPDATE AGENT_RUN SET STATUS = 'INPUT_REQUIRED' WHERE RUN_ID = 'attempt'");
		}
		assertTrue(RouterEvidenceStore.attemptRunning(attemptRoomIds));
		assertFalse(RouterEvidenceStore.attemptRunning(List.of()));
		assertEquals(2, RouterEvidenceStore.runsSinceLastAttempt(login("alice"), "router-1", attemptRoomIds, true));
		assertEquals(1, RouterEvidenceStore.runsSinceLastAttempt(login("alice"), "router-1", attemptRoomIds, false));
		assertEquals(5, RouterEvidenceStore.runsSinceLastAttempt(login("alice"), "router-1", List.of(), true));

		Instant since = Instant.now().minusSeconds(60);
		Instant through = Instant.now().plusSeconds(60);
		assertEquals(Set.of("runA", "ordinaryPrefix"), Set.copyOf(RouterEvidenceStore.runIdsExcludingLearningReplays(
				login("alice"), "router-1", since, through, 10, 0, false, attemptRoomIds)));
		assertEquals(Set.of("runA", "runB", "ordinaryPrefix", "ordinaryLegacy"), Set.copyOf(RouterEvidenceStore.runIdsExcludingLearningReplays(
				login("alice"), "router-1", since, through, 10, 0, true, attemptRoomIds)));
		assertEquals(Set.of("runA", "ordinaryPrefix", "replay"), Set.copyOf(RouterEvidenceStore.runIdsExcludingLearningReplays(
				login("alice"), "router-1", since, through, 10, 0, false, List.of())));
	}

	@Test
	void theLogsAreOnlyOpenedThroughTheStorePackageNotFromAReactor() {
		assertThrows(SecurityException.class, SystemEngineRegistry::getModelInferenceLogsDb);
	}

	@Test
	void onlyAnOwnerOfTheRouterIsPooledAndMayReadAnotherUsersRun() {
		User user = mock(User.class);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		try (MockedStatic<SecurityEngineUtils> security = mockStatic(SecurityEngineUtils.class)) {
			security.when(() -> SecurityEngineUtils.userIsOwner(user, "router-1")).thenReturn(true);
			security.when(() -> SecurityEngineUtils.userIsOwner(user, "router-2")).thenReturn(false);
			assertTrue(GetRouterEvidenceReactor.pooled(insight, "router-1"));
			assertFalse(GetRouterEvidenceReactor.pooled(insight, "router-2"));
			assertThrows(SecurityException.class, () -> AgentRunService.get().getRunOfOwnedRouter("runB", insight, "router-2"));
		}
	}
}
