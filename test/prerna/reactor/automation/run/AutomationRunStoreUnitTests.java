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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;

class AutomationRunStoreUnitTests {

	@Test
	void missingSchedulerDatabaseKeepsInitializationStateError() {
		try (var registry = mockStatic(SystemEngineRegistry.class, CALLS_REAL_METHODS)) {
			registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
			IllegalStateException error = assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.initializeRun("run", "project", "automation", 1, "hash", "{}",
							Map.of(), "manual", "user", List.of(), Map.of(), Map.of()));
			assertEquals("System database 'scheduler' is required for initializing automation run history",
					error.getMessage());
			registry.verify(SystemEngineRegistry::getSchedulerDb, org.mockito.Mockito.never());
		}
	}

	private void schema(JdbcTestDatabase db) throws Exception {
		db.execute(
				"CREATE TABLE AUTOMATION_RUNS (RUN_ID VARCHAR PRIMARY KEY, PROJECT_ID VARCHAR, AUTOMATION_ID VARCHAR, DEFINITION_VERSION INT, DEFINITION_HASH VARCHAR, DEFINITION_SNAPSHOT CLOB, INPUT_SNAPSHOT CLOB, STATUS VARCHAR, TRIGGER_TYPE VARCHAR, STARTED_AT TIMESTAMP, LAST_HEARTBEAT TIMESTAMP, TOTAL_NODES INT, COMPLETED_NODES INT, CREATED_BY VARCHAR, CANCEL_REQUESTED BOOLEAN, RESULT_SUMMARY VARCHAR, COMPLETED_AT TIMESTAMP, FAILED_NODE_ID VARCHAR, ERROR_MESSAGE VARCHAR, REPROCESSED_FROM_RUN_ID VARCHAR)",
				"CREATE TABLE AUTOMATION_RUN_NODE_SOURCES (RUN_ID VARCHAR, NODE_ID VARCHAR, SOURCE_HASH VARCHAR, SOURCE_CODE CLOB)",
				"CREATE TABLE AUTOMATION_NODE_OUTPUTS (RUN_ID VARCHAR, NODE_ID VARCHAR, NODE_LABEL VARCHAR, EXECUTION_ORDER INT, STATUS VARCHAR, ROOM_ID VARCHAR, WORKSPACE_ID VARCHAR, STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, DURATION_MS BIGINT, OUTPUT_VAR_NAME VARCHAR, OUTPUT_KIND VARCHAR, OUTPUT_VALUE CLOB, OUTPUT_PREVIEW VARCHAR, MODEL_MESSAGE_ID VARCHAR, AGENT_RUN_ID VARCHAR, 				ERROR_MESSAGE VARCHAR, SCOPE_SNAPSHOT CLOB, CARRIED_OVER BOOLEAN)");
	}

	private void savedResultSchema(JdbcTestDatabase db) throws Exception {
		db.execute(
				"ALTER TABLE AUTOMATION_NODE_OUTPUTS ADD PARENT_NODE_ID VARCHAR",
								"ALTER TABLE AUTOMATION_NODE_OUTPUTS ADD SOURCE_NODE_ID VARCHAR",
				"ALTER TABLE AUTOMATION_NODE_OUTPUTS ADD ITERATION_INDEX INT",
				"CREATE TABLE AUTOMATION_SAVED_RESULTS (RESULT_ID VARCHAR PRIMARY KEY, PROJECT_ID VARCHAR, RUN_ID VARCHAR, NODE_ID VARCHAR, PARENT_NODE_ID VARCHAR, ITERATION_INDEX INT, RESULT_KIND VARCHAR, FORMAT_VERSION INT, ROW_COUNT BIGINT, BYTE_COUNT BIGINT, CHECKSUM VARCHAR, CREATED_AT TIMESTAMP, RETAIN_UNTIL TIMESTAMP, CAPTURE_STATE VARCHAR, RESULT_ROLE VARCHAR, LOGICAL_NAME VARCHAR, PAYLOAD_VALUE CLOB)",
				"CREATE TABLE AUTOMATION_RESULT_CHUNKS (RESULT_ID VARCHAR, CHUNK_INDEX INT, FIRST_ROW_INDEX BIGINT, CHUNK_ROW_COUNT INT, BYTE_COUNT BIGINT, CHECKSUM VARCHAR, CHUNK_VALUE CLOB)");
	}

	private void initialize(String runId, Map<String, String> sources) {
		AutomationRunStore.initializeRun(runId, "project", "automation", 1, "hash", "{}",
				Map.of("value", "<text>"), "manual", "user", List.of(), Map.of(), sources);
	}

	@Test
	void initializationAndClaimPreserveSnapshotAndSingleClaim() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.manual();
			initialize("run", Map.of("node", "  return 1\n"));
			db.connection.rollback();
			assertEquals(1, db.count("AUTOMATION_RUNS"));
			assertEquals("  return 1\n", db.value("SELECT SOURCE_CODE FROM AUTOMATION_RUN_NODE_SOURCES"));
			assertTrue(AutomationRunStore.claimRun("run"));
			assertFalse(AutomationRunStore.claimRun("run"));
			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS"));
			AutomationRunStore.setCancelRequested("run");
			assertEquals(true, db.value("SELECT CANCEL_REQUESTED FROM AUTOMATION_RUNS"));
			assertTrue(AutomationRunStore.updateHeartbeat("run", 2));
			assertTrue(AutomationRunStore.touchHeartbeat("run"));
			assertTrue(AutomationRunStore.updateRunSummary("run", "  result  "));
			AutomationRunStore.completeRun("run", "project", "SUCCESS", null, null);
			assertEquals("SUCCESS", db.value("SELECT STATUS FROM AUTOMATION_RUNS"));
			assertEquals("  result  ", db.value("SELECT RESULT_SUMMARY FROM AUTOMATION_RUNS"));
			assertFalse(db.connection.isClosed());
		}
	}

	@Test
	void failedSnapshotInsertRollsBackRunAndSupportsReuse() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.execute("ALTER TABLE AUTOMATION_RUN_NODE_SOURCES ADD CHECK (NODE_ID <> 'reject')");
			assertThrows(IllegalStateException.class, () -> initialize("failed", Map.of("reject", "source")));
			assertEquals(0, db.count("AUTOMATION_RUNS"));
			assertEquals(0, db.count("AUTOMATION_RUN_NODE_SOURCES"));
			verify(db.connection).rollback();
			initialize("retry", Map.of());
			assertEquals(1, db.count("AUTOMATION_RUNS"));
		}
	}

	private static Map<String, Object> graphNode(String id, String label) {
		return Map.of(AutomationConstants.NODE_FIELD_ID, id, AutomationConstants.NODE_FIELD_LABEL, label);
	}

	private static AutomationRunStore.NodeRerun plainRerun() {
		return new AutomationRunStore.NodeRerun("b", "B", null, null, java.util.Set.of("b"), Map.of());
	}

	private void failedRunWithThreeNodes(JdbcTestDatabase db) throws Exception {
		schema(db);
		savedResultSchema(db);
		AutomationRunStore.initializeRun("run", "project", "automation", 1, "old-hash", "{\"old\":true}", Map.of(),
				"manual", "user", List.of(graphNode("a", "A"), graphNode("b", "B"), graphNode("c", "C")), Map.of(),
				Map.of("b", "old source"));
		db.execute(
				"UPDATE AUTOMATION_RUNS SET STATUS = 'FAILED', COMPLETED_AT = CURRENT_TIMESTAMP, FAILED_NODE_ID = 'b', ERROR_MESSAGE = 'boom', RESULT_SUMMARY = 'old summary', CANCEL_REQUESTED = TRUE",
				"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = 'SUCCESS', OUTPUT_VAR_NAME = 'a_out', OUTPUT_VALUE = '1' WHERE NODE_ID = 'a'",
				"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = 'FAILED', ERROR_MESSAGE = 'boom', OUTPUT_VALUE = '2', SCOPE_SNAPSHOT = '{}', ROOM_ID = 'old-room' WHERE NODE_ID = 'b'",
				"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = 'SKIPPED' WHERE NODE_ID = 'c'",
				"INSERT INTO AUTOMATION_NODE_OUTPUTS (RUN_ID, NODE_ID, EXECUTION_ORDER, STATUS, PARENT_NODE_ID, ITERATION_INDEX) VALUES ('run', 'child', 1000000, 'SUCCESS', 'b', 0)",
				"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, RUN_ID, NODE_ID, PARENT_NODE_ID) VALUES ('saved-b', 'run', 'b', NULL)",
				"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, RUN_ID, NODE_ID, PARENT_NODE_ID) VALUES ('saved-child', 'run', 'child', 'b')",
				"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, RUN_ID, NODE_ID, PARENT_NODE_ID) VALUES ('saved-a', 'run', 'a', NULL)",
				"INSERT INTO AUTOMATION_RESULT_CHUNKS (RESULT_ID, CHUNK_INDEX) VALUES ('saved-b', 0)",
				"INSERT INTO AUTOMATION_RESULT_CHUNKS (RESULT_ID, CHUNK_INDEX) VALUES ('saved-child', 0)",
				"INSERT INTO AUTOMATION_RESULT_CHUNKS (RESULT_ID, CHUNK_INDEX) VALUES ('saved-a', 0)");
	}

	@Test
	void reopeningForNodeRerunClearsOnlySelectedNodesAndRecordsTheirNewSettings() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);

			assertTrue(AutomationRunStore.reopenRunForNodeRerun("run", "project", "new-hash", "{\"new\":true}",
					List.of(new AutomationRunStore.NodeRerun("b", "B renamed", "new-room", null, java.util.Set.of("b"),
							Map.of("b", "new source")))));

			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS"));
			assertNull(db.value("SELECT COMPLETED_AT FROM AUTOMATION_RUNS"));
			assertNull(db.value("SELECT FAILED_NODE_ID FROM AUTOMATION_RUNS"));
			assertNull(db.value("SELECT ERROR_MESSAGE FROM AUTOMATION_RUNS"));
			assertNull(db.value("SELECT RESULT_SUMMARY FROM AUTOMATION_RUNS"));
			assertEquals(false, db.value("SELECT CANCEL_REQUESTED FROM AUTOMATION_RUNS"));
			assertEquals("new-hash", db.value("SELECT DEFINITION_HASH FROM AUTOMATION_RUNS"));
			assertEquals("{\"new\":true}", db.value("SELECT DEFINITION_SNAPSHOT FROM AUTOMATION_RUNS"));

			assertEquals("PENDING", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertEquals("B renamed", db.value("SELECT NODE_LABEL FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertEquals("new-room", db.value("SELECT ROOM_ID FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertNull(db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertNull(db.value("SELECT ERROR_MESSAGE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertNull(db.value("SELECT SCOPE_SNAPSHOT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));

			assertEquals("SUCCESS", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'a'"));
			assertEquals("1", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'a'"));
			assertEquals("SKIPPED", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'c'"));
			assertEquals(0, db.count("AUTOMATION_NODE_OUTPUTS WHERE PARENT_NODE_ID = 'b'"));

			assertEquals(1, db.count("AUTOMATION_SAVED_RESULTS"));
			assertEquals("saved-a", db.value("SELECT RESULT_ID FROM AUTOMATION_SAVED_RESULTS"));
			assertEquals(1, db.count("AUTOMATION_RESULT_CHUNKS"));

			assertEquals("new source", db.value("SELECT SOURCE_CODE FROM AUTOMATION_RUN_NODE_SOURCES"));
			assertEquals(1, db.count("AUTOMATION_RUN_NODE_SOURCES"));
			assertEquals(3, db.value("SELECT TOTAL_NODES FROM AUTOMATION_RUNS"));
			assertEquals(2, db.value("SELECT COMPLETED_NODES FROM AUTOMATION_RUNS"));
		}
	}

	@Test
	void reopeningForNodeRerunDropsTheSourceOfANodeThatNoLongerHasOne() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);

			assertTrue(AutomationRunStore.reopenRunForNodeRerun("run", "project", "new-hash", "{}",
					List.of(plainRerun())));

			assertEquals(0, db.count("AUTOMATION_RUN_NODE_SOURCES"));
		}
	}

	@Test
	void reopeningForNodeRerunRefusesARunThatIsNotFinishedAndChangesNothing() throws Exception {
		for (String status : List.of("RUNNING", "WAITING_FOR_INPUT", "SUBMITTED")) {
			try (var db = new JdbcTestDatabase()) {
				failedRunWithThreeNodes(db);
				db.execute("UPDATE AUTOMATION_RUNS SET STATUS = '" + status + "'");

				assertFalse(AutomationRunStore.reopenRunForNodeRerun("run", "project", "new-hash", "{}",
						List.of(plainRerun())), status);

				assertEquals("old-hash", db.value("SELECT DEFINITION_HASH FROM AUTOMATION_RUNS"), status);
				assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"),
						status);
				assertEquals(3, db.count("AUTOMATION_SAVED_RESULTS"), status);
			}
		}
	}

	@Test
	void reopeningForNodeRerunOnlyClaimsTheRunOnce() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			var nodes = List.of(plainRerun());

			assertTrue(AutomationRunStore.reopenRunForNodeRerun("run", "project", "new-hash", "{}", nodes));
			assertFalse(AutomationRunStore.reopenRunForNodeRerun("run", "project", "other-hash", "{}", nodes));

			assertEquals("new-hash", db.value("SELECT DEFINITION_HASH FROM AUTOMATION_RUNS"));
		}
	}

	@Test
	void reopeningForNodeRerunRefusesAnotherProjectsRun() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);

			assertFalse(AutomationRunStore.reopenRunForNodeRerun("run", "other-project", "new-hash", "{}",
					List.of(plainRerun())));

			assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_RUNS"));
		}
	}

	@Test
	void reopeningForALoopNodeRerunClearsOnlyThatIterationsRow() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			db.execute(
					"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = 'FAILED', ERROR_MESSAGE = 'bad', OUTPUT_VALUE = '5', ROOM_ID = 'old-room' WHERE NODE_ID = 'child'",
					"INSERT INTO AUTOMATION_NODE_OUTPUTS (RUN_ID, NODE_ID, EXECUTION_ORDER, STATUS, PARENT_NODE_ID, ITERATION_INDEX, OUTPUT_VALUE) VALUES ('run', 'child-2', 1000001, 'SUCCESS', 'b', 1, '7')",
					"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, RUN_ID, NODE_ID, PARENT_NODE_ID) VALUES ('saved-child-2', 'run', 'child-2', 'b')",
					"INSERT INTO AUTOMATION_RESULT_CHUNKS (RESULT_ID, CHUNK_INDEX) VALUES ('saved-child-2', 0)");

			assertTrue(AutomationRunStore.reopenRunForLoopNodeRerun("run", "project", "new-hash", "{\"new\":true}",
					List.of(new AutomationRunStore.LoopNodeRerun("child", "x", "X renamed", "new-room", null)),
					java.util.Set.of("x"), Map.of("x", "new source")));

			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS"));
			assertEquals("new-hash", db.value("SELECT DEFINITION_HASH FROM AUTOMATION_RUNS"));
			assertNull(db.value("SELECT ERROR_MESSAGE FROM AUTOMATION_RUNS"));

			assertEquals("PENDING", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child'"));
			assertEquals("X renamed",
					db.value("SELECT NODE_LABEL FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child'"));
			assertEquals("new-room", db.value("SELECT ROOM_ID FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child'"));
			assertNull(db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child'"));
			assertNull(db.value("SELECT ERROR_MESSAGE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child'"));

			// Another iteration, and the loop node and its siblings, keep their results.
			assertEquals("SUCCESS",
					db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child-2'"));
			assertEquals("7", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child-2'"));
			assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertEquals("2", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertEquals("1", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'a'"));

			assertEquals(3, db.count("AUTOMATION_SAVED_RESULTS"));
			assertEquals(0, db.count("AUTOMATION_SAVED_RESULTS WHERE RESULT_ID = 'saved-child'"));
			assertEquals(3, db.count("AUTOMATION_RESULT_CHUNKS"));

			assertEquals("new source",
					db.value("SELECT SOURCE_CODE FROM AUTOMATION_RUN_NODE_SOURCES WHERE NODE_ID = 'x'"));
			assertEquals("old source",
					db.value("SELECT SOURCE_CODE FROM AUTOMATION_RUN_NODE_SOURCES WHERE NODE_ID = 'b'"));
			assertEquals(5, db.value("SELECT TOTAL_NODES FROM AUTOMATION_RUNS"));
			assertEquals(3, db.value("SELECT COMPLETED_NODES FROM AUTOMATION_RUNS"));
		}
	}

	@Test
	void reopeningForALoopNodeRerunRefusesARunThatIsNotFinishedAndChangesNothing() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			db.execute("UPDATE AUTOMATION_RUNS SET STATUS = 'RUNNING'");

			assertFalse(AutomationRunStore.reopenRunForLoopNodeRerun("run", "project", "new-hash", "{}",
					List.of(new AutomationRunStore.LoopNodeRerun("child", "x", "X", null, null)),
					java.util.Set.of("x"), Map.of()));

			assertEquals("old-hash", db.value("SELECT DEFINITION_HASH FROM AUTOMATION_RUNS"));
			assertEquals("SUCCESS", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'child'"));
			assertEquals(3, db.count("AUTOMATION_SAVED_RESULTS"));
		}
	}

	@Test
	void zeroRowCancellationStillFailsAndRollsBack() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.manual();
			var error = assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.setCancelRequested("absent"));
			assertEquals("Unable to persist the automation cancellation request.", error.getMessage());
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void nodeResultsAndWaitContinuationRetainStatePredicatesAndTrace() throws Exception {
		try (var db = new JdbcTestDatabase();
				var audit = mockStatic(prerna.usertracking.UserAuditTrailUtils.class);
				var queries = mockStatic(prerna.util.QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			schema(db);
			db.execute(
					"CREATE TABLE AUTOMATION_RUN_WAITS (WAIT_ID VARCHAR, RUN_ID VARCHAR, NODE_ID VARCHAR, WAIT_TYPE VARCHAR, AGENT_RUN_ID VARCHAR, ROOM_ID VARCHAR, RESUME_NODE_ID VARCHAR, STATUS VARCHAR, CREATED_BY VARCHAR, STARTED_AT TIMESTAMP, EXPIRES_AT TIMESTAMP, RESOLVED_AT TIMESTAMP, RESOLVED_BY VARCHAR)");
			AutomationRunStore.initializeRun("run", "project", "automation", 1, "hash", "{}", Map.of(), "manual",
					"user", List.of(Map.of("id", "node", "label", "Node"),
							Map.of("id", "summary", "label", "Summary"),
							Map.of("id", "skipped", "label", "Skipped")),
					Map.of(), Map.of());
			assertTrue(AutomationRunStore.claimRun("run"));
			AutomationRunStore.markNodeRunning("run", "node");
			AutomationRunStore.updateNodeAgentRunTrace("run", "node", "agent");
			String waitId = AutomationRunStore.persistAgentWait("run", "project", "node", "output", " {} ",
					"preview", "agent", "room", null, "user", java.time.Instant.now().plusSeconds(600), 4L);
			assertEquals("WAITING_FOR_INPUT", db.value("SELECT STATUS FROM AUTOMATION_RUNS"));
			var waiting = new java.util.HashMap<String, Object>(
					Map.of("WAIT_ID", waitId, "NODE_ID", "node", "STATUS", "PENDING"));
			queries.when(() -> prerna.util.QueryExecutionUtility.flushRsToMap(eq(db.engine),
					any(prerna.query.querystruct.SelectQueryStruct.class))).thenReturn(List.of(waiting));
			assertNotNull(AutomationRunStore.claimWaitingRun("run", "project"));
			assertNull(AutomationRunStore.claimWaitingRun("run", "project"));
			AutomationRunStore.resolveWait("run", waitId, "resolver");
			assertEquals("RESOLVED", db.value("SELECT STATUS FROM AUTOMATION_RUN_WAITS"));
			var started = java.sql.Timestamp.from(java.time.Instant.now());
			AutomationRunStore.updateNodeSuccess("run", "node", started, 5L, "output",
					AutomationConstants.OUTPUT_KIND_TABLE, "  {}  ", "preview", null, "agent");
			assertEquals("  {}  ", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
			assertEquals(AutomationConstants.OUTPUT_KIND_TABLE,
					db.value("SELECT OUTPUT_KIND FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
			String ordinarySummary = "{\"dataType\":\"table\",\"rowCount\":2,\"columnCount\":1}";
			AutomationRunStore.markNodeRunning("run", "summary");
			AutomationRunStore.updateNodeSuccess("run", "summary", started, 5L, "summary_output", null,
					ordinarySummary, "ordinary JSON", null, null);
			assertNull(db.value("SELECT OUTPUT_KIND FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='summary'"));
			assertEquals(ordinarySummary,
					db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='summary'"));
			AutomationRunStore.updateNodeFailed("run", "node", started, 6L, "failure");
			AutomationRunStore.updateNodeFailedWithResult("run", "node", started, 7L, "output", null, null, null,
					null, "failed result");
			assertEquals("agent", db.value("SELECT AGENT_RUN_ID FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
			assertNull(db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
			AutomationRunStore.skipPendingNodes("run", "not selected");
			assertEquals("SKIPPED", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='skipped'"));
			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.updateNodeAgentRunTrace("run", "missing", "agent"));
			AutomationRunStore.completeRun("run", "project", "FAILED", "node", "failure");
			audit.verify(() -> prerna.usertracking.UserAuditTrailUtils.recordEvent(any()));
			assertThrows(IllegalStateException.class, () -> AutomationRunStore.completeRun("run", "project", "FAILED", "node", "failure"));
			audit.verifyNoMoreInteractions();
		}
	}

	@Test
	void terminalOutcomesAreAuditedOnlyAfterTheirStateCommits() throws Exception {
		try (var db = new JdbcTestDatabase(); var audit = mockStatic(prerna.usertracking.UserAuditTrailUtils.class)) {
			schema(db);
			for (String status : List.of("SUCCESS", "FAILED", "CANCELLED", "INTERRUPTED")) {
				initialize(status, Map.of());
				assertTrue(AutomationRunStore.claimRun(status));
				AutomationRunStore.completeRun(status, "project", status, null, null);
			}
			audit.verify(() -> prerna.usertracking.UserAuditTrailUtils.recordEvent(any()), org.mockito.Mockito.times(4));
			assertThrows(IllegalStateException.class, () -> AutomationRunStore.completeRun("absent", "project", "SUCCESS", null, null));
			audit.verifyNoMoreInteractions();
		}
	}

	@Test
	void scopeSnapshotIsPersistedToItsOwnColumnOnSuccess() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("run", "project", "automation", 1, "hash", "{}", Map.of(),
					"manual", "user", List.of(Map.of("id", "node", "label", "Node")), Map.of(), Map.of());
			assertTrue(AutomationRunStore.claimRun("run"));
			AutomationRunStore.markNodeRunning("run", "node");

			String scopeSnapshot = "{\"greeting\":\"hi\",\"count\":2}";
			AutomationRunStore.updateNodeSuccess("run", "node", java.sql.Timestamp.from(java.time.Instant.now()), 5L,
					"greeting", null, "\"hi\"", "hi", null, null, scopeSnapshot);

			assertEquals(scopeSnapshot,
					db.value("SELECT SCOPE_SNAPSHOT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
			assertEquals("\"hi\"",
					db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
		}
	}

	@Test
	void nodeWithoutARetainedScopeSurfacesANullSnapshot() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("run", "project", "automation", 1, "hash", "{}", Map.of(),
					"manual", "user", List.of(Map.of("id", "node", "label", "Node")), Map.of(), Map.of());
			assertTrue(AutomationRunStore.claimRun("run"));
			AutomationRunStore.markNodeRunning("run", "node");

			AutomationRunStore.updateNodeSuccess("run", "node", java.sql.Timestamp.from(java.time.Instant.now()), 5L,
					"greeting", "\"hi\"", "hi", null, "agent");

			assertNull(db.value("SELECT SCOPE_SNAPSHOT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
		}
	}

	@Test
	void buildNodeResultsSurfacesTheScopeSnapshotUnderItsResultKey() {
		Map<String, Object> row = new LinkedHashMap<>();
		row.put(AutomationConstants.RUN_ID, "run");
		row.put(AutomationConstants.NODE_ID, "node");
		row.put(AutomationConstants.NODE_LABEL, "Node");
		row.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		row.put(AutomationConstants.SCOPE_SNAPSHOT, "{\"foo\":\"bar\"}");

		List<Map<String, Object>> results = AutomationRunStore.buildNodeResults(List.of(row));

		assertEquals(1, results.size());
		assertEquals("{\"foo\":\"bar\"}", results.get(0).get(AutomationConstants.RESULT_SCOPE_SNAPSHOT));
	}

	@Test
	void buildNodeResultsLeavesTheScopeSnapshotNullWhenNoneWasRetained() {
		Map<String, Object> row = new LinkedHashMap<>();
		row.put(AutomationConstants.RUN_ID, "run");
		row.put(AutomationConstants.NODE_ID, "node");
		row.put(AutomationConstants.NODE_LABEL, "Node");
		row.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);

		List<Map<String, Object>> results = AutomationRunStore.buildNodeResults(List.of(row));

		assertEquals(1, results.size());
		assertNull(results.get(0).get(AutomationConstants.RESULT_SCOPE_SNAPSHOT));
	}

	@Test
	void buildNodeResultsExposesEachNodesStartAndEndTime() {
		java.sql.Timestamp started = java.sql.Timestamp.valueOf("2026-11-11 09:00:00");
		java.sql.Timestamp completed = java.sql.Timestamp.valueOf("2026-11-11 09:00:05");
		Map<String, Object> timed = new LinkedHashMap<>();
		timed.put(AutomationConstants.RUN_ID, "run");
		timed.put(AutomationConstants.NODE_ID, "timed");
		timed.put(AutomationConstants.NODE_LABEL, "Timed");
		timed.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		timed.put(AutomationConstants.STARTED_AT, started);
		timed.put(AutomationConstants.COMPLETED_AT, completed);
		Map<String, Object> neverRan = new LinkedHashMap<>();
		neverRan.put(AutomationConstants.RUN_ID, "run");
		neverRan.put(AutomationConstants.NODE_ID, "pending");
		neverRan.put(AutomationConstants.NODE_LABEL, "Pending");
		neverRan.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_PENDING);

		List<Map<String, Object>> results = AutomationRunStore.buildNodeResults(List.of(timed, neverRan));

		assertEquals(started, results.get(0).get(AutomationConstants.STARTED_AT));
		assertEquals(completed, results.get(0).get(AutomationConstants.COMPLETED_AT));
		assertNull(results.get(1).get(AutomationConstants.STARTED_AT));
		assertNull(results.get(1).get(AutomationConstants.COMPLETED_AT));
	}

	@Test
	void cloningAFinishedRunCopiesItWholeAndLeavesTheOriginalUntouched() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);

			assertTrue(AutomationRunStore.cloneRunForReprocess("run", "copy", "project", "someone-else"));

			assertEquals(2, db.count("AUTOMATION_RUNS"));
			assertEquals("run", db.value("SELECT REPROCESSED_FROM_RUN_ID FROM AUTOMATION_RUNS WHERE RUN_ID = 'copy'"));
			assertNull(db.value("SELECT REPROCESSED_FROM_RUN_ID FROM AUTOMATION_RUNS WHERE RUN_ID = 'run'"));
			assertEquals("someone-else", db.value("SELECT CREATED_BY FROM AUTOMATION_RUNS WHERE RUN_ID = 'copy'"));
			assertEquals("{\"old\":true}",
					db.value("SELECT DEFINITION_SNAPSHOT FROM AUTOMATION_RUNS WHERE RUN_ID = 'copy'"));
			assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID = 'copy'"));

			// Every node row, loop rows included, comes across carried over; the original's are not.
			assertEquals(4, db.count("AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'copy' AND CARRIED_OVER = TRUE"));
			assertEquals(0, db.count("AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'run' AND CARRIED_OVER = TRUE"));
			assertEquals("2", db.value(
					"SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'copy' AND NODE_ID = 'b'"));
			assertEquals("old source",
					db.value("SELECT SOURCE_CODE FROM AUTOMATION_RUN_NODE_SOURCES WHERE RUN_ID = 'copy'"));

			// Saved results are copied under their own ids, so neither run's results can disappear with the other's.
			assertEquals(3, db.count("AUTOMATION_SAVED_RESULTS WHERE RUN_ID = 'copy'"));
			assertEquals(3, db.count("AUTOMATION_SAVED_RESULTS WHERE RUN_ID = 'run'"));
			assertEquals(0, db.count("AUTOMATION_SAVED_RESULTS c WHERE c.RUN_ID = 'copy' AND c.RESULT_ID IN "
					+ "(SELECT RESULT_ID FROM AUTOMATION_SAVED_RESULTS WHERE RUN_ID = 'run')"));
			assertEquals(6, db.count("AUTOMATION_RESULT_CHUNKS"));
		}
	}

	@Test
	void cloningRefusesARunThatIsNotFinishedAndCopiesNothing() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			db.execute("UPDATE AUTOMATION_RUNS SET STATUS = 'RUNNING'");

			assertFalse(AutomationRunStore.cloneRunForReprocess("run", "copy", "project", "user"));
			assertFalse(AutomationRunStore.cloneRunForReprocess("run", "copy", "other-project", "user"));

			assertEquals(1, db.count("AUTOMATION_RUNS"));
			assertEquals(0, db.count("AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'copy'"));
		}
	}

	@Test
	void reprocessingOnACopyLeavesTheOriginalAndTheCopysOtherNodesAlone() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			assertTrue(AutomationRunStore.cloneRunForReprocess("run", "copy", "project", "user"));

			assertTrue(AutomationRunStore.reopenRunForNodeRerun("copy", "project", "new-hash", "{}",
					List.of(new AutomationRunStore.NodeRerun("b", "B", null, null, java.util.Set.of("b"), Map.of()))));

			// The chosen node is no longer carried over; the rest still are.
			assertEquals(0, db.count("AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'copy' AND NODE_ID = 'b' AND CARRIED_OVER = TRUE"));
			assertEquals("PENDING", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'copy' AND NODE_ID = 'b'"));
			assertEquals(2, db.count("AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'copy' AND NODE_ID IN ('a', 'c') AND CARRIED_OVER = TRUE"));

			assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'run' AND NODE_ID = 'b'"));
			assertEquals("2", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID = 'run' AND NODE_ID = 'b'"));
			assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID = 'run'"));
			assertEquals("old-hash", db.value("SELECT DEFINITION_HASH FROM AUTOMATION_RUNS WHERE RUN_ID = 'run'"));
			assertEquals(3, db.count("AUTOMATION_SAVED_RESULTS WHERE RUN_ID = 'run'"));
		}
	}

	@Test
	void aRunRecordsWhichRunItWasStartedFrom() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("fresh", "project", "automation", 1, "hash", "{}", Map.of(), "manual",
					"user", List.of(), Map.of(), Map.of(), "original");
			initialize("plain", Map.of());

			assertEquals("original", db.value("SELECT REPROCESSED_FROM_RUN_ID FROM AUTOMATION_RUNS WHERE RUN_ID = 'fresh'"));
			assertNull(db.value("SELECT REPROCESSED_FROM_RUN_ID FROM AUTOMATION_RUNS WHERE RUN_ID = 'plain'"));
		}
	}

	@Test
	void buildNodeResultsFlagsCarriedOverNodes() {
		Map<String, Object> carried = new LinkedHashMap<>();
		carried.put(AutomationConstants.RUN_ID, "run");
		carried.put(AutomationConstants.NODE_ID, "carried");
		carried.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		carried.put(AutomationConstants.CARRIED_OVER, true);
		Map<String, Object> reran = new LinkedHashMap<>();
		reran.put(AutomationConstants.RUN_ID, "run");
		reran.put(AutomationConstants.NODE_ID, "reran");
		reran.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);

		List<Map<String, Object>> results = AutomationRunStore.buildNodeResults(List.of(carried, reran));

		assertEquals(true, results.get(0).get(AutomationConstants.CARRIED_OVER));
		assertEquals(false, results.get(1).get(AutomationConstants.CARRIED_OVER));
	}

	@Test
	void reprocessingANodeReplacesItsTimesAndLeavesOtherNodesTimesAlone() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			db.execute(
					"UPDATE AUTOMATION_NODE_OUTPUTS SET STARTED_AT = TIMESTAMP '2020-01-01 00:00:00', COMPLETED_AT = TIMESTAMP '2020-01-01 00:00:05' WHERE NODE_ID IN ('a', 'b')");

			assertTrue(AutomationRunStore.reopenRunForNodeRerun("run", "project", "new-hash", "{}",
					List.of(new AutomationRunStore.NodeRerun("b", "B", null, null, java.util.Set.of("b"), Map.of()))));

			assertNull(db.value("SELECT STARTED_AT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertNull(db.value("SELECT COMPLETED_AT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'"));
			assertEquals(java.sql.Timestamp.valueOf("2020-01-01 00:00:05"),
					db.value("SELECT COMPLETED_AT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'a'"));

			AutomationRunStore.markNodeRunning("run", "b");
			AutomationRunStore.updateNodeSuccess("run", "b", java.sql.Timestamp.from(java.time.Instant.now()), 5L,
					"b_out", "\"x\"", "x", null, null);

			java.sql.Timestamp oldTime = java.sql.Timestamp.valueOf("2020-01-01 00:00:05");
			assertTrue(((java.sql.Timestamp) db
					.value("SELECT STARTED_AT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'")).after(oldTime));
			assertTrue(((java.sql.Timestamp) db
					.value("SELECT COMPLETED_AT FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID = 'b'")).after(oldTime));
		}
	}

	@Test
	void reprocessingStartsTheRunsClockOverAndClearsItsEnd() throws Exception {
		java.sql.Timestamp old = java.sql.Timestamp.valueOf("2020-01-01 00:00:00");
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			db.execute("UPDATE AUTOMATION_RUNS SET STARTED_AT = TIMESTAMP '2020-01-01 00:00:00'");

			assertTrue(AutomationRunStore.reopenRunForNodeRerun("run", "project", "h", "{}",
					List.of(new AutomationRunStore.NodeRerun("b", "B", null, null, java.util.Set.of("b"), Map.of()))));

			assertTrue(((java.sql.Timestamp) db.value("SELECT STARTED_AT FROM AUTOMATION_RUNS")).after(old));
			assertNull(db.value("SELECT COMPLETED_AT FROM AUTOMATION_RUNS"));
		}
		try (var db = new JdbcTestDatabase()) {
			failedRunWithThreeNodes(db);
			db.execute("UPDATE AUTOMATION_RUNS SET STARTED_AT = TIMESTAMP '2020-01-01 00:00:00'");

			assertTrue(AutomationRunStore.reopenRunForLoopNodeRerun("run", "project", "h", "{}",
					List.of(new AutomationRunStore.LoopNodeRerun("child", "x", "X", null, null)),
					java.util.Set.of("x"), Map.of()));

			assertTrue(((java.sql.Timestamp) db.value("SELECT STARTED_AT FROM AUTOMATION_RUNS")).after(old));
			assertNull(db.value("SELECT COMPLETED_AT FROM AUTOMATION_RUNS"));
		}
	}

	@Test
	void tableSuccessPersistsDurableHistoryWithTheNodeResult() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			savedResultSchema(db);
			AutomationRunStore.initializeRun("run", "project", "automation", 1, "hash", "{}", Map.of(),
					"manual", "user", List.of(Map.of("id", "query", "label", "Query")), Map.of(), Map.of());
			assertTrue(AutomationRunStore.claimRun("run"));
			AutomationRunStore.markNodeRunning("run", "query");

			String payload = "[[\"Ada\"],[\"Grace\"]]";
			byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
			String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
			AutomationTableCaptureService.TableCapture capture = new AutomationTableCaptureService.TableCapture(
					"[{\"name\":\"hero\",\"type\":\"STRING\"}]",
					List.of(new AutomationTableCaptureService.TableChunk(0, 0, 2, payload, bytes.length, checksum)),
					2, bytes.length, checksum);

			AutomationRunStore.updateNodeSuccess("run", "query", java.sql.Timestamp.from(java.time.Instant.now()),
					8L, "heroes", AutomationConstants.OUTPUT_KIND_TABLE,
					"{\"dataType\":\"table\",\"rowCount\":2}", "2 rows", null, null, capture);

			assertEquals(1, db.count("AUTOMATION_SAVED_RESULTS"));
			assertEquals(1, db.count("AUTOMATION_RESULT_CHUNKS"));
			assertEquals("project", db.value("SELECT PROJECT_ID FROM AUTOMATION_SAVED_RESULTS"));
			assertEquals(2L, ((Number) db.value("SELECT ROW_COUNT FROM AUTOMATION_SAVED_RESULTS")).longValue());
			assertEquals(payload, db.value("SELECT CHUNK_VALUE FROM AUTOMATION_RESULT_CHUNKS"));
			assertEquals(AutomationConstants.OUTPUT_KIND_TABLE,
					db.value("SELECT OUTPUT_KIND FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='query'"));
		}
	}

	@Test
	void inlineScopeRunsCompleteAndDeleteOnlyTheirOwnRows() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			savedResultSchema(db);
			db.execute(
					"CREATE TABLE AUTOMATION_RUN_WAITS (WAIT_ID VARCHAR, RUN_ID VARCHAR, NODE_ID VARCHAR, WAIT_TYPE VARCHAR, AGENT_RUN_ID VARCHAR, ROOM_ID VARCHAR, RESUME_NODE_ID VARCHAR, STATUS VARCHAR, CREATED_BY VARCHAR, STARTED_AT TIMESTAMP, EXPIRES_AT TIMESTAMP, RESOLVED_AT TIMESTAMP, RESOLVED_BY VARCHAR)");
			String scope = AutomationConstants.INLINE_RUN_SCOPE_PREFIX + "owner";
			String otherScope = AutomationConstants.INLINE_RUN_SCOPE_PREFIX + "other";
			List<Map<String, Object>> nodes = List.of(Map.of("id", "node", "label", "Node"));
			AutomationRunStore.initializeRun("brief", scope, "brief", 1, "hash", "{}", Map.of(), "SCHEDULED", "user",
					nodes, Map.of(), Map.of("node", "source"));
			AutomationRunStore.initializeRun("other-automation", scope, "default", 1, "hash", "{}", Map.of(),
					"MANUAL", "user", nodes, Map.of(), Map.of("node", "source"));
			AutomationRunStore.initializeRun("other-owner", otherScope, "brief", 1, "hash", "{}", Map.of(),
					"SCHEDULED", "other", nodes, Map.of(), Map.of("node", "source"));
			assertEquals(scope, db.value("SELECT PROJECT_ID FROM AUTOMATION_RUNS WHERE RUN_ID='brief'"));

			assertTrue(AutomationRunStore.claimRun("brief"));
			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.completeRun("brief", otherScope, "SUCCESS", null, null));
			AutomationRunStore.completeRun("brief", scope, "SUCCESS", null, null);
			assertEquals("SUCCESS", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='brief'"));
			db.execute("INSERT INTO AUTOMATION_RUN_WAITS (WAIT_ID, RUN_ID, NODE_ID, STATUS) "
					+ "VALUES ('wait', 'brief', 'node', 'RESOLVED')",
					"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, RUN_ID) VALUES ('result', 'brief')",
					"INSERT INTO AUTOMATION_RESULT_CHUNKS (RESULT_ID, CHUNK_INDEX) VALUES ('result', 0)");

			assertEquals(1, AutomationRunStore.deleteRuns(scope, "brief"));

			assertEquals(2, db.count("AUTOMATION_RUNS"));
			assertNull(db.value("SELECT RUN_ID FROM AUTOMATION_RUNS WHERE RUN_ID='brief'"));
			assertNull(db.value("SELECT RUN_ID FROM AUTOMATION_RUN_NODE_SOURCES WHERE RUN_ID='brief'"));
			assertNull(db.value("SELECT RUN_ID FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID='brief'"));
			assertEquals(0, db.count("AUTOMATION_RUN_WAITS"));
			assertEquals(0, db.count("AUTOMATION_SAVED_RESULTS"));
			assertEquals(0, db.count("AUTOMATION_RESULT_CHUNKS"));
			assertEquals(2, db.count("AUTOMATION_RUN_NODE_SOURCES"));
			assertEquals(2, db.count("AUTOMATION_NODE_OUTPUTS"));
			assertEquals(0, AutomationRunStore.deleteRuns(scope, "brief"));
		}
	}

	@Test
	void deletingOneRunLeavesTheAutomationsOtherRuns() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			savedResultSchema(db);
			db.execute(
					"CREATE TABLE AUTOMATION_RUN_WAITS (WAIT_ID VARCHAR, RUN_ID VARCHAR, NODE_ID VARCHAR, WAIT_TYPE VARCHAR, AGENT_RUN_ID VARCHAR, ROOM_ID VARCHAR, RESUME_NODE_ID VARCHAR, STATUS VARCHAR, CREATED_BY VARCHAR, STARTED_AT TIMESTAMP, EXPIRES_AT TIMESTAMP, RESOLVED_AT TIMESTAMP, RESOLVED_BY VARCHAR)");
			String scope = AutomationConstants.INLINE_RUN_SCOPE_PREFIX + "owner";
			List<Map<String, Object>> nodes = List.of(Map.of("id", "node", "label", "Node"));
			AutomationRunStore.initializeRun("old", scope, "brief", 1, "hash", "{}", Map.of(), "SCHEDULED", "user",
					nodes, Map.of(), Map.of("node", "source"));
			AutomationRunStore.initializeRun("new", scope, "brief", 1, "hash", "{}", Map.of(), "SCHEDULED", "user",
					nodes, Map.of(), Map.of("node", "source"));
			db.execute("INSERT INTO AUTOMATION_RUN_WAITS (WAIT_ID, RUN_ID, NODE_ID, STATUS) "
					+ "VALUES ('wait', 'new', 'node', 'RESOLVED')",
					"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, RUN_ID) VALUES ('result', 'new')",
					"INSERT INTO AUTOMATION_RESULT_CHUNKS (RESULT_ID, CHUNK_INDEX) VALUES ('result', 0)");

			// the run id alone is not enough: another scope or automation deletes nothing
			assertEquals(0, AutomationRunStore.deleteRun(AutomationConstants.INLINE_RUN_SCOPE_PREFIX + "other",
					"brief", "new"));
			assertEquals(0, AutomationRunStore.deleteRun(scope, "default", "new"));
			assertEquals(1, AutomationRunStore.deleteRun(scope, "brief", "new"));

			assertEquals(1, db.count("AUTOMATION_RUNS"));
			assertEquals("old", db.value("SELECT RUN_ID FROM AUTOMATION_RUNS"));
			assertEquals(1, db.count("AUTOMATION_RUN_NODE_SOURCES"));
			assertEquals(1, db.count("AUTOMATION_NODE_OUTPUTS"));
			assertEquals(0, db.count("AUTOMATION_RUN_WAITS"));
			assertEquals(0, db.count("AUTOMATION_SAVED_RESULTS"));
			assertEquals(0, db.count("AUTOMATION_RESULT_CHUNKS"));
			assertEquals(0, AutomationRunStore.deleteRun(scope, "brief", "new"));
		}
	}

	@Test
	void lookAlikeScopeGetsNoRunsFromADatabaseThatIgnoresCase() throws Exception {
		String scope = AutomationConstants.INLINE_RUN_SCOPE_PREFIX + "owner";
		String lookAlike = scope.toLowerCase();
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(prerna.util.QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			// Stands in for SQL Server or MySQL, whose default collation matches the look-alike id.
			queries.when(() -> prerna.util.QueryExecutionUtility.flushRsToMap(eq(db.engine),
					any(prerna.query.querystruct.SelectQueryStruct.class)))
					.thenReturn(List.of(Map.of("RUN_ID", "brief", "PROJECT_ID", scope)));

			assertTrue(AutomationRunStore.getRunsForProject(lookAlike, 10).isEmpty());
			assertFalse(AutomationRunStore.hasAgentRunTrace(lookAlike, "brief", "node", "agent"));
			assertEquals(1, AutomationRunStore.getRunsForProject(scope, 10).size());
			assertTrue(AutomationRunStore.hasAgentRunTrace(scope, "brief", "node", "agent"));
		}
	}

	@Test
	void deletingRunsWithoutTheSchedulerDatabaseIsANoOp() {
		try (var registry = mockStatic(SystemEngineRegistry.class)) {
			assertEquals(0, AutomationRunStore.deleteRuns("scope", "automation"));
		}
	}

	@Test
	void staleRecoveryLeavesFreshHeartbeatsRunning() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(prerna.util.QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			schema(db);
			initialize("stale", Map.of());
			initialize("fresh", Map.of());
			var stale = java.sql.Timestamp.from(java.time.Instant.now().minusSeconds(86400));
			var fresh = java.sql.Timestamp.from(java.time.Instant.now());
			try (var ps = db.connection
					.prepareStatement("UPDATE AUTOMATION_RUNS SET LAST_HEARTBEAT=? WHERE RUN_ID='stale'")) {
				ps.setTimestamp(1, stale);
				ps.executeUpdate();
			}
			queries.when(() -> prerna.util.QueryExecutionUtility.flushRsToMap(eq(db.engine),
					any(prerna.query.querystruct.SelectQueryStruct.class)))
					.thenReturn(List.of(Map.of("RUN_ID", "stale", "STATUS", "SUBMITTED", "LAST_HEARTBEAT", stale),
							Map.of("RUN_ID", "fresh", "STATUS", "SUBMITTED", "LAST_HEARTBEAT", fresh)));
			AutomationRunStore.markStaleRunsInterrupted();
			assertEquals("INTERRUPTED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='stale'"));
			assertEquals("SUBMITTED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='fresh'"));
		}
	}

	@Test
	void schemaUpgradeIsIdempotentAcrossTwoInitializations() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.execute("ALTER TABLE AUTOMATION_NODE_OUTPUTS DROP COLUMN OUTPUT_KIND");

			AutomationRunStore.initialize();
			AutomationRunStore.initialize();

			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
					+ "WHERE TABLE_NAME='AUTOMATION_NODE_OUTPUTS' AND COLUMN_NAME='OUTPUT_KIND'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_RUNS'")).intValue());
		}
	}
}
