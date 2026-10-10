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
				"CREATE TABLE AUTOMATION_RUNS (RUN_ID VARCHAR PRIMARY KEY, PROJECT_ID VARCHAR, AUTOMATION_ID VARCHAR, DEFINITION_VERSION INT, DEFINITION_HASH VARCHAR, DEFINITION_SNAPSHOT CLOB, INPUT_SNAPSHOT CLOB, STATUS VARCHAR, TRIGGER_TYPE VARCHAR, STARTED_AT TIMESTAMP, LAST_HEARTBEAT TIMESTAMP, TOTAL_NODES INT, COMPLETED_NODES INT, CREATED_BY VARCHAR, CANCEL_REQUESTED BOOLEAN, RESULT_SUMMARY VARCHAR, COMPLETED_AT TIMESTAMP, FAILED_NODE_ID VARCHAR, ERROR_MESSAGE VARCHAR)",
				"CREATE TABLE AUTOMATION_RUN_NODE_SOURCES (RUN_ID VARCHAR, NODE_ID VARCHAR, SOURCE_HASH VARCHAR, SOURCE_CODE CLOB)",
				"CREATE TABLE AUTOMATION_NODE_OUTPUTS (RUN_ID VARCHAR, NODE_ID VARCHAR, NODE_LABEL VARCHAR, EXECUTION_ORDER INT, STATUS VARCHAR, ROOM_ID VARCHAR, WORKSPACE_ID VARCHAR, STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, DURATION_MS BIGINT, OUTPUT_VAR_NAME VARCHAR, OUTPUT_KIND VARCHAR, OUTPUT_VALUE CLOB, OUTPUT_PREVIEW VARCHAR, MODEL_MESSAGE_ID VARCHAR, AGENT_RUN_ID VARCHAR, ERROR_MESSAGE VARCHAR)");
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
					AutomationConstants.OUTPUT_KIND_FRAME, "  {}  ", "preview", null, "agent");
			assertEquals("  {}  ", db.value("SELECT OUTPUT_VALUE FROM AUTOMATION_NODE_OUTPUTS WHERE NODE_ID='node'"));
			assertEquals(AutomationConstants.OUTPUT_KIND_FRAME,
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
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_RUN_DATA'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_RUN_DATA_CHUNKS'")).intValue());
		}
	}
}
