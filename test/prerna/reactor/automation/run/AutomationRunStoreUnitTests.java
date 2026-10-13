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
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

	@Test
	void operationalMetricsRequireSchedulerDatabase() {
		try (var registry = mockStatic(SystemEngineRegistry.class, CALLS_REAL_METHODS)) {
			registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
			IllegalStateException error = assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.operationalMetrics("project"));
			assertEquals("System database 'scheduler' is required for collecting Automation operational metrics",
					error.getMessage());
		}
	}

	private void schema(JdbcTestDatabase db) throws Exception {
		db.execute(
				"CREATE TABLE AUTOMATION_RUNS (RUN_ID VARCHAR PRIMARY KEY, PROJECT_ID VARCHAR, AUTOMATION_ID VARCHAR, DEFINITION_VERSION INT, DEFINITION_HASH VARCHAR, DEFINITION_SNAPSHOT CLOB, EXECUTION_CONTRACT_VERSION INT, EXECUTION_CONTRACT_SNAPSHOT CLOB, INPUT_SNAPSHOT CLOB, STATUS VARCHAR, TRIGGER_TYPE VARCHAR, STARTED_AT TIMESTAMP, LAST_HEARTBEAT TIMESTAMP, ATTEMPT_ID VARCHAR, ATTEMPT_NUMBER INT, LEASE_OWNER VARCHAR, LEASE_TOKEN VARCHAR, LEASE_EXPIRES_AT TIMESTAMP, TOTAL_NODES INT, COMPLETED_NODES INT, CREATED_BY VARCHAR, PRINCIPAL_ACCESS VARCHAR, ASYNC_ADMISSION BOOLEAN, CANCEL_REQUESTED BOOLEAN, RESULT_SUMMARY VARCHAR, COMPLETED_AT TIMESTAMP, FAILED_NODE_ID VARCHAR, ERROR_MESSAGE VARCHAR)",
				"CREATE TABLE AUTOMATION_ADMISSION_LOCK (LOCK_ID VARCHAR PRIMARY KEY)",
				"INSERT INTO AUTOMATION_ADMISSION_LOCK (LOCK_ID) VALUES ('GLOBAL')",
				"CREATE TABLE AUTOMATION_RUN_NODE_SOURCES (RUN_ID VARCHAR, NODE_ID VARCHAR, SOURCE_HASH VARCHAR, SOURCE_CODE CLOB)",
				"CREATE TABLE AUTOMATION_NODE_OUTPUTS (RUN_ID VARCHAR, NODE_ID VARCHAR, PARENT_NODE_ID VARCHAR, ITERATION_INDEX INT, SOURCE_NODE_ID VARCHAR, NODE_LABEL VARCHAR, EXECUTION_ORDER INT, STATUS VARCHAR, ROOM_ID VARCHAR, WORKSPACE_ID VARCHAR, STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, DURATION_MS BIGINT, OUTPUT_VAR_NAME VARCHAR, OUTPUT_KIND VARCHAR, OUTPUT_VALUE CLOB, OUTPUT_PREVIEW VARCHAR, MODEL_MESSAGE_ID VARCHAR, AGENT_RUN_ID VARCHAR, ERROR_MESSAGE VARCHAR)",
				"CREATE TABLE AUTOMATION_SAVED_RESULTS (RESULT_ID VARCHAR PRIMARY KEY, PROJECT_ID VARCHAR, RUN_ID VARCHAR, NODE_ID VARCHAR, PARENT_NODE_ID VARCHAR, ITERATION_INDEX INT, RESULT_KIND VARCHAR, FORMAT_VERSION INT, ROW_COUNT BIGINT, BYTE_COUNT BIGINT, CHECKSUM VARCHAR, CREATED_AT TIMESTAMP, RETAIN_UNTIL TIMESTAMP, CAPTURE_STATE VARCHAR, RESULT_ROLE VARCHAR, LOGICAL_NAME VARCHAR, PAYLOAD_VALUE CLOB)",
				"CREATE TABLE AUTOMATION_RESULT_CHUNKS (RESULT_ID VARCHAR, CHUNK_INDEX INT, FIRST_ROW_INDEX BIGINT, CHUNK_ROW_COUNT BIGINT, BYTE_COUNT BIGINT, CHECKSUM VARCHAR, CHUNK_VALUE CLOB)",
				"CREATE TABLE AUTOMATION_ITEMS (ITEM_ID VARCHAR PRIMARY KEY, RUN_ID VARCHAR, NODE_ID VARCHAR, ITEM_INDEX BIGINT, ITEM_INPUT_SNAPSHOT CLOB, ITEM_REFERENCE_SNAPSHOT CLOB, STATUS VARCHAR, ATTEMPT_ID VARCHAR, ATTEMPT_NUMBER INT, MAX_ATTEMPTS INT, LEASE_OWNER VARCHAR, LEASE_TOKEN VARCHAR, LEASE_EXPIRES_AT TIMESTAMP, NEXT_ATTEMPT_AT TIMESTAMP, CREATED_AT TIMESTAMP, STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, ERROR_MESSAGE CLOB)",
				"CREATE TABLE AUTOMATION_ITEM_ATTEMPTS (ATTEMPT_ID VARCHAR PRIMARY KEY, ITEM_ID VARCHAR, RUN_ID VARCHAR, ATTEMPT_NUMBER INT, STATUS VARCHAR, LEASE_OWNER VARCHAR, LEASE_TOKEN VARCHAR, STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, ERROR_MESSAGE CLOB)",
				"CREATE TABLE AUTOMATION_EFFECTS (EFFECT_ID VARCHAR PRIMARY KEY, RUN_ID VARCHAR, NODE_ID VARCHAR, ITEM_ID VARCHAR, ATTEMPT_ID VARCHAR, EFFECT_STATE VARCHAR, OPERATION_KEY VARCHAR, IDEMPOTENCY_KEY VARCHAR, RECEIPT_HASH VARCHAR, RECEIPT_METADATA CLOB, PREPARED_AT TIMESTAMP, ACCEPTED_AT TIMESTAMP, COMMITTED_AT TIMESTAMP, ERROR_MESSAGE CLOB)");
	}

	private void initializeAsync(String runId, String projectId) {
		AutomationRunStore.initializeRun(runId, projectId, "automation", 1, "hash", "{}", 1,
				"{\"version\":1,\"nodes\":{}}", Map.of(), "manual", "user", "NATIVE:user", true,
				List.of(), Map.of(), Map.of());
	}

	@Test
	void operationalMetricsAreProjectScopedBoundedAggregates() throws Exception {
		String previousGlobal = System.getProperty("semoss.automation.admission.global");
		String previousProject = System.getProperty("semoss.automation.admission.project");
		String previousWorkers = System.getProperty("semoss.automation.worker.concurrency");
		String previousItemWorkers = System.getProperty("semoss.automation.itemWorker.concurrency");
		System.setProperty("semoss.automation.admission.global", "77");
		System.setProperty("semoss.automation.admission.project", "12");
		System.setProperty("semoss.automation.worker.concurrency", "3");
		System.setProperty("semoss.automation.itemWorker.concurrency", "5");
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.execute(
					"INSERT INTO AUTOMATION_RUNS (RUN_ID, PROJECT_ID, STATUS, STARTED_AT, LEASE_TOKEN, LEASE_EXPIRES_AT) VALUES "
							+ "('submitted','project-a','SUBMITTED',TIMESTAMP '2020-01-01 00:00:00','fresh-run',TIMESTAMP '2099-01-01 00:00:00'),"
							+ "('running','project-a','RUNNING',CURRENT_TIMESTAMP,'expired-run',TIMESTAMP '2020-01-01 00:00:00'),"
							+ "('wait-input','project-a','WAITING_FOR_INPUT',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('wait-items','project-a','WAITING_FOR_ITEMS',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('interrupted','project-a','INTERRUPTED',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('cancelled','project-a','CANCELLED',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('failed','project-a','FAILED',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('success','project-a','SUCCESS',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('blocked','project-a','BLOCKED',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('unexpected','project-a','UNEXPECTED',CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('other-project','project-b','SUBMITTED',TIMESTAMP '2010-01-01 00:00:00','other',TIMESTAMP '2020-01-01 00:00:00')",
					"INSERT INTO AUTOMATION_ITEMS (ITEM_ID, RUN_ID, NODE_ID, ITEM_INDEX, ITEM_INPUT_SNAPSHOT, STATUS, ATTEMPT_NUMBER, MAX_ATTEMPTS, CREATED_AT, LEASE_TOKEN, LEASE_EXPIRES_AT) VALUES "
							+ "('pending-item','submitted','node',0,'{}','PENDING',0,3,CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('running-item','submitted','node',1,'{}','RUNNING',1,3,CURRENT_TIMESTAMP,'fresh-item',TIMESTAMP '2099-01-01 00:00:00'),"
							+ "('retry-item','submitted','node',2,'{}','RETRY',1,3,CURRENT_TIMESTAMP,'expired-item',TIMESTAMP '2020-01-01 00:00:00'),"
							+ "('success-item','submitted','node',3,'{}','SUCCESS',1,3,CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('failed-item','submitted','node',4,'{}','FAILED',1,3,CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('cancelled-item','submitted','node',5,'{}','CANCELLED',1,3,CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('blocked-item','submitted','node',6,'{}','BLOCKED',1,3,CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('unknown-item','submitted','node',7,'{}','UNEXPECTED',1,3,CURRENT_TIMESTAMP,NULL,NULL),"
							+ "('other-item','other-project','node',0,'{}','FAILED',1,3,CURRENT_TIMESTAMP,NULL,NULL)",
					"INSERT INTO AUTOMATION_EFFECTS (EFFECT_ID, RUN_ID, NODE_ID, EFFECT_STATE, OPERATION_KEY, PREPARED_AT) VALUES "
							+ "('prepared-effect','submitted','node','PREPARED','one',CURRENT_TIMESTAMP),"
							+ "('accepted-effect','submitted','node','ACCEPTED','two',CURRENT_TIMESTAMP),"
							+ "('committed-effect','submitted','node','COMMITTED','three',CURRENT_TIMESTAMP),"
							+ "('unknown-effect','submitted','node','UNKNOWN','four',CURRENT_TIMESTAMP),"
							+ "('unexpected-effect','submitted','node','UNEXPECTED','five',CURRENT_TIMESTAMP),"
							+ "('other-effect','other-project','node','COMMITTED','six',CURRENT_TIMESTAMP)",
					"INSERT INTO AUTOMATION_SAVED_RESULTS (RESULT_ID, PROJECT_ID, RUN_ID, NODE_ID, RESULT_KIND, FORMAT_VERSION, BYTE_COUNT, CHECKSUM, CREATED_AT, CAPTURE_STATE, RESULT_ROLE) VALUES "
							+ "('retained-result','project-a','submitted','node','VALUE',1,100,'a',CURRENT_TIMESTAMP,'RETAINED','OUTPUT'),"
							+ "('expired-result','project-a','submitted','node','VALUE',1,25,'b',CURRENT_TIMESTAMP,'EXPIRED','OUTPUT'),"
							+ "('failed-result','project-a','submitted','node','VALUE',1,999,'c',CURRENT_TIMESTAMP,'FAILED','OUTPUT'),"
							+ "('other-result','project-b','other-project','node','VALUE',1,1000,'d',CURRENT_TIMESTAMP,'RETAINED','OUTPUT')");

			Map<String, Object> metrics = AutomationRunStore.operationalMetrics("project-a");
			assertEquals(Set.of("projectId", "collectedAt", "runs", "leases", "durableItems", "effects",
					"retention", "limits"), metrics.keySet());
			Map<String, Object> runs = nestedMap(metrics, "runs");
			assertEquals(Set.of("submitted", "running", "waiting", "interrupted", "cancelled", "failed",
					"success", "blocked", "unknown", "oldestQueueAgeMs"), runs.keySet());
			assertEquals(1L, runs.get("submitted"));
			assertEquals(1L, runs.get("running"));
			assertEquals(2L, runs.get("waiting"));
			assertEquals(1L, runs.get("interrupted"));
			assertEquals(1L, runs.get("cancelled"));
			assertEquals(1L, runs.get("failed"));
			assertEquals(1L, runs.get("success"));
			assertEquals(1L, runs.get("blocked"));
			assertEquals(1L, runs.get("unknown"));
			assertTrue((Long) runs.get("oldestQueueAgeMs") > 0);

			Map<String, Object> leases = nestedMap(metrics, "leases");
			assertEquals(Set.of("fresh", "expired", "runFresh", "runExpired", "itemFresh", "itemExpired"),
					leases.keySet());
			assertEquals(2L, leases.get("fresh"));
			assertEquals(2L, leases.get("expired"));
			assertEquals(1L, leases.get("runFresh"));
			assertEquals(1L, leases.get("itemExpired"));

			Map<String, Object> items = nestedMap(metrics, "durableItems");
			assertEquals(Set.of("pending", "running", "retry", "success", "failed", "cancelled", "blocked",
					"unknown"), items.keySet());
			assertEquals(1L, items.get("retry"));
			assertEquals(1L, items.get("blocked"));
			assertEquals(1L, items.get("unknown"));

			Map<String, Object> effects = nestedMap(metrics, "effects");
			assertEquals(Set.of("prepared", "accepted", "committed", "unknown"), effects.keySet());
			assertEquals(1L, effects.get("prepared"));
			assertEquals(1L, effects.get("accepted"));
			assertEquals(1L, effects.get("committed"));
			assertEquals(2L, effects.get("unknown"));

			Map<String, Object> retention = nestedMap(metrics, "retention");
			assertEquals(Set.of("retainedPayloads", "retainedBytes", "expiredPayloads", "expiredBytes"),
					retention.keySet());
			assertEquals(1L, retention.get("retainedPayloads"));
			assertEquals(100L, retention.get("retainedBytes"));
			assertEquals(1L, retention.get("expiredPayloads"));
			assertEquals(25L, retention.get("expiredBytes"));

			Map<String, Object> limits = nestedMap(metrics, "limits");
			assertEquals(Set.of("globalAdmissionQuota", "projectAdmissionQuota", "runWorkerConcurrency",
					"itemWorkerConcurrency", "retentionDays", "pageDefaultRows", "pageMaxRows", "pageMaxBytes",
					"openMaxRows", "openMaxBytes"), limits.keySet());
			assertEquals(77, limits.get("globalAdmissionQuota"));
			assertEquals(12, limits.get("projectAdmissionQuota"));
			assertEquals(3, limits.get("runWorkerConcurrency"));
			assertEquals(5, limits.get("itemWorkerConcurrency"));
			assertEquals("project-a", metrics.get("projectId"));
			assertNotNull(metrics.get("collectedAt"));
			assertFalse(metrics.containsKey(AutomationConstants.CREATED_BY));
			assertFalse(metrics.containsKey(AutomationConstants.INPUT_SNAPSHOT));
			assertFalse(metrics.containsKey(AutomationConstants.RECEIPT_METADATA));
		} finally {
			restoreProperty("semoss.automation.admission.global", previousGlobal);
			restoreProperty("semoss.automation.admission.project", previousProject);
			restoreProperty("semoss.automation.worker.concurrency", previousWorkers);
			restoreProperty("semoss.automation.itemWorker.concurrency", previousItemWorkers);
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> nestedMap(Map<String, Object> value, String key) {
		return (Map<String, Object>) value.get(key);
	}

	private static void restoreProperty(String name, String value) {
		if (value == null) {
			System.clearProperty(name);
		} else {
			System.setProperty(name, value);
		}
	}

	@Test
	void oldestAsyncAdmissionsAreAtomicallyClaimedWithoutConvoying() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("newer", "project");
			initializeAsync("older", "project");
			db.execute("UPDATE AUTOMATION_RUNS SET STARTED_AT=TIMESTAMP '2020-01-02 00:00:00' WHERE RUN_ID='newer'",
					"UPDATE AUTOMATION_RUNS SET STARTED_AT=TIMESTAMP '2020-01-01 00:00:00' WHERE RUN_ID='older'");

			AutomationRunStore.ClaimedRun first = AutomationRunStore.claimOldestSubmittedRun();
			AutomationRunStore.ClaimedRun second = AutomationRunStore.claimOldestSubmittedRun();

			assertEquals("older", first.runId());
			assertEquals("newer", second.runId());
			assertNull(AutomationRunStore.claimOldestSubmittedRun());
			assertFalse(AutomationRunStore.claimRun("older"));
			assertTrue(AutomationRunStore.activateClaimedLease(first));
			AutomationRunStore.clearClaimedLease(first);
			assertTrue(AutomationRunStore.activateClaimedLease(second));
			AutomationRunStore.clearClaimedLease(second);
		}
	}

	@Test
	void admissionQuotaDeniesProjectWithExplicitError() throws Exception {
		String previous = System.getProperty("semoss.automation.admission.project");
		System.setProperty("semoss.automation.admission.project", "1");
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("first", "project");
			var error = assertThrows(AutomationRunStore.AutomationAdmissionException.class,
					() -> initializeAsync("second", "project"));
			assertTrue(error.getMessage().contains("project 'project'"));
			assertEquals(1, db.count("AUTOMATION_RUNS"));
		} finally {
			if (previous == null) {
				System.clearProperty("semoss.automation.admission.project");
			} else {
				System.setProperty("semoss.automation.admission.project", previous);
			}
		}
	}

	@Test
	void admissionQuotaDeniesGlobalWithExplicitError() throws Exception {
		String previous = System.getProperty("semoss.automation.admission.global");
		System.setProperty("semoss.automation.admission.global", "1");
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("first", "project-a");
			var error = assertThrows(AutomationRunStore.AutomationAdmissionException.class,
					() -> initializeAsync("second", "project-b"));
			assertTrue(error.getMessage().contains("Global Automation admission quota"));
			assertEquals(1, db.count("AUTOMATION_RUNS"));
		} finally {
			if (previous == null) {
				System.clearProperty("semoss.automation.admission.global");
			} else {
				System.setProperty("semoss.automation.admission.global", previous);
			}
		}
	}

	@Test
	void queuedCancellationPreventsClaim() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("queued", "project");
			AutomationRunStore.CancellationResult result =
					AutomationRunStore.cancelQueuedOrRunningRun("queued", "project");
			assertEquals("CANCELLED", result.status());
			assertFalse(result.active());
			assertEquals("CANCELLED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='queued'"));
			assertFalse(AutomationRunStore.claimRun("queued"));
		}
	}

	@Test
	void cancellationSurvivesSubmittedToRunningRace() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("racing", "project");

			// Models a worker claim after the reactor observed SUBMITTED but before its
			// atomic cancellation operation reaches the store.
			db.execute("UPDATE AUTOMATION_RUNS SET STATUS='RUNNING' WHERE RUN_ID='racing'");
			AutomationRunStore.CancellationResult result =
					AutomationRunStore.cancelQueuedOrRunningRun("racing", "project");

			assertEquals("RUNNING", result.status());
			assertTrue(result.active());
			assertEquals(true,
					db.value("SELECT CANCEL_REQUESTED FROM AUTOMATION_RUNS WHERE RUN_ID='racing'"));
		}
	}

	@Test
	void rejectedExecutorSubmissionReleasesOwnedClaimForRetry() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("queued", "project");

			AutomationRunStore.ClaimedRun rejected = AutomationRunStore.claimOldestSubmittedRun();
			AutomationRunStore.reconcileRejectedClaim(rejected);
			assertEquals("SUBMITTED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='queued'"));
			assertNull(db.value("SELECT LEASE_TOKEN FROM AUTOMATION_RUNS WHERE RUN_ID='queued'"));

			AutomationRunStore.ClaimedRun retried = AutomationRunStore.claimOldestSubmittedRun();
			assertEquals("queued", retried.runId());
			assertFalse(rejected.leaseToken().equals(retried.leaseToken()));
		}
	}

	@Test
	void rejectedExecutorSubmissionHonorsConcurrentCancellation() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeAsync("queued", "project");

			AutomationRunStore.ClaimedRun rejected = AutomationRunStore.claimOldestSubmittedRun();
			AutomationRunStore.cancelQueuedOrRunningRun("queued", "project");
			AutomationRunStore.reconcileRejectedClaim(rejected);

			assertEquals("CANCELLED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='queued'"));
			assertNull(db.value("SELECT LEASE_TOKEN FROM AUTOMATION_RUNS WHERE RUN_ID='queued'"));
		}
	}

	@Test
	void synchronousInitializationStoresNoPrincipalSnapshot() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("sync", "project", "automation", 1, "hash", "{}", 1,
					"{\"version\":1,\"nodes\":{}}", Map.of(), "manual", "legacy-user", null, false,
					List.of(), Map.of(), Map.of());

			assertNull(db.value("SELECT PRINCIPAL_ACCESS FROM AUTOMATION_RUNS WHERE RUN_ID='sync'"));
			assertEquals(false,
					db.value("SELECT ASYNC_ADMISSION FROM AUTOMATION_RUNS WHERE RUN_ID='sync'"));
		}
	}

	@Test
	void synchronousDurableInitializationStoresPrincipalSnapshotWithoutAsyncAdmission() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("sync-durable", "project", "automation", 1, "hash", "{}", 1,
					"{\"version\":1,\"nodes\":{}}", Map.of(), "manual", "legacy-user", "NATIVE:user", false,
					List.of(), Map.of(), Map.of());

			assertEquals("NATIVE:user",
					db.value("SELECT PRINCIPAL_ACCESS FROM AUTOMATION_RUNS WHERE RUN_ID='sync-durable'"));
			assertEquals(false,
					db.value("SELECT ASYNC_ADMISSION FROM AUTOMATION_RUNS WHERE RUN_ID='sync-durable'"));
		}
	}

	private void initialize(String runId, Map<String, String> sources) {
		AutomationRunStore.initializeRun(runId, "project", "automation", 1, "hash", "{}", 1,
				"{\"version\":1,\"nodes\":{}}", Map.of("value", "<text>"), "manual", "user", List.of(), Map.of(),
				sources);
	}

	private void initializeWithNode(String runId, String nodeId) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, nodeId);
		node.put(AutomationConstants.NODE_FIELD_LABEL, "Loop");
		node.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_CONTROL_LOOP);
		AutomationRunStore.initializeRun(runId, "project", "automation", 1, "hash", "{}", 1,
				"{\"version\":1,\"nodes\":{}}", Map.of(), "manual", "user", List.of(node), Map.of(), Map.of());
		assertTrue(AutomationRunStore.claimRun(runId));
	}

	private void parkDurableRun(String runId, List<?> snapshots, int maxAttempts) {
		initializeWithNode(runId, "loop");
		AutomationRunStore.markNodeRunning(runId, "loop");
		AutomationRunStore.handoffToDurableItems(runId, "loop", snapshots, maxAttempts);
	}

	@Test
	void durableItemHandoffIsAtomicAcrossInsertionFailure() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.markNodeRunning("run", "loop");
			String collidingId = UUID.nameUUIDFromBytes("run\nloop\n0".getBytes(StandardCharsets.UTF_8)).toString();
			db.execute("INSERT INTO AUTOMATION_ITEMS (ITEM_ID,RUN_ID,NODE_ID,ITEM_INDEX,STATUS,ATTEMPT_NUMBER,"
					+ "MAX_ATTEMPTS,CREATED_AT,NEXT_ATTEMPT_AT) VALUES ('" + collidingId
					+ "','other','loop',0,'PENDING',0,2,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)");

			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 2));

			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertEquals(0L, db.value("SELECT COUNT(*) FROM AUTOMATION_ITEMS WHERE RUN_ID='run'"));
		}
	}

	@Test
	void durableItemHandoffIsIdempotentButRejectsConflictingSnapshots() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.markNodeRunning("run", "loop");
			List<String> first = AutomationRunStore.handoffToDurableItems(
					"run", "loop", List.of(List.of("first"), List.of("second")), 2);

			assertEquals(first, AutomationRunStore.handoffToDurableItems(
					"run", "loop", List.of(List.of("first"), List.of("second")), 2));
			assertEquals("WAITING_FOR_ITEMS",
					db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertEquals(2L, db.value("SELECT COUNT(*) FROM AUTOMATION_ITEMS WHERE RUN_ID='run'"));
			assertThrows(IllegalStateException.class, () -> AutomationRunStore.handoffToDurableItems(
					"run", "loop", List.of(List.of("changed"), List.of("second")), 2));
		}
	}

	@Test
	void startupRecoveryParksPreFixRunningRunWithPendingItems() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.markNodeRunning("run", "loop");
			initializeWithNode("fresh", "loop");
			AutomationRunStore.markNodeRunning("fresh", "loop");
			db.execute("INSERT INTO AUTOMATION_ITEMS (ITEM_ID,RUN_ID,NODE_ID,ITEM_INDEX,STATUS,ATTEMPT_NUMBER,"
					+ "MAX_ATTEMPTS,CREATED_AT,NEXT_ATTEMPT_AT) VALUES ('item','run','loop',0,'PENDING',0,2,"
					+ "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
					"INSERT INTO AUTOMATION_ITEMS (ITEM_ID,RUN_ID,NODE_ID,ITEM_INDEX,STATUS,ATTEMPT_NUMBER,"
							+ "MAX_ATTEMPTS,CREATED_AT,NEXT_ATTEMPT_AT) VALUES "
							+ "('fresh-item','fresh','loop',0,'PENDING',0,2,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
					"UPDATE AUTOMATION_RUNS SET LEASE_EXPIRES_AT=TIMESTAMP '2000-01-01 00:00:00' "
							+ "WHERE RUN_ID='run'");

			assertEquals(1, AutomationRunStore.recoverStrandedDurableItemHandoffs());
			assertEquals("WAITING_FOR_ITEMS",
					db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='fresh'"));
			assertNull(db.value("SELECT LEASE_TOKEN FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertNotNull(AutomationRunStore.claimOldestDurableItem());
		}
	}

	@Test
	void sharedItemQueueGivesSimulatedWorkersDistinctOldestClaims() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			parkDurableRun("run", List.of(List.of("first"), List.of("second")), 2);

			AutomationRunStore.ItemClaim firstWorker = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.ItemClaim secondWorker = AutomationRunStore.claimOldestDurableItem();

			assertNotNull(firstWorker);
			assertNotNull(secondWorker);
			assertEquals(0, firstWorker.itemIndex());
			assertEquals(1, secondWorker.itemIndex());
			assertFalse(firstWorker.itemId().equals(secondWorker.itemId()));
		}
	}

	@Test
	void expiredItemLeaseIsRecoveredAndReclaimedAsNewAttempt() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			parkDurableRun("run", List.of(List.of("item")), 3);
			AutomationRunStore.ItemClaim crashed = AutomationRunStore.claimOldestDurableItem();
			db.execute("UPDATE AUTOMATION_ITEMS SET LEASE_EXPIRES_AT=TIMESTAMP '2000-01-01 00:00:00' "
					+ "WHERE ITEM_ID='" + crashed.itemId() + "'");

			AutomationRunStore.recoverStaleDurableItems();
			AutomationRunStore.ItemClaim reclaimed = AutomationRunStore.claimOldestDurableItem();

			assertNotNull(reclaimed);
			assertEquals(crashed.itemId(), reclaimed.itemId());
			assertFalse(crashed.attemptId().equals(reclaimed.attemptId()));
			assertEquals(2, reclaimed.attemptNumber());
		}
	}

	@Test
	void exactlyOneCoordinatorFinalizesTerminalDurableLoop() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			parkDurableRun("run", List.of(List.of("first"), List.of("second")), 2);
			for (int index = 0; index < 2; index++) {
				AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
				AutomationRunStore.activateItemClaim(claim);
				AutomationRunStore.completeDurableItem(claim);
				AutomationRunStore.clearItemClaim(claim);
			}

			assertTrue(AutomationRunStore.coordinateDurableLoop("run", "loop", "loopResult"));
			assertFalse(AutomationRunStore.coordinateDurableLoop("run", "loop", "loopResult"));
			assertEquals("SUCCESS", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertEquals("SUCCESS",
					db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID='run' AND NODE_ID='loop'"));
		}
	}

	@Test
	void unknownItemEffectBlocksCoordinatorAndRun() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			parkDurableRun("run", List.of(List.of("item")), 3);
			AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(claim);
			AutomationRunStore.EffectInvocation effect =
					AutomationRunStore.prepareEffect("run", "body-execution", "body-source");
			AutomationRunStore.markEffectUnknown(effect.effectId(), "destination status unavailable");
			assertEquals("BLOCKED", AutomationRunStore.failDurableItem(claim, "effect uncertain"));
			AutomationRunStore.clearItemClaim(claim);

			assertTrue(AutomationRunStore.coordinateDurableLoop("run", "loop", "loopResult"));
			assertEquals("BLOCKED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertNull(AutomationRunStore.claimOldestDurableItem());
		}
	}

	@Test
	void durableEffectOperationKeyIsStableWithoutClaimingDestinationIdempotency() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			parkDurableRun("run", List.of(List.of("item")), 2);
			AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(claim);

			AutomationRunStore.EffectInvocation first =
					AutomationRunStore.prepareEffect("run", "execution-1", "source-node");
			AutomationRunStore.EffectInvocation second =
					AutomationRunStore.prepareEffect("run", "execution-2", "source-node");

			assertEquals(first.operationKey(), second.operationKey());
			assertNull(db.value("SELECT IDEMPOTENCY_KEY FROM AUTOMATION_EFFECTS WHERE EFFECT_ID='"
					+ first.effectId() + "'"));
			AutomationRunStore.clearItemClaim(claim);
		}
	}

	@Test
	void parkedRunCancellationCancelsQueuedAndActiveItems() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			parkDurableRun("run", List.of(List.of("active"), List.of("queued")), 2);
			AutomationRunStore.ItemClaim active = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(active);
			AutomationRunStore.prepareEffect("run", "body", "body");

			AutomationRunStore.CancellationResult result =
					AutomationRunStore.cancelQueuedOrRunningRun("run", "project");

			assertEquals("CANCELLED", result.status());
			assertTrue(result.active());
			assertEquals("CANCELLED", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='run'"));
			assertEquals(2L, db.value("SELECT COUNT(*) FROM AUTOMATION_ITEMS WHERE STATUS='CANCELLED'"));
			assertEquals("UNKNOWN", db.value("SELECT EFFECT_STATE FROM AUTOMATION_EFFECTS"));
			assertEquals("FAILED",
					db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID='run' AND NODE_ID='loop'"));
			assertThrows(IllegalStateException.class, () -> AutomationRunStore.completeDurableItem(active));
			AutomationRunStore.clearItemClaim(active);
		}
	}

	@Test
	void durableItemKeepsBusinessIdAndCreatesNewAttemptPerRetry() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			List<String> ids = AutomationRunStore.handoffToDurableItems("run", "loop",
					List.of(List.of(Map.of("case", 1))), 3);
			assertEquals(ids, AutomationRunStore.handoffToDurableItems("run", "loop",
					List.of(List.of(Map.of("case", 1))), 3));

			AutomationRunStore.ItemClaim first = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(first);
			assertEquals("RETRY", AutomationRunStore.failDurableItem(first, "transient"));
			AutomationRunStore.clearItemClaim(first);
			AutomationRunStore.ItemClaim second = AutomationRunStore.claimOldestDurableItem();

			assertEquals(first.itemId(), second.itemId());
			assertFalse(first.attemptId().equals(second.attemptId()));
			assertEquals(1, first.attemptNumber());
			assertEquals(2, second.attemptNumber());
			AutomationRunStore.activateItemClaim(second);
			AutomationRunStore.completeDurableItem(second);
			AutomationRunStore.clearItemClaim(second);
		}
	}

	@Test
	void durableItemClaimIsOldestFirstAndRejectsStaleOwner() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.handoffToDurableItems("run", "loop",
					List.of(List.of("first"), List.of("second")), 2);
			AutomationRunStore.ItemClaim first = AutomationRunStore.claimOldestDurableItem();
			assertEquals(0, first.itemIndex());
			db.execute("UPDATE AUTOMATION_ITEMS SET LEASE_TOKEN='replacement' WHERE ITEM_ID='" + first.itemId() + "'");
			AutomationRunStore.activateItemClaim(first);
			assertThrows(IllegalStateException.class, () -> AutomationRunStore.completeDurableItem(first));
			AutomationRunStore.clearItemClaim(first);
			assertEquals("RUNNING",
					db.value("SELECT STATUS FROM AUTOMATION_ITEMS WHERE ITEM_ID='" + first.itemId() + "'"));
		}
	}

	@Test
	void durableItemRetryCapStopsFurtherClaims() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 2);
			for (int attempt = 1; attempt <= 2; attempt++) {
				AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
				AutomationRunStore.activateItemClaim(claim);
				assertEquals(attempt == 1 ? "RETRY" : "FAILED",
						AutomationRunStore.failDurableItem(claim, "failure"));
				AutomationRunStore.clearItemClaim(claim);
			}
			assertNull(AutomationRunStore.claimOldestDurableItem());
			assertEquals(2, db.count("AUTOMATION_ITEM_ATTEMPTS"));
		}
	}

	@Test
	void unknownEffectBlocksBlindItemRetry() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 3);
			AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(claim);
			AutomationRunStore.EffectInvocation effect = AutomationRunStore.prepareEffect("run", "loop");
			AutomationRunStore.markEffectUnknown(effect.effectId(), "destination outcome unavailable");
			assertEquals("BLOCKED", AutomationRunStore.failDurableItem(claim, "effect uncertain"));
			AutomationRunStore.clearItemClaim(claim);

			assertNull(AutomationRunStore.claimOldestDurableItem());
			assertEquals("UNKNOWN", db.value("SELECT EFFECT_STATE FROM AUTOMATION_EFFECTS"));
		}
	}

	@Test
	void acceptedEffectCommitsWithNodePublication() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 2);
			AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(claim);
			AutomationRunStore.EffectInvocation effect = AutomationRunStore.prepareEffect("run", "loop");
			AutomationRunStore.markEffectAccepted(effect.effectId(), "destination-key",
					"{\"operationId\":\"receipt-1\"}");
			AutomationRunStore.markNodeRunning("run", "loop");
			AutomationRunStore.updateNodeSuccess("run", "loop", new java.sql.Timestamp(System.currentTimeMillis()),
					1, "output", null, "{\"ok\":true}", "ok", null, null, null, effect.effectId());

			assertEquals("COMMITTED", db.value("SELECT EFFECT_STATE FROM AUTOMATION_EFFECTS"));
			assertNotNull(db.value("SELECT COMMITTED_AT FROM AUTOMATION_EFFECTS"));
			AutomationRunStore.completeDurableItem(claim);
			AutomationRunStore.clearItemClaim(claim);
		}
	}

	@Test
	void failedNodePublicationDoesNotFalselyCommitAcceptedEffect() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.EffectInvocation effect = AutomationRunStore.prepareEffect("run", "loop");
			AutomationRunStore.markEffectAccepted(effect.effectId(), null, "{\"operationId\":\"receipt-1\"}");

			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.updateNodeSuccess("run", "missing",
							new java.sql.Timestamp(System.currentTimeMillis()), 1, "output", null, "{}", "ok",
							null, null, null, effect.effectId()));

			assertEquals("ACCEPTED", db.value("SELECT EFFECT_STATE FROM AUTOMATION_EFFECTS"));
			assertNull(db.value("SELECT COMMITTED_AT FROM AUTOMATION_EFFECTS"));
			AutomationRunStore.markEffectUnknown(effect.effectId(), "publication failed");
			assertEquals("UNKNOWN", db.value("SELECT EFFECT_STATE FROM AUTOMATION_EFFECTS"));
		}
	}

	@Test
	void committedEffectBlocksAutomaticReplayAfterLaterItemFailure() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 3);
			AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
			AutomationRunStore.activateItemClaim(claim);
			AutomationRunStore.EffectInvocation effect = AutomationRunStore.prepareEffect("run", "loop");
			AutomationRunStore.markNodeRunning("run", "loop");
			AutomationRunStore.updateNodeSuccess("run", "loop", new java.sql.Timestamp(System.currentTimeMillis()),
					1, "output", null, "{}", "ok", null, null, null, effect.effectId());

			assertEquals("BLOCKED", AutomationRunStore.failDurableItem(claim, "later step failed"));
			AutomationRunStore.clearItemClaim(claim);
			assertNull(AutomationRunStore.claimOldestDurableItem());
		}
	}

	@Test
	void cancellingRunCancelsPendingDurableItems() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			initializeWithNode("run", "loop");
			AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 2);

			AutomationRunStore.CancellationResult result =
					AutomationRunStore.cancelQueuedOrRunningRun("run", "project");

			assertEquals("CANCELLED", result.status());
			assertTrue(result.active());
			assertEquals("CANCELLED", db.value("SELECT STATUS FROM AUTOMATION_ITEMS"));
			assertNull(AutomationRunStore.claimOldestDurableItem());
		}
	}

	@Test
	void cancellationUnknownsEffectsAndFencesStaleItemPublication() throws Exception {
		try (var db = new JdbcTestDatabase()) {
				schema(db);
				initializeWithNode("run", "loop");
				AutomationRunStore.handoffToDurableItems("run", "loop", List.of(List.of("item")), 2);
				AutomationRunStore.ItemClaim claim = AutomationRunStore.claimOldestDurableItem();
				AutomationRunStore.activateItemClaim(claim);
				AutomationRunStore.prepareEffect("run", "loop");
				AutomationRunStore.EffectInvocation effect = AutomationRunStore.prepareEffect("run", "loop");
				AutomationRunStore.markEffectAccepted(effect.effectId(), "destination-key",
						"{\"operationId\":\"receipt-1\"}");
				AutomationRunStore.markNodeRunning("run", "loop");

				AutomationRunStore.cancelQueuedOrRunningRun("run", "project");

				assertEquals(2L, db.value(
						"SELECT COUNT(*) FROM AUTOMATION_EFFECTS WHERE EFFECT_STATE='UNKNOWN'"));
				assertEquals("CANCELLED", db.value("SELECT STATUS FROM AUTOMATION_ITEM_ATTEMPTS"));
				assertNull(db.value("SELECT LEASE_TOKEN FROM AUTOMATION_ITEMS"));
				assertThrows(IllegalStateException.class,
						() -> AutomationRunStore.updateNodeSuccess("run", "loop",
								java.sql.Timestamp.from(java.time.Instant.now()), 1, "output", null,
								"{\"stale\":true}", "stale", null, null, null, effect.effectId()));
				assertEquals(0, db.count("AUTOMATION_SAVED_RESULTS"));
				assertEquals(2L, db.value(
						"SELECT COUNT(*) FROM AUTOMATION_EFFECTS WHERE EFFECT_STATE='UNKNOWN'"));
				assertEquals("FAILED", db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS"));
				AutomationRunStore.clearItemClaim(claim);
		}
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
			assertEquals(1, db.value("SELECT EXECUTION_CONTRACT_VERSION FROM AUTOMATION_RUNS"));
			assertEquals("{\"version\":1,\"nodes\":{}}",
					db.value("SELECT EXECUTION_CONTRACT_SNAPSHOT FROM AUTOMATION_RUNS"));
			try (var queries = mockStatic(prerna.util.QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
				queries.when(() -> prerna.util.QueryExecutionUtility.flushRsToMap(eq(db.engine),
						any(prerna.query.querystruct.SelectQueryStruct.class)))
						.thenReturn(List.of(Map.of(
								AutomationConstants.EXECUTION_CONTRACT_VERSION, 1,
								AutomationConstants.EXECUTION_CONTRACT_SNAPSHOT,
								"{\"version\":1,\"nodes\":{}}")));
				AutomationRunStore.PersistedExecutionContract contract =
						AutomationRunStore.getRunExecutionContract("run");
				assertEquals(1, contract.version());
				assertEquals("{\"version\":1,\"nodes\":{}}", contract.snapshot());
			}
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
	void staleLeaseCannotHeartbeatOrCompleteAfterTakeover() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("leased", "project", "automation", 1, "hash", "{}", 3,
					"{\"version\":3,\"nodes\":{}}",
					Map.of(), "manual", "user", List.of(Map.of("id", "node", "label", "Node")), Map.of(), Map.of());
			assertTrue(AutomationRunStore.claimRun("leased"));
			db.execute("UPDATE AUTOMATION_RUNS SET LEASE_TOKEN='replacement' WHERE RUN_ID='leased'");

			assertFalse(AutomationRunStore.touchHeartbeat("leased"));
			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.markNodeRunning("leased", "node"));
			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.updateNodeSuccess("leased", "node",
							java.sql.Timestamp.from(java.time.Instant.now()), 1, "output", "\"stale\"",
							"stale", null, null));
			assertThrows(IllegalStateException.class,
					() -> AutomationRunStore.completeRun("leased", "project", "SUCCESS", null, null));
			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='leased'"));
			assertEquals("PENDING",
					db.value("SELECT STATUS FROM AUTOMATION_NODE_OUTPUTS WHERE RUN_ID='leased' AND NODE_ID='node'"));
			assertEquals(0, ((Number) db.value("SELECT COUNT(*) FROM AUTOMATION_SAVED_RESULTS")).intValue());
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
			var tableCapture = new AutomationTableCaptureService.TableCapture(
					"[{\"name\":\"ID\",\"type\":\"INT\"}]",
					List.of(new AutomationTableCaptureService.TableChunk(0, 0, 2, "[[1],[2]]", 9, "chunk-checksum")),
					2, 9, "table-checksum");
			AutomationRunStore.updateNodeSuccess("run", "node", started, 5L, "output",
					AutomationConstants.OUTPUT_KIND_TABLE, "  {}  ", "preview", null, "agent", tableCapture);
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
			assertEquals(2, db.count("AUTOMATION_SAVED_RESULTS"));
			assertEquals("TABLE",
					db.value("SELECT RESULT_KIND FROM AUTOMATION_SAVED_RESULTS WHERE NODE_ID='node'"));
			assertEquals(2L, db.value("SELECT ROW_COUNT FROM AUTOMATION_SAVED_RESULTS WHERE NODE_ID='node'"));
			assertEquals("[[1],[2]]", db.value("SELECT CHUNK_VALUE FROM AUTOMATION_RESULT_CHUNKS"));
			assertEquals("VALUE",
					db.value("SELECT RESULT_KIND FROM AUTOMATION_SAVED_RESULTS WHERE NODE_ID='summary'"));
			assertEquals((long) ordinarySummary.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
					db.value("SELECT BYTE_COUNT FROM AUTOMATION_SAVED_RESULTS WHERE NODE_ID='summary'"));
			assertEquals(64, ((String) db
					.value("SELECT CHECKSUM FROM AUTOMATION_SAVED_RESULTS WHERE NODE_ID='summary'")).length());
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
	void expirationRemovesPayloadButPreservesMetadata() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			AutomationRunStore.initializeRun("run", "project", "automation", 1, "hash", "{}", Map.of(), "manual",
					"user", List.of(Map.of("id", "node", "label", "Node")), Map.of(), Map.of());
			assertTrue(AutomationRunStore.claimRun("run"));
			AutomationRunStore.markNodeRunning("run", "node");
			AutomationRunStore.updateNodeSuccess("run", "node", java.sql.Timestamp.from(java.time.Instant.now()), 1,
					"output", "{\"value\":1}", "value", null, null);
			db.execute("UPDATE AUTOMATION_RUNS SET STATUS='SUCCESS' WHERE RUN_ID='run'",
					"UPDATE AUTOMATION_SAVED_RESULTS SET RETAIN_UNTIL=TIMESTAMP '2000-01-01 00:00:00'");

			assertEquals(1, AutomationRunStore.expireSavedResults(java.time.Instant.now(), 10));
			assertEquals("EXPIRED", db.value("SELECT CAPTURE_STATE FROM AUTOMATION_SAVED_RESULTS"));
			assertNull(db.value("SELECT PAYLOAD_VALUE FROM AUTOMATION_SAVED_RESULTS"));
			assertEquals(64, ((String) db.value("SELECT CHECKSUM FROM AUTOMATION_SAVED_RESULTS")).length());
		}
	}

	@Test
	void effectReceiptExpirationIncludesRunOwnedEffectsWithoutItems() throws Exception {
		try (var db = new JdbcTestDatabase()) {
				schema(db);
				initializeWithNode("run", "loop");
				AutomationRunStore.EffectInvocation effect = AutomationRunStore.prepareEffect("run", "loop");
				AutomationRunStore.markEffectAccepted(effect.effectId(), "destination-key",
						"{\"operationId\":\"receipt-1\"}");
				String hash = (String) db.value("SELECT RECEIPT_HASH FROM AUTOMATION_EFFECTS");
				String operationKey = (String) db.value("SELECT OPERATION_KEY FROM AUTOMATION_EFFECTS");
				db.execute("UPDATE AUTOMATION_RUNS SET STATUS='SUCCESS',"
						+ "COMPLETED_AT=TIMESTAMP '2000-01-01 00:00:00' WHERE RUN_ID='run'");

				assertEquals(1, AutomationRunStore.expireDurableItemPayloads(java.time.Instant.now(), 10));
				assertNull(db.value("SELECT RECEIPT_METADATA FROM AUTOMATION_EFFECTS"));
				assertNull(db.value("SELECT ITEM_ID FROM AUTOMATION_EFFECTS"));
				assertEquals("ACCEPTED", db.value("SELECT EFFECT_STATE FROM AUTOMATION_EFFECTS"));
				assertEquals(hash, db.value("SELECT RECEIPT_HASH FROM AUTOMATION_EFFECTS"));
				assertEquals(operationKey, db.value("SELECT OPERATION_KEY FROM AUTOMATION_EFFECTS"));
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
	void staleRecoveryUsesLeaseExpiryForRunningOwners() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(prerna.util.QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			schema(db);
			initialize("expired", Map.of());
			initialize("fresh", Map.of());
			initialize("malformed", Map.of());
			var expiredAt = java.sql.Timestamp.from(java.time.Instant.now().minusSeconds(86400));
			var freshUntil = java.sql.Timestamp.from(java.time.Instant.now().plusSeconds(86400));
			try (var ps = db.connection.prepareStatement(
					"UPDATE AUTOMATION_RUNS SET STATUS='RUNNING', LEASE_TOKEN=?, LEASE_EXPIRES_AT=? WHERE RUN_ID=?")) {
				ps.setString(1, "expired-token");
				ps.setTimestamp(2, expiredAt);
				ps.setString(3, "expired");
				ps.executeUpdate();
				ps.setString(1, "fresh-token");
				ps.setTimestamp(2, freshUntil);
				ps.setString(3, "fresh");
				ps.executeUpdate();
			}
			var staleHeartbeat = java.sql.Timestamp.from(java.time.Instant.now().minusSeconds(3600));
			var malformedLease = java.sql.Timestamp.from(java.time.Instant.now().plusSeconds(10800));
			try (var ps = db.connection.prepareStatement(
					"UPDATE AUTOMATION_RUNS SET STATUS='RUNNING', LAST_HEARTBEAT=?, LEASE_TOKEN=?, "
							+ "LEASE_EXPIRES_AT=? WHERE RUN_ID='malformed'")) {
				ps.setTimestamp(1, staleHeartbeat);
				ps.setString(2, "malformed-token");
				ps.setTimestamp(3, malformedLease);
				ps.executeUpdate();
			}
			queries.when(() -> prerna.util.QueryExecutionUtility.flushRsToMap(eq(db.engine),
					any(prerna.query.querystruct.SelectQueryStruct.class)))
					.thenReturn(List.of(
							Map.of("RUN_ID", "expired", "STATUS", "RUNNING",
									AutomationConstants.LEASE_TOKEN, "expired-token",
									AutomationConstants.LEASE_EXPIRES_AT,
									new prerna.date.SemossDate(expiredAt, java.time.ZoneId.systemDefault(),
											"yyyy-MM-dd HH:mm:ss")),
							Map.of("RUN_ID", "fresh", "STATUS", "RUNNING",
									AutomationConstants.LEASE_TOKEN, "fresh-token",
									AutomationConstants.LEASE_EXPIRES_AT,
									new prerna.date.SemossDate(freshUntil, java.time.ZoneId.systemDefault(),
											"yyyy-MM-dd HH:mm:ss"))));

			AutomationRunStore.markStaleRunsInterrupted();

			assertEquals("INTERRUPTED",
					db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='expired'"));
			assertEquals("RUNNING", db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='fresh'"));
			assertEquals("INTERRUPTED",
					db.value("SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID='malformed'"));
			assertNull(db.value("SELECT LEASE_TOKEN FROM AUTOMATION_RUNS WHERE RUN_ID='expired'"));
		}
	}

	@Test
	void schemaUpgradeIsIdempotentAcrossTwoInitializations() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.execute(
					"ALTER TABLE AUTOMATION_NODE_OUTPUTS DROP COLUMN OUTPUT_KIND",
					"ALTER TABLE AUTOMATION_RUNS DROP COLUMN EXECUTION_CONTRACT_VERSION",
					"ALTER TABLE AUTOMATION_RUNS DROP COLUMN EXECUTION_CONTRACT_SNAPSHOT",
					"DROP TABLE AUTOMATION_RESULT_CHUNKS",
					"DROP TABLE AUTOMATION_SAVED_RESULTS",
					"DROP TABLE AUTOMATION_EFFECTS",
					"DROP TABLE AUTOMATION_ITEM_ATTEMPTS",
					"DROP TABLE AUTOMATION_ITEMS",
					"INSERT INTO AUTOMATION_RUNS (RUN_ID, PROJECT_ID, STATUS, TRIGGER_TYPE, STARTED_AT) "
							+ "VALUES ('legacy', 'project', 'WAITING_FOR_INPUT', 'manual', CURRENT_TIMESTAMP)");

			AutomationRunStore.initialize();
			AutomationRunStore.initialize();

			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
					+ "WHERE TABLE_NAME='AUTOMATION_NODE_OUTPUTS' AND COLUMN_NAME='OUTPUT_KIND'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_RUNS'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
					+ "WHERE TABLE_NAME='AUTOMATION_RUNS' AND COLUMN_NAME='EXECUTION_CONTRACT_VERSION'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
					+ "WHERE TABLE_NAME='AUTOMATION_RUNS' AND COLUMN_NAME='EXECUTION_CONTRACT_SNAPSHOT'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_SAVED_RESULTS'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_RESULT_CHUNKS'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_ITEMS'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_ITEM_ATTEMPTS'")).intValue());
			assertEquals(1, ((Number) db.value("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
					+ "WHERE TABLE_NAME='AUTOMATION_EFFECTS'")).intValue());
			assertEquals(1, db.count("AUTOMATION_RUNS"));
			assertNull(db.value("SELECT EXECUTION_CONTRACT_VERSION FROM AUTOMATION_RUNS WHERE RUN_ID='legacy'"));
			assertNull(db.value("SELECT EXECUTION_CONTRACT_SNAPSHOT FROM AUTOMATION_RUNS WHERE RUN_ID='legacy'"));
			try (var queries = mockStatic(prerna.util.QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
				queries.when(() -> prerna.util.QueryExecutionUtility.flushRsToMap(eq(db.engine),
						any(prerna.query.querystruct.SelectQueryStruct.class)))
						.thenReturn(List.of(new java.util.HashMap<>()));
				AutomationRunStore.PersistedExecutionContract legacy =
						AutomationRunStore.getRunExecutionContract("legacy");
				assertNull(legacy.version());
				assertNull(legacy.snapshot());
			}
		}
	}
}
