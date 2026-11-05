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
package prerna.reactor.scheduler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class SchedulerDatabaseUtilityUnitTests {

	@Test
	void tagReplacementRollsBackOnBatchFailureAndRetainsTrimming() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE SMSS_JOB_TAGS (JOB_ID VARCHAR, JOB_TAG VARCHAR CHECK (JOB_TAG <> 'reject'))",
					"INSERT INTO SMSS_JOB_TAGS VALUES ('job','original')");
			assertFalse(SchedulerDatabaseUtility.updateJobTags("job", List.of("ok", "reject")));
			assertEquals("original", db.value("SELECT JOB_TAG FROM SMSS_JOB_TAGS"));
			verify(db.connection).rollback();
			assertTrue(SchedulerDatabaseUtility.updateJobTags("job", List.of("  spaced  ", "")));
			assertEquals(2, db.count("SMSS_JOB_TAGS"));
			assertEquals("spaced", db.value("SELECT JOB_TAG FROM SMSS_JOB_TAGS WHERE JOB_TAG <> ''"));
			assertTrue(SchedulerDatabaseUtility.updateJobTags("job", null));
			assertEquals(0, db.count("SMSS_JOB_TAGS"));
		}
	}

	@Test
	void executionLifecycleWorksWithManualConnectionAndZeroRowDelete() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE SMSS_EXECUTION (EXEC_ID VARCHAR, JOB_ID VARCHAR, JOB_GROUP VARCHAR)");
			db.manual();
			assertTrue(SchedulerDatabaseUtility.insertIntoExecutionTable("exec", "job", "group"));
			db.connection.rollback();
			assertArrayEquals(new String[] { "job", "group" }, SchedulerDatabaseUtility.executionIdExists("exec"));
			assertNull(SchedulerDatabaseUtility.executionIdExists("missing"));
			assertTrue(SchedulerDatabaseUtility.removeExecutionId("exec"));
			assertTrue(SchedulerDatabaseUtility.removeExecutionId("missing"));
			assertEquals(0, db.count("SMSS_EXECUTION"));
			assertFalse(db.connection.isClosed());
		}
	}

	@Test
	void auditInsertFailureRestoresPriorLatestFlag() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SchedulerDatabaseUtility.queryUtil = db.engine.getQueryUtil();
			db.execute(
					"CREATE TABLE SMSS_AUDIT_TRAIL (JOB_ID VARCHAR, JOB_GROUP VARCHAR, EXECUTION_START TIMESTAMP, EXECUTION_END TIMESTAMP, EXECUTION_DELTA VARCHAR, SUCCESS BOOLEAN, IS_LATEST BOOLEAN, SCHEDULER_OUTPUT CLOB)",
					"INSERT INTO SMSS_AUDIT_TRAIL (JOB_ID,JOB_GROUP,IS_LATEST) VALUES ('job','old',TRUE)",
					"ALTER TABLE SMSS_AUDIT_TRAIL ADD CHECK (JOB_GROUP <> 'reject')");
			assertFalse(SchedulerDatabaseUtility.insertIntoAuditTrailTable("job", "reject", 0L, 100L, true, "text"));
			assertEquals(true, db.value("SELECT IS_LATEST FROM SMSS_AUDIT_TRAIL"));
			assertEquals(1, db.count("SMSS_AUDIT_TRAIL"));
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void recipesAndTagsAreAtomicForBothInsertAndUpdate() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SchedulerDatabaseUtility.queryUtil = db.engine.getQueryUtil();
			db.execute(
					"CREATE TABLE SMSS_JOB_RECIPES (USER_ID VARCHAR, JOB_ID VARCHAR, JOB_NAME VARCHAR, JOB_GROUP VARCHAR, CRON_EXPRESSION VARCHAR, CRON_TIMEZONE VARCHAR, PIXEL_RECIPE BLOB, PIXEL_RECIPE_PARAMETERS BLOB, JOB_CATEGORY VARCHAR, TRIGGER_ON_LOAD BOOLEAN, UI_STATE BLOB)",
					"CREATE TABLE SMSS_JOB_TAGS (JOB_ID VARCHAR, JOB_TAG VARCHAR CHECK (JOB_TAG <> 'reject'))");
			var tz = java.util.TimeZone.getTimeZone("UTC");
			assertFalse(SchedulerDatabaseUtility.insertIntoJobRecipesTable("u", "job", "n", "g", "cron", tz, "recipe",
					"params", "category", false, null, List.of("reject")));
			assertEquals(0, db.count("SMSS_JOB_RECIPES"));
			assertTrue(SchedulerDatabaseUtility.insertIntoJobRecipesTable("u", "job", "original", "g", "cron", tz,
					"recipe", "params", "category", false, null, List.of("old")));
			assertFalse(SchedulerDatabaseUtility.updateJobRecipesTable("u", "job", "changed", "g", "cron", tz, "recipe",
					"params", "category", false, null, "original", "g", List.of("reject")));
			assertEquals("original", db.value("SELECT JOB_NAME FROM SMSS_JOB_RECIPES"));
			assertEquals("old", db.value("SELECT JOB_TAG FROM SMSS_JOB_TAGS"));
			assertTrue(SchedulerDatabaseUtility.existsInJobRecipesTable("job", "g"));
			assertFalse(SchedulerDatabaseUtility.existsInJobRecipesTable("missing", "g"));
			assertTrue(SchedulerDatabaseUtility.removeFromJobRecipesTable("job", "g"));
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void triggerReadsMaterializeResultsAndEndManualTransactions() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE QRTZ_TRIGGERS (TRIGGER_STATE VARCHAR, NEXT_FIRE_TIME BIGINT)",
					"INSERT INTO QRTZ_TRIGGERS VALUES ('WAITING',10),('WAITING',30),('PAUSED',5)");
			db.manual();
			assertEquals(2L, SchedulerDatabaseUtility.getTriggerStateCounts().get("WAITING"));
			assertEquals(1, SchedulerDatabaseUtility.getOverdueTriggerCount(20));
			assertEquals(30L, SchedulerDatabaseUtility.getNextScheduledRunTime(20));
			assertNull(SchedulerDatabaseUtility.getNextScheduledRunTime(40));
			verify(db.connection, times(4)).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void jobListQueryFailuresRetainEmptyResultsAndKeepSharedConnectionOpen() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SchedulerDatabaseUtility.queryUtil = db.engine.getQueryUtil();
			assertTrue(SchedulerDatabaseUtility.retrieveJobsForProject("p", null).isEmpty());
			assertTrue(SchedulerDatabaseUtility.retrieveUsersJobsForProject("u", "p", null).isEmpty());
			assertTrue(SchedulerDatabaseUtility.retrieveUsersJobs("u", null).isEmpty());
			assertTrue(SchedulerDatabaseUtility.retrieveAllJobs(null).isEmpty());
			assertFalse(db.connection.isClosed());
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void jobListsMaterializeRecipesAndRespectOwnerAndProjectFilters() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SchedulerDatabaseUtility.queryUtil = db.engine.getQueryUtil();
			db.execute(
					"CREATE TABLE SMSS_JOB_RECIPES (USER_ID VARCHAR, JOB_ID VARCHAR PRIMARY KEY, JOB_NAME VARCHAR, JOB_GROUP VARCHAR, CRON_EXPRESSION VARCHAR, CRON_TIMEZONE VARCHAR, PIXEL_RECIPE BLOB, PIXEL_RECIPE_PARAMETERS BLOB, JOB_CATEGORY VARCHAR, TRIGGER_ON_LOAD BOOLEAN, UI_STATE BLOB)",
					"CREATE TABLE SMSS_JOB_TAGS (JOB_ID VARCHAR, JOB_TAG VARCHAR)",
					"CREATE TABLE QRTZ_TRIGGERS (JOB_NAME VARCHAR, JOB_GROUP VARCHAR, NEXT_FIRE_TIME BIGINT, TRIGGER_STATE VARCHAR)",
					"CREATE TABLE SMSS_AUDIT_TRAIL (JOB_ID VARCHAR, IS_LATEST BOOLEAN, EXECUTION_START TIMESTAMP)");
			var tz = java.util.TimeZone.getTimeZone("UTC");
			assertTrue(SchedulerDatabaseUtility.insertIntoJobRecipesTable("alice", "job", "Name", "project", "cron", tz,
					"recipe text", "parameter text", "category", false, "{ui}", List.of("tag")));
			assertTrue(SchedulerDatabaseUtility.insertIntoJobRecipesTable("bob", "other", "Other", "elsewhere", "cron",
					tz, "other recipe", "", "category", false, null, null));
			var projectJobs = SchedulerDatabaseUtility.retrieveJobsForProject("project", null);
			assertEquals(java.util.Set.of("project.job"), projectJobs.keySet());
			assertEquals("recipe text", projectJobs.get("project.job").get("recipe"));
			assertEquals("tag", projectJobs.get("project.job").get("jobTags"));
			assertEquals(projectJobs, SchedulerDatabaseUtility.retrieveUsersJobsForProject("project", "alice", null));
			assertEquals(projectJobs, SchedulerDatabaseUtility.retrieveUsersJobs("alice", null));
			assertEquals(2, SchedulerDatabaseUtility.retrieveAllJobs(null).size());
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void jobListsKeepJobsReadBeforeACursorFailureAndEndManualTransactions() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SchedulerDatabaseUtility.queryUtil = db.engine.getQueryUtil();
			var statement = mock(java.sql.PreparedStatement.class);
			var result = mock(java.sql.ResultSet.class);
			doReturn(statement).when(db.connection).prepareStatement(startsWith("SELECT SMSS_JOB_RECIPES.USER_ID"));
			when(statement.executeQuery()).thenReturn(result);
			when(result.next()).thenReturn(true).thenThrow(new java.sql.SQLException("cursor failure"));
			when(result.getString(SchedulerConstants.JOB_ID)).thenReturn("job");
			when(result.getString(SchedulerConstants.JOB_GROUP)).thenReturn("project");
			db.manual();
			var jobs = SchedulerDatabaseUtility.retrieveUsersJobsForProject("project", "alice", null);
			assertEquals(java.util.Set.of("project.job"), jobs.keySet());
			assertEquals("INACTIVE", jobs.get("project.job").get(SchedulerConstants.NEXT_FIRE_TIME));
			verify(statement).setBoolean(1, true);
			verify(statement).setString(2, "alice");
			verify(statement).setString(3, "project");
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			verify(statement).close();
			verify(result).close();
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void unreadableBlobContentKeepsTheJobWhileColumnFailuresEndTheRead() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var util = spy(db.engine.getQueryUtil());
			SchedulerDatabaseUtility.queryUtil = util;
			createJobListTables(db);
			var tz = java.util.TimeZone.getTimeZone("UTC");
			assertTrue(SchedulerDatabaseUtility.insertIntoJobRecipesTable("alice", "job", "Name", "project", "cron", tz,
					"recipe text", "parameter text", "category", false, "{ui}", null));
			doThrow(new java.io.IOException("unreadable")).when(util).handleBlobRetrieval(any(java.sql.ResultSet.class),
					eq(SchedulerConstants.UI_STATE));
			var job = SchedulerDatabaseUtility.retrieveAllJobs(null).get("project.job");
			assertEquals("recipe text", job.get("recipe"));
			assertEquals("parameter text", job.get("recipeParameters"));
			assertFalse(job.containsKey(ScheduleJobReactor.UI_STATE));
			doThrow(new java.sql.SQLException("column failure")).when(util)
					.handleBlobRetrieval(any(java.sql.ResultSet.class), eq(SchedulerConstants.PIXEL_RECIPE));
			db.manual();
			assertTrue(SchedulerDatabaseUtility.retrieveAllJobs(null).isEmpty());
			verify(db.connection).rollback();
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void jobListsReportAnUnavailableConnectionAsAnEmptyList() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			SchedulerDatabaseUtility.queryUtil = db.engine.getQueryUtil();
			doThrow(new java.sql.SQLException("down")).when(db.engine).getConnection();
			assertTrue(SchedulerDatabaseUtility.retrieveJobsForProject("p", null).isEmpty());
			assertTrue(SchedulerDatabaseUtility.retrieveUsersJobs("u", null).isEmpty());
			assertTrue(SchedulerDatabaseUtility.retrieveAllJobs(null).isEmpty());
			assertEquals(0, SchedulerDatabaseUtility.computeSchedulerAuditStats(java.time.Instant.EPOCH).totalRuns());
		} finally {
			SchedulerDatabaseUtility.queryUtil = null;
		}
	}

	@Test
	void auditStatsKeepRowsCountedBeforeACursorFailure() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var statement = mock(java.sql.PreparedStatement.class);
			var result = mock(java.sql.ResultSet.class);
			doReturn(statement).when(db.connection).prepareStatement(startsWith("SELECT at.JOB_ID"));
			when(statement.executeQuery()).thenReturn(result);
			when(result.next()).thenReturn(true).thenReturn(true)
					.thenThrow(new java.sql.SQLException("cursor failure"));
			when(result.getString("JOB_ID")).thenReturn("job");
			when(result.getBoolean("SUCCESS")).thenReturn(false);
			when(result.getString("EXECUTION_DELTA")).thenReturn("10").thenReturn(" 30 ");
			when(result.getString("JOB_NAME")).thenReturn("Name");
			db.manual();
			var stats = SchedulerDatabaseUtility.computeSchedulerAuditStats(java.time.Instant.EPOCH);
			assertEquals(2, stats.totalRuns());
			assertEquals(2, stats.failures());
			assertEquals(20, stats.avgDurationMs());
			assertEquals(30, stats.p95DurationMs());
			assertEquals("job", stats.worstJobId());
			assertEquals("Name", stats.worstJobName());
			assertEquals(2, stats.worstConsecutiveFailures());
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			verify(statement).close();
			verify(result).close();
		}
	}

	@Test
	void triggerOnLoadFiresJobsOnlyAfterTheReadEnds() throws Exception {
		try (var db = new JdbcTestDatabase(); var factories = mockStatic(SchedulerFactorySingleton.class)) {
			var scheduler = mockScheduler(factories);
			db.execute(
					"CREATE TABLE SMSS_JOB_RECIPES (JOB_ID VARCHAR, JOB_NAME VARCHAR, JOB_GROUP VARCHAR, TRIGGER_ON_LOAD BOOLEAN)",
					"INSERT INTO SMSS_JOB_RECIPES VALUES ('a','A','g',TRUE),('b','B','g',TRUE),('c','C','g',FALSE)");
			db.manual();
			SchedulerDatabaseUtility.executeAllTriggerOnLoads();
			var order = inOrder(db.connection, scheduler);
			order.verify(db.connection).rollback();
			order.verify(scheduler, times(2)).triggerJob(any(org.quartz.JobKey.class));
			verify(scheduler).triggerJob(org.quartz.JobKey.jobKey("a", "g"));
			verify(scheduler).triggerJob(org.quartz.JobKey.jobKey("b", "g"));
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void triggerOnLoadFiresNothingWhenTheReadFailsAndStopsAtAQuartzFailure() throws Exception {
		try (var db = new JdbcTestDatabase(); var factories = mockStatic(SchedulerFactorySingleton.class)) {
			var scheduler = mockScheduler(factories);
			SchedulerDatabaseUtility.executeAllTriggerOnLoads();
			verify(scheduler, never()).triggerJob(any(org.quartz.JobKey.class));
			db.execute(
					"CREATE TABLE SMSS_JOB_RECIPES (JOB_ID VARCHAR, JOB_NAME VARCHAR, JOB_GROUP VARCHAR, TRIGGER_ON_LOAD BOOLEAN)",
					"INSERT INTO SMSS_JOB_RECIPES VALUES ('a','A','g',TRUE),('b','B','g',TRUE)");
			doThrow(new org.quartz.SchedulerException("refused")).when(scheduler)
					.triggerJob(any(org.quartz.JobKey.class));
			assertDoesNotThrow(SchedulerDatabaseUtility::executeAllTriggerOnLoads);
			verify(scheduler, times(1)).triggerJob(any(org.quartz.JobKey.class));
		}
	}

	private static org.quartz.Scheduler mockScheduler(org.mockito.MockedStatic<SchedulerFactorySingleton> factories) {
		var factory = mock(SchedulerFactorySingleton.class);
		var scheduler = mock(org.quartz.Scheduler.class);
		factories.when(SchedulerFactorySingleton::getInstance).thenReturn(factory);
		when(factory.getScheduler()).thenReturn(scheduler);
		return scheduler;
	}

	private static void createJobListTables(JdbcTestDatabase db) throws Exception {
		db.execute(
				"CREATE TABLE SMSS_JOB_RECIPES (USER_ID VARCHAR, JOB_ID VARCHAR PRIMARY KEY, JOB_NAME VARCHAR, JOB_GROUP VARCHAR, CRON_EXPRESSION VARCHAR, CRON_TIMEZONE VARCHAR, PIXEL_RECIPE BLOB, PIXEL_RECIPE_PARAMETERS BLOB, JOB_CATEGORY VARCHAR, TRIGGER_ON_LOAD BOOLEAN, UI_STATE BLOB)",
				"CREATE TABLE SMSS_JOB_TAGS (JOB_ID VARCHAR, JOB_TAG VARCHAR)",
				"CREATE TABLE QRTZ_TRIGGERS (JOB_NAME VARCHAR, JOB_GROUP VARCHAR, NEXT_FIRE_TIME BIGINT, TRIGGER_STATE VARCHAR)",
				"CREATE TABLE SMSS_AUDIT_TRAIL (JOB_ID VARCHAR, IS_LATEST BOOLEAN, EXECUTION_START TIMESTAMP)");
	}

	@Test
	void projectJobRemovalRollsBackTagsIfRecipeDeletionFails() throws Exception {
		try (var db = new JdbcTestDatabase(); var factories = mockStatic(SchedulerFactorySingleton.class)) {
			db.execute("CREATE TABLE SMSS_JOB_RECIPES (JOB_ID VARCHAR PRIMARY KEY, JOB_GROUP VARCHAR)",
					"CREATE TABLE SMSS_JOB_TAGS (JOB_ID VARCHAR, JOB_TAG VARCHAR)",
					"CREATE TABLE JOB_GUARD (JOB_ID VARCHAR REFERENCES SMSS_JOB_RECIPES(JOB_ID))",
					"INSERT INTO SMSS_JOB_RECIPES VALUES ('job','project'), ('other','elsewhere')",
					"INSERT INTO SMSS_JOB_TAGS VALUES ('job','original'), ('other','kept')",
					"INSERT INTO JOB_GUARD VALUES ('job')");
			var factory = mock(SchedulerFactorySingleton.class);
			var scheduler = mock(org.quartz.Scheduler.class);
			factories.when(SchedulerFactorySingleton::getInstance).thenReturn(factory);
			when(factory.getScheduler()).thenReturn(scheduler);
			when(scheduler.getJobKeys(any())).thenReturn(java.util.Set.of(org.quartz.JobKey.jobKey("job", "project")));
			when(scheduler.deleteJobs(anyList())).thenReturn(true);
			assertThrows(IllegalStateException.class, () -> SchedulerDatabaseUtility.removeJobsForProject(" project "));
			assertEquals(2, db.count("SMSS_JOB_RECIPES"));
			assertEquals("original", db.value("SELECT JOB_TAG FROM SMSS_JOB_TAGS WHERE JOB_ID='job'"));
			verify(db.connection).rollback();
			db.execute("DELETE FROM JOB_GUARD");
			SchedulerDatabaseUtility.removeJobsForProject("project");
			assertEquals(1, db.count("SMSS_JOB_RECIPES"));
			assertEquals("other", db.value("SELECT JOB_ID FROM SMSS_JOB_RECIPES"));
			assertEquals("kept", db.value("SELECT JOB_TAG FROM SMSS_JOB_TAGS"));
		}
	}
}
