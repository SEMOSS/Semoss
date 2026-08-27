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

import static prerna.reactor.scheduler.SchedulerConstants.BIGINT;
import static prerna.reactor.scheduler.SchedulerConstants.BLOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.BOOL_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR;
import static prerna.reactor.scheduler.SchedulerConstants.CALENDAR_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.CHECKIN_INTERVAL;
import static prerna.reactor.scheduler.SchedulerConstants.CRON_EXPRESSION;
import static prerna.reactor.scheduler.SchedulerConstants.CRON_TIMEZONE;
import static prerna.reactor.scheduler.SchedulerConstants.DEC_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.DEC_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.DESCRIPTION;
import static prerna.reactor.scheduler.SchedulerConstants.END_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.ENTRY_ID;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_DELTA;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_END;
import static prerna.reactor.scheduler.SchedulerConstants.EXECUTION_START;
import static prerna.reactor.scheduler.SchedulerConstants.EXEC_ID;
import static prerna.reactor.scheduler.SchedulerConstants.FIRED_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.INSTANCE_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.INTEGER;
import static prerna.reactor.scheduler.SchedulerConstants.INT_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.INT_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.IS_DURABLE;
import static prerna.reactor.scheduler.SchedulerConstants.IS_LATEST;
import static prerna.reactor.scheduler.SchedulerConstants.IS_NONCONCURRENT;
import static prerna.reactor.scheduler.SchedulerConstants.IS_UPDATE_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_CATEGORY;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_CLASS_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_DATA;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_GROUP;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_ID;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_TAG;
import static prerna.reactor.scheduler.SchedulerConstants.JOB_TAGS;
import static prerna.reactor.scheduler.SchedulerConstants.LAST_CHECKIN_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.LOCK_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.LONG_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.LONG_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.MISFIRE_INSTR;
import static prerna.reactor.scheduler.SchedulerConstants.NEXT_FIRE_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.NOT_NULL;
import static prerna.reactor.scheduler.SchedulerConstants.NUMERIC_13_4;
import static prerna.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE;
import static prerna.reactor.scheduler.SchedulerConstants.PIXEL_RECIPE_PARAMETERS;
import static prerna.reactor.scheduler.SchedulerConstants.PREV_FIRE_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.PRIORITY;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_BLOB_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_CALENDARS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_CRON_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_FIRED_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_JOB_DETAILS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_LOCKS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_PAUSED_TRIGGER_GRPS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SCHEDULER_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SIMPLE_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_SIMPROP_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.QRTZ_TRIGGERS;
import static prerna.reactor.scheduler.SchedulerConstants.REPEAT_COUNT;
import static prerna.reactor.scheduler.SchedulerConstants.REPEAT_INTERVAL;
import static prerna.reactor.scheduler.SchedulerConstants.REQUESTS_RECOVERY;
import static prerna.reactor.scheduler.SchedulerConstants.SCHEDULER_OUTPUT;
import static prerna.reactor.scheduler.SchedulerConstants.SCHED_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.SCHED_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.SMALLINT;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_AUDIT_TRAIL;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_EXECUTION;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_JOB_RECIPES;
import static prerna.reactor.scheduler.SchedulerConstants.SMSS_JOB_TAGS;
import static prerna.reactor.scheduler.SchedulerConstants.START_TIME;
import static prerna.reactor.scheduler.SchedulerConstants.STATE;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_1;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_2;
import static prerna.reactor.scheduler.SchedulerConstants.STR_PROP_3;
import static prerna.reactor.scheduler.SchedulerConstants.SUCCESS;
import static prerna.reactor.scheduler.SchedulerConstants.TIMESTAMP;
import static prerna.reactor.scheduler.SchedulerConstants.TIMES_TRIGGERED;
import static prerna.reactor.scheduler.SchedulerConstants.TIME_ZONE_ID;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_GROUP;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_NAME;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_ON_LOAD;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.TRIGGER_TYPE;
import static prerna.reactor.scheduler.SchedulerConstants.UI_STATE;
import static prerna.reactor.scheduler.SchedulerConstants.USER_ID;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_120;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_16;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_200;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_250;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_255;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_40;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_512;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_8;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_80;
import static prerna.reactor.scheduler.SchedulerConstants.VARCHAR_95;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IRDBMSEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SchedulerDatabaseUtility {

	private static final Logger classLogger = LogManager.getLogger(SchedulerDatabaseUtility.class);
	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// ----------------------------------------------------------------------------
	// SQL queries
	// All non-DDL queries used by this utility live here so the schema interactions
	// can be reviewed in a single place. DDL/migration statements that depend on
	// queryUtil and conditional logic remain inline with their callers.
	// ----------------------------------------------------------------------------

	// SMSS_JOB_RECIPES / job listing - used by createJobQuery to assemble the
	// SELECT shared by retrieveJobsForProject, retrieveUsersJobsForProject,
	// retrieveUsersJobs, and retrieveAllJobs.
	private static final String BASE_JOB_DETAILS_QUERY = """
			SELECT SMSS_JOB_RECIPES.USER_ID, \
			SMSS_JOB_RECIPES.JOB_ID, \
			SMSS_JOB_RECIPES.JOB_NAME, \
			SMSS_JOB_RECIPES.JOB_GROUP, \
			SMSS_JOB_RECIPES.CRON_EXPRESSION, \
			SMSS_JOB_RECIPES.CRON_TIMEZONE, \
			SMSS_JOB_RECIPES.PIXEL_RECIPE, \
			SMSS_JOB_RECIPES.PIXEL_RECIPE_PARAMETERS, \
			SMSS_JOB_RECIPES.UI_STATE, \
			QRTZ_TRIGGERS.NEXT_FIRE_TIME, \
			SMSS_AUDIT_TRAIL.EXECUTION_START, \
			QRTZ_TRIGGERS.TRIGGER_STATE""";

	// SMSS_AUDIT_TRAIL join fetches the previous run time based on IS_LATEST
	private static final String JOIN_JOB_DETAILS_QUERY = """
			LEFT OUTER JOIN QRTZ_TRIGGERS \
			ON SMSS_JOB_RECIPES.JOB_ID = QRTZ_TRIGGERS.JOB_NAME \
			AND SMSS_JOB_RECIPES.JOB_GROUP = QRTZ_TRIGGERS.JOB_GROUP \
			LEFT OUTER JOIN SMSS_AUDIT_TRAIL \
			ON SMSS_JOB_RECIPES.JOB_ID = SMSS_AUDIT_TRAIL.JOB_ID \
			AND SMSS_AUDIT_TRAIL.IS_LATEST=? """;

	// SMSS_EXECUTION CRUD
	private static final String INSERT_EXECUTION_QUERY = "INSERT INTO SMSS_EXECUTION (EXEC_ID, JOB_ID, JOB_GROUP) VALUES (?,?,?)";
	private static final String SELECT_EXECUTION_BY_ID_QUERY = "SELECT JOB_ID, JOB_GROUP FROM SMSS_EXECUTION WHERE EXEC_ID = ?";
	private static final String DELETE_EXECUTION_QUERY = "DELETE FROM SMSS_EXECUTION WHERE EXEC_ID = ?";

	// SMSS_AUDIT_TRAIL CRUD
	private static final String CLEAR_AUDIT_TRAIL_LATEST_QUERY = "UPDATE SMSS_AUDIT_TRAIL SET IS_LATEST=? WHERE JOB_ID=?";
	private static final String INSERT_AUDIT_TRAIL_QUERY = """
			INSERT INTO SMSS_AUDIT_TRAIL \
			(JOB_ID, JOB_GROUP, EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS, IS_LATEST, SCHEDULER_OUTPUT) \
			VALUES (?,?,?,?,?,?,?,?)""";

	// SMSS_JOB_RECIPES CRUD
	private static final String INSERT_JOB_RECIPES_QUERY = """
			INSERT INTO SMSS_JOB_RECIPES \
			(USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, CRON_TIMEZONE, \
			PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, JOB_CATEGORY, TRIGGER_ON_LOAD, UI_STATE) \
			VALUES (?,?,?,?,?,?,?,?,?,?,?)""";
	private static final String UPDATE_JOB_RECIPES_QUERY = """
			UPDATE SMSS_JOB_RECIPES SET \
			USER_ID = ?, JOB_NAME = ?, JOB_GROUP = ?, \
			CRON_EXPRESSION = ?, CRON_TIMEZONE = ?, \
			PIXEL_RECIPE = ?, PIXEL_RECIPE_PARAMETERS = ?, \
			JOB_CATEGORY = ?, TRIGGER_ON_LOAD = ?, UI_STATE = ? \
			WHERE JOB_ID = ? AND JOB_GROUP = ?""";
	private static final String DELETE_JOB_RECIPES_QUERY = "DELETE FROM SMSS_JOB_RECIPES WHERE JOB_ID =? AND JOB_GROUP=?";
	private static final String EXISTS_JOB_RECIPES_QUERY = "SELECT COUNT(JOB_ID) FROM SMSS_JOB_RECIPES WHERE JOB_ID =? AND JOB_GROUP=?";
	private static final String SELECT_PROJECT_JOB_IDS_QUERY = "SELECT JOB_ID FROM SMSS_JOB_RECIPES WHERE JOB_GROUP=?";
	private static final String DELETE_PROJECT_JOB_RECIPES_QUERY = "DELETE FROM SMSS_JOB_RECIPES WHERE JOB_GROUP=?";
	private static final String SELECT_TRIGGER_ON_LOAD_QUERY = "SELECT * FROM SMSS_JOB_RECIPES WHERE TRIGGER_ON_LOAD=?";

	// SMSS_JOB_TAGS CRUD
	private static final String DELETE_JOB_TAGS_QUERY = "DELETE FROM SMSS_JOB_TAGS WHERE JOB_ID=?";
	private static final String INSERT_JOB_TAGS_QUERY = "INSERT INTO SMSS_JOB_TAGS (JOB_ID, JOB_TAG) VALUES (?,?)";

	// SchedulerStats queries - audit-trail aggregation + worst-job streak
	private static final String SCHEDULER_AUDIT_STATS_QUERY = """
			SELECT at.JOB_ID, at.SUCCESS, at.EXECUTION_DELTA, jr.JOB_NAME \
			FROM SMSS_AUDIT_TRAIL at \
			LEFT OUTER JOIN SMSS_JOB_RECIPES jr ON at.JOB_ID = jr.JOB_ID \
			WHERE at.EXECUTION_START >= ? \
			ORDER BY at.JOB_ID, at.EXECUTION_START DESC""";

	// SchedulerStats queries - current Quartz trigger state
	private static final String TRIGGER_STATE_COUNT_QUERY = """
			SELECT TRIGGER_STATE, COUNT(*) \
			FROM QRTZ_TRIGGERS \
			GROUP BY TRIGGER_STATE""";
	private static final String OVERDUE_TRIGGER_COUNT_QUERY = """
			SELECT COUNT(*) FROM QRTZ_TRIGGERS \
			WHERE NEXT_FIRE_TIME > 0 AND NEXT_FIRE_TIME < ? \
			AND TRIGGER_STATE NOT IN ('PAUSED', 'PAUSED_BLOCKED', 'COMPLETE')""";
	private static final String NEXT_SCHEDULED_RUN_QUERY = """
			SELECT MIN(NEXT_FIRE_TIME) FROM QRTZ_TRIGGERS \
			WHERE NEXT_FIRE_TIME > ? \
			AND TRIGGER_STATE NOT IN ('PAUSED', 'PAUSED_BLOCKED', 'COMPLETE')""";

	// ----------------------------------------------------------------------------

	static AbstractSqlQueryUtil queryUtil;

	private SchedulerDatabaseUtility() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Bootstraps the scheduler subsystem: loads the OWL if needed, creates and
	 * migrates every Quartz and SMSS scheduler table, then starts the Quartz
	 * scheduler. Called once at server startup.
	 */
	public static void startServer() throws Exception {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = schedulerDb.getConnection();
		try {
			queryUtil = schedulerDb.getQueryUtil();

			SchedulerOwlCreator owlCreator = new SchedulerOwlCreator();
			if (owlCreator.needsRemake(schedulerDb)) {
				owlCreator.remakeOwl(schedulerDb);
			}

			initialize();

			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
			try {
				if (!scheduler.isStarted()) {
					classLogger.info("Scheduler is not active. Starting up scheduler...");
					scheduler.start();
				}
			} catch (SchedulerException e) {
				classLogger.error("Failed to start scheduler: {}", e.getMessage(), e);
			}
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	/**
	 * Creates the Quartz and SMSS scheduler tables (idempotent) and adds primary
	 * and foreign key constraints. Safe to call on every startup; statements are
	 * gated by IF NOT EXISTS or by metadata lookups depending on the rdbms.
	 */
	public static void initialize() throws SQLException {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		String database = schedulerDb.getDatabase();
		String schema = schedulerDb.getSchema();
		Connection conn = schedulerDb.getConnection();
		try {
			createQuartzTables(conn, database, schema);
			createSemossTables(conn, database, schema);
			addAllPrimaryKeys(conn, database, schema);
			addAllForeignKeys(conn, database, schema);

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	/**
	 * Borrows a JDBC connection to the scheduler database. With connection pooling
	 * the caller is responsible for closing the returned connection; without
	 * pooling the connection is the underlying long-lived one and must not be
	 * closed.
	 *
	 * @return a non-null Connection
	 * @throws NullPointerException if a connection cannot be obtained
	 */
	public static Connection connectToScheduler() {
		Connection connection = null;

		try {
			IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
			connection = schedulerDb.getConnection();
		} catch (SQLException se) {
			classLogger.error("SQL error obtaining scheduler db connection: {}", se.getMessage(), se);
		} catch (Exception ex) {
			classLogger.error("Unexpected error obtaining scheduler db connection: {}", ex.getMessage(), ex);
		}

		if (connection == null) {
			throw new NullPointerException("Connection wasn't able to be created.");
		}

		return connection;
	}

	/**
	 * @return the scheduler database engine resolved via
	 *         {@link SystemEngineRegistry}
	 */
	public static IRDBMSEngine getSchedulerDB() {
		return SystemEngineRegistry.getSchedulerDb();
	}

	/**
	 * @return the rdbms-specific {@link AbstractSqlQueryUtil} for the scheduler db
	 */
	public static AbstractSqlQueryUtil getQueryUtil() {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		return schedulerDb.getQueryUtil();
	}

	/**
	 * Closes the given connection, swallowing and logging any SQLException. No-op
	 * when {@code connection} is null.
	 */
	public static void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
		}
	}

	/**
	 * Records a new scheduler execution attempt in SMSS_EXECUTION. The execId is
	 * the in-flight handle used by the rest of the scheduler runtime to look up the
	 * owning job.
	 *
	 * @param execId   unique execution id for this run
	 * @param jobId    Quartz job id
	 * @param jobGroup Quartz job group
	 * @return true on success, false if the insert failed (error logged)
	 */
	public static boolean insertIntoExecutionTable(String execId, String jobId, String jobGroup) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn.prepareStatement(INSERT_EXECUTION_QUERY)) {
			statement.setString(1, execId);
			statement.setString(2, jobId);
			statement.setString(3, jobGroup);
			statement.executeUpdate();
		} catch (SQLException e) {
			classLogger.error("Failed to insert into SMSS_EXECUTION for execId '{}', jobId '{}', jobGroup '{}': {}",
					execId, jobId, jobGroup, e.getMessage(), e);
			return false;
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return true;
	}

	/**
	 * Looks up the (jobId, jobGroup) pair owning the given execId.
	 *
	 * @param execId execution id to look up
	 * @return a two-element array {jobId, jobGroup} when the execId exists, or null
	 *         when not found / on query failure
	 */
	public static String[] executionIdExists(String execId) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		ResultSet rs = null;
		try (PreparedStatement statement = conn.prepareStatement(SELECT_EXECUTION_BY_ID_QUERY)) {
			statement.setString(1, execId);
			rs = statement.executeQuery();
			if (rs.next()) {
				String jobId = rs.getString(1);
				String jobGroup = rs.getString(2);
				return new String[] { jobId, jobGroup };
			}
		} catch (SQLException e) {
			classLogger.error("Failed to look up execution id '{}' in SMSS_EXECUTION: {}", execId, e.getMessage(), e);
			return null;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close ResultSet: {}", e.getMessage(), e);
				}
			}
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return null;
	}

	/**
	 * Deletes the execution row matching the given execId. Called after a job run
	 * finishes (success or failure) to release the in-flight handle.
	 *
	 * @param execId execution id to remove
	 * @return true on success, false on SQL failure (error logged)
	 */
	public static boolean removeExecutionId(String execId) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn.prepareStatement(DELETE_EXECUTION_QUERY)) {
			statement.setString(1, execId);
			statement.executeUpdate();
		} catch (SQLException e) {
			classLogger.error("Failed to remove execution id '{}' from SMSS_EXECUTION: {}", execId, e.getMessage(), e);
			return false;
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return true;
	}

	/**
	 * Records a completed job run in SMSS_AUDIT_TRAIL. Clears IS_LATEST on any
	 * prior rows for the same jobId, then inserts the new row with IS_LATEST=true
	 * so the latest run can be located by a single indexed lookup.
	 *
	 * @param jobId           Quartz job id
	 * @param jobGroup        Quartz job group
	 * @param start           epoch millis when the run started
	 * @param end             epoch millis when the run finished
	 * @param success         true if the run completed without error
	 * @param schedulerOutput captured output / error text (stored as CLOB)
	 * @return true on success, false on SQL failure (error logged)
	 */
	public static boolean insertIntoAuditTrailTable(String jobId, String jobGroup, Long start, Long end,
			boolean success, String schedulerOutput) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();

		Timestamp startTimeStamp = Utility
				.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneOffset.UTC));
		Timestamp endTimeStamp = Utility
				.getSqlTimestampUTC(LocalDateTime.ofInstant(Instant.ofEpochMilli(end), ZoneOffset.UTC));

		// update is_latest to false for all the existing records of this job id
		try {
			try (PreparedStatement updateAuditTrailStatement = conn.prepareStatement(CLEAR_AUDIT_TRAIL_LATEST_QUERY)) {
				updateAuditTrailStatement.setBoolean(1, false);
				updateAuditTrailStatement.setString(2, jobId);
				updateAuditTrailStatement.executeUpdate();
			} catch (SQLException e) {
				classLogger.error("Failed to clear IS_LATEST flag in SMSS_AUDIT_TRAIL for jobId '{}': {}", jobId,
						e.getMessage(), e);
				return false;
			}
			// now insert the new record with is_latest as true
			try (PreparedStatement statement = conn.prepareStatement(INSERT_AUDIT_TRAIL_QUERY)) {
				int index = 1;
				statement.setString(index++, jobId);
				statement.setString(index++, jobGroup);
				statement.setTimestamp(index++, startTimeStamp);
				statement.setTimestamp(index++, endTimeStamp);
				statement.setString(index++, String.valueOf(end - start));
				statement.setBoolean(index++, success);
				statement.setBoolean(index++, true);
				queryUtil.handleInsertionOfClob(conn, statement, schedulerOutput, index++, gson);
				statement.executeUpdate();
			} catch (UnsupportedEncodingException | SQLException e) {
				classLogger.error("Failed to insert audit trail row for jobId '{}', jobGroup '{}': {}", jobId, jobGroup,
						e.getMessage(), e);
				return false;
			}
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}
		return true;
	}

	/**
	 * Inserts a new row into SMSS_JOB_RECIPES describing a scheduled job, then
	 * upserts its tags via {@link #updateJobTags(String, List)}. Called by
	 * {@code ScheduleJobReactor} after the corresponding Quartz job is added.
	 *
	 * @param userId           owner of the schedule
	 * @param jobId            unique job id
	 * @param jobName          user-facing job name
	 * @param jobGroup         job group (typically the project/app id)
	 * @param cronExpression   cron expression
	 * @param cronTimeZone     time zone the cron expression is interpreted in
	 * @param recipe           pixel recipe to execute (stored as BLOB)
	 * @param recipeParameters pixel parameters (stored as BLOB)
	 * @param jobCategory      job category (e.g. "Default")
	 * @param triggerOnLoad    fire the job on every server startup
	 * @param uiState          serialized UI state (stored as BLOB, may be null)
	 * @param jobTags          tags to associate with this job, or null for none
	 * @return true on success, false on SQL failure (error logged)
	 */
	public static boolean insertIntoJobRecipesTable(String userId, String jobId, String jobName, String jobGroup,
			String cronExpression, TimeZone cronTimeZone, String recipe, String recipeParameters, String jobCategory,
			boolean triggerOnLoad, String uiState, List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();

		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn.prepareStatement(INSERT_JOB_RECIPES_QUERY)) {
			int index = 1;
			statement.setString(index++, userId);
			statement.setString(index++, jobId);
			statement.setString(index++, jobName);
			statement.setString(index++, jobGroup);
			statement.setString(index++, cronExpression);
			statement.setString(index++, cronTimeZone.getID());
			queryUtil.handleInsertionOfBlob(conn, statement, recipe, index++);
			queryUtil.handleInsertionOfBlob(conn, statement, recipeParameters, index++);
			statement.setString(index++, jobCategory);
			statement.setBoolean(index++, triggerOnLoad);
			queryUtil.handleInsertionOfBlob(conn, statement, uiState, index++);

			statement.executeUpdate();
		} catch (SQLException | UnsupportedEncodingException e) {
			classLogger.error("Failed to insert SMSS_JOB_RECIPES row for jobId '{}', jobGroup '{}': {}", jobId,
					jobGroup, e.getMessage(), e);
			return false;
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return updateJobTags(jobId, jobTags);
	}

	/**
	 * Replaces the tags associated with a job. Deletes every row in SMSS_JOB_TAGS
	 * for the jobId, then bulk-inserts the new set in a single batch. A null
	 * jobTags list only clears existing tags.
	 *
	 * @param jobId   job whose tags should be replaced
	 * @param jobTags new tag values (may be null to clear); each tag is trimmed
	 * @return true on success, false on SQL failure (error logged)
	 */
	public static boolean updateJobTags(String jobId, List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();

		try {
			// first we delete old tags
			try (PreparedStatement statement = conn.prepareStatement(DELETE_JOB_TAGS_QUERY)) {
				statement.setString(1, jobId);
				statement.execute();
			} catch (SQLException e) {
				classLogger.error("Failed to delete existing tags from SMSS_JOB_TAGS for jobId '{}': {}", jobId,
						e.getMessage(), e);
				return false;
			}

			if (jobTags == null) {
				return true;
			}

			// bulk insert for the job tags
			try (PreparedStatement statement = conn.prepareStatement(INSERT_JOB_TAGS_QUERY)) {
				for (String jobTag : jobTags) {
					statement.setString(1, jobId);
					statement.setString(2, jobTag.trim());
					statement.addBatch();
				}
				statement.executeBatch();
			} catch (SQLException e) {
				classLogger.error("Failed to bulk insert tags into SMSS_JOB_TAGS for jobId '{}': {}", jobId,
						e.getMessage(), e);
				return false;
			}
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return true;
	}

	/**
	 * Updates an existing SMSS_JOB_RECIPES row in place (matched by the original
	 * jobId and jobGroup) and refreshes its tags. Called by
	 * {@code EditScheduledJobReactor} when an existing schedule is rewritten.
	 *
	 * @param userId           owner of the schedule
	 * @param jobId            stable job id used as the WHERE-clause anchor
	 * @param jobName          new user-facing name
	 * @param jobGroup         new job group
	 * @param cronExpression   new cron expression
	 * @param cronTimeZone     new cron time zone
	 * @param recipe           new pixel recipe (stored as BLOB)
	 * @param recipeParameters new pixel parameters (stored as BLOB)
	 * @param jobCategory      new job category
	 * @param triggerOnLoad    new trigger-on-load flag
	 * @param uiState          new serialized UI state (stored as BLOB)
	 * @param existingJobName  unused - retained for source compatibility
	 * @param existingJobGroup prior job group used in the WHERE clause
	 * @param jobTags          new set of tags (may be null to clear)
	 * @return true on success, false on SQL failure (error logged)
	 */
	public static boolean updateJobRecipesTable(String userId, String jobId, String jobName, String jobGroup,
			String cronExpression, TimeZone cronTimeZone, String recipe, String recipeParameters, String jobCategory,
			boolean triggerOnLoad, String uiState, String existingJobName, String existingJobGroup,
			List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();

		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn.prepareStatement(UPDATE_JOB_RECIPES_QUERY)) {
			int index = 1;
			statement.setString(index++, userId);
			statement.setString(index++, jobName);
			statement.setString(index++, jobGroup);
			statement.setString(index++, cronExpression);
			statement.setString(index++, cronTimeZone.getID());
			queryUtil.handleInsertionOfBlob(conn, statement, recipe, index++);
			queryUtil.handleInsertionOfBlob(conn, statement, recipeParameters, index++);
			statement.setString(index++, jobCategory);
			statement.setBoolean(index++, triggerOnLoad);
			queryUtil.handleInsertionOfBlob(conn, statement, uiState, index++);

			// where clause filters
			statement.setString(index++, jobId);
			statement.setString(index++, existingJobGroup);

			statement.executeUpdate();
		} catch (SQLException | UnsupportedEncodingException e) {
			classLogger.error("Failed to update SMSS_JOB_RECIPES for jobId '{}', existingJobGroup '{}': {}", jobId,
					existingJobGroup, e.getMessage(), e);
			return false;
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return updateJobTags(jobId, jobTags);
	}

	/**
	 * Deletes the SMSS_JOB_RECIPES row matching the (jobId, jobGroup) pair. The
	 * scheduled Quartz job itself must be removed separately by the caller.
	 *
	 * @param jobId    job id
	 * @param jobGroup job group
	 * @return true on success, false on SQL failure (error logged)
	 */
	public static boolean removeFromJobRecipesTable(String jobId, String jobGroup) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn.prepareStatement(DELETE_JOB_RECIPES_QUERY)) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);

			statement.executeUpdate();
		} catch (SQLException e) {
			classLogger.error("Failed to delete SMSS_JOB_RECIPES row for jobId '{}', jobGroup '{}': {}", jobId,
					jobGroup, e.getMessage(), e);
			return false;
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return true;
	}

	/**
	 * Removes every future Quartz job and stored recipe owned by a project job
	 * group. Scheduler audit rows remain available for operational history.
	 *
	 * @param projectId project id used as the Quartz job group
	 * @throws IllegalStateException when Quartz or scheduler-database cleanup fails
	 */
	public static void removeJobsForProject(String projectId) {
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Project id is required to remove scheduled jobs");
		}

		String jobGroup = projectId.trim();
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		try {
			Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(jobGroup));
			if (!jobKeys.isEmpty() && !scheduler.deleteJobs(new ArrayList<>(jobKeys))) {
				throw new IllegalStateException("Quartz did not remove every scheduled job for project " + jobGroup);
			}
		} catch (SchedulerException e) {
			classLogger.error("Failed to remove Quartz jobs for project '{}': {}", jobGroup, e.getMessage(), e);
			throw new IllegalStateException("Failed to remove scheduled jobs for project " + jobGroup, e);
		}

		removeProjectJobRecords(jobGroup);
	}

	private static void removeProjectJobRecords(String jobGroup) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		boolean originalAutoCommit = true;
		try {
			originalAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);

			List<String> jobIds = new ArrayList<>();
			try (PreparedStatement select = conn.prepareStatement(SELECT_PROJECT_JOB_IDS_QUERY)) {
				select.setString(1, jobGroup);
				try (ResultSet result = select.executeQuery()) {
					while (result.next()) {
						jobIds.add(result.getString(1));
					}
				}
			}

			if (!jobIds.isEmpty()) {
				try (PreparedStatement deleteTags = conn.prepareStatement(DELETE_JOB_TAGS_QUERY)) {
					for (String jobId : jobIds) {
						deleteTags.setString(1, jobId);
						deleteTags.addBatch();
					}
					deleteTags.executeBatch();
				}
			}

			try (PreparedStatement deleteRecipes = conn.prepareStatement(DELETE_PROJECT_JOB_RECIPES_QUERY)) {
				deleteRecipes.setString(1, jobGroup);
				deleteRecipes.executeUpdate();
			}
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException rollbackError) {
				e.addSuppressed(rollbackError);
			}
			classLogger.error("Failed to remove scheduler records for project '{}': {}", jobGroup, e.getMessage(), e);
			throw new IllegalStateException("Failed to remove scheduler records for project " + jobGroup, e);
		} finally {
			try {
				conn.setAutoCommit(originalAutoCommit);
			} catch (SQLException e) {
				classLogger.error("Failed to restore scheduler connection auto-commit: {}", e.getMessage(), e);
			}
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * @param jobId    job id
	 * @param jobGroup job group
	 * @return true if a SMSS_JOB_RECIPES row exists for the pair; false when
	 *         missing or on SQL failure
	 */
	public static boolean existsInJobRecipesTable(String jobId, String jobGroup) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		try (PreparedStatement statement = conn.prepareStatement(EXISTS_JOB_RECIPES_QUERY)) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					int count = result.getInt(1);
					if (count == 0) {
						return false;
					}
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to check existence in SMSS_JOB_RECIPES for jobId '{}', jobGroup '{}': {}", jobId,
					jobGroup, e.getMessage(), e);
			return false;
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return true;
	}

	/**
	 * Returns every scheduled job belonging to a single project (jobGroup),
	 * optionally narrowed to those carrying any of the supplied tags.
	 *
	 * @param appId   project id (matched against SMSS_JOB_RECIPES.JOB_GROUP)
	 * @param jobTags optional tag filter; pass null to skip tag filtering
	 * @return map keyed by Quartz JobKey toString -> flattened job-details map
	 */
	public static Map<String, Map<String, String>> retrieveJobsForProject(String appId, List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();

		try (PreparedStatement statement = conn
				.prepareStatement(createJobQuery("WHERE SMSS_JOB_RECIPES.JOB_GROUP=?", jobTags))) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			statement.setString(2, appId);

			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve scheduled jobs for project '{}': {}", appId, e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return jobMap;
	}

	/**
	 * Returns the scheduled jobs owned by a specific user within a single project,
	 * optionally narrowed by tags.
	 *
	 * @param appId   project id (matched against JOB_GROUP)
	 * @param userId  owner id (matched against USER_ID)
	 * @param jobTags optional tag filter; pass null to skip tag filtering
	 * @return map keyed by Quartz JobKey toString -> flattened job-details map
	 */
	public static Map<String, Map<String, String>> retrieveUsersJobsForProject(String appId, String userId,
			List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		try (PreparedStatement statement = conn.prepareStatement(
				createJobQuery(" WHERE SMSS_JOB_RECIPES.USER_ID=? AND SMSS_JOB_RECIPES.JOB_GROUP=?", jobTags))) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			statement.setString(2, userId);
			statement.setString(3, appId);

			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve scheduled jobs for user '{}' in project '{}': {}", userId, appId,
					e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return jobMap;
	}

	/**
	 * Returns every scheduled job owned by a user across all projects, optionally
	 * narrowed by tags.
	 *
	 * @param userId  owner id (matched against USER_ID)
	 * @param jobTags optional tag filter; pass null to skip tag filtering
	 * @return map keyed by Quartz JobKey toString -> flattened job-details map
	 */
	public static Map<String, Map<String, String>> retrieveUsersJobs(String userId, List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		try (PreparedStatement statement = conn
				.prepareStatement(createJobQuery(" WHERE SMSS_JOB_RECIPES.USER_ID=?", jobTags))) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			statement.setString(2, userId);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve scheduled jobs for user '{}': {}", userId, e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return jobMap;
	}

	/**
	 * Assembles the SELECT used by the {@code retrieveJobs*} methods on top of
	 * {@link #BASE_JOB_DETAILS_QUERY} and {@link #JOIN_JOB_DETAILS_QUERY}. The
	 * caller supplies a WHERE fragment and an optional set of required tags; the
	 * helper injects a group_concat sub-select for JOB_TAGS and appends a
	 * tag-membership filter when {@code jobTags} is non-null.
	 *
	 * @param where   additional WHERE clause to AND-in (may be null)
	 * @param jobTags required tags; non-null adds an "any-of-these tags" filter
	 * @return the fully-assembled SQL
	 */
	public static String createJobQuery(String where, List<String> jobTags) {
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append(BASE_JOB_DETAILS_QUERY);
		// add the job tags
		// this depends on how group_concat is defined based on the rdbms type
		queryBuilder.append(", (SELECT ").append(queryUtil.processGroupByFunction("JOB_TAG", ",", true))
				.append(" FROM SMSS_JOB_TAGS WHERE SMSS_JOB_TAGS.JOB_ID=SMSS_JOB_RECIPES.JOB_ID) AS JOB_TAGS ");
		if (jobTags == null) {
			queryBuilder.append("FROM SMSS_JOB_RECIPES ");
		} else {
			queryBuilder.append("FROM SMSS_JOB_TAGS,SMSS_JOB_RECIPES ");
		}

		queryBuilder.append(JOIN_JOB_DETAILS_QUERY);

		if (where != null) {
			queryBuilder.append(' ');
			queryBuilder.append(where);
		}
		if (jobTags != null) {
			if (where != null) {
				queryBuilder.append(" AND ");
			} else {
				queryBuilder.append(" WHERE ");
			}

			Iterator<String> i = jobTags.iterator();
			while (i.hasNext()) {
				queryBuilder.append(String.format(
						" '%s' IN (SELECT SMSS_JOB_TAGS.JOB_TAG FROM SMSS_JOB_TAGS WHERE SMSS_JOB_TAGS.JOB_ID=SMSS_JOB_RECIPES.JOB_ID)",
						i.next()));
				if (i.hasNext()) {
					queryBuilder.append(" OR ");
				}
			}
		}

		return queryBuilder.toString();
	}

	/**
	 * Returns every scheduled job in the system, optionally narrowed by tags.
	 * Admin-style call; intended for server-wide views.
	 *
	 * @param jobTags optional tag filter; pass null to skip tag filtering
	 * @return map keyed by Quartz JobKey toString -> flattened job-details map
	 */
	public static Map<String, Map<String, String>> retrieveAllJobs(List<String> jobTags) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		Map<String, Map<String, String>> jobMap = new HashMap<>();
		String query = createJobQuery(null, jobTags);
		try (PreparedStatement statement = conn.prepareStatement(query)) {
			// always have the is_latest value
			statement.setBoolean(1, true);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					fillJobDetailsMap(jobMap, result);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve all scheduled jobs: {}", e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		return jobMap;
	}

	/**
	 * Snapshot of audit-trail derived stats over a time window, plus the job with
	 * the longest current consecutive-failure streak inside that window.
	 */
	public record SchedulerAuditStats(long totalRuns, long failures, long avgDurationMs, long p95DurationMs,
			String worstJobId, String worstJobName, long worstConsecutiveFailures) {
	}

	/**
	 * Aggregates audit-trail rows whose EXECUTION_START is at or after windowStart:
	 * run count, failure count, mean and p95 of EXECUTION_DELTA (ms), and the job
	 * whose latest sequence of failed runs is the longest. Rows are streamed in
	 * (JOB_ID, EXECUTION_START DESC) order so each job's current failure streak is
	 * computed in a single pass.
	 */
	public static SchedulerAuditStats computeSchedulerAuditStats(Instant windowStart) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		long totalRuns = 0;
		long failures = 0;
		List<Long> durations = new ArrayList<>();

		String currentJobId = null;
		String currentJobName = null;
		boolean stillCountingStreak = false;
		long currentStreak = 0;
		long worstStreak = 0;
		String worstJobId = null;
		String worstJobName = null;

		try (PreparedStatement ps = conn.prepareStatement(SCHEDULER_AUDIT_STATS_QUERY)) {
			ps.setTimestamp(1, Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(windowStart, ZoneOffset.UTC)));
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					totalRuns++;
					String jobId = rs.getString("JOB_ID");
					boolean success = rs.getBoolean("SUCCESS");
					String deltaStr = rs.getString("EXECUTION_DELTA");
					String jobName = rs.getString("JOB_NAME");

					if (!success) {
						failures++;
					}
					if (deltaStr != null) {
						try {
							durations.add(Long.parseLong(deltaStr.trim()));
						} catch (NumberFormatException ignore) {
							// legacy rows may store non-numeric values; skip
						}
					}

					if (jobId != null && !jobId.equals(currentJobId)) {
						currentJobId = jobId;
						currentJobName = jobName;
						currentStreak = 0;
						stillCountingStreak = true;
					}
					if (stillCountingStreak) {
						if (!success) {
							currentStreak++;
							if (currentStreak > worstStreak) {
								worstStreak = currentStreak;
								worstJobId = currentJobId;
								worstJobName = currentJobName;
							}
						} else {
							stillCountingStreak = false;
						}
					}
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to query scheduler audit stats: {}", e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}

		long avg = 0;
		long p95 = 0;
		if (!durations.isEmpty()) {
			long sum = 0;
			for (long d : durations) {
				sum += d;
			}
			avg = sum / durations.size();
			Collections.sort(durations);
			int idx = (int) Math.ceil(durations.size() * 0.95) - 1;
			if (idx < 0) {
				idx = 0;
			} else if (idx >= durations.size()) {
				idx = durations.size() - 1;
			}
			p95 = durations.get(idx);
		}

		return new SchedulerAuditStats(totalRuns, failures, avg, p95, worstJobId, worstJobName, worstStreak);
	}

	/**
	 * @return current count of QRTZ_TRIGGERS rows grouped by TRIGGER_STATE; empty
	 *         on query failure
	 */
	public static Map<String, Long> getTriggerStateCounts() {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		Map<String, Long> counts = new HashMap<>();
		try (PreparedStatement ps = conn.prepareStatement(TRIGGER_STATE_COUNT_QUERY);
				ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				String state = rs.getString(1);
				if (state != null) {
					counts.put(state, rs.getLong(2));
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to query trigger state counts: {}", e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}
		return counts;
	}

	/**
	 * @param beforeEpochMillis upper bound (typically "now") for NEXT_FIRE_TIME
	 * @return count of non-paused, non-complete triggers whose NEXT_FIRE_TIME has
	 *         already passed
	 */
	public static long getOverdueTriggerCount(long beforeEpochMillis) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		try (PreparedStatement ps = conn.prepareStatement(OVERDUE_TRIGGER_COUNT_QUERY)) {
			ps.setLong(1, beforeEpochMillis);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getLong(1);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to query overdue trigger count: {}", e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}
		return 0;
	}

	/**
	 * @param afterEpochMillis lower bound (typically "now") for NEXT_FIRE_TIME
	 * @return earliest upcoming NEXT_FIRE_TIME across non-paused triggers, or null
	 *         when nothing is scheduled
	 */
	public static Long getNextScheduledRunTime(long afterEpochMillis) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		Connection conn = connectToScheduler();
		try (PreparedStatement ps = conn.prepareStatement(NEXT_SCHEDULED_RUN_QUERY)) {
			ps.setLong(1, afterEpochMillis);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					long next = rs.getLong(1);
					if (!rs.wasNull() && next > 0) {
						return next;
					}
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to query next trigger fire time: {}", e.getMessage(), e);
		} finally {
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}
		return null;
	}

	/**
	 * Reads one row of the {@link #BASE_JOB_DETAILS_QUERY} ResultSet, decodes the
	 * three BLOB columns (PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, UI_STATE), formats
	 * PREV_FIRE_TIME and NEXT_FIRE_TIME into display strings, and appends the
	 * resulting flattened map to {@code jobMap} keyed by {@code JobKey.toString()}.
	 *
	 * @param jobMap accumulator the caller passes in; mutated in place
	 * @param result the current row of the audit/recipes query
	 */
	private static void fillJobDetailsMap(Map<String, Map<String, String>> jobMap, ResultSet result)
			throws SQLException {
		Map<String, String> jobDetailsMap = new HashMap<>();

		String userId = result.getString(USER_ID);
		String jobId = result.getString(JOB_ID);
		String jobTags = result.getString(JOB_TAGS);
		String jobName = result.getString(JOB_NAME);
		String jobGroup = result.getString(JOB_GROUP);
		String cronExpression = result.getString(CRON_EXPRESSION);
		String cronTimeZone = result.getString(CRON_TIMEZONE);
		String recipe = null;
		String recipeParameters = null;
		String uiState = null;
		BigInteger nextExecTime = null;
		BigDecimal nExecTimeD = result.getBigDecimal(NEXT_FIRE_TIME);
		String tiggerState = result.getString(TRIGGER_STATE);
		Timestamp previousRun = result.getTimestamp(EXECUTION_START);

		if (nExecTimeD != null) {
			nextExecTime = nExecTimeD.toBigInteger();
		}

		try {
			recipe = queryUtil.handleBlobRetrieval(result, PIXEL_RECIPE);
		} catch (SQLException | IOException e) {
			classLogger.error("Failed to read PIXEL_RECIPE blob for jobId '{}': {}", jobId, e.getMessage(), e);
		}

		try {
			recipeParameters = queryUtil.handleBlobRetrieval(result, PIXEL_RECIPE_PARAMETERS);
		} catch (SQLException | IOException e) {
			classLogger.error("Failed to read PIXEL_RECIPE_PARAMETERS blob for jobId '{}': {}", jobId, e.getMessage(),
					e);
		}

		try {
			uiState = queryUtil.handleBlobRetrieval(result, UI_STATE);
		} catch (SQLException | IOException e) {
			classLogger.error("Failed to read UI_STATE blob for jobId '{}': {}", jobId, e.getMessage(), e);
		}

		jobDetailsMap.put(USER_ID, userId);
		jobDetailsMap.put(ReactorKeysEnum.JOB_ID.getKey(), jobId);
		jobDetailsMap.put(ReactorKeysEnum.JOB_GROUP.getKey(), jobGroup);
		jobDetailsMap.put(ReactorKeysEnum.JOB_NAME.getKey(), jobName);
		jobDetailsMap.put(ReactorKeysEnum.JOB_TAGS.getKey(), jobTags);
		jobDetailsMap.put(ReactorKeysEnum.CRON_EXPRESSION.getKey(), cronExpression);
		jobDetailsMap.put(ReactorKeysEnum.CRON_TZ.getKey(), cronTimeZone);
		jobDetailsMap.put(ReactorKeysEnum.RECIPE.getKey(), recipe);
		jobDetailsMap.put(ReactorKeysEnum.RECIPE_PARAMETERS.getKey(), recipeParameters);
		if (uiState != null && !(uiState = uiState.trim()).isEmpty()) {
			jobDetailsMap.put(ScheduleJobReactor.UI_STATE, uiState);
		}
		// setting the prev_fire_time fom the smss audit table
		if (previousRun != null) {
			jobDetailsMap.put(PREV_FIRE_TIME, previousRun.toString());
		} else {
			jobDetailsMap.put(PREV_FIRE_TIME, "N/A");
		}

		// add next fire time
		if (nextExecTime != null && !tiggerState.equals("PAUSED")) {
			if (nextExecTime.intValue() == -1) {
				jobDetailsMap.put(NEXT_FIRE_TIME, "EXECUTING");
			} else {
				Instant instant = Instant.ofEpochMilli(nextExecTime.longValue());
				DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				jobDetailsMap.put(NEXT_FIRE_TIME, fmt
						.format(instant.atZone(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()).toZoneId())));
			}
		} else {
			jobDetailsMap.put(NEXT_FIRE_TIME, "INACTIVE");
		}
		// add to the job map
		JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
		jobMap.put(jobKey.toString(), jobDetailsMap);
	}

	/**
	 * Fires every SMSS_JOB_RECIPES row marked TRIGGER_ON_LOAD=true once. Called
	 * after the scheduler is started so opt-in jobs run at server boot. No-op on a
	 * clustered scheduler - one node fires for the cluster and we do not want every
	 * node to duplicate the run.
	 */
	public static void executeAllTriggerOnLoads() {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		if (ClusterUtil.IS_CLUSTERED_SCHEDULER) {
			return;
		}

		Connection conn = connectToScheduler();
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		ResultSet result = null;

		try (PreparedStatement preparedStatement = conn.prepareStatement(SELECT_TRIGGER_ON_LOAD_QUERY)) {
			preparedStatement.setBoolean(1, true);
			result = preparedStatement.executeQuery();

			while (result.next()) {
				String jobId = result.getString(JOB_ID);
				String jobName = result.getString(JOB_NAME);
				String jobGroup = result.getString(JOB_GROUP);
				JobKey jobKey = JobKey.jobKey(jobId, jobGroup);
				classLogger.info("Triggering job on startup {}", Utility.cleanLogString(jobName));
				scheduler.triggerJob(jobKey);
			}

			classLogger.info("All trigger on load jobs executed successfully");
		} catch (SQLException sqe) {
			classLogger.error("Failed to query trigger-on-load jobs from SMSS_JOB_RECIPES: {}", sqe.getMessage(), sqe);
		} catch (SchedulerException se) {
			classLogger.error("Failed to fire trigger-on-load job: {}", se.getMessage(), se);
		} finally {
			try {
				if (result != null) {
					result.close();
				}
			} catch (SQLException sqe) {
				classLogger.error("Failed to close ResultSet: {}", sqe.getMessage(), sqe);
			}
			if (schedulerDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close scheduler db connection: {}", e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * Starts the given Quartz scheduler if it isn't already started. Errors are
	 * logged; callers can continue without throwing because Quartz will resurface
	 * the failure on subsequent operations.
	 *
	 * @param scheduler scheduler to start
	 */
	public static void startScheduler(Scheduler scheduler) {
		try {
			classLogger.info("Scheduler starting up...");
			if (!scheduler.isStarted()) {
				scheduler.start();
			}
			classLogger.info("Scheduler started at {}", new Date());
		} catch (SchedulerException se) {
			classLogger.error("Failed to start scheduler: {}", se.getMessage(), se);
		}
	}

	/**
	 * Validates the three required inputs for scheduling/editing a job. Throws
	 * {@link IllegalArgumentException} with a user-facing message when any input is
	 * missing or the cron expression is malformed.
	 *
	 * @param jobName        non-empty job name
	 * @param jobGroup       non-empty job group
	 * @param cronExpression Quartz-compatible cron expression
	 */
	public static void validateInput(String jobName, String jobGroup, String cronExpression) {
		if (jobName == null || jobName.length() <= 0) {
			throw new IllegalArgumentException("Must provide job name");
		}

		if (jobGroup == null || jobGroup.length() <= 0) {
			throw new IllegalArgumentException("Must provide job group");
		}

		if (!CronExpression.isValidExpression(cronExpression)) {
			throw new IllegalArgumentException("Must provide a valid cron expression!");
		}
	}

	/**
	 * Verifies the recipe is non-empty and URI-decodes it. The frontend posts
	 * recipes URI-encoded so they survive the transport layer; the scheduler stores
	 * them decoded.
	 *
	 * @param recipe URI-encoded pixel recipe text
	 * @return decoded recipe
	 * @throws IllegalArgumentException when recipe is null or empty
	 */
	public static String validateAndDecodeRecipe(String recipe) {
		if (recipe == null || recipe.length() <= 0) {
			throw new IllegalArgumentException("Must provide a recipe");
		}

		return Utility.decodeURIComponent(recipe);
	}

	/**
	 * URI-decodes optional recipe parameters. Unlike the recipe itself, parameters
	 * are permitted to be absent - null/empty input returns null so callers can
	 * persist a SQL NULL.
	 *
	 * @param recipeParameters URI-encoded parameter text, may be null/empty
	 * @return decoded parameters, or null when input was null/empty
	 */
	public static String validateAndDecodeRecipeParameters(String recipeParameters) {
		if (recipeParameters == null || recipeParameters.isEmpty()) {
			return null;
		}

		return Utility.decodeURIComponent(recipeParameters);
	}

	/**
	 * Creates the eleven Quartz scheduler tables (QRTZ_CALENDARS,
	 * QRTZ_CRON_TRIGGERS, QRTZ_FIRED_TRIGGERS, QRTZ_PAUSED_TRIGGER_GRPS,
	 * QRTZ_SCHEDULER_STATE, QRTZ_LOCKS, QRTZ_JOB_DETAILS, QRTZ_SIMPLE_TRIGGERS,
	 * QRTZ_SIMPROP_TRIGGERS, QRTZ_BLOB_TRIGGERS, QRTZ_TRIGGERS). When the rdbms
	 * supports {@code CREATE TABLE IF NOT EXISTS} the helper uses that single
	 * statement per table; otherwise it queries the metadata first and only issues
	 * a CREATE on missing tables.
	 *
	 * @param connection live JDBC connection to the scheduler db
	 * @param database   database name (used for metadata lookups)
	 * @param schema     schema name (used for metadata lookups)
	 */
	private static void createQuartzTables(Connection connection, String database, String schema) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String IMAGE_DATATYPE = queryUtil.getImageDataTypeName();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

		String[] colNames = null;
		String[] types = null;
		Object[] constraints = null;

		try {
			// QRTZ_CALENDARS
			colNames = new String[] { SCHED_NAME, CALENDAR_NAME, CALENDAR };
			types = new String[] { VARCHAR_120, VARCHAR_200, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL };
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_CALENDARS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_CALENDARS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_CALENDARS, colNames, types, constraints));
				}
			}

			// QRTZ_CRON_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, CRON_EXPRESSION, TIME_ZONE_ID };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_120, VARCHAR_80 };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_CRON_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_CRON_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_CRON_TRIGGERS, colNames,
							types, constraints));
				}
			}

			// QRTZ_FIRED_TRIGGERS
			colNames = new String[] { SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME,
					SCHED_TIME, PRIORITY, STATE, JOB_NAME, JOB_GROUP, IS_NONCONCURRENT, REQUESTS_RECOVERY };
			types = new String[] { VARCHAR_120, VARCHAR_95, VARCHAR_200, VARCHAR_200, VARCHAR_200, BIGINT, BIGINT,
					INTEGER, VARCHAR_16, VARCHAR_200, VARCHAR_200, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL,
					NOT_NULL, null, null, null, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_FIRED_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_FIRED_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_FIRED_TRIGGERS, colNames,
							types, constraints));
				}
			}

			// QRTZ_PAUSED_TRIGGER_GRPS
			colNames = new String[] { SCHED_NAME, TRIGGER_GROUP };
			types = new String[] { VARCHAR_120, VARCHAR_200 };
			constraints = new String[] { NOT_NULL, NOT_NULL };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_PAUSED_TRIGGER_GRPS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_PAUSED_TRIGGER_GRPS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_PAUSED_TRIGGER_GRPS,
							colNames, types, constraints));
				}
			}

			// QRTZ_SCHEDULER_STATE
			colNames = new String[] { SCHED_NAME, INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL };
			types = new String[] { VARCHAR_120, VARCHAR_200, BIGINT, BIGINT };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SCHEDULER_STATE,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SCHEDULER_STATE, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_SCHEDULER_STATE, colNames,
							types, constraints));
				}
			}

			// QRTZ_LOCKS
			colNames = new String[] { SCHED_NAME, LOCK_NAME };
			types = new String[] { VARCHAR_120, VARCHAR_40 };
			constraints = new String[] { NOT_NULL, NOT_NULL };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_LOCKS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_LOCKS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_LOCKS, colNames, types, constraints));
				}
			}

			// QRTZ_JOB_DETAILS
			colNames = new String[] { SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE,
					IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_250, VARCHAR_250, BOOLEAN_DATATYPE,
					BOOLEAN_DATATYPE, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL,
					NOT_NULL, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_JOB_DETAILS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_JOB_DETAILS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_JOB_DETAILS, colNames, types, constraints));
				}
			}

			// QRTZ_SIMPLE_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, REPEAT_COUNT, REPEAT_INTERVAL,
					TIMES_TRIGGERED };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, BIGINT, BIGINT, BIGINT };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SIMPLE_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SIMPLE_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_SIMPLE_TRIGGERS, colNames,
							types, constraints));
				}
			}

			// QRTZ_SIMPROP_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, STR_PROP_1, STR_PROP_2, STR_PROP_3,
					INT_PROP_1, INT_PROP_2, LONG_PROP_1, LONG_PROP_2, DEC_PROP_1, DEC_PROP_2, BOOL_PROP_1,
					BOOL_PROP_2 };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_512, VARCHAR_512, VARCHAR_512,
					INTEGER, INTEGER, BIGINT, BIGINT, NUMERIC_13_4, NUMERIC_13_4, BOOLEAN_DATATYPE, BOOLEAN_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, null, null, null, null,
					null, null, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_SIMPROP_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_SIMPROP_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_SIMPROP_TRIGGERS, colNames,
							types, constraints));
				}
			}

			// QRTZ_BLOB_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, BLOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_BLOB_TRIGGERS,
						colNames, types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_BLOB_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(queryUtil.createTableWithCustomConstraints(QRTZ_BLOB_TRIGGERS, colNames,
							types, constraints));
				}
			}

			// QRTZ_TRIGGERS
			colNames = new String[] { SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, JOB_NAME, JOB_GROUP, DESCRIPTION,
					NEXT_FIRE_TIME, PREV_FIRE_TIME, PRIORITY, TRIGGER_STATE, TRIGGER_TYPE, START_TIME, END_TIME,
					CALENDAR_NAME, MISFIRE_INSTR, JOB_DATA };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_200, VARCHAR_200, VARCHAR_250, BIGINT,
					BIGINT, INTEGER, VARCHAR_16, VARCHAR_8, BIGINT, BIGINT, VARCHAR_200, SMALLINT, IMAGE_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null,
					NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null };

			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExistsWithCustomConstraints(QRTZ_TRIGGERS, colNames,
						types, constraints));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, QRTZ_TRIGGERS, database, schema)) {
					// make the table
					schedulerDb.insertData(
							queryUtil.createTableWithCustomConstraints(QRTZ_TRIGGERS, colNames, types, constraints));
				}
			}
		} catch (Exception se) {
			classLogger.error("Failed to create one or more Quartz scheduler tables: {}", se.getMessage(), se);
		}
	}

	/**
	 * Creates and migrates the SMSS-side scheduler tables (SMSS_JOB_RECIPES,
	 * SMSS_JOB_TAGS, SMSS_AUDIT_TRAIL, SMSS_EXECUTION). Beyond initial creation
	 * this method also runs idempotent column back-fills for older schemas - see
	 * the {@code // ADDED <date>} blocks for the individual migrations. Every
	 * migration step is gated on a column/constraint existence check so the method
	 * can be re-run on every startup.
	 *
	 * @param connection live JDBC connection to the scheduler db
	 * @param database   database name (used for metadata lookups)
	 * @param schema     schema name (used for metadata lookups)
	 */
	private static void createSemossTables(Connection connection, String database, String schema) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		String dateTimeType = queryUtil.getDateWithTimeDataType();
		final String BLOB_DATATYPE = queryUtil.getBlobDataTypeName();
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String CLOB_DATATYPE = queryUtil.getClobDataTypeName();
		String[] colNames = null;
		String[] types = null;
		Object[] constraints = null;

		try {
			// SMSS_JOB_RECIPES
			colNames = new String[] { USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, CRON_TIMEZONE,
					PIXEL_RECIPE, PIXEL_RECIPE_PARAMETERS, JOB_CATEGORY, TRIGGER_ON_LOAD, UI_STATE };
			types = new String[] { VARCHAR_120, VARCHAR_200, VARCHAR_200, VARCHAR_200, VARCHAR_250, VARCHAR_120,
					BLOB_DATATYPE, BLOB_DATATYPE, VARCHAR_200, BOOLEAN_DATATYPE, BLOB_DATATYPE };
			constraints = new String[] { NOT_NULL, NOT_NULL, NOT_NULL, NOT_NULL, null, null, null, null, null, null,
					null };

			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_JOB_RECIPES, colNames, types,
						constraints);
				classLogger.info("Running sql: {}", sql);
				schedulerDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_JOB_RECIPES, database, schema)) {
					// make the table
					String sql = queryUtil.createTableWithCustomConstraints(SMSS_JOB_RECIPES, colNames, types,
							constraints);
					classLogger.info("Running sql: {}", sql);
					schedulerDb.insertData(sql);
				}
			}

			// SMSS_JOB_TAGS
			colNames = new String[] { JOB_ID, JOB_TAG };
			types = new String[] { VARCHAR_200, VARCHAR_200 };
			constraints = new String[] { NOT_NULL, NOT_NULL };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_JOB_TAGS, colNames, types,
						constraints);
				classLogger.info("Running sql: {}", sql);
				schedulerDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_JOB_TAGS, database, schema)) {
					// make the table
					String sql = queryUtil.createTableWithCustomConstraints(SMSS_JOB_TAGS, colNames, types,
							constraints);
					classLogger.info("Running sql: {}", sql);
					schedulerDb.insertData(sql);
				}
			}

			// SMSS_AUDIT_TRAIL
			// adding is_latest flag to mark the latest record
			colNames = new String[] { JOB_ID, JOB_GROUP, EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS,
					IS_LATEST, SCHEDULER_OUTPUT };
			types = new String[] { VARCHAR_200, VARCHAR_200, TIMESTAMP, TIMESTAMP, VARCHAR_255, BOOLEAN_DATATYPE,
					BOOLEAN_DATATYPE, CLOB_DATATYPE };
			if (!dateTimeType.equals(TIMESTAMP)) {
				types = cleanUpDataType(types, TIMESTAMP, dateTimeType);
			}
			constraints = new String[] { NOT_NULL, NOT_NULL, null, null, null, null, null, null };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExistsWithCustomConstraints(SMSS_AUDIT_TRAIL, colNames, types,
						constraints);
				classLogger.info("Running sql: {}", sql);
				schedulerDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_AUDIT_TRAIL, database, schema)) {
					// make the table
					String sql = queryUtil.createTableWithCustomConstraints(SMSS_AUDIT_TRAIL, colNames, types,
							constraints);
					classLogger.info("Running sql: {}", sql);
					schedulerDb.insertData(sql);
				}
			}

			// SMSS_EXECUTION_SCHEDULE
			colNames = new String[] { EXEC_ID, JOB_ID, JOB_GROUP };
			types = new String[] { VARCHAR_200, VARCHAR_200, VARCHAR_200 };
			if (!dateTimeType.equals(TIMESTAMP)) {
				types = cleanUpDataType(types, TIMESTAMP, dateTimeType);
			}
			if (allowIfExistsTable) {
				schedulerDb.insertData(queryUtil.createTableIfNotExists(SMSS_EXECUTION, colNames, types));
			} else {
				// see if table exists
				if (!queryUtil.tableExists(connection, SMSS_EXECUTION, database, schema)) {
					// make the table
					String sql = queryUtil.createTable(SMSS_EXECUTION, colNames, types);
					classLogger.info("Running sql: {}", sql);
					schedulerDb.insertData(sql);
				}
			}
		} catch (Exception se) {
			classLogger.error("Failed to create or migrate one or more SMSS scheduler tables: {}", se.getMessage(), se);
		}
	}

	/**
	 * In-place find/replace across a String[]. Used by the table-creation logic to
	 * swap a generic placeholder type (e.g. {@code TIMESTAMP}) for the
	 * rdbms-specific equivalent reported by the query util.
	 *
	 * @param arrays      array to mutate
	 * @param value       value to find
	 * @param replacement value to substitute
	 * @return the same array reference (for fluent use)
	 */
	private static String[] cleanUpDataType(String[] arrays, String value, String replacement) {
		for (int i = 0; i < arrays.length; i++) {
			if (arrays[i].equals(value)) {
				arrays[i] = replacement;
			}
		}
		return arrays;
	}

	/**
	 * Adds primary-key constraints to the ten Quartz tables that require them. When
	 * the rdbms supports {@code ADD CONSTRAINT IF NOT EXISTS} the helper fires all
	 * ten in a single try/catch; otherwise each one is gated on a
	 * pg_constraint-style lookup so re-running is idempotent. Each failure is
	 * logged and the helper continues - a single failing PK does not stop the
	 * scheduler from coming up.
	 *
	 * @param conn     live JDBC connection to the scheduler db
	 * @param database database name (used for constraint-existence lookups)
	 * @param schema   schema name (used for constraint-existence lookups)
	 */
	private static void addAllPrimaryKeys(Connection conn, String database, String schema) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		if (queryUtil.allowIfExistsAddConstraint()) {
			String query1 = "ALTER TABLE QRTZ_CALENDARS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_CALENDARS PRIMARY KEY ( SCHED_NAME, CALENDAR_NAME);";
			String query2 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_CRON_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query3 = "ALTER TABLE QRTZ_FIRED_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_FIRED_TRIGGERS PRIMARY KEY ( SCHED_NAME, ENTRY_ID );";
			String query4 = "ALTER TABLE QRTZ_PAUSED_TRIGGER_GRPS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_PAUSED_TRIGGER_GRPS PRIMARY KEY ( SCHED_NAME, TRIGGER_GROUP );";
			String query5 = "ALTER TABLE QRTZ_SCHEDULER_STATE ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_SCHEDULER_STATE PRIMARY KEY ( SCHED_NAME, INSTANCE_NAME );";
			String query6 = "ALTER TABLE QRTZ_LOCKS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_LOCKS PRIMARY KEY ( SCHED_NAME, LOCK_NAME );";
			String query7 = "ALTER TABLE QRTZ_JOB_DETAILS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_JOB_DETAILS PRIMARY KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP );";
			String query8 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_SIMPLE_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query9 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_SIMPROP_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query10 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT IF NOT EXISTS PK_QRTZ_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";

			try {
				schedulerDb.insertData(query1);
				schedulerDb.insertData(query2);
				schedulerDb.insertData(query3);
				schedulerDb.insertData(query4);
				schedulerDb.insertData(query5);
				schedulerDb.insertData(query6);
				schedulerDb.insertData(query7);
				schedulerDb.insertData(query8);
				schedulerDb.insertData(query9);
				schedulerDb.insertData(query10);
			} catch (Exception se) {
				classLogger.error("Failed to add Quartz primary key constraints (IF NOT EXISTS path): {}",
						se.getMessage(), se);
			}
		} else {
			String query1 = "ALTER TABLE QRTZ_CALENDARS ADD CONSTRAINT PK_QRTZ_CALENDARS PRIMARY KEY ( SCHED_NAME, CALENDAR_NAME);";
			String query2 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT PK_QRTZ_CRON_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query3 = "ALTER TABLE QRTZ_FIRED_TRIGGERS ADD CONSTRAINT PK_QRTZ_FIRED_TRIGGERS PRIMARY KEY ( SCHED_NAME, ENTRY_ID );";
			String query4 = "ALTER TABLE QRTZ_PAUSED_TRIGGER_GRPS ADD CONSTRAINT PK_QRTZ_PAUSED_TRIGGER_GRPS PRIMARY KEY ( SCHED_NAME, TRIGGER_GROUP );";
			String query5 = "ALTER TABLE QRTZ_SCHEDULER_STATE ADD CONSTRAINT PK_QRTZ_SCHEDULER_STATE PRIMARY KEY ( SCHED_NAME, INSTANCE_NAME );";
			String query6 = "ALTER TABLE QRTZ_LOCKS ADD CONSTRAINT PK_QRTZ_LOCKS PRIMARY KEY ( SCHED_NAME, LOCK_NAME );";
			String query7 = "ALTER TABLE QRTZ_JOB_DETAILS ADD CONSTRAINT PK_QRTZ_JOB_DETAILS PRIMARY KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP );";
			String query8 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT PK_QRTZ_SIMPLE_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query9 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT PK_QRTZ_SIMPROP_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";
			String query10 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT PK_QRTZ_TRIGGERS PRIMARY KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP );";

			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_CALENDARS", "QRTZ_CALENDARS", database, schema)) {
					schedulerDb.insertData(query1);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_CALENDARS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_CRON_TRIGGERS", "QRTZ_CRON_TRIGGERS", database,
						schema)) {
					schedulerDb.insertData(query2);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_CRON_TRIGGERS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_FIRED_TRIGGERS", "QRTZ_FIRED_TRIGGERS", database,
						schema)) {
					schedulerDb.insertData(query3);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_FIRED_TRIGGERS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_PAUSED_TRIGGER_GRPS", "QRTZ_PAUSED_TRIGGER_GRPS",
						database, schema)) {
					schedulerDb.insertData(query4);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_PAUSED_TRIGGER_GRPS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_SCHEDULER_STATE", "QRTZ_SCHEDULER_STATE", database,
						schema)) {
					schedulerDb.insertData(query5);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_SCHEDULER_STATE: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_LOCKS", "QRTZ_LOCKS", database, schema)) {
					schedulerDb.insertData(query6);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_LOCKS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_JOB_DETAILS", "QRTZ_JOB_DETAILS", database,
						schema)) {
					schedulerDb.insertData(query7);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_JOB_DETAILS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_SIMPLE_TRIGGERS", "QRTZ_SIMPLE_TRIGGERS", database,
						schema)) {
					schedulerDb.insertData(query8);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_SIMPLE_TRIGGERS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_SIMPROP_TRIGGERS", "QRTZ_SIMPROP_TRIGGERS",
						database, schema)) {
					schedulerDb.insertData(query9);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_SIMPROP_TRIGGERS: {}", se.getMessage(), se);
			}
			try {
				if (!queryUtil.tableConstraintExists(conn, "PK_QRTZ_TRIGGERS", "QRTZ_TRIGGERS", database, schema)) {
					schedulerDb.insertData(query10);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add primary key PK_QRTZ_TRIGGERS: {}", se.getMessage(), se);
			}
		}
	}

	/**
	 * Adds the four foreign-key constraints linking QRTZ_CRON_TRIGGERS,
	 * QRTZ_SIMPLE_TRIGGERS, QRTZ_SIMPROP_TRIGGERS and QRTZ_TRIGGERS to their parent
	 * tables. Same idempotency strategy as
	 * {@link #addAllPrimaryKeys(Connection, String, String)}: uses
	 * {@code IF NOT EXISTS} when available, otherwise a metadata lookup per FK.
	 *
	 * @param conn     live JDBC connection to the scheduler db
	 * @param database database name (used for constraint-existence lookups)
	 * @param schema   schema name (used for constraint-existence lookups)
	 */
	private static void addAllForeignKeys(Connection conn, String database, String schema) {
		IRDBMSEngine schedulerDb = SystemEngineRegistry.getSchedulerDb();
		AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
		if (queryUtil.allowIfExistsAddConstraint()) {
			String query1 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query2 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query3 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query4 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT IF NOT EXISTS FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS FOREIGN KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP ) REFERENCES QRTZ_JOB_DETAILS ( SCHED_NAME, JOB_NAME, JOB_GROUP );";

			try {
				schedulerDb.insertData(query1);
				schedulerDb.insertData(query2);
				schedulerDb.insertData(query3);
				schedulerDb.insertData(query4);
			} catch (Exception se) {
				classLogger.error("Failed to add Quartz foreign key constraints (IF NOT EXISTS path): {}",
						se.getMessage(), se);
			}
		} else {
			String query1 = "ALTER TABLE QRTZ_CRON_TRIGGERS ADD CONSTRAINT FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query2 = "ALTER TABLE QRTZ_SIMPLE_TRIGGERS ADD CONSTRAINT FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query3 = "ALTER TABLE QRTZ_SIMPROP_TRIGGERS ADD CONSTRAINT FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS FOREIGN KEY ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) REFERENCES QRTZ_TRIGGERS ( SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP ) ON DELETE CASCADE;";
			String query4 = "ALTER TABLE QRTZ_TRIGGERS ADD CONSTRAINT FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS FOREIGN KEY ( SCHED_NAME, JOB_NAME, JOB_GROUP ) REFERENCES QRTZ_JOB_DETAILS ( SCHED_NAME, JOB_NAME, JOB_GROUP );";

			try {
				if (!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS", database,
						schema)) {
					schedulerDb.insertData(query1);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add foreign key FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS: {}", se.getMessage(),
						se);
			}
			try {
				if (!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS", database,
						schema)) {
					schedulerDb.insertData(query2);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add foreign key FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS: {}",
						se.getMessage(), se);
			}
			try {
				if (!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS", database,
						schema)) {
					schedulerDb.insertData(query3);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add foreign key FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS: {}",
						se.getMessage(), se);
			}
			try {
				if (!queryUtil.referentialConstraintExists(conn, "FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS", database,
						schema)) {
					schedulerDb.insertData(query4);
				}
			} catch (Exception se) {
				classLogger.error("Failed to add foreign key FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS: {}", se.getMessage(),
						se);
			}
		}
	}

	/**
	 * Maps an RDBMS type to the fully-qualified Quartz {@code DriverDelegate} class
	 * used to interpret the QRTZ_* tables. Used during quartz.properties assembly
	 * in {@link SchedulerFactorySingleton}.
	 *
	 * @param type rdbms type
	 * @return fully-qualified delegate class name; falls back to StdJDBCDelegate
	 *         for engines that don't need a specialized variant
	 */
	public static String getQuartzDelegateForRdbms(RdbmsTypeEnum type) {
		if (type == RdbmsTypeEnum.SQL_SERVER) {
			return "org.quartz.impl.jdbcjobstore.MSSQLDelegate";
		} else if (type == RdbmsTypeEnum.POSTGRES) {
			return "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate";
		} else if (type == RdbmsTypeEnum.ORACLE) {
			return "org.quartz.impl.jdbcjobstore.oracle.OracleDelegate";
		} else {
			return "org.quartz.impl.jdbcjobstore.StdJDBCDelegate";
		}
	}
}
