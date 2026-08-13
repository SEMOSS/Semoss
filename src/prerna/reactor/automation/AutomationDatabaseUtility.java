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
package prerna.reactor.automation;

import prerna.reactor.automation.utils.AutomationExecutionUtils;

import static prerna.reactor.automation.AutomationConstants.AUTOMATION_ID;
import static prerna.reactor.automation.AutomationConstants.BIGINT;
import static prerna.reactor.automation.AutomationConstants.CANCEL_REQUESTED;
import static prerna.reactor.automation.AutomationConstants.RESULT_SUMMARY_COL;
import static prerna.reactor.automation.AutomationConstants.CLAIMED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_NODES;
import static prerna.reactor.automation.AutomationConstants.CREATED_BY;
import static prerna.reactor.automation.AutomationConstants.DURATION_MS;
import static prerna.reactor.automation.AutomationConstants.ERROR_MESSAGE;
import static prerna.reactor.automation.AutomationConstants.EXECUTION_ORDER;
import static prerna.reactor.automation.AutomationConstants.FAILED_NODE_ID;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_PROJECT;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_STARTED;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_STATUS;
import static prerna.reactor.automation.AutomationConstants.INTEGER;
import static prerna.reactor.automation.AutomationConstants.LAST_HEARTBEAT;
import static prerna.reactor.automation.AutomationConstants.NODE_FIELD_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_FIELD_LABEL;
import static prerna.reactor.automation.AutomationConstants.NODE_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_LABEL;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_FAILED;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_PENDING;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_RUNNING;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_SUCCESS;
import static prerna.reactor.automation.AutomationConstants.NOT_NULL;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_PREVIEW;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_VALUE;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_VAR;
import static prerna.reactor.automation.AutomationConstants.PK_AUTOMATION_RUNS;
import static prerna.reactor.automation.AutomationConstants.PK_AUTO_ACTIVE_RUN;
import static prerna.reactor.automation.AutomationConstants.PK_AUTO_NODE_OUT;
import static prerna.reactor.automation.AutomationConstants.PROJECT_ID;
import static prerna.reactor.automation.AutomationConstants.RUN_ID;
import static prerna.reactor.automation.AutomationConstants.STALE_HEARTBEAT_THRESHOLD_MINUTES;
import static prerna.reactor.automation.AutomationConstants.STARTED_AT;
import static prerna.reactor.automation.AutomationConstants.STATUS;
import static prerna.reactor.automation.AutomationConstants.STATUS_INTERRUPTED;
import static prerna.reactor.automation.AutomationConstants.STATUS_RUNNING;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_ACTIVE_RUN;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_NODE_OUTPUTS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_RUNS;
import static prerna.reactor.automation.AutomationConstants.TOTAL_NODES;
import static prerna.reactor.automation.AutomationConstants.TRIGGER_TYPE;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_2000;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_255;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_50;
import static prerna.reactor.automation.AutomationConstants.VARCHAR_500;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Database utility for the Automation Engine subsystem.
 * Manages AUTOMATION_RUNS, AUTOMATION_NODE_OUTPUTS, and AUTOMATION_ACTIVE_RUN tables
 * in the scheduler database.
 *
 * Follows the same patterns as {@link prerna.reactor.scheduler.SchedulerDatabaseUtility}.
 * Called at platform startup to create tables; provides CRUD for automation execution state.
 */
public final class AutomationDatabaseUtility {

	private static final Logger classLogger = LogManager.getLogger(AutomationDatabaseUtility.class);

	// Table name shortcuts for SelectQueryStruct (TABLE__COLUMN format)
	private static final String TABLE_RUNS = TABLE_AUTOMATION_RUNS;
	private static final String TABLE_NODE_OUTPUTS = TABLE_AUTOMATION_NODE_OUTPUTS;
	private static final String TABLE_ACTIVE_RUN = TABLE_AUTOMATION_ACTIVE_RUN;

	private AutomationDatabaseUtility() {
		// static utility - no instantiation
	}

	// -- SQL Statements (INSERT/UPDATE/DELETE - PreparedStatement per SEMOSS conventions) --

	// AUTOMATION_RUNS
	private static final String INSERT_RUN = """
			INSERT INTO AUTOMATION_RUNS \
			(RUN_ID, PROJECT_ID, AUTOMATION_ID, STATUS, TRIGGER_TYPE, \
			STARTED_AT, LAST_HEARTBEAT, TOTAL_NODES, COMPLETED_NODES, CREATED_BY) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""";

	private static final String UPDATE_RUN_STATUS = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			FAILED_NODE_ID = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ?""";

	private static final String UPDATE_RUN_SUMMARY =
			"UPDATE AUTOMATION_RUNS SET RESULT_SUMMARY = ? WHERE RUN_ID = ?";

	private static final String UPDATE_HEARTBEAT =
			"UPDATE AUTOMATION_RUNS SET LAST_HEARTBEAT = ?, COMPLETED_NODES = ? WHERE RUN_ID = ?";

	private static final String TOUCH_HEARTBEAT =
			"UPDATE AUTOMATION_RUNS SET LAST_HEARTBEAT = ? WHERE RUN_ID = ?";

	private static final String SET_CANCEL_REQUESTED =
			"UPDATE AUTOMATION_RUNS SET CANCEL_REQUESTED = ? WHERE RUN_ID = ?";

	// AUTOMATION_ACTIVE_RUN - single row per project, PK on PROJECT_ID enforces exclusivity
	private static final String CLAIM_ACTIVE_RUN =
			"INSERT INTO AUTOMATION_ACTIVE_RUN (PROJECT_ID, RUN_ID, CLAIMED_AT) VALUES (?, ?, ?)";

	private static final String RELEASE_ACTIVE_RUN =
			"DELETE FROM AUTOMATION_ACTIVE_RUN WHERE PROJECT_ID = ? AND RUN_ID = ?";

	private static final String MARK_STALE_INTERRUPTED = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			ERROR_MESSAGE = ? WHERE RUN_ID = ?""";

	// AUTOMATION_NODE_OUTPUTS
	private static final String INSERT_NODE_OUTPUT = """
			INSERT INTO AUTOMATION_NODE_OUTPUTS \
			(RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS) \
			VALUES (?, ?, ?, ?, ?)""";

	private static final String UPDATE_NODE_OUTPUT_SUCCESS = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, OUTPUT_VAR = ?, OUTPUT_VALUE = ?, OUTPUT_PREVIEW = ? \
			WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_OUTPUT_FAILED = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_STATUS =
			"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ? WHERE RUN_ID = ? AND NODE_ID = ?";

	// -- Initialization ------------------------------------------------------------

	/**
	 * Creates automation tables in the scheduler DB if they don't exist, and
	 * registers them in the OWL. Called at platform startup after the scheduler
	 * DB is loaded. Safe to call on every startup (uses IF NOT EXISTS / metadata
	 * checks).
	 */
	public static void initialize() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			classLogger.warn("Scheduler DB not available - automation tables will not be created");
			return;
		}

		// Register the automation OWL schema in the scheduler DB if any tables or
		// columns are missing. This keeps the OWL declaration entirely within this
		// package rather than depending on SchedulerOwlCreator.
		AutomationOwlCreator owlCreator = new AutomationOwlCreator();
		if (owlCreator.needsRemake(schedulerDb)) {
			try {
				owlCreator.remakeOwl(schedulerDb);
			} catch (Exception e) {
				classLogger.error("Failed to update automation OWL schema in scheduler DB", e);
			}
		}

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
			String database = schedulerDb.getDatabase();
			String schema = schedulerDb.getSchema();

			boolean allowIfExists = queryUtil.allowsIfExistsTableSyntax();
			String dateTimeType = queryUtil.getDateWithTimeDataType();
			String clobType = queryUtil.getClobDataTypeName();

			createAutomationRunsTable(conn, queryUtil, database, schema, allowIfExists, dateTimeType, clobType);
			createAutomationNodeOutputsTable(conn, queryUtil, database, schema, allowIfExists, dateTimeType, clobType);
			createAutomationActiveRunTable(conn, queryUtil, database, schema, allowIfExists, dateTimeType);

			if (!conn.getAutoCommit()) {
				conn.commit();
			}

			classLogger.info("Automation engine tables initialized successfully");
		} catch (Exception e) {
			classLogger.error("Failed to initialize automation engine tables", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * On startup, marks any runs stuck in RUNNING as INTERRUPTED.
	 * This handles the case where the server crashed mid-execution.
	 */
	public static void markStaleRunsInterrupted() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RUN_ID, RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + PROJECT_ID, PROJECT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + LAST_HEARTBEAT, LAST_HEARTBEAT));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__" + STATUS, "==", STATUS_RUNNING, PixelDataType.CONST_STRING));

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results == null || results.isEmpty()) {
			return;
		}

		Timestamp threshold = toTimestamp(Instant.now().minusSeconds(
				STALE_HEARTBEAT_THRESHOLD_MINUTES * 60L));
		Timestamp now = toTimestamp(Instant.now());

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			for (Map<String, Object> row : results) {
				String runId = (String) row.get(RUN_ID);
				String projectId = (String) row.get(PROJECT_ID);

				// Only interrupt runs whose heartbeat is actually stale. A run with a fresh
				// heartbeat is still alive (e.g. executing on another node in a cluster), so
				// interrupting it would clobber active work. A missing/unparseable heartbeat
				// is treated as stale (a crashed run that never checkpointed).
				Timestamp lastHeartbeat = toTimestampSafe(row.get(LAST_HEARTBEAT));
				if (lastHeartbeat != null && lastHeartbeat.after(threshold)) {
					classLogger.debug("Skipping automation run {} - heartbeat {} is newer than stale threshold {}",
							runId, lastHeartbeat, threshold);
					continue;
				}

				try (PreparedStatement ps = conn.prepareStatement(MARK_STALE_INTERRUPTED)) {
					int index = 1;
					ps.setString(index++, STATUS_INTERRUPTED);
					ps.setTimestamp(index++, now);
					ps.setString(index++, "Server restarted during execution");
					ps.setString(index++, runId);
					int updated = ps.executeUpdate();
					if (updated > 0) {
						classLogger.info("Marked stale automation run {} as INTERRUPTED", runId);
					}
				}
				// Release the active-run slot so the project can be re-triggered - otherwise a
				// crashed run would permanently block that project from ever running again.
				if (projectId != null) {
					releaseActiveRun(projectId, runId);
				}
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to mark stale automation runs", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	// -- AUTOMATION_RUNS CRUD --------------------------------------------------------

	/**
	 * Checks if an automation already has an active (RUNNING) run for the given project.
	 *
	 * @return the active run ID, or null if no run is active
	 */
	public static String getActiveRun(String projectId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RUN_ID, RUN_ID));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__" + PROJECT_ID, "==", projectId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__" + STATUS, "==", STATUS_RUNNING, PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			Object runId = results.get(0).get(RUN_ID);
			return runId != null ? runId.toString() : null;
		}
		return null;
	}

	/**
	 * Atomically claims the "active run" slot for a project. Backed by a single-row-per-project
	 * marker table ({@code AUTOMATION_ACTIVE_RUN}, PK on {@code PROJECT_ID}) in the shared scheduler
	 * DB, so this is correct across every pod in a cluster - not just within one JVM. Unlike
	 * {@link #getActiveRun(String)} (a plain SELECT), this is a single atomic INSERT: a PK
	 * violation means another run is already active for that project, closing the check-then-insert
	 * race where two concurrent triggers for the same project could otherwise both start a run and
	 * double up any node with side effects (e.g. a database-update node running twice).
	 *
	 * <p>If the scheduler DB is unavailable, fails open (returns true) to match the existing
	 * degraded-mode behavior of the rest of this class (e.g. {@link #insertRun}, which silently
	 * no-ops when the scheduler DB can't be reached) rather than introduce a new failure mode.
	 *
	 * @return true if the slot was claimed (caller may proceed), false if another run already
	 *         holds it for this project
	 */
	public static boolean claimActiveRun(String projectId, String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			classLogger.warn("Scheduler DB not available - cannot enforce single-active-run guard for project {}", projectId);
			return true;
		}

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(CLAIM_ACTIVE_RUN)) {
				int index = 1;
				ps.setString(index++, projectId);
				ps.setString(index++, runId);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			// Constraint violation (another run already holds this project's slot) is the
			// expected/common case here, not an error - log at debug, not error.
			classLogger.debug("Could not claim active-run slot for project {} (likely already active): {}",
					projectId, e.getMessage());
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Releases the "active run" slot for a project, allowing a new run to be claimed.
	 * Must be called on every terminal run status (SUCCESS/FAILED/CANCELLED/INTERRUPTED),
	 * including the stale-run sweep in {@link #markStaleRunsInterrupted()}.
	 */
	public static boolean releaseActiveRun(String projectId, String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(RELEASE_ACTIVE_RUN)) {
				ps.setString(1, projectId);
				ps.setString(2, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to release active-run slot for project {}, run {}",
					projectId, runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Returns the active run ID for a project directly from the {@code AUTOMATION_ACTIVE_RUN} lock
	 * table. Unlike {@link #getActiveRun(String)}, this is populated at {@link #claimActiveRun} time
	 * — before {@code AUTOMATION_RUNS} is written — so callers polling for a newly started run will
	 * see it sooner.
	 *
	 * @return the run ID, or {@code null} if no run is currently active for the project
	 */
	public static String getClaimedActiveRun(String projectId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_ACTIVE_RUN + "__" + RUN_ID, RUN_ID));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_ACTIVE_RUN + "__" + PROJECT_ID, "==", projectId, PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			Object runId = results.get(0).get(RUN_ID);
			return runId != null ? runId.toString() : null;
		}
		return null;
	}

	/**
	 * Sets the cluster-safe cancellation flag on a run. Called by {@code CancelAutomationRunReactor}
	 * regardless of which pod receives the cancel request - unlike the in-memory
	 * {@code TriggerAutomationReactor.CANCELLATION_FLAGS} map (a same-pod-only fast path), this is
	 * visible to whichever pod is actually executing the run via {@link #isCancelRequested(String)}.
	 */
	public static boolean setCancelRequested(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(SET_CANCEL_REQUESTED)) {
				ps.setBoolean(1, true);
				ps.setString(2, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to set cancel-requested flag for run '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Checks the cluster-safe cancellation flag for a run. Polled by the executing pod's
	 * between-node cancellation check in addition to the local in-memory flag, so a cancel
	 * request landing on a different pod than the one executing the run is still honored.
	 */
	public static boolean isCancelRequested(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + CANCEL_REQUESTED,
				CANCEL_REQUESTED));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__" + RUN_ID, "==", runId, PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results == null || results.isEmpty()) {
			return false;
		}
		Object flag = results.get(0).get(CANCEL_REQUESTED);
		if (flag instanceof Boolean) {
			return (Boolean) flag;
		}
		return flag != null && Boolean.parseBoolean(flag.toString());
	}

	/**
	 * Inserts a new automation run record.
	 */
	public static boolean insertRun(String runId, String projectId, String automationId,
			String triggerType, int totalNodes, String createdBy) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			Timestamp now = toTimestamp(Instant.now());

			try (PreparedStatement ps = conn.prepareStatement(INSERT_RUN)) {
				int index = 1;
				ps.setString(index++, runId);
				ps.setString(index++, projectId);
				ps.setString(index++, automationId);
				ps.setString(index++, STATUS_RUNNING);
				ps.setString(index++, triggerType);
				ps.setTimestamp(index++, now);
				ps.setTimestamp(index++, now);
				ps.setInt(index++, totalNodes);
				ps.setString(index++, createdBy);
				ps.executeUpdate();
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to insert automation run '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates the status of an automation run (on completion or failure).
	 */
	public static boolean updateRunStatus(String runId, String status,
			String failedNodeId, String errorMessage) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(UPDATE_RUN_STATUS)) {
				int index = 1;
				ps.setString(index++, status);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				setNullableString(ps, index++, failedNodeId);
				setNullableString(ps, index++, errorMessage);
				ps.setString(index++, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to update run status for '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Persists the human-readable outcome summary for a completed run.
	 * Called after the run finishes, separately from {@link #updateRunStatus} because the
	 * summary is built by the caller ({@code TriggerAutomationReactor}) after the engine returns.
	 */
	public static boolean updateRunSummary(String runId, String resultSummary) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(UPDATE_RUN_SUMMARY)) {
				setNullableString(ps, 1, resultSummary);
				ps.setString(2, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to update run summary for '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates the heartbeat timestamp and completed node count for a running automation.
	 */
	public static boolean updateHeartbeat(String runId, int completedNodes) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(UPDATE_HEARTBEAT)) {
				int index = 1;
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setInt(index++, completedNodes);
				ps.setString(index++, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to update heartbeat for run '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates only the heartbeat timestamp for a running automation.
	 * Used when the node count hasn't changed but liveness needs to be signaled.
	 */
	public static boolean touchHeartbeat(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(TOUCH_HEARTBEAT)) {
				ps.setTimestamp(1, toTimestamp(Instant.now()));
				ps.setString(2, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to touch heartbeat for run '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Lists automation runs for a project, newest first.
	 *
	 * @param projectId the project to query
	 * @param limit     max number of runs to return
	 * @return list of run summary maps
	 */
	public static List<Map<String, Object>> getRunsForProject(String projectId, int limit) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RUN_ID, RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + PROJECT_ID, PROJECT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AUTOMATION_ID, AUTOMATION_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STATUS, STATUS));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TRIGGER_TYPE, TRIGGER_TYPE));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STARTED_AT, STARTED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_AT, COMPLETED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + FAILED_NODE_ID, FAILED_NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TOTAL_NODES, TOTAL_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_NODES, COMPLETED_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + CREATED_BY, CREATED_BY));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RESULT_SUMMARY_COL, RESULT_SUMMARY_COL));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__" + PROJECT_ID, "==", projectId, PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_RUNS + "__" + STARTED_AT,
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.DESC.toString());
		qs.setLimit(limit);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return results != null ? results : new ArrayList<>();
	}

	/**
	 * Gets a single run detail by run ID (includes error message).
	 */
	public static Map<String, Object> getRunDetail(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RUN_ID, RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + PROJECT_ID, PROJECT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AUTOMATION_ID, AUTOMATION_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STATUS, STATUS));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TRIGGER_TYPE, TRIGGER_TYPE));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STARTED_AT, STARTED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_AT, COMPLETED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + FAILED_NODE_ID, FAILED_NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + ERROR_MESSAGE, ERROR_MESSAGE));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TOTAL_NODES, TOTAL_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_NODES, COMPLETED_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + CREATED_BY, CREATED_BY));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RESULT_SUMMARY_COL, RESULT_SUMMARY_COL));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__" + RUN_ID, "==", runId, PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			return results.get(0);
		}
		return null;
	}

	// -- AUTOMATION_NODE_OUTPUTS CRUD ------------------------------------------------

	/**
	 * Batch-inserts all node outputs for a run (all PENDING).
	 */
	public static boolean insertAllNodeOutputs(String runId, List<Map<String, Object>> orderedNodes) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(INSERT_NODE_OUTPUT)) {
				for (int i = 0; i < orderedNodes.size(); i++) {
					Map<String, Object> node = orderedNodes.get(i);
					int index = 1;
					ps.setString(index++, runId);
					ps.setString(index++, (String) node.get(NODE_FIELD_ID));
					ps.setString(index++, (String) node.get(NODE_FIELD_LABEL));
					ps.setInt(index++, i);
					ps.setString(index++, NODE_STATUS_PENDING);
					ps.addBatch();
				}
				ps.executeBatch();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to batch-insert node outputs for run '{}'", runId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Marks a node as RUNNING (before pixel execution starts).
	 */
	public static boolean markNodeRunning(String runId, String nodeId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_STATUS)) {
				int index = 1;
				ps.setString(index++, NODE_STATUS_RUNNING);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to mark node running for run '{}', node '{}'",
					runId, nodeId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates a node output after successful execution.
	 */
	public static boolean updateNodeSuccess(String runId, String nodeId, Timestamp startedAt,
			long durationMs, String outputVar, String outputValue, String outputPreview) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();

			try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_SUCCESS)) {
				int index = 1;
				ps.setString(index++, NODE_STATUS_SUCCESS);
				ps.setTimestamp(index++, startedAt);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setLong(index++, durationMs);
				ps.setString(index++, outputVar);
				// Handle CLOB for potentially large output values
				queryUtil.handleInsertionOfClob(conn, ps, outputValue, index++, AutomationExecutionUtils.GSON);
				ps.setString(index++, outputPreview);
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to update node success for run '{}', node '{}'",
					runId, nodeId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates a node output after failed execution.
	 */
	public static boolean updateNodeFailed(String runId, String nodeId, Timestamp startedAt,
			long durationMs, String errorMessage) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_FAILED)) {
				int index = 1;
				ps.setString(index++, NODE_STATUS_FAILED);
				ps.setTimestamp(index++, startedAt);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setLong(index++, durationMs);
				setNullableString(ps, index++, errorMessage);
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to update node failed for run '{}', node '{}'",
					runId, nodeId, e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Gets all node outputs for a run, ordered by execution order.
	 */
	public static List<Map<String, Object>> getNodeOutputsForRun(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + NODE_ID, NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + NODE_LABEL, NODE_LABEL));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + EXECUTION_ORDER, EXECUTION_ORDER));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + STATUS, STATUS));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + STARTED_AT, STARTED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + COMPLETED_AT, COMPLETED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + DURATION_MS, DURATION_MS));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_VAR, OUTPUT_VAR));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_VALUE, OUTPUT_VALUE));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_PREVIEW, OUTPUT_PREVIEW));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + ERROR_MESSAGE, ERROR_MESSAGE));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_NODE_OUTPUTS + "__" + RUN_ID, "==", runId, PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_NODE_OUTPUTS + "__" + EXECUTION_ORDER,
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return results != null ? results : new ArrayList<>();
	}

	// -- Result Assembly -----------------------------------------------------------

	/**
	 * Builds a list of per-node result maps from the raw DB output rows returned by
	 * {@link #getNodeOutputsForRun(String)}. The shape matches what
	 * {@link prerna.reactor.automation.GetAutomationRunReactor} and
	 * {@link prerna.reactor.automation.TriggerAutomationReactor} return to callers.
	 *
	 * <p>Each entry contains: nodeId, nodeLabel, status, durationMs, outputPreview
	 * (falls back from outputValue when blank), outputValue, and errorMessage.
	 *
	 * @param nodeOutputs ordered rows from {@link #getNodeOutputsForRun(String)}
	 * @return mutable list of node result maps (empty when {@code nodeOutputs} is null)
	 */
	public static List<Map<String, Object>> buildNodeResults(List<Map<String, Object>> nodeOutputs) {
		List<Map<String, Object>> nodeResults = new ArrayList<>();
		if (nodeOutputs == null) {
			return nodeResults;
		}
		for (Map<String, Object> output : nodeOutputs) {
			Map<String, Object> nodeResult = new java.util.HashMap<>();
			nodeResult.put(AutomationConstants.NODE_ID, output.get(AutomationConstants.NODE_ID));
			nodeResult.put(AutomationConstants.NODE_LABEL, output.get(AutomationConstants.NODE_LABEL));
			nodeResult.put(AutomationConstants.STATUS, output.get(AutomationConstants.STATUS));
			nodeResult.put(AutomationConstants.DURATION_MS, output.get(AutomationConstants.DURATION_MS));
			String outputForDisplay = (String) output.get(AutomationConstants.OUTPUT_VALUE);
			if (outputForDisplay == null || outputForDisplay.isBlank()) {
				outputForDisplay = (String) output.get(AutomationConstants.OUTPUT_PREVIEW);
			}
			nodeResult.put(AutomationConstants.OUTPUT_PREVIEW, outputForDisplay);
			nodeResult.put(AutomationConstants.OUTPUT_VALUE, output.get(AutomationConstants.OUTPUT_VALUE));
			nodeResult.put(AutomationConstants.ERROR_MESSAGE, output.get(AutomationConstants.ERROR_MESSAGE));
			nodeResults.add(nodeResult);
		}
		return nodeResults;
	}

	// -- Table Creation ------------------------------------------------------------

	private static void createAutomationRunsTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType, String clobType) throws SQLException {

		String tableName = TABLE_AUTOMATION_RUNS;

		if (!allowIfExists && queryUtil.tableExists(conn, tableName, database, schema)) {
			return;
		}

		String[] colNames = { RUN_ID, PROJECT_ID, AUTOMATION_ID, STATUS, TRIGGER_TYPE,
				STARTED_AT, COMPLETED_AT, FAILED_NODE_ID, ERROR_MESSAGE, LAST_HEARTBEAT,
				TOTAL_NODES, COMPLETED_NODES, CREATED_BY, CANCEL_REQUESTED, RESULT_SUMMARY_COL };
		String[] types = { VARCHAR_255, VARCHAR_255, VARCHAR_255, VARCHAR_50, VARCHAR_50,
				dateTimeType, dateTimeType, VARCHAR_255, clobType, dateTimeType,
				INTEGER, INTEGER, VARCHAR_255, queryUtil.getBooleanDataTypeName(), VARCHAR_2000 };
		String[] constraints = { NOT_NULL, NOT_NULL, null, NOT_NULL, NOT_NULL,
				NOT_NULL, null, null, null, null,
				null, null, null, null, null };

		String sql;
		if (allowIfExists) {
			sql = queryUtil.createTableIfNotExistsWithCustomConstraints(tableName, colNames, types, constraints);
		} else {
			sql = queryUtil.createTableWithCustomConstraints(tableName, colNames, types, constraints);
		}
		classLogger.info("Creating table {}: {}", tableName, sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.execute();
		}

		// Migrate installs that predate cluster-safe cancel
		addColumnIfNotExists(conn, queryUtil, tableName, CANCEL_REQUESTED, queryUtil.getBooleanDataTypeName());
		// Migrate installs that predate result summary
		addColumnIfNotExists(conn, queryUtil, tableName, RESULT_SUMMARY_COL, VARCHAR_2000);

		// Primary key
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, PK_AUTOMATION_RUNS,
				new String[]{ RUN_ID });

		// Indexes
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_AR_PROJECT, tableName,
				new String[]{ PROJECT_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_AR_STATUS, tableName,
				new String[]{ PROJECT_ID, STATUS });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_AR_STARTED, tableName,
				new String[]{ PROJECT_ID, STARTED_AT });
	}

	private static void createAutomationNodeOutputsTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType, String clobType) throws SQLException {

		String tableName = TABLE_AUTOMATION_NODE_OUTPUTS;

		if (!allowIfExists && queryUtil.tableExists(conn, tableName, database, schema)) {
			return;
		}

		String[] colNames = { RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS,
				STARTED_AT, COMPLETED_AT, DURATION_MS, OUTPUT_VAR,
				OUTPUT_VALUE, OUTPUT_PREVIEW, ERROR_MESSAGE };
		String[] types = { VARCHAR_255, VARCHAR_255, VARCHAR_500, INTEGER, VARCHAR_50,
				dateTimeType, dateTimeType, BIGINT, VARCHAR_255,
				clobType, VARCHAR_2000, clobType };
		String[] constraints = { NOT_NULL, NOT_NULL, null, NOT_NULL, NOT_NULL,
				null, null, null, null,
				null, null, null };

		String sql;
		if (allowIfExists) {
			sql = queryUtil.createTableIfNotExistsWithCustomConstraints(tableName, colNames, types, constraints);
		} else {
			sql = queryUtil.createTableWithCustomConstraints(tableName, colNames, types, constraints);
		}
		classLogger.info("Creating table {}: {}", tableName, sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.execute();
		}

		// Composite primary key
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, PK_AUTO_NODE_OUT,
				new String[]{ RUN_ID, NODE_ID });

		// Indexes
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_RUN, tableName,
				new String[]{ RUN_ID });
	}

	/**
	 * Creates the AUTOMATION_ACTIVE_RUN marker table - a single row per project, keyed on
	 * PROJECT_ID, used to atomically enforce "at most one active run per project" cluster-wide.
	 * See {@link #claimActiveRun(String, String)} / {@link #releaseActiveRun(String, String)}.
	 */
	private static void createAutomationActiveRunTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType) throws SQLException {

		String tableName = TABLE_AUTOMATION_ACTIVE_RUN;

		if (!allowIfExists && queryUtil.tableExists(conn, tableName, database, schema)) {
			return;
		}

		String[] colNames = { PROJECT_ID, RUN_ID, CLAIMED_AT };
		String[] types = { VARCHAR_255, VARCHAR_255, dateTimeType };
		String[] constraints = { NOT_NULL, NOT_NULL, NOT_NULL };

		String sql;
		if (allowIfExists) {
			sql = queryUtil.createTableIfNotExistsWithCustomConstraints(tableName, colNames, types, constraints);
		} else {
			sql = queryUtil.createTableWithCustomConstraints(tableName, colNames, types, constraints);
		}
		classLogger.info("Creating table {}: {}", tableName, sql);
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.execute();
		}

		// Primary key on PROJECT_ID alone (not RUN_ID) is what makes claimActiveRun atomic:
		// a second INSERT for the same project - from any pod - violates this constraint.
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, PK_AUTO_ACTIVE_RUN,
				new String[]{ PROJECT_ID });
	}

	// -- Helpers -------------------------------------------------------------------

	private static IRDBMSEngine getSchedulerDb() {
		try {
			return SystemEngineRegistry.getSchedulerDb();
		} catch (Exception e) {
			classLogger.warn("Could not obtain scheduler DB: {}", e.getMessage());
			return null;
		}
	}

	private static void closeConnection(IRDBMSEngine engine, Connection conn) {
		ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
	}

	/**
	 * Binds a nullable VARCHAR column value, using {@code setNull(Types.VARCHAR)} instead of
	 * {@code setString(index, null)} when the value is absent - some JDBC drivers require an
	 * explicit SQL type for a null bind rather than inferring it from a null String argument.
	 */
	private static void setNullableString(PreparedStatement ps, int index, String value) throws SQLException {
		if (value != null) {
			ps.setString(index, value);
		} else {
			ps.setNull(index, Types.VARCHAR);
		}
	}

	private static Timestamp toTimestamp(Instant instant) {
		return Utility.getSqlTimestampUTC(
				LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
	}

	/**
	 * Best-effort conversion of a value read from the result set into a {@link Timestamp}.
	 * Handles {@link Timestamp}, any {@link Date}, and parseable timestamp strings.
	 * Returns null when the value is null or cannot be interpreted.
	 */
	private static Timestamp toTimestampSafe(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Timestamp) {
			return (Timestamp) value;
		}
		if (value instanceof Date) {
			return new Timestamp(((Date) value).getTime());
		}
		try {
			return Timestamp.valueOf(value.toString().trim());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	private static void addPrimaryKeyIfNotExists(Connection conn, AbstractSqlQueryUtil queryUtil,
			String tableName, String database, String schema, String pkName, String[] columns) {
		try {
			if (queryUtil.allowIfExistsAddConstraint()) {
				String colList = String.join(", ", columns);
				String sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT IF NOT EXISTS " +
						pkName + " PRIMARY KEY (" + colList + ")";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.execute();
				}
			} else {
				// Try to add and swallow the error if it already exists
				String colList = String.join(", ", columns);
				String sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT " +
						pkName + " PRIMARY KEY (" + colList + ")";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.execute();
				}
			}
		} catch (Exception e) {
			classLogger.debug("Primary key {} may already exist on {}: {}", pkName, tableName, e.getMessage());
		}
	}

	/**
	 * Adds a column to an existing table if it isn't already present - used to migrate
	 * installs that predate a column addition. Errors (column already exists) are swallowed.
	 */
	private static void addColumnIfNotExists(Connection conn, AbstractSqlQueryUtil queryUtil,
			String tableName, String columnName, String columnType) {
		try {
			String sql = queryUtil.allowIfExistsModifyColumnSyntax()
					? queryUtil.alterTableAddColumnIfNotExists(tableName, columnName, columnType)
					: queryUtil.alterTableAddColumn(tableName, columnName, columnType);
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.execute();
			}
		} catch (Exception e) {
			classLogger.debug("Column {} may already exist on {}: {}", columnName, tableName, e.getMessage());
		}
	}

	private static void createIndexIfNotExists(Connection conn, AbstractSqlQueryUtil queryUtil,
			boolean allowIfExists, String indexName, String tableName, String[] columns) {
		try {
			List<String> colList = Arrays.asList(columns);
			String sql;
			if (allowIfExists && queryUtil.allowIfExistsIndexSyntax()) {
				sql = queryUtil.createIndexIfNotExists(indexName, tableName, colList);
			} else {
				sql = queryUtil.createIndex(indexName, tableName, colList);
			}
			if (sql != null && !sql.isEmpty()) {
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.execute();
				}
			}
		} catch (Exception e) {
			classLogger.debug("Index {} may already exist on {}: {}", indexName, tableName, e.getMessage());
		}
	}

}
