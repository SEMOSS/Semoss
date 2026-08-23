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

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

import static prerna.reactor.automation.AutomationConstants.AUTOMATION_ID;
import static prerna.reactor.automation.AutomationConstants.AGENT_RUN_ID;
import static prerna.reactor.automation.AutomationConstants.BIGINT;
import static prerna.reactor.automation.AutomationConstants.CANCEL_REQUESTED;
import static prerna.reactor.automation.AutomationConstants.RESULT_SUMMARY_COL;
import static prerna.reactor.automation.AutomationConstants.CLAIMED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_NODES;
import static prerna.reactor.automation.AutomationConstants.CREATED_BY;
import static prerna.reactor.automation.AutomationConstants.DEFINITION_HASH;
import static prerna.reactor.automation.AutomationConstants.DEFINITION_SNAPSHOT;
import static prerna.reactor.automation.AutomationConstants.DEFINITION_VERSION;
import static prerna.reactor.automation.AutomationConstants.DURATION_MS;
import static prerna.reactor.automation.AutomationConstants.ERROR_MESSAGE;
import static prerna.reactor.automation.AutomationConstants.EXECUTION_ORDER;
import static prerna.reactor.automation.AutomationConstants.FAILED_NODE_ID;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_AGENT_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_MODEL_MSG;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_ROOM;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_PROJECT;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_STARTED;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_STATUS;
import static prerna.reactor.automation.AutomationConstants.INTEGER;
import static prerna.reactor.automation.AutomationConstants.LAST_HEARTBEAT;
import static prerna.reactor.automation.AutomationConstants.MODEL_MESSAGE_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_FIELD_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_FIELD_LABEL;
import static prerna.reactor.automation.AutomationConstants.NODE_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_LABEL;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_FAILED;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_PENDING;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_RUNNING;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_SKIPPED;
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
import static prerna.reactor.automation.AutomationConstants.ROOM_ID;
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

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
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
			(RUN_ID, PROJECT_ID, AUTOMATION_ID, DEFINITION_VERSION, DEFINITION_HASH, DEFINITION_SNAPSHOT, \
			STATUS, TRIGGER_TYPE, \
			STARTED_AT, LAST_HEARTBEAT, TOTAL_NODES, COMPLETED_NODES, CREATED_BY) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""";

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
			(RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS, ROOM_ID) \
			VALUES (?, ?, ?, ?, ?, ?)""";

	private static final String UPDATE_NODE_OUTPUT_SUCCESS = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, OUTPUT_VAR = ?, OUTPUT_VALUE = ?, OUTPUT_PREVIEW = ?, \
			MODEL_MESSAGE_ID = ?, AGENT_RUN_ID = ? \
			WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_OUTPUT_FAILED = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_STATUS =
			"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ? WHERE RUN_ID = ? AND NODE_ID = ?";

	private static final String SKIP_PENDING_NODE_OUTPUTS =
			"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ? AND STATUS = ?";

	// -- Initialization ------------------------------------------------------------

	/**
	 * Creates and migrates the physical automation tables in the scheduler DB.
	 * The scheduler's authoritative OWL schema is owned by
	 * {@link prerna.reactor.scheduler.SchedulerOwlCreator}. Called at platform
	 * startup after the scheduler DB and its OWL are initialized. Safe to call on
	 * every startup (uses IF NOT EXISTS / metadata checks).
	 */
	public static void initialize() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			classLogger.warn("Scheduler DB not available - automation tables will not be created");
			return;
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
	 * Claims the project's active-run slot and creates its complete initial history. The claim,
	 * run record, and all pending node-output rows are committed as one transaction, so callers
	 * cannot observe an active run without its run and node records.
	 *
	 * @return {@code true} when the run was initialized; {@code false} when the active-run insert
	 *         was rejected (normally because another run holds the project)
	 * @throws IllegalStateException when the scheduler database is unavailable or history cannot be
	 *                               initialized
	 */
	public static boolean claimAndInitializeRun(String runId, String projectId, String automationId,
			int definitionVersion, String definitionHash, String definitionSnapshot,
			String triggerType, String createdBy, List<Map<String, Object>> orderedNodes,
			Map<String, String> traceRoomIds) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			throw new IllegalStateException(
					"Scheduler DB is not available; automation run history cannot be initialized.");
		}

		Connection conn = null;
		boolean originalAutoCommit = false;
		try {
			conn = schedulerDb.getConnection();
			originalAutoCommit = conn.getAutoCommit();
			if (originalAutoCommit) {
				conn.setAutoCommit(false);
			}
			Timestamp now = toTimestamp(Instant.now());

			try (PreparedStatement ps = conn.prepareStatement(CLAIM_ACTIVE_RUN)) {
				int index = 1;
				ps.setString(index++, projectId);
				ps.setString(index++, runId);
				ps.setTimestamp(index++, now);
				try {
					ps.executeUpdate();
				} catch (SQLException e) {
					if (isConstraintViolation(e)) {
						rollback(conn, e);
						classLogger.debug("Active-run slot is already claimed for project {}: {}",
								projectId, e.getMessage());
						return false;
					}
					throw e;
				}
			}

			insertRun(conn, schedulerDb.getQueryUtil(), runId, projectId, automationId,
					definitionVersion, definitionHash, definitionSnapshot, triggerType,
					orderedNodes.size(), createdBy, now);
			insertAllNodeOutputs(conn, runId, orderedNodes, traceRoomIds);
			conn.commit();
			return true;
		} catch (Exception e) {
			rollback(conn, e);
			classLogger.error("Failed to initialize automation run '{}' for project '{}'", runId, projectId, e);
			throw new IllegalStateException("Unable to initialize automation run history.", e);
		} finally {
			restoreAutoCommit(conn, originalAutoCommit);
			closeConnection(schedulerDb, conn);
		}
	}

	private static void insertRun(Connection conn, AbstractSqlQueryUtil queryUtil, String runId,
			String projectId, String automationId, int definitionVersion, String definitionHash,
			String definitionSnapshot, String triggerType, int totalNodes, String createdBy,
			Timestamp now) throws SQLException, UnsupportedEncodingException {
		try (PreparedStatement ps = conn.prepareStatement(INSERT_RUN)) {
			int index = 1;
			ps.setString(index++, runId);
			ps.setString(index++, projectId);
			ps.setString(index++, automationId);
			ps.setInt(index++, definitionVersion);
			ps.setString(index++, definitionHash);
			queryUtil.handleInsertionOfClob(conn, ps, definitionSnapshot, index++, AutomationRuntimeUtils.GSON);
			ps.setString(index++, STATUS_RUNNING);
			ps.setString(index++, triggerType);
			ps.setTimestamp(index++, now);
			ps.setTimestamp(index++, now);
			ps.setInt(index++, totalNodes);
			ps.setString(index++, createdBy);
			ps.executeUpdate();
		}
	}

	private static void insertAllNodeOutputs(Connection conn, String runId,
			List<Map<String, Object>> orderedNodes, Map<String, String> traceRoomIds) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(INSERT_NODE_OUTPUT)) {
			for (int i = 0; i < orderedNodes.size(); i++) {
				Map<String, Object> node = orderedNodes.get(i);
				int index = 1;
				ps.setString(index++, runId);
				ps.setString(index++, (String) node.get(NODE_FIELD_ID));
				ps.setString(index++, (String) node.get(NODE_FIELD_LABEL));
				ps.setInt(index++, i);
				ps.setString(index++, NODE_STATUS_PENDING);
				setNullableString(ps, index++, traceRoomIds == null
						? null
						: traceRoomIds.get((String) node.get(NODE_FIELD_ID)));
				ps.addBatch();
			}
			ps.executeBatch();
		}
	}

	/**
	 * Releases the "active run" slot for a project, allowing a new run to be claimed.
	 * Must be called on every terminal run status (SUCCESS/FAILED/CANCELLED/INTERRUPTED),
	 * including the stale-run sweep in {@link #markStaleRunsInterrupted()}.
	 */
	public static void releaseActiveRun(String projectId, String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("release the active automation run");

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
		} catch (SQLException e) {
			rollback(conn, e);
			classLogger.error("Failed to release active-run slot for project {}, run {}",
					projectId, runId, e);
			throw new IllegalStateException("Unable to release the active automation run.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Returns the active run ID for a project directly from the {@code AUTOMATION_ACTIVE_RUN} lock
	 * table. It is populated by {@link #claimAndInitializeRun} in the same transaction as
	 * {@code AUTOMATION_RUNS} and the pending node-output rows.
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
	public static void setCancelRequested(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persist the automation cancellation request");

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(SET_CANCEL_REQUESTED)) {
				ps.setBoolean(1, true);
				ps.setString(2, runId);
				requireSingleRow(ps.executeUpdate(), "set the cancellation flag", runId, null);
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			rollback(conn, e);
			classLogger.error("Failed to set cancel-requested flag for run '{}'", runId, e);
			throw new IllegalStateException("Unable to persist the automation cancellation request.", e);
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
	 * Updates the status of an automation run (on completion or failure).
	 */
	public static void updateRunStatus(String runId, String status,
			String failedNodeId, String errorMessage) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persist the terminal automation status");

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
				requireSingleRow(ps.executeUpdate(), "update the terminal run status", runId, null);
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			rollback(conn, e);
			classLogger.error("Failed to update run status for '{}'", runId, e);
			throw new IllegalStateException("Unable to persist the terminal automation status.", e);
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
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_VERSION, DEFINITION_VERSION));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_HASH, DEFINITION_HASH));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_SNAPSHOT, DEFINITION_SNAPSHOT));
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
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_VERSION, DEFINITION_VERSION));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_HASH, DEFINITION_HASH));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_SNAPSHOT, DEFINITION_SNAPSHOT));
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
	 * Marks a node as RUNNING (before pixel execution starts).
	 */
	public static void markNodeRunning(String runId, String nodeId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("mark the automation node as running");

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_STATUS)) {
				int index = 1;
				ps.setString(index++, NODE_STATUS_RUNNING);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				requireSingleRow(ps.executeUpdate(), "mark the node as running", runId, nodeId);
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			rollback(conn, e);
			classLogger.error("Failed to mark node running for run '{}', node '{}'",
					runId, nodeId, e);
			throw new IllegalStateException("Unable to mark the automation node as running.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Marks all nodes that did not start because the run reached a terminal state as skipped.
	 */
	public static void skipPendingNodes(String runId, String reason) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persist skipped automation nodes");

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(SKIP_PENDING_NODE_OUTPUTS)) {
				ps.setString(1, NODE_STATUS_SKIPPED);
				setNullableString(ps, 2, reason);
				ps.setString(3, runId);
				ps.setString(4, NODE_STATUS_PENDING);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			rollback(conn, e);
			classLogger.error("Failed to skip pending nodes for run '{}'", runId, e);
			throw new IllegalStateException("Unable to persist skipped automation nodes.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates a node output after successful execution.
	 */
	public static void updateNodeSuccess(String runId, String nodeId, Timestamp startedAt,
			long durationMs, String outputVar, String outputValue, String outputPreview,
			String modelMessageId, String agentRunId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persist the successful automation node result");

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
				queryUtil.handleInsertionOfClob(conn, ps, outputValue, index++, AutomationRuntimeUtils.GSON);
				ps.setString(index++, outputPreview);
				setNullableString(ps, index++, modelMessageId);
				setNullableString(ps, index++, agentRunId);
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				requireSingleRow(ps.executeUpdate(), "persist the successful node result", runId, nodeId);
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			rollback(conn, e);
			classLogger.error("Failed to update node success for run '{}', node '{}'",
					runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the successful automation node result.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates a node output after failed execution.
	 */
	public static void updateNodeFailed(String runId, String nodeId, Timestamp startedAt,
			long durationMs, String errorMessage) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persist the failed automation node result");

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
				requireSingleRow(ps.executeUpdate(), "persist the failed node result", runId, nodeId);
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			rollback(conn, e);
			classLogger.error("Failed to update node failed for run '{}', node '{}'",
					runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the failed automation node result.", e);
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
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + ROOM_ID, ROOM_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + MODEL_MESSAGE_ID, MODEL_MESSAGE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + AGENT_RUN_ID, AGENT_RUN_ID));
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
	 * (falls back from outputValue when blank), outputValue, errorMessage, and an optional trace map.
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
			Map<String, Object> trace = new LinkedHashMap<>();
			putIfPresent(trace, AutomationConstants.TRACE_ROOM_ID,
					output.get(AutomationConstants.ROOM_ID));
			putIfPresent(trace, AutomationConstants.TRACE_MODEL_MESSAGE_ID,
					output.get(AutomationConstants.MODEL_MESSAGE_ID));
			putIfPresent(trace, AutomationConstants.TRACE_AGENT_RUN_ID,
					output.get(AutomationConstants.AGENT_RUN_ID));
			if (!trace.isEmpty()) {
				nodeResult.put(AutomationConstants.RESULT_TRACE, trace);
			}
			nodeResults.add(nodeResult);
		}
		return nodeResults;
	}

	// -- Table Creation ------------------------------------------------------------

	private static void createAutomationRunsTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType, String clobType) throws SQLException {

		String tableName = TABLE_AUTOMATION_RUNS;

		boolean tableExists = !allowIfExists && queryUtil.tableExists(conn, tableName, database, schema);
		if (!tableExists) {
			String[] colNames = { RUN_ID, PROJECT_ID, AUTOMATION_ID, DEFINITION_VERSION, DEFINITION_HASH,
					DEFINITION_SNAPSHOT, STATUS, TRIGGER_TYPE, STARTED_AT, COMPLETED_AT, FAILED_NODE_ID,
					ERROR_MESSAGE, LAST_HEARTBEAT, TOTAL_NODES, COMPLETED_NODES, CREATED_BY,
					CANCEL_REQUESTED, RESULT_SUMMARY_COL };
			String[] types = { VARCHAR_255, VARCHAR_255, VARCHAR_255, INTEGER, VARCHAR_255,
					clobType, VARCHAR_50, VARCHAR_50, dateTimeType, dateTimeType, VARCHAR_255,
					clobType, dateTimeType, INTEGER, INTEGER, VARCHAR_255,
					queryUtil.getBooleanDataTypeName(), VARCHAR_2000 };
			String[] constraints = { NOT_NULL, NOT_NULL, null, null, null,
					null, NOT_NULL, NOT_NULL, NOT_NULL, null, null,
					null, null, null, null, null,
					null, null };

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
		}

		// Additive migration for existing installations.
		addColumnIfNotExists(conn, queryUtil, tableName, CANCEL_REQUESTED, queryUtil.getBooleanDataTypeName());
		addColumnIfNotExists(conn, queryUtil, tableName, RESULT_SUMMARY_COL, VARCHAR_2000);
		addColumnIfNotExists(conn, queryUtil, tableName, DEFINITION_VERSION, INTEGER);
		addColumnIfNotExists(conn, queryUtil, tableName, DEFINITION_HASH, VARCHAR_255);
		addColumnIfNotExists(conn, queryUtil, tableName, DEFINITION_SNAPSHOT, clobType);

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

		boolean tableExists = !allowIfExists && queryUtil.tableExists(conn, tableName, database, schema);
		if (!tableExists) {
			String[] colNames = { RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS,
					STARTED_AT, COMPLETED_AT, DURATION_MS, OUTPUT_VAR,
					OUTPUT_VALUE, OUTPUT_PREVIEW, ROOM_ID, MODEL_MESSAGE_ID, AGENT_RUN_ID, ERROR_MESSAGE };
			String[] types = { VARCHAR_255, VARCHAR_255, VARCHAR_500, INTEGER, VARCHAR_50,
					dateTimeType, dateTimeType, BIGINT, VARCHAR_255,
					clobType, VARCHAR_2000, VARCHAR_50, VARCHAR_50, VARCHAR_50, clobType };
			String[] constraints = { NOT_NULL, NOT_NULL, null, NOT_NULL, NOT_NULL,
					null, null, null, null,
					null, null, null, null, null, null };

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
		}

		// Additive trace migration for existing installations.
		addColumnIfNotExists(conn, queryUtil, tableName, ROOM_ID, VARCHAR_50);
		addColumnIfNotExists(conn, queryUtil, tableName, MODEL_MESSAGE_ID, VARCHAR_50);
		addColumnIfNotExists(conn, queryUtil, tableName, AGENT_RUN_ID, VARCHAR_50);

		// Composite primary key
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, PK_AUTO_NODE_OUT,
				new String[]{ RUN_ID, NODE_ID });

		// Indexes
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_RUN, tableName,
				new String[]{ RUN_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_ROOM, tableName,
				new String[]{ ROOM_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_MODEL_MSG, tableName,
				new String[]{ MODEL_MESSAGE_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_AGENT_RUN, tableName,
				new String[]{ AGENT_RUN_ID });
	}

	private static void putIfPresent(Map<String, Object> target, String key, Object value) {
		if (value != null && !value.toString().isBlank()) {
			target.put(key, value);
		}
	}

	/**
	 * Creates the AUTOMATION_ACTIVE_RUN marker table - a single row per project, keyed on
	 * PROJECT_ID, used to atomically enforce "at most one active run per project" cluster-wide.
	 * See {@link #claimAndInitializeRun} / {@link #releaseActiveRun(String, String)}.
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

		// Primary key on PROJECT_ID alone (not RUN_ID) makes the run claim atomic:
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

	private static IRDBMSEngine requireSchedulerDb(String operation) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			throw new IllegalStateException("Scheduler DB is not available; unable to " + operation + ".");
		}
		return schedulerDb;
	}

	private static void closeConnection(IRDBMSEngine engine, Connection conn) {
		ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
	}

	private static void rollback(Connection conn, Exception cause) {
		if (conn == null) {
			return;
		}
		try {
			if (!conn.getAutoCommit()) {
				conn.rollback();
			}
		} catch (SQLException rollbackError) {
			cause.addSuppressed(rollbackError);
			classLogger.error("Failed to roll back automation database transaction", rollbackError);
		}
	}

	private static boolean isConstraintViolation(SQLException exception) {
		for (SQLException current = exception; current != null; current = current.getNextException()) {
			if (current instanceof SQLIntegrityConstraintViolationException) {
				return true;
			}
			String sqlState = current.getSQLState();
			if (sqlState != null && sqlState.startsWith("23")) {
				return true;
			}
		}
		return false;
	}

	private static void requireSingleRow(int updatedRows, String operation, String runId, String nodeId) {
		if (updatedRows == 1) {
			return;
		}
		String nodeContext = nodeId == null ? "" : ", node '" + nodeId + "'";
		throw new IllegalStateException("Unable to " + operation + " for run '" + runId + "'"
				+ nodeContext + ": expected one row but updated " + updatedRows + ".");
	}

	private static void restoreAutoCommit(Connection conn, boolean originalAutoCommit) {
		if (conn == null || !originalAutoCommit) {
			return;
		}
		try {
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			classLogger.warn("Failed to restore scheduler database autocommit before closing connection", e);
		}
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
