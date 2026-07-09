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
package prerna.reactor.workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Database utility for the Workflow Engine subsystem.
 * Manages WORKFLOW_RUNS, WORKFLOW_NODE_OUTPUTS, and WORKFLOW_FOREACH_ROWS tables
 * in the scheduler database.
 *
 * Follows the same patterns as {@link prerna.reactor.scheduler.SchedulerDatabaseUtility}.
 * Called at platform startup to create tables; provides CRUD for workflow execution state.
 */
public final class WorkflowDatabaseUtility {

	private static final Logger classLogger = LogManager.getLogger(WorkflowDatabaseUtility.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	// Table name shortcuts for SelectQueryStruct (TABLE__COLUMN format)
	private static final String TABLE_RUNS = WorkflowConstants.TABLE_WORKFLOW_RUNS;
	private static final String TABLE_NODE_OUTPUTS = WorkflowConstants.TABLE_WORKFLOW_NODE_OUTPUTS;
	private static final String TABLE_FOREACH = WorkflowConstants.TABLE_WORKFLOW_FOREACH_ROWS;

	private WorkflowDatabaseUtility() {
		// static utility — no instantiation
	}

	// ── SQL Statements (INSERT/UPDATE/DELETE — PreparedStatement per SEMOSS conventions) ──

	// WORKFLOW_RUNS
	private static final String INSERT_RUN = """
			INSERT INTO WORKFLOW_RUNS \
			(RUN_ID, PROJECT_ID, WORKFLOW_ID, STATUS, TRIGGER_TYPE, RESUMED_FROM_RUN, \
			STARTED_AT, LAST_HEARTBEAT, TOTAL_NODES, COMPLETED_NODES, CREATED_BY, \
			PARENT_RUN_ID, PARENT_NODE_ID) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""";

	private static final String UPDATE_RUN_STATUS = """
			UPDATE WORKFLOW_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			FAILED_NODE_ID = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ?""";

	private static final String UPDATE_HEARTBEAT =
			"UPDATE WORKFLOW_RUNS SET LAST_HEARTBEAT = ?, COMPLETED_NODES = ? WHERE RUN_ID = ?";

	private static final String TOUCH_HEARTBEAT =
			"UPDATE WORKFLOW_RUNS SET LAST_HEARTBEAT = ? WHERE RUN_ID = ?";

	private static final String MARK_STALE_INTERRUPTED = """
			UPDATE WORKFLOW_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			ERROR_MESSAGE = ? WHERE RUN_ID = ?""";

	// WORKFLOW_NODE_OUTPUTS
	private static final String INSERT_NODE_OUTPUT = """
			INSERT INTO WORKFLOW_NODE_OUTPUTS \
			(RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS) \
			VALUES (?, ?, ?, ?, ?)""";

	private static final String UPDATE_NODE_OUTPUT_SUCCESS = """
			UPDATE WORKFLOW_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, OUTPUT_VAR = ?, OUTPUT_VALUE = ?, OUTPUT_PREVIEW = ?, ROW_COUNT = ? \
			WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_OUTPUT_FAILED = """
			UPDATE WORKFLOW_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_STATUS =
			"UPDATE WORKFLOW_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ? WHERE RUN_ID = ? AND NODE_ID = ?";

	// WORKFLOW_FOREACH_ROWS
	private static final String INSERT_FOREACH_ROW = """
			INSERT INTO WORKFLOW_FOREACH_ROWS \
			(RUN_ID, NODE_ID, ROW_INDEX, ROW_KEY, STATUS, STARTED_AT, COMPLETED_AT, DURATION_MS, ERROR_MESSAGE) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""";

	// Aggregate queries (use PreparedStatement — CASE WHEN not easily expressed in SelectQueryStruct)
	private static final String SELECT_FOREACH_PROGRESS = """
			SELECT COUNT(*) AS TOTAL, \
			SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCEEDED, \
			SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED \
			FROM WORKFLOW_FOREACH_ROWS WHERE RUN_ID = ? AND NODE_ID = ?""";

	// ── Initialization ────────────────────────────────────────────────────────────

	/**
	 * Creates workflow tables in the scheduler DB if they don't exist.
	 * Called at platform startup after the scheduler DB is loaded.
	 * Safe to call on every startup (uses IF NOT EXISTS / metadata checks).
	 */
	public static void initialize() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			classLogger.warn("Scheduler DB not available — workflow tables will not be created");
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

			createWorkflowRunsTable(conn, queryUtil, database, schema, allowIfExists, dateTimeType, clobType);
			createWorkflowNodeOutputsTable(conn, queryUtil, database, schema, allowIfExists, dateTimeType, clobType);
			createWorkflowForEachRowsTable(conn, queryUtil, database, schema, allowIfExists, dateTimeType, clobType);

			if (!conn.getAutoCommit()) {
				conn.commit();
			}

			classLogger.info("Workflow engine tables initialized successfully");
		} catch (Exception e) {
			classLogger.error("Failed to initialize workflow engine tables: {}", e.getMessage(), e);
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

		// Use SelectQueryStruct to find stale runs
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__RUN_ID", "RUN_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__STATUS", "==", WorkflowConstants.STATUS_RUNNING, PixelDataType.CONST_STRING));

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results == null || results.isEmpty()) {
			return;
		}

		// For each running run, check if heartbeat is stale and mark as interrupted
		Timestamp threshold = toTimestamp(Instant.now().minusSeconds(
				WorkflowConstants.STALE_HEARTBEAT_THRESHOLD_MINUTES * 60L));
		Timestamp now = toTimestamp(Instant.now());

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			for (Map<String, Object> row : results) {
				String runId = (String) row.get("RUN_ID");
				try (PreparedStatement ps = conn.prepareStatement(MARK_STALE_INTERRUPTED)) {
					int index = 1;
					ps.setString(index++, WorkflowConstants.STATUS_INTERRUPTED);
					ps.setTimestamp(index++, now);
					ps.setString(index++, "Server restarted during execution");
					ps.setString(index++, runId);
					int updated = ps.executeUpdate();
					if (updated > 0) {
						classLogger.info("Marked stale workflow run {} as INTERRUPTED", runId);
					}
				}
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to mark stale workflow runs: {}", e.getMessage(), e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	// ── WORKFLOW_RUNS CRUD ────────────────────────────────────────────────────────

	/**
	 * Checks if a workflow already has an active (RUNNING) run for the given project.
	 *
	 * @return the active run ID, or null if no run is active
	 */
	public static String getActiveRun(String projectId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__RUN_ID", "RUN_ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__PROJECT_ID", "==", projectId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__STATUS", "==", WorkflowConstants.STATUS_RUNNING, PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			Object runId = results.get(0).get("RUN_ID");
			return runId != null ? runId.toString() : null;
		}
		return null;
	}

	/**
	 * Inserts a new workflow run record.
	 */
	public static boolean insertRun(String runId, String projectId, String workflowId,
			String triggerType, String resumedFromRun, int totalNodes, String createdBy) {
		return insertRun(runId, projectId, workflowId, triggerType, resumedFromRun,
				totalNodes, createdBy, null, null);
	}

	/**
	 * Inserts a new workflow run record, optionally linked to a parent run/node — used when
	 * a sub-workflow node triggers another project's workflow. {@code parentRunId} and
	 * {@code parentNodeId} are null for top-level (manual/scheduled/resume) runs.
	 */
	public static boolean insertRun(String runId, String projectId, String workflowId,
			String triggerType, String resumedFromRun, int totalNodes, String createdBy,
			String parentRunId, String parentNodeId) {
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
				ps.setString(index++, workflowId);
				ps.setString(index++, WorkflowConstants.STATUS_RUNNING);
				ps.setString(index++, triggerType);
				ps.setString(index++, resumedFromRun);
				ps.setTimestamp(index++, now);
				ps.setTimestamp(index++, now);
				ps.setInt(index++, totalNodes);
				ps.setString(index++, createdBy);
				ps.setString(index++, parentRunId);
				ps.setString(index++, parentNodeId);
				ps.executeUpdate();
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to insert workflow run '{}': {}", runId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates the status of a workflow run (on completion or failure).
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
				ps.setString(index++, failedNodeId);
				ps.setString(index++, errorMessage);
				ps.setString(index++, runId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to update run status for '{}': {}", runId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates the heartbeat timestamp and completed node count for a running workflow.
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
			classLogger.error("Failed to update heartbeat for run '{}': {}", runId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates only the heartbeat timestamp for a running workflow.
	 * Used during long-running for-each batches where completed node count hasn't changed.
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
			classLogger.error("Failed to touch heartbeat for run '{}': {}", runId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Lists workflow runs for a project, newest first.
	 *
	 * @param projectId the project to query
	 * @param limit     max number of runs to return
	 * @return list of run summary maps
	 */
	public static List<Map<String, Object>> getRunsForProject(String projectId, int limit) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__RUN_ID", "RUN_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__PROJECT_ID", "PROJECT_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__WORKFLOW_ID", "WORKFLOW_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__STATUS", "STATUS"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__TRIGGER_TYPE", "TRIGGER_TYPE"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__RESUMED_FROM_RUN", "RESUMED_FROM_RUN"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__STARTED_AT", "STARTED_AT"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__COMPLETED_AT", "COMPLETED_AT"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__FAILED_NODE_ID", "FAILED_NODE_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__TOTAL_NODES", "TOTAL_NODES"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__COMPLETED_NODES", "COMPLETED_NODES"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__CREATED_BY", "CREATED_BY"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__PROJECT_ID", "==", projectId, PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_RUNS + "__STARTED_AT",
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
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__RUN_ID", "RUN_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__PROJECT_ID", "PROJECT_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__WORKFLOW_ID", "WORKFLOW_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__STATUS", "STATUS"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__TRIGGER_TYPE", "TRIGGER_TYPE"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__RESUMED_FROM_RUN", "RESUMED_FROM_RUN"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__STARTED_AT", "STARTED_AT"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__COMPLETED_AT", "COMPLETED_AT"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__FAILED_NODE_ID", "FAILED_NODE_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__ERROR_MESSAGE", "ERROR_MESSAGE"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__TOTAL_NODES", "TOTAL_NODES"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__COMPLETED_NODES", "COMPLETED_NODES"));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__CREATED_BY", "CREATED_BY"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RUNS + "__RUN_ID", "==", runId, PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			return results.get(0);
		}
		return null;
	}

	// ── WORKFLOW_NODE_OUTPUTS CRUD ────────────────────────────────────────────────

	/**
	 * Inserts a node output record with PENDING status (before execution).
	 */
	public static boolean insertNodeOutput(String runId, String nodeId, String nodeLabel, int executionOrder) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(INSERT_NODE_OUTPUT)) {
				int index = 1;
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				ps.setString(index++, nodeLabel);
				ps.setInt(index++, executionOrder);
				ps.setString(index++, WorkflowConstants.NODE_STATUS_PENDING);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to insert node output for run '{}', node '{}': {}",
					runId, nodeId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

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
					ps.setString(index++, (String) node.get("id"));
					ps.setString(index++, (String) node.get("label"));
					ps.setInt(index++, i);
					ps.setString(index++, WorkflowConstants.NODE_STATUS_PENDING);
					ps.addBatch();
				}
				ps.executeBatch();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to batch-insert node outputs for run '{}': {}", runId, e.getMessage(), e);
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
				ps.setString(index++, WorkflowConstants.NODE_STATUS_RUNNING);
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
			classLogger.error("Failed to mark node running for run '{}', node '{}': {}",
					runId, nodeId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Updates a node output after successful execution.
	 */
	public static boolean updateNodeSuccess(String runId, String nodeId, Timestamp startedAt,
			long durationMs, String outputVar, String outputValue, String outputPreview, Integer rowCount) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();

			try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_SUCCESS)) {
				int index = 1;
				ps.setString(index++, WorkflowConstants.NODE_STATUS_SUCCESS);
				ps.setTimestamp(index++, startedAt);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setLong(index++, durationMs);
				ps.setString(index++, outputVar);
				// Handle CLOB for potentially large output values
				queryUtil.handleInsertionOfClob(conn, ps, outputValue, index++, GSON);
				ps.setString(index++, outputPreview);
				if (rowCount != null) {
					ps.setInt(index++, rowCount);
				} else {
					ps.setNull(index++, java.sql.Types.INTEGER);
				}
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to update node success for run '{}', node '{}': {}",
					runId, nodeId, e.getMessage(), e);
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
				ps.setString(index++, WorkflowConstants.NODE_STATUS_FAILED);
				ps.setTimestamp(index++, startedAt);
				ps.setTimestamp(index++, toTimestamp(Instant.now()));
				ps.setLong(index++, durationMs);
				ps.setString(index++, errorMessage);
				ps.setString(index++, runId);
				ps.setString(index++, nodeId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to update node failed for run '{}', node '{}': {}",
					runId, nodeId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Gets all node outputs for a run (for scope reconstruction during resume).
	 *
	 * @return list of node output maps ordered by execution order
	 */
	public static List<Map<String, Object>> getNodeOutputsForRun(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__NODE_ID", "NODE_ID"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__NODE_LABEL", "NODE_LABEL"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__EXECUTION_ORDER", "EXECUTION_ORDER"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__STATUS", "STATUS"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__STARTED_AT", "STARTED_AT"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__COMPLETED_AT", "COMPLETED_AT"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__DURATION_MS", "DURATION_MS"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__OUTPUT_VAR", "OUTPUT_VAR"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__OUTPUT_VALUE", "OUTPUT_VALUE"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__OUTPUT_PREVIEW", "OUTPUT_PREVIEW"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__ROW_COUNT", "ROW_COUNT"));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__ERROR_MESSAGE", "ERROR_MESSAGE"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_NODE_OUTPUTS + "__RUN_ID", "==", runId, PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_NODE_OUTPUTS + "__EXECUTION_ORDER",
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return results != null ? results : new ArrayList<>();
	}

	// ── WORKFLOW_FOREACH_ROWS CRUD ────────────────────────────────────────────────

	/**
	 * Batch-inserts for-each row results. Called with batches of
	 * {@link WorkflowConstants#FOREACH_BATCH_SIZE} rows during for-each execution.
	 */
	public static boolean insertForEachRowsBatch(String runId, String nodeId,
			List<ForEachRowResult> rows) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return false;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(INSERT_FOREACH_ROW)) {
				for (ForEachRowResult row : rows) {
					int index = 1;
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					ps.setInt(index++, row.rowIndex());
					ps.setString(index++, row.rowKey());
					ps.setString(index++, row.status());
					ps.setTimestamp(index++, row.startedAt());
					ps.setTimestamp(index++, toTimestamp(Instant.now()));
					ps.setLong(index++, row.durationMs());
					ps.setString(index++, row.errorMessage());
					ps.addBatch();
				}
				ps.executeBatch();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
			return true;
		} catch (SQLException e) {
			classLogger.error("Failed to batch-insert for-each rows for run '{}', node '{}': {}",
					runId, nodeId, e.getMessage(), e);
			return false;
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Gets aggregate progress for a for-each node.
	 * Uses PreparedStatement directly because this query involves conditional
	 * aggregates (SUM with CASE WHEN) not easily expressed via SelectQueryStruct.
	 *
	 * @return map with keys "total", "succeeded", "failed"
	 */
	public static Map<String, Integer> getForEachProgress(String runId, String nodeId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		Map<String, Integer> progress = new HashMap<>();
		if (schedulerDb == null) return progress;

		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(SELECT_FOREACH_PROGRESS)) {
				ps.setString(1, runId);
				ps.setString(2, nodeId);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						progress.put("total", rs.getInt(1));
						progress.put("succeeded", rs.getInt(2));
						progress.put("failed", rs.getInt(3));
					}
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to get for-each progress for run '{}', node '{}': {}",
					runId, nodeId, e.getMessage(), e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
		return progress;
	}

	/**
	 * Gets the failed rows for a for-each node (for drill-down).
	 */
	public static List<Map<String, Object>> getForEachFailures(String runId, String nodeId, int limit) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_FOREACH + "__ROW_INDEX", "ROW_INDEX"));
		qs.addSelector(new QueryColumnSelector(TABLE_FOREACH + "__ROW_KEY", "ROW_KEY"));
		qs.addSelector(new QueryColumnSelector(TABLE_FOREACH + "__ERROR_MESSAGE", "ERROR_MESSAGE"));
		qs.addSelector(new QueryColumnSelector(TABLE_FOREACH + "__COMPLETED_AT", "COMPLETED_AT"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_FOREACH + "__RUN_ID", "==", runId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_FOREACH + "__NODE_ID", "==", nodeId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_FOREACH + "__STATUS", "==", WorkflowConstants.NODE_STATUS_FAILED, PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_FOREACH + "__ROW_INDEX",
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());
		qs.setLimit(limit);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return results != null ? results : new ArrayList<>();
	}

	/**
	 * Gets the max row index already processed for a for-each node (for resume).
	 *
	 * @return the max row index, or -1 if no rows have been processed
	 */
	public static int getForEachLastProcessedIndex(String runId, String nodeId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) return -1;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(
				QueryFunctionHelper.MAX, TABLE_FOREACH + "__ROW_INDEX", "MAX_INDEX"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_FOREACH + "__RUN_ID", "==", runId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_FOREACH + "__NODE_ID", "==", nodeId, PixelDataType.CONST_STRING));

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			Object maxVal = results.get(0).get("MAX_INDEX");
			if (maxVal instanceof Number) {
				return ((Number) maxVal).intValue();
			}
		}
		return -1;
	}

	// ── Table Creation ────────────────────────────────────────────────────────────

	private static void createWorkflowRunsTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType, String clobType) throws SQLException {

		String tableName = WorkflowConstants.TABLE_WORKFLOW_RUNS;

		if (!allowIfExists && queryUtil.tableExists(conn, tableName, database, schema)) {
			return;
		}

		String[] colNames = { "RUN_ID", "PROJECT_ID", "WORKFLOW_ID", "STATUS", "TRIGGER_TYPE",
				"RESUMED_FROM_RUN", "STARTED_AT", "COMPLETED_AT", "FAILED_NODE_ID",
				"ERROR_MESSAGE", "LAST_HEARTBEAT", "TOTAL_NODES", "COMPLETED_NODES", "CREATED_BY",
				"PARENT_RUN_ID", "PARENT_NODE_ID" };
		String[] types = { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(50)", "VARCHAR(50)",
				"VARCHAR(255)", dateTimeType, dateTimeType, "VARCHAR(255)",
				clobType, dateTimeType, "INTEGER", "INTEGER", "VARCHAR(255)",
				"VARCHAR(255)", "VARCHAR(255)" };
		String[] constraints = { "NOT NULL", "NOT NULL", null, "NOT NULL", "NOT NULL",
				null, "NOT NULL", null, null,
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

		// Migrate installs whose WORKFLOW_RUNS predates sub-workflow support
		addColumnIfNotExists(conn, queryUtil, tableName, "PARENT_RUN_ID", "VARCHAR(255)");
		addColumnIfNotExists(conn, queryUtil, tableName, "PARENT_NODE_ID", "VARCHAR(255)");

		// Primary key
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, "PK_WORKFLOW_RUNS", new String[]{"RUN_ID"});

		// Indexes
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WR_PROJECT", tableName, new String[]{"PROJECT_ID"});
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WR_STATUS", tableName, new String[]{"PROJECT_ID", "STATUS"});
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WR_STARTED", tableName, new String[]{"PROJECT_ID", "STARTED_AT"});
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WR_PARENT", tableName, new String[]{"PARENT_RUN_ID"});
	}

	private static void createWorkflowNodeOutputsTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType, String clobType) throws SQLException {

		String tableName = WorkflowConstants.TABLE_WORKFLOW_NODE_OUTPUTS;

		if (!allowIfExists && queryUtil.tableExists(conn, tableName, database, schema)) {
			return;
		}

		String[] colNames = { "RUN_ID", "NODE_ID", "NODE_LABEL", "EXECUTION_ORDER", "STATUS",
				"STARTED_AT", "COMPLETED_AT", "DURATION_MS", "OUTPUT_VAR",
				"OUTPUT_VALUE", "OUTPUT_PREVIEW", "ROW_COUNT", "ERROR_MESSAGE" };
		String[] types = { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(500)", "INTEGER", "VARCHAR(50)",
				dateTimeType, dateTimeType, "BIGINT", "VARCHAR(255)",
				clobType, "VARCHAR(2000)", "INTEGER", clobType };
		String[] constraints = { "NOT NULL", "NOT NULL", null, "NOT NULL", "NOT NULL",
				null, null, null, null,
				null, null, null, null };

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
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, "PK_WF_NODE_OUT", new String[]{"RUN_ID", "NODE_ID"});

		// Indexes
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WNO_RUN", tableName, new String[]{"RUN_ID"});
	}

	private static void createWorkflowForEachRowsTable(Connection conn, AbstractSqlQueryUtil queryUtil,
			String database, String schema, boolean allowIfExists, String dateTimeType, String clobType) throws SQLException {

		String tableName = WorkflowConstants.TABLE_WORKFLOW_FOREACH_ROWS;

		if (!allowIfExists && queryUtil.tableExists(conn, tableName, database, schema)) {
			return;
		}

		String[] colNames = { "RUN_ID", "NODE_ID", "ROW_INDEX", "ROW_KEY", "STATUS",
				"STARTED_AT", "COMPLETED_AT", "DURATION_MS", "ERROR_MESSAGE" };
		String[] types = { "VARCHAR(255)", "VARCHAR(255)", "INTEGER", "VARCHAR(1000)", "VARCHAR(50)",
				dateTimeType, dateTimeType, "BIGINT", clobType };
		String[] constraints = { "NOT NULL", "NOT NULL", "NOT NULL", null, "NOT NULL",
				null, null, null, null };

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
		addPrimaryKeyIfNotExists(conn, queryUtil, tableName, database, schema, "PK_WF_FE_ROWS", new String[]{"RUN_ID", "NODE_ID", "ROW_INDEX"});

		// Indexes
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WFR_RUN_NODE", tableName, new String[]{"RUN_ID", "NODE_ID"});
		createIndexIfNotExists(conn, queryUtil, allowIfExists, "IDX_WFR_STATUS", tableName, new String[]{"RUN_ID", "NODE_ID", "STATUS"});
	}

	// ── Helpers ───────────────────────────────────────────────────────────────────

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

	private static Timestamp toTimestamp(Instant instant) {
		return Utility.getSqlTimestampUTC(
				LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
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
	 * Adds a column to an existing table if it isn't already present — used to migrate
	 * WORKFLOW_RUNS for installs that created the table before PARENT_RUN_ID/PARENT_NODE_ID
	 * existed. Safe to call unconditionally on every startup; errors (column already exists)
	 * are swallowed just like {@link #addPrimaryKeyIfNotExists}.
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
			List<String> colList = java.util.Arrays.asList(columns);
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

	// ── Data Transfer Object ──────────────────────────────────────────────────────

	/**
	 * Record for a single for-each row result, used in batch inserts.
	 */
	public record ForEachRowResult(
			int rowIndex,
			String rowKey,
			String status,
			String errorMessage,
			Timestamp startedAt,
			long durationMs
	) {
		public ForEachRowResult(int rowIndex, String rowKey, String status, String errorMessage, long startTimeMs) {
			this(rowIndex, rowKey, status, errorMessage,
					Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(
							Instant.ofEpochMilli(startTimeMs), ZoneOffset.UTC)),
					System.currentTimeMillis() - startTimeMs);
		}
	}
}
