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
package prerna.engine.impl.model.inferencetracking;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Utility for persisting and querying agent execution traces stored in the
 * {@code AGENT_TRACE} table of the ModelInferenceLogs database.
 *
 * <p>All SELECT operations use {@link SelectQueryStruct} to follow SEMOSS query
 * conventions. INSERT operations use {@link PreparedStatement} because
 * {@code SelectQueryStruct} is read-only.
 */
public final class AgentTraceLogsUtils {

	private AgentTraceLogsUtils() {
		// utility class
	}

	private static final Logger classLogger = LogManager.getLogger(AgentTraceLogsUtils.class);

	/** Maps insightId → active traceId for parent trace propagation across threads. */
	private static final ConcurrentHashMap<String, String> ACTIVE_TRACE_MAP = new ConcurrentHashMap<>();

	/** Per-trace step counter for deterministic STEP_NUMBER assignment before parallel dispatch. */
	private static final ConcurrentHashMap<String, AtomicInteger> STEP_COUNTER_MAP = new ConcurrentHashMap<>();

	// -------------------------------------------------------------------------
	// Active trace ID management
	// -------------------------------------------------------------------------

	public static void setActiveTraceId(String insightId, String traceId) {
		if (insightId != null && traceId != null) {
			ACTIVE_TRACE_MAP.put(insightId, traceId);
		}
	}

	public static String getActiveTraceId(String insightId) {
		if (insightId == null) {
			return null;
		}
		return ACTIVE_TRACE_MAP.get(insightId);
	}

	public static void clearActiveTraceId(String insightId) {
		if (insightId != null) {
			ACTIVE_TRACE_MAP.remove(insightId);
		}
	}

	// -------------------------------------------------------------------------
	// Step counter helpers (call before dispatching parallel tool work)
	// -------------------------------------------------------------------------

	/**
	 * Atomically reserves and returns the next step index for the given trace.
	 * Use this on the single-tool fast path.
	 */
	public static int nextStepIndex(String traceId) {
		return STEP_COUNTER_MAP.computeIfAbsent(traceId, k -> new AtomicInteger(0)).getAndIncrement();
	}

	/**
	 * Atomically reserves {@code n} consecutive step indices and returns the base index.
	 * Use this before dispatching a parallel tool batch: {@code baseStep + i} gives each
	 * tool its deterministic step number regardless of finish order.
	 */
	public static int reserveStepIndices(String traceId, int n) {
		return STEP_COUNTER_MAP.computeIfAbsent(traceId, k -> new AtomicInteger(0)).getAndAdd(n);
	}

	/** Clears the step counter for a trace. Call in the same finally block as clearActiveTraceId. */
	public static void clearStepCounter(String traceId) {
		if (traceId != null) {
			STEP_COUNTER_MAP.remove(traceId);
		}
	}

	// -------------------------------------------------------------------------
	// Write operations
	// -------------------------------------------------------------------------

	/**
	 * Inserts a completed agent trace into the AGENT_TRACE table.
	 * Handles null parentTraceId gracefully (stored as SQL NULL).
	 * If the database is unavailable, logs a warning and returns without throwing.
	 */
	public static void logTrace(
			String traceId,
			String roomId,
			String userId,
			String modelEngineId,
			String harnessType,
			Instant startTime,
			Instant endTime,
			int iterations,
			int toolCallCount,
			String terminationReason,
			String metricsJson,
			String parentTraceId) {
		logTrace(traceId, roomId, userId, null, modelEngineId, harnessType,
				startTime, endTime, iterations, toolCallCount, terminationReason, metricsJson, parentTraceId);
	}

	/**
	 * Inserts a completed agent trace into the AGENT_TRACE table, including the app PROJECT_ID.
	 * Delegates null-projectId calls to the base overload for backward compatibility.
	 */
	public static void logTrace(
			String traceId,
			String roomId,
			String userId,
			String projectId,
			String modelEngineId,
			String harnessType,
			Instant startTime,
			Instant endTime,
			int iterations,
			int toolCallCount,
			String terminationReason,
			String metricsJson,
			String parentTraceId) {

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (modelInferenceLogsDb == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; skipping agent trace log for traceId '{}'.", traceId);
			return;
		}

		String query = "INSERT INTO AGENT_TRACE "
				+ "(TRACE_ID, ROOM_ID, USER_ID, PROJECT_ID, MODEL_ENGINE_ID, HARNESS_TYPE, "
				+ "START_TIME, END_TIME, ITERATIONS, TOOL_CALL_COUNT, "
				+ "TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, traceId);
			ps.setString(index++, roomId);
			ps.setString(index++, userId);
			if (projectId != null) {
				ps.setString(index++, projectId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.setString(index++, modelEngineId);
			ps.setString(index++, harnessType);
			ps.setTimestamp(index++, startTime != null ? Timestamp.from(startTime) : null);
			ps.setTimestamp(index++, endTime != null ? Timestamp.from(endTime) : null);
			ps.setInt(index++, iterations);
			ps.setInt(index++, toolCallCount);
			ps.setString(index++, terminationReason);
			modelInferenceLogsDb.getQueryUtil().handleInsertionOfClob(ps, metricsJson, index++, null);
			if (parentTraceId != null) {
				ps.setString(index++, parentTraceId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert agent trace for traceId '{}'.", traceId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Updates the END_TIME and TOOL_CALL_COUNT of an existing trace.
	 * Called when a multi-tool chain completes (AddPlaygroundToolExecution final response).
	 */
	public static void updateTraceEndTime(String traceId, Instant endTime, int additionalToolCalls) {
		if (traceId == null) return;
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) return;

		String sql = "UPDATE AGENT_TRACE SET END_TIME = ?, TOOL_CALL_COUNT = TOOL_CALL_COUNT + ? WHERE TRACE_ID = ?";
		PreparedStatement ps = null;
		try {
			ps = db.getPreparedStatement(sql);
			ps.setTimestamp(1, Timestamp.from(endTime));
			ps.setInt(2, additionalToolCalls);
			ps.setString(3, traceId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.warn("updateTraceEndTime: failed for traceId '{}'.", traceId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	/**
	 * Returns the short (7-char) HEAD commit hash for a project or engine's git repo.
	 * Uses EngineUtility to resolve the version folder, then JGit to read HEAD.
	 *
	 * <p>No caching for now — resolve("HEAD") is a single file read and is fast enough.
	 * If profiling shows this is a bottleneck, consider adding a ConcurrentHashMap cache
	 * keyed by engineId with invalidation on deploy/commit.
	 *
	 * <p>Security note: this is only called after the tool has already been executed
	 * within an authorized room context — the engineId was resolved by MCPUtility
	 * within the user's session. No additional access check is needed here.
	 */
	/**
	 * Resolves the git HEAD commit hash for an engine by its ID and catalog type.
	 */
	private static String getEngineCommitHash(String engineId) {
		try {
			IEngine.CATALOG_TYPE catType = SecurityEngineUtils.getEngineType(engineId);
			String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
			String versionFolder = EngineUtility.getSpecificEngineVersionFolder(catType, engineId, engineName);
			return readGitHead(versionFolder);
		} catch (Exception e) {
			classLogger.debug("getEngineCommitHash: failed for '{}': {}", engineId, e.getMessage(), e);
			return null;
		}
	}

	/**
	 * Resolves the git HEAD commit hash for a project by its ID.
	 */
	private static String getProjectCommitHash(String projectId) {
		try {
			String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
			if (projectName == null) return null;
			String versionFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
			return readGitHead(versionFolder);
		} catch (Exception e) {
			classLogger.debug("getProjectCommitHash: failed for '{}': {}", projectId, e.getMessage(), e);
			return null;
		}
	}

	/**
	 * Reads the full 40-char git HEAD hash from a version folder, or null if
	 * no .git directory exists or no commits have been made.
	 */
	private static String readGitHead(String versionFolder) {
		if (versionFolder == null) return null;
		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) return null;
		try (Git git = Git.open(new File(versionFolder))) {
			ObjectId head = git.getRepository().resolve("HEAD");
			return head != null ? head.getName() : null;
		} catch (Exception e) {
			classLogger.debug("readGitHead: could not resolve HEAD in '{}': {}", versionFolder, e.getMessage(), e);
			return null;
		}
	}

	/**
	 * Resolves the TOOL_GIT_COMMIT value for a tool step based on its source classification.
	 *
	 * @param engineId   resolved engine UUID (null for internal/SDK tools)
	 * @param engineType CATALOG_TYPE name (null for internal/SDK tools)
	 * @param isMcp      whether this is an MCP tool
	 * @param harnessType harness type string (e.g., "claude_code", "github_copilot"), or null
	 * @return commit hash, "INTERNAL", "SDK:{harness}", or null
	 */
	public static String resolveToolGitCommit(String engineId, String engineType, boolean isMcp, String harnessType) {
		// SDK tools — only AppBuildingHarness subclasses pass a non-null harnessType
		if (harnessType != null) {
			return "SDK:" + harnessType;
		}
		// Engine or project tools — use the type to pick the right path
		if (engineId != null && engineType != null) {
			IEngine.CATALOG_TYPE catType;
			try {
				catType = IEngine.CATALOG_TYPE.valueOf(engineType);
			} catch (IllegalArgumentException e) {
				return null;
			}
			if (catType == IEngine.CATALOG_TYPE.PROJECT) {
				return getProjectCommitHash(engineId);
			} else {
				return getEngineCommitHash(engineId);
			}
		}
		// Internal tools — no engine, not SDK
		if (!isMcp && engineId == null) {
			return "INTERNAL";
		}
		return null;
	}

	/**
	 * Records a single tool-call step for the given trace into the AGENT_TRACE_STEP table.
	 *
	 * <p>Call {@link #nextStepIndex(String)} or {@link #reserveStepIndices(String, int)} to
	 * obtain {@code stepNumber} <em>before</em> parallel tool dispatch so that step ordering
	 * reflects model-issued order, not completion order.
	 *
	 * @param traceId      owning trace
	 * @param stepNumber   pre-assigned step index (0-based)
	 * @param toolCallId   LLM-issued tool_call_id from the message
	 * @param rawToolName  full tool name as returned by the model (may include engine-id prefix)
	 * @param engineId     SEMOSS engine UUID, or null for non-MCP tools
	 * @param engineType   {@link prerna.engine.api.IEngine.CATALOG_TYPE} name, or null
	 * @param isMcp        true when the call was routed through InternalMCP/RemoteMCP
	 * @param startMs      wall-clock ms at tool execution start
	 * @param endMs        wall-clock ms at tool execution end
	 * @param status       "success" or "error" (matches TOOL_STATUS_* constants in harnesses)
	 * @param toolInputJson JSON-serialised input params, stored as TOOL_INPUT_JSON (may be null)
	 * @param outputText   tool result string, stored as OUTPUT_TEXT
	 * @param errorMsg     error detail when status is "error", stored as ERROR_MESSAGE (may be null)
	 * @param toolGitCommit git HEAD hash of the tool's project/engine, "INTERNAL", "SDK:{harness}", or null
	 *
	 * <p>TODO: TOOL_OUTPUT_JSON can be populated here once tool results are returned as structured
	 * objects rather than rendered strings. For now we only store OUTPUT_TEXT.
	 */
	public static void recordTraceStep(
			String traceId,
			int stepNumber,
			String toolCallId,
			String rawToolName,
			String engineId,
			String engineType,
			boolean isMcp,
			long startMs,
			long endMs,
			String status,
			String toolInputJson,
			String outputText,
			String errorMsg,
			String toolGitCommit) {

		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database unavailable; skipping trace step for traceId '{}'.", traceId);
			return;
		}

		String query = "INSERT INTO AGENT_TRACE_STEP "
				+ "(STEP_ID, TRACE_ID, STEP_NUMBER, STEP_TYPE, OUTPUT_TEXT, TOOL_NAME, "
				+ "TOOL_INPUT_JSON, START_TIME, END_TIME, ERROR_MESSAGE, "
				+ "TOOL_CALL_ID, ENGINE_ID, ENGINE_TYPE, IS_MCP, STATUS, TOOL_GIT_COMMIT) "
				+ "VALUES (?, ?, ?, 'TOOL_CALL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement ps = null;
		try {
			ps = db.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, UUID.randomUUID().toString());
			ps.setString(index++, traceId);
			ps.setInt(index++, stepNumber);
			db.getQueryUtil().handleInsertionOfClob(ps, outputText, index++, null);
			ps.setString(index++, rawToolName);
			db.getQueryUtil().handleInsertionOfClob(ps, toolInputJson, index++, null);
			ps.setTimestamp(index++, new Timestamp(startMs));
			ps.setTimestamp(index++, new Timestamp(endMs));
			db.getQueryUtil().handleInsertionOfClob(ps, errorMsg, index++, null);
			if (toolCallId != null) {
				ps.setString(index++, toolCallId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (engineId != null) {
				ps.setString(index++, engineId);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			if (engineType != null) {
				ps.setString(index++, engineType);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.setBoolean(index++, isMcp);
			ps.setString(index++, status);
			if (toolGitCommit != null) {
				ps.setString(index++, toolGitCommit);
			} else {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert trace step for traceId '{}' step {}.", traceId, stepNumber, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, null);
		}
	}

	// -------------------------------------------------------------------------
	// Read operations  (all use SelectQueryStruct per SEMOSS convention)
	// -------------------------------------------------------------------------

	private static final String AGENT_TRACE_TABLE = "AGENT_TRACE__";

	/**
	 * Returns traces filtered by roomId and/or userId, ordered by START_TIME DESC.
	 * Null parameters are omitted from the WHERE clause.
	 *
	 * @param roomId  room filter (nullable)
	 * @param userId  user filter (nullable)
	 * @param limit   max rows; use 0 for no limit
	 * @return list of trace maps, newest first
	 */
	public static List<Map<String, Object>> listTraces(String roomId, String userId, int limit) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty trace list.");
			return new ArrayList<>();
		}
		SelectQueryStruct qs = buildTraceSelector();
		if (roomId != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "ROOM_ID", "==", roomId));
		}
		if (userId != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "USER_ID", "==", userId));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector(AGENT_TRACE_TABLE + "START_TIME", "DESC"));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		try {
			return QueryExecutionUtility.flushRsToMap(db, qs);
		} catch (Exception e) {
			classLogger.error("Failed to list agent traces for roomId '{}', userId '{}'.", roomId, userId, e);
			return new ArrayList<>();
		}
	}

	/**
	 * Returns all traces ordered by START_TIME DESC.
	 *
	 * @param limit max rows; use 0 for no limit
	 * @return list of trace maps, newest first
	 */
	public static List<Map<String, Object>> listAllTraces(int limit) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty trace list.");
			return new ArrayList<>();
		}
		SelectQueryStruct qs = buildTraceSelector();
		qs.addOrderBy(new QueryColumnOrderBySelector(AGENT_TRACE_TABLE + "START_TIME", "DESC"));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		try {
			return QueryExecutionUtility.flushRsToMap(db, qs);
		} catch (Exception e) {
			classLogger.error("Failed to list all agent traces.", e);
			return new ArrayList<>();
		}
	}

	/**
	 * Returns a single trace by TRACE_ID, or null if not found.
	 *
	 * @param traceId trace identifier
	 * @return trace map or {@code null}
	 */
	public static Map<String, Object> getTrace(String traceId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; cannot retrieve traceId '{}'.", traceId);
			return null;
		}
		SelectQueryStruct qs = buildTraceSelector();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "TRACE_ID", "==", traceId));
		try {
			List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(db, qs);
			return rows.isEmpty() ? null : rows.get(0);
		} catch (Exception e) {
			classLogger.error("Failed to retrieve agent trace for traceId '{}'.", traceId, e);
			return null;
		}
	}

	/**
	 * Returns all child traces whose PARENT_TRACE_ID matches, ordered by START_TIME ASC.
	 *
	 * @param parentTraceId parent trace identifier
	 * @return list of child trace maps, oldest first
	 */
	public static List<Map<String, Object>> getChildTraces(String parentTraceId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty child trace list.");
			return new ArrayList<>();
		}
		SelectQueryStruct qs = buildTraceSelector();
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "PARENT_TRACE_ID", "==", parentTraceId));
		qs.addOrderBy(new QueryColumnOrderBySelector(AGENT_TRACE_TABLE + "START_TIME", "ASC"));
		try {
			return QueryExecutionUtility.flushRsToMap(db, qs);
		} catch (Exception e) {
			classLogger.error("Failed to retrieve child traces for parentTraceId '{}'.", parentTraceId, e);
			return new ArrayList<>();
		}
	}

	// -------------------------------------------------------------------------
	// Internal helpers
	// -------------------------------------------------------------------------

	/** Builds a {@link SelectQueryStruct} selecting all AGENT_TRACE columns. */
	private static SelectQueryStruct buildTraceSelector() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "TRACE_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "ROOM_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "USER_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "PROJECT_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "MODEL_ENGINE_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "HARNESS_TYPE"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "START_TIME"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "END_TIME"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "ITERATIONS"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "TOOL_CALL_COUNT"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "TERMINATION_REASON"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "METRICS_JSON"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "PARENT_TRACE_ID"));
		return qs;
	}

	private static final String AGENT_TRACE_STEP_TABLE = "AGENT_TRACE_STEP__";

	/**
	 * Returns all steps for a trace, ordered by STEP_NUMBER ASC.
	 * Verifies ownership by joining to AGENT_TRACE and checking USER_ID == userId.
	 *
	 * @param traceId trace to fetch steps for
	 * @param userId  requesting user — used to verify trace ownership
	 * @return ordered list of step maps, or empty list if not found / not authorized
	 */
	public static List<Map<String, Object>> listTraceSteps(String traceId, String userId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database unavailable; returning empty step list.");
			return new ArrayList<>();
		}

		// Verify ownership: query the single trace and check USER_ID
		SelectQueryStruct ownerQs = new SelectQueryStruct();
		ownerQs.addSelector(new QueryColumnSelector(AGENT_TRACE_TABLE + "USER_ID"));
		ownerQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "TRACE_ID", "==", traceId));
		try {
			List<Map<String, Object>> traceRow = QueryExecutionUtility.flushRsToMap(db, ownerQs);
			if (traceRow.isEmpty()) {
				return Collections.emptyList();
			}
			Object traceUserId = traceRow.get(0).get("AGENT_TRACE__USER_ID");
			if (traceUserId == null) traceUserId = traceRow.get(0).get("USER_ID");
			if (!userId.equals(String.valueOf(traceUserId))) {
				classLogger.warn("User '{}' attempted to list steps for trace '{}' they do not own.", userId, traceId);
				return Collections.emptyList();
			}
		} catch (Exception e) {
			classLogger.error("Failed ownership check for traceId '{}'.", traceId, e);
			return Collections.emptyList();
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "STEP_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "TRACE_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "STEP_NUMBER"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "STEP_TYPE"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "TOOL_NAME"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "OUTPUT_TEXT"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "TOOL_INPUT_JSON"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "START_TIME"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "END_TIME"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "ERROR_MESSAGE"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "TOOL_CALL_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "ENGINE_ID"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "ENGINE_TYPE"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "IS_MCP"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "STATUS"));
		qs.addSelector(new QueryColumnSelector(AGENT_TRACE_STEP_TABLE + "TOOL_GIT_COMMIT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_STEP_TABLE + "TRACE_ID", "==", traceId));
		qs.addOrderBy(new QueryColumnOrderBySelector(AGENT_TRACE_STEP_TABLE + "STEP_NUMBER", "ASC"));
		try {
			return QueryExecutionUtility.flushRsToMap(db, qs);
		} catch (Exception e) {
			classLogger.error("Failed to list trace steps for traceId '{}'.", traceId, e);
			return new ArrayList<>();
		}
	}

	// -------------------------------------------------------------------------
	// Token recovery from MESSAGE table
	// -------------------------------------------------------------------------

	/**
	 * Sums INPUT_TOKENS, OUTPUT_TOKENS, CACHE_READ_TOKENS, CACHE_CREATION_TOKENS
	 * from the MESSAGE table for the given room since a given time.
	 *
	 * <p>Used to recover real token totals after a ClaudeCode/Copilot run.
	 * These harnesses route through the SEMOSS proxy which logs actual Anthropic
	 * token counts to MESSAGE, but returns 0 in the SDK response.
	 *
	 * @param roomId the model-engine room ID; returns all-zeros when null
	 * @param since  inclusive lower-bound; scopes query to this run only
	 * @return int[4] — {inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens}
	 */
	public static int[] sumTokensForRoom(String roomId, Instant since) {
		return sumTokensForRoomBounded(roomId, since, null);
	}

	/**
	 * Sums token usage for the given room bounded by [since, until].
	 * When both bounds are provided, only messages within the time window are counted,
	 * preventing token duplication when multiple traces share the same room.
	 *
	 * @return int[4] — {inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens}
	 */
	public static int[] sumTokensForRoomBounded(String roomId, Instant since, Instant until) {
		int[] zeros = {0, 0, 0, 0};
		if (roomId == null) return zeros;

		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database unavailable; cannot recover tokens for roomId '{}'.", roomId);
			return zeros;
		}

		StringBuilder sql = new StringBuilder(
				"SELECT COALESCE(SUM(INPUT_TOKENS),0), COALESCE(SUM(OUTPUT_TOKENS),0), "
				+ "COALESCE(SUM(CACHE_READ_TOKENS),0), COALESCE(SUM(CACHE_CREATION_TOKENS),0) "
				+ "FROM MESSAGE WHERE ROOM_ID = ?");
		if (since != null) {
			sql.append(" AND DATE_CREATED >= ?");
		}
		if (until != null) {
			sql.append(" AND DATE_CREATED <= ?");
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement(sql.toString());
			int index = 1;
			ps.setString(index++, roomId);
			if (since != null) {
				ps.setTimestamp(index++, Timestamp.from(since));
			}
			if (until != null) {
				ps.setTimestamp(index++, Timestamp.from(until));
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				return new int[] { rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4) };
			}
		} catch (Exception e) {
			classLogger.warn("sumTokensForRoomBounded: failed for roomId '{}'.", roomId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return zeros;
	}

	/**
	 * Sums tokens from MESSAGE table for a specific trace by joining on the trace's
	 * stored START_TIME/END_TIME. Accounts for timezone storage mismatch: AGENT_TRACE
	 * uses Timestamp.from(Instant) (true UTC epoch) while MESSAGE uses
	 * Timestamp.valueOf(LocalDateTime) which shifts by JVM timezone.
	 * We compensate by shifting trace bounds by the JVM timezone offset.
	 *
	 * @param traceId the trace ID to get tokens for
	 * @param roomId  the room ID (for MESSAGE filtering)
	 * @return int[4] — {inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens}
	 */
	public static int[] sumTokensForTrace(String traceId, String roomId) {
		int[] zeros = {0, 0, 0, 0};
		if (traceId == null || roomId == null) return zeros;

		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) return zeros;

		// Direct TRACE_ID correlation — no timezone hacks needed
		String sql = "SELECT COALESCE(SUM(m.INPUT_TOKENS),0), COALESCE(SUM(m.OUTPUT_TOKENS),0), "
				+ "COALESCE(SUM(m.CACHE_READ_TOKENS),0), COALESCE(SUM(m.CACHE_CREATION_TOKENS),0) "
				+ "FROM MESSAGE m "
				+ "WHERE m.TRACE_ID = ? AND m.ROOM_ID = ?";

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement(sql);
			ps.setString(1, traceId);
			ps.setString(2, roomId);
			rs = ps.executeQuery();
			if (rs.next()) {
				return new int[] { rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4) };
			}
		} catch (Exception e) {
			classLogger.warn("sumTokensForTrace: query failed for traceId '{}'.", traceId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return zeros;
	}

	// -------------------------------------------------------------------------
	// Project-scoped trace listing
	// -------------------------------------------------------------------------

	/**
	 * Lists traces filtered by PROJECT_ID.
	 *
	 * @param projectId project filter
	 * @param userId    user filter (for access control)
	 * @param limit     max rows; use 0 for no limit
	 * @return list of trace maps, newest first
	 */
	public static List<Map<String, Object>> listTracesByProject(String projectId, String userId, int limit) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty trace list.");
			return new ArrayList<>();
		}
		SelectQueryStruct qs = buildTraceSelector();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "PROJECT_ID", "==", projectId));
		if (userId != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(AGENT_TRACE_TABLE + "USER_ID", "==", userId));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector(AGENT_TRACE_TABLE + "START_TIME", "DESC"));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		try {
			return QueryExecutionUtility.flushRsToMap(db, qs);
		} catch (Exception e) {
			classLogger.error("Failed to list traces for projectId '{}', userId '{}'.", projectId, userId, e);
			return new ArrayList<>();
		}
	}

	// -------------------------------------------------------------------------
	// User message retrieval (for prompt correlation)
	// -------------------------------------------------------------------------

	/**
	 * Fetches the user INPUT message that most closely precedes the given timestamp
	 * in the specified room. Used to correlate "what the user typed" to an agent trace.
	 *
	 * @param roomId         the room to search
	 * @param traceStartTime timestamp string (format: "yyyy-MM-dd HH:mm:ss" or ISO)
	 * @return the user prompt text, or null if not found
	 */
	public static String fetchUserPromptBeforeTime(String roomId, String traceStartTime) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) return null;

		String sql = "SELECT MESSAGE_DATA FROM MESSAGE "
				+ "WHERE ROOM_ID = ? AND MESSAGE_TYPE = 'INPUT' AND DATE_CREATED <= ? "
				+ "ORDER BY DATE_CREATED DESC";

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = db.getPreparedStatement(sql);
			ps.setString(1, roomId);
			String tsStr = traceStartTime.replace("T", " ");
			if (tsStr.endsWith("Z")) tsStr = tsStr.substring(0, tsStr.length() - 1);
			ps.setTimestamp(2, Timestamp.valueOf(tsStr));
			ps.setMaxRows(1);
			rs = ps.executeQuery();
			if (rs.next()) {
				byte[] data = rs.getBytes(1);
				if (data != null) {
					return new String(data, StandardCharsets.UTF_8);
				}
			}
		} catch (Exception e) {
			classLogger.debug("fetchUserPromptBeforeTime: could not retrieve prompt for room '{}': {}", roomId, e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return null;
	}

	/**
	 * Fetches all user INPUT messages for a room, in chronological order.
	 *
	 * @param roomId the room to fetch messages for
	 * @return list of maps with MESSAGE_ID, MESSAGE_DATA, DATE_CREATED
	 */
	public static List<Map<String, Object>> fetchUserMessagesForRoom(String roomId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) return new ArrayList<>();

		String sql = "SELECT MESSAGE_ID, MESSAGE_DATA, DATE_CREATED FROM MESSAGE "
				+ "WHERE ROOM_ID = ? AND MESSAGE_TYPE = 'INPUT' "
				+ "ORDER BY DATE_CREATED ASC";

		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> messages = new ArrayList<>();
		try {
			ps = db.getPreparedStatement(sql);
			ps.setString(1, roomId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> msg = new LinkedHashMap<>();
				msg.put("MESSAGE_ID", rs.getString("MESSAGE_ID"));
				byte[] data = rs.getBytes("MESSAGE_DATA");
				msg.put("MESSAGE_DATA", data != null ? new String(data, StandardCharsets.UTF_8) : null);
				Timestamp ts = rs.getTimestamp("DATE_CREATED");
				msg.put("DATE_CREATED", ts != null ? ts.toInstant().toString() : null);
				messages.add(msg);
			}
		} catch (Exception e) {
			classLogger.debug("fetchUserMessagesForRoom: could not retrieve messages for room '{}': {}", roomId, e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return messages;
	}

	/**
	 * Fetches both INPUT and RESPONSE messages for a room, chronologically.
	 * Each row includes MESSAGE_ID, MESSAGE_TYPE, MESSAGE_DATA, DATE_CREATED, and TRACE_ID.
	 * TRACE_ID enables direct correlation to AGENT_TRACE rows instead of position-based pairing.
	 */
	public static List<Map<String, Object>> fetchConversationForRoom(String roomId) {
		IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
		if (db == null) return new ArrayList<>();

		String sql = "SELECT MESSAGE_ID, MESSAGE_TYPE, MESSAGE_DATA, DATE_CREATED, TRACE_ID FROM MESSAGE "
				+ "WHERE ROOM_ID = ? AND MESSAGE_TYPE IN ('INPUT', 'RESPONSE') "
				+ "ORDER BY DATE_CREATED ASC";

		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Map<String, Object>> messages = new ArrayList<>();
		try {
			ps = db.getPreparedStatement(sql);
			ps.setString(1, roomId);
			rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> msg = new LinkedHashMap<>();
				msg.put("MESSAGE_ID", rs.getString("MESSAGE_ID"));
				msg.put("MESSAGE_TYPE", rs.getString("MESSAGE_TYPE"));
				byte[] data = rs.getBytes("MESSAGE_DATA");
				msg.put("MESSAGE_DATA", data != null ? new String(data, StandardCharsets.UTF_8) : null);
				Timestamp ts = rs.getTimestamp("DATE_CREATED");
				msg.put("DATE_CREATED", ts != null ? ts.toInstant().toString() : null);
				msg.put("TRACE_ID", rs.getString("TRACE_ID"));
				messages.add(msg);
			}
		} catch (Exception e) {
			classLogger.debug("fetchConversationForRoom: could not retrieve messages for room '{}': {}", roomId, e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(db, null, ps, rs);
		}
		return messages;
	}
}
