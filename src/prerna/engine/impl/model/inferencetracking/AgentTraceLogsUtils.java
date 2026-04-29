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

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
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

		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (modelInferenceLogsDb == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; skipping agent trace log for traceId '{}'.", traceId);
			return;
		}

		String query = "INSERT INTO AGENT_TRACE "
				+ "(TRACE_ID, ROOM_ID, USER_ID, MODEL_ENGINE_ID, HARNESS_TYPE, "
				+ "START_TIME, END_TIME, ITERATIONS, TOOL_CALL_COUNT, "
				+ "TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, traceId);
			ps.setString(index++, roomId);
			ps.setString(index++, userId);
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
}
