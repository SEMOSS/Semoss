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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

public class AgentTraceLogsUtils {

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
	// Read operations
	// -------------------------------------------------------------------------

	/**
	 * Returns traces filtered by roomId and/or userId, ordered by START_TIME DESC.
	 * Null parameters are omitted from the WHERE clause.
	 */
	public static List<Map<String, Object>> listTraces(String roomId, String userId, int limit) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (modelInferenceLogsDb == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty trace list.");
			return new ArrayList<>();
		}

		StringBuilder sql = new StringBuilder(
				"SELECT TRACE_ID, ROOM_ID, USER_ID, MODEL_ENGINE_ID, HARNESS_TYPE, "
				+ "START_TIME, END_TIME, ITERATIONS, TOOL_CALL_COUNT, "
				+ "TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID "
				+ "FROM AGENT_TRACE WHERE 1=1");
		if (roomId != null) {
			sql.append(" AND ROOM_ID = ?");
		}
		if (userId != null) {
			sql.append(" AND USER_ID = ?");
		}
		sql.append(" ORDER BY START_TIME DESC");
		if (limit > 0) {
			sql.append(" LIMIT ").append(limit);
		}

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(sql.toString());
			int index = 1;
			if (roomId != null) {
				ps.setString(index++, roomId);
			}
			if (userId != null) {
				ps.setString(index++, userId);
			}
			ps.execute();
			ResultSet rs = ps.getResultSet();
			return resultSetToList(rs);
		} catch (Exception e) {
			classLogger.error("Failed to list agent traces for roomId '{}', userId '{}'.", roomId, userId, e);
			return new ArrayList<>();
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Returns all traces ordered by START_TIME DESC (admin use).
	 */
	public static List<Map<String, Object>> listAllTraces(int limit) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (modelInferenceLogsDb == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty trace list.");
			return new ArrayList<>();
		}

		String sql = "SELECT TRACE_ID, ROOM_ID, USER_ID, MODEL_ENGINE_ID, HARNESS_TYPE, "
				+ "START_TIME, END_TIME, ITERATIONS, TOOL_CALL_COUNT, "
				+ "TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID "
				+ "FROM AGENT_TRACE ORDER BY START_TIME DESC"
				+ (limit > 0 ? " LIMIT " + limit : "");

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(sql);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			return resultSetToList(rs);
		} catch (Exception e) {
			classLogger.error("Failed to list all agent traces.", e);
			return new ArrayList<>();
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Returns a single trace by TRACE_ID, or null if not found.
	 */
	public static Map<String, Object> getTrace(String traceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (modelInferenceLogsDb == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; cannot retrieve traceId '{}'.", traceId);
			return null;
		}

		String sql = "SELECT TRACE_ID, ROOM_ID, USER_ID, MODEL_ENGINE_ID, HARNESS_TYPE, "
				+ "START_TIME, END_TIME, ITERATIONS, TOOL_CALL_COUNT, "
				+ "TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID "
				+ "FROM AGENT_TRACE WHERE TRACE_ID = ?";

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(sql);
			ps.setString(1, traceId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			List<Map<String, Object>> rows = resultSetToList(rs);
			return rows.isEmpty() ? null : rows.get(0);
		} catch (Exception e) {
			classLogger.error("Failed to retrieve agent trace for traceId '{}'.", traceId, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	/**
	 * Returns all traces whose PARENT_TRACE_ID matches the given parentTraceId.
	 */
	public static List<Map<String, Object>> getChildTraces(String parentTraceId) {
		IRDBMSEngine modelInferenceLogsDb = SystemEngineRegistry.getModelInferenceLogsDb();
		if (modelInferenceLogsDb == null) {
			classLogger.warn("ModelInferenceLogs database is unavailable; returning empty child trace list.");
			return new ArrayList<>();
		}

		String sql = "SELECT TRACE_ID, ROOM_ID, USER_ID, MODEL_ENGINE_ID, HARNESS_TYPE, "
				+ "START_TIME, END_TIME, ITERATIONS, TOOL_CALL_COUNT, "
				+ "TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID "
				+ "FROM AGENT_TRACE WHERE PARENT_TRACE_ID = ? ORDER BY START_TIME ASC";

		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(sql);
			ps.setString(1, parentTraceId);
			ps.execute();
			ResultSet rs = ps.getResultSet();
			return resultSetToList(rs);
		} catch (Exception e) {
			classLogger.error("Failed to retrieve child traces for parentTraceId '{}'.", parentTraceId, e);
			return new ArrayList<>();
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, ps, null);
		}
	}

	// -------------------------------------------------------------------------
	// Internal helpers
	// -------------------------------------------------------------------------

	private static List<Map<String, Object>> resultSetToList(ResultSet rs) throws Exception {
		List<Map<String, Object>> results = new ArrayList<>();
		if (rs == null) {
			return results;
		}
		ResultSetMetaData meta = rs.getMetaData();
		int columnCount = meta.getColumnCount();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= columnCount; i++) {
				row.put(meta.getColumnLabel(i), rs.getObject(i));
			}
			results.add(row);
		}
		return results;
	}
}
