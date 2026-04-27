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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentHarnessResult.DecisionStep;
import prerna.reactor.agent.AgentHarnessResult.ToolCallRecord;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

/**
 * Utility for persisting agent decision traces to the model-inference tracking database.
 *
 * <p>Follows the same pattern as {@link ModelInferenceLogsUtils}:
 * <ul>
 *   <li>Uses {@link SystemEngineRegistry#getModelInferenceLogsDb()} as the backing store.
 *   <li>Respects the model engine's content-retention policy — when {@code keepInputOutput} is
 *       {@code false}, only metadata (IDs, counts, timing) is stored; no content or tool results.
 *   <li>Schema is registered in {@link ModelInferenceLogsOwlCreator} and created automatically
 *       by {@link ModelInferenceLogsUtils#initModelInferenceLogsDatabase()} at server startup.
 * </ul>
 *
 * <p>Callers should invoke {@link #logTrace} from a background thread (e.g.
 * {@code CompletableFuture.runAsync}) with all content retention decisions made before dispatch.
 * Only immutable scalar values should cross thread boundaries.
 */
public final class AgentTraceLogsUtils {

    private static final Logger classLogger = LogManager.getLogger(AgentTraceLogsUtils.class);

    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

    // ── Table and column names ────────────────────────────────────────────────

    public static final String TABLE_AGENT_TRACE      = "AGENT_TRACE";
    public static final String TABLE_AGENT_TRACE_STEP = "AGENT_TRACE_STEP";

    // AGENT_TRACE columns
    private static final String COL_TRACE_ID           = "TRACE_ID";
    private static final String COL_ROOM_ID            = "ROOM_ID";
    private static final String COL_USER_ID            = "USER_ID";
    private static final String COL_MODEL_ENGINE_ID    = "MODEL_ENGINE_ID";
    private static final String COL_START_TIME         = "START_TIME";
    private static final String COL_END_TIME           = "END_TIME";
    private static final String COL_ITERATIONS         = "ITERATIONS";
    private static final String COL_REFLECTIONS_USED   = "REFLECTIONS_USED";
    private static final String COL_TERMINATION_REASON = "TERMINATION_REASON";
    private static final String COL_METRICS_JSON       = "METRICS_JSON";
    private static final String COL_TOOL_CALL_COUNT    = "TOOL_CALL_COUNT";

    // AGENT_TRACE_STEP columns
    private static final String COL_STEP_INDEX          = "STEP_INDEX";
    private static final String COL_RESPONSE_TYPE       = "RESPONSE_TYPE";
    private static final String COL_INPUT_MESSAGE_ID    = "INPUT_MESSAGE_ID";
    private static final String COL_RESPONSE_MESSAGE_ID = "RESPONSE_MESSAGE_ID";
    private static final String COL_TOOL_CALLS_JSON     = "TOOL_CALLS_JSON";
    private static final String COL_GUARDRAILS_JSON     = "GUARDRAILS_JSON";
    private static final String COL_TOKEN_COUNT         = "TOKEN_COUNT";
    private static final String COL_STEP_TIMESTAMP      = "STEP_TIMESTAMP";

    private AgentTraceLogsUtils() { }

    // ── Trace persistence ─────────────────────────────────────────────────────

    /**
     * Persists an agent trace to the model-inference tracking database.
     *
     * <p>Content retention is caller-controlled: pass the result of
     * {@code modelEngine.keepInputOutput()} as {@code keepContent}. This must be resolved on
     * the calling thread before dispatching to a background thread, since the model engine
     * object should not be accessed from multiple threads.
     *
     * <p>This method is safe to call from a background thread as long as the parameters are
     * immutable scalars or already-serialized data.
     *
     * @param result      the completed agent run result (must have non-null traceId)
     * @param userId      user who initiated the run (for security scoping queries)
     * @param keepContent whether the model engine permits content logging
     */
    public static void logTrace(AgentHarnessResult result, String userId, boolean keepContent) {
        String traceId = result.getTraceId();
        if (traceId == null) {
            classLogger.debug("AgentTraceLogsUtils.logTrace: result has no traceId; skipping.");
            return;
        }

        IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
        if (db == null) {
            classLogger.warn("AgentTraceLogsUtils.logTrace: model inference logs DB not available; trace {} dropped.", traceId);
            return;
        }

        Connection conn = null;
        try {
            conn = db.getConnection();
            insertTraceRecord(db, conn, result, userId);
            if (result.getSteps() != null) {
                for (DecisionStep step : result.getSteps()) {
                    insertStepRecord(db, conn, traceId, step, keepContent);
                }
            }
            if (!conn.getAutoCommit()) conn.commit();
            classLogger.debug("AgentTraceLogsUtils.logTrace: persisted trace {} ({} steps).",
                    traceId, result.getSteps() != null ? result.getSteps().size() : 0);
        } catch (Exception e) {
            classLogger.error("AgentTraceLogsUtils.logTrace: failed to persist trace {}.", traceId, e);
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ignored) { }
            }
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(db, conn, null, null);
        }
    }

    private static void insertTraceRecord(IRDBMSEngine db, Connection conn,
            AgentHarnessResult result, String userId) throws SQLException, java.io.UnsupportedEncodingException {
        String sql = "INSERT INTO " + TABLE_AGENT_TRACE + " ("
                + COL_TRACE_ID + ", " + COL_ROOM_ID + ", " + COL_USER_ID + ", "
                + COL_MODEL_ENGINE_ID + ", " + COL_START_TIME + ", " + COL_END_TIME + ", "
                + COL_ITERATIONS + ", " + COL_REFLECTIONS_USED + ", "
                + COL_TERMINATION_REASON + ", " + COL_METRICS_JSON + ", " + COL_TOOL_CALL_COUNT
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            int index = 1;
            ps.setString(index++, result.getTraceId());
            ps.setString(index++, result.getRoomId());
            ps.setString(index++, userId);
            ps.setString(index++, result.getModelEngineId());
            ps.setTimestamp(index++, result.getStartTime() != null
                    ? Timestamp.from(result.getStartTime()) : null);
            ps.setTimestamp(index++, result.getEndTime() != null
                    ? Timestamp.from(result.getEndTime()) : null);
            ps.setInt(index++, result.getIterations());
            ps.setInt(index++, result.getReflectionsUsed());
            ps.setString(index++, result.getTerminationReason());
            String metricsJson = result.getMetrics() != null ? GSON.toJson(result.getMetrics()) : null;
            db.getQueryUtil().handleInsertionOfClob(ps, metricsJson, index++, GSON);
            ps.setInt(index++, result.getToolCallRecords() != null
                    ? result.getToolCallRecords().size() : 0);
            ps.executeUpdate();
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps);
        }
    }

    private static void insertStepRecord(IRDBMSEngine db, Connection conn,
            String traceId, DecisionStep step, boolean keepContent) throws SQLException, java.io.UnsupportedEncodingException {
        String sql = "INSERT INTO " + TABLE_AGENT_TRACE_STEP + " ("
                + COL_TRACE_ID + ", " + COL_STEP_INDEX + ", " + COL_RESPONSE_TYPE + ", "
                + COL_INPUT_MESSAGE_ID + ", " + COL_RESPONSE_MESSAGE_ID + ", "
                + COL_TOOL_CALLS_JSON + ", " + COL_GUARDRAILS_JSON + ", "
                + COL_TOKEN_COUNT + ", " + COL_STEP_TIMESTAMP
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            int index = 1;
            ps.setString(index++, traceId);
            ps.setInt(index++, step.getStepIndex());
            ps.setString(index++, step.getModelResponseType() != null
                    ? step.getModelResponseType().name() : null);
            // Message IDs are always stored — they are references, not content
            ps.setString(index++, step.getInputMessageId());
            ps.setString(index++, step.getResponseMessageId());
            // Tool call details only if content logging is enabled
            String toolCallsJson;
            if (keepContent) {
                toolCallsJson = serializeToolCalls(step.getToolCalls());
            } else {
                // Store count only, not the full results (which may contain sensitive content)
                int count = step.getToolCalls() != null ? step.getToolCalls().size() : 0;
                toolCallsJson = GSON.toJson(Collections.singletonMap("count", count));
            }
            db.getQueryUtil().handleInsertionOfClob(ps, toolCallsJson, index++, GSON);
            String guardrailsJson = step.getGuardrailsFired() != null
                    ? GSON.toJson(step.getGuardrailsFired()) : null;
            db.getQueryUtil().handleInsertionOfClob(ps, guardrailsJson, index++, GSON);
            ps.setLong(index++, step.getTokenCount());
            ps.setTimestamp(index++, step.getTimestamp() != null
                    ? Timestamp.from(step.getTimestamp()) : null);
            ps.executeUpdate();
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps);
        }
    }

    private static String serializeToolCalls(List<ToolCallRecord> toolCalls) {
        if (toolCalls == null || toolCalls.isEmpty()) return "[]";
        var list = toolCalls.stream()
                .map(tc -> java.util.Map.of(
                        "toolName",   tc.getToolName()   != null ? tc.getToolName()   : "",
                        "toolCallId", tc.getToolCallId() != null ? tc.getToolCallId() : "",
                        "durationMs", tc.getDurationMs(),
                        "success",    tc.isSuccess(),
                        "result",     tc.getResult()     != null ? tc.getResult()     : ""))
                .collect(java.util.stream.Collectors.toList());
        return GSON.toJson(list);
    }

    // ── Query helpers ─────────────────────────────────────────────────────────

    /**
     * Returns a lightweight summary of traces for the given room, ordered by start time
     * descending. Does NOT return step details — use {@link #getTrace} for a full trace.
     *
     * @param roomId the room to query
     * @param limit  maximum rows to return
     * @return list of trace summary maps (traceId, startTime, endTime, iterations, terminationReason)
     */
    public static List<Map<String, Object>> listTraces(String roomId, int limit) {
        IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
        if (db == null) return Collections.emptyList();

        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT " + COL_TRACE_ID + ", " + COL_ROOM_ID + ", " + COL_MODEL_ENGINE_ID
                + ", " + COL_START_TIME + ", " + COL_END_TIME + ", " + COL_ITERATIONS
                + ", " + COL_REFLECTIONS_USED + ", " + COL_TERMINATION_REASON + ", " + COL_TOOL_CALL_COUNT
                + " FROM " + TABLE_AGENT_TRACE
                + " WHERE " + COL_ROOM_ID + " = ?"
                + " ORDER BY " + COL_START_TIME + " DESC";

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = db.getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, roomId);
            if (limit > 0) {
                ps.setMaxRows(limit);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("traceId",           rs.getString(COL_TRACE_ID));
                row.put("roomId",            rs.getString(COL_ROOM_ID));
                row.put("modelEngineId",     rs.getString(COL_MODEL_ENGINE_ID));
                row.put("startTime",         rs.getTimestamp(COL_START_TIME));
                row.put("endTime",           rs.getTimestamp(COL_END_TIME));
                row.put("iterations",        rs.getInt(COL_ITERATIONS));
                row.put("reflectionsUsed",   rs.getInt(COL_REFLECTIONS_USED));
                row.put("terminationReason", rs.getString(COL_TERMINATION_REASON));
                row.put("toolCallCount",     rs.getInt(COL_TOOL_CALL_COUNT));
                results.add(row);
            }
        } catch (Exception e) {
            classLogger.error("AgentTraceLogsUtils.listTraces: failed for roomId={}.", roomId, e);
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(db, conn, ps, rs);
        }
        return results;
    }

    /**
     * Returns the full trace record (header + steps) for the given trace ID.
     *
     * @param traceId the trace to retrieve
     * @return map with "header" and "steps" keys, or empty map if not found
     */
    public static Map<String, Object> getTrace(String traceId) {
        IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
        if (db == null) return Collections.emptyMap();

        Map<String, Object> trace = new LinkedHashMap<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = db.getConnection();

            // Header — explicit column projection; ps.setMaxRows(1) avoids full scan on duplicates
            String headerSql = "SELECT "
                    + COL_TRACE_ID + ", " + COL_ROOM_ID + ", " + COL_USER_ID + ", "
                    + COL_MODEL_ENGINE_ID + ", " + COL_START_TIME + ", " + COL_END_TIME + ", "
                    + COL_ITERATIONS + ", " + COL_REFLECTIONS_USED + ", "
                    + COL_TERMINATION_REASON + ", " + COL_METRICS_JSON + ", " + COL_TOOL_CALL_COUNT
                    + " FROM " + TABLE_AGENT_TRACE
                    + " WHERE " + COL_TRACE_ID + " = ?";
            ps = conn.prepareStatement(headerSql);
            ps.setString(1, traceId);
            ps.setMaxRows(1);
            rs = ps.executeQuery();
            if (rs.next()) {
                Map<String, Object> header = new LinkedHashMap<>();
                header.put("traceId",           rs.getString(COL_TRACE_ID));
                header.put("roomId",            rs.getString(COL_ROOM_ID));
                header.put("userId",            rs.getString(COL_USER_ID));
                header.put("modelEngineId",     rs.getString(COL_MODEL_ENGINE_ID));
                header.put("startTime",         rs.getTimestamp(COL_START_TIME));
                header.put("endTime",           rs.getTimestamp(COL_END_TIME));
                header.put("iterations",        rs.getInt(COL_ITERATIONS));
                header.put("reflectionsUsed",   rs.getInt(COL_REFLECTIONS_USED));
                header.put("terminationReason", rs.getString(COL_TERMINATION_REASON));
                header.put("metricsJson",       rs.getString(COL_METRICS_JSON));
                header.put("toolCallCount",     rs.getInt(COL_TOOL_CALL_COUNT));
                trace.put("header", header);
            }
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps, rs);
            ps = null;
            rs = null;

            // Steps — explicit column projection
            String stepSql = "SELECT "
                    + COL_TRACE_ID + ", " + COL_STEP_INDEX + ", " + COL_RESPONSE_TYPE + ", "
                    + COL_INPUT_MESSAGE_ID + ", " + COL_RESPONSE_MESSAGE_ID + ", "
                    + COL_TOOL_CALLS_JSON + ", " + COL_GUARDRAILS_JSON + ", "
                    + COL_TOKEN_COUNT + ", " + COL_STEP_TIMESTAMP
                    + " FROM " + TABLE_AGENT_TRACE_STEP
                    + " WHERE " + COL_TRACE_ID + " = ?"
                    + " ORDER BY " + COL_STEP_INDEX + " ASC";
            ps = conn.prepareStatement(stepSql);
            ps.setString(1, traceId);
            rs = ps.executeQuery();
            List<Map<String, Object>> steps = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> step = new LinkedHashMap<>();
                step.put("stepIndex",         rs.getInt(COL_STEP_INDEX));
                step.put("responseType",      rs.getString(COL_RESPONSE_TYPE));
                step.put("inputMessageId",    rs.getString(COL_INPUT_MESSAGE_ID));
                step.put("responseMessageId", rs.getString(COL_RESPONSE_MESSAGE_ID));
                step.put("toolCallsJson",     rs.getString(COL_TOOL_CALLS_JSON));
                step.put("guardrailsJson",    rs.getString(COL_GUARDRAILS_JSON));
                step.put("tokenCount",        rs.getLong(COL_TOKEN_COUNT));
                step.put("stepTimestamp",     rs.getTimestamp(COL_STEP_TIMESTAMP));
                steps.add(step);
            }
            trace.put("steps", steps);
        } catch (Exception e) {
            classLogger.error("AgentTraceLogsUtils.getTrace: failed for traceId={}.", traceId, e);
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(db, conn, ps, rs);
        }
        return trace;
    }

    /**
     * Lists trace summaries across ALL rooms. Intended for admin dashboards only.
     * Callers are responsible for enforcing the admin security check before invoking this.
     *
     * @param limit max rows (0 = no limit, capped by caller at MAX_LIMIT)
     * @return list of trace summary maps ordered by start time descending
     */
    public static List<Map<String, Object>> listAllTraces(int limit) {
        IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
        if (db == null) return Collections.emptyList();

        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT " + COL_TRACE_ID + ", " + COL_ROOM_ID + ", " + COL_USER_ID + ", "
                + COL_MODEL_ENGINE_ID + ", " + COL_START_TIME + ", " + COL_END_TIME + ", "
                + COL_ITERATIONS + ", " + COL_REFLECTIONS_USED + ", "
                + COL_TERMINATION_REASON + ", " + COL_TOOL_CALL_COUNT
                + " FROM " + TABLE_AGENT_TRACE
                + " ORDER BY " + COL_START_TIME + " DESC";

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = db.getConnection();
            ps = conn.prepareStatement(sql);
            if (limit > 0) {
                ps.setMaxRows(limit);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("traceId",           rs.getString(COL_TRACE_ID));
                row.put("roomId",            rs.getString(COL_ROOM_ID));
                row.put("userId",            rs.getString(COL_USER_ID));
                row.put("modelEngineId",     rs.getString(COL_MODEL_ENGINE_ID));
                row.put("startTime",         rs.getTimestamp(COL_START_TIME));
                row.put("endTime",           rs.getTimestamp(COL_END_TIME));
                row.put("iterations",        rs.getInt(COL_ITERATIONS));
                row.put("reflectionsUsed",   rs.getInt(COL_REFLECTIONS_USED));
                row.put("terminationReason", rs.getString(COL_TERMINATION_REASON));
                row.put("toolCallCount",     rs.getInt(COL_TOOL_CALL_COUNT));
                results.add(row);
            }
        } catch (Exception e) {
            classLogger.error("AgentTraceLogsUtils.listAllTraces: failed.", e);
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(db, conn, ps, rs);
        }
        return results;
    }

    /**
     * Computes aggregate dashboard metrics across all rooms.
     * Intended for admin use only — callers must enforce the security check.
     *
     * <p>Returns a map with:
     * <ul>
     *   <li>{@code totalRuns}         — all-time count
     *   <li>{@code runsLast24h}       — runs started in the last 24 hours
     *   <li>{@code avgIterations}     — average agent iterations (excluding null end-times)
     *   <li>{@code avgDurationMs}     — average run duration in milliseconds
     *   <li>{@code successRate}       — fraction of runs with terminationReason = 'text_response'
     *   <li>{@code totalToolCalls}    — sum of TOOL_CALL_COUNT
     *   <li>{@code topModels}         — list of {modelEngineId, runCount} ordered by runCount desc
     *   <li>{@code runsByDay}         — list of {date (yyyy-MM-dd), count} for the last 7 days
     * </ul>
     */
    public static Map<String, Object> getMetrics() {
        IRDBMSEngine db = SystemEngineRegistry.getModelInferenceLogsDb();
        Map<String, Object> metrics = new LinkedHashMap<>();
        if (db == null) {
            metrics.put("error", "ModelInferenceLogs database not available");
            return metrics;
        }

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = db.getConnection();

            // ── 1. Overall aggregates ──────────────────────────────────────────
            String aggSql = "SELECT COUNT(*) AS total_runs,"
                    + " SUM(CASE WHEN TERMINATION_REASON = 'text_response' THEN 1 ELSE 0 END) AS success_count,"
                    + " AVG(ITERATIONS) AS avg_iterations,"
                    + " SUM(TOOL_CALL_COUNT) AS total_tool_calls"
                    + " FROM " + TABLE_AGENT_TRACE;
            ps = conn.prepareStatement(aggSql);
            rs = ps.executeQuery();
            long totalRuns = 0;
            long successCount = 0;
            double avgIterations = 0;
            long totalToolCalls = 0;
            if (rs.next()) {
                totalRuns      = rs.getLong("total_runs");
                successCount   = rs.getLong("success_count");
                avgIterations  = rs.getDouble("avg_iterations");
                totalToolCalls = rs.getLong("total_tool_calls");
            }
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps, rs);
            ps = null; rs = null;

            // ── 2. Average duration (Java-side to stay DB-agnostic) ───────────
            String durSql = "SELECT " + COL_START_TIME + ", " + COL_END_TIME
                    + " FROM " + TABLE_AGENT_TRACE
                    + " WHERE " + COL_END_TIME + " IS NOT NULL";
            ps = conn.prepareStatement(durSql);
            rs = ps.executeQuery();
            long durationSum = 0;
            long durationCount = 0;
            while (rs.next()) {
                Timestamp start = rs.getTimestamp(COL_START_TIME);
                Timestamp end   = rs.getTimestamp(COL_END_TIME);
                if (start != null && end != null) {
                    durationSum += (end.getTime() - start.getTime());
                    durationCount++;
                }
            }
            long avgDurationMs = durationCount > 0 ? durationSum / durationCount : 0;
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps, rs);
            ps = null; rs = null;

            // ── 3. Runs in last 24 hours ──────────────────────────────────────
            long cutoff24h = System.currentTimeMillis() - (24L * 60 * 60 * 1000);
            String recentSql = "SELECT COUNT(*) FROM " + TABLE_AGENT_TRACE
                    + " WHERE " + COL_START_TIME + " >= ?";
            ps = conn.prepareStatement(recentSql);
            ps.setTimestamp(1, new Timestamp(cutoff24h));
            rs = ps.executeQuery();
            long runsLast24h = rs.next() ? rs.getLong(1) : 0;
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps, rs);
            ps = null; rs = null;

            // ── 4. Top models ─────────────────────────────────────────────────
            String modelSql = "SELECT " + COL_MODEL_ENGINE_ID + ", COUNT(*) AS run_count"
                    + " FROM " + TABLE_AGENT_TRACE
                    + " GROUP BY " + COL_MODEL_ENGINE_ID
                    + " ORDER BY run_count DESC";
            ps = conn.prepareStatement(modelSql);
            ps.setMaxRows(10);
            rs = ps.executeQuery();
            List<Map<String, Object>> topModels = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("modelEngineId", rs.getString(COL_MODEL_ENGINE_ID));
                m.put("runCount",      rs.getLong("run_count"));
                topModels.add(m);
            }
            ConnectionUtils.closeAllConnectionsIfPooling(null, ps, rs);
            ps = null; rs = null;

            // ── 5. Runs by day — last 7 days (Java-side bucketing) ───────────
            long cutoff7d = System.currentTimeMillis() - (7L * 24 * 60 * 60 * 1000);
            String daysSql = "SELECT " + COL_START_TIME
                    + " FROM " + TABLE_AGENT_TRACE
                    + " WHERE " + COL_START_TIME + " >= ?";
            ps = conn.prepareStatement(daysSql);
            ps.setTimestamp(1, new Timestamp(cutoff7d));
            rs = ps.executeQuery();
            // Bucket by ISO date string
            java.util.TreeMap<String, Integer> dayCounts = new java.util.TreeMap<>();
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp(COL_START_TIME);
                if (ts != null) {
                    String day = sdf.format(new java.util.Date(ts.getTime()));
                    dayCounts.merge(day, 1, Integer::sum);
                }
            }
            List<Map<String, Object>> runsByDay = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : dayCounts.entrySet()) {
                Map<String, Object> d = new LinkedHashMap<>();
                d.put("date",  entry.getKey());
                d.put("count", entry.getValue());
                runsByDay.add(d);
            }

            // ── Assemble result ───────────────────────────────────────────────
            metrics.put("totalRuns",      totalRuns);
            metrics.put("runsLast24h",    runsLast24h);
            metrics.put("avgIterations",  Math.round(avgIterations * 10.0) / 10.0);
            metrics.put("avgDurationMs",  avgDurationMs);
            metrics.put("successRate",    totalRuns > 0 ? Math.round((double) successCount / totalRuns * 1000.0) / 10.0 : 0.0);
            metrics.put("totalToolCalls", totalToolCalls);
            metrics.put("topModels",      topModels);
            metrics.put("runsByDay",      runsByDay);

        } catch (Exception e) {
            classLogger.error("AgentTraceLogsUtils.getMetrics: failed.", e);
            metrics.put("error", e.getMessage());
        } finally {
            ConnectionUtils.closeAllConnectionsIfPooling(db, conn, ps, rs);
        }
        return metrics;
    }
}
