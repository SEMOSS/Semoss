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
package prerna.reactor.agent;

import java.time.Instant;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;

/**
 * Abstract base class for agent harnesses that centralizes trace lifecycle management.
 *
 * <p>Mirrors the {@link prerna.engine.impl.model.AbstractModelEngine} pattern where
 * {@code askRoom()} wraps {@code askCall()} with cross-cutting concerns (token tracking,
 * inference logging). Here, {@link #execute(AgentRunContext)} wraps
 * {@link #executeCall(AgentRunContext)} with trace open/close lifecycle.
 *
 * <p>Provides:
 * <ul>
 *   <li>Automatic trace creation ({@code AGENT_TRACE} row) — opens before, closes after</li>
 *   <li>Termination reason tracking (SUCCESS vs ERROR)</li>
 *   <li>Active traceId registration/cleanup for downstream token attribution</li>
 *   <li>Common userId/projectId resolution</li>
 * </ul>
 *
 * <p>Subclasses implement {@link #executeCall(AgentRunContext)} which contains only the
 * harness-specific execution logic. The base class handles all observability concerns.
 *
 * <p>Harnesses that need custom trace lifecycle (e.g. {@link RoomAgentHarness} with per-tool
 * step recording) override
 * {@link #execute(AgentRunContext)} directly and manage their own trace calls using
 * the protected helpers provided here.
 *
 * @see IAgentHarness
 * @see AppBuildingHarness
 */
public abstract class AbstractAgentHarness implements IAgentHarness {

    private static final Logger classLogger = LogManager.getLogger(AbstractAgentHarness.class);

    /**
     * Default trace-wrapping implementation. Opens a trace, delegates to
     * {@link #executeCall(AgentRunContext)}, then closes the trace in a finally block.
     *
     * <p>Subclasses with simple lifecycle (e.g. app-building harnesses) rely on this default.
     * Subclasses with complex lifecycle (e.g. RoomAgentHarness)
     * override this method entirely and manage their own trace using the protected helpers.
     */
    @Override
    public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
        String traceId = UUID.randomUUID().toString();
        Instant startTime = Instant.now();
        String terminationReason = "SUCCESS";
        AgentHarnessResult result = null;

        String userId = resolveUserId(ctx);
        String projectId = resolveProjectId(ctx);
        String roomId = ctx.getRoom() != null ? ctx.getRoom().getId() : null;
        String engineId = ctx.getModelEngine() != null ? ctx.getModelEngine().getEngineId() : null;

        AgentTraceLogsUtils.setActiveTraceId(ctx.getInsight().getInsightId(), traceId);

        try {
            result = executeCall(ctx);
            return result;
        } catch (Exception e) {
            terminationReason = "ERROR: " + e.getClass().getSimpleName();
            throw e;
        } finally {
            Instant endTime = Instant.now();
            AgentTraceLogsUtils.clearActiveTraceId(ctx.getInsight().getInsightId());
            AgentTraceLogsUtils.clearStepCounter(traceId);
            int toolCalls = result != null ? result.getToolCallRecords().size() : 0;
            int iterations = result != null ? result.getIterations() : 0;
            // Build metricsJson with token usage scoped to this trace's time window
            String metricsJson = buildMetricsJson(roomId, startTime, endTime);
            AgentTraceLogsUtils.logTrace(
                    traceId,
                    roomId,
                    userId,
                    projectId,
                    engineId,
                    getName(),
                    startTime,
                    endTime,
                    iterations,
                    toolCalls,
                    terminationReason,
                    metricsJson,
                    ctx.getParentTraceId());
        }
    }

    /**
     * Builds a metrics JSON string with token usage for this trace's time window.
     * Queries the MESSAGE table bounded by [startTime, endTime] so each trace
     * gets only its own tokens — preventing duplication when multiple traces
     * share the same room.
     */
    private String buildMetricsJson(String roomId, Instant startTime, Instant endTime) {
        if (roomId == null) return null;
        try {
            int[] tokens = AgentTraceLogsUtils.sumTokensForRoomBounded(roomId, startTime, endTime);
            if (tokens[0] == 0 && tokens[1] == 0) return null;
            return "{\"inputTokens\":" + tokens[0] + ",\"outputTokens\":" + tokens[1] + "}";
        } catch (Exception e) {
            classLogger.debug("Failed to build metrics JSON for trace: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Subclass implementation point — contains harness-specific execution logic.
     *
     * <p>Follows the SEMOSS engine convention: {@code askRoom() → askCall()},
     * {@code nearestNeighbor() → nearestNeighborCall()}, {@code execute() → executeCall()}.
     *
     * <p>When this method is called, a traceId has already been registered as active.
     * The trace will be automatically closed after this method returns (or throws).
     *
     * @param ctx fully-resolved context (Room, model, insight, params)
     * @return result with final text, iteration count, and tool-call records
     * @throws Exception on unrecoverable errors
     */
    protected abstract AgentHarnessResult executeCall(AgentRunContext ctx) throws Exception;

    // ============================================================
    // Common resolution helpers
    // ============================================================

    protected static String resolveUserId(AgentRunContext ctx) {
        if (ctx.getInsight() != null && ctx.getInsight().getUser() != null
                && ctx.getInsight().getUser().getPrimaryLoginToken() != null) {
            return ctx.getInsight().getUser().getPrimaryLoginToken().getId();
        }
        return null;
    }

    protected static String resolveProjectId(AgentRunContext ctx) {
        if (ctx.getInsight() != null) {
            return ctx.getInsight().getProjectId();
        }
        return null;
    }
}
