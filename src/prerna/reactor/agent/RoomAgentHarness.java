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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.RunMCPToolReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * {@link IAgentHarness} implementation that drives the SEMOSS Room tool-calling loop.
 *
 * <p>Replicates the {@code AgentHarness} logic from the AI_Repository app project in core so
 * that any SEMOSS server-side job can run a full model-and-tools loop without depending on an
 * app-scoped class.
 *
 * <p>Tool execution uses {@link McpCallMode#REACTOR} by default (delegates to
 * {@link RunMCPToolReactor}) which inherits all SEMOSS engine-resolution and security changes
 * automatically.
 */
public class RoomAgentHarness implements IAgentHarness {

    private static final Logger logger = LogManager.getLogger(RoomAgentHarness.class);

    /** Registry name used by {@link AgentHarnessRegistry}. */
    public static final String NAME = "room_loop";

    /**
     * Pattern to extract UUID from SEMOSS-prefixed tool name: {@code "a<UUID>_toolName"}.
     */
    private static final Pattern UUID_PREFIX_PATTERN = Pattern.compile(
            "^a[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}_");

    /**
     * How MCP tools are executed inside the harness.
     */
    public enum McpCallMode {
        /**
         * Delegates to {@link RunMCPToolReactor} — picks up all SEMOSS internal changes
         * (engine resolution logic, security checks) automatically.
         */
        REACTOR,
        /**
         * Calls the MCP API directly: resolves engine/project from the tool-name prefix,
         * then calls {@link IMCP#callTool}. No security check overhead.
         */
        DIRECT_API
    }

    private final McpCallMode mcpCallMode;

    /** Constructs a harness using {@link McpCallMode#REACTOR} (recommended default). */
    public RoomAgentHarness() {
        this(McpCallMode.REACTOR);
    }

    public RoomAgentHarness(McpCallMode mcpCallMode) {
        this.mcpCallMode = mcpCallMode;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private static final String REFLECTION_PROMPT =
            "Review the analysis you just produced. Are there important aspects you have not yet "
            + "examined, or tool calls that would meaningfully improve the completeness or accuracy "
            + "of your answer? If yes, make those tool calls now and incorporate the new findings "
            + "into your answer. If the analysis is already thorough and complete, respond with "
            + "your final consolidated answer.";

    @Override
    @SuppressWarnings({"unchecked"})
    public AgentHarnessResult execute(GenericAgentContext ctx) throws Exception {
        Room                room           = ctx.getRoom();
        int                 maxIterations  = ctx.getMaxIterations();
        int                 maxReflections = ctx.getMaxReflections();
        Map<String, Object> paramMap       = new HashMap<>(ctx.getParamMap());

        List<AgentHarnessResult.ToolCallRecord> toolCallRecords = new ArrayList<>();

        // ── 1. Initial ask ────────────────────────────────────────────────────
        String systemPrompt = room.getEffectiveSystemPrompt();
        InputMessage firstMsg = InputMessage.builder(room)
                .withSystemPrompt(systemPrompt)
                .withText(ctx.getInput())
                .withModelType(ctx.getModelEngine().getModelType())
                .withParamMap(paramMap)
                .build();

        ResponseMessage response = room.ask(firstMsg, ctx.getModelEngine(), null);

        // ── 2. Tool loop ──────────────────────────────────────────────────────
        int[] iterationsHolder = {0};
        response = driveToolLoop(response, iterationsHolder, paramMap, toolCallRecords, ctx);

        // ── 3. Reflection rounds ──────────────────────────────────────────────
        int reflectionsUsed = 0;
        while (reflectionsUsed < maxReflections
                && response != null
                && response.getMessageType() == MessageType.RESPONSE_TEXT) {

            reflectionsUsed++;
            logger.info("RoomAgentHarness: reflection round {}/{}", reflectionsUsed, maxReflections);

            InputMessage reflectionMsg = InputMessage.builder(room)
                    .withSystemPrompt(systemPrompt)
                    .withText(REFLECTION_PROMPT)
                    .withModelType(ctx.getModelEngine().getModelType())
                    .withParamMap(new HashMap<>(paramMap))
                    .build();

            response = room.ask(reflectionMsg, ctx.getModelEngine(), null);

            // If the model decided to make more tool calls after reflection, drive the loop again.
            if (response != null && response.getMessageType() == MessageType.RESPONSE_TOOL) {
                response = driveToolLoop(response, iterationsHolder, paramMap, toolCallRecords, ctx);
            }
        }

        // ── 4. Iteration cap check ────────────────────────────────────────────
        if (response != null && response.getMessageType() == MessageType.RESPONSE_TOOL) {
            logger.warn("RoomAgentHarness: maxIterations ({}) reached without RESPONSE_TEXT", maxIterations);
            throw new AgentMaxIterationsException(maxIterations);
        }

        // ── 5. Return result ──────────────────────────────────────────────────
        String content = (response != null) ? response.getContent() : null;
        return new AgentHarnessResult(content, iterationsHolder[0], toolCallRecords, reflectionsUsed);
    }

    /**
     * Drives the tool-call loop from a starting response until RESPONSE_TEXT or the iteration
     * cap is hit. Shared by the main loop and each reflection round.
     *
     * @param iterationsHolder single-element array accumulating iterations across rounds
     */
    @SuppressWarnings("unchecked")
    private ResponseMessage driveToolLoop(
            ResponseMessage response,
            int[] iterationsHolder,
            Map<String, Object> paramMap,
            List<AgentHarnessResult.ToolCallRecord> toolCallRecords,
            GenericAgentContext ctx) throws Exception {

        Room room          = ctx.getRoom();
        int  maxIterations = ctx.getMaxIterations();

        while (response != null
                && response.getMessageType() == MessageType.RESPONSE_TOOL
                && iterationsHolder[0] < maxIterations) {

            iterationsHolder[0]++;
            int iterations = iterationsHolder[0];

            String parentMessageId = response.getMessageId();
            List<Map<String, Object>> toolCalls = response.getToolResponses();

            AskModelEngineResponse<?> nextModelResponse = null;
            for (Map<String, Object> toolCall : toolCalls) {
                String toolCallId  = String.valueOf(toolCall.get("id"));
                String rawToolName = String.valueOf(toolCall.get("name"));

                Object argsObj = toolCall.get("arguments");
                if (argsObj == null) {
                    argsObj = toolCall.get("input");
                }
                Map<String, Object> toolParams =
                        (argsObj instanceof Map) ? (Map<String, Object>) argsObj : new HashMap<>();

                logger.info("RoomAgentHarness executing tool: name={} callId={} iter={}",
                        rawToolName, toolCallId, iterations);

                long startMs = System.currentTimeMillis();
                ToolExecOutcome outcome = executeToolSafely(rawToolName, toolParams, ctx);
                long durationMs = System.currentTimeMillis() - startMs;

                logger.info("RoomAgentHarness tool result: name={} durationMs={} success={}",
                        rawToolName, durationMs, outcome.success);

                toolCallRecords.add(new AgentHarnessResult.ToolCallRecord(
                        rawToolName, toolCallId, outcome.content, durationMs, outcome.success));

                // IMPORTANT: pass a FRESH copy of paramMap on every call.
                // Room.appendToolsToParams() mutates the map; reusing it doubles the tools list.
                nextModelResponse = room.addToolExecutionResult(
                        toolCallId,
                        rawToolName,
                        outcome.content,
                        toolParams,
                        new HashMap<>(paramMap),
                        parentMessageId,
                        ctx.getModelEngine(),
                        ctx.getInsight(),
                        outcome.success ? "success" : "error");
            }

            if (nextModelResponse != null) {
                Object lastMsg = room.getMessages().getLast();
                if (lastMsg instanceof ResponseMessage) {
                    response = (ResponseMessage) lastMsg;
                } else {
                    logger.warn("RoomAgentHarness: unexpected last message type: {}",
                            lastMsg == null ? "null" : lastMsg.getClass().getName());
                    break;
                }
            } else {
                break;
            }
        }

        return response;
    }

    // ── Tool execution ────────────────────────────────────────────────────────

    /** Outcome of a single tool execution. */
    private static final class ToolExecOutcome {
        final String  content;
        final boolean success;
        ToolExecOutcome(String content, boolean success) {
            this.content = content;
            this.success = success;
        }
    }

    private ToolExecOutcome executeToolSafely(String rawToolName, Map<String, Object> params,
                                              GenericAgentContext ctx) {
        String engineId = extractEngineId(rawToolName);
        if (engineId == null) {
            String msg = "Tool execution error: cannot parse engine/project id from tool name '"
                    + rawToolName + "'";
            logger.warn("RoomAgentHarness: {}", msg);
            return new ToolExecOutcome(msg, false);
        }
        try {
            String result = mcpCallMode == McpCallMode.REACTOR
                    ? callMcpToolViaReactor(rawToolName, engineId, params, ctx)
                    : callMcpToolViaApi(rawToolName, engineId, params, ctx);
            boolean success = result == null || !result.startsWith("Tool execution error:");
            return new ToolExecOutcome(result, success);
        } catch (Exception e) {
            String msg = "Tool execution error: " + e.getMessage();
            logger.warn("RoomAgentHarness: uncaught exception from tool '{}': {}", rawToolName, e.getMessage(), e);
            return new ToolExecOutcome(msg, false);
        }
    }

    private String callMcpToolViaReactor(String rawToolName, String engineId,
                                         Map<String, Object> params, GenericAgentContext ctx) {
        try {
            RunMCPToolReactor reactor = new RunMCPToolReactor();
            reactor.In();
            reactor.setInsight(ctx.getInsight());

            GenRowStruct engineGrs = new GenRowStruct();
            engineGrs.add(new NounMetadata(engineId, PixelDataType.CONST_STRING));
            reactor.getNounStore().addNoun(ReactorKeysEnum.ENGINE.getKey(), engineGrs);

            GenRowStruct functionGrs = new GenRowStruct();
            functionGrs.add(new NounMetadata(rawToolName, PixelDataType.CONST_STRING));
            reactor.getNounStore().addNoun(ReactorKeysEnum.FUNCTION.getKey(), functionGrs);

            if (params != null && !params.isEmpty()) {
                GenRowStruct paramGrs = new GenRowStruct();
                paramGrs.add(new NounMetadata(params, PixelDataType.MAP));
                reactor.getNounStore().addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), paramGrs);
            }

            NounMetadata result = reactor.execute();
            return result != null && result.getValue() != null ? result.getValue().toString() : "";
        } catch (Exception e) {
            return "Tool execution error: " + e.getMessage();
        }
    }

    private String callMcpToolViaApi(String rawToolName, String engineId,
                                     Map<String, Object> params, GenericAgentContext ctx) {
        IEngine engine = null;
        try {
            engine = Utility.getEngine(engineId);
        } catch (Exception ignored) {
            // not an engine — try project
        }
        if (engine == null) {
            engine = (IEngine) Utility.getProject(engineId);
        }
        if (engine == null) {
            return "Tool execution error: no engine or project found with id '" + engineId + "'";
        }
        String toolName = MCPUtility.removeEngineIdFromToolsMethodName(engine.getEngineId(), rawToolName);
        IMCP mcp = MCPFactory.build(engine);
        try {
            Object result = mcp.callTool(toolName, params, ctx.getInsight());
            return result != null ? result.toString() : "";
        } catch (Exception e) {
            return "Tool execution error: " + e.getMessage();
        }
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private static String extractEngineId(String rawToolName) {
        if (rawToolName == null) return null;
        Matcher m = UUID_PREFIX_PATTERN.matcher(rawToolName);
        if (!m.find()) return null;
        String prefix = m.group();
        return prefix.substring(1, prefix.length() - 1); // strip leading "a" and trailing "_"
    }
}
