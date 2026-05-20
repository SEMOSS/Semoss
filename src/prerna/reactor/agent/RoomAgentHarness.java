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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
import prerna.reactor.agent.exceptions.AgentMaxTurnsException;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.RunMCPToolReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Legacy {@link IAgentHarness} that drives the SEMOSS Room tool loop directly.
 *
 * <p>Multiple tool calls from one response can run in parallel. New work
 * should use {@link prerna.reactor.agent.runtime.SemossAgentHarness}.
 *
 * @deprecated Retained as {@code "room_loop"} for backward compatibility.
 */
@Deprecated
public class RoomAgentHarness implements IAgentHarness {

    private static final Logger logger = LogManager.getLogger(RoomAgentHarness.class);

    /** Registry name used by {@link AgentHarnessRegistry}. */
    public static final String NAME = "room_loop";

    private static final String TOOL_STATUS_SUCCESS = "success";
    private static final String TOOL_STATUS_ERROR   = "error";

    /** How MCP tools are executed inside the harness. */
    public enum McpCallMode {
        /** Delegates to {@link RunMCPToolReactor} - picks up all SEMOSS engine-resolution and security changes. */
        REACTOR,
        /** Calls the MCP API directly via {@link IMCP#callTool}. No security check overhead. */
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
    public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
        Room                room           = ctx.getRoom();
        int                 maxReflections = ctx.getMaxReflections();
        Map<String, Object> paramMap       = new HashMap<>(ctx.getParamMap());

        List<AgentHarnessResult.ToolCallRecord> toolCallRecords = new ArrayList<>();
        AtomicInteger iterationsCounter = new AtomicInteger(0);

        // 1. Initial ask
        String systemPrompt = room.getRoomOrWorkspaceSystemPrompt();
        InputMessage firstMsg = InputMessage.builder(room)
                .withSystemPrompt(systemPrompt)
                .withText(ctx.getInput())
                .withModelType(ctx.getModelEngine().getModelType())
                .withParamMap(paramMap)
                .build();

        logger.info("RoomAgentHarness: initial ask room={} model={} inputLength={}",
                room.getId(), ctx.getModelEngine().getEngineId(), ctx.getInput().length());
        ResponseMessage response = room.ask(firstMsg, ctx.getModelEngine(), null);

        // 2. Tool loop
        response = driveToolLoop(response, iterationsCounter, paramMap, toolCallRecords, ctx);

        // 3. Reflection rounds
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

            if (response != null && response.getMessageType() == MessageType.RESPONSE_TOOL) {
                response = driveToolLoop(response, iterationsCounter, paramMap, toolCallRecords, ctx);
            }
        }

        // 4. Turn cap check
        if (response != null && response.getMessageType() == MessageType.RESPONSE_TOOL) {
            logger.warn("RoomAgentHarness: maxTurns ({}) reached without RESPONSE_TEXT",
                    ctx.getMaxTurns());
            throw new AgentMaxTurnsException(ctx.getMaxTurns());
        }

        // 5. Return result
        String content = (response != null) ? response.getContent() : null;
        return new AgentHarnessResult(content, iterationsCounter.get(), toolCallRecords, reflectionsUsed);
    }

    /**
     * Drives the tool loop until text is returned or the turn cap is reached.
     *
     * @param iterationsCounter accumulates iteration count across calls
     */
    @SuppressWarnings("unchecked")
    private ResponseMessage driveToolLoop(
            ResponseMessage response,
            AtomicInteger iterationsCounter,
            Map<String, Object> paramMap,
            List<AgentHarnessResult.ToolCallRecord> toolCallRecords,
            AgentRunContext ctx) throws Exception {

        Room room     = ctx.getRoom();
        int  maxTurns = ctx.getMaxTurns();

        while (response != null
                && response.getMessageType() == MessageType.RESPONSE_TOOL
                && iterationsCounter.get() < maxTurns) {

            int iterations = iterationsCounter.incrementAndGet();

            String parentMessageId = response.getMessageId();
            List<Map<String, Object>> toolCalls = response.getToolResponses();

            AskModelEngineResponse<?> nextModelResponse = null;

            if (toolCalls.size() == 1) {
                // Fast path: single tool - no thread overhead.
                ParsedToolCall tc = new ParsedToolCall(toolCalls.get(0));
                ToolExecResult result = executeOneTool(tc, iterations, paramMap, parentMessageId, ctx);
                toolCallRecords.add(result.record);
                nextModelResponse = result.modelResponse;

            } else {
                // Parallel path: execute all tools concurrently.
                // Room.addToolExecutionResult() is synchronized and only triggers the next model
                // call once every tool ID in the batch has been answered, so concurrent
                // submissions from multiple threads are safe.
                logger.info("RoomAgentHarness executing {} tools in parallel iter={}",
                        toolCalls.size(), iterations);
                ExecutorService pool = Executors.newFixedThreadPool(toolCalls.size());
                try {
                    @SuppressWarnings("unchecked")
                    CompletableFuture<ToolExecResult>[] futures = new CompletableFuture[toolCalls.size()];

                    for (int i = 0; i < toolCalls.size(); i++) {
                        final ParsedToolCall tc = new ParsedToolCall(toolCalls.get(i));
                        futures[i] = CompletableFuture.supplyAsync(
                                () -> executeOneTool(tc, iterations, paramMap, parentMessageId, ctx),
                                pool);
                    }

                    // Wait for all tools to finish, then collect results from the main thread
                    // (no synchronization needed on toolCallRecords since we add after allOf).
                    CompletableFuture.allOf(futures).join();
                    for (CompletableFuture<ToolExecResult> f : futures) {
                        try {
                            ToolExecResult r = f.get();
                            toolCallRecords.add(r.record);
                            if (r.modelResponse != null) nextModelResponse = r.modelResponse;
                        } catch (ExecutionException e) {
                            logger.error("RoomAgentHarness: tool execution threw unexpectedly", e.getCause());
                        }
                    }
                } finally {
                    pool.shutdown();
                }
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

    // Tool parsing

    /** Tool call fields parsed from the model response map. */
    @SuppressWarnings("unchecked")
    private static final class ParsedToolCall {
        final String toolCallId;
        final String rawToolName;
        final Map<String, Object> toolParams;

        ParsedToolCall(Map<String, Object> toolCall) {
            this.toolCallId  = String.valueOf(toolCall.get("id"));
            this.rawToolName = String.valueOf(toolCall.get("name"));
            // Some model providers use "input" instead of "arguments"
            Object argsObj = toolCall.get("arguments");
            if (argsObj == null) argsObj = toolCall.get("input");
            this.toolParams = (argsObj instanceof Map) ? (Map<String, Object>) argsObj : new HashMap<>();
        }
    }

    /** Paired result from executing one tool: the tracking record and the model response (if any). */
    private static final class ToolExecResult {
        final AgentHarnessResult.ToolCallRecord record;
        final AskModelEngineResponse<?> modelResponse;

        ToolExecResult(AgentHarnessResult.ToolCallRecord record, AskModelEngineResponse<?> modelResponse) {
            this.record = record;
            this.modelResponse = modelResponse;
        }
    }

    /**
     * Executes one tool call and submits the result to the Room.
     * Called from both the single-tool fast path and each parallel future.
     */
    private ToolExecResult executeOneTool(ParsedToolCall tc, int iter, Map<String, Object> paramMap,
                                          String parentMessageId, AgentRunContext ctx) {
        Room room = ctx.getRoom();
        logger.info("RoomAgentHarness executing tool: name={} callId={} iter={}",
                tc.rawToolName, tc.toolCallId, iter);
        long startMs = System.currentTimeMillis();
        ToolExecOutcome outcome = executeToolSafely(tc.rawToolName, tc.toolParams, ctx);
        long durationMs = System.currentTimeMillis() - startMs;
        logger.info("RoomAgentHarness tool result: name={} durationMs={} success={}",
                tc.rawToolName, durationMs, outcome.success);

        AgentHarnessResult.ToolCallRecord record = new AgentHarnessResult.ToolCallRecord(
                tc.rawToolName, tc.toolCallId, outcome.content, durationMs, outcome.success);

        // IMPORTANT: pass a fresh copy of paramMap - Room.appendToolsToParams() mutates it.
        AskModelEngineResponse<?> modelResponse = room.addToolExecutionResult(
                tc.toolCallId, tc.rawToolName, outcome.content, tc.toolParams,
                new HashMap<>(paramMap), parentMessageId,
                ctx.getModelEngine(), ctx.getInsight(),
                outcome.success ? TOOL_STATUS_SUCCESS : TOOL_STATUS_ERROR);

        return new ToolExecResult(record, modelResponse);
    }

    // Tool execution

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
                                              AgentRunContext ctx) {
        String[] parsed = MCPUtility.parseEngineIdFromFunctionName(rawToolName);
        if (parsed == null) {
            String msg = "Tool execution error: cannot parse engine/project id from tool name '"
                    + rawToolName + "'";
            logger.warn("RoomAgentHarness: {}", msg);
            return new ToolExecOutcome(msg, false);
        }
        String engineId = parsed[0];
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
                                         Map<String, Object> params, AgentRunContext ctx) {
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
                                     Map<String, Object> params, AgentRunContext ctx) {
        IEngine engine = null;
        try {
            engine = Utility.getEngine(engineId);
        } catch (Exception ignored) {
            // not an engine - try project
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
}
