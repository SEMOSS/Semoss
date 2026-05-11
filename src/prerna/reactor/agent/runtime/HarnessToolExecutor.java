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
package prerna.reactor.agent.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.RunMCPToolReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Tool dispatch utilities for {@link SemossAgentHarness}.
 *
 * <p>Handles parallel execution of one or more tool calls from a single model response,
 * submits results back to the Room, and returns the next model {@link ResponseMessage}.
 */
final class HarnessToolExecutor {

    private static final Logger logger = LogManager.getLogger(HarnessToolExecutor.class);

    private static final String TOOL_STATUS_SUCCESS = "success";
    private static final String TOOL_STATUS_ERROR   = "error";

    private static final Gson GSON = new Gson();

    /** How often the parallel-batch wait polls for cancellation. */
    private static final long CANCEL_POLL_MS = 100L;

    private HarnessToolExecutor() {}

    /**
     * Executes all tool calls in {@code toolResponse} - single-threaded when there is one,
     * parallel otherwise - records results on {@code state}, and returns the next
     * {@link ResponseMessage} from the model, or {@code null} if the Room produced no follow-up.
     */
    @SuppressWarnings("unchecked")
    static ResponseMessage executeToolBatch(
            ResponseMessage toolResponse,
            AgentLoopState state,
            Map<String, Object> paramMap,
            AgentRunContext ctx) {

        Room room = ctx.getRoom();
        String parentMsgId = toolResponse.getMessageId();
        List<Map<String, Object>> toolCalls = toolResponse.getToolResponses();
        String jobId = ThreadStore.getJobId();
        AskModelEngineResponse<?> nextModelResp = null;

        if (toolCalls.size() == 1) {
            ParsedToolCall tc = new ParsedToolCall(toolCalls.get(0));
            ToolExecResult r  = executeOneTool(tc, state.getIterations(), paramMap, parentMsgId, ctx, jobId);
            state.addToolCallRecord(r.record);
            nextModelResp = r.modelResponse;

        } else {
            logger.info("HarnessToolExecutor: {} tools in parallel iter={} room={}",
                    toolCalls.size(), state.getIterations(), room.getId());

            ExecutorService pool = Executors.newFixedThreadPool(toolCalls.size());
            try {
                CompletableFuture<ToolExecResult>[] futures = new CompletableFuture[toolCalls.size()];
                for (int i = 0; i < toolCalls.size(); i++) {
                    final ParsedToolCall tc = new ParsedToolCall(toolCalls.get(i));
                    futures[i] = CompletableFuture.supplyAsync(
                            () -> executeOneTool(tc, state.getIterations(), paramMap, parentMsgId, ctx, jobId),
                            pool);
                }
                // Poll instead of allOf().join() so a cancel signal aborts the batch promptly.
                CompletableFuture<Void> all = CompletableFuture.allOf(futures);
                while (!all.isDone()) {
                    if (Thread.currentThread().isInterrupted()) {
                        for (CompletableFuture<ToolExecResult> f : futures) {
                            f.cancel(true);
                        }
                        throw new AgentCancelledException(
                                "Agent run cancelled during parallel tool batch (iter="
                                + state.getIterations() + ")");
                    }
                    try {
                        all.get(CANCEL_POLL_MS, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException ignored) {
                        // keep polling
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        // loop back to the isInterrupted() branch, which cancels and throws.
                    } catch (ExecutionException ignored) {
                        // per-future failures are surfaced in the f.get() loop below.
                    }
                }
                for (CompletableFuture<ToolExecResult> f : futures) {
                    if (f.isCancelled()) continue;
                    try {
                        ToolExecResult r = f.get();
                        state.addToolCallRecord(r.record);
                        if (r.modelResponse != null) nextModelResp = r.modelResponse;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.warn("HarnessToolExecutor: interrupted waiting for tool future");
                    } catch (ExecutionException e) {
                        logger.error("HarnessToolExecutor: tool future threw unexpectedly", e.getCause());
                    }
                }
            } finally {
                pool.shutdown();
            }
        }

        if (nextModelResp == null) {
            return null;
        }
        Object lastMsg = room.getMessages().getLast();
        if (lastMsg instanceof ResponseMessage) {
            return (ResponseMessage) lastMsg;
        }
        logger.warn("HarnessToolExecutor: last message after tool batch is not ResponseMessage: {}",
                lastMsg == null ? "null" : lastMsg.getClass().getName());
        return null;
    }

    /**
     * Executes one tool call and submits the result to the Room.
     * Safe to call concurrently - Room.addToolExecutionResult() is synchronized.
     */
    private static ToolExecResult executeOneTool(
            ParsedToolCall tc,
            int currentIter,
            Map<String, Object> paramMap,
            String parentMsgId,
            AgentRunContext ctx,
            String jobId) {

        logger.info("HarnessToolExecutor: tool start name={} callId={} iter={}",
                tc.rawToolName, tc.toolCallId, currentIter);
        SemossAgentStream.toolInvocation(jobId, tc.toolCallId, tc.rawToolName, tc.toolParams, tc.toolCall);

        long startMs = System.currentTimeMillis();
        ToolExecOutcome outcome = executeToolSafely(tc.rawToolName, tc.toolParams, ctx);
        long durMs = System.currentTimeMillis() - startMs;
        SemossAgentStream.toolResult(jobId, tc.toolCallId, tc.rawToolName, outcome.success, durMs, outcome.content,
                tc.toolParams, tc.toolCall);

        logger.info("HarnessToolExecutor: tool end name={} durationMs={} success={}",
                tc.rawToolName, durMs, outcome.success);

        AgentHarnessResult.ToolCallRecord record = new AgentHarnessResult.ToolCallRecord(
                tc.rawToolName, tc.toolCallId, outcome.content, durMs, outcome.success);

        // Pass a fresh copy - Room.appendToolsToParams() mutates the map.
        AskModelEngineResponse<?> modelResp = ctx.getRoom().addToolExecutionResult(
                tc.toolCallId, tc.rawToolName, outcome.content, tc.toolParams,
                new HashMap<>(paramMap), parentMsgId,
                ctx.getModelEngine(), ctx.getInsight(),
                outcome.success ? TOOL_STATUS_SUCCESS : TOOL_STATUS_ERROR);

        return new ToolExecResult(record, modelResp);
    }

    private static ToolExecOutcome executeToolSafely(
            String rawToolName, Map<String, Object> params, AgentRunContext ctx) {

        String[] parsed = MCPUtility.parseEngineIdFromFunctionName(rawToolName);
        if (parsed == null) {
            String msg = "Tool execution error: cannot parse engine/project id from tool name '"
                    + rawToolName + "'";
            logger.warn("HarnessToolExecutor: {}", msg);
            return new ToolExecOutcome(msg, false);
        }
        try {
            String result = callMcpToolViaReactor(rawToolName, parsed[0], params, ctx);
            boolean success = result == null || !result.startsWith("Tool execution error:");
            return new ToolExecOutcome(result, success);
        } catch (Exception e) {
            String msg = "Tool execution error: " + e.getMessage();
            logger.warn("HarnessToolExecutor: uncaught exception from tool '{}': {}",
                    rawToolName, e.getMessage(), e);
            return new ToolExecOutcome(msg, false);
        }
    }

    private static String callMcpToolViaReactor(
            String rawToolName, String engineId, Map<String, Object> params, AgentRunContext ctx) {
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

    // Internal value types

    @SuppressWarnings("unchecked")
    static final class ParsedToolCall {
        final String              toolCallId;
        final String              rawToolName;
        final Map<String, Object> toolParams;
        final Map<String, Object> toolCall;

        ParsedToolCall(Map<String, Object> toolCall) {
            this.toolCall = toolCall != null ? new HashMap<>(toolCall) : new HashMap<>();
            this.toolCallId  = String.valueOf(toolCall.get("id"));
            this.rawToolName = String.valueOf(toolCall.get("name"));
            // Providers vary: Anthropic uses "input" (Map), OpenAI/Responses-style uses "arguments"
            // which can arrive as a Map OR as a JSON-encoded String. Mirror the parsing in
            // AskToolModelEngineResponse so we don't drop string-encoded payloads.
            Object argsObj = toolCall.get("arguments");
            if (argsObj == null) argsObj = toolCall.get("input");
            Map<String, Object> parsed = null;
            if (argsObj == null) {
                parsed = new HashMap<>();
            } else if (argsObj instanceof Map) {
                parsed = (Map<String, Object>) argsObj;
            } else {
                String json = argsObj.toString();
                try {
                    parsed = GSON.fromJson(json, Map.class);
                } catch (Exception e) {
                    logger.warn("HarnessToolExecutor: failed to parse tool arguments JSON for tool '{}': {}",
                            this.rawToolName, e.getMessage());
                }
            }
            this.toolParams = parsed != null ? parsed : new HashMap<>();
        }
    }

    static final class ToolExecResult {
        final AgentHarnessResult.ToolCallRecord record;
        final AskModelEngineResponse<?>         modelResponse;

        ToolExecResult(AgentHarnessResult.ToolCallRecord record, AskModelEngineResponse<?> modelResponse) {
            this.record        = record;
            this.modelResponse = modelResponse;
        }
    }

    private static final class ToolExecOutcome {
        final String  content;
        final boolean success;

        ToolExecOutcome(String content, boolean success) {
            this.content = content;
            this.success = success;
        }
    }
}
