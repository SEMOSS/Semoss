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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.copilot.sdk.json.ToolDefinition;

import prerna.auth.User;
import prerna.engine.impl.model.GitHubCopilotManager;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.RunMCPToolReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * {@link IAgentHarness} implementation that delegates to the GitHub Copilot SDK.
 *
 * <p>Uses BYOK to route model calls through SEMOSS's OpenAI-compatible endpoint
 * ({@code OpenAIEndpoints.java} in Monolith), authenticating with a temporal
 * access/secret key pair. The SEMOSS model engine ID is passed as the
 * {@code model} field so the endpoint routes to the correct engine.
 *
 * <p>SEMOSS MCP tools registered in the room are bridged into the Copilot session
 * as {@link ToolDefinition} handlers. Each handler calls {@link RunMCPToolReactor}
 * (same execution path as {@link RoomAgentHarness}).
 *
 * <p>The Copilot SDK manages its own internal agentic loop, so this harness returns
 * {@code iterations = 0}. Tool call records are collected inside each ToolDefinition
 * handler where the tool name and timing are available.
 */
public class GitHubCopilotAgentHarness implements IAgentHarness {

    private static final Logger logger = LogManager.getLogger(GitHubCopilotAgentHarness.class);

    /** Registry name used by {@link AgentHarnessRegistry}. */
    public static final String NAME = "github_copilot";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AgentHarnessResult execute(GenericAgentContext ctx) throws Exception {
        Room room = ctx.getRoom();
        String input = ctx.getInput();

        // Require model engine ID
        String engineId = room.getModelId();
        if (engineId == null || engineId.isBlank()) {
            throw new IllegalArgumentException(
                    "GitHubCopilotAgentHarness: room does not have a modelId set");
        }

        // Require user
        User user = ctx.getInsight().getUser();
        if (user == null) {
            throw new IllegalArgumentException(
                    "GitHubCopilotAgentHarness: insight has no user");
        }

        String systemPrompt = room.getEffectiveSystemPrompt();
        if (systemPrompt == null) systemPrompt = "";

        // Bridge SEMOSS MCP tools to Copilot ToolDefinitions.
        // Records are populated inside each handler where we have full context.
        List<AgentHarnessResult.ToolCallRecord> toolCallRecords = new ArrayList<>();
        List<ToolDefinition> tools = buildToolDefinitions(room, toolCallRecords, ctx);

        logger.debug("GitHubCopilotAgentHarness: engineId={} tools={}", engineId, tools.size());

        // GenericAgent resolves filePath from paramValues.project via
        // AssetUtility.getProjectAssetsFolder(projectId). Pass it as workingDirectory
        // so the Copilot SDK knows where to read/write files for the target project.
        String workingDirectory = ctx.getFilePath();

        String roomId = room.getId();
        String insightId = ctx.getInsight().getInsightId();
        String roomFolderPath = room.getRoomFolderPath();

        GitHubCopilotManager manager = new GitHubCopilotManager();
        String output = manager.query(
                ctx.getInsight(),
                user,
                engineId,
                systemPrompt,
                input,
                tools,
                toolCallRecords,
                workingDirectory,
                roomId,
                insightId,
                roomFolderPath
        );

        // SDK manages internal loop; iterations=0 like ClaudeCodeAgentHarness
        return new AgentHarnessResult(output, 0, toolCallRecords);
    }

    @SuppressWarnings("unchecked")
    private List<ToolDefinition> buildToolDefinitions(
            Room room,
            List<AgentHarnessResult.ToolCallRecord> toolCallRecords,
            GenericAgentContext ctx) {

        List<Map<String, Object>> semossTools = room.getAllToolsJsonForRoom(Integer.MAX_VALUE);
        if (semossTools == null || semossTools.isEmpty()) {
            return Collections.emptyList();
        }

        List<ToolDefinition> result = new ArrayList<>();
        for (Map<String, Object> toolJson : semossTools) {
            String name = String.valueOf(toolJson.get("name"));
            String description = toolJson.containsKey("description")
                    ? String.valueOf(toolJson.get("description")) : "";
            Map<String, Object> schema = toolJson.containsKey("parameters")
                    ? (Map<String, Object>) toolJson.get("parameters") : new HashMap<>();

            final String rawToolName = name;
            result.add(ToolDefinition.create(name, description, schema, invocation -> {
                Map<String, Object> args = invocation.getArguments();
                long startMs = System.currentTimeMillis();
                String toolResult = callMcpToolViaReactor(rawToolName, args, ctx);
                long durationMs = System.currentTimeMillis() - startMs;
                boolean success = toolResult == null || !toolResult.startsWith("Tool execution error:");
                toolCallRecords.add(new AgentHarnessResult.ToolCallRecord(
                        rawToolName, invocation.getToolCallId(), toolResult, durationMs, success));
                return CompletableFuture.completedFuture(toolResult);
            }));
        }
        return result;
    }

    /** Same Reactor execution path as {@link RoomAgentHarness}. */
    private String callMcpToolViaReactor(String rawToolName, Map<String, Object> params,
                                          GenericAgentContext ctx) {
        try {
            String[] parsed = MCPUtility.parseEngineIdFromFunctionName(rawToolName);
            if (parsed == null) {
                String msg = "Tool execution error: cannot parse engine id from '" + rawToolName + "'";
                logger.warn("GitHubCopilotAgentHarness: {}", msg);
                return msg;
            }
            String engineId = parsed[0];

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

            NounMetadata res = reactor.execute();
            return res != null && res.getValue() != null ? res.getValue().toString() : "";
        } catch (Exception e) {
            String msg = "Tool execution error: " + e.getMessage();
            logger.warn("GitHubCopilotAgentHarness: uncaught exception from tool '{}': {}",
                    rawToolName, e.getMessage(), e);
            return msg;
        }
    }
}
