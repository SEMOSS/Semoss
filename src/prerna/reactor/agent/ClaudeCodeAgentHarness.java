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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.ClaudeCodeManager;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.sandbox.AgentSandboxConfig;
import prerna.reactor.agent.sandbox.SandboxPolicy;

/**
 * {@link IAgentHarness} that delegates to {@link ClaudeCodeManager} - spawns the
 * external {@code claude} CLI in the agent's working directory and parses its
 * transcript.
 *
 * <p>Reads from {@link AgentRunContext}:
 * <ul>
 *   <li>working dir - {@code ctx.getFilePath()} (raw - no {@code /client} suffix
 *       imposed; callers wanting that pass {@code subdir="client"} to {@code RunAgent})</li>
 *   <li>system prompt - {@code ctx.getRoom().getRoomOrWorkspaceSystemPrompt()} (raw
 *       authored layer; no enterprise-template wrap, no {@code {{VAR}}} expansion -
 *       the external CLI gets the agent's prompt as-authored)</li>
 *   <li>engine id - {@code ctx.getRoom().getModelId()}</li>
 *   <li>MCP list - {@code ctx.getAgentConfig().getMcps()} (union of
 *       {@code WORKSPACE_RESOURCE} + {@code room.options.mcp[]})</li>
 *   <li>{@code allowed_tools}, {@code permission_mode} - read inline from paramMap</li>
 * </ul>
 *
 * <p>{@code ClaudeCodeManager} manages its own internal agentic loop, so this harness
 * returns {@code iterations = 0} and an empty tool-call trace.
 */
public class ClaudeCodeAgentHarness implements IAgentHarness {

    private static final Logger logger = LogManager.getLogger(ClaudeCodeAgentHarness.class);

    /** Registry name used by {@link AgentHarnessRegistry}. */
    public static final String NAME = "claude_code";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
        Room                room     = ctx.getRoom();
        Map<String, Object> params   = ctx.getParamMap();
        String              input    = ctx.getInput();
        String              filePath = ctx.getFilePath();

        String       engineId       = room.getModelId();
        if (engineId == null || engineId.trim().isEmpty()) {
            throw new IllegalArgumentException(NAME + ": room does not have a modelId set");
        }
        String       systemPrompt   = room.getRoomOrWorkspaceSystemPrompt();
        if (systemPrompt == null) {
            systemPrompt = "";
        }
        User         user           = ctx.getInsight().getUser();
        if (user == null) {
            throw new IllegalArgumentException(NAME + ": insight has no user");
        }
        List<String> allowedTools   = resolveAllowedTools(params, Collections.singletonList("*"));
        String       permissionMode = resolvePermissionMode(params);
        List<Map<String, String>> mcps = ctx.getAgentConfig().getMcps();

        logger.debug("[{}] pre-message: room={}, inputLen={}",
                NAME, room.getId(), input == null ? 0 : input.length());
        logger.debug("ClaudeCodeAgentHarness: engine={} filePath={} mcps={}",
                engineId, filePath, mcps == null ? 0 : mcps.size());

        ClaudeCodeManager manager = new ClaudeCodeManager();
        String targetBinary = ClaudeCodeManager.resolveClaudeBinary();
        SandboxPolicy policy = AgentSandboxConfig.buildEffectivePolicy(
                room.getRoomFolderPath(), filePath, targetBinary, ctx.getSandboxPolicy());
        String output = manager.query(
                ctx.getInsight(),
                user,
                engineId,
                filePath,
                input,
                systemPrompt,
                room.getId(),
                allowedTools,
                permissionMode,
                mcps,
                policy);

        AgentHarnessResult result = new AgentHarnessResult(output, 0, new ArrayList<>());
        logger.debug("[{}] post-message: room={}, iterations={}, finalTextLen={}",
                NAME, room.getId(), result.getIterations(),
                output == null ? 0 : output.length());
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<String> resolveAllowedTools(Map<String, Object> params, List<String> defaults) {
        if (params == null) {
            return defaults;
        }
        Object o = params.get("allowed_tools");
        if (o instanceof List) {
            return (List<String>) o;
        }
        return defaults;
    }

    private static String resolvePermissionMode(Map<String, Object> params) {
        if (params == null || !params.containsKey("permission_mode")) {
            return "default";
        }
        return String.valueOf(params.get("permission_mode"));
    }
}
