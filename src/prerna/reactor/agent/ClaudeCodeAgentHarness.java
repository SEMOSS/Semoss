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
 * {@link IAgentHarness} that delegates to {@link ClaudeCodeManager}.
 *
 * <p>Resolves:
 * <ul>
 *   <li>{@code projectId} from {@code ctx.getFilePath()} or from {@code paramMap} under
 *       key {@code "project_id"}
 *   <li>MCP list from {@code room.getOptionsMap()} under key {@code "mcp"}
 *   <li>{@code allowedTools} from {@code paramMap} under key {@code "allowed_tools"};
 *       defaults to {@code ["*"]}
 *   <li>{@code permissionMode} from {@code paramMap} under key {@code "permission_mode"};
 *       defaults to {@code "default"}
 * </ul>
 *
 * <p>{@code ClaudeCodeManager} manages its own internal agentic loop, so this harness
 * returns {@code iterations = 0} and an empty tool-call trace.
 */
public class ClaudeCodeAgentHarness extends AppBuildingHarness {

    private static final Logger logger = LogManager.getLogger(ClaudeCodeAgentHarness.class);

    /** Registry name used by {@link AgentHarnessRegistry}. */
    public static final String NAME = "claude_code";

    private static final IMessageHook LOGGING_HOOK = new IMessageHook() {
        @Override
        public void beforeMessage(AgentRunContext ctx) {
            String input = ctx.getInput();
            logger.debug("[claude_code] pre-message: room={}, inputLen={}",
                    ctx.getRoom().getId(),
                    input == null ? 0 : input.length());
        }

        @Override
        public void afterMessage(AgentRunContext ctx, AgentHarnessResult result) {
            String finalText = result.getFinalText();
            logger.debug("[claude_code] post-message: room={}, iterations={}, finalTextLen={}",
                    ctx.getRoom().getId(),
                    result.getIterations(),
                    finalText == null ? 0 : finalText.length());
        }
    };

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Common hooks first, then this harness's own hooks. Drop the
     * {@code super} call to opt out of the common hooks; reorder the
     * {@code addAll} / {@code add} calls to control execution order.
     */
    @Override
    protected List<IMessageHook> getMessageHooks() {
        List<IMessageHook> all = new ArrayList<>(super.getMessageHooks());
        all.add(LOGGING_HOOK);
        return all;
    }

    @Override
    protected AgentHarnessResult doExecute(AgentRunContext ctx) throws Exception {
        Room                room   = ctx.getRoom();
        Map<String, Object> params = ctx.getParamMap();
        String              input  = ctx.getInput();
        String              filePath = ctx.getFilePath();

        String       engineId       = resolveEngineId(room);
        String       systemPrompt   = resolveSystemPrompt(room);
        List<String> allowedTools   = resolveAllowedTools(params, Collections.singletonList("*"));
        String       permissionMode = resolvePermissionMode(params);
        List<Map<String, String>> mcps = buildMcpList(room);
        User         user           = resolveUser(ctx.getInsight());

        logger.debug("ClaudeCodeAgentHarness: engine={} filePath={} mcps={}", engineId, filePath, mcps.size());
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

        return new AgentHarnessResult(output, 0, new ArrayList<>());
    }
}
