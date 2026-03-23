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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.ClaudeCodeManager;
import prerna.engine.impl.model.Room;

/**
 * {@link IAgentHarness} implementation that delegates to {@link ClaudeCodeManager}.
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
 * <p>{@code ClaudeCodeManager} manages its own internal agentic loop (file read/write/edit
 * tools), so this harness returns {@code iterations = 0} and an empty tool-call trace.
 */
public class ClaudeCodeAgentHarness implements IAgentHarness {

    private static final Logger logger = LogManager.getLogger(ClaudeCodeAgentHarness.class);

    /** Registry name used by {@link AgentHarnessRegistry}. */
    public static final String NAME = "claude_code";

    private static final String PARAM_ALLOWED_TOOLS  = "allowed_tools";
    private static final String PARAM_PERMISSION_MODE = "permission_mode";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AgentHarnessResult execute(GenericAgentContext ctx) throws Exception {
        Room               room    = ctx.getRoom();
        Map<String, Object> params = ctx.getParamMap();
        String             input   = ctx.getInput();

        String filePath = ctx.getFilePath();

        //Resolve model engine ID
        String engineId = room.getModelId();
        if (engineId == null || engineId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "ClaudeCodeAgentHarness: room does not have a modelId set");
        }

        //Resolve system prompt
        String systemPrompt = room.getEffectiveSystemPrompt();
        if (systemPrompt == null) systemPrompt = "";

        //Resolve allowed tools
        List<String> allowedTools;
        Object allowedToolsObj = params.get(PARAM_ALLOWED_TOOLS);
        if (allowedToolsObj instanceof List) {
            allowedTools = (List<String>) allowedToolsObj;
        } else {
            allowedTools = Collections.singletonList("*");
        }

        // Resolve permission mode
        String permissionMode = params.containsKey(PARAM_PERMISSION_MODE)
                ? String.valueOf(params.get(PARAM_PERMISSION_MODE))
                : "default";

        // Build MCP list from room options
        List<Map<String, String>> mcps = buildMcpList(room);

        // Resolve User
        User user = ctx.getInsight().getUser();
        if (user == null) {
            throw new IllegalArgumentException("ClaudeCodeAgentHarness: insight has no user");
        }

        // Delegate to ClaudeCodeManager
        logger.debug("ClaudeCodeAgentHarness: engine={} filePath={} mcps={}", engineId, filePath, mcps.size());
        ClaudeCodeManager manager = new ClaudeCodeManager();
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
                mcps);

        return new AgentHarnessResult(output, 0, new ArrayList<>());
    }

    // Helpers

    @SuppressWarnings("unchecked")
    private List<Map<String, String>> buildMcpList(Room room) {
        List<Map<String, String>> result = new ArrayList<>();
        Map<String, Object> opts = room.getOptionsMap();
        if (opts == null || !opts.containsKey("mcp")) return result;
        Object mcpObj = opts.get("mcp");
        if (!(mcpObj instanceof List)) return result;
        List<?> mcpList = (List<?>) mcpObj;
        for (Object item : mcpList) {
            if (!(item instanceof Map)) continue;
            Map<String, Object> mcpEntry = (Map<String, Object>) item;
            String id   = mcpEntry.containsKey("id")   ? String.valueOf(mcpEntry.get("id"))   : null;
            String name = mcpEntry.containsKey("name") ? String.valueOf(mcpEntry.get("name")) : id;
            if (id != null) {
                Map<String, String> entry = new HashMap<>();
                entry.put("id",   id);
                entry.put("name", name != null ? name : id);
                result.add(entry);
            }
        }
        return result;
    }
}
