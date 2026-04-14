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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.GitHubCopilotManager;
import prerna.engine.impl.model.Room;

/**
 * {@link IAgentHarness} implementation that delegates to the GitHub Copilot SDK.
 *
 * <p>Uses BYOK to route model calls through SEMOSS's OpenAI-compatible endpoint
 * ({@code OpenAIEndpoints.java} in Monolith), authenticating with a temporal
 * access/secret key pair. The SEMOSS model engine ID is passed as the
 * {@code model} field so the endpoint routes to the correct engine.
 *
 * <p>SEMOSS MCP tools registered in the room are connected as native HTTP MCP
 * servers via the {@code /api/ext/mcp/{id}/comms} endpoint, following the same
 * pattern as {@link ClaudeCodeAgentHarness}.
 *
 * <p>The Copilot SDK manages its own internal agentic loop, so this harness returns
 * {@code iterations = 0}.
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

        // Build MCP list from room options -- same pattern as ClaudeCodeAgentHarness
        List<Map<String, String>> mcps = buildMcpList(room);

        // GenericAgent resolves filePath from paramValues.project via
        // AssetUtility.getProjectAssetsFolder(projectId). Pass it as workingDirectory
        // so the Copilot SDK knows where to read/write files for the target project.
        String workingDirectory = ctx.getFilePath();
        String roomId = room.getId();
        String roomFolderPath = room.getRoomFolderPath();

        logger.debug("GitHubCopilotAgentHarness: engineId={} mcps={}", engineId, mcps.size());

        GitHubCopilotManager manager = new GitHubCopilotManager();
        String output = manager.query(
                ctx.getInsight(),
                user,
                engineId,
                systemPrompt,
                input,
                mcps,
                workingDirectory,
                roomId,
                roomFolderPath
        );

        // SDK manages internal loop; iterations=0 like ClaudeCodeAgentHarness
        return new AgentHarnessResult(output, 0, new ArrayList<>());
    }

    /**
     * Builds MCP engine list from room options.
     * Same pattern as {@link ClaudeCodeAgentHarness#buildMcpList(Room)}.
     */
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
