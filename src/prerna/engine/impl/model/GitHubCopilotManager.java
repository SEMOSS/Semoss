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
package prerna.engine.impl.model;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.copilot.sdk.CopilotClient;
import com.github.copilot.sdk.CopilotSession;
import com.github.copilot.sdk.events.AssistantMessageEvent;
import com.github.copilot.sdk.SystemMessageMode;
import com.github.copilot.sdk.json.MessageOptions;
import com.github.copilot.sdk.json.PermissionHandler;
import com.github.copilot.sdk.json.ProviderConfig;
import com.github.copilot.sdk.json.SessionConfig;
import com.github.copilot.sdk.json.SystemMessageConfig;
import com.github.copilot.sdk.json.ToolDefinition;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;

/**
 * Manages a single GitHub Copilot SDK session pointed at SEMOSS's
 * OpenAI-compatible endpoint via BYOK.
 *
 * The model engine is selected by setting the {@code model} field in
 * {@link SessionConfig} to the SEMOSS engine ID. The
 * {@code OpenAIFilter} in Monolith reads the {@code Authorization: Bearer
 * accessKey:secretKey} header and maps it to the requesting user.
 *
 * Modeled after {@link ClaudeCodeManager} but pure Java - no Python process.
 */
public class GitHubCopilotManager {

    private static final Logger logger = LogManager.getLogger(GitHubCopilotManager.class);

    /**
     * Runs one agentic turn using the GitHub Copilot SDK pointed at SEMOSS's
     * OpenAI-compatible endpoint (BYOK).
     *
     * @param insight         user insight (for security context)
     * @param user            authenticated SEMOSS user
     * @param engineId        SEMOSS model engine ID sent as the "model" field
     * @param systemPrompt    effective system prompt from room (may be empty)
     * @param input           user prompt text
     * @param tools           ToolDefinitions built from room MCP tools
     * @param toolCallRecords mutable list populated with per-tool trace records
     * @return final assistant text response, or null if no response received
     */
    public String query(
            Insight insight,
            User user,
            String engineId,
            String systemPrompt,
            String input,
            List<ToolDefinition> tools,
            List<AgentHarnessResult.ToolCallRecord> toolCallRecords
    ) throws Exception {

        // Build base URL pointing at SEMOSS OpenAI-compatible endpoint.
        // OpenAIFilter now supports temporal keys via LocalUserStore in addition to
        // permanent SecurityDB keys, same Bearer accessKey:secretKey format.
        Integer localPort = ThreadStore.getLocalPort();
        String localProtocol = ThreadStore.getLocalProtocol();
        String baseUrl = localProtocol + "://localhost:" + localPort + "/Monolith/api/model/openai/v1/";

        // Temporal credentials - OpenAIFilter expects "Bearer accessKey:secretKey"
        String[] keyPair = user.createCachedTemporalAccessSecretKey();
        String bearerToken = keyPair[0] + ":" + keyPair[1];

        ProviderConfig provider = new ProviderConfig()
                .setType("openai")
                .setBaseUrl(baseUrl)
                .setBearerToken(bearerToken)
                .setWireApi("completions");

        SessionConfig config = new SessionConfig()
                .setModel(engineId)
                .setProvider(provider)
                .setOnPermissionRequest(PermissionHandler.APPROVE_ALL);

        if (systemPrompt != null && !systemPrompt.isBlank()) {
            config.setSystemMessage(new SystemMessageConfig()
                    .setMode(SystemMessageMode.APPEND)
                    .setContent(systemPrompt));
        }
        if (tools != null && !tools.isEmpty()) {
            config.setTools(tools);
        }

        logger.debug("GitHubCopilotManager: engineId={} baseUrl={} tools={}",
                engineId, baseUrl, tools != null ? tools.size() : 0);

        try (CopilotClient client = new CopilotClient()) {
            client.start().get();
            CopilotSession session = client.createSession(config).get();

            // Pass timeoutMs=0 to disable the SDK's default 60-second hard timeout.
            // SEMOSS's own reactor/request timeout governs the upper bound instead.
            AssistantMessageEvent event = session.sendAndWait(
                    new MessageOptions().setPrompt(input), 0
            ).get();

            session.close();

            if (event == null || event.getData() == null) {
                logger.warn("GitHubCopilotManager: sendAndWait returned null event for engineId={}", engineId);
                return null;
            }

            return event.getData().content();
        }
    }
}
