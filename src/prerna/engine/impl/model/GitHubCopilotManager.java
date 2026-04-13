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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.copilot.sdk.CopilotClient;
import com.github.copilot.sdk.CopilotSession;
import com.github.copilot.sdk.SystemMessageMode;
import com.github.copilot.sdk.events.AssistantMessageDeltaEvent;
import com.github.copilot.sdk.events.AssistantMessageEvent;
import com.github.copilot.sdk.events.SystemNotificationEvent;
import com.github.copilot.sdk.json.CopilotClientOptions;
import com.github.copilot.sdk.json.MessageOptions;
import com.github.copilot.sdk.json.PermissionHandler;
import com.github.copilot.sdk.json.ProviderConfig;
import com.github.copilot.sdk.json.ResumeSessionConfig;
import com.github.copilot.sdk.json.SessionConfig;
import com.github.copilot.sdk.json.SystemMessageConfig;
import com.github.copilot.sdk.json.ToolDefinition;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.sablecc2.comm.PixelJobManager;

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
     * @param tools            ToolDefinitions built from room MCP tools
     * @param toolCallRecords  mutable list populated with per-tool trace records
     * @param workingDirectory optional filesystem path Copilot SDK uses as cwd;
     *                         pass null/blank to let the SDK use its default
     * @param roomId           SEMOSS room ID to reuse; encoded in the bearer token so
     *                         OpenAIEndpoints creates at most one Room row per agentic turn
     * @param insightId        SEMOSS insight ID to reuse; same rationale as roomId
     * @param roomFolderPath   path to the Room's folder on disk; passed as
     *                         --config-dir to the Copilot CLI so session state
     *                         (events.jsonl) is stored per-room, not in ~/.copilot/
     * @return final assistant text response, or null if no response received
     */
    public String query(
            Insight insight,
            User user,
            String engineId,
            String systemPrompt,
            String input,
            List<ToolDefinition> tools,
            List<AgentHarnessResult.ToolCallRecord> toolCallRecords,
            String workingDirectory,
            String roomId,
            String insightId,
            String roomFolderPath
    ) throws Exception {

        // Build base URL pointing at SEMOSS OpenAI-compatible endpoint.
        Integer localPort = ThreadStore.getLocalPort();
        String localProtocol = ThreadStore.getLocalProtocol();
        String baseUrl = localProtocol + "://localhost:" + localPort + "/Monolith/api/model/openai/v1/";

        // Temporal credentials - OpenAIFilter expects "Bearer accessKey:secretKey"
        // Optional 3rd segment "room-{parentRoomId}" links Copilot SDK turns to the parent room
        String[] keyPair = user.createCachedTemporalAccessSecretKey();
        String bearerToken = keyPair[0] + ":" + keyPair[1];
        if (roomId != null && !roomId.isBlank()) {
            bearerToken += ":room-" + roomId;
        }

        ProviderConfig provider = new ProviderConfig()
                .setType("openai")
                .setBaseUrl(baseUrl)
                .setBearerToken(bearerToken)
                .setWireApi("completions");

        // Embed SEMOSS context tag so OpenAIEndpoints can extract insight/room IDs
        // from the system message and reuse them across all internal LLM turns,
        // preventing a new Room row being created on each turn. Same pattern used
        // by AnthropicEndpoints / SemossContextExtractor.
        String contextTag = (insightId != null && !insightId.isBlank()
                            && roomId != null && !roomId.isBlank())
                ? "[[SEMOSS_CONTEXT:insightId=" + insightId + ",roomId=" + roomId + "]]"
                : null;
        String effectiveSystem = systemPrompt != null && !systemPrompt.isBlank() ? systemPrompt : "";
        if (contextTag != null) {
            effectiveSystem = contextTag + (effectiveSystem.isBlank() ? "" : "\n" + effectiveSystem);
        }

        SystemMessageConfig systemMessageConfig = null;
        if (!effectiveSystem.isBlank()) {
            systemMessageConfig = new SystemMessageConfig()
                    .setMode(SystemMessageMode.APPEND)
                    .setContent(effectiveSystem);
        }

        // Determine if a prior Copilot session exists for this room.
        // --config-dir points the CLI at the room folder, so session state lives
        // at {roomFolderPath}/session-state/{sessionId}/events.jsonl.
        // Using roomId as the sessionId gives a stable 1:1 mapping.
        String copilotSessionId = roomId;
        boolean hasPriorSession = false;
        if (roomFolderPath != null && !roomFolderPath.isBlank() && copilotSessionId != null) {
            File sessionEventsFile = new File(roomFolderPath,
                    "session-state" + File.separator + copilotSessionId + File.separator + "events.jsonl");
            hasPriorSession = sessionEventsFile.exists();
        }

        logger.debug("GitHubCopilotManager: engineId={} baseUrl={} tools={} hasPriorSession={}",
                engineId, baseUrl, tools != null ? tools.size() : 0, hasPriorSession);

        // Capture jobId on the calling thread - it is set in ThreadStore by the
        // async pixel job framework and is used to route partial output back to
        // the polling client via /engine/pixelJobStreaming.
        String jobId = ThreadStore.getJobId();

        // Pass --config-dir to the Copilot CLI so session state is stored
        // in the room folder rather than the shared ~/.copilot/ directory.
        CopilotClientOptions clientOptions = new CopilotClientOptions();
        if (roomFolderPath != null && !roomFolderPath.isBlank()) {
            clientOptions.setCliArgs(new String[]{"--config-dir", roomFolderPath});
        }

        try (CopilotClient client = new CopilotClient(clientOptions)) {
            client.start().get();

            CopilotSession session;
            if (hasPriorSession) {
                // Resume from prior session state persisted in the room folder.
                // The SDK reads events.jsonl and reconstructs full conversation context.
                ResumeSessionConfig resumeConfig = new ResumeSessionConfig()
                        .setModel(engineId)
                        .setProvider(provider)
                        .setStreaming(true)
                        .setOnPermissionRequest(PermissionHandler.APPROVE_ALL);
                if (workingDirectory != null && !workingDirectory.isBlank()) {
                    resumeConfig.setWorkingDirectory(workingDirectory);
                }
                if (systemMessageConfig != null) {
                    resumeConfig.setSystemMessage(systemMessageConfig);
                }
                if (tools != null && !tools.isEmpty()) {
                    resumeConfig.setTools(tools);
                }
                session = client.resumeSession(copilotSessionId, resumeConfig).get();
                logger.info("GitHubCopilotManager: resumed session {} for room {}", copilotSessionId, roomId);
            } else {
                // First run for this room -- create a fresh session.
                SessionConfig config = new SessionConfig()
                        .setModel(engineId)
                        .setProvider(provider)
                        .setStreaming(true)
                        .setOnPermissionRequest(PermissionHandler.APPROVE_ALL);
                if (workingDirectory != null && !workingDirectory.isBlank()) {
                    config.setWorkingDirectory(workingDirectory);
                }
                if (systemMessageConfig != null) {
                    config.setSystemMessage(systemMessageConfig);
                }
                if (tools != null && !tools.isEmpty()) {
                    config.setTools(tools);
                }
                // Use roomId as sessionId for stable 1:1 mapping; point configDir
                // at the room folder so events.jsonl persists across runs.
                if (copilotSessionId != null) {
                    config.setSessionId(copilotSessionId);
                }
                session = client.createSession(config).get();
                logger.info("GitHubCopilotManager: created session {} for room {}", copilotSessionId, roomId);
            }

            if (jobId != null) {
                // Token-level text streaming: emit each chunk as a "content" stream message.
                session.on(AssistantMessageDeltaEvent.class, evt -> {
                    String delta = evt.getData() != null ? evt.getData().deltaContent() : null;
                    if (delta != null && !delta.isEmpty()) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("content", delta);
                        Map<String, Object> msg = new HashMap<>();
                        msg.put("stream_type", "content");
                        msg.put("data", data);
                        PixelJobManager.getManager().addStreamOut(jobId, msg);
                    }
                });

                // Tool invocation signal: lets the UI show a "calling tool..." indicator.
                session.on(AssistantMessageEvent.class, evt -> {
                    var msgData = evt.getData();
                    if (msgData != null && msgData.toolRequests() != null && !msgData.toolRequests().isEmpty()) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("finish_reason", "tool_use");
                        Map<String, Object> msg = new HashMap<>();
                        msg.put("stream_type", "tool");
                        msg.put("data", data);
                        PixelJobManager.getManager().addStreamOut(jobId, msg);
                    }
                });

                // System notifications (e.g. "Running: npm install", "Created file x.ts").
                session.on(SystemNotificationEvent.class, evt -> {
                    String content = evt.getData() != null ? evt.getData().content() : null;
                    if (content != null && !content.isEmpty()) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("content", content);
                        Map<String, Object> msg = new HashMap<>();
                        msg.put("stream_type", "content");
                        msg.put("data", data);
                        PixelJobManager.getManager().addStreamOut(jobId, msg);
                    }
                });
            }

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

            String result = event.getData().content();

            // Push room folder (including session-state/) to cloud storage
            // so other nodes in the cluster can resume this session.
            if (roomId != null) {
                ClusterUtil.pushRoomAsync(roomId);
            }

            return result;
        }
    }
}
