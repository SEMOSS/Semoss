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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.copilot.sdk.CopilotClient;
import com.github.copilot.sdk.CopilotSession;
import com.github.copilot.sdk.SystemMessageMode;
import com.github.copilot.sdk.events.AbstractSessionEvent;
import com.github.copilot.sdk.events.AssistantIntentEvent;
import com.github.copilot.sdk.events.AssistantMessageDeltaEvent;
import com.github.copilot.sdk.events.AssistantMessageEvent;
import com.github.copilot.sdk.events.AssistantReasoningDeltaEvent;
import com.github.copilot.sdk.events.AssistantReasoningEvent;
import com.github.copilot.sdk.events.AssistantTurnEndEvent;
import com.github.copilot.sdk.events.AssistantTurnStartEvent;
import com.github.copilot.sdk.events.AssistantUsageEvent;
import com.github.copilot.sdk.events.SessionErrorEvent;
import com.github.copilot.sdk.events.SessionIdleEvent;
import com.github.copilot.sdk.events.ToolExecutionCompleteEvent;
import com.github.copilot.sdk.events.ToolExecutionPartialResultEvent;
import com.github.copilot.sdk.events.ToolExecutionProgressEvent;
import com.github.copilot.sdk.events.ToolExecutionStartEvent;
import com.github.copilot.sdk.json.CopilotClientOptions;
import com.github.copilot.sdk.json.MessageOptions;
import com.github.copilot.sdk.json.PermissionHandler;
import com.github.copilot.sdk.json.ProviderConfig;
import com.github.copilot.sdk.json.ResumeSessionConfig;
import com.github.copilot.sdk.json.SessionConfig;
import com.github.copilot.sdk.json.SystemMessageConfig;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.util.Utility;

/**
 * GitHub Copilot SDK manager that keeps disk-backed session state in the room
 * folder while publishing normalized transcript events into pixelJobStreaming.
 */
public class GitHubCopilotManager {

	private static final Logger classLogger = LogManager.getLogger(GitHubCopilotManager.class);
	private static final String CLIENT_NAME = "SEMOSS Agent47";

	public String query(Insight insight, User user, String engineId, String filePath, String prompt, String systemPrompt,
			String roomId, List<String> allowedTools, String permissionMode, List<Map<String, String>> mcps)
			throws Exception {
		String roomFolderPath = Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
		Files.createDirectories(Paths.get(roomFolderPath));

		String workingDirectory = (filePath != null && !filePath.trim().isEmpty()) ? filePath : roomFolderPath;
		Files.createDirectories(Paths.get(workingDirectory));

		String[] keyPair = user.createCachedTemporalAccessSecretKey();
		String bearerToken = buildBearerToken(keyPair[0], keyPair[1], roomId);
		String baseUrl = buildOpenAIBaseUrl();
		String runId = UUID.randomUUID().toString();
		GitHubCopilotPixelJobStreamer streamer = new GitHubCopilotPixelJobStreamer(ThreadStore.getJobId(), engineId,
				runId);

		AtomicBoolean terminalErrorSeen = new AtomicBoolean(false);
		AtomicBoolean idlePublished = new AtomicBoolean(false);
		AtomicReference<String> sessionErrorMessage = new AtomicReference<>(null);

		CopilotClientOptions clientOptions = new CopilotClientOptions();
		clientOptions.setCliArgs(new String[] { "--config-dir", roomFolderPath });

		try (CopilotClient client = new CopilotClient(clientOptions)) {
			client.start().get();

			String lastSessionId = null;
			try {
				lastSessionId = client.getLastSessionId().get();
			} catch (Exception e) {
				classLogger.debug("Unable to get last Copilot session id for room {}", roomId, e);
			}

			SessionConfig sessionConfig = buildSessionConfig(engineId, workingDirectory, roomFolderPath, systemPrompt,
					bearerToken, baseUrl, allowedTools, permissionMode, mcps, streamer, terminalErrorSeen,
					idlePublished, sessionErrorMessage);
			ResumeSessionConfig resumeConfig = buildResumeSessionConfig(engineId, workingDirectory, roomFolderPath,
					systemPrompt, bearerToken, baseUrl, allowedTools, permissionMode, mcps, streamer,
					terminalErrorSeen, idlePublished, sessionErrorMessage);

			try (CopilotSession session = openSession(client, lastSessionId, roomFolderPath, sessionConfig, resumeConfig)) {
				AssistantMessageEvent finalMessage = session
						.sendAndWait(new MessageOptions().setPrompt(prompt), 0)
						.get();
				if (sessionErrorMessage.get() != null) {
					throw new IllegalStateException(sessionErrorMessage.get());
				}
				return finalMessage != null && finalMessage.getData() != null ? finalMessage.getData().content() : "";
			}
		}
	}

	private CopilotSession openSession(CopilotClient client, String lastSessionId, String roomFolderPath,
			SessionConfig sessionConfig, ResumeSessionConfig resumeConfig) throws Exception {
		if (lastSessionId != null && !lastSessionId.trim().isEmpty() && hasPersistedSessionState(roomFolderPath)) {
			try {
				return client.resumeSession(lastSessionId, resumeConfig).get();
			} catch (Exception e) {
				classLogger.warn("Falling back to new Copilot session after resume failed for {}", lastSessionId, e);
			}
		}
		return client.createSession(sessionConfig).get();
	}

	private boolean hasPersistedSessionState(String roomFolderPath) {
		Path sessionState = Paths.get(roomFolderPath, "session-state");
		return Files.exists(sessionState) && Files.isDirectory(sessionState);
	}

	private SessionConfig buildSessionConfig(String engineId, String workingDirectory, String roomFolderPath,
			String systemPrompt, String bearerToken, String baseUrl, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps, GitHubCopilotPixelJobStreamer streamer,
			AtomicBoolean terminalErrorSeen, AtomicBoolean idlePublished,
			AtomicReference<String> sessionErrorMessage) {
		SessionConfig config = new SessionConfig();
		config.setClientName(CLIENT_NAME);
		config.setModel(engineId);
		config.setWorkingDirectory(workingDirectory);
		config.setConfigDir(roomFolderPath);
		config.setStreaming(true);
		config.setProvider(buildProviderConfig(baseUrl, bearerToken));
		config.setOnPermissionRequest(resolvePermissionHandler(permissionMode));
		config.setMcpServers(buildMcpServers(mcps, bearerToken));
		if (allowedTools != null && !allowedTools.isEmpty()) {
			config.setAvailableTools(allowedTools);
		}
		if (systemPrompt != null && !systemPrompt.trim().isEmpty()) {
			config.setSystemMessage(new SystemMessageConfig().setMode(SystemMessageMode.APPEND).setContent(systemPrompt));
		}
		config.setOnEvent(
			event -> publishEvent(event, streamer, terminalErrorSeen, idlePublished, sessionErrorMessage));
		return config;
	}

	private ResumeSessionConfig buildResumeSessionConfig(String engineId, String workingDirectory, String roomFolderPath,
			String systemPrompt, String bearerToken, String baseUrl, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps, GitHubCopilotPixelJobStreamer streamer,
			AtomicBoolean terminalErrorSeen, AtomicBoolean idlePublished,
			AtomicReference<String> sessionErrorMessage) {
		ResumeSessionConfig config = new ResumeSessionConfig();
		config.setClientName(CLIENT_NAME);
		config.setModel(engineId);
		config.setWorkingDirectory(workingDirectory);
		config.setConfigDir(roomFolderPath);
		config.setStreaming(true);
		config.setProvider(buildProviderConfig(baseUrl, bearerToken));
		config.setOnPermissionRequest(resolvePermissionHandler(permissionMode));
		config.setMcpServers(buildMcpServers(mcps, bearerToken));
		if (allowedTools != null && !allowedTools.isEmpty()) {
			config.setAvailableTools(allowedTools);
		}
		if (systemPrompt != null && !systemPrompt.trim().isEmpty()) {
			config.setSystemMessage(new SystemMessageConfig().setMode(SystemMessageMode.APPEND).setContent(systemPrompt));
		}
		config.setOnEvent(
			event -> publishEvent(event, streamer, terminalErrorSeen, idlePublished, sessionErrorMessage));
		return config;
	}

	private ProviderConfig buildProviderConfig(String baseUrl, String bearerToken) {
		return new ProviderConfig().setType("openai").setWireApi("completions").setBaseUrl(baseUrl)
				.setBearerToken(bearerToken);
	}

	private PermissionHandler resolvePermissionHandler(String permissionMode) {
		return PermissionHandler.APPROVE_ALL;
	}

	private Map<String, Object> buildMcpServers(List<Map<String, String>> mcps, String bearerToken) {
		if (mcps == null || mcps.isEmpty()) {
			return Collections.emptyMap();
		}

		Integer localPort = ThreadStore.getLocalPort();
		String localProtocol = ThreadStore.getLocalProtocol();
		if (localPort == null || localProtocol == null || localProtocol.trim().isEmpty()) {
			return Collections.emptyMap();
		}

		String mcpBaseUrl = localProtocol + "://localhost:" + localPort + "/Monolith/api/ext/mcp/";
		Map<String, Object> servers = new LinkedHashMap<>();
		for (Map<String, String> mcp : mcps) {
			String id = mcp.get("id");
			if (id == null || id.trim().isEmpty()) {
				continue;
			}
			String name = mcp.get("name") != null && !mcp.get("name").trim().isEmpty() ? mcp.get("name") : id;
			Map<String, Object> serverConfig = new LinkedHashMap<>();
			serverConfig.put("type", "http");
			serverConfig.put("url", mcpBaseUrl + id + "/comms");
			serverConfig.put("headers", Map.of("Authorization", "Bearer " + bearerToken));
			serverConfig.put("tools", List.of("*"));
			servers.put(name, serverConfig);
		}
		return servers;
	}

	private void publishEvent(AbstractSessionEvent event, GitHubCopilotPixelJobStreamer streamer,
			AtomicBoolean terminalErrorSeen, AtomicBoolean idlePublished,
			AtomicReference<String> sessionErrorMessage) {
		if (event == null) {
			return;
		}

		String timestamp = normalizeTimestamp(event.getTimestamp());

		if (event instanceof AssistantMessageDeltaEvent messageDeltaEvent) {
			streamer.publishAssistantMessageDelta(messageDeltaEvent.getData().messageId(),
					messageDeltaEvent.getData().deltaContent(), messageDeltaEvent.getData().parentToolCallId(),
					timestamp);
			return;
		}

		if (event instanceof AssistantMessageEvent messageEvent) {
			streamer.publishAssistantMessage(messageEvent.getData().messageId(), messageEvent.getData().content(),
					messageEvent.getData().parentToolCallId(), toToolRequestMaps(messageEvent.getData().toolRequests()),
					timestamp);
			return;
		}

		if (event instanceof ToolExecutionStartEvent toolStartEvent) {
			streamer.publishToolExecutionStart(toolStartEvent.getData().toolCallId(),
					toolStartEvent.getData().toolName(), toolStartEvent.getData().arguments(),
					toolStartEvent.getData().parentToolCallId(), timestamp);
			return;
		}

		if (event instanceof ToolExecutionPartialResultEvent partialResultEvent) {
			streamer.publishToolExecutionPartialResult(partialResultEvent.getData().toolCallId(),
					partialResultEvent.getData().partialOutput(), timestamp);
			return;
		}

		if (event instanceof ToolExecutionProgressEvent progressEvent) {
			streamer.publishToolExecutionProgress(progressEvent.getData().toolCallId(),
					progressEvent.getData().progressMessage(), timestamp);
			return;
		}

		if (event instanceof ToolExecutionCompleteEvent completeEvent) {
			Map<String, Object> result = null;
			if (completeEvent.getData().result() != null) {
				result = new LinkedHashMap<>();
				result.put("content", completeEvent.getData().result().content());
				result.put("detailedContent", completeEvent.getData().result().detailedContent());
			}
			Map<String, Object> error = null;
			if (completeEvent.getData().error() != null) {
				error = new LinkedHashMap<>();
				error.put("message", completeEvent.getData().error().message());
				error.put("code", completeEvent.getData().error().code());
			}
			streamer.publishToolExecutionComplete(completeEvent.getData().toolCallId(),
					completeEvent.getData().success(), result, error, timestamp);
			return;
		}

		if (event instanceof SessionIdleEvent) {
			if (!terminalErrorSeen.get()) {
				idlePublished.compareAndSet(false, true);
			}
			return;
		}

		if (event instanceof SessionErrorEvent sessionErrorEvent) {
			terminalErrorSeen.set(true);
			idlePublished.set(true);
			String errorMessage = sessionErrorEvent.getData().message();
			sessionErrorMessage.compareAndSet(null,
					errorMessage != null && !errorMessage.isBlank() ? errorMessage : "GitHub Copilot session error");
			streamer.publishSessionError(sessionErrorEvent.getData().errorType(), errorMessage,
					sessionErrorEvent.getData().statusCode(), timestamp);
		}
	}

	private List<Map<String, Object>> toToolRequestMaps(List<AssistantMessageEvent.AssistantMessageData.ToolRequest> requests) {
		List<Map<String, Object>> items = new ArrayList<>();
		if (requests == null) {
			return items;
		}
		for (AssistantMessageEvent.AssistantMessageData.ToolRequest request : requests) {
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("toolCallId", request.toolCallId());
			item.put("name", request.name());
			item.put("arguments", request.arguments());
			items.add(item);
		}
		return items;
	}

	private String normalizeTimestamp(OffsetDateTime timestamp) {
		return timestamp != null ? timestamp.toString() : OffsetDateTime.now().toString();
	}

	private String buildBearerToken(String accessKey, String secretKey, String roomId) {
		return accessKey + ":" + secretKey + ":room-" + roomId;
	}

	private String buildOpenAIBaseUrl() {
		Integer localPort = ThreadStore.getLocalPort();
		String localProtocol = ThreadStore.getLocalProtocol();
		if (localPort == null || localProtocol == null || localProtocol.trim().isEmpty()) {
			throw new IllegalStateException("Unable to resolve local SEMOSS protocol/port for Copilot provider");
		}
		return localProtocol + "://localhost:" + localPort + "/Monolith/api/model/openai/v1";
	}
}
