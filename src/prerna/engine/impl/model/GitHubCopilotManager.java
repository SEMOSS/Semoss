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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.copilot.sdk.CopilotClient;
import com.github.copilot.sdk.CopilotSession;
import com.github.copilot.sdk.SystemMessageMode;
import com.github.copilot.sdk.events.AbstractSessionEvent;
import com.github.copilot.sdk.events.AssistantMessageEvent;
import com.github.copilot.sdk.events.SessionErrorEvent;
import com.github.copilot.sdk.json.CopilotClientOptions;
import com.github.copilot.sdk.json.MessageOptions;
import com.github.copilot.sdk.json.ModelCapabilities;
import com.github.copilot.sdk.json.ModelInfo;
import com.github.copilot.sdk.json.ModelLimits;
import com.github.copilot.sdk.json.PermissionHandler;
import com.github.copilot.sdk.json.ProviderConfig;
import com.github.copilot.sdk.json.ResumeSessionConfig;
import com.github.copilot.sdk.json.SessionConfig;
import com.github.copilot.sdk.json.SystemMessageConfig;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.sandbox.EnforcementMode;
import prerna.reactor.agent.sandbox.SandboxLaunchPlan;
import prerna.reactor.agent.sandbox.SandboxLauncher;
import prerna.reactor.agent.sandbox.SandboxLauncherRegistry;
import prerna.reactor.agent.sandbox.SandboxPolicy;
import prerna.util.Utility;

/**
 * GitHub Copilot SDK manager that keeps disk-backed session state in the room
 * folder and buffers the provider's internal stream into a completed response.
 */
public class GitHubCopilotManager {

	private static final Logger classLogger = LogManager.getLogger(GitHubCopilotManager.class);
	private static final String CLIENT_NAME = "SEMOSS Agent47";

	/** DIHelper key for the absolute path of the copilot CLI binary. */
	public static final String CFG_COPILOT_CLI_PATH = "GITHUB_COPILOT_CLI_PATH";

	public String query(Insight insight, User user, String engineId, String filePath, String prompt,
			String systemPrompt, String roomId, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps) throws Exception {
		return query(insight, user, engineId, filePath, prompt, systemPrompt, roomId, allowedTools, permissionMode,
				mcps, 0, null);
	}

	public String query(Insight insight, User user, String engineId, String filePath, String prompt,
			String systemPrompt, String roomId, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps, int contextWindow, SandboxPolicy sandboxPolicy) throws Exception {
		String roomFolderPath = Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
		Files.createDirectories(Paths.get(roomFolderPath));

		String workingDirectory = (filePath != null && !filePath.trim().isEmpty()) ? filePath : roomFolderPath;
		Files.createDirectories(Paths.get(workingDirectory));

		String[] keyPair = user.createCachedTemporalAccessSecretKey();
		String bearerToken = buildBearerToken(keyPair[0], keyPair[1], roomId);
		String baseUrl = buildOpenAIBaseUrl();
		AtomicReference<String> sessionErrorMessage = new AtomicReference<>();

		CopilotClientOptions clientOptions = new CopilotClientOptions();
		clientOptions.setCliArgs(new String[] { "--config-dir", roomFolderPath });
		// Always honor the configured (or discovered) Copilot binary; the sandbox
		// path below will override with a wrapper script when enabled.
		String resolvedBinary = resolveCopilotBinary();
		clientOptions.setCliPath(resolvedBinary);
		if (contextWindow > 0) {
			clientOptions.setOnListModels(
					() -> CompletableFuture.completedFuture(List.of(buildModelInfo(engineId, contextWindow))));
		}
		applySandbox(clientOptions, sandboxPolicy, resolvedBinary, workingDirectory);

		try (CopilotClient client = new CopilotClient(clientOptions)) {
			client.start().get();

			String lastSessionId = null;
			try {
				lastSessionId = client.getLastSessionId().get();
			} catch (Exception e) {
				classLogger.debug("Unable to get last Copilot session id for room {}", roomId, e);
			}

			SessionConfig sessionConfig = buildSessionConfig(engineId, workingDirectory, roomFolderPath, systemPrompt,
					bearerToken, baseUrl, allowedTools, permissionMode, mcps, sessionErrorMessage);
			ResumeSessionConfig resumeConfig = buildResumeSessionConfig(engineId, workingDirectory, roomFolderPath,
					systemPrompt, bearerToken, baseUrl, allowedTools, permissionMode, mcps, sessionErrorMessage);

			try (CopilotSession session = openSession(client, lastSessionId, roomFolderPath, sessionConfig,
					resumeConfig)) {
				AssistantMessageEvent finalMessage = session.sendAndWait(new MessageOptions().setPrompt(prompt), 0)
						.get();
				// sendAndWait may complete normally even when the session ended in
				// an error event (auth, provider, quota); surface it to the caller.
				String err = sessionErrorMessage.get();
				if (err != null) {
					throw new IllegalStateException("Copilot session error: " + err);
				}
				return finalMessage != null && finalMessage.getData() != null ? finalMessage.getData().content() : "";
			}
		}
	}

	/**
	 * If sandbox is enabled, override the SDK's {@code cliPath} with our launcher
	 * wrapper so landlock (Linux) or Seatbelt (macOS) applies before the copilot
	 * CLI's {@code exec}. No-op when sandbox is disabled - the caller's prior
	 * {@code setCliPath(targetBinary)} stands.
	 */
	private void applySandbox(CopilotClientOptions opts, SandboxPolicy policy, String targetBinary,
			String workingDirectory) {
		if (policy == null || policy.getEnforcement() == EnforcementMode.DISABLED) {
			return;
		}
		SandboxLauncher launcher = SandboxLauncherRegistry.get();
		SandboxLaunchPlan plan = launcher.plan(policy, targetBinary, null);
		opts.setCliPath(plan.getCliPath());
		// Merge inherited env with plan additions; drop plan removals.
		Map<String, String> env = new java.util.LinkedHashMap<>(System.getenv());
		for (String key : plan.getEnvironmentRemovals()) {
			env.remove(key);
		}
		env.putAll(plan.getEnvironmentAdditions());
		opts.setEnvironment(env);
		opts.setCwd(workingDirectory);

		classLogger.info("Copilot sandbox applied: backend={} target={} policy-paths={}", plan.getBackend(),
				targetBinary, policy.getAllowedPaths().size());
	}

	/**
	 * Resolve the absolute path to the copilot CLI, preferring the DIHelper config
	 * entry {@link #CFG_COPILOT_CLI_PATH} and falling back to common install
	 * locations. Returns {@code "copilot"} if nothing matches - the SDK / sandbox
	 * launcher will surface a clear {@code execvp} error.
	 *
	 * <p>
	 * Public + static so the agent harness can pre-compute the path used by sandbox
	 * policy carve-outs (the binary's parent dir must be readable).
	 */
	public static String resolveCopilotBinary() {
		String configured = Utility.getDIHelperProperty(CFG_COPILOT_CLI_PATH);
		if (configured != null && !configured.trim().isEmpty()) {
			return configured.trim();
		}
		String[] candidates = new String[] { "/usr/local/bin/copilot", "/usr/bin/copilot",
				System.getProperty("user.home") + "/.local/bin/copilot" };
		for (String c : candidates) {
			if (Files.isExecutable(Paths.get(c))) {
				return c;
			}
		}
		return "copilot";
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

	private ModelInfo buildModelInfo(String engineId, int contextWindow) {
		return new ModelInfo().setId(engineId).setName(engineId).setCapabilities(
				new ModelCapabilities().setLimits(new ModelLimits().setMaxContextWindowTokens(contextWindow)));
	}

	private SessionConfig buildSessionConfig(String engineId, String workingDirectory, String roomFolderPath,
			String systemPrompt, String bearerToken, String baseUrl, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps, AtomicReference<String> sessionErrorMessage) {
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
			config.setSystemMessage(
					new SystemMessageConfig().setMode(SystemMessageMode.APPEND).setContent(systemPrompt));
		}
		config.setOnEvent(event -> captureSessionError(event, sessionErrorMessage));
		return config;
	}

	private ResumeSessionConfig buildResumeSessionConfig(String engineId, String workingDirectory,
			String roomFolderPath, String systemPrompt, String bearerToken, String baseUrl, List<String> allowedTools,
			String permissionMode, List<Map<String, String>> mcps, AtomicReference<String> sessionErrorMessage) {
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
			config.setSystemMessage(
					new SystemMessageConfig().setMode(SystemMessageMode.APPEND).setContent(systemPrompt));
		}
		config.setOnEvent(event -> captureSessionError(event, sessionErrorMessage));
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

	private void captureSessionError(AbstractSessionEvent event, AtomicReference<String> sessionErrorMessage) {
		if (event instanceof SessionErrorEvent sessionErrorEvent) {
			String message = sessionErrorEvent.getData().message();
			String errorType = sessionErrorEvent.getData().errorType();
			Double statusCode = sessionErrorEvent.getData().statusCode();
			// Capture once - first terminal error wins; later events shouldn't
			// overwrite the cause shown to the caller.
			sessionErrorMessage.compareAndSet(null, (errorType != null ? errorType : "error")
					+ (statusCode != null ? " (" + statusCode + ")" : "") + (message != null ? ": " + message : ""));
		}
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
