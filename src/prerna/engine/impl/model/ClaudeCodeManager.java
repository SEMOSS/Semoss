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
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AppBuildingHarness;
import prerna.reactor.agent.sandbox.EnforcementMode;
import prerna.reactor.agent.sandbox.SandboxLaunchPlan;
import prerna.reactor.agent.sandbox.SandboxLauncher;
import prerna.reactor.agent.sandbox.SandboxLauncherRegistry;
import prerna.reactor.agent.sandbox.SandboxPolicy;
import prerna.tcp.PayloadStruct;
import prerna.util.Utility;

public class ClaudeCodeManager {

	private static final Logger classLogger = LogManager.getLogger(ClaudeCodeManager.class);

	/** DIHelper key for an explicit override of the claude CLI path. */
	public static final String CFG_CLAUDE_CLI_PATH = "CLAUDE_CODE_CLI_PATH";

	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;

	protected PyTranslator pyTranslator = null;
	protected File cacheFolder;
	private ClientProcessWrapper cpw = null;

	protected String varName = null;
	protected Map<String, String> vars = new HashMap<>();

	private String createInitScript(String roomId, String filePath, String accessKey, String secretKey,
			List<String> allowedTools, String permissionMode, String model, List<Map<String, String>> mcps,
			String insightId, SandboxPolicy sandboxPolicy) throws Exception {

		Integer localPort = ThreadStore.getLocalPort();
		String localProtocol = ThreadStore.getLocalProtocol();
		String baseUrl = localProtocol + "://" + "localhost" + ":" + localPort + "/Monolith/api/model/anthropic";
		String mcpBaseUrl = localProtocol + "://" + "localhost" + ":" + localPort + "/Monolith/api/ext/mcp/";
		String roomFolderPath = Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
		boolean agentHistoryExists = agentHistoryExists(roomFolderPath, roomId);

		String allowedToolsLiteral;
		if (allowedTools == null || allowedTools.isEmpty()) {
			allowedToolsLiteral = "[]";
		} else {
			allowedToolsLiteral = allowedTools.stream()
					.map(PyUtils::pyQuote)
					.collect(Collectors.joining(",", "[", "]"));
		}

		StringBuilder mcpsLiteral = new StringBuilder("[");
		if (mcps != null) {
			boolean first = true;
			for (Map<String, String> mcp : mcps) {
				if (mcp == null) {
					continue;
				}
				String name = mcp.get("name");
				String mcpProjectId = mcp.get("id");
				if (name == null || mcpProjectId == null) {
					continue;
				}
				if (!first) {
					mcpsLiteral.append(",");
				}
				first = false;
				mcpsLiteral.append("{")
						.append("'name':").append(PyUtils.pyQuote(name)).append(",")
						.append("'url':").append(PyUtils.pyQuote(mcpBaseUrl + mcpProjectId + "/comms"))
						.append("}");
			}
		}
		mcpsLiteral.append("]");

		StringBuilder script = new StringBuilder();
		script.append("import genai_client;claude_code = genai_client.ClaudeCodeClient(")
				.append("model=").append(PyUtils.pyQuote(model)).append(",")
				.append("cwd_path=").append(PyUtils.pyQuote(filePath)).append(",")
				.append("room_id=").append(PyUtils.pyQuote(roomId)).append(",")
				.append("access_key=").append(PyUtils.pyQuote(accessKey)).append(",")
				.append("secret_key=").append(PyUtils.pyQuote(secretKey)).append(",")
				.append("allowed_tools=").append(allowedToolsLiteral).append(",")
				.append("permission_mode=").append(PyUtils.pyQuote(permissionMode != null ? permissionMode : "default")).append(",")
				.append("base_url=").append(PyUtils.pyQuote(baseUrl)).append(",")
				.append("mcps=").append(mcpsLiteral).append(",")
				.append("insight_id=").append(PyUtils.pyQuote(insightId != null ? insightId : "")).append(",")
				.append("room_folder_path=").append(PyUtils.pyQuote(roomFolderPath)).append(",")
				.append("agent_history_exists=").append(agentHistoryExists ? "True" : "False")
				.append(buildSandboxKwargs(sandboxPolicy, filePath, roomFolderPath))
				.append(")");
		return script.toString();
	}

	private boolean agentHistoryExists(String roomFolderPath, String roomId) {
		Path projectsDir = Paths.get(roomFolderPath, "projects");
		boolean exists = Files.exists(projectsDir) && Files.isDirectory(projectsDir);
		classLogger.debug("Agent history check for room {}: projects folder {} at {}", roomId, exists ? "found" : "not found", projectsDir);
		return exists;
	}

	private String createQueryScript(String prompt, String systemPrompt) {
		return "claude_code.query_cc(prompt=" + PyUtils.pyQuote(prompt != null ? prompt : "")
				+ ", system_prompt=" + PyUtils.pyQuote(systemPrompt != null ? systemPrompt : "") + ")";
	}

	/**
	 * Writes the sandbox policy/profile and returns the {@code ,sandbox_cli_path=...,sandbox_env={...}}
	 * kwargs fragment. The SDK will launch the wrapper script instead of the bundled binary;
	 * the wrapper applies sandbox-exec (macOS) or landlock (Linux) before exec'ing the real binary.
	 * Returns an empty string when sandbox is disabled or no policy is set.
	 */
	private String buildSandboxKwargs(SandboxPolicy policy, String filePath, String roomFolderPath) {
		if (policy == null || policy.getEnforcement() == EnforcementMode.DISABLED) {
			return "";
		}
		SandboxLauncher launcher = SandboxLauncherRegistry.get();
		String targetBinary = resolveClaudeBinary();
		SandboxLaunchPlan plan = launcher.plan(policy, targetBinary, null);
		StringBuilder envLiteral = new StringBuilder("{");
		boolean first = true;
		for (Map.Entry<String, String> e : plan.getEnvironmentAdditions().entrySet()) {
			if (!first) envLiteral.append(", ");
			first = false;
			envLiteral.append(PyUtils.pyQuote(e.getKey())).append(": ")
					.append(PyUtils.pyQuote(e.getValue()));
		}
		envLiteral.append("}");
		classLogger.info("Claude sandbox applied: backend={} target={} policy-paths={}",
				plan.getBackend(), targetBinary, policy.getAllowedPaths().size());
		return ",sandbox_cli_path=" + PyUtils.pyQuote(plan.getCliPath()) + ",sandbox_env=" + envLiteral;
	}

	/**
	 * Resolves the Claude CLI binary path. Resolution order:
	 * <ol>
	 *   <li>DIHelper override via {@link #CFG_CLAUDE_CLI_PATH}</li>
	 *   <li>Binary bundled inside the installed {@code claude-agent-sdk} Python package
	 *       ({@code <site-packages>/claude_agent_sdk/_bundled/claude}) — the same binary
	 *       the SDK uses when no {@code cli_path} is set</li>
	 *   <li>Common npm / system install paths</li>
	 *   <li>{@code "claude"} sentinel — OS PATH lookup at exec time</li>
	 * </ol>
	 */
	public static String resolveClaudeBinary() {
		String configured = Utility.getDIHelperProperty(CFG_CLAUDE_CLI_PATH);
		if (configured != null && !configured.trim().isEmpty()) {
			return configured.trim();
		}
		try {
			String sitePackages = PyUtils.appendSitePackagesPath(PyUtils.getPythonHomeDir());
			Path bundled = Paths.get(sitePackages, "claude_agent_sdk", "_bundled", "claude");
			if (Files.isExecutable(bundled)) {
				return bundled.toString();
			}
		} catch (Exception e) {
			classLogger.debug("claude-agent-sdk bundled binary not found via PY_HOME: {}", e.getMessage());
		}
		String[] candidates = {
				"/usr/local/bin/claude",
				"/usr/bin/claude",
				System.getProperty("user.home") + "/.npm-global/bin/claude",
				System.getProperty("user.home") + "/.local/bin/claude",
				System.getProperty("user.home") + "/node_modules/.bin/claude",
				System.getProperty("user.home") + "/.yarn/bin/claude",
				System.getProperty("user.home") + "/.claude/local/claude"
		};
		for (String c : candidates) {
			if (Files.isExecutable(Paths.get(c))) {
				return c;
			}
		}
		return "claude";
	}

	public String query(Insight insight, User user, String engineId, String filePath, String prompt,
			String systemPrompt, String roomId, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps, SandboxPolicy sandboxPolicy) throws Exception {

		String insightId = insight.getInsightId();
		classLogger.debug("InsightID for this query is {} and the roomId is {}", insightId, roomId);

		String base = (filePath != null && !filePath.trim().isEmpty())
				? filePath
				: Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
		String finalFilePath = base + "/client";

		AppBuildingHarness.ensureClaudeStructure(finalFilePath);

		String[] keyPair = user.createCachedTemporalAccessSecretKey();
		String accessKey = keyPair[0];
		String secretKey = keyPair[1];
		String initScript = createInitScript(roomId, finalFilePath, accessKey, secretKey, allowedTools, permissionMode,
				engineId, mcps, insightId, sandboxPolicy);
		checkSocketStatus(initScript);
		String queryScript = createQueryScript(prompt, systemPrompt);
		Object output = pyTranslator.runDirectPy(insight, queryScript);
		return String.valueOf(output);
	}

	/**
	 * This method is responsible for starting the python process that is linked to
	 * this model engine.
	 * 
	 * @param port The port number to use when creating the server/client
	 *             connection.
	 */
	protected synchronized void startServer(int port, String initScript) {
		if (this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
		if (this.workingDirectoryBasePath == null) {
			this.createCacheFolder();
		}

		ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
		if (this.cpw != null) {
			this.cpw.shutdown(false);
		}

		String timeout = "30";

		if (cpwToInit.getSocketClient() == null) {
			boolean debug = false;

			String forcePort = null; // Not sure where I'd keep this; possibly as reactor param
			String customClassPath = null;
			String loggerLevel = null;

			if (port < 0) {
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						classLogger.warn("Claude Code" + " has an invalid FORCE_PORT value");
					}
				}
			}

			String serverDirectory = this.cacheFolder.getAbsolutePath();

			try {
				cpwToInit.createProcessAndClient(true, null, port, null, serverDirectory, customClassPath, debug,
						timeout, "INFO");
			} catch (Exception e) {
				classLogger.error("Failed to create the python process for Claude Code Agent: {}", e);
				throw new IllegalArgumentException("Unable to connect to server for python Claude Code Agent.");
			}
		} else if (!cpwToInit.getSocketClient().isConnected()) {
			cpwToInit.shutdown(false);
			try {
				cpwToInit.reconnect();
			} catch (Exception e) {
				classLogger.error("Failed to reconnect to the python process for Claude Code Agent: {}", e);
				throw new IllegalArgumentException("Failed to start TCP Server for Claude Code Agent: {}", e);
			}
		}

		// create the py translator
		Insight processInsight = new Insight();
		InsightStore.getInstance().put(processInsight);
		this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

		try {
			String initCommands = initScript;
			String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
			for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
				commands[commandIndex] = fillVars(commands[commandIndex]);
			}
			this.pyTranslator.runEmptyPy(commands);
			classLogger.info(
					"Initializing Claude Code" + " python process with commands >>> " + String.join("\n", commands));
			setPrefix(cpwToInit);

			this.cpw = cpwToInit;
		} catch (Exception e) {
			classLogger.error("Failed to  to the python process for Claude Code", e);
			if (cpwToInit != null) {
				classLogger.warn("Able to start the python process for Claude Code but the start script failed");
				cpwToInit.shutdown(false);
			}
			throw e;
		}
	}

	/**
	 * This method checks whether the socket client is instantiated and connected.
	 */
	protected void checkSocketStatus(String initScript) {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1, initScript);
		}
	}

	/**
	 * 
	 */
	private void setPrefix(ClientProcessWrapper cpwToInit) {
		this.prefix = cpwToInit.getPrefix();
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = new String[] { "prefix", this.prefix };
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		cpwToInit.getSocketClient().executeCommand(prefixPayload);
	}

	/**
	 * 
	 */
	private void createCacheFolder() {
		this.workingDirectory = "CLAUDECODE_" + "_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);

		if (!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}

}
