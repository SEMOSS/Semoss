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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Python-sidecar GitHub Copilot manager. Mirrors {@link ClaudeCodeManager}: spawns a
 * Python process via {@link ClientProcessWrapper}, instantiates
 * {@code genai_client.GitHubCopilotClient}, and forwards user prompts through it.
 *
 * <p>Drop-in replacement for {@link GitHubCopilotManager} that avoids the in-Java
 * {@code copilot-sdk-java} CLI launch (which is the path that hits chroot
 * permission errors today). Used by {@link prerna.reactor.agent.GitHubCopilotPyAgentHarness}
 * under harness name {@code "github_copilot_py"}.
 */
public class GitHubCopilotPyManager {

	private static final Logger classLogger = LogManager.getLogger(GitHubCopilotPyManager.class);

	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;

	protected PyTranslator pyTranslator = null;
	protected File cacheFolder;
	private ClientProcessWrapper cpw = null;

	protected String varName = null;
	protected Map<String, String> vars = new HashMap<>();

	public String query(Insight insight, User user, String engineId, String filePath, String prompt,
			String systemPrompt, String roomId, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps, int contextWindow) throws Exception {

		String insightId = insight.getInsightId();
		classLogger.debug("InsightID for this query is {} and the roomId is {}", insightId, roomId);

		String roomFolderPath = Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
		Files.createDirectories(Paths.get(roomFolderPath));

		String workingDir = (filePath != null && !filePath.trim().isEmpty()) ? filePath : roomFolderPath;
		Files.createDirectories(Paths.get(workingDir));

		boolean sessionExists = sessionStateExists(roomFolderPath);

		String[] keyPair = user.createCachedTemporalAccessSecretKey();
		String accessKey = keyPair[0];
		String secretKey = keyPair[1];

		String cliPath = trimToNull(DIHelper.getInstance().getProperty(Constants.GITHUB_COPILOT_CLI_PATH));

		String initScript = createInitScript(roomId, workingDir, roomFolderPath, accessKey, secretKey, allowedTools,
				permissionMode, engineId, mcps, insightId, cliPath, sessionExists);
		checkSocketStatus(initScript);

		String queryScript = createQueryScript(prompt, systemPrompt);
		Object output = pyTranslator.runDirectPy(insight, queryScript);
		return String.valueOf(output);
	}

	// Script builders

	private String createInitScript(String roomId, String cwdPath, String roomFolderPath, String accessKey,
			String secretKey, List<String> allowedTools, String permissionMode, String model,
			List<Map<String, String>> mcps, String insightId, String cliPath, boolean sessionExists) {

		Integer localPort = ThreadStore.getLocalPort();
		String localProtocol = ThreadStore.getLocalProtocol();
		if (localPort == null || localProtocol == null || localProtocol.trim().isEmpty()) {
			throw new IllegalStateException(
					"GitHubCopilotPyManager: unable to resolve local SEMOSS protocol/port for OpenAI base URL");
		}
		String baseUrl = localProtocol + "://localhost:" + localPort + "/Monolith/api/model/openai/v1";
		String mcpBaseUrl = localProtocol + "://localhost:" + localPort + "/Monolith/api/ext/mcp/";

		String allowedToolsLiteral;
		if (allowedTools == null || allowedTools.isEmpty()) {
			allowedToolsLiteral = "[]";
		} else {
			allowedToolsLiteral = allowedTools.stream()
					.map(GitHubCopilotPyManager::pyQuote)
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
						.append("'name':").append(pyQuote(name)).append(",")
						.append("'url':").append(pyQuote(mcpBaseUrl + mcpProjectId + "/comms"))
						.append("}");
			}
		}
		mcpsLiteral.append("]");

		StringBuilder script = new StringBuilder();
		script.append("import genai_client;github_copilot = genai_client.GitHubCopilotClient(")
				.append("model=").append(pyQuote(model)).append(",")
				.append("cwd_path=").append(pyQuote(cwdPath)).append(",")
				.append("room_id=").append(pyQuote(roomId)).append(",")
				.append("access_key=").append(pyQuote(accessKey)).append(",")
				.append("secret_key=").append(pyQuote(secretKey)).append(",")
				.append("allowed_tools=").append(allowedToolsLiteral).append(",")
				.append("permission_mode=").append(pyQuote(permissionMode != null ? permissionMode : "default"))
				.append(",")
				.append("base_url=").append(pyQuote(baseUrl)).append(",")
				.append("mcps=").append(mcpsLiteral).append(",")
				.append("insight_id=").append(pyQuote(insightId != null ? insightId : "")).append(",")
				.append("room_folder_path=").append(pyQuote(roomFolderPath)).append(",")
				.append("session_exists=").append(sessionExists ? "True" : "False");
		if (cliPath != null) {
			script.append(",cli_path=").append(pyQuote(cliPath));
		}
		script.append(")");
		return script.toString();
	}

	private String createQueryScript(String prompt, String systemPrompt) {
		return "github_copilot.query_copilot(prompt=" + pyQuote(prompt != null ? prompt : "")
				+ ", system_prompt=" + pyQuote(systemPrompt != null ? systemPrompt : "") + ")";
	}

	private boolean sessionStateExists(String roomFolderPath) {
		Path sentinel = Paths.get(roomFolderPath, "copilot-session", ".created");
		return Files.exists(sentinel);
	}

	/** Quote-and-escape a Java string into a single-quoted Python string literal. */
	static String pyQuote(String value) {
		if (value == null) {
			return "None";
		}
		StringBuilder sb = new StringBuilder(value.length() + 4);
		sb.append('\'');
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			switch (c) {
				case '\\': sb.append("\\\\"); break;
				case '\'': sb.append("\\'"); break;
				case '\n': sb.append("\\n"); break;
				case '\r': sb.append("\\r"); break;
				case '\t': sb.append("\\t"); break;
				default:
					if (c < 0x20) {
						sb.append(String.format("\\x%02x", (int) c));
					} else {
						sb.append(c);
					}
			}
		}
		sb.append('\'');
		return sb.toString();
	}

	private static String trimToNull(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}

	// Sidecar process plumbing — copied from ClaudeCodeManager.

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
			String forcePort = null;
			String customClassPath = null;

			if (port < 0) {
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						classLogger.warn("GitHub Copilot Py has an invalid FORCE_PORT value");
					}
				}
			}

			String serverDirectory = this.cacheFolder.getAbsolutePath();
			try {
				cpwToInit.createProcessAndClient(true, null, port, null, serverDirectory, customClassPath, debug,
						timeout, "INFO");
			} catch (Exception e) {
				classLogger.error("Failed to create the python process for GitHub Copilot Py Agent: {}", e);
				throw new IllegalArgumentException("Unable to connect to server for python GitHub Copilot Py Agent.");
			}
		} else if (!cpwToInit.getSocketClient().isConnected()) {
			cpwToInit.shutdown(false);
			try {
				cpwToInit.reconnect();
			} catch (Exception e) {
				classLogger.error("Failed to reconnect to the python process for GitHub Copilot Py Agent: {}", e);
				throw new IllegalArgumentException("Failed to start TCP Server for GitHub Copilot Py Agent: {}", e);
			}
		}

		Insight processInsight = new Insight();
		InsightStore.getInstance().put(processInsight);
		this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

		try {
			String[] commands = initScript.split(PyUtils.PY_COMMAND_SEPARATOR);
			for (int i = 0; i < commands.length; i++) {
				commands[i] = fillVars(commands[i]);
			}
			this.pyTranslator.runEmptyPy(commands);
			classLogger.info("Initializing GitHub Copilot Py python process with commands >>> "
					+ String.join("\n", commands));
			setPrefix(cpwToInit);
			this.cpw = cpwToInit;
		} catch (Exception e) {
			classLogger.error("Failed init for GitHub Copilot Py python process", e);
			if (cpwToInit != null) {
				classLogger.warn("Started python process for GitHub Copilot Py but the init script failed");
				cpwToInit.shutdown(false);
			}
			throw e;
		}
	}

	protected void checkSocketStatus(String initScript) {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1, initScript);
		}
	}

	private void setPrefix(ClientProcessWrapper cpwToInit) {
		this.prefix = cpwToInit.getPrefix();
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = new String[] { "prefix", this.prefix };
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		cpwToInit.getSocketClient().executeCommand(prefixPayload);
	}

	private void createCacheFolder() {
		this.workingDirectory = "GITHUBCOPILOT_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);
		if (!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}

	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		return sub.replace(input);
	}
}
