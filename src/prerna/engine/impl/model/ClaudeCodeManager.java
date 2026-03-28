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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.UncheckedIOException;

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
import prerna.project.api.IProject;
import prerna.tcp.PayloadStruct;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class ClaudeCodeManager {

	private static final Logger classLogger = LogManager.getLogger(ClaudeCodeManager.class);

	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;

	protected PyTranslator pyTranslator = null;
	protected File cacheFolder;
	private ClientProcessWrapper cpw = null;

	protected String varName = null;
	protected Map<String, String> vars = new HashMap<>();

	private String createInitScript(String roomId, String filePath, String accessKey, String secretKey,
			List<String> allowedTools, String permissionMode, String model, List<Map<String, String>> mcps)
			throws Exception {

		String allowedToolsString = "allowed_tools=["
				+ allowedTools.stream().map(tool -> "'" + tool + "'").collect(Collectors.joining(",")) + "]";
		Integer localPort = ThreadStore.getLocalPort();
		String localProtocol = ThreadStore.getLocalProtocol();
		String baseUrl = localProtocol + "://" + "localhost" + ":" + localPort + "/Monolith/api/model/anthropic";
		String mcpBaseUrl = localProtocol + "://" + "localhost" + ":" + localPort + "/Monolith/api/ext/mcp/";
		List<Map<String, String>> mcpUrlsAndNames = new ArrayList<>();
		if (mcps != null) {
			for (Map<String, String> mcp : mcps) {
				Map<String, String> mcpConfig = new HashMap<>();
				mcpConfig.put("name", mcp.get("name"));
				String mcpProjectId = mcp.get("id");
				String fullMcpUrl = mcpBaseUrl + mcpProjectId + "/comms";
				mcpConfig.put("url", fullMcpUrl);
				mcpUrlsAndNames.add(mcpConfig);
			}
		}
		String mcpsString = mcpUrlsAndNames.stream()
				.map(mcp -> "{'name':'" + mcp.get("name") + "', 'url': '" + mcp.get("url") + "'}")
				.collect(Collectors.joining(",", "[", "]"));

		return String.format(
				"import genai_client;claude_code = genai_client.ClaudeCodeClient(model='%s', cwd_path='%s', room_id='%s', access_key='%s', secret_key='%s', %s, permission_mode='%s', base_url='%s', mcps=%s)",
				model, filePath, roomId, accessKey, secretKey, allowedToolsString, permissionMode, baseUrl, mcpsString);
	}

	private String createQueryScript(String prompt, String systemPrompt) {
		return String.format("claude_code.query_cc(prompt='%s', system_prompt='%s')", prompt, systemPrompt);
	}

	private void createClaudeDir(String projectPath) {
		try {
			Path claudeDir = Paths.get(projectPath, ".claude");
			if (!Files.exists(claudeDir)) {
				Files.createDirectories(claudeDir);
			}

			Path skillsDir = claudeDir.resolve("skills");
			if (!Files.exists(skillsDir)) {
				Files.createDirectories(skillsDir);
			}

			Path logsDir = claudeDir.resolve("logs");
			if (!Files.exists(logsDir)) {
				Files.createDirectories(logsDir);
				Path changeLogPath = claudeDir.resolve("logs/change_log.txt");
				Files.createFile(changeLogPath);
			}

			Path claudeFile = Paths.get(projectPath, "CLAUDE.md");
			if (!Files.exists(claudeFile)) {
				Files.createFile(claudeFile);
			}

		} catch (IOException e) {
			classLogger.error("Failed to create .claude directory structure at: " + projectPath, e);
		}
	}

	public String query(Insight insight, User user, String engineId, String filePath, String prompt,
			String systemPrompt, String roomId, List<String> allowedTools, String permissionMode,
			List<Map<String, String>> mcps) throws Exception {

		createClaudeDir(filePath);

		String[] keyPair = user.createCachedTemporalAccessSecretKey();
		String accessKey = keyPair[0];
		String secretKey = keyPair[1];
		String initScript = createInitScript(roomId, filePath, accessKey, secretKey, allowedTools, permissionMode,
				engineId, mcps);
		checkSocketStatus(initScript);
		String queryScript = createQueryScript(prompt, systemPrompt);
		Object output = pyTranslator.runDirectPy(insight, queryScript);
		return String.valueOf(output);
	}

	public Boolean deleteSkill(User user, String projectId, String skillName) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Could not find or load project = " + projectId);
		}
		String projectName = project.getProjectName();
		String projectPath = EngineUtility.getSpecificEngineAssetsFolder(project.getCatalogType(), projectId,
				projectName);

		Path skillPath = Paths.get(projectPath, ".claude", "skills", skillName);

		if (!Files.exists(skillPath)) {
			return true;
		}

		try (Stream<Path> walk = Files.walk(skillPath)) {
			walk.sorted(Comparator.reverseOrder()).forEach(path -> {
				try {
					Files.delete(path);
				} catch (IOException e) {
					classLogger.error("Failed to delete path: " + path + " - " + e);
					throw new UncheckedIOException(e);
				}
			});
			return true;
		} catch (IOException | UncheckedIOException e) {
			classLogger.error("Failed to delete skills directory: " + e);
			return false;
		}
	}

	public Boolean createSkill(User user, String projectId, String skillName, String skillContent) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Could not find or load project = " + projectId);
		}
		String projectName = project.getProjectName();
		String projectPath = EngineUtility.getSpecificEngineAssetsFolder(project.getCatalogType(), projectId,
				projectName);
		String slugifiedName = skillName.toLowerCase().replace(" ", "-");
		Path skillPath = Paths.get(projectPath, ".claude", "skills", slugifiedName, "SKILL.md");

		try {
			Files.createDirectories(skillPath.getParent());
			Files.createFile(skillPath);
			Files.write(skillPath, skillContent.getBytes(StandardCharsets.UTF_8));
			return true;
		} catch (IOException e) {
			classLogger.error("Failed to write skill file: " + e);
			return false;
		}
	}

	public Boolean updateSkill(User user, String projectId, String skillName, String skillContent) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Could not find or load project = " + projectId);
		}
		String projectName = project.getProjectName();
		String projectPath = EngineUtility.getSpecificEngineAssetsFolder(project.getCatalogType(), projectId,
				projectName);

		Path skillPath = Paths.get(projectPath, ".claude", "skills", skillName, "SKILL.md");

		try {
			Files.createDirectories(skillPath.getParent());
			Files.write(skillPath, skillContent.getBytes(StandardCharsets.UTF_8));
			return true;
		} catch (IOException e) {
			classLogger.error("Failed to write skill file: " + e);
			return false;
		}

	}

	public Map<String, String> getSkills(User user, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Could not find or load project = " + projectId);
		}
		String projectName = project.getProjectName();
		String projectPath = EngineUtility.getSpecificEngineAssetsFolder(project.getCatalogType(), projectId,
				projectName);
		Map<String, String> skillsMap = new HashMap<>();

		Path claudeMd = Paths.get(projectPath, "CLAUDE.md");
		if (Files.exists(claudeMd)) {
			try {
				String content = new String(Files.readAllBytes(claudeMd));
				skillsMap.put("CLAUDE.MD", content);
			} catch (IOException e) {
				classLogger.error("Failed to read Claude.md file: " + e);
			}
		}

		Path skillsDir = Paths.get(projectPath, ".claude", "skills");
		if (!Files.exists(skillsDir)) {
			return skillsMap;
		}
		try {
			Files.list(skillsDir).forEach(dir -> {
				try {
					String skillName = dir.getFileName().toString();
					Path skillFilePath = dir.resolve("SKILL.md");
					String skillContent = new String(Files.readAllBytes(skillFilePath));
					skillsMap.put(skillName, skillContent);
				} catch (IOException e) {
					classLogger.error("Failed to get skill file contents: " + e);
				}
			});
		} catch (IOException e) {
			classLogger.error("Failed to list skills directory: " + skillsDir, e);
		}
		return skillsMap;
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
