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
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.GitHubCopilotPyManager;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.sandbox.AgentSandboxConfig;
import prerna.reactor.agent.sandbox.SandboxPolicy;

/**
 * Python-sidecar GitHub Copilot harness. Routes through
 * {@link GitHubCopilotPyManager}, which spawns a Python sidecar that wraps the
 * github-copilot-sdk.
 *
 * <p>Registered under the harness name {@code "github_copilot_py"} so callers
 * can opt in via Pixel: {@code RunAgent(harnessType="github_copilot_py", ...)}.
 *
 * <p>Uses {@code ctx.getFilePath()} directly as the CLI cwd - callers wanting
 * the legacy {@code /client} subdir must pass {@code subdir="client"}.
 *
 * <p>Like the other CLI harnesses, SEMOSS passes only the authored
 * room/workspace system prompt. The external agent runtime is responsible for
 * discovering filesystem instructions such as {@code AGENTS.md} or
 * {@code CLAUDE.md} from the working directory.
 */
public class GitHubCopilotPyAgentHarness implements IAgentHarness {

	private static final Logger logger = LogManager.getLogger(GitHubCopilotPyAgentHarness.class);

	public static final String NAME = "github_copilot_py";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
		Room                room  = ctx.getRoom();
		Map<String, Object> params = ctx.getParamMap();
		String              input = ctx.getInput();
		String              cwd   = ctx.getFilePath();

		String       engineId       = room.getModelId();
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException(NAME + ": room does not have a modelId set");
		}
			String       systemPrompt   = room.getSystemPromptForModel();
		if (systemPrompt == null) {
			systemPrompt = "";
		}
		User         user           = ctx.getInsight().getUser();
		if (user == null) {
			throw new IllegalArgumentException(NAME + ": insight has no user");
		}
		List<String> allowedTools   = resolveAllowedTools(params, Collections.emptyList());
		String       permissionMode = resolvePermissionMode(params);
		List<Map<String, String>> mcps = ctx.getAgentConfig().getMcps();

		IModelEngine modelEngine = ctx.getModelEngine();
		if (modelEngine == null) {
			throw new IllegalArgumentException(NAME + ": model engine is required");
		}
		int contextWindow = modelEngine.getContextWindow();

		String targetBinary = GitHubCopilotPyManager.resolveCopilotBinary();
		SandboxPolicy sandboxPolicy = AgentSandboxConfig.buildEffectivePolicy(
				room.getRoomFolderPath(), cwd, targetBinary, ctx.getSandboxPolicy());

		logger.debug("GitHubCopilotPyAgentHarness: engine={} cwd={} mcps={}",
				engineId, cwd, mcps == null ? 0 : mcps.size());

		GitHubCopilotPyManager manager = new GitHubCopilotPyManager();
		String output = manager.query(ctx.getInsight(), user, engineId, cwd, input, systemPrompt, room.getId(),
				allowedTools, permissionMode, mcps, contextWindow, sandboxPolicy);

		return new AgentHarnessResult(output, 0, new ArrayList<>());
	}

	@SuppressWarnings("unchecked")
	private static List<String> resolveAllowedTools(Map<String, Object> params, List<String> defaults) {
		if (params == null) {
			return defaults;
		}
		Object o = params.get("allowed_tools");
		if (o instanceof List) {
			return (List<String>) o;
		}
		return defaults;
	}

	private static String resolvePermissionMode(Map<String, Object> params) {
		if (params == null || !params.containsKey("permission_mode")) {
			return "default";
		}
		return String.valueOf(params.get("permission_mode"));
	}
}
