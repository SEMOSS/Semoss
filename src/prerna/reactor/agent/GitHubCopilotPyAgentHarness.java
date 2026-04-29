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

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.GitHubCopilotPyManager;
import prerna.engine.impl.model.Room;

/**
 * Python-sidecar GitHub Copilot harness. Same behaviour and parameter shape as
 * {@link GitHubCopilotAgentHarness}, but routes through {@link GitHubCopilotPyManager}
 * (which spawns a Python sidecar that wraps the github-copilot-sdk) instead of
 * the in-Java copilot-sdk-java path.
 *
 * <p>Registered under the harness name {@code "github_copilot_py"} so callers
 * can opt in via Pixel: {@code RunAgent(harnessType="github_copilot_py", ...)}.
 */
public class GitHubCopilotPyAgentHarness implements IAgentHarness {

	public static final String NAME = "github_copilot_py";

	private static final String PARAM_ALLOWED_TOOLS = "allowed_tools";
	private static final String PARAM_PERMISSION_MODE = "permission_mode";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	@SuppressWarnings("unchecked")
	public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
		Room room = ctx.getRoom();
		Map<String, Object> params = ctx.getParamMap();
		String input = ctx.getInput();
		String promptId = ctx.getPromptId();

		String engineId = room.getModelId();
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("GitHubCopilotPyAgentHarness: room does not have a modelId set");
		}

		String systemPrompt = room.getEffectiveSystemPrompt();
		if (systemPrompt == null) {
			systemPrompt = "";
		}

		List<String> allowedTools;
		Object allowedToolsObj = params.get(PARAM_ALLOWED_TOOLS);
		if (allowedToolsObj instanceof List) {
			allowedTools = (List<String>) allowedToolsObj;
		} else {
			allowedTools = Collections.emptyList();
		}

		String permissionMode = params.containsKey(PARAM_PERMISSION_MODE)
				? String.valueOf(params.get(PARAM_PERMISSION_MODE))
				: "default";

		User user = ctx.getInsight().getUser();
		if (user == null) {
			throw new IllegalArgumentException("GitHubCopilotPyAgentHarness: insight has no user");
		}

		IModelEngine modelEngine = ctx.getModelEngine();
		if (modelEngine == null) {
			throw new IllegalArgumentException("GitHubCopilotPyAgentHarness: model engine is required");
		}
		int contextWindow = modelEngine.getContextWindow();

		String cwd = null;
		if (ctx.getFilePath() != null && !ctx.getFilePath().trim().isEmpty()) {
			cwd = ctx.getFilePath() + "/client";
		}

		GitHubCopilotPyManager manager = new GitHubCopilotPyManager();
		String output = manager.query(ctx.getInsight(), user, engineId, cwd, input, promptId, systemPrompt,
				room.getId(), allowedTools, permissionMode, buildMcpList(room), contextWindow, ctx.getMediaInputs());

		return new AgentHarnessResult(output, 0, new ArrayList<>());
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, String>> buildMcpList(Room room) {
		List<Map<String, String>> result = new ArrayList<>();
		Map<String, Object> opts = room.getOptionsMap();
		if (opts == null || !opts.containsKey("mcp")) {
			return result;
		}
		Object mcpObj = opts.get("mcp");
		if (!(mcpObj instanceof List)) {
			return result;
		}
		List<?> mcpList = (List<?>) mcpObj;
		for (Object item : mcpList) {
			if (!(item instanceof Map)) {
				continue;
			}
			Map<String, Object> mcpEntry = (Map<String, Object>) item;
			String id = mcpEntry.containsKey("id") ? String.valueOf(mcpEntry.get("id")) : null;
			String name = mcpEntry.containsKey("name") ? String.valueOf(mcpEntry.get("name")) : id;
			if (id != null) {
				Map<String, String> entry = new HashMap<>();
				entry.put("id", id);
				entry.put("name", name != null ? name : id);
				result.add(entry);
			}
		}
		return result;
	}
}
