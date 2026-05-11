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
import prerna.engine.impl.model.GitHubCopilotManager;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.sandbox.AgentSandboxConfig;
import prerna.reactor.agent.sandbox.SandboxPolicy;

/**
 * GitHub Copilot SDK-backed agent harness.
 */
public class GitHubCopilotAgentHarness extends AppBuildingHarness {

	private static final Logger logger = LogManager.getLogger(GitHubCopilotAgentHarness.class);

	public static final String NAME = "github_copilot";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	protected AgentHarnessResult doExecute(AgentRunContext ctx) throws Exception {
		Room                room  = ctx.getRoom();
		Map<String, Object> params = ctx.getParamMap();
		String              input = ctx.getInput();

		String       engineId       = resolveEngineId(room);
		String       systemPrompt   = resolveSystemPrompt(room);
		List<String> allowedTools   = resolveAllowedTools(params, Collections.emptyList());
		String       permissionMode = resolvePermissionMode(params);
		User         user           = resolveUser(ctx.getInsight());

		IModelEngine modelEngine = ctx.getModelEngine();
		if (modelEngine == null) {
			throw new IllegalArgumentException(NAME + ": model engine is required");
		}
		int contextWindow = modelEngine.getContextWindow();

		String cwd = resolveClientPath(ctx);

		String targetBinary = GitHubCopilotManager.resolveCopilotBinary();
		SandboxPolicy policy = AgentSandboxConfig.buildEffectivePolicy(
				room.getRoomFolderPath(), ctx.getFilePath(), targetBinary, ctx.getSandboxPolicy());

		GitHubCopilotManager manager = new GitHubCopilotManager();
		String output = manager.query(ctx.getInsight(), user, engineId, cwd, input, systemPrompt,
				room.getId(), allowedTools, permissionMode, buildMcpList(room), contextWindow, policy);

		return new AgentHarnessResult(output, 0, new ArrayList<>());
	}
}
