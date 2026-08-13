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
package prerna.reactor.automation;

import prerna.reactor.automation.utils.AutomationGenerationUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.reactor.agent.run.RunAgentRequest;
import prerna.reactor.agent.run.RunAgentResult;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Room-aware conversational AI assistant for building automation workflows.
 * Uses the platform RunAgent harness for server-side history and MCP tool access.
 * All user engines are registered as MCP tools so the model can query databases,
 * search vectors, etc. during the design conversation.
 *
 * <p>Pixel: {@code AutomationAskRoom(project=["appId"], room=["roomId"], command=["base64text"])}
 */
public class AutomationAskRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AutomationAskRoomReactor.class);

	private static final String ROOM_KEY = "room";
	private static final String HARNESS_TYPE = "semoss";
	private static final int MAX_TURNS = 8;
	private static final long CHAT_TIMEOUT_MS = 180_000L;

	private static final String SYSTEM_PROMPT =
		"You are an AI assistant that helps users design automation workflows on a no-code platform. "
		+ "Workflows are linear sequences of steps: database queries, AI model calls, file storage, vector search, or custom functions. "
		+ "Only manual triggers exist  - do not ask about scheduling.\n\n"
		+ "You have access to tools. Use them to help the user (for example, query a database to understand its structure "
		+ "so you can give accurate step descriptions).\n\n"
		+ "CONVERSATION PHASES:\n\n"
		+ "Phase 1  - Gather requirements (at most 1-2 short questions, one at a time):\n"
		+ "- Ask what the automation should do if unclear.\n"
		+ "- Ask where results should go, or one other essential clarification.\n"
		+ "- Keep responses under 50 words per question.\n\n"
		+ "Phase 2  - Plan + build signal (in ONE response, once you have enough info):\n"
		+ "- Present a concise numbered plain-English plan.\n"
		+ "- On the very next line after the plan, output the build signal JSON:\n"
		+ "  {\"action\":\"build\",\"description\":\"<comprehensive 2-4 sentence description of the full workflow>\"}\n"
		+ "- The description must include all steps in enough detail for an AI to build them.\n"
		+ "- Do NOT ask 'does this look right?'  - include the build signal in the same response as the plan.\n\n"
		+ "Phase 3  - If the user requests changes after seeing the plan:\n"
		+ "- Acknowledge in one short sentence.\n"
		+ "- Immediately output the revised plan + a new build signal in the same response.\n"
		+ "- Do NOT narrate what you will change  - just show the revised plan and the signal.\n\n"
		+ "RULES: Never mention engine IDs, node types, or JSON structure to the user. Be concise.";

	public AutomationAskRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ROOM_KEY, ReactorKeysEnum.COMMAND.getKey() };
		this.keyRequired = new int[] { 1, 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in.");
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String roomId = this.keyValue.get(ROOM_KEY);
		String rawCommand = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access.");
		}

		String command = decodeCommand(rawCommand);
		if (command == null || command.isBlank()) {
			throw new IllegalArgumentException("command must not be empty.");
		}

		String engineId = AutomationGenerationUtils.findFirstModelEngine(user);
		if (engineId == null || engineId.isBlank()) {
			throw new IllegalArgumentException(
				"No AI model engine is available. Add a model engine connection to use this feature.");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException("Model engine could not be loaded.");
		}

		if (roomId == null || roomId.isBlank()) {
			roomId = "automationchat" + projectId.replace("-", "").substring(0, Math.min(8, projectId.replace("-", "").length()));
		}

		Map<String, Object> options = AutomationGenerationUtils.buildEngineMcpOptions(user, SYSTEM_PROMPT);

		RoomUtils.createRoomIfNotExists(roomId, this.insight, modelEngine, command, null, options, null, projectId, null);

		RunAgentRequest request = new RunAgentRequest(
				roomId, command, engineId, HARNESS_TYPE, null,
				MAX_TURNS, 0, null, null, null, null, this.insight);

		RunAgentResult handle = AgentRuntimeManager.get().run(request);
		Map<String, Object> result;
		try {
			result = AgentRuntimeManager.get().waitForRun(handle.getRunId(), this.insight, CHAT_TIMEOUT_MS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Chat interrupted.", e);
		}

		String status = (String) result.get("status");
		if ("FAILED".equals(status)) {
			String errMsg = (String) result.get("errorMessage");
			classLogger.error("AutomationAskRoom run failed: project={} error={}", projectId, (errMsg != null ? errMsg : "unknown error"));
			throw new RuntimeException("Chat failed: " + (errMsg != null ? errMsg : "unknown error"));
		}

		String finalText = (String) result.get("finalText");
		if (finalText == null || finalText.isBlank()) {
			throw new IllegalStateException("The AI model did not respond. Try again.");
		}

		classLogger.info("AutomationAskRoom completed: project={}", projectId);
		return new NounMetadata(finalText.strip(), PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	/**
	 * Decodes a base64-encoded command string. Falls back to the raw value when
	 * the input is not valid base64 (supports plain-text callers during testing).
	 */
	private static String decodeCommand(String raw) {
		if (raw == null) {
			return null;
		}
		try {
			return new String(Base64.getDecoder().decode(raw.trim()), StandardCharsets.UTF_8);
		} catch (Exception e) {
			return raw;
		}
	}

	@Override
	public String getReactorDescription() {
		return "Room-aware conversational AI for designing automation workflows. "
			+ "Uses the platform RunAgent harness with MCP tool access  - the model can query "
			+ "databases, search vectors, and use other engines during the conversation. "
			+ "History is managed server-side. "
			+ "Signals build-readiness via: {\"action\":\"build\",\"description\":\"...\"}";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The project ID the automation belongs to.";
			case "room" -> "Room ID for this conversation (defaults to automationchat{projectId}).";
			case "command" -> "The user message, base64-encoded.";
			default -> super.getDescriptionForKey(key);
		};
	}
}
