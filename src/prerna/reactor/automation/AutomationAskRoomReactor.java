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
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.reactor.automation.utils.PixelExecutionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Room-aware conversational AI assistant for building automation workflows.
 * Uses the established AskPlayground room turn flow for server-side history and MCP tool access.
 * All user engines are registered as MCP tools so the model can query databases,
 * search vectors, etc. during the design conversation.
 *
 * <p>Pixel: {@code AutomationAskRoom(project=["appId"], room=["roomId"], command=["base64text"])}
 */
public class AutomationAskRoomReactor extends AbstractReactor {

	private static final String ROOM_KEY = "room";

	private static final String SYSTEM_PROMPT =
		"You help users design and safely author directed automation workflows. Workflows contain a manual "
		+ "trigger and typed actions for databases, models, vectors, storage, functions, Pixel, or managed Python. "
		+ "Graph edges determine execution order; do not describe the workflow as an inherently linear sequence.\n\n"
		+ "Use available read-only tools to gather necessary context before proposing an action. For example, inspect "
		+ "database schema before suggesting SQL. Never invent tables, columns, engine identifiers, or capabilities.\n\n"
		+ "When the user asks to add an action, explain the intended action in concise business language, then use "
		+ "AddAutomationStep. Automation tools execute automatically. Select one supported "
		+ "operation, provide the matching node type and complete JSON config, a unique safe node ID, a unique output "
		+ "variable, and the upstream node ID when it is not trigger. Do not call broad document-replacement tools to "
		+ "add a normal step.\n\n"
		+ "For an editable external integration such as GitHub or email, first use AddAutomationStep with "
		+ "python-step.skeleton, then use UpdateAutomationCustomStep with the returned sourceHash. Put the integration "
		+ "inside run(context, inputs), keep credentials as user-configured placeholders, and never make the request "
		+ "directly from the chat.\n\n"
		+ "Managed source is project-owned. AddAutomationStep creates it only once. Never claim an existing generated "
		+ "Python file was replaced: setup changes require PreviewAutomationStepUpdate and explicit "
		+ "ApplyAutomationStepUpdate using the previewed source hash. Do not expose internal IDs, node types, raw JSON, "
		+ "or source hashes unless the user asks for technical details. Ask at most one essential clarification at a "
		+ "time and keep normal replies concise.";

	public AutomationAskRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ROOM_KEY, ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1, 0, 1, 0 };
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

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.isBlank()) {
			engineId = AutomationGenerationUtils.findFirstModelEngine(user);
		}
		if (engineId == null || engineId.isBlank()) {
			throw new IllegalArgumentException(
				"No AI model engine is available. Add a model engine connection to use this feature.");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model engine does not exist or user does not have access.");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException("Model engine could not be loaded.");
		}

		if (roomId == null || roomId.isBlank()) {
			String projectToken = projectId.replace("-", "");
			String engineToken = engineId.replaceAll("[^A-Za-z0-9]", "");
			roomId = "automationchat" + projectToken.substring(0, Math.min(8, projectToken.length()))
					+ engineToken.substring(0, Math.min(8, engineToken.length()));
		}

		Map<String, Object> options = AutomationGenerationUtils.buildEngineMcpOptions(
				user, SYSTEM_PROMPT, projectId);

		RoomUtils.createRoomIfNotExists(roomId, this.insight, modelEngine, command, null, options, null, projectId, null);

		Object result = PixelExecutionUtils.runAndCollect(this.insight,
				"AskPlayground(engine=" + pixelStringList(engineId) + ", roomId=" + pixelStringList(roomId)
						+ ", command=" + pixelStringList(command) + ", context=[], image=[], paramValues=[{}]);");
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
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

	private static String pixelStringList(String value) {
		return "[" + AutomationExecutionUtils.GSON.toJson(value) + "]";
	}

	@Override
	public String getReactorDescription() {
		return "Room-aware conversational AI for designing automation workflows. "
			+ "Uses AskPlayground with project-scoped MCP tools and server-side room history.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The project ID the automation belongs to.";
			case "room" -> "Room ID for this conversation (defaults to automationchat{projectId}).";
			case "command" -> "The user message, base64-encoded.";
			case "engine" -> "Optional model engine ID for this chat. Defaults to the first accessible model.";
			default -> super.getDescriptionForKey(key);
		};
	}
}
