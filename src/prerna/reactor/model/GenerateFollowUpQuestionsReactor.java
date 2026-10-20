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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.message.MessagePartType;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.run.DeferredAgentTools;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GenerateFollowUpQuestionsReactor extends AbstractReactor {

	private static final String SUGGESTION_CONTEXT = "suggestionContext";

	public GenerateFollowUpQuestionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				SUGGESTION_CONTEXT, ReactorKeysEnum.PARENT_MESSAGE_ID.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		String roomId = this.keyValue.get(this.keysToGet[1]);

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(modelId);
		String userId = user.getPrimaryLoginToken().getId();
		ModelInferenceLogsUtils.validUserRoom(roomId, userId);

		Room sourceRoom = RoomUtils.getOrLoadRoom(roomId, insight);
		Room room = sourceRoom.createInferenceSnapshot();
		if (room.getMessages().isEmpty()) {
			return new NounMetadata(Map.of("suggestions", List.of()), PixelDataType.MAP);
		}

		Map<String, Object> context = getNamedMap(SUGGESTION_CONTEXT);
		applyAttachmentSnapshot(room, context, user);
		String parentId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		if (parentId == null || parentId.isBlank()) {
			parentId = room.getMessages().getLast().getMessageId();
		}
		String anchorId = parentId;
		AbstractMessage anchor = room.getMessages().stream()
				.filter(message -> anchorId.equals(message.getMessageId())).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Parent message does not belong to this room"));
		if (!isSettledResponse(anchor)) {
			return new NounMetadata(Map.of("suggestions", List.of()), PixelDataType.MAP);
		}
		RoomMessageStore.normalizeForProviderPayload(room);

		Map<String, Object> params = generationParameters(getParamMap(), room.getOptionsMap());
		int numResponses = parsePositiveInt(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()), 3);
		params.put("schema", jsonToMap(String.format(PlaygroundUtils.FOLLOW_UP_SUGGESTIONS_SCHEMA,
				numResponses, numResponses)));
		String prompt = PlaygroundUtils.FOLLOW_UP_SUGGESTIONS_PROMPT
				+ "\nCurrently attached capabilities (context only):\n"
				+ new com.google.gson.Gson().toJson(resolvedCapabilitySummaries(room, context));
		InputMessage input = InputMessage.builder(room).withText(prompt)
				.withModelType(modelEngine.getModelType()).withParamMap(params).build();
		// Preserve existing model/parameter controls for engines that do not use history.
		boolean useHistory = Room.shouldPersistTurn(input, modelEngine) && modelEngine.keepsConversationHistory();
		input.setParentMessageId(useHistory ? parentId : null);
		Object useHistoryParam = params.get("use_history");
		if (useHistoryParam instanceof Boolean || "false".equalsIgnoreCase(String.valueOf(useHistoryParam))) {
			params.remove("use_history");
		}

		// askRoom retains the entire existing branch and normal model limits/logging.
		// Calling the engine directly avoids Room.ask's callable-tool injection and
		// history mutations. The uncached snapshot also isolates serialization changes.
		AskModelEngineResponse<?> modelResponse = modelEngine.askRoom(input, room, params);
		ResponseMessage response = ResponseMessage.Builder.fromAskModelEngineResponse(modelResponse).build();
		if (response.hasToolCallPart()) {
			throw new IllegalStateException("Model returned a tool call instead of follow-up suggestions");
		}
		String jsonResponse = response.getContent();
		if (jsonResponse == null || jsonResponse.isBlank()) {
			throw new IllegalStateException("Model did not return structured follow-up suggestions");
		}
		return new NounMetadata(validateSuggestions(jsonToMap(jsonResponse), numResponses), PixelDataType.MAP);
	}

	static boolean isSettledResponse(AbstractMessage message) {
		if (message.hasParts()) {
			return message.getIo() == MessageIO.OUTPUT
					&& message.getParts().getLast().getType() != MessagePartType.TOOL_CALL;
		}
		return message.getMessageType() == MessageType.RESPONSE_TEXT
				|| message.getMessageType() == MessageType.RESPONSE_MEDIA;
	}

	static Map<String, Object> generationParameters(Map<String, Object> supplied, Map<String, Object> options) {
		Map<String, Object> params = supplied == null ? new HashMap<>() : new HashMap<>(supplied);
		// These parameters can execute tools or replace/append room history. They
		// never belong in an auxiliary, suggestion-only inference call.
		for (String key : List.of("tools", "tool_choice", "full_prompt", "append_full_prompt",
				DeferredAgentTools.RUN_AGENT_PARAM)) {
			params.remove(key);
		}
		Object temperature = options.get("temperature");
		if (temperature != null) {
			params.putIfAbsent("temperature", temperature);
		}
		return params;
	}

	private static void applyAttachmentSnapshot(Room room, Map<String, Object> context, User user) {
		if (!context.containsKey("mcp")) {
			return;
		}
		Object selected = context.get("mcp");
		if (!(selected instanceof List<?> selections)) {
			throw new IllegalArgumentException("suggestionContext.mcp must be an array");
		}
		List<Map<String, Object>> references = new ArrayList<>();
		for (Object selection : selections) {
			if (!(selection instanceof Map<?, ?> reference) || !(reference.get("id") instanceof String id)
					|| id.isBlank()) {
				throw new IllegalArgumentException("Each suggested MCP attachment must have an id");
			}
			if (!MCPUtility.ROOM_MCP_ID.equals(id) && !SecurityEngineUtils.userCanViewEngine(user, id)
					&& !SecurityProjectUtils.userCanViewProject(user, id)) {
				throw new IllegalArgumentException("MCP attachment does not exist or is not accessible");
			}
			references.add(Map.of("id", id));
		}
		Map<String, Object> options = new HashMap<>(room.getOptionsMap());
		options.put("mcp", references);
		room.setOptionsMap(options);
	}

	static List<Map<String, String>> resolvedCapabilitySummaries(Room room, Map<String, Object> context) {
		List<Map<String, Object>> definitions = room.getAllToolsJsonForRoom();
		Map<String, Map<String, Object>> lookup = room.getToolLookupByLLMName();
		List<Map<String, Object>> enriched = new ArrayList<>();
		for (Map<String, Object> definition : definitions) {
			Map<String, Object> tool = new HashMap<>(definition);
			Map<String, Object> metadata = new HashMap<>();
			Map<String, Object> entry = lookup.get(text(definition.get("name")));
			if (entry != null && entry.get("_meta") instanceof Map<?, ?> meta) {
				meta.forEach((key, value) -> metadata.put(String.valueOf(key), value));
			}
			if (definition.get("_meta") instanceof Map<?, ?> meta) {
				meta.forEach((key, value) -> metadata.put(String.valueOf(key), value));
			}
			tool.put("_meta", metadata);
			enriched.add(tool);
		}
		return capabilitySummaries(enriched, context);
	}

	static List<Map<String, String>> capabilitySummaries(List<Map<String, Object>> resolved,
			Map<String, Object> context) {
		Set<String> unavailable = new LinkedHashSet<>();
		if (context.get("unavailableReactors") instanceof List<?> names) {
			for (Object name : names) {
				if (name instanceof String value) {
					unavailable.add(value);
				}
			}
		}
		Map<String, Map<String, String>> summaries = new LinkedHashMap<>();
		for (Map<String, Object> tool : resolved) {
			Map<?, ?> meta = tool.get("_meta") instanceof Map<?, ?> map ? map : Map.of();
			if (MCPUtility.MCPExecution.DISABLED.getValue().equals(meta.get(MCPUtility.SMSS_MCP_EXECUTION))
					|| unavailable.contains(meta.get(MCPUtility.SMSS_FUNCTION_NAME))) {
				continue;
			}
			String name = text(meta.get(MCPUtility.SMSS_ORIGINAL_TOOL_NAME));
			if (name.isBlank()) {
				name = text(tool.get("name"));
			}
			addCapability(summaries, name, text(tool.get("description")), text(meta.get(MCPUtility.SMSS_ENGINE_NAME)));
		}
		// Browser/folder tools are client-scoped. These descriptions only provide
		// generation context; they are never registered as executable tools.
		if (context.get("tools") instanceof List<?> tools) {
			for (Object value : tools) {
				if (value instanceof Map<?, ?> tool) {
					addCapability(summaries, text(tool.get("name")), text(tool.get("description")), "Browser/folder tools");
				}
			}
		}
		return new ArrayList<>(summaries.values());
	}

	private static void addCapability(Map<String, Map<String, String>> summaries, String name,
			String description, String toolbox) {
		if (!name.isBlank()) {
			summaries.putIfAbsent(toolbox + "\n" + name,
					Map.of("name", name, "description", description, "toolbox", toolbox));
		}
	}

	private static String text(Object value) {
		return value instanceof String string ? string.trim() : "";
	}

	static Map<String, Object> validateSuggestions(Map<String, Object> response, int count) {
		if (response == null || !(response.get("suggestions") instanceof List<?> values) || values.size() != count) {
			throw new IllegalStateException("Model returned an invalid number of follow-up suggestions");
		}
		List<String> suggestions = new ArrayList<>();
		Set<String> unique = new LinkedHashSet<>();
		for (Object value : values) {
			if (!(value instanceof String suggestion)) {
				throw new IllegalStateException("Follow-up suggestions must be strings");
			}
			String trimmed = suggestion.trim();
			if (trimmed.isBlank() || trimmed.length() > 240 || trimmed.split("\\s+").length > 24
					|| !unique.add(trimmed.replaceAll("\\s+", " ").toLowerCase(Locale.ROOT))) {
				throw new IllegalStateException("Model returned empty, oversized, or duplicate follow-up suggestions");
			}
			suggestions.add(trimmed);
		}
		return Map.of("suggestions", suggestions);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getNamedMap(String key) {
		GenRowStruct row = this.store.getGenRowStruct(key);
		if (row != null && !row.isEmpty()) {
			List<NounMetadata> maps = row.getNounsOfType(PixelDataType.MAP);
			if (!maps.isEmpty()) {
				return (Map<String, Object>) maps.get(0).getValue();
			}
		}
		return Map.of();
	}

	private static int parsePositiveInt(String value, int defaultValue) {
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		Map<String, Object> context = getNamedMap(SUGGESTION_CONTEXT);
		for (NounMetadata noun : this.curRow.getNounsOfType(PixelDataType.MAP)) {
			if (noun.getValue() != context) {
				return (Map<String, Object>) noun.getValue();
			}
		}
		return null;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (SUGGESTION_CONTEXT.equals(key) || ReactorKeysEnum.PARAM_VALUES_MAP.getKey().equals(key)) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Generates suggested follow-up messages from room history and currently attached capabilities.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The model engine that generates follow up suggestions.";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room id used to locate conversation history and attached capabilities.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional number of follow up suggestions to generate (defaults to 3).";
		}
		if (SUGGESTION_CONTEXT.equals(key)) {
			return "Optional current attachment snapshot: mcp id references, browser tool name/description summaries, and unavailableReactors. Context only; never authorizes execution.";
		}
		if (ReactorKeysEnum.PARENT_MESSAGE_ID.getKey().equals(key)) {
			return "Optional conversation branch anchor; defaults to the latest room message.";
		}
		return super.getDescriptionForKey(key);
	}
}
