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
package prerna.playground.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPToolDiscoveryService;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AskCOTRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(Room.class);

	public AskCOTRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0, required
				ReactorKeysEnum.VECTORDB.getKey(), // 1, optional (can be null)
				ReactorKeysEnum.ROOM_ID.getKey(), // 2, optional (not required, will use insight)
				ReactorKeysEnum.COMMAND.getKey(), // 3, required (actual user query)
				ReactorKeysEnum.CONTEXT.getKey(), // 4, tbd on how it is used
				ReactorKeysEnum.IMAGE.getKey(), // 5, optional, TODO: add in support
				ReactorKeysEnum.URL.getKey(), // 6, optional, TODO: add in support
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 7, optional
		};

		this.keyRequired = new int[] { 1, 0, 0, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// Required
		String modelId = this.keyValue.get(this.keysToGet[0]);
		String userQuery = this.keyValue.get(this.keysToGet[3]);
		// Optional
		List<String> vectorDbIds = getListString(ReactorKeysEnum.VECTORDB.getKey());
		String roomId = this.keyValue.get(this.keysToGet[2]);
		// context, images, URLs: future - see keysToGet map

		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		// Room and Engine
		IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, userQuery);

		// ==== Step 1. Grab RAG context if vectorDB present ====
		StringBuilder joinedContextBuilder = new StringBuilder();

		if (vectorDbIds != null && !vectorDbIds.isEmpty()) {
			int chunkLimit = 3; // TODO: configurable
			for (String vectorDbId : vectorDbIds) {
				if (vectorDbId == null || vectorDbId.trim().isEmpty()) {
					continue;
				}
				if (!SecurityEngineUtils.userCanViewEngine(user, vectorDbId)) {
					classLogger.info("User does not have access to vector db : " + vectorDbId);
					continue;
				}
				IVectorDatabaseEngine vectorDbEng = Utility.getVectorDatabase(vectorDbId);
				if (vectorDbEng == null) {
					continue; // Or log as missing/bad engine.
				}

				List<Map<String, Object>> output = vectorDbEng.nearestNeighbor(this.insight, userQuery, chunkLimit,
						null);
				for (Map<String, Object> chunk : output) {
					String content = (String) chunk.get(VectorDatabaseCSVTable.CONTENT);
					if (content != null && !content.isEmpty()) {
						joinedContextBuilder.append(content).append("\n");
					}
				}
			}
		}
		String joinedChunks = joinedContextBuilder.toString();
		// ==== Step 2. Gather Tool Descriptions from the room ====
		List<String> mcpToolNames = new ArrayList<>();
		List<Map<String, Object>> toolMap = room.getAllToolsJsonForRoom();
		for (Map<String, Object> tool : toolMap) {
			mcpToolNames.add((String) tool.get("name"));
		}
		String toolsDescription = GSON.toJson(room.getAllToolsJsonForRoom());

		// ==== Step 3. Build Prompts ====
		// String cotSchema = PlaygroundUtils.COT_JSON_SCHEMA.replaceAll("\\s+", " ");
		String userPrompt = String.format(PlaygroundUtils.COT_PROMPT_TEMPLATE, toolsDescription, joinedChunks,
				userQuery);

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		// get all the mcp IDs and format them into a string
		String formattedEnum;
		if (mcpToolNames.isEmpty()) {
			formattedEnum = "\"none, DO NOT CHOOSE THIS ANY OF.\""; // this is some experimentation ...
		} else {
			formattedEnum = mcpToolNames.stream().map(t -> "\"" + t + "\"").collect(Collectors.joining(", "));
		}

		// put the string of mcp ids into the below
		String formattedSchemaJson = PlaygroundUtils.COT_JSON_SCHEMA_NO_HUMAN_INTERVENTION.formatted(formattedEnum);
		Map<String, Object> jsonSchemaMap = jsonToMap(formattedSchemaJson);

		paramMap.put("schema", jsonSchemaMap);
		// can only send tool_choice if tools exist
		if (!"[]".equals(toolsDescription)) {
			paramMap.put("tool_choice", MessageUtils.makeToolChoice(MessageUtils.ToolChoiceType.NONE, null));
		}

		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(PlaygroundUtils.COT_SYSTEM_PROMPT)
				.withText(userPrompt, userQuery).withModelType(modelEngine.getModelType()).withParamMap(paramMap)
				.build(); //

		// ==== Step 4. Run LLM ====
		ResponseMessage response = room.ask(inputMsg, modelEngine);

		// ==== Step 5. Try to parse as COT JSON ====
		Map<String, Object> cotJson = null;
		boolean isValidJson = false;
		try {
			cotJson = jsonToMap(response.getContent());
			isValidJson = (cotJson != null && cotJson.containsKey("steps"));
		} catch (Exception ex) {
			classLogger.info("Could not parse response from model into a COT json");
			classLogger.error(Constants.STACKTRACE, ex);
		}

		if (isValidJson) {
			// let the FE know this is a COT
			response.setOrnament(PlaygroundUtils.PLAYGROUND_MESSAGE_TYPE, "COT");

			// edit the COT to add a uuid to the plan
			cotJson.put("plan_id", GUID.v7().toUUID().toString());

			// need to also clean up the tools
			List<Map<String, Object>> steps = (List<Map<String, Object>>) cotJson.get("steps");
			for (Map<String, Object> thisStep : steps) {
				String thisStepType = (String) thisStep.get("type");
				if ("tool_call".equals(thisStepType)) {
					MCPUtility.updateCOTToolStepWithEngineMeta(thisStep);
				} else if ("no_tool_available".equals(thisStepType)) {
					addToolRecommendations(user, thisStep, userQuery);
				}
			}

			response.setContent(GSON.toJson(cotJson));
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);

	}

	/**
	 * Adds MCP tool recommendations to a COT step when the model reports that no
	 * current room tool can perform the required action.
	 *
	 * @param user      logged-in user used for MCP tool security filtering.
	 * @param step      COT step containing the missing tool details.
	 * @param userQuery original user request used as a fallback search query.
	 */
	private void addToolRecommendations(User user, Map<String, Object> step, String userQuery) {
		String recommendationQuery = null;
		Object detailsObj = step.get("details");
		if (detailsObj instanceof Map<?, ?> details) {
			Object missingCapability = details.get("missing_capability");
			if (missingCapability != null) {
				recommendationQuery = missingCapability.toString();
			}
		}
		if (recommendationQuery == null || recommendationQuery.trim().isEmpty()) {
			Object description = step.get("description");
			if (description != null) {
				recommendationQuery = description.toString();
			}
		}
		if (recommendationQuery == null || recommendationQuery.trim().isEmpty()) {
			recommendationQuery = userQuery;
		}

		Map<String, Object> recommendationResponse;
		try {
			recommendationResponse = new MCPToolDiscoveryService().recommend(user, recommendationQuery, null, 5);
		} catch (Exception e) {
			classLogger.debug("Unable to recommend MCP tools for missing capability", e);
			return;
		}

		Object results = recommendationResponse.get("results");
		if (results instanceof List<?> resultList && !resultList.isEmpty()) {
			Map<String, Object> recommendationPayload = new LinkedHashMap<>();
			recommendationPayload.put("message", "These tools might help. Add to room?");
			recommendationPayload.put("results", resultList);
			step.put("tool_recommendations", recommendationPayload);
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Takes a user's query and returns a chain-of-thought JSON plan by combining tool descriptions,
				RAG context (if vector db is provided), and the original query according to a strict JSON schema.
				If the output matches the schema, the type is 'COT'. Otherwise, returns as simple chat (type 'CHAT').
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id of the model used for the message.";
		} else if (key.equals(ReactorKeysEnum.VECTORDB.getKey())) {
			return "The vector db for knowledge search for this room/query (optional).";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room id corresponding to message history (optional, used for context/history/tools if provided).";
		} else if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "The raw user query.";
		}
		return super.getDescriptionForKey(key);
	}
}
