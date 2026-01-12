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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AskPlaygroundReactor extends AbstractReactor {

	private static Logger classLogger = LogManager.getLogger(AskPlaygroundReactor.class);

	public AskPlaygroundReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.IMAGE.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.VECTORDB.getKey(),
				ReactorKeysEnum.CHUNK_LIMIT.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		////// SET UP //////////
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		List<String> inputImages = getListString(ReactorKeysEnum.IMAGE.getKey());
		List<String> inputImageURLs = getListString(ReactorKeysEnum.URL.getKey());

		IModelEngine modelEngine = Utility.getModel(engineId);

		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
		room.setProjectId(PlaygroundUtils.PLAYGROUND_PROJECT_ID);

		// Collect vector database IDs and retrieve RAG context
		List<String> vectorDbIds = collectVectorDatabaseIds(room);
		int chunkLimit = getChunkLimit();
		String ragContext = retrieveRagContext(vectorDbIds, user, question, chunkLimit);
		String givenSystemPrompt = buildSystemPromptWithRag(room.getEffectiveSystemPrompt(), ragContext);

		List<String> copiedImages = MessageUtils.copyFilesToRoomFolder(inputImages, room, insight);

		// ---- Build the InputMessage
		InputMessage msg = InputMessage.builder(room).withSystemPrompt(givenSystemPrompt).withInputUIPrompt(question)
				.withInputPrompt(question).withModelType(modelEngine.getModelType()).withParamMap(paramMap)
				.withMediaInputs(copiedImages, room).withMediaUrls(inputImageURLs)
				// .withTools(tools)
				.build();

		// ---- Actually run LLM call
		ResponseMessage response = room.ask(msg, modelEngine, parentMessageId);

		// parse the response for code blocks
		if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
			response = MessageUtils.processMarkdownCodeBlocks(response, modelEngine, room);
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(),
					insight.getUser().getPrimaryLoginToken().getId(), room.getMessagesAsString());
		} else if (response.getMessageType() == MessageType.RESPONSE_TOOL) {
			MCPUtility.updateToolResponseWithProjectMeta(response);
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJsonWithImage(msg)));
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJsonWithImage(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call (Playground) returns both input and response message objects.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the prompt to execute against the LLM";
		} else if (key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
			return "The system prompt to use for the LLM call";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "This is the room ID that will be used for storing messages. If no room id is passed in, then insight id will be used for the room";
		} else if (key.equals(ReactorKeysEnum.IMAGE.getKey())) {
			return "This is  an array of image file names that have already been uploaded to the insight folder.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return """
					Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc.
					In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for
					<replacement>
					"""
					.replace("<replacement>", Arrays.asList(ReactorKeysEnum.COMMAND.getKey(),
							ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey()).toString());
		} else if (key.equals(ReactorKeysEnum.CHUNK_LIMIT.getKey())) {
			return "The number of chunks to retrieve per vector database for RAG context. Default is 5.";
		}
		return super.getDescriptionForKey(key);
	}

	/**
	 * Collects vector database IDs from reactor parameters, room MCP options, and workspace knowledge
	 * @param room The room to extract vector database IDs from
	 * @return List of vector database IDs
	 */
	private List<String> collectVectorDatabaseIds(Room room) {
		List<String> vectorDbIds = getListString(ReactorKeysEnum.VECTORDB.getKey());
		if (vectorDbIds == null) {
			vectorDbIds = new ArrayList<>();
		}

		Map<String, Object> roomOptions = room.getOptionsMap();
		if (roomOptions != null) {
			collectVectorDbsFromMcp(roomOptions, vectorDbIds);
			collectVectorDbsFromWorkspace(roomOptions, vectorDbIds);
		}

		return vectorDbIds;
	}

	/**
	 * Extracts vector database IDs from the room's MCP array
	 */
	private void collectVectorDbsFromMcp(Map<String, Object> roomOptions, List<String> vectorDbIds) {
		if (!roomOptions.containsKey("mcp")) {
			return;
		}

		try {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> mcpList = (List<Map<String, Object>>) roomOptions.get("mcp");
			if (mcpList != null) {
				for (Map<String, Object> mcpEntry : mcpList) {
					String type = (String) mcpEntry.get("type");
					String id = (String) mcpEntry.get("id");
					if ("VECTOR".equalsIgnoreCase(type) && id != null && !id.trim().isEmpty()) {
						vectorDbIds.add(id);
						classLogger.info("Added vector database from room MCP: " + id);
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * Extracts vector database IDs from the attached workspace
	 */
	private void collectVectorDbsFromWorkspace(Map<String, Object> roomOptions, List<String> vectorDbIds) {
		if (!roomOptions.containsKey("workspace")) {
			return;
		}

		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> workspace = (Map<String, Object>) roomOptions.get("workspace");
			if (workspace != null && workspace.containsKey("workspace_id")) {
				String workspaceId = (String) workspace.get("workspace_id");
				List<Map<String, Object>> workspaceKnowledge = ModelInferenceLogsUtils
						.getWorkspaceResourcesByType(workspaceId, prerna.engine.api.IEngine.CATALOG_TYPE.VECTOR.toString());

				if (workspaceKnowledge != null) {
					for (Map<String, Object> knowledgeEntry : workspaceKnowledge) {
						String knowledgeId = (String) knowledgeEntry.get("resource_id");
						if (knowledgeId != null && !knowledgeId.trim().isEmpty()) {
							vectorDbIds.add(knowledgeId);
							classLogger.info("Added vector database from workspace: " + knowledgeId);
						}
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * Gets the chunk limit parameter with default value of 5
	 * @return The chunk limit value
	 */
	private int getChunkLimit() {
		String chunkLimitStr = this.keyValue.get(ReactorKeysEnum.CHUNK_LIMIT.getKey());
		if (chunkLimitStr != null && !chunkLimitStr.isEmpty()) {
			try {
				return Integer.parseInt(chunkLimitStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid chunk limit value: " + chunkLimitStr + ". Using default of 5.");
			}
		}
		return 5;
	}

	/**
	 * Retrieves RAG context from vector databases using nearest neighbor search
	 * @param vectorDbIds List of vector database IDs
	 * @param user User for permission checking
	 * @param question The question to search for relevant context
	 * @param chunkLimit Number of chunks to retrieve per vector database
	 * @return RAG context as a string
	 */
	private String retrieveRagContext(List<String> vectorDbIds, User user, String question, int chunkLimit) {
		if (vectorDbIds == null || vectorDbIds.isEmpty()) {
			return "";
		}

		StringBuilder ragContextBuilder = new StringBuilder();

		for (String vectorDbId : vectorDbIds) {
			if (vectorDbId == null || vectorDbId.trim().isEmpty()) {
				continue;
			}
			if (!SecurityEngineUtils.userCanViewEngine(user, vectorDbId)) {
				classLogger.info("User does not have access to vector db: " + vectorDbId);
				continue;
			}

			prerna.engine.api.IVectorDatabaseEngine vectorDbEng = Utility.getVectorDatabase(vectorDbId);
			if (vectorDbEng == null) {
				continue;
			}

			List<Map<String, Object>> output = vectorDbEng.nearestNeighbor(this.insight, question, chunkLimit, null);
			for (Map<String, Object> chunk : output) {
				String content = (String) chunk.get(prerna.engine.impl.vector.VectorDatabaseCSVTable.CONTENT);
				if (content != null && !content.isEmpty()) {
					ragContextBuilder.append(content).append("\n");
				}
			}
		}

		return ragContextBuilder.toString();
	}

	/**
	 * Builds the system prompt with RAG context appended if available
	 * @param baseSystemPrompt The base system prompt from the room
	 * @param ragContext The RAG context to append
	 * @return The final system prompt
	 */
	private String buildSystemPromptWithRag(String baseSystemPrompt, String ragContext) {
		if (ragContext == null || ragContext.isEmpty()) {
			return baseSystemPrompt;
		}

		if (baseSystemPrompt != null && !baseSystemPrompt.isEmpty()) {
			return baseSystemPrompt + "\n\n## Relevant Knowledge:\n" + ragContext;
		} else {
			return "## Relevant Knowledge:\n" + ragContext;
		}
	}

}