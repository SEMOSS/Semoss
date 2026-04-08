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
package prerna.engine.impl.model.inferencetracking.reactors.memory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import prerna.util.Utility;

/**
 * Reactor that sends a conversation's recent messages to an LLM to extract
 * key facts, user preferences, and important decisions as persistent memories.
 * <p>
 * This is the manual/scheduled extraction component of the hybrid memory
 * approach. Background extraction also runs automatically via
 * {@link prerna.reactor.agent.RoomAgentHarness} after each conversation turn
 * when memory is enabled.
 * <p>
 * Pixel usage:
 * <pre>
 *   ExtractMemoriesFromRoom(roomId=["abc-123"], engine=["model-engine-id"]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#insertMemory(String, String, String, String, String, String)
 */
public class ExtractMemoriesFromRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExtractMemoriesFromRoomReactor.class);

	/** Prompt sent to the LLM for memory extraction. */
	static final String EXTRACTION_PROMPT =
			"Extract key facts, user preferences, and important decisions from this conversation. "
			+ "Only extract information that would be useful to remember across future conversations. "
			+ "Do NOT extract trivial or obvious things. Do NOT extract greetings or small talk. "
			+ "Return ONLY a JSON array, no other text. Each element should have: "
			+ "[{\"memoryType\": \"FACT\"|\"PREFERENCE\"|\"EPISODE\", \"content\": \"...\"}]"
			+ "\n\nIf there is nothing worth remembering, return an empty array: []";

	public ExtractMemoriesFromRoomReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.ENGINE.getKey()
		};
		this.keyRequired = new int[] { 1, 1 };
	}

	/**
	 * Loads recent messages from a room and sends them to an LLM for memory extraction.
	 * <p>
	 * Required parameters:
	 * <ul>
	 *   <li>{@code roomId} — the room whose messages to extract from</li>
	 *   <li>{@code engine} — the model engine ID to use for extraction</li>
	 * </ul>
	 * Optional parameters:
	 * <ul>
	 *   <li>{@code limit} — number of recent messages to consider (default: 20)</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code extracted} count and {@code memories} list
	 * @throws IllegalArgumentException if required params are missing or extraction fails
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to extract memories");
		}
		if (user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User authentication token is missing");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("Room ID is required");
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Engine ID is required for memory extraction");
		}

		try {
			IModelEngine modelEngine = (IModelEngine) Utility.getEngine(engineId);

			// Load room by ID and user
			Room room = ModelInferenceLogsUtils.getRoomById(roomId, userId);
			if (room == null) {
				throw new IllegalArgumentException("Room " + roomId + " not found for current user");
			}

			java.util.List<prerna.engine.impl.model.message.AbstractMessage> allMessages = room.getMessages();
			if (allMessages.isEmpty()) {
				Map<String, Object> output = new HashMap<>();
				output.put("extracted", 0);
				output.put("memories", List.of());
				return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
			}

			// Use the serialized messages JSON — the most complete representation
			String messagesJson = room.getMessagesAsString();

			String fullPrompt = EXTRACTION_PROMPT + "\n\n--- CONVERSATION ---\n" + messagesJson;

			// Ask the LLM to extract memories
			Map<String, Object> params = new HashMap<>();
			params.put("max_new_tokens", 2000);
			params.put("temperature", 0.1);
			AskModelEngineResponse<?> response = modelEngine.ask(fullPrompt, null, this.insight, params);
			String responseText = response.getResponse().toString();

			// Parse the JSON array response
			List<Map<String, Object>> extractedMemories = parseExtractionResponse(responseText);

			// Store each extracted memory
			int stored = 0;
			for (Map<String, Object> mem : extractedMemories) {
				String memoryType = (String) mem.getOrDefault("memoryType", "FACT");
				String content = (String) mem.get("content");
				if (content != null && !content.trim().isEmpty()) {
					String memoryId = java.util.UUID.randomUUID().toString();
					String metadata = GSON.toJson(Map.of("source", "auto_extraction", "roomId", roomId));
					ModelInferenceLogsUtils.insertMemory(memoryId, userId, roomId, memoryType, content, metadata);
					stored++;
				}
			}

			Map<String, Object> output = new HashMap<>();
			output.put("extracted", stored);
			output.put("memories", extractedMemories);
			return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);

		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to extract memories from room '{}'.", roomId, e);
			throw new IllegalArgumentException("Failed to extract memories: " + e.getMessage());
		}
	}

	/**
	 * Parses the LLM extraction response into a list of memory maps.
	 * Handles various response formats gracefully (JSON array, wrapped in markdown, etc.).
	 *
	 * @param responseText raw LLM output
	 * @return list of extracted memory maps with "memoryType" and "content" keys
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> parseExtractionResponse(String responseText) {
		if (responseText == null || responseText.trim().isEmpty()) {
			return List.of();
		}

		// Strip markdown code fence if present
		String cleaned = responseText.trim();
		if (cleaned.startsWith("```")) {
			int firstNewline = cleaned.indexOf('\n');
			int lastFence = cleaned.lastIndexOf("```");
			if (firstNewline > 0 && lastFence > firstNewline) {
				cleaned = cleaned.substring(firstNewline + 1, lastFence).trim();
			}
		}

		// Find the JSON array boundaries
		int start = cleaned.indexOf('[');
		int end = cleaned.lastIndexOf(']');
		if (start < 0 || end < 0 || end <= start) {
			return List.of();
		}
		String jsonArray = cleaned.substring(start, end + 1);

		try {
			return GSON.fromJson(jsonArray, List.class);
		} catch (Exception e) {
			classLogger.warn("Failed to parse extraction response as JSON: {}", e.getMessage());
			return List.of();
		}
	}

}
