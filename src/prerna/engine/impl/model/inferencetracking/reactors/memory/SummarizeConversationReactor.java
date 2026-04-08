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
 * Reactor that summarizes a room's conversation into a single SUMMARY memory.
 * <p>
 * Sends the conversation to an LLM with a summarization prompt, then stores
 * the result as a SUMMARY-type memory linked to the source room.
 * <p>
 * Pixel usage:
 * <pre>
 *   SummarizeConversation(roomId=["abc-123"], engine=["model-engine-id"]);
 * </pre>
 *
 * @see ModelInferenceLogsUtils#insertMemory(String, String, String, String, String, String)
 */
public class SummarizeConversationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SummarizeConversationReactor.class);

	/** Prompt sent to the LLM for conversation summarization. */
	static final String SUMMARIZATION_PROMPT =
			"Summarize the following conversation in 2-4 sentences. "
			+ "Focus on the key topics discussed, decisions made, and any action items. "
			+ "Write the summary as a factual record, not a narrative.";

	public SummarizeConversationReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.ENGINE.getKey()
		};
		this.keyRequired = new int[] { 1, 1 };
	}

	/**
	 * Loads the full conversation from a room and generates a summary via LLM.
	 * <p>
	 * Required parameters:
	 * <ul>
	 *   <li>{@code roomId} — the room to summarize</li>
	 *   <li>{@code engine} — the model engine ID to use for summarization</li>
	 * </ul>
	 *
	 * @return {@link NounMetadata} map containing {@code memoryId}, {@code summary}, and {@code status}
	 * @throws IllegalArgumentException if required params are missing or summarization fails
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in to summarize conversations");
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
			throw new IllegalArgumentException("Engine ID is required for summarization");
		}

		try {
			IModelEngine modelEngine = (IModelEngine) Utility.getEngine(engineId);

			// Load the room by ID and user
			Room room = ModelInferenceLogsUtils.getRoomById(roomId, userId);
			if (room == null) {
				throw new IllegalArgumentException("Room " + roomId + " not found for current user");
			}

			if (room.getMessages().isEmpty()) {
				throw new IllegalArgumentException("Room has no messages to summarize");
			}

			// Use serialized messages JSON — the most complete representation
			String messagesJson = room.getMessagesAsString();

			String fullPrompt = SUMMARIZATION_PROMPT + "\n\n--- CONVERSATION ---\n" + messagesJson;

			// Ask the LLM to summarize
			Map<String, Object> params = new HashMap<>();
			params.put("max_new_tokens", 500);
			params.put("temperature", 0.3);
			AskModelEngineResponse<?> response = modelEngine.ask(fullPrompt, null, this.insight, params);
			String summary = response.getResponse().toString().trim();

			// Store the summary as a SUMMARY memory
			String memoryId = java.util.UUID.randomUUID().toString();
			String metadata = GSON.toJson(Map.of("source", "conversation_summary", "roomId", roomId));
			ModelInferenceLogsUtils.insertMemory(memoryId, userId, roomId, "SUMMARY", summary, metadata);

			Map<String, Object> output = new HashMap<>();
			output.put("memoryId", memoryId);
			output.put("summary", summary);
			output.put("status", "stored");
			return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);

		} catch (IllegalArgumentException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to summarize conversation in room '{}'.", roomId, e);
			throw new IllegalArgumentException("Failed to summarize conversation: " + e.getMessage());
		}
	}

}
