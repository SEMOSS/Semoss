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
package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Generates values for Playwright TYPE steps using room conversation history
 * for context. Returns the model output along with the extracted JSON so the
 * player can update inputs by id.
 */
public class GenerateInputValuesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateInputValuesReactor.class);

	// pagination defaults
	private static final int DEFAULT_OFFSET = 0;
	private static final int DEFAULT_LIMIT = -1;
	private static final String DEFAULT_SORT_ORDER = "ASC";

	public GenerateInputValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "sessionId", ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), "sortOrder" };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String roomId = this.keyValue.get(this.keysToGet[2]);
		Map<String, Object> paramValues = getMap(this.keysToGet[3]);

		// optional pagination params
		int offset = getOptionalInt(ReactorKeysEnum.OFFSET.getKey(), DEFAULT_OFFSET);
		int limit = getOptionalInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT);
		String sortOrder = getOptionalSortOrder("sortOrder", DEFAULT_SORT_ORDER);

		Map<String, Object> result = generateValues(engineId, sessionId, roomId, paramValues, offset, limit, sortOrder);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	/**
	 * 
	 * @param engineId
	 * @param sessionId
	 * @param roomId
	 * @param params
	 * @param offset
	 * @param limit
	 * @param sortOrder
	 * @return
	 */
	private Map<String, Object> generateValues(String engineId, String sessionId, String roomId,
			Map<String, Object> params, int offset, int limit, String sortOrder) {
		try {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> inputs = (List<Map<String, Object>>) params.get("inputs");

			if (inputs == null || inputs.isEmpty()) {
				throw new IllegalArgumentException("inputs are required");
			}

			IModelEngine modelEngine = Utility.getModel(engineId);

			// TODO: should look into using a json schema instead of asking for json in
			// prompt

			// Use provided room id ONLY for history.
			// Always send prompt with a fresh room id.
			String historyRoomId = roomId;
			String promptRoomId = UUID.randomUUID().toString();

			String conversationHistory = buildConversationHistory(historyRoomId, offset, limit, sortOrder);
			String prompt = buildPrompt(inputs, conversationHistory);

			String modelOutput = null;
			try {
				Room room = RoomUtils.createRoomIfNotExists(promptRoomId, this.insight, modelEngine, null);
				InputMessage inputMessage = InputMessage.builder(room).withText(prompt).build();
				ResponseMessage response = room.ask(inputMessage, modelEngine);
				modelOutput = response.getContent();
			} catch (Exception e) {
				throw new RuntimeException("Model call failed: " + e.getMessage(), e);
			}

			String cleanedResponse = extractJsonArray(modelOutput);

			Map<String, Object> result = new HashMap<>();
			result.put("success", true);
			result.put("rawResponse", modelOutput);
			result.put("inputsJson", cleanedResponse);
			result.put("sessionId", sessionId);
			result.put("roomId", promptRoomId);
			result.put("historyRoomId", historyRoomId);
			result.put("offset", offset);
			result.put("limit", limit);
			result.put("sortOrder", sortOrder);
			return result;
		} catch (Exception e) {
			Map<String, Object> errorResult = new HashMap<>();
			errorResult.put("success", false);
			errorResult.put("error", e.getMessage());
			errorResult.put("rawResponse", "");
			return errorResult;
		}
	}

	/**
	 * 
	 * @param inputs
	 * @param conversationHistory
	 * @return
	 */
	private String buildPrompt(List<Map<String, Object>> inputs, String conversationHistory) {
		StringBuilder sb = new StringBuilder();
		sb.append("You are an assistant that fills form input values for an end-to-end Playwright recording.\n");
		sb.append("Use ONLY the conversation history to decide values. Ignore any existing default values.\n");
		sb.append("Return a JSON array only. Do not add markdown or explanations.\n\n");

		sb.append("CONVERSATION HISTORY:\n");
		if (conversationHistory == null || conversationHistory.isEmpty()) {
			sb.append("[]\n");
		} else {
			sb.append(conversationHistory).append("\n");
		}

		sb.append("\nINPUTS:\n");
		for (Map<String, Object> input : inputs) {
			sb.append("- id: ").append(input.getOrDefault("id", "")).append(", ");
			sb.append("label: ").append(input.getOrDefault("label", "")).append(", ");
			if (input.containsKey("placeholder")) {
				sb.append("placeholder: ").append(input.get("placeholder")).append(", ");
			}
			if (Boolean.TRUE.equals(input.get("isPassword"))) {
				sb.append("type: password, ");
			}
			if (input.containsKey("currentValue")) {
				sb.append("currentValue: ").append(input.get("currentValue"));
			}
			sb.append("\n");
		}

		sb.append("""

				OUTPUT FORMAT (JSON ONLY):
				[
				  { "id": "<input id>", "value": "<string to type>" }
				]

				Rules:
				- Provide a value for every input id.
				- If a clear value is not derivable, return an empty string for that id.
				- Overwrite any pre-filled values.
				- Do not include any additional keys besides id and value.
				""");
		return sb.toString();
	}

	/**
	 * 
	 * @param roomId
	 * @param offset
	 * @param limit
	 * @param sortOrder
	 * @return
	 */
	private String buildConversationHistory(String roomId, int offset, int limit, String sortOrder) {
		if (roomId == null || roomId.trim().isEmpty()) {
			return "";
		}

		try {
			Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
			List<AbstractMessage> messages = RoomUtils.getPagedMessages(room.getMessages(), sortOrder, offset, limit);
			List<Map<String, Object>> pairs = collectConversationHistory(messages).pairs;
			return buildHistoryString(pairs);
		} catch (Exception e) {
			classLogger.warn("Unable to load room history for {}", roomId, e);
			return "";
		}
	}

	private static final class ConversationHistory {
		private final List<Map<String, Object>> pairs;
		@SuppressWarnings("unused")
		private final Map<String, Object> pendingQuestion;

		private ConversationHistory(List<Map<String, Object>> pairs, Map<String, Object> pendingQuestion) {
			this.pairs = pairs;
			this.pendingQuestion = pendingQuestion;
		}
	}

	/**
	 * 
	 * @param messages
	 * @return
	 */
	private ConversationHistory collectConversationHistory(List<AbstractMessage> messages) {
		List<Map<String, Object>> pairs = new ArrayList<>();
		InputMessage pendingInput = null;

		for (AbstractMessage message : messages) {
			if (message == null || !message.isVisible()) {
				continue;
			}
			if (message instanceof InputMessage inputMessage) {
				pendingInput = inputMessage;
			} else if (message instanceof ResponseMessage responseMessage) {
				if (pendingInput == null) {
					continue;
				}
				pairs.add(buildPair(pendingInput, responseMessage));
				pendingInput = null;
			}
		}

		Map<String, Object> pendingQuestion = null;
		if (pendingInput != null) {
			pendingQuestion = buildPair(pendingInput, null);
		}

		return new ConversationHistory(pairs, pendingQuestion);
	}

	/**
	 * 
	 * @param question
	 * @param answer
	 * @return
	 */
	private Map<String, Object> buildPair(InputMessage question, ResponseMessage answer) {
		Map<String, Object> pair = new LinkedHashMap<>();
		String questionText = question != null
				? firstNonBlank(question.getInputUIPrompt(), question.getFullInputPrompt())
				: null;
		pair.put("question", questionText);

		String answerText = answer != null ? extractAnswerText(answer) : null;
		pair.put("answer", answerText);
		return pair;
	}

	/**
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	private String firstNonBlank(String first, String second) {
		if (first != null && !first.trim().isEmpty()) {
			return first;
		}
		if (second != null && !second.trim().isEmpty()) {
			return second;
		}
		return null;
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	private String extractAnswerText(ResponseMessage response) {
		String answer = response.getContent();
		if (answer == null || answer.trim().isEmpty()) {
			answer = response.getThinking();
		}
		if ((answer == null || answer.trim().isEmpty()) && response.hasToolResponses()) {
			answer = response.getToolResponses().toString();
		}
		return answer;
	}

	/**
	 * 
	 * @param pairs
	 * @return
	 */
	private String buildHistoryString(List<Map<String, Object>> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < pairs.size(); i++) {
			Map<String, Object> pair = pairs.get(i);
			String question = (String) pair.get("question");
			String answer = (String) pair.get("answer");
			sb.append("User: ").append(question != null ? question : "");
			sb.append("\nAssistant: ").append(answer != null ? answer : "");
			if (i < pairs.size() - 1) {
				sb.append("\n---\n");
			}
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param response
	 * @return
	 */
	private String extractJsonArray(String response) {
		if (response == null) {
			return "";
		}

		String cleaned = response.replaceAll("(?s)```json|```", "");

		int start = cleaned.indexOf('[');
		int end = cleaned.lastIndexOf(']');

		if (start >= 0 && end > start) {
			return cleaned.substring(start, end + 1);
		}

		return cleaned;
	}

	/**
	 * Safely read an optional integer key, falling back to the given default when
	 * the key is missing or cannot be parsed.
	 * 
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private int getOptionalInt(String key, int defaultValue) {
		String raw = this.keyValue.get(key);
		if (raw == null || raw.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			return Integer.parseInt(raw.trim());
		} catch (NumberFormatException e) {
			classLogger.warn("Invalid integer for key '{}': {}. Using default {}", key, raw, defaultValue);
			return defaultValue;
		}
	}

	/**
	 * Safely read an optional sort order (ASC/DESC), falling back to default when
	 * missing or invalid.
	 * 
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private String getOptionalSortOrder(String key, String defaultValue) {
		String raw = this.keyValue.get(key);
		if (raw == null || raw.trim().isEmpty()) {
			return defaultValue;
		}
		String upper = raw.trim().toUpperCase();
		if (!"ASC".equals(upper) && !"DESC".equals(upper)) {
			classLogger.warn("Invalid sortOrder for key '{}': {}. Using default {}", key, raw, defaultValue);
			return defaultValue;
		}
		return upper;
	}

	@Override
	public String getReactorDescription() {
		return "Generate input values for Playwright TYPE steps using room conversation history.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the Model Engine";
		} else if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "Optional room id to use as conversation context for generation.";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Optional starting index for conversation history pagination (default 0).";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional max number of conversation messages to use (default -1 for all).";
		} else if (key.equals("sortOrder")) {
			return "Optional sort order for conversation history messages: 'ASC' or 'DESC' (default ASC).";
		}

		return super.getDescriptionForKey(key);
	}

}
