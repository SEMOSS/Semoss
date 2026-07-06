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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * AddToolExecutionReactor: Input: roomId, toolId, toolName,
 * tool_execution_response, tool_
 */
public class AddPlaygroundToolExecutionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AddPlaygroundToolExecutionReactor.class);

	@Deprecated
	private final String tool_execution_response = "tool_execution_response";

	/**
	 * When present, the LLM follow-up call is skipped and the reactor persists
	 * the tool-result input plus a follow-up assembled from the caller-supplied
	 * response parts. Used by the FE cancel flow when the user stopped a stream
	 * that fired after a tool completed.
	 */
	private static final String RESPONSE_PARTS_KEY = "responseParts";

	/**
	 * Cancel-flow only. When paired with {@link #RESPONSE_PARTS_KEY}, a hidden
	 * user note carrying this string is appended after the tool follow-up (plus
	 * an auto-generated assistant ack) so the model sees on the next turn that
	 * its previous response was cut short. Ignored on live LLM calls.
	 */
	private static final String HIDDEN_MESSAGE_KEY = "hiddenMessage";

	public AddPlaygroundToolExecutionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				"roomId", // 1
				"toolId", // 2
				"toolName", // 3
				"toolExecutionResponse", // 4
				"toolParameterValues", // 5
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), // 6
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), // 7
				tool_execution_response, // 8
				ReactorKeysEnum.MCP_TOOL_STATUS.getKey(), // 9
				RESPONSE_PARTS_KEY, // 10
				HIDDEN_MESSAGE_KEY, // 11
		};
		// TODO: once we remove the legacy tool_execution_response, we will make
		// toolExecutionResponse mandatory field
		this.keyRequired = new int[] { 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		String roomId = this.keyValue.get(this.keysToGet[1]);
		String toolId = this.keyValue.get(this.keysToGet[2]);
		String toolName = this.keyValue.get(this.keysToGet[3]);
		String toolResponseRaw = this.keyValue.get(this.keysToGet[4]);
		if (toolResponseRaw == null) {
			toolResponseRaw = this.keyValue.get(tool_execution_response);
		}
		if (toolResponseRaw == null) {
			throw new IllegalArgumentException("Field " + this.keysToGet[4] + " cannot be empty");
		}
		Map<String, Object> toolParamterValues = getMap(this.keysToGet[5]);
		String parentMessageId = this.keyValue.get(this.keysToGet[6]);
		Map<String, Object> paramMap = getMap(this.keysToGet[7]);
		String toolStatus = this.keyValue.get(this.keysToGet[9]);
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> responseParts = getList(RESPONSE_PARTS_KEY);
		String hiddenMessage = this.keyValue.get(HIDDEN_MESSAGE_KEY);

		User user = this.insight.getUser();
		String userId = user.getPrimaryLoginToken().getId();

		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}
		IModelEngine modelEngine = Utility.getModel(modelId);

		// --- 1. Security/room loading ---
		if (!ModelInferenceLogsUtils.validUserRoom(roomId, userId)) {
			throw new IllegalArgumentException("User does not have access to room " + roomId);
		}
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		room.setProjectId(PlaygroundUtils.PLAYGROUND_PROJECT_ID);

		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
		}

		Map<String, Object> pixelReturn = new HashMap<>();
		// Collects any non-visible messages persisted alongside the visible pair
		// (currently only the cancel-flow hidden pair) so they can be surfaced
		// back to the FE via `extraMessages`. Always emitted, empty on live turns.
		List<AbstractMessage> extraMessages = new ArrayList<>();
		try {
			ResponseMessage prebuiltResponse = responseParts != null
					? PlaygroundUtils.buildResponseMessageFromParts(responseParts) : null;

			AskModelEngineResponse response = room.addToolExecutionResult(toolId, toolName, toolResponseRaw,
					toolParamterValues, paramMap, parentMessageId, modelEngine, insight, toolStatus, prebuiltResponse);

			// Two null-return cases: live LLM call short-circuits when more tools
			// are still pending (existing behavior), and the cancel-persistence
			// path always returns null because there is no llmResponse to hand
			// back. Route on prebuiltResponse to disambiguate.
			if (response == null && prebuiltResponse == null) {
				pixelReturn.put("responseMessage",
						"Tool output added successfully. Additional tool executions required to continue");
				return new NounMetadata("Tool output added successfully", PixelDataType.CONST_STRING);
			}

			// parse the response for code blocks
			AbstractMessage inputMessage = room.getMessages().get(room.getMessages().size() - 2);
			ResponseMessage lastMessage = (ResponseMessage) room.getMessages().getLast();

			if (prebuiltResponse != null) {
				// Cancel path: the FE-supplied response is already persisted by
				// Room#addToolExecutionResult. Append the optional hidden
				// user-note / assistant-ack pair after it so the model sees on
				// the next turn that its response was cut short.
				if (hiddenMessage != null && !hiddenMessage.isEmpty()) {
					PlaygroundUtils.appendHiddenPair(room, modelEngine, hiddenMessage, lastMessage.getMessageId(),
							insight.getUser().getPrimaryLoginToken().getId(), extraMessages);
				}
			} else if (lastMessage.getMessageType() == MessageType.RESPONSE_TEXT) {
				RoomMessageStore.persist(room, insight.getUser().getPrimaryLoginToken().getId());
			} else if (lastMessage.getMessageType() == MessageType.RESPONSE_TOOL) {
				room.updateToolResponseMeta(lastMessage);
			}

			Map<String, Object> inputMap = jsonToMap(MessageUtils.toJson(inputMessage));
			Map<String, Object> responseMap = jsonToMap(MessageUtils.toJson(lastMessage));
			pixelReturn.put("inputMessage", inputMap);
			pixelReturn.put("responseMessage", responseMap);

			// Extra (non-visible) input/response pairs — same shape as
			// {inputMessage, responseMessage} above, repeated per extra turn.
			// Currently only populated on the cancel path with the hidden
			// user-note/assistant-ack pair. Assumes extras arrive strictly as
			// InputMessage+Response couplets in conversation order (which
			// appendHiddenPair guarantees).
			List<Map<String, Object>> extraMessagesList = new ArrayList<>();
			for (int i = 0; i + 1 < extraMessages.size(); i += 2) {
				Map<String, Object> pair = new LinkedHashMap<>();
				pair.put("inputMessage", jsonToMap(MessageUtils.toJson(extraMessages.get(i))));
				pair.put("responseMessage", jsonToMap(MessageUtils.toJson(extraMessages.get(i + 1))));
				extraMessagesList.add(pair);
			}
			pixelReturn.put("extraMessages", extraMessagesList);

			return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
		} finally {
			// there might be times when this is unnecessary
			// but we dont know if a tool output generated a file in the room
			ClusterUtil.pushRoomAsync(room.getId());
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Add a tool execution input message to the message history.
				If all the tools have been executed from the previous tool response message, this will return the LLM response.
				Otherwise, a default string message that more tools responses are needed
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id of the model used for the message. If all the tools are added for the tool_resposne message, this model is used to invoke for the response.";
		} else if (key.equals("roomId")) {
			return "The room id corresponding to the message history";
		} else if (key.equals("toolId")) {
			return "The id of the tool that was executed - must match the tool id of tool_response message";
		} else if (key.equals("toolName")) {
			return "The name of the tool that was executed - must match the tool name of tool_response message";
		} else if (key.equals("toolExecutionResponse")) {
			return "The raw string output of the tool output";
		} else if (key.equals("toolParameterValues")) {
			return "Map object with the string parameterName to object value for the tool execution";
		} else if (key.equals(tool_execution_response)) {
			return "Deprecated parameter. Please switch to toolExecutionResponse";
		} else if (RESPONSE_PARTS_KEY.equals(key)) {
			return "Optional. When provided, the LLM follow-up call is skipped and this array of response parts"
					+ " (each a map with type=THINKING|TEXT and matching payload) is persisted as the assistant"
					+ " follow-up. Used by the FE cancel flow when the user stopped a stream that fired after"
					+ " tool execution.";
		} else if (HIDDEN_MESSAGE_KEY.equals(key)) {
			return "Optional. Cancel-flow only (paired with " + RESPONSE_PARTS_KEY + "). A hidden user-side note"
					+ " appended after the tool follow-up, plus an auto-generated assistant ack, so the model sees on"
					+ " the next turn that its previous response was cut short. Ignored on live LLM calls.";
		}
		return super.getDescriptionForKey(key);
	}
}
