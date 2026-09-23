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
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Adds a tool execution result to a room without applying Playground project
 * policy and returns the next paired room turn once every tool is answered.
 */
public class AddToolExecutionReactor extends AbstractReactor {

	private static final String TOOL_EXECUTION_RESPONSE = "toolExecutionResponse";
	private static final String TOOL_PARAMETER_VALUES = "toolParameterValues";

	@Deprecated
	private static final String LEGACY_TOOL_EXECUTION_RESPONSE = "tool_execution_response";

	private static final String RESPONSE_PARTS_KEY = "responseParts";
	private static final String HIDDEN_MESSAGE_KEY = "hiddenMessage";

	public AddToolExecutionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "roomId", "toolId", "toolName",
				TOOL_EXECUTION_RESPONSE, TOOL_PARAMETER_VALUES, ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), LEGACY_TOOL_EXECUTION_RESPONSE,
				ReactorKeysEnum.MCP_TOOL_STATUS.getKey(), RESPONSE_PARTS_KEY, HIDDEN_MESSAGE_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String toolId = this.keyValue.get("toolId");
		String toolName = this.keyValue.get("toolName");
		String toolResponseRaw = this.keyValue.get(TOOL_EXECUTION_RESPONSE);
		if (toolResponseRaw == null) {
			toolResponseRaw = this.keyValue.get(LEGACY_TOOL_EXECUTION_RESPONSE);
		}
		if (toolResponseRaw == null) {
			throw new IllegalArgumentException("Field " + TOOL_EXECUTION_RESPONSE + " cannot be empty");
		}

		Map<String, Object> toolParameterValues = getMap(TOOL_PARAMETER_VALUES);
		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		String toolStatus = this.keyValue.get(ReactorKeysEnum.MCP_TOOL_STATUS.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> responseParts = getList(RESPONSE_PARTS_KEY);
		String hiddenMessage = this.keyValue.get(HIDDEN_MESSAGE_KEY);

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		if (!ModelInferenceLogsUtils.validUserRoom(roomId, userId)) {
			throw new IllegalArgumentException("User does not have access to room " + roomId);
		}

		IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		String projectIdOverride = getProjectIdOverride();
		if (projectIdOverride != null) {
			room.setProjectId(projectIdOverride);
		}
		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
		}

		if (room.hasToolCallBeenAnswered(toolId)) {
			return new NounMetadata("Tool output not added: duplicate response for toolCallId " + toolId,
					PixelDataType.CONST_STRING);
		}

		Map<String, Object> pixelReturn = new HashMap<>();
		List<AbstractMessage> extraMessages = new ArrayList<>();
		try {
			ResponseMessage prebuiltResponse = responseParts != null
					? MessageUtils.buildResponseMessageFromParts(responseParts)
					: null;

			AskModelEngineResponse<?> response = room.addToolExecutionResult(toolId, toolName, toolResponseRaw,
					toolParameterValues, paramMap, parentMessageId, modelEngine, insight, toolStatus, prebuiltResponse);

			AbstractMessage tail = room.getMessages().isEmpty() ? null : room.getMessages().getLast();
			boolean followUpAppended = prebuiltResponse != null ? tail == prebuiltResponse : response != null;
			if (!followUpAppended) {
				pixelReturn.put("responseMessage",
						"Tool output added successfully. Additional tool executions required to continue");
				return new NounMetadata("Tool output added successfully", PixelDataType.CONST_STRING);
			}

			AbstractMessage inputMessage = room.getMessages().get(room.getMessages().size() - 2);
			ResponseMessage responseMessage = (ResponseMessage) tail;
			if (prebuiltResponse != null) {
				if (hiddenMessage != null && !hiddenMessage.isEmpty()
						&& responseMessage.getMessageType() == MessageType.RESPONSE_TEXT) {
					appendHiddenPairWithPersist(room, modelEngine, hiddenMessage, responseMessage.getMessageId(), userId,
							extraMessages);
				}
			} else if (responseMessage.getMessageType() == MessageType.RESPONSE_TEXT) {
				RoomMessageStore.persist(room, userId);
			} else if (responseMessage.getMessageType() == MessageType.RESPONSE_TOOL) {
				room.updateToolResponseMeta(responseMessage);
			}

			pixelReturn.put("inputMessage", jsonToMap(MessageUtils.toJson(inputMessage)));
			pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJson(responseMessage)));

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
			ClusterUtil.pushRoomAsync(room.getId());
		}
	}

	/**
	 * Returns a runtime-surface project override, or {@code null} to preserve the
	 * room's existing project ownership.
	 */
	protected String getProjectIdOverride() {
		return null;
	}

	private void appendHiddenPairWithPersist(Room room, IModelEngine modelEngine, String hiddenMessage,
			String hiddenParentId, String userId, List<AbstractMessage> extrasOut) {
		synchronized (room) {
			try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(room)) {
				RoomMessageStore.refreshFromLatestProjection(room, userId);
				boolean parentExists = room.getMessages().stream()
						.anyMatch(message -> hiddenParentId.equals(message.getMessageId()));
				if (!parentExists) {
					throw new IllegalStateException(
							"Cannot append hidden cancellation messages because the parent response no longer exists");
				}
				MessageUtils.appendHiddenPair(room, modelEngine, hiddenMessage, hiddenParentId, extrasOut);
				RoomMessageStore.persist(room, userId);
			}
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
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room id corresponding to the message history";
		} else if (key.equals("toolId")) {
			return "The id of the tool that was executed - must match the tool id of tool_response message";
		} else if (key.equals("toolName")) {
			return "The name of the tool that was executed - must match the tool name of tool_response message";
		} else if (key.equals(TOOL_EXECUTION_RESPONSE)) {
			return "The raw string output of the tool output";
		} else if (key.equals(TOOL_PARAMETER_VALUES)) {
			return "Map object with the string parameterName to object value for the tool execution";
		} else if (key.equals(LEGACY_TOOL_EXECUTION_RESPONSE)) {
			return "Deprecated parameter. Please switch to " + TOOL_EXECUTION_RESPONSE;
		} else if (RESPONSE_PARTS_KEY.equals(key)) {
			return "Optional. When provided, the LLM follow-up call is skipped and this array of response parts"
					+ " (each a map with type=THINKING|TEXT and matching payload) is persisted as the assistant"
					+ " follow-up. Used by a cancel flow when the user stopped a stream that fired after"
					+ " tool execution.";
		} else if (HIDDEN_MESSAGE_KEY.equals(key)) {
			return "Optional. Cancel-flow only (paired with " + RESPONSE_PARTS_KEY + "). A hidden user-side note"
					+ " appended after the tool follow-up, plus an auto-generated assistant ack, so the model sees on"
					+ " the next turn that its previous response was cut short. Ignored on live LLM calls.";
		}
		return super.getDescriptionForKey(key);
	}
}
