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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddCOTToolExecutionReactor extends AbstractReactor {

	public AddCOTToolExecutionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				"roomId", // 1
				"toolId", // 2
				"toolName", // 3
				"toolPredictedArguments", // 4
				"toolExecutionResponse", // 5
				"toolParameterValues", // 6
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), // 7
				ReactorKeysEnum.MCP_TOOL_STATUS.getKey() // 8
		};
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String modelId = this.keyValue.get(this.keysToGet[index++]);
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		String toolId = this.keyValue.get(this.keysToGet[index++]);
		String toolName = this.keyValue.get(this.keysToGet[index++]);
		Map<String, Object> toolPredictionArgs = getMap(this.keysToGet[index++]);
		String toolResponseRaw = this.keyValue.get(this.keysToGet[index++]);
		if (toolResponseRaw == null) {
			throw new IllegalArgumentException("Field " + this.keysToGet[index - 1] + " cannot be empty");
		}
		Map<String, Object> toolParamterValues = getMap(this.keysToGet[index++]);
		String parentMessageId = this.keyValue.get(this.keysToGet[index++]);

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
		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException("Room message history is empty. Cannot add tool execution results.");
		}
		if (parentMessageId == null) {
			AbstractMessage lastMessage = messages.getLast();
			parentMessageId = lastMessage.getMessageId();
		}

		// we will now mock and add a fake input message to get the tool
		// and then we will add the tool execution
		InputMessage toolExecutionMessage = null;
		ResponseMessage toolAcknowledgedMessage = null;
		// we will add the tool execution
		{
			String toolStatus = this.keyValue.get(this.keysToGet[index++]);
			toolExecutionMessage = InputMessage.toolExecution(room, toolId, toolName, toolResponseRaw,
					toolParamterValues, toolStatus, false);
			toolExecutionMessage.setSystemPrompt(room.getEffectiveSystemPrompt());
			toolExecutionMessage.setModel(modelEngine);
			toolExecutionMessage.setParentMessageId(parentMessageId);
		}
		// now we will fake a tool acknoledgement response from the LLM
		{
			toolAcknowledgedMessage = ResponseMessage
					.text("The tool execution has been confirmed with response: " + toolResponseRaw);
			toolAcknowledgedMessage.setOrnament(PlaygroundUtils.PLAYGROUND_MESSAGE_TYPE, "Tool Execution Acknowledged");
			toolAcknowledgedMessage.setPlatformGenerated(true);
			toolAcknowledgedMessage.setParentMessageId(toolExecutionMessage.getMessageId());
		}

		messages.add(toolExecutionMessage);
		messages.add(toolAcknowledgedMessage);

		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		pixelReturn.put("toolExecution", jsonToMap(MessageUtils.toJson(toolExecutionMessage)));
		pixelReturn.put("toolResponse", jsonToMap(MessageUtils.toJson(toolAcknowledgedMessage)));

		RoomMessageStore.persist(room, insight.getUser().getPrimaryLoginToken().getId());

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return """
				Add a tool execution input message to the message history.
				This will add 2 or 3 messages:
					1. Input message for calling the tool - only if the last response in the history was a response message
					2. Response message of the tool
					3. Input message for the tool execution
				This does not execute the messages, only appends to the message history.
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
		}
		return super.getDescriptionForKey(key);
	}

}
