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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.MessageUtils.ToolChoiceType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddCOTLLMReasoningReactor extends AbstractReactor {

	public AddCOTLLMReasoningReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0
				ReactorKeysEnum.ROOM_ID.getKey(), // 1
				"stepNumber", // 2
		};
		this.keyRequired = new int[] { 1, 1, 1, };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String modelId = this.keyValue.get(this.keysToGet[index++]);
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		String stepNumber = this.keyValue.get(this.keysToGet[index++]);

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		List<AbstractMessage> messages = room.getMessages();
		if (messages.isEmpty()) {
			throw new IllegalStateException(
					"Room message history is empty. Cannot run an LLM reasoning step before COT has been executed");
		}

		IModelEngine modelEngine = Utility.getModel(modelId);

		String userPrompt = String.format("Continue the chain of thought to perform reasoning for step: " + stepNumber);

		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("tool_choice", MessageUtils.makeToolChoice(ToolChoiceType.NONE, null));

		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(PlaygroundUtils.COT_SYSTEM_PROMPT)
				.withText(userPrompt, "Continuing with the next step").withModelType(modelEngine.getModelType())
				.withParamMap(paramMap).build();
		inputMsg.setPlatformGenerated(true);

		// Run LLM (not saving in history for now)
		ResponseMessage response = room.ask(inputMsg, modelEngine);
		response.setParentMessageId(inputMsg.getParentMessageId());

		// parse the response for code blocks
		// this should only be response text
		if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
			ModelInferenceLogsUtils.llm2_updateRoomMessages(room.getId(),
					insight.getUser().getPrimaryLoginToken().getId(), room.getMessagesAsString());
		} else if (response.getMessageType() == MessageType.RESPONSE_TOOL) {
			room.updateToolResponseMeta(response);
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();

		pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(response)));

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return """
				Add an LLM reasoning step for the Chain of Thought processing.
				""";
	}

}
