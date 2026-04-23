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
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.MessageUtils.ToolChoiceType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class COTRoomResultReactor extends AbstractReactor {

	public COTRoomResultReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		////// SET UP //////////
		organizeKeys();
		String modelId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}
		IModelEngine modelEngine = Utility.getModel(modelId);

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("tool_choice", MessageUtils.makeToolChoice(ToolChoiceType.NONE, null));

		String prompt = "Provide a summary of each step in the plan and then the final result that answers the user's question with an explanation";
		InputMessage inputMsg = InputMessage.builder(room).withText(prompt, "Result of Plan Execution")
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		// Run LLM (not saving in history for now)
		ResponseMessage response = room.ask(inputMsg, modelEngine);

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
		return "This method is used to have the LLM provide a final result of the chain-of-thought execution";
	}

}
