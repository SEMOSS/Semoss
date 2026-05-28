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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
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

public class COTConfirmationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(COTConfirmationReactor.class);

	public COTConfirmationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0, required
				ReactorKeysEnum.ROOM_ID.getKey(), // 1, required
				"cotPlan", // 2, required (json from FE)
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 3, optional
		};
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String modelId = this.keyValue.get(this.keysToGet[index++]);
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		// User query is not vital here, included for completeness
		String cotPlanStr = this.keyValue.get(this.keysToGet[index++]); // JSON string

		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		List<AbstractMessage> messages = room.getMessages();

		Map<String, Object> paramMap = getMap(this.keysToGet[index++]);
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}
		paramMap.put("tool_choice", MessageUtils.makeToolChoice(ToolChoiceType.NONE, null));

		// Compose input prompt for LLM -- just feed the JSON plan string
		final String inputPrompt = PlaygroundUtils.CONFIRM_COT_PLAN.formatted(cotPlanStr);

		// we will now mock the input and response
		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(PlaygroundUtils.COT_SYSTEM_PROMPT)
				.withText(inputPrompt, "Confirmed Plan").withModelType(modelEngine.getModelType())
				.withParamMap(paramMap).build();
		inputMsg.setParentMessageId(room.getMessages().getLast().getMessageId());
		inputMsg.setTransactionId(inputMsg.getMessageId());

		ResponseMessage response = ResponseMessage.text("Your plan has been confirmed");
		response.setOrnament(PlaygroundUtils.PLAYGROUND_MESSAGE_TYPE, "COT_CONFIRM");
		response.setParentMessageId(inputMsg.getMessageId());
		response.setTransactionId(response.getMessageId());

		messages.add(inputMsg);
		messages.add(response);

		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(response)));
		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return """
				Confirms the chain-of-thought JSON plan by the user.
				This does not execute any messages against the LLM but appends an input/response to the message history
				""";
	}
}
