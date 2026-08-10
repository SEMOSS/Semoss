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
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.PlaygroundThemeUtils;
import prerna.util.Utility;

public class AskPlaygroundReactor extends AbstractReactor {

	private static Logger classLogger = LogManager.getLogger(AskPlaygroundReactor.class);

	// When present, skips the LLM call and persists a turn built from these parts (cancel flow).
	private static final String RESPONSE_PARTS_KEY = "responseParts";

	// Cancel-flow only: hidden user note appended after the visible turn, paired with {@link #RESPONSE_PARTS_KEY}.
	private static final String HIDDEN_MESSAGE_KEY = "hiddenMessage";

	public AskPlaygroundReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.IMAGE.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), RESPONSE_PARTS_KEY, HIDDEN_MESSAGE_KEY };
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

		String question = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		List<String> inputImages = getListString(ReactorKeysEnum.IMAGE.getKey());
		List<String> inputImageURLs = getListString(ReactorKeysEnum.URL.getKey());

		@SuppressWarnings("unchecked")
		List<Map<String, Object>> responseParts = getList(RESPONSE_PARTS_KEY);
		String hiddenMessage = this.keyValue.get(HIDDEN_MESSAGE_KEY);

		IModelEngine modelEngine = Utility.getModel(engineId);

		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question, null, null, null,
				PlaygroundUtils.PLAYGROUND_PROJECT_ID, null);
		room.setProjectId(PlaygroundUtils.PLAYGROUND_PROJECT_ID);

		String givenSystemPrompt = room.getSystemPromptForModel();

		List<String> copiedImages = RoomUtils.copyFilesToRoomFolder(inputImages, room, insight);

		// ---- Build the InputMessage
		InputMessage msg = InputMessage.builder(room).withSystemPrompt(givenSystemPrompt)
				.withMediaInputs(copiedImages, room).withMediaUrls(inputImageURLs).withText(question)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap)
				// .withTools(tools)
				.build();

		// Non-visible messages (e.g. cancel-flow hidden pair) surfaced back to the FE via `extraMessages`.
		List<AbstractMessage> extraMessages = new ArrayList<>();

		ResponseMessage response;
		if (responseParts != null) {
			// Cancel flow: skip the LLM call, persist a turn built from the caller-supplied parts.
			response = room.commitPrebuiltTurn(msg, modelEngine, parentMessageId, responseParts, hiddenMessage,
					extraMessages);
		} else {
			response = room.ask(msg, modelEngine, parentMessageId);

			// parse the response for code blocks
			if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
				RoomMessageStore.persist(room, insight.getUser().getPrimaryLoginToken().getId());
			} else if (response.getMessageType() == MessageType.RESPONSE_TOOL) {
				room.updateToolResponseMeta(response);
			}
		}

		// ---- Return both messages as a Map
		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		boolean hideSystemMessages = PlaygroundThemeUtils.hidePlaygroundSystemMessages();

		Map<String, Object> inputMap = jsonToMap(MessageUtils.toJsonWithImage(msg));
		if (hideSystemMessages) {
			MessageUtils.removeSystemPromptFromMessageMap(inputMap);
		}
//		MessageUtils.applyLegacyInputFields(msg, inputMap);
		pixelReturn.put("inputMessage", inputMap);

		Map<String, Object> responseMap = jsonToMap(MessageUtils.toJsonWithImage(response));
		// MessageUtils.applyLegacyResponseFields(response, responseMap);
		pixelReturn.put("responseMessage", responseMap);

		// Extra (non-visible) input/response pairs, same shape as inputMessage/responseMessage above.
		List<Map<String, Object>> extraMessagesList = new ArrayList<>();
		for (int i = 0; i + 1 < extraMessages.size(); i += 2) {
			Map<String, Object> pair = new LinkedHashMap<>();
			pair.put("inputMessage", jsonToMap(MessageUtils.toJsonWithImage(extraMessages.get(i))));
			pair.put("responseMessage", jsonToMap(MessageUtils.toJsonWithImage(extraMessages.get(i + 1))));
			extraMessagesList.add(pair);
		}
		pixelReturn.put("extraMessages", extraMessagesList);

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(ReactorKeysEnum.IMAGE.getKey()) || key.equals(ReactorKeysEnum.URL.getKey())
				|| RESPONSE_PARTS_KEY.equals(key)) {
			return MCP_KEY_TYPE.ARRAY;
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return MCP_KEY_TYPE.OBJECT;
		}
		return super.getKeyTypeForMCP(key);
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
			return "This is an array of image file names that have already been uploaded to the insight folder, or base64 data URIs for images/PDFs (e.g. data:image/jpeg;base64,.... or data:application/pdf;base64,....).";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return """
					Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc.
					In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for
					<replacement>
					"""
					.replace("<replacement>", Arrays.asList(ReactorKeysEnum.COMMAND.getKey(),
							ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey()).toString());
		} else if (RESPONSE_PARTS_KEY.equals(key)) {
			return "Optional. When provided, the LLM call is skipped and this array of response parts"
					+ " (each a map with type=THINKING|TEXT and matching payload) is persisted as the assistant"
					+ " response. Used by the FE cancel flow to commit whatever streamed before the user hit stop.";
		} else if (HIDDEN_MESSAGE_KEY.equals(key)) {
			return "Optional. Cancel-flow only (paired with " + RESPONSE_PARTS_KEY + "). A hidden user-side note"
					+ " appended after the visible turn, plus an auto-generated assistant ack, so the model sees on"
					+ " the next turn that its previous response was cut short. Ignored on live LLM calls.";
		}
		return super.getDescriptionForKey(key);
	}

}
