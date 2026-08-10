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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Executes one persistent, room-aware model turn without applying Playground
 * project or theme policy.
 */
public class AskRoomReactor extends AbstractReactor {

	private static final String RESPONSE_PARTS_KEY = "responseParts";
	private static final String HIDDEN_MESSAGE_KEY = "hiddenMessage";

	public AskRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.IMAGE.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), RESPONSE_PARTS_KEY, HIDDEN_MESSAGE_KEY };
		this.keyRequired = new int[] { 1, 0, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
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
		String projectIdOverride = getProjectIdOverride();
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question, null, null, null,
				projectIdOverride, null);
		if (projectIdOverride != null) {
			room.setProjectId(projectIdOverride);
		}
		String givenSystemPrompt = room.getSystemPromptForModel();
		List<String> copiedImages = RoomUtils.copyFilesToRoomFolder(inputImages, room, insight);

		InputMessage inputMessage = InputMessage.builder(room).withSystemPrompt(givenSystemPrompt)
				.withMediaInputs(copiedImages, room).withMediaUrls(inputImageURLs).withText(question)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		List<AbstractMessage> extraMessages = new ArrayList<>();
		ResponseMessage responseMessage;
		if (responseParts != null) {
			responseMessage = room.commitPrebuiltTurn(inputMessage, modelEngine, parentMessageId, responseParts,
					hiddenMessage, extraMessages);
		} else {
			responseMessage = room.ask(inputMessage, modelEngine, parentMessageId);
			if (responseMessage.getMessageType() == MessageType.RESPONSE_TEXT) {
				RoomMessageStore.persist(room, user.getPrimaryLoginToken().getId());
			} else if (responseMessage.getMessageType() == MessageType.RESPONSE_TOOL) {
				room.updateToolResponseMeta(responseMessage);
			}
		}

		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		Map<String, Object> inputMap = jsonToMap(MessageUtils.toJsonWithImage(inputMessage));
		if (shouldHideSystemMessages()) {
			MessageUtils.removeSystemPromptFromMessageMap(inputMap);
		}
		pixelReturn.put("inputMessage", inputMap);
		pixelReturn.put("responseMessage", jsonToMap(MessageUtils.toJsonWithImage(responseMessage)));

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

	/**
	 * Returns a runtime-surface project override, or {@code null} to let room
	 * creation derive the project from the current insight.
	 */
	protected String getProjectIdOverride() {
		return null;
	}

	/**
	 * Returns whether system content should be removed from the client response.
	 */
	protected boolean shouldHideSystemMessages() {
		return false;
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
		return "Runs a room-aware LLM turn and returns both input and response message objects.";
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
					+ " response. Used by a cancel flow to commit whatever streamed before the user stopped it.";
		} else if (HIDDEN_MESSAGE_KEY.equals(key)) {
			return "Optional. Cancel-flow only (paired with " + RESPONSE_PARTS_KEY + "). A hidden user-side note"
					+ " appended after the visible turn, plus an auto-generated assistant ack, so the model sees on"
					+ " the next turn that its previous response was cut short. Ignored on live LLM calls.";
		}
		return super.getDescriptionForKey(key);
	}
}
