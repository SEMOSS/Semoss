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

	/**
	 * When present, the LLM call is skipped and the reactor persists a turn
	 * assembled from the caller-supplied response parts instead. Used by the FE
	 * cancel flow to commit whatever streamed before the user hit stop.
	 */
	private static final String RESPONSE_PARTS_KEY = "responseParts";

	/**
	 * Cancel-flow only. When paired with {@link #RESPONSE_PARTS_KEY}, a hidden
	 * user note carrying this string is appended after the visible turn (plus
	 * an auto-generated assistant ack) so the model sees on the next turn that
	 * its previous response was cut short. Ignored on live LLM calls.
	 */
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

		// Collects any non-visible messages (e.g. cancel-flow hidden pair) so
		// they can be surfaced back to the FE via `extraMessages` in the return
		// map. The FE won't render them, but does need them to stay in sync
		// with the room's provider history.
		List<AbstractMessage> extraMessages = new ArrayList<>();

		ResponseMessage response;
		if (responseParts != null) {
			// ---- FE-supplied response (cancel flow): skip the LLM call and persist
			// the input + a response built from the caller-supplied parts. Mirrors
			// the surrounding scaffold of Room.ask so the persisted turn looks
			// identical to a live one.
			response = commitPrebuiltTurn(room, modelEngine, msg, parentMessageId, responseParts, hiddenMessage,
					extraMessages);
		} else {
			// ---- Actually run LLM call
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
//		MessageUtils.applyLegacyResponseFields(response, responseMap);
		pixelReturn.put("responseMessage", responseMap);

		// Extra (non-visible) messages persisted alongside the visible pair.
		// Currently only populated on the cancel path, but always emitted (as
		// an empty list on normal turns) so the FE contract stays consistent.
		List<Map<String, Object>> extraMessagesList = new ArrayList<>();
		for (AbstractMessage extra : extraMessages) {
			extraMessagesList.add(jsonToMap(MessageUtils.toJson(extra)));
		}
		pixelReturn.put("extraMessages", extraMessagesList);

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	/**
	 * Persist a caller-provided input + response as a completed turn. Mirrors
	 * the surrounding scaffold of {@link Room#ask} (mutation lock, latest
	 * projection refresh, orphan-tool normalization, parent-id resolution,
	 * append, room-name inference, persist) but skips the LLM call — the
	 * response is built from {@code responseParts}. Optionally appends a hidden
	 * user-note / assistant-ack pair after the visible turn.
	 */
	private ResponseMessage commitPrebuiltTurn(Room room, IModelEngine modelEngine, InputMessage msg,
			String parentMessageId, List<Map<String, Object>> responseParts, String hiddenMessage,
			List<AbstractMessage> extrasOut) {
		ResponseMessage response = PlaygroundUtils.buildResponseMessageFromParts(responseParts);
		response.setModel(modelEngine);

		String userId = insight.getUser().getPrimaryLoginToken().getId();
		synchronized (room) {
			try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(room)) {
				RoomMessageStore.refreshFromLatestProjection(room, userId);
				RoomMessageStore.normalizeForProviderPayload(room);

				msg.setModel(modelEngine);

				// Parent-id resolution mirrors Room.ask: explicit param wins, otherwise
				// hang off the latest message, otherwise this is the first message.
				if (!room.getMessages().isEmpty()) {
					if (parentMessageId != null && !parentMessageId.isEmpty()) {
						msg.setParentMessageId(parentMessageId);
					} else {
						AbstractMessage lastMsg = room.getMessages().get(room.getMessages().size() - 1);
						msg.setParentMessageId(lastMsg.getMessageId());
					}
				} else {
					msg.setParentMessageId(null);
				}
				response.setParentMessageId(msg.getMessageId());

				room.getMessages().add(msg);
				room.getMessages().add(response);

				if (hiddenMessage != null && !hiddenMessage.isEmpty()) {
					appendHiddenPair(room, modelEngine, hiddenMessage, response.getMessageId(), extrasOut);
				}

				// Room-name inference + 4-arg/2-arg persist switch (from Room.ask's tail).
				String prevRoomName = room.getRoomName();
				if (prevRoomName == null || prevRoomName.trim().isEmpty()) {
					for (AbstractMessage m : room.getMessages()) {
						if (m instanceof InputMessage) {
							String prompt = ((InputMessage) m).getInputUIPrompt();
							if (prompt != null && !prompt.trim().isEmpty()) {
								room.setRoomName(prompt.substring(0, Math.min(prompt.length(), 100)));
								break;
							}
						}
					}
				}
				if ((prevRoomName == null || prevRoomName.trim().isEmpty()) && room.getRoomName() != null
						&& !room.getRoomName().trim().isEmpty()) {
					RoomMessageStore.persist(room, userId, room.getRoomName(), modelEngine.getEngineId());
				} else {
					RoomMessageStore.persist(room, userId);
				}
			}
		}
		return response;
	}

	/**
	 * Append a hidden user note + canned assistant ack to the room history.
	 * Both are invisible to the FE (visible=false, platformGenerated=true) but
	 * ride along to the model on the next turn via
	 * {@link RoomMessageStore#providerMessageHistory}, keeping the payload
	 * role-alternating and telling the model its prior response was cut short.
	 */
	private void appendHiddenPair(Room room, IModelEngine modelEngine, String hiddenMessage, String hiddenParentId,
			List<AbstractMessage> extrasOut) {
		InputMessage hiddenUserNote = InputMessage.builder(room).withText(hiddenMessage)
				.withModelType(modelEngine.getModelType()).build();
		hiddenUserNote.setPlatformGenerated(true);
		hiddenUserNote.setVisible(false);
		hiddenUserNote.setParentMessageId(hiddenParentId);

		ResponseMessage hiddenAck = ResponseMessage.text(PlaygroundUtils.HIDDEN_MESSAGE_ACK);
		hiddenAck.setPlatformGenerated(true);
		hiddenAck.setVisible(false);
		hiddenAck.setParentMessageId(hiddenUserNote.getMessageId());

		room.getMessages().add(hiddenUserNote);
		room.getMessages().add(hiddenAck);

		if (extrasOut != null) {
			extrasOut.add(hiddenUserNote);
			extrasOut.add(hiddenAck);
		}
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
