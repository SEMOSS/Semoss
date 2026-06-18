/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ******************************************************************************/
package prerna.playground.reactors;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
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

/**
 * Persists the visible user input + partial assistant response from a
 * cancelled LLM stream, plus a hidden "I cancelled you / understood" round
 * trip that gives the model context on the next turn that its previous
 * response was cut short.
 *
 * Called by the FE immediately after StopPixelExecution. The FE supplies
 * the user prompt as plain text (command) and the partial assistant response
 * as the accumulated parts array (responseParts) — a JSON array of
 * THINKING and TEXT chunks.
 *
 * Returns the persisted inputMessage and responseMessage so the FE can sync
 * its placeholder IDs without a separate room reload.
 */
public class RecordCancelledTurnReactor extends AbstractReactor {

	private static final String RESPONSE_PARTS_KEY = "responseParts";

	private static final String HIDDEN_USER_NOTE = "[System note: I cancelled your previous response before it"
			+ " finished streaming. The partial output above is everything you had generated."
			+ " Please wait for my next message before continuing.]";

	private static final String HIDDEN_ASSISTANT_ACK = "Understood — I'll wait for your next instruction.";

	public RecordCancelledTurnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(), RESPONSE_PARTS_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		String question = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String responsePartsJson = this.keyValue.get(RESPONSE_PARTS_KEY);

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException("No model engine could be resolved for id " + engineId);
		}
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
		room.setProjectId(PlaygroundUtils.PLAYGROUND_PROJECT_ID);

		// --- Build the visible user input from the plain-text command.
		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(room.getEffectiveSystemPrompt())
				.withText(question).withModelType(modelEngine.getModelType()).build();
		if (parentMessageId != null && !parentMessageId.isEmpty()) {
			inputMsg.setParentMessageId(parentMessageId);
		}

		// --- Build the partial assistant response from the FE-supplied parts array.
		// Expected shape: [{type:"THINKING",thinking:"..."},{type:"TEXT",text:"..."}].
		// Order is preserved so the rendered output (and provider history) matches
		// what the user saw on screen.
		ResponseMessage partialMsg = buildPartialFromParts(responsePartsJson);
		if (partialMsg != null) {
			partialMsg.setModel(modelEngine);
			partialMsg.setParentMessageId(inputMsg.getMessageId());
		}

		// --- Hidden user-note / assistant-ack pair. Visibility=false keeps them out
		// of the FE UI but they still ride along to the model on the next turn (see
		// RoomMessageStore.providerMessageHistory).
		String hiddenParent = partialMsg != null ? partialMsg.getMessageId() : inputMsg.getMessageId();
		InputMessage hiddenUserNote = InputMessage.builder(room).withText(HIDDEN_USER_NOTE)
				.withModelType(modelEngine.getModelType()).build();
		hiddenUserNote.setPlatformGenerated(true);
		hiddenUserNote.setVisible(false);
		hiddenUserNote.setParentMessageId(hiddenParent);

		ResponseMessage hiddenAck = ResponseMessage.text(HIDDEN_ASSISTANT_ACK);
		hiddenAck.setPlatformGenerated(true);
		hiddenAck.setVisible(false);
		hiddenAck.setParentMessageId(hiddenUserNote.getMessageId());

		// --- Append to room history
		room.getMessages().add(inputMsg);
		if (partialMsg != null) {
			room.getMessages().add(partialMsg);
		}
		room.getMessages().add(hiddenUserNote);
		room.getMessages().add(hiddenAck);

		// --- Persist. First-message rooms need the 4-arg signature (roomName +
		// engineId) to actually create the row; subsequent updates use the 2-arg
		// form. Mirror AskPlaygroundReactor / Room.ask's logic.
		String userId = user.getPrimaryLoginToken().getId();
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
		boolean isNewRoom = (prevRoomName == null || prevRoomName.trim().isEmpty()) && room.getRoomName() != null
				&& !room.getRoomName().trim().isEmpty();
		if (isNewRoom) {
			RoomMessageStore.persist(room, userId, room.getRoomName(), modelEngine.getEngineId());
		} else {
			RoomMessageStore.persist(room, userId);
		}

		// --- Return the visible pair so FE can sync placeholder IDs without
		// reloading the whole room.
		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
		if (partialMsg != null) {
			pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(partialMsg)));
		}
		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	/**
	 * Parse the FE-supplied parts array and build a ResponseMessage with those
	 * parts in order. Returns null if the payload is empty or yields no usable
	 * parts (e.g. cancel fired before any token streamed).
	 */
	private ResponseMessage buildPartialFromParts(String responsePartsJson) {
		if (responsePartsJson == null || responsePartsJson.trim().isEmpty()) {
			return null;
		}

		JsonElement parsed;
		try {
			parsed = JsonParser.parseString(responsePartsJson);
		} catch (Exception e) {
			throw new IllegalArgumentException("responseParts payload was not valid JSON", e);
		}
		if (!parsed.isJsonArray()) {
			throw new IllegalArgumentException("responseParts payload must be a JSON array of part objects");
		}

		ResponseMessage.Builder builder = ResponseMessage.builder();
		boolean hasAnyPart = false;
		for (JsonElement element : (JsonArray) parsed) {
			if (element == null || !element.isJsonObject()) {
				continue;
			}
			JsonObject part = element.getAsJsonObject();
			String type = part.has("type") && !part.get("type").isJsonNull() ? part.get("type").getAsString() : null;
			if ("THINKING".equals(type)) {
				String thinking = part.has("thinking") && !part.get("thinking").isJsonNull()
						? part.get("thinking").getAsString() : null;
				if (thinking != null && !thinking.isEmpty()) {
					builder.withThinking(thinking);
					hasAnyPart = true;
				}
			} else if ("TEXT".equals(type)) {
				String text = part.has("text") && !part.get("text").isJsonNull() ? part.get("text").getAsString() : null;
				if (text != null && !text.isEmpty()) {
					builder.withText(text);
					hasAnyPart = true;
				}
			}
			// Other part types (TOOL_CALL/TOOL_RESULT/MEDIA) are not produced by a
			// cancelled stream and are intentionally ignored here.
		}
		return hasAnyPart ? builder.build() : null;
	}

	@Override
	public String getReactorDescription() {
		return "Persist the partial assistant response from a cancelled LLM stream plus a"
				+ " hidden user-note/assistant-ack pair so the model has context on the next turn.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ENGINE.getKey().equals(key)) {
			return "The model engine id";
		} else if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "The room id";
		} else if (ReactorKeysEnum.PARENT_MESSAGE_ID.getKey().equals(key)) {
			return "Optional parent message id";
		} else if (ReactorKeysEnum.COMMAND.getKey().equals(key)) {
			return "The user prompt that was sent to the model";
		} else if (RESPONSE_PARTS_KEY.equals(key)) {
			return "JSON array of the partial response parts (THINKING + TEXT) the FE"
					+ " accumulated before cancellation";
		}
		return super.getDescriptionForKey(key);
	}
}
