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
 * Called by the FE immediately after StopPixelExecution. The FE already has
 * the streamed partial in its placeholder; this reactor just commits it to
 * the room history so subsequent AskPlayground calls see the right context.
 *
 * Returns the persisted inputMessage and responseMessage so the FE can sync
 * its placeholder IDs without a separate room reload.
 */
public class RecordCancelledTurnReactor extends AbstractReactor {

	private static final String PARTIAL_RESPONSE_KEY = "partialResponse";

	private static final String HIDDEN_USER_NOTE = "[System note: I cancelled your previous response before it"
			+ " finished streaming. The partial output above is everything you had generated."
			+ " Please wait for my next message before continuing.]";

	private static final String HIDDEN_ASSISTANT_ACK = "Understood — I'll wait for your next instruction.";

	public RecordCancelledTurnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(), PARTIAL_RESPONSE_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		String question = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String partialResponse = this.keyValue.get(PARTIAL_RESPONSE_KEY);
		if (partialResponse == null) {
			partialResponse = "";
		}

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);
		room.setProjectId(PlaygroundUtils.PLAYGROUND_PROJECT_ID);

		// --- Build the four messages: input, visible partial response, hidden
		// user note, hidden assistant ack. Visibility=false on the hidden pair
		// keeps them out of the FE UI but they still ride along to the model on
		// the next turn (see RoomMessageStore.providerMessageHistory).
		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(room.getEffectiveSystemPrompt())
				.withText(question).withModelType(modelEngine.getModelType()).build();
		if (parentMessageId != null && !parentMessageId.isEmpty()) {
			inputMsg.setParentMessageId(parentMessageId);
		}

		ResponseMessage partialMsg = null;
		if (!partialResponse.isEmpty()) {
			partialMsg = ResponseMessage.text(partialResponse);
			partialMsg.setModel(modelEngine);
			partialMsg.setParentMessageId(inputMsg.getMessageId());
		}

		String hiddenParent = partialMsg != null ? partialMsg.getMessageId() : inputMsg.getMessageId();
		InputMessage hiddenUserNote = InputMessage.builder(room).withText(HIDDEN_USER_NOTE)
				.withModelType(modelEngine.getModelType()).build();
		hiddenUserNote.setPlatformGenerated(true);
		hiddenUserNote.setVisibile(false);
		hiddenUserNote.setParentMessageId(hiddenParent);

		ResponseMessage hiddenAck = ResponseMessage.text(HIDDEN_ASSISTANT_ACK);
		hiddenAck.setPlatformGenerated(true);
		hiddenAck.setVisibile(false);
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
		} else if (PARTIAL_RESPONSE_KEY.equals(key)) {
			return "The partial assistant response that was streamed to the FE before cancellation";
		}
		return super.getDescriptionForKey(key);
	}
}
