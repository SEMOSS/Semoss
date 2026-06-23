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
 * Mirrors the surrounding scaffold of {@link Room#ask} (mutation lock, latest
 * projection refresh, orphan-tool normalization, parent-id resolution, message
 * append, room-name inference, persist) but skips the actual LLM call —
 * substituting an assistant response built from the FE-supplied parts array.
 *
 * Called by the FE immediately after StopPixelExecution. Inputs:
 *   command       — user prompt (plain text)
 *   responseParts — JSON array of THINKING/TEXT parts the FE accumulated
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
		// Mirror AskPlaygroundReactor's keys (engine, roomId, parentMessageId,
		// command, image, url, paramValues) so the FE can pass the same inputs
		// it would have passed to AskPlayground — plus the FE-accumulated
		// responseParts that stand in for the LLM's response.
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.PARENT_MESSAGE_ID.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.IMAGE.getKey(), ReactorKeysEnum.URL.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), RESPONSE_PARTS_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String parentMessageId = this.keyValue.get(ReactorKeysEnum.PARENT_MESSAGE_ID.getKey());
		String question = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		List<?> responseParts = getList(RESPONSE_PARTS_KEY);

		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}
		List<String> inputImages = getListString(ReactorKeysEnum.IMAGE.getKey());
		List<String> inputImageURLs = getListString(ReactorKeysEnum.URL.getKey());

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

		List<String> copiedImages = RoomUtils.copyFilesToRoomFolder(inputImages, room, insight);

		// Build the visible user input — matches AskPlaygroundReactor's build so
		// the persisted message looks identical to what a non-cancelled turn would.
		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(room.getEffectiveSystemPrompt())
				.withMediaInputs(copiedImages, room).withMediaUrls(inputImageURLs).withText(question)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		// Build the partial assistant response from the FE-supplied parts (this is
		// the substitute for room.ask's modelEngine.askRoom call). Always returns
		// a message — empty if no tokens streamed before the cancel.
		ResponseMessage partialMsg = buildPartialFromParts(responseParts);
		partialMsg.setModel(modelEngine);

		String userId = user.getPrimaryLoginToken().getId();
		synchronized (room) {
			try (RoomMessageStore.RoomMutationLock ignored = RoomMessageStore.acquireMutationLock(room)) {
				// Mirror Room.ask: pull the latest projection so we mutate fresh state,
				// then strip any orphan tool_use parts (a prior cancel mid-tool would
				// otherwise leave the next provider payload invalid).
				RoomMessageStore.refreshFromLatestProjection(room, userId);
				RoomMessageStore.normalizeForProviderPayload(room);

				inputMsg.setModel(modelEngine);

				// Parent-id resolution: explicit param wins; otherwise hang off the
				// latest message; otherwise this is the first message of the thread.
				if (!room.getMessages().isEmpty()) {
					if (parentMessageId != null && !parentMessageId.isEmpty()) {
						inputMsg.setParentMessageId(parentMessageId);
					} else {
						AbstractMessage lastMsg = room.getMessages().get(room.getMessages().size() - 1);
						inputMsg.setParentMessageId(lastMsg.getMessageId());
					}
				} else {
					inputMsg.setParentMessageId(null);
				}

				// Wire the partial response under the input we just built.
				partialMsg.setParentMessageId(inputMsg.getMessageId());

				// Append in conversation order.
				room.getMessages().add(inputMsg);
				room.getMessages().add(partialMsg);

				// Hidden user-note / assistant-ack pair — disabled for now, will be
				// re-enabled once we want the model explicitly told its previous turn
				// was cut short. See appendHiddenCancelContextPair below.
				// appendHiddenCancelContextPair(room, modelEngine, partialMsg.getMessageId());

				// Room-name inference + 4-arg/2-arg persist switch — copied from
				// Room.ask's tail so first-message rooms actually get created.
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

		// Return the visible pair so FE can sync placeholder IDs without reloading.
		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		pixelReturn.put("inputMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(inputMsg)));
		pixelReturn.put("responseMessage", MessageUtils.jsonToMapForPixelReturn(MessageUtils.toJson(partialMsg)));
		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	/**
	 * Build a ResponseMessage from the FE-supplied parts list, in order. Each
	 * element is expected to be a Map with {@code type} = "THINKING" or "TEXT"
	 * and the matching payload field. Always returns a message — when no usable
	 * parts come through (cancel fired before any token streamed) we still
	 * persist an empty response so the input/response pair stays balanced.
	 */
	private ResponseMessage buildPartialFromParts(List<?> responseParts) {
		ResponseMessage.Builder builder = ResponseMessage.builder();
		if (responseParts != null) {
			for (Object element : responseParts) {
				if (!(element instanceof Map)) {
					continue;
				}
				Map<?, ?> part = (Map<?, ?>) element;
				Object typeObj = part.get("type");
				String type = typeObj != null ? typeObj.toString() : null;
				if ("THINKING".equals(type)) {
					Object thinkingObj = part.get("thinking");
					String thinking = thinkingObj != null ? thinkingObj.toString() : null;
					if (thinking != null && !thinking.isEmpty()) {
						builder.withThinking(thinking);
					}
				} else if ("TEXT".equals(type)) {
					Object textObj = part.get("text");
					String text = textObj != null ? textObj.toString() : null;
					if (text != null && !text.isEmpty()) {
						builder.withText(text);
					}
				}
				// Other part types (TOOL_CALL/TOOL_RESULT/MEDIA) are not produced by a
				// cancelled stream and are intentionally ignored here.
			}
		}
		return builder.build();
	}

	/**
	 * Append a hidden user-note / assistant-ack pair to the room history. The
	 * pair is invisible to the FE (visible=false, platformGenerated=true) but
	 * still rides along to the model on the next turn via
	 * {@link RoomMessageStore#providerMessageHistory}, telling the model its
	 * prior response was cut short. Wired but not yet called — see the
	 * commented-out invocation in {@link #execute()}.
	 */
	@SuppressWarnings("unused")
	private void appendHiddenCancelContextPair(Room room, IModelEngine modelEngine, String hiddenParent) {
		InputMessage hiddenUserNote = InputMessage.builder(room).withText(HIDDEN_USER_NOTE)
				.withModelType(modelEngine.getModelType()).build();
		hiddenUserNote.setPlatformGenerated(true);
		hiddenUserNote.setVisible(false);
		hiddenUserNote.setParentMessageId(hiddenParent);

		ResponseMessage hiddenAck = ResponseMessage.text(HIDDEN_ASSISTANT_ACK);
		hiddenAck.setPlatformGenerated(true);
		hiddenAck.setVisible(false);
		hiddenAck.setParentMessageId(hiddenUserNote.getMessageId());

		room.getMessages().add(hiddenUserNote);
		room.getMessages().add(hiddenAck);
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
		} else if (ReactorKeysEnum.IMAGE.getKey().equals(key)) {
			return "Optional list of image file references attached to the user prompt";
		} else if (ReactorKeysEnum.URL.getKey().equals(key)) {
			return "Optional list of image URLs attached to the user prompt";
		} else if (ReactorKeysEnum.PARAM_VALUES_MAP.getKey().equals(key)) {
			return "Optional model parameter map (max_new_tokens, temperature, etc.)";
		} else if (RESPONSE_PARTS_KEY.equals(key)) {
			return "JSON array of the partial response parts (THINKING + TEXT) the FE"
					+ " accumulated before cancellation";
		}
		return super.getDescriptionForKey(key);
	}
}
