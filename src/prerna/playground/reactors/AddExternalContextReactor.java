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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
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

public class AddExternalContextReactor extends AbstractReactor {

	private static final String ROOM_ID = "roomId";
	private static final String CONTENT = "content";
	private static final String SOURCE_ID = "sourceId";
	private static final String CONTEXT_TYPE = "contextType";
	private static final String IDEMPOTENCY_KEY = "idempotencyKey";
	private static final String TRIGGER_MODEL = "triggerModel";
	private static final String TRIGGER_PROMPT = "triggerPrompt";
	// files: relative paths within the insight folder, or base64 data URIs
	private static final String FILES = ReactorKeysEnum.IMAGE.getKey();

	private static final String DEFAULT_TRIGGER_PROMPT = "New external context has been provided. Please review and respond accordingly.";

	public AddExternalContextReactor() {
		this.keysToGet = new String[] {
				ROOM_ID,          // 0 required
				CONTENT,          // 1 required
				SOURCE_ID,        // 2 required
				CONTEXT_TYPE,     // 3 optional
				IDEMPOTENCY_KEY,  // 4 optional
				TRIGGER_MODEL,    // 5 optional
				TRIGGER_PROMPT,   // 6 optional
				FILES             // 7 optional
		};
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int index = 0;
		String roomId = this.keyValue.get(this.keysToGet[index++]);
		String content = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[index++]));
		String sourceId = this.keyValue.get(this.keysToGet[index++]);
		String contextType = this.keyValue.get(this.keysToGet[index++]);
		String idempotencyKey = this.keyValue.get(this.keysToGet[index++]);
		String triggerModelStr = this.keyValue.get(this.keysToGet[index++]);
		String triggerPrompt = this.keyValue.get(this.keysToGet[index++]);
		List<String> inputFiles = getListString(this.keysToGet[index++]);

		if (contextType == null || contextType.trim().isEmpty()) {
			contextType = "TEXT";
		}
		boolean triggerModel = "true".equalsIgnoreCase(triggerModelStr);
		if (triggerPrompt == null || triggerPrompt.trim().isEmpty()) {
			triggerPrompt = DEFAULT_TRIGGER_PROMPT;
		}

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		// Verify the calling user owns this room
		List<Map<String, Object>> verifyResult = ModelInferenceLogsUtils.doVerifyConversation(userId, roomId);
		if (verifyResult == null || verifyResult.isEmpty()) {
			throw new IllegalArgumentException(
					"Room " + roomId + " does not exist or you do not have permission to modify it");
		}

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		List<AbstractMessage> messages = room.getMessages();

		// Idempotency check: if key provided and already present, return existing message
		if (idempotencyKey != null && !idempotencyKey.trim().isEmpty()) {
			for (AbstractMessage existing : messages) {
				Object existingKey = existing.getOrnament(IDEMPOTENCY_KEY);
				if (idempotencyKey.equals(existingKey)) {
					Map<String, Object> pixelReturn = new LinkedHashMap<>();
					pixelReturn.put("contextMessage", jsonToMap(MessageUtils.toJson(existing)));
					pixelReturn.put("duplicate", true);
					return new NounMetadata(pixelReturn, PixelDataType.MAP);
				}
			}
		}

		// Copy files from the insight folder into the room folder so the model can access them
		List<String> copiedFiles = MessageUtils.copyFilesToRoomFolder(inputFiles, room, this.insight);

		// Build the external context InputMessage
		InputMessage contextMsg = InputMessage.builder(room)
				.withText(content)
				.withMediaInputs(copiedFiles, room)
				.build();
		contextMsg.setPlatformGenerated(true);
		contextMsg.setOrnament(PlaygroundUtils.PLAYGROUND_MESSAGE_TYPE, PlaygroundUtils.EXTERNAL_CONTEXT_MESSAGE_TYPE);
		contextMsg.setOrnament(SOURCE_ID, sourceId);
		contextMsg.setOrnament(CONTEXT_TYPE, contextType);
		if (!copiedFiles.isEmpty()) {
			contextMsg.setOrnament("files", new ArrayList<>(copiedFiles));
		}
		if (idempotencyKey != null && !idempotencyKey.trim().isEmpty()) {
			contextMsg.setOrnament(IDEMPOTENCY_KEY, idempotencyKey);
		}

		// Wire into message branch
		if (!messages.isEmpty()) {
			AbstractMessage lastMsg = messages.get(messages.size() - 1);
			contextMsg.setParentMessageId(lastMsg.getMessageId());
		}
		messages.add(contextMsg);

		// Persist
		ModelInferenceLogsUtils.llm2_updateRoomMessages(roomId, userId, room.getMessagesAsString());

		Map<String, Object> pixelReturn = new LinkedHashMap<>();
		pixelReturn.put("contextMessage", jsonToMap(MessageUtils.toJson(contextMsg)));

		// Active mode: trigger the model
		if (triggerModel) {
			String modelId = room.getModelId();
			if (modelId == null || modelId.trim().isEmpty()) {
				throw new IllegalArgumentException(
						"Room does not have an associated model. Cannot trigger model response.");
			}
			if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
				throw new IllegalArgumentException(
						"Model " + modelId + " does not exist or user does not have access to this model");
			}
			IModelEngine modelEngine = Utility.getModel(modelId);

			InputMessage triggerMsg = InputMessage.builder(room)
					.withSystemPrompt(room.getEffectiveSystemPrompt())
					.withText(triggerPrompt)
					.withModelType(modelEngine.getModelType())
					.build();

			ResponseMessage modelResponse = room.ask(triggerMsg, modelEngine, contextMsg.getMessageId());

			ModelInferenceLogsUtils.llm2_updateRoomMessages(roomId, userId, room.getMessagesAsString());

			pixelReturn.put("triggerMessage", jsonToMap(MessageUtils.toJson(triggerMsg)));
			pixelReturn.put("modelResponse", jsonToMap(MessageUtils.toJson(modelResponse)));
		}

		return new NounMetadata(pixelReturn, PixelDataType.MAP);
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(FILES)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Inject external context into an existing room as a distinct, visible message.
				The message is marked as platform-generated with ornament PLAYGROUND_MESSAGE_TYPE=EXTERNAL_CONTEXT.
				Supports attaching files (images, PDFs) from the insight folder so the model can reference them.
				Supports idempotency via an optional idempotencyKey ornament.
				Optionally triggers the model to respond to the injected context (triggerModel=true).
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case ROOM_ID -> "The room ID to inject external context into";
			case CONTENT -> "The context text to inject (e.g. '3 new receipt images were uploaded for claim #12345')";
			case SOURCE_ID -> "Identifier of the external system pushing the context (e.g. 'claims-intake-system')";
			case CONTEXT_TYPE -> "Type hint for the content: TEXT (default), JSON, or FILE_REF";
			case IDEMPOTENCY_KEY -> "Optional key to prevent duplicate injection; if a message with this key already exists, the existing message is returned";
			case TRIGGER_MODEL -> "If true, triggers the room's model to respond to the injected context immediately (default: false)";
			case TRIGGER_PROMPT -> "Custom prompt sent to the model in active mode (default: '" + DEFAULT_TRIGGER_PROMPT + "')";
			case FILES -> "Array of file paths relative to the insight folder (e.g. 'receipts/img001.jpg'), or base64 data URIs. Files are copied into the room folder so the model can read them.";
			default -> super.getDescriptionForKey(key);
		};
	}
}
