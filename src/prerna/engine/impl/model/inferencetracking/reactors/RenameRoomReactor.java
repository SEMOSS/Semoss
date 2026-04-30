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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RenameRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RenameRoomReactor.class);

	public RenameRoomReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.ENGINE.getKey()
		};
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String roomName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String userId = insight.getUser().getPrimaryLoginToken().getId();

		// If a name was explicitly provided, persist it directly
		if (roomName != null && !roomName.trim().isEmpty()) {
			boolean result = ModelInferenceLogsUtils.doSetNameForRoom(userId, roomId, roomName);
			return new NounMetadata(result, PixelDataType.BOOLEAN);
		}

		// No name provided — generate one via LLM from the question
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("An engine must be provided when no room name is supplied");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		Room room = RoomUtils.getOrLoadRoom(roomId, insight);

		// Use the first user message as the question for title generation
		String question = room.getMessages().stream()
				.filter(m -> m instanceof InputMessage)
				.map(m -> ((InputMessage) m).getInputUIPrompt())
				.filter(p -> p != null && !p.trim().isEmpty())
				.findFirst()
				.orElse("");

		String generatedName = generateRoomTitle(room, modelEngine, question);
		room.setRoomName(generatedName);
		boolean result = ModelInferenceLogsUtils.doSetNameForRoom(userId, roomId, generatedName);
		if (!result) {
			classLogger.warn("Room title was not persisted for room {}", roomId);
		}
		return new NounMetadata(generatedName, PixelDataType.CONST_STRING);
	}

	/**
	 * Calls the model engine directly (bypassing synchronized room.ask()) to
	 * generate a concise title from the given question.
	 * Falls back to a truncated version of the question if the LLM call fails.
	 */
	public static String generateRoomTitle(Room room, IModelEngine modelEngine, String question) {
		String fallback = (question != null && !question.trim().isEmpty())
				? question.trim()
				: "New Conversation";

		String title = null;
		try {
			InputMessage titleMsg = InputMessage.builder(room)
					.withText("Generate a concise conversation title. "
							+ "Hard limit: 50 characters. "
							+ "Return only the title text with no prefix, quotes, or explanation.\n\nQuestion: "
							+ fallback)
					.withModelType(modelEngine.getModelType())
					.build();

			// Call the engine directly — avoids the synchronized room.ask() lock
			// which would block concurrent user prompts.
			// Pass message_json so the Python model receives the prompt correctly.
			Map<String, Object> params = new HashMap<>();
			params.put("message_json",
					MessageUtils.toJsonArrayWithImageData(Arrays.asList(titleMsg)));

			AskModelEngineResponse<?> llmResponse = modelEngine.askRoom(
					titleMsg.getInputPrompt(), room, titleMsg, params);
			Object raw = (llmResponse != null) ? llmResponse.getResponse() : null;
			if (raw != null && !raw.toString().trim().isEmpty()) {
				title = raw.toString().trim();
			}
		} catch (Exception e) {
			classLogger.warn("Could not generate LLM room title for room {}", room.getId(), e);
		}

		if (title == null || title.isEmpty()) {
			title = fallback;
		}
		if (title.length() > 50) {
			title = title.substring(0, 50).trim();
		}
		return title;
	}

}
