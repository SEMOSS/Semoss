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

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GenerateRoomNameReactor extends AbstractReactor {

	private static final int PROMPT_CHAR_LIMIT = 500;

	private static final String TITLE_INSTRUCTION =
			"Generate a concise 3-5 word title summarizing the topic of the following user message. "
			+ "Return ONLY the title. No punctuation, no quotes, no explanation.";

	public GenerateRoomNameReactor() {
		this.keysToGet = new String[] {
			ReactorKeysEnum.ROOM_ID.getKey(),
			ReactorKeysEnum.PROMPT.getKey(),
			ReactorKeysEnum.ENGINE.getKey()
		};
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String prompt = this.keyValue.get(ReactorKeysEnum.PROMPT.getKey());
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		ModelInferenceLogsUtils.validUserRoom(roomId, userId);

		// only used for permission checks / model resolution - the actual ask()
		// runs against a detached room below so it doesn't block on this one's lock
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);

		// Fall back to the room's configured model if no engine was passed
		if (engineId == null || engineId.trim().isEmpty()) {
			engineId = room.getModelId();
		}

		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("No model engine provided and none configured on the room");
		}

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);

		// fresh from the DB, not the cached instance, so it doesn't fight over the live room's lock
		Room detachedRoom = ModelInferenceLogsUtils.getRoomById(roomId, userId);
		if (detachedRoom == null) {
			throw new IllegalStateException("Room not found");
		}
		detachedRoom.setInsight(this.insight);

		// Truncate to avoid sending a full essay to the model
		String truncatedPrompt = prompt.length() > PROMPT_CHAR_LIMIT
				? prompt.substring(0, PROMPT_CHAR_LIMIT)
				: prompt;

		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("use_history", false);

		InputMessage inputMsg = InputMessage.builder(detachedRoom)
				.withText(TITLE_INSTRUCTION + "\n\n" + truncatedPrompt)
				.withModelType(modelEngine.getModelType())
				.withParamMap(paramMap)
				.build();

		// if this thread's jobId matches a live RunAgent stream, these tokens get
		// forwarded straight into that run's response (PyTranslator tags every chunk
		// with the current jobId, and PixelJobManager forwards anything tagged with
		// a registered run). Null it out so this call can't bleed into someone
		// else's stream - same isolation AgentRoomNamer gets for free from a fresh Thread.
		String callingJobId = ThreadStore.getJobId();
		ResponseMessage response;
		try {
			ThreadStore.setJobId(null);
			// appendToHistory=false: nothing gets written back to the room
			response = detachedRoom.ask(inputMsg, modelEngine, null, false);
		} finally {
			ThreadStore.setJobId(callingJobId);
		}

		String raw = response.getContent();
		if (raw == null || raw.trim().isEmpty()) {
			throw new IllegalStateException("Model did not return a room name");
		}

		String generatedName = raw.trim().replaceAll("^[\"']+|[\"']+$", "");
		if (generatedName.isEmpty()) {
			throw new IllegalStateException("Model returned an empty room name");
		}

		ModelInferenceLogsUtils.doSetNameForRoom(userId, roomId, generatedName);

		// sync the cached room's name too, so a later persist() from the live turn
		// doesn't resurrect the old one
		room.setRoomName(generatedName);

		return new NounMetadata(generatedName, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Generates a concise room name from a user prompt using a one-off model call and persists it to the room.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room ID to rename.";
		} else if (key.equals(ReactorKeysEnum.PROMPT.getKey())) {
			return "The user prompt to derive a title from. Truncated to " + PROMPT_CHAR_LIMIT + " characters before sending.";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Optional model engine ID. Falls back to the room's configured model if not provided.";
		}
		return super.getDescriptionForKey(key);
	}
}
