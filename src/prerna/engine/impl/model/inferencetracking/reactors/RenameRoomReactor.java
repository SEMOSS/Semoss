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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;


public class RenameRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RenameRoomReactor.class);

	private static final int PROMPT_CHAR_LIMIT = 500;
	private static final int TITLE_CHAR_LIMIT = 100;
	private static final int TITLE_MAX_TOKENS = 32;

	private static final String TITLE_INSTRUCTION =
			"Generate a concise 3-5 word title summarizing the topic of the following user message. "
					+ "Return ONLY the title. No punctuation, no quotes, no explanation.";
	
	public RenameRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.MODEL.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String roomName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());

		String userId = user.getPrimaryLoginToken().getId();
		ModelInferenceLogsUtils.validUserRoom(roomId, userId);
		if (roomName == null || roomName.trim().isEmpty()) {
			throw new IllegalArgumentException("A room name must be provided");
		}

		String requestedName = roomName.trim();
		String finalName;
		if (modelId != null && !modelId.trim().isEmpty()) {
			// Generated rename: treat name as source text for the title model.
			finalName = generateRoomTitle(user, modelId.trim(), requestedName);
		} else {
			// Manual rename: use the supplied name without calling a model.
			finalName = requestedName;
		}

		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		room.setRoomName(finalName);
		if (!ModelInferenceLogsUtils.doSetNameForRoom(userId, roomId, finalName)) {
			throw new IllegalStateException("Room name could not be persisted");
		}

		return new NounMetadata(finalName, PixelDataType.CONST_STRING);
	}

	private String generateRoomTitle(User user, String modelId, String source) {
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(modelId);
		if (modelEngine == null) {
			throw new IllegalArgumentException("Could not load model engine " + modelId);
		}

		Room titleRoom = null;
		String titleRoomId = GUID.v7().toUUID().toString();
		try {
			// Some providers key conversation state by room ID. A temporary room keeps
			// this metadata request isolated from the user's actual conversation.
			titleRoom = RoomUtils.createRoomForStatelessAsk(titleRoomId, this.insight, modelEngine, null);

			Map<String, Object> paramMap = new HashMap<>();
			paramMap.put("use_history", false);
			paramMap.put("stream", false);
			paramMap.put("max_tokens", TITLE_MAX_TOKENS);

			InputMessage titleMessage = InputMessage.builder(titleRoom)
					.withText(TITLE_INSTRUCTION + "\n\n" + truncate(source, PROMPT_CHAR_LIMIT))
					.withModelType(modelEngine.getModelType())
					.withParamMap(paramMap)
					.build();

			ResponseMessage response = titleRoom.ask(titleMessage, modelEngine, null, false);
			String rawTitle = response == null ? null : response.getContent();
			String title = rawTitle == null ? null
					: StringUtils.normalizeSpace(PixelUtility.removeSurroundingQuotes(rawTitle));
			if (title == null || title.isEmpty()) {
				throw new IllegalStateException("Model did not return a room name");
			}
			return truncate(title, TITLE_CHAR_LIMIT);
		} finally {
			if (titleRoom != null) {
				try {
					ModelInferenceLogsUtils.doSetRoomToInactive(user.getPrimaryLoginToken().getId(), titleRoomId);
					user.getRoomHash().remove(titleRoomId);
				} catch (Exception e) {
					classLogger.warn("Could not clean up temporary title room {}", titleRoomId, e);
				}
			}
		}
	}

	private static String truncate(String value, int maxLength) {
		return value.substring(0, Math.min(value.length(), maxLength));
	}

	@Override
	public String getReactorDescription() {
		return "Renames a room directly, or uses a model to generate a title from the supplied name text.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room ID to rename.";
		} else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "The literal room name, or the source text to summarize when a model is provided.";
		} else if (key.equals(ReactorKeysEnum.MODEL.getKey())) {
			return "Optional model ID used to generate a title from the supplied name text.";
		}
		return super.getDescriptionForKey(key);
	}
}
