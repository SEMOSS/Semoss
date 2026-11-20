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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class LLMReactor extends AbstractReactor {

	/**
	 * One parameter, two accepted names. 'media' is canonical; 'image' is a
	 * deprecated legacy alias, kept only so existing pixels keep working. Keeping
	 * them in a single aliased key definition (rather than two keys) means the
	 * positional argument order is unchanged and the MCP JSON advertises just
	 * 'media'.
	 */
	private static final String MEDIA_KEY = ReactorKeysEnum.MEDIA.getKey() + "," + ReactorKeysEnum.IMAGE.getKey();

	public LLMReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.CONTEXT.getKey(), ReactorKeysEnum.USE_HISTORY.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), ReactorKeysEnum.ROOM_ID.getKey(), MEDIA_KEY,
				ReactorKeysEnum.URL.getKey(), };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		User user = this.insight.getUser();

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		String question = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());

		Map<String, Object> paramMap = getParamMap();
		IModelEngine modelEngine = Utility.getModel(engineId);
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		Boolean useHistoryParam = Boolean
				.parseBoolean(this.keyValue.getOrDefault(ReactorKeysEnum.USE_HISTORY.getKey(), "true") + "");
		paramMap.put("use_history", useHistoryParam);

		List<String> inputMedia = getMediaInputs();
		List<String> inputMediaURLs = getMediaURLs();

		String parentRoomId = resolveParentRoomId(paramMap, roomId);

		Room room = useHistoryParam
				? RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question, null, null, null, null,
						parentRoomId)
				: RoomUtils.createRoomForStatelessAsk(roomId, insight, modelEngine, question, null, null, null, null,
						parentRoomId);
		List<String> copiedMedia = RoomUtils.copyFilesToRoomFolder(inputMedia, room, insight);

		InputMessage msg = InputMessage.builder(room).withSystemPrompt(context).withText(question)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).withMediaInputs(copiedMedia, room)
				.withMediaUrls(inputMediaURLs).build();

		ResponseMessage response = room.ask(msg, modelEngine);
		return new NounMetadata(response.getModelEngineResponse().toMap(), PixelDataType.MAP,
				PixelOperationType.OPERATION);
	}

	// ----------- media/file helpers, paramMap etc. -------------
	/**
	 * Media files to attach to the message, from either accepted name. Not
	 * images-only: any file type the model accepts (pdf, document, spreadsheet,
	 * audio, video) travels the same way.
	 */
	private List<String> getMediaInputs() {
		return getFileKeyValues(MEDIA_KEY);
	}

	private List<String> getMediaURLs() {
		return getFileKeyValues(ReactorKeysEnum.URL.getKey());
	}

	/**
	 * Values for an aliased key definition ("media,image"), falling back to the
	 * unnamed current row only when no alias was supplied - preserving this
	 * reactor's original positional-argument behavior.
	 */
	private List<String> getFileKeyValues(String keyDefinition) {
		List<String> inputStrings = getListStringForAliasedKey(keyDefinition);
		if (!inputStrings.isEmpty()) {
			return inputStrings;
		}
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			inputStrings.add(this.curRow.get(i).toString());
		}
		return inputStrings;
	}

	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	private String resolveParentRoomId(Map<String, Object> paramMap, String roomId) {
		Object raw = paramMap.remove("PARENT_ROOM_ID");
		if (raw == null) {
			return null;
		}
		String parentRoomId = raw.toString().trim();
		if (parentRoomId.isEmpty() || parentRoomId.equals(roomId)) {
			return null;
		}
		return parentRoomId;
	}

	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "This is the room ID that will be used for storing messages. If no room id is passed in, then insight id will be used for the room";
		} else if (key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the prompt to execute against the LLM";
		} else if (key.equals(ReactorKeysEnum.CONTEXT.getKey())) {
			return "The system prompt to use for the LLM call";
		} else if (key.equals(ReactorKeysEnum.MEDIA.getKey())) {
			return "This is an array of media file names that have already been uploaded to the insight folder, or base64 data URIs (e.g. data:image/jpeg;base64,.... or data:application/pdf;base64,....). "
					+ "Any file type is accepted here - image, pdf, document, spreadsheet, audio, video - and what actually gets used depends on what the model supports.";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "This is an array of media file urls whose contents will be fetched when building the message content.";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			return "Map containing the key-value pairs for model parameters like 'temperature', 'top_p', etc. "
					+ "In addition, you can pass in 'full_prompt' to represent a full prompt and history via ChatML format which will ignore inputs for "
					+ Arrays.asList(ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(),
							ReactorKeysEnum.USE_HISTORY.getKey(), ReactorKeysEnum.MEDIA.getKey(),
							ReactorKeysEnum.URL.getKey())
					+ ". Media for a full_prompt call has to travel inside the prompt as content parts instead.";
		} else if (key.equals(ReactorKeysEnum.USE_HISTORY.getKey())) {
			return "Boolean true/false to determine if we should incorporate the user's chat history based on the previous chats in this insight id. "
					+ "Default is true. This is the same as passing 'use_history' in the "
					+ ReactorKeysEnum.PARAM_VALUES_MAP.getKey() + " key-value map";
		}

		return super.getDescriptionForKey(key);
	}

}
