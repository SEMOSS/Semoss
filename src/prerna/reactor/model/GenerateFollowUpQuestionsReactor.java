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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GenerateFollowUpQuestionsReactor extends AbstractReactor {

	public GenerateFollowUpQuestionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ROOM_ID.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String modelId = this.keyValue.get(this.keysToGet[0]);
		String roomId = this.keyValue.get(this.keysToGet[1]);

		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		IModelEngine modelEngine = Utility.getModel(modelId);
		String userId = user.getPrimaryLoginToken().getId();
		ModelInferenceLogsUtils.validUserRoom(roomId, userId);

		Room room = RoomUtils.getOrLoadRoom(roomId, insight);
		if (!room.roomCanAcceptNewInputMessage()) {
			Map<String, Object> emptyResponse = new HashMap<>();
			emptyResponse.put("suggestions", new ArrayList<>());
			return new NounMetadata(emptyResponse, PixelDataType.MAP);
		}

		Map<String, Object> paramMap = getParamMap();
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}

		int numResponses = parsePositiveInt(this.keyValue.get(this.keysToGet[2]), 3);
		String jsonSchemaString = String.format(PlaygroundUtils.FOLLOW_UP_SUGGESTIONS_SCHEMA, numResponses,
				numResponses);
		Map<String, Object> jsonSchemaMap = jsonToMap(jsonSchemaString);
		paramMap.put("schema", jsonSchemaMap);

		InputMessage inputMsg = InputMessage.builder(room).withText(PlaygroundUtils.FOLLOW_UP_SUGGESTIONS_PROMPT)
				.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		ResponseMessage response = room.ask(inputMsg, modelEngine, null, false);
		String jsonResponse = response.getContent();
		if (jsonResponse == null || jsonResponse.trim().isEmpty()) {
			throw new IllegalStateException("Model did not return structured follow up suggestions.");
		}
		Map<String, Object> pixelResponse = jsonToMap(jsonResponse);
		return new NounMetadata(pixelResponse, PixelDataType.MAP);
	}

	private static int parsePositiveInt(String value, int defaultValue) {
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

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

	@Override
	public String getReactorDescription() {
		return "Generates suggested follow-up questions based on the most recent room conversation.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The model engine that generates follow up suggestions.";
		} else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room id used to locate the latest conversation messages.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional number of follow up suggestions to generate (defaults to 3).";
		}
		return super.getDescriptionForKey(key);
	}
}
