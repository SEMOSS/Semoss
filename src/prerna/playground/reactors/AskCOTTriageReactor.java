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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskCOTTriageReactor extends AbstractReactor {

	// TODO:
	// 1. Determine params?
	// 2. Add schema to thing
	// 3. blah blah blah
	public AskCOTTriageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), // 0, required
				ReactorKeysEnum.ROOM_ID.getKey(), // 1, optional (not required, will use insight)
				ReactorKeysEnum.COMMAND.getKey(), // 2, required (actual user query)
				ReactorKeysEnum.CONTEXT.getKey(), // 3, tbd on how it is used
				ReactorKeysEnum.IMAGE.getKey(), // 4, optional, TODO: add in support
				ReactorKeysEnum.URL.getKey(), // 5, optional, TODO: add in support
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 6, optional
		};
		this.keyRequired = new int[] { 1, 0, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {

		String modelId = this.keyValue.get(this.keysToGet[0]);

		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, modelId)) {
			throw new IllegalArgumentException(
					"Model " + modelId + " does not exist or user does not have access to this model");
		}

		String userQuery = this.keyValue.get(this.keysToGet[2]);
		String roomId = this.keyValue.get(this.keysToGet[1]);

		IModelEngine modelEngine = Utility.getModel(modelId);
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, userQuery);

		Map<String, Object> paramMap = getParamMap();
		if (paramMap == null) {
			paramMap = new HashMap<>();
		}
		paramMap.put("schema", PlaygroundUtils.COT_JSON_SCHEMA);

		InputMessage inputMsg = InputMessage.builder(room).withSystemPrompt(PlaygroundUtils.TRIAGE_PROMPT)
				.withText(userQuery).withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

		ResponseMessage response = room.ask(inputMsg, modelEngine);

		// TODO: test. If JSON Schema responses are enforced, this should work without
		// catching.
		// Copy AskCOTRoom ResponseMessage parsing if this is currently nonfunctional
		Object pixelReturn = "";
		try {
			pixelReturn = GSON.fromJson(response.getContent(), new TypeToken<Map<String, Object>>() {
			}.getType());
		} catch (JsonSyntaxException e) {
			throw e;
		}

		return new NounMetadata(pixelReturn, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

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

}
