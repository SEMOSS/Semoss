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
package prerna.reactor.insights;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightActiveRoomModelReactor extends AbstractReactor {

	private static final String MODEL_ID_OPTION_KEY = "modelId";
	/** Older rooms stored the model under these option keys. Same keys AgentRunner reads. */
	private static final String[] LEGACY_MODEL_ID_OPTION_KEYS = { "engine", "model", "engineId" };

	@Override
	public NounMetadata execute() {
		String roomId = this.insight.getRoomId();
		if (roomId == null) {
			throw new IllegalArgumentException("Insight is not associated with any room");
		}
		User user = this.insight.getUser();
		String userId = user.getPrimaryLoginToken().getId();

		// The cached room is the one RunAgent works on, so it carries the model the
		// run resolved even when that never reached the stored row.
		Room cachedRoom = user.getRoomHash().get(roomId);
		Room storedRoom = ModelInferenceLogsUtils.getRoomById(roomId, userId);
		if (cachedRoom == null && storedRoom == null) {
			throw new IllegalArgumentException("Room " + roomId + " does not exist");
		}

		// The playground writes the selected model to options.modelId, so it wins
		String modelId = getOption(storedRoom, MODEL_ID_OPTION_KEY);
		if (modelId == null) {
			modelId = getOption(cachedRoom, MODEL_ID_OPTION_KEY);
		}
		if (modelId == null) {
			modelId = getModelId(cachedRoom);
		}
		if (modelId == null) {
			modelId = getModelId(storedRoom);
		}
		for (int i = 0; modelId == null && i < LEGACY_MODEL_ID_OPTION_KEYS.length; i++) {
			modelId = getOption(storedRoom, LEGACY_MODEL_ID_OPTION_KEYS[i]);
		}
		if (modelId == null) {
			throw new IllegalArgumentException("No model associated with the room");
		}

		return new NounMetadata(modelId, PixelDataType.CONST_STRING);
	}

	private static String getOption(Room room, String key) {
		if (room == null) {
			return null;
		}
		Map<String, Object> optionsMap = room.getOptionsMap();
		if (optionsMap == null) {
			return null;
		}
		Object value = optionsMap.get(key);
		return value instanceof String ? StringUtils.trimToNull((String) value) : null;
	}

	private static String getModelId(Room room) {
		return room == null ? null : StringUtils.trimToNull(room.getModelId());
	}

	@Override
	public String getReactorDescription() {
		return "Get the active model for the room set in the current insight";
	}
}
