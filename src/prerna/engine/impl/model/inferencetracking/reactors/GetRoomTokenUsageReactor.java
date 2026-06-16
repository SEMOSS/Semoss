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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityRoomTokenUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor that returns the token usage status for a specific room.
 * Returns the room token limit (max, input, output), current usage
 * (combined, input, output), and remaining tokens (limit - usage) per dimension.
 * If no limit is configured or no usage data exists, remaining is null.
 */
public class GetRoomTokenUsageReactor extends AbstractReactor {

	@SuppressWarnings("unused")
	private static final Logger classLogger = LogManager.getLogger(GetRoomTokenUsageReactor.class);

	public GetRoomTokenUsageReactor() {
		this.keysToGet = new String[] { "roomId" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(this.keysToGet[0]);
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a roomId parameter");
		}

		// get the effective room token limit for the current user from SecurityRoomTokenUtils
		Map<String, Object> roomLimit = SecurityRoomTokenUtils.getEffectiveRoomTokenLimit(user.getAccessToken(user.getLogins().get(0)).getId());

		// get token usage for this room from ModelInferenceLogsUtils
		Number combinedUsed = ModelInferenceLogsUtils.getTotalTokensForRoom(roomId, null);
		Number inputUsed = ModelInferenceLogsUtils.getTotalTokensForRoom(roomId, "INPUT");
		Number outputUsed = ModelInferenceLogsUtils.getTotalTokensForRoom(roomId, "RESPONSE");

		Map<String, Object> result = new HashMap<>();
		result.put("roomId", roomId);

		// Token limit information
		if (roomLimit != null) {
			Object maxTokens = roomLimit.get("maxTokens");
			Object maxInputTokens = roomLimit.get("maxInputTokens");
			Object maxOutputTokens = roomLimit.get("maxOutputTokens");
			Object isActive = roomLimit.get("isActive");

			result.put("limitActive", isActive == null || Boolean.TRUE.equals(isActive));

			Number maxTokensLong = maxTokens instanceof Number ? (Number) maxTokens : null;
			Number maxInputTokensLong = maxInputTokens instanceof Number ? (Number) maxInputTokens : null;
			Number maxOutputTokensLong = maxOutputTokens instanceof Number ? (Number) maxOutputTokens : null;

			result.put("maxTokens", maxTokensLong);
			result.put("maxInputTokens", maxInputTokensLong);
			result.put("maxOutputTokens", maxOutputTokensLong);

			// calculate remaining tokens
			if (maxTokensLong != null && maxTokensLong.longValue() > 0) {
				result.put("remainingTokens", maxTokensLong.longValue() - (combinedUsed != null ? combinedUsed.longValue() : 0));
			} else {
				result.put("remainingTokens", null);
			}
			if (maxInputTokensLong != null && maxInputTokensLong.longValue() > 0) {
				result.put("remainingInputTokens", maxInputTokensLong.longValue() - (inputUsed != null ? inputUsed.longValue() : 0));
			} else {
				result.put("remainingInputTokens", null);
			}
			if (maxOutputTokensLong != null && maxOutputTokensLong.longValue() > 0) {
				result.put("remainingOutputTokens", maxOutputTokensLong.longValue() - (outputUsed != null ? outputUsed.longValue() : 0));
			} else {
				result.put("remainingOutputTokens", null);
			}
		} else {
			// no limit configured for this user
			result.put("limitActive", false);
			result.put("maxTokens", null);
			result.put("maxInputTokens", null);
			result.put("maxOutputTokens", null);
			result.put("remainingTokens", null);
			result.put("remainingInputTokens", null);
			result.put("remainingOutputTokens", null);
		}

		// actual usage values (always reported even if no limit is set)`
		result.put("usedTokens", combinedUsed);
		result.put("usedInputTokens", inputUsed);
		result.put("usedOutputTokens", outputUsed);

		List<Map<String, Object>> outputList = new ArrayList<>();
		outputList.add(result);

		return new NounMetadata(outputList, PixelDataType.FORMATTED_DATA_SET);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("roomId")) {
			return "The room or conversation ID to check token usage for";
		}
		return super.getDescriptionForKey(key);
	}
}
