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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetPlaygroundMessagesReactor extends AbstractReactor {

	public GetPlaygroundMessagesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.SORT.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String limitStr = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		Integer limit = -1;
		String offsetStr = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		Integer offset = -1;
		String dateSortStr = this.keyValue.get(ReactorKeysEnum.SORT.getKey());
		String dateSort = "ASC";

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		/**
		 * Check whether the room is valid for the user. If not, error is thrown.
		 */
		ModelInferenceLogsUtils.validUserRoom(roomId, userId);

		/**
		 * Parse limit, offset and sort keys
		 */
		if (limitStr != null && !limitStr.isEmpty() && (offsetStr != null && !offsetStr.isEmpty())) {
			try {
				limit = Integer.parseInt(limitStr);
				offset = Integer.parseInt(offsetStr);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Invalid value for limit or offset passed");
			}
		}
		if (dateSortStr != null && dateSortStr.equals("DESC")) {
			dateSort = "DESC";
		}

		/**
		 * Convert each message to output map for return
		 */
		List<Map<String, Object>> outputMap = new ArrayList<>();

		/**
		 * Get room object (will load or fetch as needed) and convert messages to new
		 * format from legacy
		 */
		Room room;
		try {
			room = RoomUtils.getOrLoadRoom(roomId, this.insight);
		} catch (Exception e) {
			/**
			 * Returning empty map as that is the closet to the old design of empty array
			 * from db
			 */
			return new NounMetadata(outputMap, PixelDataType.VECTOR);
		}

		/**
		 * Filter/slice results without altering room message object
		 */
		List<AbstractMessage> page = RoomUtils.getPagedMessages(room.getMessages(), dateSort, offset, limit);

		/**
		 * Populate the per-room LLM-name lookup map so that new short-prefix tool names
		 * can be resolved without regex parsing. Uses the model engine's max length to
		 * reproduce the same name transformations that were applied when messages were
		 * stored.
		 */
		IModelEngine roomModelEngine = null;
		String modelId = room.getModelId();
		if (modelId != null) {
			try {
				roomModelEngine = (IModelEngine) Utility.getEngine(modelId);
			} catch (Exception ignore) {
			}
		}
		room.getAllToolsJsonForRoom(MCPUtility.getMaxToolNameLength(roomModelEngine));

		Map<String, JSONObject> toolCache = new HashMap<>();
		for (AbstractMessage m : page) {
			if (m.hasParts() && m.hasToolCallPart()) {
				MCPUtility.updateToolResponseWithProjectMeta((ResponseMessage) m, toolCache,
						room.getToolLookupByLLMName());
			}
			// legacy check
			else if (m.getMessageType() == MessageType.RESPONSE_TOOL) {
				MCPUtility.updateToolResponseWithProjectMeta((ResponseMessage) m, toolCache,
						room.getToolLookupByLLMName());
			}
			Map<String, Object> messageMap = jsonToMap(MessageUtils.toJsonWithImage(m));
//				if (m instanceof prerna.engine.impl.model.message.InputMessage) {
//					MessageUtils.applyLegacyInputFields((prerna.engine.impl.model.message.InputMessage) m,
//							messageMap);
//				} else if (m instanceof ResponseMessage) {
//					MessageUtils.applyLegacyResponseFields((ResponseMessage) m, messageMap);
//				}
			outputMap.add(messageMap);
		}

		return new NounMetadata(outputMap, PixelDataType.VECTOR);
	}

}
