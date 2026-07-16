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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 *      This program is free software; you can redistribute it and/or
 *      modify it under the terms of the GNU General Public License
 *      as published by the Free Software Foundation; either version 2
 *      of the License, or (at your option) any later version.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.room;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.InsightMCP;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Safely adds the {@link InsightMCP#INSIGHT_MCP_ID} entry to a room's
 * {@code options.mcp} list without overwriting any other room settings.
 *
 * <p>Unlike a raw {@code UpdateRoomOptions()} call (which replaces the entire
 * OPTIONS blob), this reactor does a read-modify-write:
 * <ol>
 *   <li>Read current room OPTIONS from the database.</li>
 *   <li>Add {@code {type:"INSIGHT", id:"__insight__", name:"Room Recordings"}}
 *       to the {@code mcp} array if not already present.</li>
 *   <li>Write the merged OPTIONS back.</li>
 * </ol>
 *
 * <p>Pixel usage:
 * <pre>
 *   AddInsightMCPToRoom(roomId="...");
 * </pre>
 */
public class AddInsightMCPToRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AddInsightMCPToRoomReactor.class);

	public AddInsightMCPToRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@SuppressWarnings("unchecked")
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId == null || roomId.isBlank()) {
			throw new IllegalArgumentException("roomId is required");
		}

		String userId = user.getPrimaryLoginToken().getId();
		Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);

		// ── Read existing options ──────────────────────────────────────────────
		List<Map<String, Object>> rows = ModelInferenceLogsUtils.getRoomOptions(roomId, userId);

		Map<String, Object> options = new HashMap<>();
		if (rows != null && !rows.isEmpty() && rows.get(0).get("OPTIONS") instanceof Map) {
			options = new HashMap<>((Map<String, Object>) rows.get(0).get("OPTIONS"));
		}

		// ── Merge insight MCP entry ────────────────────────────────────────────
		List<Map<String, Object>> mcpList = new ArrayList<>();
		Object existingMcp = options.get("mcp");
		if (existingMcp instanceof List) {
			for (Object item : (List<?>) existingMcp) {
				if (item instanceof Map) {
					mcpList.add(new HashMap<>((Map<String, Object>) item));
				}
			}
		}

		boolean alreadyPresent = mcpList.stream()
				.anyMatch(m -> InsightMCP.INSIGHT_MCP_ID.equals(m.get("id")));

		if (alreadyPresent) {
			// Repair a stale in-memory Room even when the database is already correct.
			room.setOptionsMap(options);
			classLogger.info("AddInsightMCPToRoom: __insight__ already in room '{}' — no change", roomId);
			Map<String, Object> result = new HashMap<>();
			result.put("added", false);
			result.put("roomId", roomId);
			return new NounMetadata(result, PixelDataType.MAP);
		}

		Map<String, Object> insightEntry = new HashMap<>();
		insightEntry.put("type", "INSIGHT");
		insightEntry.put("id", InsightMCP.INSIGHT_MCP_ID);
		insightEntry.put("name", InsightMCP.INSIGHT_MCP_NAME);
		mcpList.add(insightEntry);

		options.put("mcp", mcpList);

		// ── Write merged options back ──────────────────────────────────────────
		ModelInferenceLogsUtils.setRoomOptions(roomId, userId, options);
		room.setOptionsMap(options);

		classLogger.info("AddInsightMCPToRoom: added __insight__ MCP to room '{}'", roomId);
		Map<String, Object> result = new HashMap<>();
		result.put("added", true);
		result.put("roomId", roomId);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Safely adds the insight MCP (__insight__) to a room's tool list without overwriting other room settings.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The ID of the Playground room to add the insight MCP to.";
		}
		return super.getDescriptionForKey(key);
	}
}
