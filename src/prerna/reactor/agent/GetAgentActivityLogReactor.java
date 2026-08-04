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
package prerna.reactor.agent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns a page of durable agent runs owned by the current user.
 */
public class GetAgentActivityLogReactor extends AbstractReactor {

	private static final String AGENT_ID_KEY = "agentId";
	private static final String SORT_BY_ROOM_KEY = "sortByRoom";
	private static final long DEFAULT_LIMIT = 20L;

	public GetAgentActivityLogReactor() {
		this.keysToGet = new String[] { AGENT_ID_KEY, ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), SORT_BY_ROOM_KEY };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String agentId = StringUtils.trimToNull(this.keyValue.get(AGENT_ID_KEY));
		if (agentId == null) {
			throw new IllegalArgumentException("agentId is required");
		}
		long limit = getPageValue(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT);
		long offset = getPageValue(ReactorKeysEnum.OFFSET.getKey(), 0L);
		if (limit <= 0) {
			throw new IllegalArgumentException("limit must be greater than 0");
		}
		if (offset < 0) {
			throw new IllegalArgumentException("offset must be greater than or equal to 0");
		}

		List<Map<String, Object>> runs = new AgentRunStore().getActivityLog(this.insight, agentId, limit, offset);
		if (Boolean.parseBoolean(this.keyValue.get(SORT_BY_ROOM_KEY))) {
			return new NounMetadata(groupByRoom(runs), PixelDataType.MAP, PixelOperationType.OPERATION);
		}
		return new NounMetadata(runs, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	private Map<String, List<Map<String, Object>>> groupByRoom(List<Map<String, Object>> runs) {
		Map<String, List<Map<String, Object>>> runsByRoom = new LinkedHashMap<>();
		for (Map<String, Object> run : runs) {
			Object roomIdValue = run.get("roomId");
			String roomId = roomIdValue == null ? null : String.valueOf(roomIdValue);
			runsByRoom.computeIfAbsent(roomId, key -> new ArrayList<>()).add(run);
		}
		return runsByRoom;
	}

	private long getPageValue(String key, long defaultValue) {
		String value = this.keyValue.get(key);
		return value == null || value.trim().isEmpty() ? defaultValue : Long.parseLong(value.trim());
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves a page of runs for the specified agent and current user, ordered newest first. Results can optionally be grouped by room.";
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (SORT_BY_ROOM_KEY.equals(key)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (AGENT_ID_KEY.equals(key)) {
			return "The agent ID to filter by. This corresponds to AGENT_RUN.WORKSPACE_ID.";
		} else if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Maximum number of agent runs to return. Defaults to 20 and must be greater than 0.";
		} else if (ReactorKeysEnum.OFFSET.getKey().equals(key)) {
			return "Number of agent runs to skip. Defaults to 0 and must not be negative.";
		} else if (SORT_BY_ROOM_KEY.equals(key)) {
			return "When true, returns a map keyed by room ID whose values are the runs for that room.";
		}
		return super.getDescriptionForKey(key);
	}
}
