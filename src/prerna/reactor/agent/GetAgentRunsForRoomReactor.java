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
 * -----------------------------------------------------------------------------
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the durable top-level agent runs for one room owned by the current
 * user. The room-specific surface lets clients restore a conversation without
 * paging through the agent-wide activity log.
 */
public class GetAgentRunsForRoomReactor extends AbstractReactor {

	private static final String ROOM_ID_KEY = "roomId";

	public GetAgentRunsForRoomReactor() {
		this.keysToGet = new String[] { ROOM_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = StringUtils.trimToNull(this.keyValue.get(ROOM_ID_KEY));
		if (roomId == null) {
			throw new IllegalArgumentException("roomId is required");
		}

		List<Map<String, Object>> runs = new AgentRunStore().getRunsForRoom(this.insight, roomId);
		// The store is newest-first for activity logs. Conversation playback is
		// chronological and only needs top-level runs; children are loaded through
		// GetSubagentRuns using the durable parentRunId relationship.
		runs.removeIf(run -> run.get("parentRunId") != null);
		Collections.reverse(runs);
		// Ownership is enforced by the query. Do not echo the internal user key to
		// the browser as part of the presentation contract.
		runs.forEach(run -> run.remove("userId"));

		return new NounMetadata(runs, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the current user's durable top-level AgentRun rows for a room in chronological order.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ROOM_ID_KEY.equals(key)) {
			return "The room whose top-level agent runs should be returned.";
		}
		return super.getDescriptionForKey(key);
	}
}
