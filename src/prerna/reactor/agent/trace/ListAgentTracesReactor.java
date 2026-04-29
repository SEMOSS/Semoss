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
package prerna.reactor.agent.trace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists agent traces, filtered by optional room or parent trace.
 *
 * <pre>
 * ListAgentTraces()                          -- traces for current user, up to 50
 * ListAgentTraces(limit=[100])               -- with custom limit
 * ListAgentTraces(roomId=["&lt;id&gt;"])           -- filtered by room (user must own it)
 * ListAgentTraces(parentTraceId=["&lt;id&gt;"])   -- children of a given trace
 * </pre>
 *
 * Returns: VECTOR of MAPs each containing TRACE_ID, ROOM_ID, USER_ID,
 * MODEL_ENGINE_ID, HARNESS_TYPE, START_TIME, END_TIME, ITERATIONS,
 * TOOL_CALL_COUNT, TERMINATION_REASON, METRICS_JSON, PARENT_TRACE_ID.
 */
public class ListAgentTracesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListAgentTracesReactor.class);

	private static final String KEY_LIMIT = "limit";
	private static final String KEY_ROOM_ID = "roomId";
	private static final String KEY_PARENT_TRACE_ID = "parentTraceId";
	private static final int DEFAULT_LIMIT = 50;

	public ListAgentTracesReactor() {
		this.keysToGet = new String[] { KEY_LIMIT, KEY_ROOM_ID, KEY_PARENT_TRACE_ID };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated to list agent traces");
		}
		String userId = user.getPrimaryLoginToken().getId();

		int limit = DEFAULT_LIMIT;
		String limitStr = this.keyValue.get(KEY_LIMIT);
		if (limitStr != null && !limitStr.isEmpty()) {
			try {
				limit = Integer.parseInt(limitStr);
			} catch (NumberFormatException e) {
				classLogger.warn("ListAgentTracesReactor: invalid limit '{}', using default {}.", limitStr, DEFAULT_LIMIT);
			}
		}

		String parentTraceId = this.keyValue.get(KEY_PARENT_TRACE_ID);
		String roomId = this.keyValue.get(KEY_ROOM_ID);

		List<Map<String, Object>> results;
		try {
			if (parentTraceId != null && !parentTraceId.isEmpty()) {
				// Children of a given trace — still scoped to current user via TRACE ownership
				results = AgentTraceLogsUtils.getChildTraces(parentTraceId);
			} else if (roomId != null && !roomId.isEmpty()) {
				// Room-scoped — filter by both roomId and userId
				results = AgentTraceLogsUtils.listTraces(roomId, userId, limit);
			} else {
				// Default: current user's traces only
				results = AgentTraceLogsUtils.listTraces(null, userId, limit);
			}
		} catch (Exception e) {
			classLogger.warn("ListAgentTracesReactor: error fetching traces.", e);
			results = new ArrayList<>();
		}

		if (results == null) {
			results = new ArrayList<>();
		}

		return new NounMetadata(results, PixelDataType.VECTOR);
	}
}
