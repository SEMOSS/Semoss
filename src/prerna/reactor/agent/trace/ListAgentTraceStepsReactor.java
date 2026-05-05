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
 * Returns all tool-call steps for an agent trace, ordered by STEP_NUMBER ASC.
 *
 * <pre>
 * ListAgentTraceSteps(traceId=["&lt;id&gt;"])
 * </pre>
 *
 * Returns: LIST of MAPs, each containing STEP_ID, TRACE_ID, STEP_NUMBER, STEP_TYPE,
 * TOOL_NAME, OUTPUT_TEXT, TOOL_INPUT_JSON, START_TIME, END_TIME, ERROR_MESSAGE,
 * TOOL_CALL_ID, ENGINE_ID, ENGINE_TYPE, IS_MCP, STATUS.
 * Returns an empty list if no steps exist.
 * Returns an error noun if the requesting user does not own the trace.
 */
public class ListAgentTraceStepsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListAgentTraceStepsReactor.class);

	private static final String KEY_TRACE_ID = "traceId";

	public ListAgentTraceStepsReactor() {
		this.keysToGet = new String[] { KEY_TRACE_ID };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated to list trace steps.");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String traceId = this.keyValue.get(KEY_TRACE_ID);
		if (traceId == null || traceId.trim().isEmpty()) {
			throw new IllegalArgumentException("Required parameter 'traceId' is missing.");
		}
		traceId = traceId.trim();

		classLogger.info("ListAgentTraceSteps: userId='{}' traceId='{}'", userId, traceId);

		List<Map<String, Object>> steps = AgentTraceLogsUtils.listTraceSteps(traceId, userId);

		// Compute DURATION_MS from timestamps
		for (Map<String, Object> step : steps) {
			Object startObj = step.get("START_TIME");
			Object endObj = step.get("END_TIME");
			if (startObj != null && endObj != null) {
				try {
					// Convert to string representation regardless of actual type
					String startStr = String.valueOf(startObj).trim();
					String endStr = String.valueOf(endObj).trim();
					java.sql.Timestamp s = java.sql.Timestamp.valueOf(startStr);
					java.sql.Timestamp e = java.sql.Timestamp.valueOf(endStr);
					step.put("DURATION_MS", Math.max(0L, e.getTime() - s.getTime()));
				} catch (Exception ignored) {
					// leave DURATION_MS absent if unparseable
				}
			}
		}

		return new NounMetadata(steps, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
