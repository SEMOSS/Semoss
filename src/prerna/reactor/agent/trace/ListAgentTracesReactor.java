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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists agent traces, filtered by optional room, project, or parent trace.
 *
 * <pre>
 * ListTraces(limit=["100"])                   -- traces for current user
 * ListTraces(limit=["50"], projectId=["&lt;id&gt;"]) -- filtered by project
 * ListTraces(roomId=["&lt;id&gt;"])                 -- filtered by room
 * ListTraces(parentTraceId=["&lt;id&gt;"])          -- children of a given trace
 * </pre>
 *
 * Returns: LIST of trace objects with UI-compatible field names:
 * TRACE_ID, ROOM_ID, USER_ID, HARNESS_NAME, STATUS, STARTED_AT, ENDED_AT,
 * DURATION_MS, TOTAL_INPUT_TOKENS, TOTAL_OUTPUT_TOKENS, PARENT_TRACE_ID, PROJECT_ID.
 */
public class ListAgentTracesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListAgentTracesReactor.class);
	private static final Gson GSON = new Gson();

	private static final String KEY_LIMIT = "limit";
	private static final String KEY_ROOM_ID = "roomId";
	private static final String KEY_PROJECT_ID = "projectId";
	private static final String KEY_PARENT_TRACE_ID = "parentTraceId";
	private static final int DEFAULT_LIMIT = 50;

	public ListAgentTracesReactor() {
		this.keysToGet = new String[] { KEY_LIMIT, KEY_ROOM_ID, KEY_PROJECT_ID, KEY_PARENT_TRACE_ID };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
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
		String projectId = this.keyValue.get(KEY_PROJECT_ID);

		List<Map<String, Object>> results;
		try {
			if (parentTraceId != null && !parentTraceId.isEmpty()) {
				results = AgentTraceLogsUtils.getChildTraces(parentTraceId);
			} else if (projectId != null && !projectId.isEmpty()) {
				results = AgentTraceLogsUtils.listTracesByProject(projectId, userId, limit);
			} else if (roomId != null && !roomId.isEmpty()) {
				results = AgentTraceLogsUtils.listTraces(roomId, userId, limit);
			} else {
				results = AgentTraceLogsUtils.listTraces(null, userId, limit);
			}
		} catch (Exception e) {
			classLogger.warn("ListAgentTracesReactor: error fetching traces.", e);
			results = new ArrayList<>();
		}

		if (results == null) {
			results = new ArrayList<>();
		}

		// Transform to UI field names
		List<Map<String, Object>> transformed = new ArrayList<>(results.size());
		for (Map<String, Object> row : results) {
			transformed.add(transformForUI(row));
		}

		return new NounMetadata(transformed, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * Maps DB column names to the field names the UI expects.
	 */
	static Map<String, Object> transformForUI(Map<String, Object> row) {
		Map<String, Object> ui = new LinkedHashMap<>();
		ui.put("TRACE_ID", row.get("TRACE_ID"));
		ui.put("ROOM_ID", row.get("ROOM_ID"));
		ui.put("USER_ID", row.get("USER_ID"));
		ui.put("PROJECT_ID", row.get("PROJECT_ID"));
		ui.put("HARNESS_NAME", row.get("HARNESS_TYPE"));
		ui.put("STARTED_AT", row.get("START_TIME"));
		ui.put("ENDED_AT", row.get("END_TIME"));
		ui.put("DURATION_MS", computeDurationMs(row.get("START_TIME"), row.get("END_TIME")));
		ui.put("STATUS", normalizeStatus(row.get("TERMINATION_REASON")));
		ui.put("PARENT_TRACE_ID", row.get("PARENT_TRACE_ID"));
		ui.put("ITERATIONS", row.get("ITERATIONS"));
		ui.put("TOOL_CALL_COUNT", row.get("TOOL_CALL_COUNT"));

		// Extract token totals from METRICS_JSON if available
		int[] tokens = extractTokensFromMetrics(row.get("METRICS_JSON"));
		if (tokens[0] == 0 && tokens[1] == 0) {
			// Recover from MESSAGE table at read-time
			tokens = recoverTokensFromMessage(row);
		}
		ui.put("TOTAL_INPUT_TOKENS", tokens[0]);
		ui.put("TOTAL_OUTPUT_TOKENS", tokens[1]);

		return ui;
	}

	/**
	 * Attempts to recover token usage from the MESSAGE table when METRICS_JSON is empty.
	 */
	private static int[] recoverTokensFromMessage(Map<String, Object> row) {
		String roomId = (String) row.get("ROOM_ID");
		Object startTimeObj = row.get("START_TIME");
		if (roomId == null || startTimeObj == null) {
			return new int[] {0, 0};
		}
		try {
			String startStr = String.valueOf(startTimeObj).replace(" ", "T");
			if (!startStr.endsWith("Z") && !startStr.contains("+")) startStr += "Z";
			java.time.Instant since = java.time.Instant.parse(startStr);
			return AgentTraceLogsUtils.sumTokensForRoom(roomId, since);
		} catch (Exception e) {
			return new int[] {0, 0};
		}
	}

	private static String normalizeStatus(Object terminationReason) {
		if (terminationReason == null) return "OK";
		String reason = String.valueOf(terminationReason);
		if ("SUCCESS".equalsIgnoreCase(reason) || "DONE".equalsIgnoreCase(reason)
				|| "RESPONSE_TEXT".equalsIgnoreCase(reason) || "RESPONSE_TOOL".equalsIgnoreCase(reason)) return "OK";
		if (reason.startsWith("ERROR")) return "ERROR";
		return reason;
	}

	private static long computeDurationMs(Object startTime, Object endTime) {
		if (startTime == null || endTime == null) return 0;
		try {
			String startStr = String.valueOf(startTime).replace(" ", "T");
			String endStr = String.valueOf(endTime).replace(" ", "T");
			if (!startStr.endsWith("Z") && !startStr.contains("+")) startStr += "Z";
			if (!endStr.endsWith("Z") && !endStr.contains("+")) endStr += "Z";
			Instant s = Instant.parse(startStr);
			Instant e = Instant.parse(endStr);
			return Duration.between(s, e).toMillis();
		} catch (Exception ex) {
			return 0;
		}
	}

	private static int[] extractTokensFromMetrics(Object metricsJson) {
		if (metricsJson == null) return new int[] {0, 0};
		try {
			JsonObject json = GSON.fromJson(String.valueOf(metricsJson), JsonObject.class);
			int input = json.has("inputTokens") ? json.get("inputTokens").getAsInt() : 0;
			int output = json.has("outputTokens") ? json.get("outputTokens").getAsInt() : 0;
			return new int[] {input, output};
		} catch (Exception e) {
			return new int[] {0, 0};
		}
	}
}
