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
 * Returns full detail for a specific room's agent activity, including:
 * - All traces in the room (with steps and token info)
 * - User prompts from the MESSAGE table correlated to each trace by timestamp
 *
 * <pre>
 * GetRoomAgentDetail(roomId=["&lt;id&gt;"])
 * GetRoomAgentDetail(roomId=["&lt;id&gt;"], includeMessages=["true"])
 * </pre>
 *
 * Returns a MAP with:
 * ROOM_ID, TRACES[] (each with steps + user prompt), MESSAGES[] (user inputs in chronological order)
 */
public class GetRoomAgentDetailReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetRoomAgentDetailReactor.class);
	private static final Gson GSON = new Gson();

	private static final String KEY_ROOM_ID = "roomId";
	private static final String KEY_INCLUDE_MESSAGES = "includeMessages";

	public GetRoomAgentDetailReactor() {
		this.keysToGet = new String[] { KEY_ROOM_ID, KEY_INCLUDE_MESSAGES };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated.");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String roomId = this.keyValue.get(KEY_ROOM_ID);
		if (roomId == null || roomId.trim().isEmpty()) {
			throw new IllegalArgumentException("Required parameter 'roomId' is missing.");
		}
		roomId = roomId.trim();

		boolean includeMessages = true;
		String inclMsgStr = this.keyValue.get(KEY_INCLUDE_MESSAGES);
		if (inclMsgStr != null && "false".equalsIgnoreCase(inclMsgStr.trim())) {
			includeMessages = false;
		}

		// Get all traces for this room
		List<Map<String, Object>> rawTraces = AgentTraceLogsUtils.listTraces(roomId, userId, 0);
		if (rawTraces == null) rawTraces = new ArrayList<>();

		// Fetch all user messages first (for prompt correlation and display)
		List<Map<String, Object>> allMessages = fetchUserMessages(roomId, userId);

		// Build enriched trace list (with steps and per-trace token recovery)
		List<Map<String, Object>> enrichedTraces = new ArrayList<>(rawTraces.size());
		for (Map<String, Object> row : rawTraces) {
			enrichedTraces.add(enrichTrace(row, userId, roomId, allMessages));
		}

		// Sort chronologically (oldest first) for room detail view
		enrichedTraces.sort((a, b) -> {
			String aTime = (String) a.get("STARTED_AT");
			String bTime = (String) b.get("STARTED_AT");
			if (aTime == null && bTime == null) return 0;
			if (aTime == null) return -1;
			if (bTime == null) return 1;
			return aTime.compareTo(bTime);
		});

		// Assign user prompts to traces by order
		assignPromptsToTraces(enrichedTraces, allMessages);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("ROOM_ID", roomId);
		result.put("TRACES", enrichedTraces);
		result.put("TOTAL_RUNS", enrichedTraces.size());

		// Include messages in response
		if (includeMessages) {
			result.put("MESSAGES", allMessages);
		}

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * Enriches a trace row with UI fields, steps, and correlated user prompt.
	 */
	private Map<String, Object> enrichTrace(Map<String, Object> row, String userId, String roomId, List<Map<String, Object>> allMessages) {
		Map<String, Object> trace = new LinkedHashMap<>();

		String traceId = extractString(row, "TRACE_ID");
		String startTime = extractString(row, "START_TIME");
		String endTime = extractString(row, "END_TIME");
		String harnessType = extractString(row, "HARNESS_TYPE");
		String termReason = extractString(row, "TERMINATION_REASON");

		trace.put("TRACE_ID", traceId);
		trace.put("ROOM_ID", roomId);
		trace.put("USER_ID", extractString(row, "USER_ID"));
		trace.put("PROJECT_ID", extractString(row, "PROJECT_ID"));
		trace.put("HARNESS_NAME", harnessType);
		trace.put("STARTED_AT", startTime);
		trace.put("ENDED_AT", endTime);
		trace.put("DURATION_MS", computeDurationMs(startTime, endTime));
		trace.put("STATUS", normalizeStatus(termReason));
		trace.put("ITERATIONS", row.get("ITERATIONS") != null ? row.get("ITERATIONS") : row.get("AGENT_TRACE__ITERATIONS"));
		trace.put("TOOL_CALL_COUNT", row.get("TOOL_CALL_COUNT") != null ? row.get("TOOL_CALL_COUNT") : row.get("AGENT_TRACE__TOOL_CALL_COUNT"));

		// Tokens — recover from MESSAGE table using trace time window for accuracy
		Object metricsJson = row.get("METRICS_JSON") != null ? row.get("METRICS_JSON") : row.get("AGENT_TRACE__METRICS_JSON");
		int[] tokens = extractTokensFromMetrics(metricsJson);
		if (tokens[0] == 0 && tokens[1] == 0) {
			// Query MESSAGE table for tokens within this trace's time window (DB-level join, no TZ issues)
			int[] recovered = AgentTraceLogsUtils.sumTokensForTrace(traceId, roomId);
			tokens[0] = recovered[0];
			tokens[1] = recovered[1];
		}
		trace.put("TOTAL_INPUT_TOKENS", tokens[0]);
		trace.put("TOTAL_OUTPUT_TOKENS", tokens[1]);
		// Also include raw metrics for admin visibility
		trace.put("METRICS_JSON", metricsJson != null ? String.valueOf(metricsJson) : null);

		// Fetch the user prompt that triggered this trace — assigned after all traces built
		// (see assignPromptsToTraces)

		// Include raw termination reason for admin visibility
		trace.put("TERMINATION_REASON", termReason);
		trace.put("MODEL_ENGINE_ID", extractString(row, "MODEL_ENGINE_ID"));
		trace.put("PARENT_TRACE_ID", extractString(row, "PARENT_TRACE_ID"));

		// Fetch tool steps
		if (traceId != null) {
			List<Map<String, Object>> steps = AgentTraceLogsUtils.listTraceSteps(traceId, userId);
			// Compute DURATION_MS for each step
			for (Map<String, Object> step : steps) {
				Object startObj = step.get("START_TIME");
				Object endObj = step.get("END_TIME");
				if (startObj != null && endObj != null) {
					try {
						java.sql.Timestamp s = java.sql.Timestamp.valueOf(String.valueOf(startObj).trim());
						java.sql.Timestamp e = java.sql.Timestamp.valueOf(String.valueOf(endObj).trim());
						step.put("DURATION_MS", Math.max(0L, e.getTime() - s.getTime()));
					} catch (Exception ignored) {}
				}
			}
			trace.put("STEPS", steps);
		}

		return trace;
	}

	/**
	 * Fetches the user prompt (INPUT message) that most closely precedes the trace start time.
	 * This correlates what the user typed to the agent run it triggered.
	 */
	private String fetchUserPromptForTrace(String roomId, String userId, String traceStartTime) {
		return AgentTraceLogsUtils.fetchUserPromptBeforeTime(roomId, traceStartTime);
	}

	/**
	 * Correlates a user prompt from the pre-fetched messages list.
	 * Strategy: since trace START_TIME is stored in local time but MESSAGE DATE_CREATED
	 * is stored in UTC, direct timestamp comparison is unreliable. Instead, we use
	 * order-based correlation: messages are chronological, traces are chronological,
	 * so we match them by position — filtering out non-prompt messages first.
	 */
	private static List<String> extractUserPrompts(List<Map<String, Object>> messages) {
		List<String> prompts = new ArrayList<>();
		for (Map<String, Object> msg : messages) {
			String data = (String) msg.get("MESSAGE_DATA");
			if (data == null || data.isEmpty()) continue;
			// Skip tool outputs, JSON blobs, error messages — only keep real user prompts
			if (data.startsWith("{") || data.startsWith("[") || data.startsWith("This tool execution")) continue;
			if (data.length() > 500) continue; // unlikely to be a user prompt
			prompts.add(data);
		}
		return prompts;
	}

	/**
	 * Assigns user prompts to traces by order. Each trace corresponds to a user prompt
	 * in the order they appear. If there are more traces than prompts, later traces get null.
	 */
	private void assignPromptsToTraces(List<Map<String, Object>> enrichedTraces, List<Map<String, Object>> messages) {
		List<String> prompts = extractUserPrompts(messages);
		for (int i = 0; i < enrichedTraces.size(); i++) {
			if (i < prompts.size()) {
				enrichedTraces.get(i).put("USER_PROMPT", prompts.get(i));
			}
		}
	}

	/**
	 * Fetches all user INPUT messages for this room, chronologically.
	 * Returns MESSAGE_DATA (the prompt text), DATE_CREATED, and MESSAGE_ID.
	 */
	private List<Map<String, Object>> fetchUserMessages(String roomId, String userId) {
		return AgentTraceLogsUtils.fetchUserMessagesForRoom(roomId);
	}

	private static String extractString(Map<String, Object> row, String key) {
		Object val = row.get(key);
		if (val == null) val = row.get("AGENT_TRACE__" + key);
		return val != null ? String.valueOf(val) : null;
	}

	private static String normalizeStatus(String terminationReason) {
		if (terminationReason == null) return "OK";
		if ("SUCCESS".equalsIgnoreCase(terminationReason) || "DONE".equalsIgnoreCase(terminationReason)
				|| "RESPONSE_TEXT".equalsIgnoreCase(terminationReason) || "RESPONSE_TOOL".equalsIgnoreCase(terminationReason)) return "OK";
		if (terminationReason.startsWith("ERROR")) return "ERROR";
		return terminationReason;
	}

	private static long computeDurationMs(Object startTime, Object endTime) {
		if (startTime == null || endTime == null) return 0;
		try {
			String startStr = String.valueOf(startTime).replace(" ", "T");
			String endStr = String.valueOf(endTime).replace(" ", "T");
			if (!startStr.endsWith("Z") && !startStr.contains("+")) startStr += "Z";
			if (!endStr.endsWith("Z") && !endStr.contains("+")) endStr += "Z";
			return Duration.between(Instant.parse(startStr), Instant.parse(endStr)).toMillis();
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
