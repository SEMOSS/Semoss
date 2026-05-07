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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * Returns agent traces grouped by ROOM_ID with per-room aggregate statistics.
 *
 * <pre>
 * ListAgentTracesByRoom(limit=["200"])
 * ListAgentTracesByRoom(limit=["100"], projectId=["&lt;id&gt;"])
 * </pre>
 *
 * Returns: LIST of room group objects, each containing:
 * ROOM_ID, PROJECT_ID, USERS[], HARNESS_TYPES[], TOTAL_RUNS, TOTAL_TOOL_CALLS,
 * TOTAL_INPUT_TOKENS, TOTAL_OUTPUT_TOKENS, TOTAL_DURATION_MS, LAST_ACTIVITY,
 * HAS_ERRORS, TRACES[] (individual trace summaries within the room).
 *
 * Rooms are sorted by LAST_ACTIVITY descending (most recent first).
 */
public class ListAgentTracesByRoomReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListAgentTracesByRoomReactor.class);
	private static final Gson GSON = new Gson();

	private static final String KEY_LIMIT = "limit";
	private static final String KEY_PROJECT_ID = "projectId";
	private static final int DEFAULT_LIMIT = 200;

	public ListAgentTracesByRoomReactor() {
		this.keysToGet = new String[] { KEY_LIMIT, KEY_PROJECT_ID };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null || user.getPrimaryLoginToken() == null) {
			throw new IllegalArgumentException("User must be authenticated to list agent traces by room.");
		}
		String userId = user.getPrimaryLoginToken().getId();

		int limit = DEFAULT_LIMIT;
		String limitStr = this.keyValue.get(KEY_LIMIT);
		if (limitStr != null && !limitStr.isEmpty()) {
			try {
				limit = Integer.parseInt(limitStr);
			} catch (NumberFormatException e) {
				classLogger.warn("ListAgentTracesByRoom: invalid limit '{}', using default.", limitStr);
			}
		}

		String projectId = this.keyValue.get(KEY_PROJECT_ID);

		// Fetch flat traces
		List<Map<String, Object>> rawTraces;
		try {
			if (projectId != null && !projectId.isEmpty()) {
				rawTraces = AgentTraceLogsUtils.listTracesByProject(projectId, userId, limit);
			} else {
				rawTraces = AgentTraceLogsUtils.listTraces(null, userId, limit);
			}
		} catch (Exception e) {
			classLogger.warn("ListAgentTracesByRoom: error fetching traces.", e);
			rawTraces = new ArrayList<>();
		}
		if (rawTraces == null) {
			rawTraces = new ArrayList<>();
		}

		// Group by ROOM_ID
		Map<String, List<Map<String, Object>>> byRoom = new LinkedHashMap<>();
		for (Map<String, Object> row : rawTraces) {
			String roomId = extractString(row, "ROOM_ID");
			if (roomId == null || roomId.isEmpty()) {
				roomId = "unknown";
			}
			byRoom.computeIfAbsent(roomId, k -> new ArrayList<>()).add(row);
		}

		// Build room group objects
		List<Map<String, Object>> roomGroups = new ArrayList<>(byRoom.size());
		for (Map.Entry<String, List<Map<String, Object>>> entry : byRoom.entrySet()) {
			roomGroups.add(buildRoomGroup(entry.getKey(), entry.getValue()));
		}

		// Sort by LAST_ACTIVITY descending
		roomGroups.sort((a, b) -> {
			String aTime = (String) a.get("LAST_ACTIVITY");
			String bTime = (String) b.get("LAST_ACTIVITY");
			if (aTime == null && bTime == null) return 0;
			if (aTime == null) return 1;
			if (bTime == null) return -1;
			return bTime.compareTo(aTime);
		});

		return new NounMetadata(roomGroups, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private Map<String, Object> buildRoomGroup(String roomId, List<Map<String, Object>> traces) {
		Map<String, Object> group = new LinkedHashMap<>();
		group.put("ROOM_ID", roomId);

		Set<String> users = new LinkedHashSet<>();
		Set<String> harnessTypes = new LinkedHashSet<>();
		String projectId = null;
		int totalToolCalls = 0;
		int totalInputTokens = 0;
		int totalOutputTokens = 0;
		long totalDurationMs = 0;
		boolean hasErrors = false;
		String lastActivity = null;

		List<Map<String, Object>> traceSummaries = new ArrayList<>(traces.size());

		for (Map<String, Object> row : traces) {
			// Collect users
			String uid = extractString(row, "USER_ID");
			if (uid != null && !uid.isEmpty()) users.add(uid);

			// Collect harness types
			String harness = extractString(row, "HARNESS_TYPE");
			if (harness != null && !harness.isEmpty()) harnessTypes.add(harness);

			// Project ID (take first non-null)
			if (projectId == null) {
				projectId = extractString(row, "PROJECT_ID");
			}

			// Tool calls
			Object toolCountObj = row.get("TOOL_CALL_COUNT");
			if (toolCountObj != null) {
				try { totalToolCalls += Integer.parseInt(String.valueOf(toolCountObj)); } catch (NumberFormatException ignored) {}
			}

			// Tokens from METRICS_JSON
			Object metricsJsonObj = row.get("METRICS_JSON") != null ? row.get("METRICS_JSON") : row.get("AGENT_TRACE__METRICS_JSON");
			int[] tokens = extractTokensFromMetrics(metricsJsonObj);
			totalInputTokens += tokens[0];
			totalOutputTokens += tokens[1];

			// Duration
			long durationMs = computeDurationMs(row.get("START_TIME"), row.get("END_TIME"));
			totalDurationMs += durationMs;

			// Error check
			String termReason = extractString(row, "TERMINATION_REASON");
			if (termReason != null && termReason.startsWith("ERROR")) {
				hasErrors = true;
			}

			// Last activity (track the latest START_TIME)
			String startTime = extractString(row, "START_TIME");
			if (startTime != null && (lastActivity == null || startTime.compareTo(lastActivity) > 0)) {
				lastActivity = startTime;
			}

			// Build trace summary
			Map<String, Object> summary = new LinkedHashMap<>();
			summary.put("TRACE_ID", extractString(row, "TRACE_ID"));
			summary.put("HARNESS_NAME", harness);
			summary.put("STATUS", normalizeStatus(termReason));
			summary.put("STARTED_AT", startTime);
			summary.put("DURATION_MS", durationMs);
			summary.put("TOOL_CALL_COUNT", toolCountObj);
			summary.put("ITERATIONS", row.get("ITERATIONS"));
			traceSummaries.add(summary);
		}

		group.put("PROJECT_ID", projectId);
		group.put("USERS", new ArrayList<>(users));
		group.put("HARNESS_TYPES", new ArrayList<>(harnessTypes));
		group.put("TOTAL_RUNS", traces.size());
		group.put("TOTAL_TOOL_CALLS", totalToolCalls);

		// If no tokens from METRICS_JSON, recover from MESSAGE table (room-level sum)
		if (totalInputTokens == 0 && totalOutputTokens == 0) {
			int[] recovered = AgentTraceLogsUtils.sumTokensForRoom(roomId, null);
			totalInputTokens = recovered[0];
			totalOutputTokens = recovered[1];
		}
		group.put("TOTAL_INPUT_TOKENS", totalInputTokens);
		group.put("TOTAL_OUTPUT_TOKENS", totalOutputTokens);
		group.put("TOTAL_DURATION_MS", totalDurationMs);
		group.put("LAST_ACTIVITY", lastActivity);
		group.put("HAS_ERRORS", hasErrors);
		group.put("TRACES", traceSummaries);

		return group;
	}

	private static String extractString(Map<String, Object> row, String key) {
		// Handle both raw column names and prefixed (AGENT_TRACE__) column names
		Object val = row.get(key);
		if (val == null) {
			val = row.get("AGENT_TRACE__" + key);
		}
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
