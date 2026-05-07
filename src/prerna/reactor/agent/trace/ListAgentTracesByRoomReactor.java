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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
			String roomId = AgentTraceViewHelper.extractString(row, "ROOM_ID");
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
			String uid = AgentTraceViewHelper.extractString(row, "USER_ID");
			if (uid != null && !uid.isEmpty()) users.add(uid);

			String harness = AgentTraceViewHelper.extractString(row, "HARNESS_TYPE");
			if (harness != null && !harness.isEmpty()) harnessTypes.add(harness);

			if (projectId == null) {
				projectId = AgentTraceViewHelper.extractString(row, "PROJECT_ID");
			}

			Object toolCountObj = row.get("TOOL_CALL_COUNT");
			if (toolCountObj != null) {
				try { totalToolCalls += Integer.parseInt(String.valueOf(toolCountObj)); } catch (NumberFormatException ignored) {}
			}

			Object metricsJsonObj = row.get("METRICS_JSON") != null ? row.get("METRICS_JSON") : row.get("AGENT_TRACE__METRICS_JSON");
			int[] tokens = AgentTraceViewHelper.extractTokensFromMetrics(metricsJsonObj);
			totalInputTokens += tokens[0];
			totalOutputTokens += tokens[1];

			long durationMs = AgentTraceViewHelper.computeDurationMs(row.get("START_TIME"), row.get("END_TIME"));
			totalDurationMs += durationMs;

			String termReason = AgentTraceViewHelper.extractString(row, "TERMINATION_REASON");
			if (termReason != null && termReason.startsWith("ERROR")) {
				hasErrors = true;
			}

			String startTime = AgentTraceViewHelper.extractString(row, "START_TIME");
			if (startTime != null && (lastActivity == null || startTime.compareTo(lastActivity) > 0)) {
				lastActivity = startTime;
			}

			Map<String, Object> summary = new LinkedHashMap<>();
			summary.put("TRACE_ID", AgentTraceViewHelper.extractString(row, "TRACE_ID"));
			summary.put("HARNESS_NAME", harness);
			summary.put("STATUS", AgentTraceViewHelper.normalizeStatus(termReason));
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

	@Override
	public String getReactorDescription() {
		return "Returns agent traces grouped by room with per-room aggregate statistics (token usage, tool calls, duration).";
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (KEY_LIMIT.equals(key)) return "Maximum number of traces to retrieve (default 200).";
		if (KEY_PROJECT_ID.equals(key)) return "Optional project ID to filter traces by.";
		return super.getDescriptionForKey(key);
	}
}
