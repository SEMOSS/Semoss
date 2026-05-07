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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

		// Fetch all conversation messages (INPUT + RESPONSE) for prompt/response correlation
		List<Map<String, Object>> conversation = AgentTraceLogsUtils.fetchConversationForRoom(roomId);
		// Also extract just user messages for backward compat
		List<Map<String, Object>> allMessages = new ArrayList<>();
		for (Map<String, Object> msg : conversation) {
			if ("INPUT".equals(msg.get("MESSAGE_TYPE"))) {
				allMessages.add(msg);
			}
		}

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

		// Assign user prompts and agent responses to traces by order
		assignPromptsAndResponsesToTraces(enrichedTraces, conversation);

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

		String traceId = AgentTraceViewHelper.extractString(row, "TRACE_ID");
		String startTime = AgentTraceViewHelper.extractString(row, "START_TIME");
		String endTime = AgentTraceViewHelper.extractString(row, "END_TIME");
		String harnessType = AgentTraceViewHelper.extractString(row, "HARNESS_TYPE");
		String termReason = AgentTraceViewHelper.extractString(row, "TERMINATION_REASON");

		trace.put("TRACE_ID", traceId);
		trace.put("ROOM_ID", roomId);
		trace.put("USER_ID", AgentTraceViewHelper.extractString(row, "USER_ID"));
		trace.put("PROJECT_ID", AgentTraceViewHelper.extractString(row, "PROJECT_ID"));
		trace.put("HARNESS_NAME", harnessType);
		trace.put("STARTED_AT", startTime);
		trace.put("ENDED_AT", endTime);
		trace.put("DURATION_MS", AgentTraceViewHelper.computeDurationMs(startTime, endTime));
		trace.put("STATUS", AgentTraceViewHelper.normalizeStatus(termReason));
		trace.put("ITERATIONS", row.get("ITERATIONS") != null ? row.get("ITERATIONS") : row.get("AGENT_TRACE__ITERATIONS"));
		trace.put("TOOL_CALL_COUNT", row.get("TOOL_CALL_COUNT") != null ? row.get("TOOL_CALL_COUNT") : row.get("AGENT_TRACE__TOOL_CALL_COUNT"));

		// Tokens — recover from MESSAGE table using direct TRACE_ID correlation
		Object metricsJson = row.get("METRICS_JSON") != null ? row.get("METRICS_JSON") : row.get("AGENT_TRACE__METRICS_JSON");
		int[] tokens = AgentTraceViewHelper.extractTokensFromMetrics(metricsJson);
		if (tokens[0] == 0 && tokens[1] == 0) {
			int[] recovered = AgentTraceLogsUtils.sumTokensForTrace(traceId, roomId);
			tokens[0] = recovered[0];
			tokens[1] = recovered[1];
		}
		trace.put("TOTAL_INPUT_TOKENS", tokens[0]);
		trace.put("TOTAL_OUTPUT_TOKENS", tokens[1]);
		trace.put("METRICS_JSON", metricsJson != null ? String.valueOf(metricsJson) : null);

		trace.put("TERMINATION_REASON", termReason);
		trace.put("MODEL_ENGINE_ID", AgentTraceViewHelper.extractString(row, "MODEL_ENGINE_ID"));
		trace.put("PARENT_TRACE_ID", AgentTraceViewHelper.extractString(row, "PARENT_TRACE_ID"));

		// Fetch tool steps
		if (traceId != null) {
			List<Map<String, Object>> steps = AgentTraceLogsUtils.listTraceSteps(traceId, userId);
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
	 * Correlates user prompts and agent responses from the conversation.
	 * Strategy: walk through conversation messages in chronological order.
	 * Each INPUT message is a user prompt; the RESPONSE that follows it is the agent's reply.
	 * Traces are matched by position (Nth trace = Nth prompt/response pair).
	 */
	private void assignPromptsAndResponsesToTraces(List<Map<String, Object>> enrichedTraces, List<Map<String, Object>> conversation) {
		// Build pairs: each pair is (userPrompt, agentResponse)
		List<String[]> pairs = new ArrayList<>();
		for (int i = 0; i < conversation.size(); i++) {
			Map<String, Object> msg = conversation.get(i);
			String type = (String) msg.get("MESSAGE_TYPE");
			String data = (String) msg.get("MESSAGE_DATA");
			if (!"INPUT".equals(type)) continue;
			if (data == null || data.isEmpty()) continue;
			// Skip tool outputs, JSON blobs — only keep real user prompts
			if (data.startsWith("{") || data.startsWith("[") || data.startsWith("This tool execution")) continue;
			if (data.length() > 500) continue;

			// Look ahead for the next RESPONSE message
			String response = null;
			for (int j = i + 1; j < conversation.size(); j++) {
				Map<String, Object> next = conversation.get(j);
				String nextType = (String) next.get("MESSAGE_TYPE");
				if ("INPUT".equals(nextType)) break; // hit next user message — no response found
				if ("RESPONSE".equals(nextType)) {
					response = (String) next.get("MESSAGE_DATA");
					break;
				}
			}
			pairs.add(new String[] { data, response });
		}

		for (int i = 0; i < enrichedTraces.size(); i++) {
			if (i < pairs.size()) {
				enrichedTraces.get(i).put("USER_PROMPT", pairs.get(i)[0]);
				if (pairs.get(i)[1] != null) {
					enrichedTraces.get(i).put("AGENT_RESPONSE", pairs.get(i)[1]);
				}
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns full detail for a specific room's agent activity including traces, steps, tokens, and user prompts.";
	}

	@Override
	public String getDescriptionForKey(String key) {
		if (KEY_ROOM_ID.equals(key)) return "The room ID to fetch agent detail for.";
		if (KEY_INCLUDE_MESSAGES.equals(key)) return "Whether to include raw user messages (default true).";
		return super.getDescriptionForKey(key);
	}
}
