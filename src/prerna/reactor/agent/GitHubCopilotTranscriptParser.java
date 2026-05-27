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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Parses GitHub Copilot session-state JSONL events into the same envelope shape
 * the live streaming path emits to the frontend.
 */
public class GitHubCopilotTranscriptParser {

	private static final int DESCRIPTION_LIMIT = 200;

	private GitHubCopilotTranscriptParser() {
		// utility class
	}

	public static List<JSONObject> parse(JSONObject raw, String sessionIdFallback) {
		return parse(raw, sessionIdFallback, new HashSet<String>());
	}

	public static List<JSONObject> parse(JSONObject raw, String sessionIdFallback, Set<String> suppressedToolCallIds) {
		List<JSONObject> events = new ArrayList<>();
		if (raw == null) {
			return events;
		}

		String type = readString(raw, "type");
		if (isBlank(type)) {
			return events;
		}

		switch (type) {
		case "user.message":
			addIfPresent(events, parseUserMessage(raw, sessionIdFallback));
			return events;
		case "assistant.message":
			return parseAssistantMessage(raw, sessionIdFallback, suppressedToolCallIds);
		case "assistant.message.delta":
			addIfPresent(events, parseAssistantMessageDelta(raw, sessionIdFallback));
			return events;
		case "assistant.intent":
			addIfPresent(events, parseAssistantIntent(raw, sessionIdFallback));
			return events;
		case "tool.execution_start":
			addIfPresent(events, parseToolExecutionStart(raw, sessionIdFallback, suppressedToolCallIds));
			return events;
		case "tool.execution_progress":
		case "tool.execution_partial_result":
			addIfPresent(events, parseToolExecutionProgress(raw, sessionIdFallback, suppressedToolCallIds));
			return events;
		case "tool.execution_complete":
			addIfPresent(events, parseToolExecutionComplete(raw, sessionIdFallback, suppressedToolCallIds));
			return events;
		default:
			return events;
		}
	}

	private static JSONObject parseUserMessage(JSONObject raw, String sessionIdFallback) {
		JSONObject data = raw.optJSONObject("data");
		String promptId = firstNonBlank(readString(raw, "id"), readString(data, "messageId"), readString(data, "message_id"));
		String prompt = firstNonBlank(readString(data, "content"), readString(data, "text"));
		if (isBlank(promptId) || isBlank(prompt)) {
			return null;
		}

		JSONObject payload = new JSONObject();
		payload.put("promptId", promptId);
		payload.put("text", prompt);
		payload.put("timestamp", readTimestamp(raw, data));
		return toEvent("user_prompt", promptId, resolveSessionId(raw, sessionIdFallback), payload);
	}

	private static List<JSONObject> parseAssistantMessage(JSONObject raw, String sessionIdFallback,
			Set<String> suppressedToolCallIds) {
		List<JSONObject> events = new ArrayList<>();
		JSONObject rawData = raw.optJSONObject("data");
		if (rawData == null) {
			return events;
		}

		String messageId = firstNonBlank(readString(rawData, "messageId"), readString(rawData, "message_id"),
				readString(raw, "id"));
		if (isBlank(messageId)) {
			return events;
		}

		String timestamp = readTimestamp(raw, rawData);
		String model = firstNonBlank(readString(rawData, "model"), readString(raw, "model"));

		JSONArray texts = new JSONArray();
		String content = readString(rawData, "content");
		if (!isBlank(content)) {
			JSONObject text = new JSONObject();
			text.put("eventId", messageId);
			text.put("text", content);
			text.put("timestamp", timestamp);
			text.put("isPartial", false);
			if (!isBlank(model)) {
				text.put("model", model);
			}
			texts.put(text);
		}

		JSONArray toolInvocations = new JSONArray();
		JSONArray toolRequests = rawData.optJSONArray("toolRequests");
		if (toolRequests == null) {
			toolRequests = rawData.optJSONArray("tool_requests");
		}
		if (toolRequests != null) {
			for (int i = 0; i < toolRequests.length(); i++) {
				JSONObject request = toolRequests.optJSONObject(i);
				if (request == null) {
					continue;
				}

				String toolCallId = firstNonBlank(readString(request, "toolCallId"), readString(request, "tool_call_id"),
						readString(request, "id"));
				String toolName = firstNonBlank(readString(request, "name"), readString(request, "toolName"),
						readString(request, "tool_name"));
				if (isBlank(toolCallId) || isBlank(toolName)) {
					continue;
				}

				if (isReportIntentTool(toolName)) {
					String intent = firstNonBlank(readString(request, "intentionSummary"),
							readString(request, "intention_summary"),
							extractDescription(request.opt("arguments")));
					if (!isBlank(toolCallId)) {
						suppressedToolCallIds.add(toolCallId);
					}
					if (isBlank(intent)) {
						continue;
					}

					JSONObject intentPayload = new JSONObject();
					intentPayload.put("eventId", toolCallId);
					intentPayload.put("text", intent);
					intentPayload.put("display", "intent");
					intentPayload.put("timestamp", timestamp);
					intentPayload.put("isPartial", false);
					if (!isBlank(model)) {
						intentPayload.put("model", model);
					}
					texts.put(intentPayload);
					continue;
				}

				JSONObject invocation = new JSONObject();
				invocation.put("eventId", toolCallId);
				invocation.put("toolUseId", toolCallId);
				invocation.put("toolName", toolName);
				invocation.put("timestamp", timestamp);

				String description = firstNonBlank(readString(request, "intentionSummary"),
						readString(request, "intention_summary"), extractDescription(request.opt("arguments")));
				if (!isBlank(description)) {
					invocation.put("description", description);
				}
				toolInvocations.put(invocation);
			}
		}

		if (texts.length() == 0 && toolInvocations.length() == 0) {
			return events;
		}

		JSONObject payload = new JSONObject();
		if (!isBlank(model)) {
			payload.put("model", model);
		}
		payload.put("timestamp", timestamp);
		if (texts.length() > 0) {
			payload.put("texts", texts);
		}
		if (toolInvocations.length() > 0) {
			payload.put("toolInvocations", toolInvocations);
		}

		events.add(toEvent("assistant", messageId, resolveSessionId(raw, sessionIdFallback), payload));
		return events;
	}

	private static JSONObject parseAssistantMessageDelta(JSONObject raw, String sessionIdFallback) {
		JSONObject data = raw.optJSONObject("data");
		if (data == null) {
			return null;
		}

		String eventId = firstNonBlank(readString(data, "messageId"), readString(data, "message_id"), readString(raw, "id"));
		String delta = firstNonBlank(readString(data, "deltaContent"), readString(data, "delta_content"));
		if (isBlank(eventId) || isBlank(delta)) {
			return null;
		}

		String timestamp = readTimestamp(raw, data);
		JSONObject text = new JSONObject();
		text.put("eventId", eventId);
		text.put("text", delta);
		text.put("timestamp", timestamp);
		text.put("isPartial", true);
		String model = firstNonBlank(readString(data, "model"), readString(raw, "model"));
		if (!isBlank(model)) {
			text.put("model", model);
		}

		String parentToolUseId = firstNonBlank(readString(data, "parentToolCallId"), readString(data, "parent_tool_call_id"));
		if (!isBlank(parentToolUseId)) {
			text.put("parentToolUseId", parentToolUseId);
		}

		JSONObject payload = new JSONObject();
		if (!isBlank(model)) {
			payload.put("model", model);
		}
		payload.put("timestamp", timestamp);
		payload.put("texts", new JSONArray().put(text));
		return toEvent("assistant", eventId, resolveSessionId(raw, sessionIdFallback), payload);
	}

	private static JSONObject parseAssistantIntent(JSONObject raw, String sessionIdFallback) {
		JSONObject data = raw.optJSONObject("data");
		if (data == null) {
			return null;
		}

		String eventId = firstNonBlank(readString(raw, "id"), readString(data, "id"));
		String intent = firstNonBlank(readString(data, "intent"), readString(data, "content"));
		if (isBlank(eventId) || isBlank(intent)) {
			return null;
		}

		String timestamp = readTimestamp(raw, data);
		String model = firstNonBlank(readString(data, "model"), readString(raw, "model"));
		JSONObject text = new JSONObject();
		text.put("eventId", eventId);
		text.put("text", intent);
		text.put("display", "intent");
		text.put("timestamp", timestamp);
		text.put("isPartial", false);
		if (!isBlank(model)) {
			text.put("model", model);
		}

		JSONObject payload = new JSONObject();
		if (!isBlank(model)) {
			payload.put("model", model);
		}
		payload.put("timestamp", timestamp);
		payload.put("texts", new JSONArray().put(text));
		return toEvent("assistant", eventId, resolveSessionId(raw, sessionIdFallback), payload);
	}

	private static JSONObject parseToolExecutionStart(JSONObject raw, String sessionIdFallback,
			Set<String> suppressedToolCallIds) {
		JSONObject data = raw.optJSONObject("data");
		if (data == null) {
			return null;
		}

		String toolCallId = firstNonBlank(readString(data, "toolCallId"), readString(data, "tool_call_id"),
				readString(raw, "id"));
		String toolName = firstNonBlank(readString(data, "toolName"), readString(data, "tool_name"));
		if (isBlank(toolCallId) || isBlank(toolName)) {
			return null;
		}
		if (isReportIntentTool(toolName)) {
			suppressedToolCallIds.add(toolCallId);
			return null;
		}

		String timestamp = readTimestamp(raw, data);
		JSONObject invocation = new JSONObject();
		invocation.put("eventId", toolCallId);
		invocation.put("toolUseId", toolCallId);
		invocation.put("toolName", toolName);
		invocation.put("timestamp", timestamp);

		String description = extractDescription(data.opt("arguments"));
		if (!isBlank(description)) {
			invocation.put("description", description);
		}

		JSONObject payload = new JSONObject();
		payload.put("timestamp", timestamp);
		payload.put("toolInvocations", new JSONArray().put(invocation));
		String model = firstNonBlank(readString(data, "model"), readString(raw, "model"));
		if (!isBlank(model)) {
			payload.put("model", model);
		}
		return toEvent("assistant", toolCallId, resolveSessionId(raw, sessionIdFallback), payload);
	}

	private static JSONObject parseToolExecutionProgress(JSONObject raw, String sessionIdFallback,
			Set<String> suppressedToolCallIds) {
		JSONObject data = raw.optJSONObject("data");
		if (data == null) {
			return null;
		}

		String toolCallId = firstNonBlank(readString(data, "toolCallId"), readString(data, "tool_call_id"),
				readString(raw, "id"));
		if (isBlank(toolCallId) || suppressedToolCallIds.contains(toolCallId)) {
			return null;
		}

		String content = firstNonBlank(readString(data, "progressMessage"), readString(data, "progress_message"),
				readString(data, "partialOutput"), readString(data, "partial_output"));
		if (isBlank(content)) {
			return null;
		}

		JSONObject payload = new JSONObject();
		payload.put("kind", "tool-result");
		payload.put("eventId", toolCallId);
		payload.put("toolUseId", toolCallId);
		payload.put("status", "running");
		payload.put("isPartial", true);
		payload.put("durationMs", 0);
		payload.put("timestamp", readTimestamp(raw, data));
		payload.put("content", content);
		return toEvent("tool_result", toolCallId, resolveSessionId(raw, sessionIdFallback), payload);
	}

	private static JSONObject parseToolExecutionComplete(JSONObject raw, String sessionIdFallback,
			Set<String> suppressedToolCallIds) {
		JSONObject data = raw.optJSONObject("data");
		if (data == null) {
			return null;
		}

		String toolCallId = firstNonBlank(readString(data, "toolCallId"), readString(data, "tool_call_id"),
				readString(raw, "id"));
		if (isBlank(toolCallId)) {
			return null;
		}
		if (suppressedToolCallIds.remove(toolCallId)) {
			return null;
		}

		JSONObject result = data.optJSONObject("result");
		JSONObject error = data.optJSONObject("error");
		boolean success = data.optBoolean("success", false);
		String content = extractToolResultContent(result, error);
		if (isBlank(content) && success) {
			content = "(tool completed with no output)";
		}

		JSONObject payload = new JSONObject();
		payload.put("kind", "tool-result");
		payload.put("eventId", toolCallId);
		payload.put("toolUseId", toolCallId);
		payload.put("status", success ? "completed" : "error");
		payload.put("isPartial", false);
		payload.put("durationMs", 0);
		payload.put("timestamp", readTimestamp(raw, data));
		if (!isBlank(content)) {
			payload.put("content", content);
		}
		return toEvent("tool_result", toolCallId, resolveSessionId(raw, sessionIdFallback), payload);
	}

	private static JSONObject toEvent(String eventType, String uuid, String sessionId, JSONObject data) {
		JSONObject event = new JSONObject();
		event.put("event", eventType);
		event.put("uuid", valueOrEmpty(uuid));
		event.put("sessionId", valueOrEmpty(sessionId));
		event.put("data", data != null ? data : new JSONObject());
		return event;
	}

	private static String resolveSessionId(JSONObject raw, String sessionIdFallback) {
		return firstNonBlank(readString(raw, "sessionId"), readString(raw.optJSONObject("data"), "sessionId"),
				sessionIdFallback);
	}

	private static String readTimestamp(JSONObject raw, JSONObject data) {
		return firstNonBlank(readString(raw, "timestamp"), readString(data, "timestamp"), "");
	}

	private static String extractDescription(Object arguments) {
		if (arguments instanceof JSONObject) {
			JSONObject argJson = (JSONObject) arguments;
			return firstNonBlank(truncate(readString(argJson, "description")), truncate(readString(argJson, "prompt")),
					truncate(readString(argJson, "file_path")), truncate(readString(argJson, "filePath")),
					truncate(readString(argJson, "path")), truncate(readString(argJson, "command")),
					truncate(readString(argJson, "pattern")), truncate(readString(argJson, "glob")),
					truncate(readString(argJson, "query")), truncate(readString(argJson, "url")),
					truncate(readString(argJson, "intent")));
		}

		if (arguments != null) {
			return truncate(String.valueOf(arguments));
		}
		return null;
	}

	private static String extractToolResultContent(JSONObject result, JSONObject error) {
		if (error != null) {
			String message = readString(error, "message");
			if (!isBlank(message)) {
				return message;
			}
		}

		if (result != null) {
			String direct = firstNonBlank(readString(result, "content"), readString(result, "detailedContent"));
			if (!isBlank(direct)) {
				return direct;
			}

			Object content = result.opt("content");
			if (content instanceof JSONArray) {
				JSONArray blocks = (JSONArray) content;
				StringBuilder builder = new StringBuilder();
				for (int i = 0; i < blocks.length(); i++) {
					Object item = blocks.opt(i);
					String piece = null;
					if (item instanceof JSONObject) {
						JSONObject block = (JSONObject) item;
						piece = firstNonBlank(readString(block, "text"), readString(block, "content"));
					} else if (item != null) {
						piece = String.valueOf(item);
					}
					if (!isBlank(piece)) {
						if (builder.length() > 0) {
							builder.append("\n");
						}
						builder.append(piece);
					}
				}
				if (builder.length() > 0) {
					return builder.toString();
				}
			}
		}

		return null;
	}

	private static boolean isReportIntentTool(String toolName) {
		return !isBlank(toolName) && "report_intent".equalsIgnoreCase(toolName.trim());
	}

	private static String truncate(String value) {
		if (isBlank(value)) {
			return null;
		}
		return value.length() <= DESCRIPTION_LIMIT ? value : value.substring(0, DESCRIPTION_LIMIT) + "...";
	}

	private static String readString(JSONObject json, String... keys) {
		if (json == null || keys == null) {
			return null;
		}
		for (String key : keys) {
			if (json.has(key) && !json.isNull(key)) {
				String value = String.valueOf(json.opt(key));
				if (!isBlank(value)) {
					return value;
				}
			}
		}
		return null;
	}

	private static String firstNonBlank(String... values) {
		if (values == null) {
			return null;
		}
		for (String value : values) {
			if (!isBlank(value)) {
				return value;
			}
		}
		return null;
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

	private static String valueOrEmpty(String value) {
		return value != null ? value : "";
	}

	private static void addIfPresent(List<JSONObject> events, JSONObject event) {
		if (event != null) {
			events.add(event);
		}
	}
}
