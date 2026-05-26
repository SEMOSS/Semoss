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
package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobStatus;

final class GitHubCopilotPixelJobStreamer {

	private static final int DESCRIPTION_LIMIT = 200;

	private final String jobId;
	private final String model;
	private final String sessionId;
	private final Map<String, StringBuilder> assistantBuffers = new LinkedHashMap<>();
	private final Map<String, ToolStreamState> toolStates = new LinkedHashMap<>();
	private final Set<String> suppressedToolCallIds = new LinkedHashSet<>();

	GitHubCopilotPixelJobStreamer(String jobId, String model, String sessionId) {
		this.jobId = jobId;
		this.model = model != null ? model : "";
		this.sessionId = sessionId != null ? sessionId : "";
	}

	void publishAssistantMessageDelta(String messageId, String deltaContent, String parentToolCallId,
			String timestamp) {
		if (isBlank(messageId) || isBlank(deltaContent)) {
			return;
		}

		StringBuilder buffer = this.assistantBuffers.computeIfAbsent(messageId, ignored -> new StringBuilder());
		buffer.append(deltaContent);
		publishAssistantEnvelope(messageId, buffer.toString(), true, parentToolCallId, null, null, timestamp);
	}

	void publishAssistantMessage(String messageId, String content, String parentToolCallId,
			List<Map<String, Object>> toolRequests, String timestamp) {
		if (isBlank(messageId)) {
			return;
		}

		String finalText = !isBlank(content) ? content : getBufferedText(messageId);
		if (!isBlank(finalText)) {
			this.assistantBuffers.put(messageId, new StringBuilder(finalText));
		}

		publishAssistantEnvelope(messageId, finalText, false, parentToolCallId,
				normalizeToolRequests(toolRequests, timestamp), null, timestamp);
	}

	void publishAssistantIntent(String eventId, String intent, String timestamp) {
		if (isBlank(eventId) || isBlank(intent)) {
			return;
		}

		publishAssistantEnvelope(eventId, intent, false, null, null, "intent", timestamp);
	}

	void suppressToolCall(String toolCallId) {
		if (!isBlank(toolCallId)) {
			this.suppressedToolCallIds.add(toolCallId);
		}
	}

	boolean isSuppressedToolCall(String toolCallId) {
		return !isBlank(toolCallId) && this.suppressedToolCallIds.contains(toolCallId);
	}

	void releaseSuppressedToolCall(String toolCallId) {
		if (!isBlank(toolCallId)) {
			this.suppressedToolCallIds.remove(toolCallId);
		}
	}

	void publishToolExecutionStart(String toolCallId, String toolName, Object arguments, String parentToolCallId,
			String timestamp) {
		if (isBlank(toolCallId) || isBlank(toolName)) {
			return;
		}

		ToolStreamState state = getOrCreateToolState(toolCallId);
		state.toolName = toolName;
		state.description = extractDescription(arguments);

		List<Map<String, Object>> toolInvocations = new ArrayList<>();
		toolInvocations.add(buildToolInvocation(toolCallId, toolName, state.description, parentToolCallId, timestamp));
		publishAssistantEnvelope(toolCallId, null, false, null, toolInvocations, null, timestamp);
	}

	void publishToolExecutionProgress(String toolCallId, String progressMessage, String timestamp) {
		if (isBlank(toolCallId) || isBlank(progressMessage) || isSuppressedToolCall(toolCallId)) {
			return;
		}

		ToolStreamState state = getOrCreateToolState(toolCallId);
		state.lastProgressMessage = progressMessage;
		publishToolResult(toolCallId, "running", buildToolProgressContent(state), true, timestamp);
	}

	void publishToolExecutionPartialResult(String toolCallId, String partialOutput, String timestamp) {
		if (isBlank(toolCallId) || isBlank(partialOutput) || isSuppressedToolCall(toolCallId)) {
			return;
		}

		ToolStreamState state = getOrCreateToolState(toolCallId);
		state.outputBuffer.append(partialOutput);
		publishToolResult(toolCallId, "running", buildToolProgressContent(state), true, timestamp);
	}

	void publishToolExecutionComplete(String toolCallId, boolean success, Map<String, Object> result,
			Map<String, Object> error, String timestamp) {
		if (isBlank(toolCallId) || isSuppressedToolCall(toolCallId)) {
			return;
		}

		ToolStreamState state = getOrCreateToolState(toolCallId);
		String content = extractToolResultContent(result, error);
		if (isBlank(content)) {
			content = buildToolProgressContent(state);
		}
		if (isBlank(content) && success) {
			String toolName = !isBlank(state.toolName) ? state.toolName : "Tool";
			content = "(" + toolName + " completed with no output)";
		}

		publishToolResult(toolCallId, success ? "completed" : "error", content, false, timestamp);
		this.toolStates.remove(toolCallId);
		this.suppressedToolCallIds.remove(toolCallId);
	}

	void publishSessionError(String errorType, String message, Number statusCode, String timestamp) {
		if (isBlank(this.jobId)) {
			return;
		}

		PixelJobManager.getManager().flagStatus(this.jobId, PixelJobStatus.ERROR);
		String errorMessage = !isBlank(message) ? message : "GitHub Copilot session error";
		StringBuilder logMessage = new StringBuilder(errorMessage);
		if (!isBlank(errorType)) {
			logMessage.append(" [").append(errorType).append("]");
		}
		if (statusCode != null) {
			logMessage.append(" (status=").append(statusCode).append(")");
		}
		PixelJobManager.getManager().addStdErr(this.jobId, logMessage.toString());
	}

	private void publishAssistantEnvelope(String eventId, String text, boolean isPartial, String parentToolCallId,
			List<Map<String, Object>> toolInvocations, String display, String timestamp) {
		boolean hasText = !isBlank(text);
		boolean hasToolInvocations = toolInvocations != null && !toolInvocations.isEmpty();
		if (!hasText && !hasToolInvocations) {
			return;
		}

		Map<String, Object> data = new LinkedHashMap<>();
		if (!isBlank(this.model)) {
			data.put("model", this.model);
		}
		if (!isBlank(timestamp)) {
			data.put("timestamp", timestamp);
		}

		if (hasText) {
			List<Map<String, Object>> texts = new ArrayList<>();
			Map<String, Object> textPayload = new LinkedHashMap<>();
			textPayload.put("eventId", eventId);
			textPayload.put("text", text);
			textPayload.put("timestamp", valueOrEmpty(timestamp));
			textPayload.put("isPartial", isPartial);
			if (!isBlank(display)) {
				textPayload.put("display", display);
			}
			if (!isBlank(this.model)) {
				textPayload.put("model", this.model);
			}
			if (!isBlank(parentToolCallId)) {
				textPayload.put("parentToolUseId", parentToolCallId);
			}
			texts.add(textPayload);
			data.put("texts", texts);
		}

		if (hasToolInvocations) {
			data.put("toolInvocations", toolInvocations);
		}

		pushEnvelope("assistant", eventId, data);
	}

	private void publishToolResult(String toolCallId, String status, String content, boolean isPartial,
			String timestamp) {
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", "tool-result");
		data.put("eventId", toolCallId);
		data.put("toolUseId", toolCallId);
		data.put("status", status);
		data.put("isPartial", isPartial);
		data.put("durationMs", 0);
		data.put("timestamp", valueOrEmpty(timestamp));
		if (!isBlank(content)) {
			data.put("content", content);
		}

		pushEnvelope("tool_result", toolCallId, data);
	}

	private List<Map<String, Object>> normalizeToolRequests(List<Map<String, Object>> toolRequests, String timestamp) {
		List<Map<String, Object>> items = new ArrayList<>();
		if (toolRequests == null) {
			return items;
		}

		for (Map<String, Object> request : toolRequests) {
			if (request == null) {
				continue;
			}

			String toolCallId = stringValue(request.get("toolCallId"));
			String toolName = stringValue(request.get("name"));
			if (isBlank(toolCallId) || isBlank(toolName) || isReportIntentTool(toolName)) {
				continue;
			}

			ToolStreamState state = getOrCreateToolState(toolCallId);
			state.toolName = toolName;
			state.description = firstNonBlank(
					stringValue(request.get("intentionSummary")),
					extractDescription(request.get("arguments")));
			items.add(buildToolInvocation(toolCallId, toolName, state.description, null, timestamp));
		}

		return items;
	}

	private Map<String, Object> buildToolInvocation(String toolCallId, String toolName, String description,
			String parentToolCallId, String timestamp) {
		Map<String, Object> toolInvocation = new LinkedHashMap<>();
		toolInvocation.put("eventId", toolCallId);
		toolInvocation.put("toolUseId", toolCallId);
		toolInvocation.put("toolName", toolName);
		toolInvocation.put("timestamp", valueOrEmpty(timestamp));
		if (!isBlank(description)) {
			toolInvocation.put("description", description);
		}
		if (!isBlank(parentToolCallId)) {
			toolInvocation.put("parentToolUseId", parentToolCallId);
		}
		return toolInvocation;
	}

	private String extractDescription(Object arguments) {
		if (arguments instanceof Map) {
			Map<?, ?> argMap = (Map<?, ?>) arguments;
			Object description = firstNonNull(
					argMap.get("description"),
					argMap.get("prompt"),
					argMap.get("file_path"),
					argMap.get("filePath"),
					argMap.get("path"),
					argMap.get("command"),
					argMap.get("pattern"),
					argMap.get("glob"),
					argMap.get("query"),
					argMap.get("url"),
					argMap.get("intent"));
			return truncate(stringValue(description));
		}
		return truncate(stringValue(arguments));
	}

	private String firstNonBlank(String... values) {
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

	private String extractToolResultContent(Map<String, Object> result, Map<String, Object> error) {
		if (error != null) {
			String message = stringValue(error.get("message"));
			if (!isBlank(message)) {
				return message;
			}
		}

		if (result != null) {
			Object content = firstNonNull(result.get("content"), result.get("detailedContent"));
			String value = stringValue(content);
			if (!isBlank(value)) {
				return value;
			}
		}

		return null;
	}

	private String buildToolProgressContent(ToolStreamState state) {
		if (state == null) {
			return null;
		}

		String output = state.outputBuffer.length() > 0 ? state.outputBuffer.toString() : null;
		if (!isBlank(state.lastProgressMessage) && !isBlank(output)) {
			return state.lastProgressMessage + "\n\n" + output;
		}
		if (!isBlank(output)) {
			return output;
		}
		return state.lastProgressMessage;
	}

	private void pushEnvelope(String event, String uuid, Map<String, Object> data) {
		if (isBlank(this.jobId)) {
			return;
		}

		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", "content");

		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("event", event);
		payload.put("uuid", valueOrEmpty(uuid));
		payload.put("sessionId", this.sessionId);
		payload.put("data", data);

		envelope.put("data", payload);
		PixelJobManager.getManager().addStreamOut(this.jobId, envelope);
	}

	private ToolStreamState getOrCreateToolState(String toolCallId) {
		return this.toolStates.computeIfAbsent(toolCallId, ignored -> new ToolStreamState());
	}

	private String getBufferedText(String messageId) {
		StringBuilder buffer = this.assistantBuffers.get(messageId);
		return buffer == null ? null : buffer.toString();
	}

	private Object firstNonNull(Object... values) {
		if (values == null) {
			return null;
		}
		for (Object value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	private String truncate(String value) {
		if (isBlank(value)) {
			return null;
		}
		if (value.length() <= DESCRIPTION_LIMIT) {
			return value;
		}
		return value.substring(0, DESCRIPTION_LIMIT) + "...";
	}

	private String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value);
		return text != null && !text.isBlank() ? text : null;
	}

	private String valueOrEmpty(String value) {
		return value != null ? value : "";
	}

	private boolean isReportIntentTool(String toolName) {
		return toolName != null && "report_intent".equalsIgnoreCase(toolName.trim());
	}

	private boolean isBlank(String value) {
		return value == null || value.isBlank();
	}

	private static final class ToolStreamState {
		private String toolName;
		private String description;
		private String lastProgressMessage;
		private final StringBuilder outputBuffer = new StringBuilder();
	}
}
