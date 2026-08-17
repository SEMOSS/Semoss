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
package prerna.reactor.agent.stream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.reactor.agent.ClaudeCodeTranscriptLocator;
import prerna.reactor.agent.ClaudeCodeTranscriptParser;
import prerna.reactor.agent.runtime.SemossAgentHarness;

/**
 * Projects Claude Code's provider-specific live envelopes and JSONL transcript
 * entries into the canonical AgentRun activity contract. The raw transcript is
 * never changed and remains the compatibility source for legacy callers.
 */
public final class ClaudeCodeRunActivityAdapter {

	private static final Logger logger = LogManager.getLogger(ClaudeCodeRunActivityAdapter.class);
	private static final long HISTORY_WINDOW_SLOP_MS = 10_000L;

	private ClaudeCodeRunActivityAdapter() {
	}

	public static boolean isProviderEnvelope(Map<String, Object> value) {
		if (value == null || !(value.get("data") instanceof Map)) {
			return false;
		}
		String event = stringValue(value.get("event"));
		return "assistant".equals(event) || "tool_result".equals(event) || "result".equals(event)
				|| "user_prompt".equals(event) || "max_turns_reached".equals(event);
	}

	public static void publishLive(String runId, Map<String, Object> providerEvent,
			AgentRunStreamService streams) {
		if (runId == null || runId.isBlank() || providerEvent == null || streams == null) {
			return;
		}
		String eventType = stringValue(providerEvent.get("event"));
		Map<String, Object> data = asMap(providerEvent.get("data"));
		if (eventType == null || data == null) {
			return;
		}
		String eventId = firstNonBlank(stringValue(providerEvent.get("uuid")), runId + ":claude");

		if ("assistant".equals(eventType)) {
			List<Object> thinking = asList(data.get("thinking"));
			for (int i = 0; i < thinking.size(); i++) {
				Map<String, Object> block = asMap(thinking.get(i));
				String summary = block == null ? null : stringValue(block.get("thinking"));
				if (summary != null && !summary.isBlank()) {
					streams.publishReasoningCompleted(runId, eventId + ":thinking:" + i, summary);
				}
			}

			List<Object> texts = asList(data.get("texts"));
			for (int i = 0; i < texts.size(); i++) {
				Map<String, Object> block = asMap(texts.get(i));
				String text = block == null ? null : stringValue(block.get("text"));
				if (text != null && !text.isBlank()) {
					streams.publishMessageCompleted(runId, eventId + ":text:" + i, text, null);
				}
			}

			List<Object> toolInvocations = asList(data.get("toolInvocations"));
			for (int i = 0; i < toolInvocations.size(); i++) {
				Map<String, Object> invocation = asMap(toolInvocations.get(i));
				if (invocation == null) {
					continue;
				}
				String toolUseId = firstNonBlank(stringValue(invocation.get("toolUseId")),
						eventId + ":tool:" + i);
				String toolName = firstNonBlank(stringValue(invocation.get("toolName")), "Tool");
				Map<String, Object> arguments = asMap(invocation.get("arguments"));
				Map<String, Object> metadata = new LinkedHashMap<>();
				putIfPresent(metadata, "subagentType", invocation.get("subagentType"));
				Map<String, Object> item = AgentStreamItems.toolItem(toolUseId, toolName, arguments, metadata,
						AgentStreamItems.TOOL_RUNNING);
				putIfPresent(item, "description", invocation.get("description"));
				streams.publishToolStarted(runId, item);
			}
			return;
		}

		if ("tool_result".equals(eventType)) {
			String toolUseId = stringValue(data.get("toolUseId"));
			if (toolUseId == null || toolUseId.isBlank()) {
				return;
			}
			boolean failed = isFailedStatus(stringValue(data.get("status")));
			String content = AgentStreamItems.truncate(stringValue(data.get("content")),
					AgentStreamItems.MAX_TOOL_OUTPUT_CHARS);
			Map<String, Object> item = new LinkedHashMap<>();
			item.put("id", toolUseId);
			item.put("kind", AgentStreamItems.KIND_TOOL);
			item.put("status", failed ? AgentStreamItems.TOOL_FAILED : AgentStreamItems.TOOL_COMPLETED);
			putIfPresent(item, failed ? "error" : "output", content);
			Long durationMs = longValue(data.get("durationMs"));
			if (durationMs != null) {
				item.put("durationMs", durationMs);
			}
			streams.publishToolCompleted(runId, item);
		}
	}

	/**
	 * Merge one run's projected Claude activity between its existing durable input
	 * and final-output messages.
	 */
	public static List<Map<String, Object>> projectMessages(Map<String, Object> run,
			List<Map<String, Object>> durableMessages) {
		List<Map<String, Object>> existing = durableMessages == null ? List.of() : durableMessages;
		String roomId = stringValue(run.get("roomId"));
		String runId = stringValue(run.get("runId"));
		if (roomId == null || runId == null) {
			return existing;
		}

		try {
			List<TranscriptEvent> events = readEvents(roomId);
			List<TranscriptEvent> segment = selectRunSegment(events, run);
			if (segment.isEmpty()) {
				return existing;
			}
			List<Map<String, Object>> projected = projectSegment(segment, run);
			if (projected.isEmpty()) {
				return existing;
			}
			normalizeAggregateFinalText(run, projected);

			List<Map<String, Object>> merged = new ArrayList<>(existing.size() + projected.size());
			for (Map<String, Object> message : existing) {
				if (!"final_output".equals(agentRunRole(message))) {
					merged.add(message);
				}
			}
			merged.addAll(projected);
			for (Map<String, Object> message : existing) {
				if ("final_output".equals(agentRunRole(message))) {
					merged.add(message);
				}
			}
			return merged;
		} catch (Exception e) {
			logger.warn("Unable to project Claude Code transcript for runId={} roomId={}", runId, roomId, e);
			return existing;
		}
	}

	/**
	 * The Claude SDK currently returns the concatenation of every assistant text
	 * block as the run final output. When that exact aggregate is already present
	 * as separate activity messages, expose the last block as finalText so the UI
	 * can de-duplicate the actual final answer instead of repeating the full run.
	 */
	private static void normalizeAggregateFinalText(Map<String, Object> run,
			List<Map<String, Object>> projectedMessages) {
		String persistedFinal = stringValue(run.get("finalText"));
		if (persistedFinal == null || persistedFinal.isBlank()) {
			return;
		}
		List<String> assistantTexts = new ArrayList<>();
		for (Map<String, Object> message : projectedMessages) {
			if (!"OUTPUT".equals(message.get("io"))) {
				continue;
			}
			for (Object partValue : asList(message.get("parts"))) {
				Map<String, Object> part = asMap(partValue);
				String text = part != null && "TEXT".equals(part.get("type"))
						? stringValue(part.get("text"))
						: null;
				if (text != null && !text.isBlank()) {
					assistantTexts.add(text);
				}
			}
		}
		if (assistantTexts.size() < 2) {
			return;
		}
		String aggregate = normalizeText(String.join("", assistantTexts));
		if (aggregate.equals(normalizeText(persistedFinal))) {
			run.put("finalText", assistantTexts.get(assistantTexts.size() - 1));
		}
	}

	private static List<TranscriptEvent> readEvents(String roomId) throws IOException {
		Path transcript = ClaudeCodeTranscriptLocator.findJsonlFile(roomId);
		if (transcript == null) {
			return List.of();
		}
		List<TranscriptEvent> events = new ArrayList<>();
		try (var lines = Files.lines(transcript)) {
			lines.forEach(line -> {
				if (line == null || line.isBlank()) {
					return;
				}
				try {
					JSONObject parsed = ClaudeCodeTranscriptParser.parse(new JSONObject(line));
					if (parsed != null) {
						Map<String, Object> envelope = parsed.toMap();
						events.add(new TranscriptEvent(envelope, eventTimestamp(envelope)));
					}
				} catch (Exception e) {
					logger.debug("Skipping malformed Claude Code JSONL entry for roomId={}", roomId, e);
				}
			});
		}
		return events;
	}

	private static List<TranscriptEvent> selectRunSegment(List<TranscriptEvent> events, Map<String, Object> run) {
		if (events.isEmpty()) {
			return List.of();
		}
		String input = normalizeText(stringValue(run.get("input")));
		long startedAt = firstTimestamp(run.get("startedAt"), run.get("dateCreated"));
		int startIndex = -1;
		long bestDistance = Long.MAX_VALUE;
		for (int i = 0; i < events.size(); i++) {
			TranscriptEvent event = events.get(i);
			if (!"user_prompt".equals(event.eventType())) {
				continue;
			}
			Map<String, Object> data = asMap(event.envelope().get("data"));
			if (!input.equals(normalizeText(data == null ? null : stringValue(data.get("text"))))) {
				continue;
			}
			long distance = startedAt > 0 && event.timestampMs() > 0
					? Math.abs(event.timestampMs() - startedAt)
					: i;
			if (distance < bestDistance) {
				bestDistance = distance;
				startIndex = i;
			}
		}

		if (startIndex >= 0) {
			int endIndex = events.size();
			for (int i = startIndex + 1; i < events.size(); i++) {
				if ("user_prompt".equals(events.get(i).eventType())) {
					endIndex = i;
					break;
				}
			}
			return new ArrayList<>(events.subList(startIndex + 1, endIndex));
		}

		long completedAt = firstTimestamp(run.get("completedAt"), run.get("startedAt"));
		long windowStart = startedAt > 0 ? startedAt - HISTORY_WINDOW_SLOP_MS : Long.MIN_VALUE;
		long windowEnd = completedAt > 0 ? completedAt + HISTORY_WINDOW_SLOP_MS : Long.MAX_VALUE;
		List<TranscriptEvent> fallback = new ArrayList<>();
		for (TranscriptEvent event : events) {
			if (event.timestampMs() >= windowStart && event.timestampMs() <= windowEnd
					&& !"user_prompt".equals(event.eventType())) {
				fallback.add(event);
			}
		}
		return fallback;
	}

	private static List<Map<String, Object>> projectSegment(List<TranscriptEvent> events, Map<String, Object> run) {
		String runId = stringValue(run.get("runId"));
		String finalText = normalizeText(stringValue(run.get("finalText")));
		String fallbackTimestamp = firstNonBlank(stringValue(run.get("startedAt")),
				stringValue(run.get("dateCreated")));
		Map<String, String> toolNames = new HashMap<>();
		List<Map<String, Object>> messages = new ArrayList<>();

		for (TranscriptEvent event : events) {
			Map<String, Object> envelope = event.envelope();
			Map<String, Object> data = asMap(envelope.get("data"));
			if (data == null) {
				continue;
			}
			String eventId = firstNonBlank(stringValue(envelope.get("uuid")), runId + ":history");
			if ("assistant".equals(event.eventType())) {
				List<Object> texts = asList(data.get("texts"));
				for (int i = 0; i < texts.size(); i++) {
					Map<String, Object> textBlock = asMap(texts.get(i));
					String text = textBlock == null ? null : stringValue(textBlock.get("text"));
					if (text == null || text.isBlank() || normalizeText(text).equals(finalText)) {
						continue;
					}
					String timestamp = firstNonBlank(stringValue(textBlock.get("timestamp")), fallbackTimestamp);
					messages.add(textMessage(runId, eventId + ":text:" + i, text, timestamp));
				}

				List<Object> invocations = asList(data.get("toolInvocations"));
				for (int i = 0; i < invocations.size(); i++) {
					Map<String, Object> invocation = asMap(invocations.get(i));
					if (invocation == null) {
						continue;
					}
					String toolUseId = firstNonBlank(stringValue(invocation.get("toolUseId")),
							eventId + ":tool:" + i);
					String toolName = firstNonBlank(stringValue(invocation.get("toolName")), "Tool");
					toolNames.put(toolUseId, toolName);
					String timestamp = firstNonBlank(stringValue(invocation.get("timestamp")), fallbackTimestamp);
					messages.add(toolCallMessage(runId, eventId + ":tool-call:" + i, toolUseId, toolName,
							invocation, timestamp));
				}
			} else if ("tool_result".equals(event.eventType())) {
				String toolUseId = stringValue(data.get("toolUseId"));
				if (toolUseId == null || toolUseId.isBlank()) {
					continue;
				}
				String timestamp = firstNonBlank(stringValue(data.get("timestamp")), fallbackTimestamp);
				messages.add(toolResultMessage(runId, eventId + ":tool-result", toolUseId,
						toolNames.get(toolUseId), data, timestamp));
			}
		}
		messages.sort(Comparator.comparing(message -> stringValue(message.get("dateCreated")),
				Comparator.nullsLast(String::compareTo)));
		return messages;
	}

	private static Map<String, Object> textMessage(String runId, String messageId, String text, String timestamp) {
		Map<String, Object> part = new LinkedHashMap<>();
		part.put("type", "TEXT");
		part.put("text", text);
		return message(runId, "assistant", messageId, "OUTPUT", "RESPONSE_TEXT", true, timestamp, part);
	}

	private static Map<String, Object> toolCallMessage(String runId, String messageId, String toolUseId,
			String toolName, Map<String, Object> invocation, String timestamp) {
		Map<String, Object> toolCall = new LinkedHashMap<>();
		toolCall.put("id", toolUseId);
		toolCall.put("name", toolName);
		putIfPresent(toolCall, "description", invocation.get("description"));
		Map<String, Object> arguments = asMap(invocation.get("arguments"));
		toolCall.put("arguments", arguments == null ? new LinkedHashMap<>() : arguments);
		Map<String, Object> part = new LinkedHashMap<>();
		part.put("type", "TOOL_CALL");
		part.put("toolCall", toolCall);
		return message(runId, "assistant_tool", messageId, "OUTPUT", "RESPONSE_TOOL", true, timestamp, part);
	}

	private static Map<String, Object> toolResultMessage(String runId, String messageId, String toolUseId,
			String toolName, Map<String, Object> data, String timestamp) {
		Map<String, Object> parameters = new LinkedHashMap<>();
		putIfPresent(parameters, "durationMs", data.get("durationMs"));
		putIfPresent(parameters, "filePath", data.get("filePath"));
		putIfPresent(parameters, "stats", data.get("stats"));
		Map<String, Object> toolResult = new LinkedHashMap<>();
		toolResult.put("toolCallId", toolUseId);
		putIfPresent(toolResult, "toolName", toolName);
		toolResult.put("output", AgentStreamItems.truncate(stringValue(data.get("content")),
				AgentStreamItems.MAX_TOOL_OUTPUT_CHARS));
		toolResult.put("toolParameterValues", parameters);
		toolResult.put("toolStatus", isFailedStatus(stringValue(data.get("status")))
				? AgentStreamItems.TOOL_FAILED
				: AgentStreamItems.TOOL_COMPLETED);
		Map<String, Object> part = new LinkedHashMap<>();
		part.put("type", "TOOL_RESULT");
		part.put("toolResult", toolResult);
		Map<String, Object> message = message(runId, "tool_result", messageId, "INPUT", "INPUT_TOOL_EXEC", false,
				timestamp, part);
		message.put("parentMessageId", toolUseId);
		return message;
	}

	private static Map<String, Object> message(String runId, String role, String messageId, String io, String type,
			boolean visible, String timestamp, Map<String, Object> part) {
		Map<String, Object> ornaments = new LinkedHashMap<>();
		ornaments.put(SemossAgentHarness.ORNAMENT_AGENT_RUN_ID, runId);
		ornaments.put(SemossAgentHarness.ORNAMENT_AGENT_RUN_ROLE, role);
		Map<String, Object> message = new LinkedHashMap<>();
		message.put("messageId", messageId);
		message.put("io", io);
		message.put("type", type);
		message.put("visible", visible);
		message.put("dateCreated", timestamp);
		message.put("ornaments", ornaments);
		message.put("parts", List.of(part));
		return message;
	}

	private static String agentRunRole(Map<String, Object> message) {
		Map<String, Object> ornaments = asMap(message.get("ornaments"));
		return ornaments == null ? null : stringValue(ornaments.get(SemossAgentHarness.ORNAMENT_AGENT_RUN_ROLE));
	}

	private static long eventTimestamp(Map<String, Object> envelope) {
		Map<String, Object> data = asMap(envelope.get("data"));
		if (data == null) {
			return -1L;
		}
		Long direct = timestampValue(data.get("timestamp"));
		if (direct != null) {
			return direct;
		}
		for (String key : List.of("texts", "toolInvocations", "thinking")) {
			for (Object value : asList(data.get(key))) {
				Map<String, Object> block = asMap(value);
				Long nested = block == null ? null : timestampValue(block.get("timestamp"));
				if (nested != null) {
					return nested;
				}
			}
		}
		return -1L;
	}

	private static long firstTimestamp(Object first, Object second) {
		Long value = timestampValue(first);
		if (value == null) {
			value = timestampValue(second);
		}
		return value == null ? -1L : value;
	}

	private static Long timestampValue(Object value) {
		String text = stringValue(value);
		if (text == null || text.isBlank()) {
			return null;
		}
		try {
			return Instant.parse(text).toEpochMilli();
		} catch (Exception ignored) {
			// try offset and JDBC timestamp forms below
		}
		try {
			return OffsetDateTime.parse(text).toInstant().toEpochMilli();
		} catch (Exception ignored) {
			// try JDBC timestamp form below
		}
		try {
			return LocalDateTime.parse(text.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
		} catch (Exception ignored) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asMap(Object value) {
		return value instanceof Map ? (Map<String, Object>) value : null;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> asList(Object value) {
		return value instanceof List ? (List<Object>) value : Collections.emptyList();
	}

	private static Long longValue(Object value) {
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		try {
			return value == null ? null : Long.parseLong(String.valueOf(value));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static boolean isFailedStatus(String status) {
		String normalized = status == null ? "" : status.toLowerCase();
		return normalized.contains("error") || normalized.contains("fail");
	}

	private static String normalizeText(String value) {
		return value == null ? "" : value.replace("\r\n", "\n").trim().replaceAll("\\s+", " ");
	}

	private static String stringValue(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	private static String firstNonBlank(String first, String second) {
		return first == null || first.isBlank() ? second : first;
	}

	private static void putIfPresent(Map<String, Object> target, String key, Object value) {
		if (value != null && !String.valueOf(value).isBlank()) {
			target.put(key, value);
		}
	}

	private record TranscriptEvent(Map<String, Object> envelope, long timestampMs) {
		private String eventType() {
			return stringValue(envelope.get("event"));
		}
	}
}
