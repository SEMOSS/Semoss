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
package prerna.reactor.agent.metrics;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

/**
 * Computes agent effectiveness metrics from the canonical AgentRun activity
 * contract: the run map returned by {@code AgentRuntimeManager.getRun(runId,
 * insight, true)}, whose {@code messages} list is already normalized across
 * harnesses (semoss parts come from RoomUtils.getMessagesForClient, claude_code
 * parts are projected from the JSONL transcript by ClaudeCodeRunActivityAdapter).
 *
 * Everything here is derived from that one shape so the metrics stay
 * harness-agnostic. Known cross-harness quirks handled below:
 * - toolStatus vocabularies differ (success/error vs COMPLETED/FAILED)
 * - part payload keys have snake_case aliases (tool_call, tool_status, ...)
 * - server-side tools (server_tool=true) never produce a local TOOL_RESULT
 *   and are excluded from failure denominators
 * - per-tool durationMs only exists for claude_code (inside toolParameterValues)
 * - token counts only exist on semoss envelopes; room-level token truth for
 *   both harnesses lives in the inference logs MESSAGE table
 */
public final class AgentEffectivenessCalculator {

	// score weights - kept visible in the payload so consumers can re-derive
	private static final double WEIGHT_COMPLETION = 40.0;
	private static final double WEIGHT_TOOL_RELIABILITY = 35.0;
	private static final double WEIGHT_EFFICIENCY = 15.0;
	private static final double WEIGHT_DISCIPLINE = 10.0;

	private static final int MAX_FAILURE_SAMPLES = 25;
	private static final int OUTPUT_PREVIEW_CHARS = 300;

	// failure fingerprints emitted by HarnessToolExecutor / RunMCPToolReactor /
	// the claude_code transcript - these are unstructured strings, not codes
	private static final String SEMOSS_ERROR_PREFIX = "tool execution error:";
	private static final String UNKNOWN_TOOL_MARKER = "cannot resolve engine/project id";
	private static final String MALFORMED_ARGS_MARKER = "required input(s) missing";
	private static final String SPAWN_REJECTED_MARKER = "spawn_rejected_per_turn_cap";
	private static final String CLAUDE_ERROR_MARKER = "<tool_use_error>";

	private AgentEffectivenessCalculator() {
	}

	/**
	 * Computes the effectiveness metrics for one agent run.
	 *
	 * @param run run map including its {@code messages} list
	 * @return metrics map for the run
	 */
	public static Map<String, Object> computeRunMetrics(Map<String, Object> run) {
		List<Map<String, Object>> messages = asListOfMaps(run.get("messages"));
		String status = stringValue(run.get("status"));
		String errorMessage = stringValue(run.get("errorMessage"));

		// --- walk the transcript once, collecting calls and results in order ---
		List<Map<String, Object>> toolCalls = new ArrayList<>();
		Map<String, Map<String, Object>> resultsByCallId = new LinkedHashMap<>();
		int assistantMessages = 0;
		int textParts = 0;
		int thinkingParts = 0;
		long inputTokens = 0;
		long outputTokens = 0;
		long thinkingTokens = 0;
		long cacheReadTokens = 0;
		long cacheCreationTokens = 0;
		boolean tokensSeen = false;

		for (Map<String, Object> message : messages) {
			if (message == null) {
				continue;
			}
			String io = stringValue(message.get("io"));
			boolean isOutput = "OUTPUT".equalsIgnoreCase(io);
			if (isOutput) {
				assistantMessages++;
			}
			long msgTokens = longValue(message.get("tokens"), 0L);
			if (message.containsKey("tokens") && msgTokens > 0) {
				tokensSeen = true;
				if (isOutput) {
					outputTokens += msgTokens;
				} else {
					inputTokens += msgTokens;
				}
			}
			thinkingTokens += longValue(message.get("thinkingTokens"), 0L);
			cacheReadTokens += longValue(message.get("cacheReadTokens"), 0L);
			cacheCreationTokens += longValue(message.get("cacheCreationTokens"), 0L);

			for (Map<String, Object> part : asListOfMaps(message.get("parts"))) {
				String type = stringValue(part.get("type"));
				if ("TEXT".equalsIgnoreCase(type)) {
					textParts++;
				} else if ("THINKING".equalsIgnoreCase(type)) {
					thinkingParts++;
				} else if ("TOOL_CALL".equalsIgnoreCase(type)) {
					Map<String, Object> toolCall = asMap(firstNonNull(part.get("toolCall"), part.get("tool_call")));
					if (toolCall != null) {
						toolCalls.add(toolCall);
					}
				} else if ("TOOL_RESULT".equalsIgnoreCase(type)) {
					Map<String, Object> toolResult = asMap(
							firstNonNull(part.get("toolResult"), part.get("tool_result")));
					if (toolResult != null) {
						String callId = stringValue(
								firstNonNull(toolResult.get("toolCallId"), toolResult.get("id")));
						if (callId != null) {
							resultsByCallId.put(callId, toolResult);
						}
					}
				}
			}
		}

		// --- classify every dispatched call ---
		int serverToolCalls = 0;
		int succeeded = 0;
		int failed = 0;
		int unanswered = 0;
		int unknownToolCalls = 0;
		int malformedArgumentCalls = 0;
		int spawnRejectedCalls = 0;
		Map<String, Map<String, Object>> byTool = new TreeMap<>();
		Map<String, Integer> duplicateCounter = new LinkedHashMap<>();
		List<Map<String, Object>> failureSamples = new ArrayList<>();
		SkillTally skills = new SkillTally();

		for (Map<String, Object> toolCall : toolCalls) {
			String name = stringValue(toolCall.get("name"));
			if (name == null) {
				name = "unknown";
			}
			String callId = stringValue(toolCall.get("id"));
			Map<String, Object> arguments = asMap(toolCall.get("arguments"));
			boolean serverTool = booleanValue(
					firstNonNull(toolCall.get("server_tool"), toolCall.get("serverTool")));

			Map<String, Object> toolStats = byTool.computeIfAbsent(name, k -> newToolStats());
			increment(toolStats, "calls");

			if (serverTool) {
				serverToolCalls++;
				increment(toolStats, "serverToolCalls");
				continue;
			}

			// duplicate detection: same tool + identical canonical arguments.
			// Paged tools (e.g. LoadSkill offset/max_bytes) naturally differ per
			// page, so legitimate paging never collides here.
			String duplicateKey = name + "::" + canonicalize(arguments);
			duplicateCounter.merge(duplicateKey, 1, Integer::sum);

			Map<String, Object> result = callId == null ? null : resultsByCallId.get(callId);
			if (result == null) {
				unanswered++;
				increment(toolStats, "unanswered");
				skills.observe(name, arguments, null, false);
				continue;
			}

			String output = stringValue(result.get("output"));
			boolean isFailure = isFailedResult(
					stringValue(firstNonNull(result.get("toolStatus"), result.get("tool_status"))), output);
			Long durationMs = extractDurationMs(result);
			if (durationMs != null) {
				addLong(toolStats, "durationMsTotal", durationMs);
				increment(toolStats, "durationSamples");
				maxLong(toolStats, "maxDurationMs", durationMs);
			}

			if (isFailure) {
				failed++;
				increment(toolStats, "failed");
				String category = classifyFailure(output);
				if ("unknown_tool".equals(category)) {
					unknownToolCalls++;
				} else if ("malformed_arguments".equals(category)) {
					malformedArgumentCalls++;
				} else if ("spawn_rejected".equals(category)) {
					spawnRejectedCalls++;
				}
				if (failureSamples.size() < MAX_FAILURE_SAMPLES) {
					Map<String, Object> sample = new LinkedHashMap<>();
					sample.put("toolName", name);
					sample.put("toolCallId", callId);
					sample.put("category", category);
					sample.put("outputPreview", preview(output));
					failureSamples.add(sample);
				}
			} else {
				succeeded++;
			}
			skills.observe(name, arguments, result, !isFailure);
		}

		int totalCalls = toolCalls.size();
		int evaluatedCalls = totalCalls - serverToolCalls;
		int answeredCalls = succeeded + failed;
		int duplicateWaste = 0;
		List<Map<String, Object>> duplicateGroups = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : duplicateCounter.entrySet()) {
			int count = entry.getValue();
			if (count > 1) {
				duplicateWaste += count - 1;
				Map<String, Object> group = new LinkedHashMap<>();
				group.put("toolName", entry.getKey().substring(0, entry.getKey().indexOf("::")));
				group.put("identicalCalls", count);
				duplicateGroups.add(group);
			}
		}

		finalizeToolStats(byTool);

		boolean maxTurnsReached = errorMessage != null
				&& errorMessage.toLowerCase(Locale.ROOT).replace("_", " ").contains("max turns");

		// --- assemble the payload ---
		Map<String, Object> metrics = new LinkedHashMap<>();
		metrics.put("runId", run.get("runId"));
		metrics.put("parentRunId", run.get("parentRunId"));
		metrics.put("harnessType", run.get("harnessType"));
		metrics.put("modelId", run.get("modelId"));
		metrics.put("status", status);
		metrics.put("startedAt", run.get("startedAt"));
		metrics.put("completedAt", run.get("completedAt"));
		metrics.put("wallClockMs", elapsedMs(run.get("startedAt"), run.get("completedAt")));

		Map<String, Object> outcome = new LinkedHashMap<>();
		outcome.put("terminal", isTerminal(status));
		outcome.put("completed", "COMPLETED".equalsIgnoreCase(status));
		outcome.put("errorMessage", errorMessage);
		outcome.put("maxTurnsReached", maxTurnsReached);
		metrics.put("outcome", outcome);

		Map<String, Object> turns = new LinkedHashMap<>();
		turns.put("assistantMessages", assistantMessages);
		turns.put("toolRoundTrips", totalCalls);
		turns.put("textParts", textParts);
		turns.put("thinkingParts", thinkingParts);
		metrics.put("turns", turns);

		Map<String, Object> tools = new LinkedHashMap<>();
		tools.put("totalCalls", totalCalls);
		tools.put("serverToolCalls", serverToolCalls);
		tools.put("evaluatedCalls", evaluatedCalls);
		tools.put("succeeded", succeeded);
		tools.put("failed", failed);
		tools.put("unanswered", unanswered);
		tools.put("successRate", answeredCalls == 0 ? null : round(succeeded / (double) answeredCalls));
		tools.put("byTool", byTool);
		tools.put("failures", failureSamples);
		metrics.put("tools", tools);

		Map<String, Object> improperUse = new LinkedHashMap<>();
		improperUse.put("unknownToolCalls", unknownToolCalls);
		improperUse.put("malformedArgumentCalls", malformedArgumentCalls);
		improperUse.put("spawnRejectedCalls", spawnRejectedCalls);
		improperUse.put("unansweredToolCalls", unanswered);
		improperUse.put("duplicateCallWaste", duplicateWaste);
		improperUse.put("duplicateCallGroups", duplicateGroups);
		metrics.put("improperUse", improperUse);

		metrics.put("skills", skills.toMap());

		Map<String, Object> tokens = new LinkedHashMap<>();
		tokens.put("available", tokensSeen);
		tokens.put("inputTokens", inputTokens);
		tokens.put("outputTokens", outputTokens);
		tokens.put("thinkingTokens", thinkingTokens);
		tokens.put("cacheReadTokens", cacheReadTokens);
		tokens.put("cacheCreationTokens", cacheCreationTokens);
		metrics.put("tokens", tokens);

		metrics.put("score", score(status, succeeded, failed, evaluatedCalls, duplicateWaste,
				unknownToolCalls + malformedArgumentCalls + spawnRejectedCalls, unanswered));
		return metrics;
	}

	/**
	 * Rolls a list of per-run metric maps up into one room-level summary.
	 *
	 * @param runMetrics per-run maps from {@link #computeRunMetrics(Map)}
	 * @return aggregate map
	 */
	public static Map<String, Object> aggregate(List<Map<String, Object>> runMetrics) {
		int completed = 0;
		int failedRuns = 0;
		int cancelled = 0;
		int inFlight = 0;
		long toolCalls = 0;
		long toolFailures = 0;
		long toolSucceeded = 0;
		long unanswered = 0;
		long duplicateWaste = 0;
		long unknownToolCalls = 0;
		long malformedArgumentCalls = 0;
		long skillLoadCalls = 0;
		long skillLoadFailures = 0;
		int maxTurnsRuns = 0;
		Set<String> distinctSkills = new LinkedHashSet<>();
		Map<String, Map<String, Object>> byTool = new TreeMap<>();
		List<Double> scores = new ArrayList<>();

		for (Map<String, Object> run : runMetrics) {
			String status = stringValue(run.get("status"));
			if ("COMPLETED".equalsIgnoreCase(status)) {
				completed++;
			} else if ("FAILED".equalsIgnoreCase(status)) {
				failedRuns++;
			} else if ("CANCELLED".equalsIgnoreCase(status)) {
				cancelled++;
			} else {
				inFlight++;
			}
			Map<String, Object> outcome = asMap(run.get("outcome"));
			if (outcome != null && Boolean.TRUE.equals(outcome.get("maxTurnsReached"))) {
				maxTurnsRuns++;
			}
			Map<String, Object> tools = asMap(run.get("tools"));
			if (tools != null) {
				toolCalls += longValue(tools.get("totalCalls"), 0L);
				toolFailures += longValue(tools.get("failed"), 0L);
				toolSucceeded += longValue(tools.get("succeeded"), 0L);
				unanswered += longValue(tools.get("unanswered"), 0L);
				Map<String, Object> runByTool = asMap(tools.get("byTool"));
				if (runByTool != null) {
					for (Map.Entry<String, Object> entry : runByTool.entrySet()) {
						Map<String, Object> src = asMap(entry.getValue());
						if (src == null) {
							continue;
						}
						Map<String, Object> dest = byTool.computeIfAbsent(entry.getKey(), k -> newToolStats());
						addLong(dest, "calls", longValue(src.get("calls"), 0L));
						addLong(dest, "failed", longValue(src.get("failed"), 0L));
						addLong(dest, "unanswered", longValue(src.get("unanswered"), 0L));
						addLong(dest, "serverToolCalls", longValue(src.get("serverToolCalls"), 0L));
					}
				}
			}
			Map<String, Object> improper = asMap(run.get("improperUse"));
			if (improper != null) {
				duplicateWaste += longValue(improper.get("duplicateCallWaste"), 0L);
				unknownToolCalls += longValue(improper.get("unknownToolCalls"), 0L);
				malformedArgumentCalls += longValue(improper.get("malformedArgumentCalls"), 0L);
			}
			Map<String, Object> skills = asMap(run.get("skills"));
			if (skills != null) {
				skillLoadCalls += longValue(skills.get("skillLoadCalls"), 0L);
				skillLoadFailures += longValue(skills.get("skillLoadFailures"), 0L);
				for (Object name : asList(skills.get("distinctSkillsLoaded"))) {
					if (name != null) {
						distinctSkills.add(String.valueOf(name));
					}
				}
			}
			Map<String, Object> score = asMap(run.get("score"));
			Object value = score == null ? null : score.get("value");
			if (value instanceof Number) {
				scores.add(((Number) value).doubleValue());
			}
		}

		for (Map<String, Object> stats : byTool.values()) {
			stats.remove("durationMsTotal");
			stats.remove("durationSamples");
			stats.remove("maxDurationMs");
		}

		int runCount = runMetrics.size();
		int terminalRuns = completed + failedRuns + cancelled;
		long answered = toolSucceeded + toolFailures;

		Map<String, Object> rollup = new LinkedHashMap<>();
		rollup.put("runCount", runCount);
		rollup.put("runsCompleted", completed);
		rollup.put("runsFailed", failedRuns);
		rollup.put("runsCancelled", cancelled);
		rollup.put("runsInFlight", inFlight);
		rollup.put("runsHitMaxTurns", maxTurnsRuns);
		rollup.put("runCompletionRate",
				terminalRuns == 0 ? null : round(completed / (double) terminalRuns));
		rollup.put("toolCalls", toolCalls);
		rollup.put("toolFailures", toolFailures);
		rollup.put("toolSuccessRate", answered == 0 ? null : round(toolSucceeded / (double) answered));
		rollup.put("unansweredToolCalls", unanswered);
		rollup.put("unknownToolCalls", unknownToolCalls);
		rollup.put("malformedArgumentCalls", malformedArgumentCalls);
		rollup.put("duplicateCallWaste", duplicateWaste);
		rollup.put("skillLoadCalls", skillLoadCalls);
		rollup.put("skillLoadFailures", skillLoadFailures);
		rollup.put("distinctSkillsLoaded", new ArrayList<>(distinctSkills));
		rollup.put("byTool", byTool);
		rollup.put("averageScore", scores.isEmpty() ? null
				: round(scores.stream().mapToDouble(Double::doubleValue).average().orElse(0)));
		rollup.put("minScore", scores.isEmpty() ? null
				: round(scores.stream().mapToDouble(Double::doubleValue).min().orElse(0)));
		rollup.put("scoredRuns", scores.size());
		return rollup;
	}

	/**
	 * Room-level token and latency truth from the inference logs MESSAGE table.
	 * This covers both harnesses (claude_code inference is proxied back through
	 * AnthropicEndpoints and recorded by ROOM_ID) even though claude_code message
	 * envelopes carry no token counts. The SQL lives in ModelInferenceLogsUtils
	 * because SystemEngineRegistry allowlists which packages may touch the
	 * inference logs engine. Callers must have already validated room ownership.
	 *
	 * @param roomId room whose MESSAGE rows should be aggregated
	 * @return inference stats map; {@code available=false} when nothing was found
	 */
	public static Map<String, Object> queryRoomInferenceStats(String roomId) {
		return ModelInferenceLogsUtils.getRoomTokenAndLatencyStats(roomId);
	}

	// ------------------------------------------------------------------
	// scoring
	// ------------------------------------------------------------------

	private static Map<String, Object> score(String status, int succeeded, int failed, int evaluatedCalls,
			int duplicateWaste, int improperCalls, int unanswered) {
		Map<String, Object> score = new LinkedHashMap<>();
		Map<String, Object> weights = new LinkedHashMap<>();
		weights.put("completion", WEIGHT_COMPLETION);
		weights.put("toolReliability", WEIGHT_TOOL_RELIABILITY);
		weights.put("efficiency", WEIGHT_EFFICIENCY);
		weights.put("discipline", WEIGHT_DISCIPLINE);
		score.put("weights", weights);

		if ("CANCELLED".equalsIgnoreCase(status)) {
			score.put("value", null);
			score.put("excludedReason", "run was cancelled by the user");
			return score;
		}
		if (!isTerminal(status)) {
			score.put("value", null);
			score.put("excludedReason", "run has not reached a terminal status");
			return score;
		}

		int answered = succeeded + failed;
		double completion = "COMPLETED".equalsIgnoreCase(status) ? WEIGHT_COMPLETION : 0.0;
		// no answered tool calls means no evidence of tool failure - full credit
		double toolReliability = answered == 0 ? WEIGHT_TOOL_RELIABILITY
				: WEIGHT_TOOL_RELIABILITY * (succeeded / (double) answered);
		int denominator = Math.max(1, evaluatedCalls);
		double efficiency = WEIGHT_EFFICIENCY * (1.0 - Math.min(1.0, duplicateWaste / (double) denominator));
		double discipline = WEIGHT_DISCIPLINE
				* (1.0 - Math.min(1.0, (improperCalls + unanswered) / (double) denominator));

		Map<String, Object> components = new LinkedHashMap<>();
		components.put("completion", round(completion));
		components.put("toolReliability", round(toolReliability));
		components.put("efficiency", round(efficiency));
		components.put("discipline", round(discipline));
		score.put("components", components);
		score.put("value", round(completion + toolReliability + efficiency + discipline));
		return score;
	}

	// ------------------------------------------------------------------
	// tool result classification
	// ------------------------------------------------------------------

	private static boolean isFailedResult(String toolStatus, String output) {
		String normalized = toolStatus == null ? "" : toolStatus.toLowerCase(Locale.ROOT);
		if (normalized.contains("error") || normalized.contains("fail")) {
			return true;
		}
		// the claude_code transcript parser can mask block-level is_error with a
		// toolUseResult status, so also match the raw error fingerprints
		if (output != null) {
			String lower = output.toLowerCase(Locale.ROOT);
			return lower.startsWith(SEMOSS_ERROR_PREFIX) || lower.contains(CLAUDE_ERROR_MARKER);
		}
		return false;
	}

	private static String classifyFailure(String output) {
		String lower = output == null ? "" : output.toLowerCase(Locale.ROOT);
		if (lower.contains(UNKNOWN_TOOL_MARKER)) {
			return "unknown_tool";
		}
		if (lower.contains(MALFORMED_ARGS_MARKER)) {
			return "malformed_arguments";
		}
		if (lower.contains(SPAWN_REJECTED_MARKER)) {
			return "spawn_rejected";
		}
		return "tool_error";
	}

	private static Long extractDurationMs(Map<String, Object> toolResult) {
		Map<String, Object> parameters = asMap(firstNonNull(toolResult.get("toolParameterValues"),
				toolResult.get("tool_parameter_values")));
		if (parameters == null) {
			return null;
		}
		Object duration = parameters.get("durationMs");
		return duration instanceof Number ? ((Number) duration).longValue() : null;
	}

	// ------------------------------------------------------------------
	// skill tracking
	// ------------------------------------------------------------------

	/**
	 * Tracks skill activity across both harness vocabularies: the claude_code
	 * CLI exposes one "Skill" tool (arguments.skill), while the semoss harness
	 * exposes "ListSkill" and a paged "LoadSkill" (arguments.skill_name). Loads
	 * are deduplicated by skill name so LoadSkill paging is not overcounted.
	 */
	private static final class SkillTally {
		private int listSkillCalls = 0;
		private int skillLoadCalls = 0;
		private int skillLoadFailures = 0;
		private final Set<String> loaded = new LinkedHashSet<>();
		private final Set<String> attempted = new LinkedHashSet<>();

		private void observe(String toolName, Map<String, Object> arguments, Map<String, Object> result,
				boolean success) {
			if ("ListSkill".equals(toolName)) {
				listSkillCalls++;
				return;
			}
			String skillName = null;
			if ("Skill".equals(toolName)) {
				skillName = arguments == null ? null : stringValue(arguments.get("skill"));
			} else if ("LoadSkill".equals(toolName)) {
				skillName = arguments == null ? null : stringValue(arguments.get("skill_name"));
			} else {
				return;
			}
			skillLoadCalls++;
			if (skillName == null) {
				skillName = "(unspecified)";
			}
			attempted.add(skillName);
			if (result != null && success) {
				loaded.add(skillName);
			} else if (result != null) {
				skillLoadFailures++;
			}
		}

		private Map<String, Object> toMap() {
			Set<String> failedOnly = new LinkedHashSet<>(attempted);
			failedOnly.removeAll(loaded);
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("listSkillCalls", listSkillCalls);
			map.put("skillLoadCalls", skillLoadCalls);
			map.put("skillLoadFailures", skillLoadFailures);
			map.put("distinctSkillsLoaded", new ArrayList<>(loaded));
			map.put("skillsNeverLoadedSuccessfully", new ArrayList<>(failedOnly));
			return map;
		}
	}

	// ------------------------------------------------------------------
	// small helpers
	// ------------------------------------------------------------------

	private static Map<String, Object> newToolStats() {
		Map<String, Object> stats = new LinkedHashMap<>();
		stats.put("calls", 0L);
		stats.put("failed", 0L);
		stats.put("unanswered", 0L);
		stats.put("serverToolCalls", 0L);
		return stats;
	}

	private static void finalizeToolStats(Map<String, Map<String, Object>> byTool) {
		for (Map<String, Object> stats : byTool.values()) {
			long samples = longValue(stats.remove("durationSamples"), 0L);
			long total = longValue(stats.remove("durationMsTotal"), 0L);
			if (samples > 0) {
				stats.put("avgDurationMs", round(total / (double) samples));
			}
		}
	}

	/**
	 * Order-insensitive canonical form of a tool argument payload, used to
	 * detect byte-identical retries regardless of map key ordering.
	 */
	private static String canonicalize(Object value) {
		if (value == null) {
			return "null";
		}
		if (value instanceof Map) {
			TreeMap<String, Object> sorted = new TreeMap<>();
			((Map<?, ?>) value).forEach((k, v) -> sorted.put(String.valueOf(k), v));
			StringBuilder sb = new StringBuilder("{");
			for (Map.Entry<String, Object> entry : sorted.entrySet()) {
				if (sb.length() > 1) {
					sb.append(",");
				}
				sb.append(entry.getKey()).append(":").append(canonicalize(entry.getValue()));
			}
			return sb.append("}").toString();
		}
		if (value instanceof List) {
			StringBuilder sb = new StringBuilder("[");
			for (Object item : (List<?>) value) {
				if (sb.length() > 1) {
					sb.append(",");
				}
				sb.append(canonicalize(item));
			}
			return sb.append("]").toString();
		}
		return String.valueOf(value);
	}

	private static boolean isTerminal(String status) {
		return "COMPLETED".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status)
				|| "CANCELLED".equalsIgnoreCase(status);
	}

	private static Long elapsedMs(Object start, Object end) {
		Long startMs = timestampMs(start);
		Long endMs = timestampMs(end);
		if (startMs == null || endMs == null || endMs < startMs) {
			return null;
		}
		return endMs - startMs;
	}

	private static Long timestampMs(Object value) {
		String text = stringValue(value);
		if (text == null) {
			return null;
		}
		try {
			return Instant.parse(text).toEpochMilli();
		} catch (Exception ignored) {
			// fall through to offset and JDBC timestamp forms
		}
		try {
			return OffsetDateTime.parse(text).toInstant().toEpochMilli();
		} catch (Exception ignored) {
			// fall through to JDBC timestamp form
		}
		try {
			return LocalDateTime.parse(text.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
		} catch (Exception ignored) {
			return null;
		}
	}

	private static String preview(String output) {
		if (output == null) {
			return null;
		}
		return output.length() <= OUTPUT_PREVIEW_CHARS ? output
				: output.substring(0, OUTPUT_PREVIEW_CHARS) + "...";
	}

	private static double round(double value) {
		return Math.round(value * 100.0) / 100.0;
	}

	private static void increment(Map<String, Object> map, String key) {
		addLong(map, key, 1L);
	}

	private static void addLong(Map<String, Object> map, String key, long amount) {
		map.merge(key, amount, (a, b) -> longValue(a, 0L) + longValue(b, 0L));
	}

	private static void maxLong(Map<String, Object> map, String key, long candidate) {
		map.merge(key, candidate, (a, b) -> Math.max(longValue(a, 0L), longValue(b, 0L)));
	}

	private static long longValue(Object value, long defaultValue) {
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		if (value != null) {
			try {
				return (long) Double.parseDouble(String.valueOf(value));
			} catch (NumberFormatException ignored) {
				return defaultValue;
			}
		}
		return defaultValue;
	}

	private static boolean booleanValue(Object value) {
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		return value != null && Boolean.parseBoolean(String.valueOf(value));
	}

	private static Object firstNonNull(Object first, Object second) {
		return first != null ? first : second;
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() ? null : text;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asMap(Object value) {
		return value instanceof Map ? (Map<String, Object>) value : null;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> asList(Object value) {
		return value instanceof List ? (List<Object>) value : Collections.emptyList();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> asListOfMaps(Object value) {
		if (!(value instanceof List)) {
			return Collections.emptyList();
		}
		List<Map<String, Object>> result = new ArrayList<>();
		for (Object item : (List<Object>) value) {
			if (item instanceof Map) {
				result.add((Map<String, Object>) item);
			}
		}
		return result;
	}
}
