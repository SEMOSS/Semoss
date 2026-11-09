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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.run.AgentRunService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * LLM-as-judge assessment of one agent run. Combines the deterministic metrics
 * from {@link AgentEffectivenessCalculator} with a rendered transcript of the
 * run's message history and asks a judge model for a structured qualitative
 * evaluation (goal achievement, tool use quality, efficiency, skill
 * utilization, communication). The judge sees the precomputed metrics so it can
 * focus on judgment calls the deterministic pass cannot make, and is asked to
 * flag any disagreement with those numbers.
 *
 * The judge ask goes straight through IModelEngine.ask with no Room attached,
 * so it does not append to the room history or pollute the room's inference
 * stats with judge tokens.
 */
public class AssessAgentEffectivenessReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AssessAgentEffectivenessReactor.class);

	private static final String RUN_ID_KEY = "runId";
	private static final String JUDGE_MODEL_KEY = "judgeModelId";
	private static final String MAX_TRANSCRIPT_CHARS_KEY = "maxTranscriptChars";
	private static final String FOCUS_KEY = "focus";

	private static final int DEFAULT_MAX_TRANSCRIPT_CHARS = 60_000;
	private static final int MIN_TRANSCRIPT_CHARS = 2_000;

	// per-item truncation caps for the rendered transcript
	private static final int TEXT_CAP = 2_000;
	private static final int THINKING_CAP = 600;
	private static final int ARGS_CAP = 600;
	private static final int OUTPUT_CAP = 1_200;
	private static final int CONTEXT_FIELD_CAP = 4_000;

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private static final String JUDGE_INSTRUCTIONS = """
			You are an expert evaluator of AI agent runs. Assess how effectively the agent below \
			completed its task. You are given (1) deterministic metrics precomputed from the \
			transcript, (2) run context, and (3) the transcript itself (possibly truncated). \
			Trust the transcript over the metrics if they conflict, and report any conflict.

			Score each dimension 0-10 (10 = flawless):
			- goalAchievement: did the final output actually satisfy the user's request?
			- toolUseQuality: right tools, well-formed arguments, sensible reaction to tool errors?
			- efficiency: minimal redundant calls, no wasted turns, direct path to the goal?
			- skillUtilization: were available skills loaded when relevant and actually applied?
			- communicationQuality: is the final response clear, faithful to the work done, complete?

			""";

	private static final String JUDGE_OUTPUT_CONTRACT = """
			Return ONLY a JSON object, no markdown fences, exactly this shape:
			{
			  "goalAchievement": {"score": 0, "rationale": "..."},
			  "toolUseQuality": {"score": 0, "rationale": "..."},
			  "efficiency": {"score": 0, "rationale": "..."},
			  "skillUtilization": {"score": 0, "rationale": "..."},
			  "communicationQuality": {"score": 0, "rationale": "..."},
			  "overallScore": 0,
			  "verdict": "one short paragraph",
			  "topIssues": ["..."],
			  "recommendations": ["..."],
			  "metricsDisagreements": ["..."]
			}
			overallScore is 0-100 and should reflect your dimension scores, weighted toward \
			goalAchievement. skillUtilization: if no skills were available or relevant, score it \
			10 and say so in the rationale.

			""";

	public AssessAgentEffectivenessReactor() {
		this.keysToGet = new String[] { RUN_ID_KEY, JUDGE_MODEL_KEY, MAX_TRANSCRIPT_CHARS_KEY, FOCUS_KEY };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String runId = StringUtils.trimToNull(this.keyValue.get(RUN_ID_KEY));
		if (runId == null) {
			throw new IllegalArgumentException("runId is required");
		}
		String focus = StringUtils.trimToNull(this.keyValue.get(FOCUS_KEY));
		int maxTranscriptChars = parsePositiveInt(this.keyValue.get(MAX_TRANSCRIPT_CHARS_KEY),
				DEFAULT_MAX_TRANSCRIPT_CHARS);
		if (maxTranscriptChars < MIN_TRANSCRIPT_CHARS) {
			maxTranscriptChars = MIN_TRANSCRIPT_CHARS;
		}

		// ownership is enforced by the run store - only the current user's runs resolve
		Map<String, Object> run = AgentRunService.get().getRun(runId, this.insight, true);
		Map<String, Object> metrics = AgentEffectivenessCalculator.computeRunMetrics(run);

		String judgeModelId = StringUtils.trimToNull(this.keyValue.get(JUDGE_MODEL_KEY));
		if (judgeModelId == null) {
			judgeModelId = StringUtils.trimToNull(stringValue(run.get("modelId")));
		}
		if (judgeModelId == null) {
			throw new IllegalArgumentException("judgeModelId is required because this run has no modelId of its own");
		}
		IModelEngine judge = Utility.getModel(judgeModelId);
		if (judge == null) {
			throw new IllegalArgumentException("Unable to load judge model engine '" + judgeModelId + "'");
		}

		String prompt = buildJudgePrompt(run, metrics, maxTranscriptChars, focus);

		Map<String, Object> llmParams = new HashMap<>();
		llmParams.put("temperature", 0.2);
		llmParams.put("max_completion_tokens", 4000);
		// The room-less ask is deprecated in favor of askRoom, but attaching this
		// call to the judged room would record judge tokens under its ROOM_ID and
		// contaminate the room's own inference stats. The judge must stay outside
		// the room, so the one-shot path is intentional here.
		@SuppressWarnings("deprecation")
		Map<String, Object> response = judge.ask(prompt, null, this.insight, llmParams).toMap();
		String rawResponse = stringValue(response.get("response"));

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("runId", runId);
		result.put("roomId", run.get("roomId"));
		result.put("harnessType", run.get("harnessType"));
		result.put("judgeModelId", judgeModelId);

		Map<String, Object> assessment = parseAssessment(rawResponse);
		if (assessment != null) {
			result.put("assessment", assessment);
		} else {
			classLogger.warn("Judge response for runId '{}' was not parseable JSON; returning raw text.", runId);
			result.put("assessment", null);
			result.put("assessmentRaw", rawResponse);
			result.put("parseError", "Judge did not return valid JSON; see assessmentRaw");
		}

		Map<String, Object> judgeUsage = new LinkedHashMap<>();
		judgeUsage.put("promptTokens", response.get("numberOfTokensInPrompt"));
		judgeUsage.put("responseTokens", response.get("numberOfTokensInResponse"));
		result.put("judgeUsage", judgeUsage);

		result.put("metrics", metrics);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	// ------------------------------------------------------------------
	// prompt construction (public static for offline testability)
	// ------------------------------------------------------------------

	/**
	 * Builds the full judge prompt: rubric and output contract, the deterministic
	 * metrics, run context, and the budgeted transcript.
	 *
	 * @param run                run map including its messages
	 * @param metrics            precomputed metrics for the same run
	 * @param maxTranscriptChars character budget for the transcript section
	 * @param focus              optional extra evaluation emphasis from the caller
	 * @return complete prompt text
	 */
	public static String buildJudgePrompt(Map<String, Object> run, Map<String, Object> metrics, int maxTranscriptChars,
			String focus) {
		String emphasis = focus == null ? "" : "Caller-requested emphasis: " + focus + "\n\n";
		String errorMessage = stringValue(run.get("errorMessage"));
		String errorLine = errorMessage == null ? ""
				: "errorMessage: " + truncate(errorMessage, CONTEXT_FIELD_CAP) + "\n";
		return JUDGE_INSTRUCTIONS + emphasis + JUDGE_OUTPUT_CONTRACT
				+ """
						=== DETERMINISTIC METRICS ===
						%s

						=== RUN CONTEXT ===
						status: %s
						harness: %s
						%suserRequest: %s
						finalOutput: %s

						=== TRANSCRIPT ===
						%s""".formatted(GSON.toJson(metrics), stringValue(run.get("status")),
						stringValue(run.get("harnessType")), errorLine,
						truncate(stringValue(run.get("input")), CONTEXT_FIELD_CAP),
						truncate(stringValue(run.get("finalText")), CONTEXT_FIELD_CAP),
						renderTranscript(asListOfMaps(run.get("messages")), maxTranscriptChars));
	}

	/**
	 * Renders the message history as readable plain text within a character budget.
	 * Messages are never cut mid-block: when the full rendering exceeds the budget,
	 * the head and tail of the run are kept (the head shows intent and setup, the
	 * tail shows resolution) with an omission marker between.
	 *
	 * @param messages           normalized message maps
	 * @param maxTranscriptChars total character budget
	 * @return rendered transcript
	 */
	public static String renderTranscript(List<Map<String, Object>> messages, int maxTranscriptChars) {
		List<String> blocks = new ArrayList<>(messages.size());
		for (int i = 0; i < messages.size(); i++) {
			blocks.add(renderMessage(i, messages.get(i)));
		}
		int total = 0;
		for (String block : blocks) {
			total += block.length();
		}
		if (total <= maxTranscriptChars) {
			return String.join("", blocks);
		}

		int headBudget = (int) (maxTranscriptChars * 0.6);
		int tailBudget = maxTranscriptChars - headBudget;

		int headEnd = 0;
		int used = 0;
		while (headEnd < blocks.size() && used + blocks.get(headEnd).length() <= headBudget) {
			used += blocks.get(headEnd).length();
			headEnd++;
		}
		int tailStart = blocks.size();
		used = 0;
		while (tailStart > headEnd && used + blocks.get(tailStart - 1).length() <= tailBudget) {
			used += blocks.get(tailStart - 1).length();
			tailStart--;
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < headEnd; i++) {
			sb.append(blocks.get(i));
		}
		sb.append("\n[... ").append(tailStart - headEnd).append(" of ").append(blocks.size())
				.append(" messages omitted for length ...]\n\n");
		for (int i = tailStart; i < blocks.size(); i++) {
			sb.append(blocks.get(i));
		}
		return sb.toString();
	}

	private static String renderMessage(int index, Map<String, Object> message) {
		StringBuilder sb = new StringBuilder();
		Map<String, Object> agentRun = asMap(message.get("agentRun"));
		String role = agentRun == null ? null : stringValue(agentRun.get("role"));
		if (role == null) {
			Map<String, Object> ornaments = asMap(message.get("ornaments"));
			role = ornaments == null ? null : stringValue(ornaments.get("agentRunRole"));
		}
		if (role == null) {
			role = "OUTPUT".equalsIgnoreCase(stringValue(message.get("io"))) ? "assistant" : "input";
		}
		sb.append("[").append(index).append("] ").append(role).append("\n");

		for (Map<String, Object> part : asListOfMaps(message.get("parts"))) {
			String type = stringValue(part.get("type"));
			if ("TEXT".equalsIgnoreCase(type)) {
				sb.append("  text: ").append(truncate(stringValue(part.get("text")), TEXT_CAP)).append("\n");
			} else if ("THINKING".equalsIgnoreCase(type)) {
				String thinking = stringValue(firstNonNull(part.get("thinking"), part.get("text")));
				if (thinking != null) {
					sb.append("  thinking: ").append(truncate(thinking, THINKING_CAP)).append("\n");
				}
			} else if ("TOOL_CALL".equalsIgnoreCase(type)) {
				Map<String, Object> toolCall = asMap(firstNonNull(part.get("toolCall"), part.get("tool_call")));
				if (toolCall != null) {
					sb.append("  tool_call ").append(stringValue(toolCall.get("name")));
					sb.append(" id=").append(stringValue(toolCall.get("id")));
					Object args = toolCall.get("arguments");
					sb.append(" args=").append(truncate(args == null ? "{}" : GSON.toJson(args), ARGS_CAP));
					sb.append("\n");
				}
			} else if ("TOOL_RESULT".equalsIgnoreCase(type)) {
				Map<String, Object> toolResult = asMap(firstNonNull(part.get("toolResult"), part.get("tool_result")));
				if (toolResult != null) {
					sb.append("  tool_result ")
							.append(stringValue(firstNonNull(toolResult.get("toolName"), toolResult.get("tool_name"))));
					sb.append(" id=")
							.append(stringValue(firstNonNull(toolResult.get("toolCallId"), toolResult.get("id"))));
					sb.append(" status=").append(
							stringValue(firstNonNull(toolResult.get("toolStatus"), toolResult.get("tool_status"))));
					sb.append("\n    output: ").append(truncate(stringValue(toolResult.get("output")), OUTPUT_CAP))
							.append("\n");
				}
			}
		}
		sb.append("\n");
		return sb.toString();
	}

	// ------------------------------------------------------------------
	// response parsing (public static for offline testability)
	// ------------------------------------------------------------------

	/**
	 * Leniently extracts the judge's JSON object from its raw response: strips
	 * markdown fences and any prose around the outermost braces.
	 *
	 * @param rawResponse raw judge model output
	 * @return parsed assessment map, or {@code null} when no JSON object parses
	 */
	public static Map<String, Object> parseAssessment(String rawResponse) {
		if (rawResponse == null) {
			return null;
		}
		String cleaned = rawResponse.trim();
		int start = cleaned.indexOf('{');
		int end = cleaned.lastIndexOf('}');
		if (start < 0 || end <= start) {
			return null;
		}
		try {
			Map<String, Object> parsed = GSON.fromJson(cleaned.substring(start, end + 1),
					new TypeToken<Map<String, Object>>() {
					}.getType());
			return parsed == null || parsed.isEmpty() ? null : parsed;
		} catch (Exception e) {
			return null;
		}
	}

	// ------------------------------------------------------------------
	// small helpers
	// ------------------------------------------------------------------

	private static int parsePositiveInt(String value, int defaultValue) {
		if (value == null || value.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			return parsed > 0 ? parsed : defaultValue;
		} catch (NumberFormatException e) {
			return defaultValue;
		}
	}

	private static String truncate(String value, int max) {
		if (value == null) {
			return "(none)";
		}
		return value.length() <= max ? value : value.substring(0, max) + "...[truncated]";
	}

	private static Object firstNonNull(Object first, Object second) {
		return first != null ? first : second;
	}

	private static String stringValue(Object value) {
		if (value == null) {
			return null;
		}
		String text = String.valueOf(value).trim();
		return text.isEmpty() || "null".equals(text) ? null : text;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> asMap(Object value) {
		return value instanceof Map ? (Map<String, Object>) value : null;
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

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Runs an LLM-as-judge assessment of one agent run: combines the deterministic effectiveness "
						+ "metrics with the run's message history and asks a judge model to score goal achievement, "
						+ "tool use quality, efficiency, skill utilization, and communication quality with rationales.")
				.key(RUN_ID_KEY, "The agent run to assess.")
				.key(JUDGE_MODEL_KEY, "Model engine id used as the judge. Defaults to the model that executed the run.")
				.key(MAX_TRANSCRIPT_CHARS_KEY,
						"Character budget for the transcript sent to the judge. Defaults to 60000; head and "
								+ "tail of the run are kept when truncation is needed.")
				.key(FOCUS_KEY, "Optional extra evaluation emphasis, e.g. 'focus on tool argument quality'.");
	}
}
