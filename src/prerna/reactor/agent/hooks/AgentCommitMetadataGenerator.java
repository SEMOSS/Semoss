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
package prerna.reactor.agent.hooks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;

/** Generates a conventional Git subject and body for one file-changing run. */
final class AgentCommitMetadataGenerator {

	private static final Logger classLogger = LogManager.getLogger(AgentCommitMetadataGenerator.class);
	private static final String DEFAULT_COMMIT_MESSAGE = "Coding Agent Edit";
	private static final int MAX_TITLE_LENGTH = 120;
	private static final int MAX_DESCRIPTION_LENGTH = 1000;
	private static final int MAX_PROMPT_TEXT_LENGTH = 4000;

	private AgentCommitMetadataGenerator() {
	}

	static String generate(AgentRunContext ctx, AgentHarnessResult result, List<String> changedFiles) {
		String prompt = buildPrompt(ctx.getInput(), result == null ? null : result.getFinalText(), changedFiles);
		try {
			Map<String, Object> params = new HashMap<>();
			params.put("temperature", 0.1);
			params.put("max_completion_tokens", 300);
			// This one-shot call intentionally stays outside the room so generated commit
			// metadata does not appear in conversation history.
			@SuppressWarnings("deprecation")
			Map<String, Object> response = ctx.getModelEngine().ask(prompt, null, ctx.getInsight(), params).toMap();
			return parse(Objects.toString(response.get("response"), null), ctx.getInput());
		} catch (Exception e) {
			classLogger.warn("Commit metadata generation failed; using user input: {}", e.getMessage());
			return fallback(ctx.getInput());
		}
	}

	static String buildPrompt(String input, String finalText, List<String> changedFiles) {
		return """
				Write Git commit metadata for one coding-agent run. Return JSON only with exactly two string fields:
				{"title":"imperative subject, at most 120 characters","description":"brief explanation of what changed and why"}
				The title must describe the implemented file change, not build status, conversation, or tool activity.

				User request:
				%s

				Final agent response:
				%s

				Changed files:
				%s
				""".formatted(truncate(input, MAX_PROMPT_TEXT_LENGTH), truncate(finalText, MAX_PROMPT_TEXT_LENGTH),
				String.join("\n", changedFiles));
	}

	static String parse(String raw, String fallbackInput) {
		try {
			int start = raw == null ? -1 : raw.indexOf('{');
			int end = raw == null ? -1 : raw.lastIndexOf('}');
			if (start < 0 || end <= start) {
				return fallback(fallbackInput);
			}
			JSONObject json = new JSONObject(raw.substring(start, end + 1));
			String title = normalize(json.optString("title", null), MAX_TITLE_LENGTH, true);
			if (title == null) {
				return fallback(fallbackInput);
			}
			String description = normalize(json.optString("description", null), MAX_DESCRIPTION_LENGTH, false);
			return description == null ? title : title + "\n\n" + description;
		} catch (Exception e) {
			return fallback(fallbackInput);
		}
	}

	private static String fallback(String input) {
		String title = normalize(input, MAX_TITLE_LENGTH, true);
		return title == null ? DEFAULT_COMMIT_MESSAGE : title;
	}

	private static String normalize(String value, int maxLength, boolean singleLine) {
		if (value == null) {
			return null;
		}
		String normalized = singleLine ? value.replaceAll("[\\r\\n]+", " ").trim().replaceAll("\\s+", " ")
				: value.trim().replaceAll("\\s+", " ");
		if (normalized.isEmpty()) {
			return null;
		}
		return normalized.substring(0, Math.min(normalized.length(), maxLength));
	}

	private static String truncate(String value, int maxLength) {
		if (value == null) {
			return "";
		}
		return value.substring(0, Math.min(value.length(), maxLength));
	}
}
