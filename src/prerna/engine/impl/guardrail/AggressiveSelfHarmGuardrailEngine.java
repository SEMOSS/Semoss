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
package prerna.engine.impl.guardrail;

import prerna.engine.api.GuardrailTypeEnum;

/**
 * Guardrail that detects aggressive or self-harm content by calling a
 * configured LLM with a classification system prompt. The model is expected to
 * respond with a single word - {@code SAFE} or {@code UNSAFE}.
 *
 * <p>
 * Required SMSS keys:
 * <ul>
 * <li>{@code MODEL_ENGINE_ID} - engine ID of the LLM to use for
 * classification</li>
 * </ul>
 * Optional SMSS keys:
 * <ul>
 * <li>{@code SYSTEM_PROMPT} - override the default classification
 * instruction</li>
 * <li>{@code BLOCKED_MESSAGE} - override the default message returned when
 * UNSAFE</li>
 * <li>{@code FAIL_OPEN} - allow the guarded call when the judge errors;
 * defaults to false</li>
 * </ul>
 */
public class AggressiveSelfHarmGuardrailEngine extends PromptGuardrailEngine {

	private static final String DEFAULT_SYSTEM_PROMPT = """
			You are a content safety classifier. Your sole task is to detect whether a user message expresses first-person intent to harm themselves or others, suicidal ideation, or aggressive threats.

			Respond with ONLY one word:
			- UNSAFE if the message expresses a wish or intent to hurt oneself (e.g. 'I want to hurt myself', 'I want to end my life', 'I hate myself and want to disappear'), threatens violence toward others, or contains aggressive language directed at a person
			- SAFE for everything else, including clinical descriptions of past self-harm, third-party reports, or general medical questions

			Do not explain your reasoning. Output only SAFE or UNSAFE.
			""";

	private static final String DEFAULT_BLOCKED_MESSAGE = "I'm sorry, I'm not able to help with that. If you or "
			+ "someone you know is in crisis, please call or text 988 to reach the Suicide & Crisis Lifeline, or call "
			+ "911 for immediate emergency assistance.";

	@Override
	protected String getDefaultSystemPrompt() {
		return DEFAULT_SYSTEM_PROMPT;
	}

	@Override
	protected String getDefaultBlockedMessage() {
		return DEFAULT_BLOCKED_MESSAGE;
	}

	@Override
	protected String getPromptGuardrailDescription() {
		return "Detects aggressive or self-harm content by asking a configured LLM to classify the prompt as "
				+ "SAFE or UNSAFE.";
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_AGGRESSIVE_SELF_HARM;
	}

	@Override
	public String getDefaultMarkdown() {
		return """
				# Aggressive / Self-Harm guardrail

				This guardrail sends selected text to the configured model engine with a safety-classification system prompt. The judge must answer `SAFE` or `UNSAFE`. An unsafe result stops the guarded call or returns the configured blocked message, depending on the pipeline settings.

				`SYSTEM_PROMPT`, `BLOCKED_MESSAGE`, and `FAIL_OPEN` are optional SMSS settings. Override them to adapt the behavior. `FAIL_OPEN` defaults to `false`, so a judge error blocks the call.

				## Example: protect model prompts

				Save this as `pipeline.json` in the model engine's assets folder, set `PIPELINE pipeline.json` in that model engine's SMSS, and restart or reload the model engine:

				```json
				{
				  "pipelines": {
				    "askRoom": {
				      "input": [
				        {
				          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
				          "params": {
				            "guardrailEngineId": "%s",
				            "inputMapping": {
				              "prompt": "arg0"
				            },
				            "blockOnGuardrailFailure": false,
				            "respondWithGuardrailMessage": true
				          }
				        }
				      ]
				    }
				  }
				}
				```

				The input reactor maps `askRoom`'s first argument to the guardrail's `prompt`. When the judge returns `UNSAFE`, the actual model call is skipped and the blocked message is returned. Do not attach a pipeline that invokes this guardrail to its judge model, because that would recurse.
				"""
				.formatted(getEngineId());
	}
}
