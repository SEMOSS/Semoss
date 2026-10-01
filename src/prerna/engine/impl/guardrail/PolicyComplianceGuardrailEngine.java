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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.impl.function.FunctionParameter;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

/**
 * Guardrail that classifies text against a configurable content policy by
 * calling a configured LLM with a classification system prompt. The model is
 * expected to respond with a single word {@code SAFE} or {@code UNSAFE}.
 * <p>
 * Typically mounted on an {@code output} pipeline to check a model's response,
 * but works identically on a user prompt when mounted on {@code input}.
 *
 * <h3>Required SMSS keys</h3>
 * <ul>
 * <li>{@code MODEL_ENGINE_ID} - engine ID of the LLM used for classification.
 * The judge model must not use a pipeline that invokes this guardrail.</li>
 * <li>{@code POLICY_DESCRIPTION} - behaviors that make the evaluated text
 * unsafe.</li>
 * </ul>
 * 
 * <h3>Optional SMSS keys</h3>
 * <ul>
 * <li>{@code SYSTEM_PROMPT} - replaces the default judge instructions. The
 * {@code ${POLICY_DESCRIPTION}} placeholder is replaced for each call.</li>
 * <li>{@code BLOCKED_MESSAGE} - replacement text used by an input pipeline when
 * {@code respondWithGuardrailMessage=true}. Output pipelines can block a
 * failing response but cannot rewrite it.</li>
 * <li>{@code FAIL_OPEN} - whether a judge error should allow the guarded call.
 * Defaults to {@code true} for compatibility. Set it to {@code false} for
 * irreversible operations such as sending mail.</li>
 * </ul>
 *
 * <h3>Optional call parameters</h3>
 * <ul>
 * <li>{@code policy} - overrides {@code POLICY_DESCRIPTION} for one call.</li>
 * </ul>
 */
public class PolicyComplianceGuardrailEngine extends PromptGuardrailEngine {

	public static final String POLICY_DESCRIPTION_KEY = "POLICY_DESCRIPTION";
	private static final String POLICY_PARAM = "policy";

	private static final String DEFAULT_SYSTEM_PROMPT = """
			You are a strict content-policy classifier. You review a piece of text - typically an AI assistant's RESPONSE, reviewed here before it is shown to the end user - and decide whether it violates the policy below.

			POLICY - the text is UNSAFE if it does any of the following:
			${POLICY_DESCRIPTION}

			The text is SAFE if it does not do any of the above - including when it factually reports negative, critical, or unfavorable information that is explicitly documented in the source material, WITHOUT adding the assistant's own opinion, characterization, or advice. Faithfully reporting documented facts is required, not prohibited, even when those facts are unflattering.

			Respond with ONLY one word:
			- UNSAFE if the text violates the policy above
			- SAFE if it does not

			Do not explain your reasoning. Output only SAFE or UNSAFE.
			""";

	private String defaultPolicyDescription;

	public PolicyComplianceGuardrailEngine() {
		super(POLICY_PARAM);
	}

	@Override
	protected void configurePromptGuardrail() {
		this.defaultPolicyDescription = getRequiredSmssProperty(POLICY_DESCRIPTION_KEY);
	}

	@Override
	protected String getDefaultSystemPrompt() {
		return DEFAULT_SYSTEM_PROMPT;
	}

	@Override
	protected boolean isFailOpenByDefault() {
		return true;
	}

	@Override
	protected String getPromptGuardrailDescription() {
		return "Classifies text against a configurable content policy using an LLM judge, returning SAFE/UNSAFE.";
	}

	@Override
	protected List<FunctionParameter> getPromptGuardrailParameters() {
		return List.of(new FunctionParameter(PROMPT_PARAM, "String", "The text to evaluate against the policy"),
				new FunctionParameter(POLICY_PARAM, "String", "Optional override of POLICY_DESCRIPTION for this call"));
	}

	@Override
	protected String resolveSystemPrompt(Map<String, String> keyValue) {
		String policyDescription = keyValue.containsKey(POLICY_PARAM) && !keyValue.get(POLICY_PARAM).isEmpty()
				? keyValue.get(POLICY_PARAM)
				: this.defaultPolicyDescription;
		return getConfiguredSystemPrompt().replace("${POLICY_DESCRIPTION}", policyDescription);
	}

	@Override
	protected GuardrailNounMetadata handleMissingText(String textToJudge) {
		Map<String, Object> details = new HashMap<>();
		details.put("classification", "SKIPPED_NO_TEXT");
		return new GuardrailNounMetadata(true, textToJudge, details);
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_POLICY_COMPLIANCE;
	}

	@Override
	public String getDefaultMarkdown() {
		return """
				# Policy Compliance guardrail

				This guardrail sends selected text and a policy to the configured model engine. The judge must answer `SAFE` or `UNSAFE`; an unsafe result prevents the guarded result from being returned.

				The SMSS `POLICY_DESCRIPTION` is the default policy. A pipeline may supply a `policy` in `directParameters` for one use case. `SYSTEM_PROMPT` can replace the default judge instructions; include `${POLICY_DESCRIPTION}` where the policy should be inserted. `BLOCKED_MESSAGE` and `FAIL_OPEN` are also optional. `FAIL_OPEN` defaults to `true`; use `false` before irreversible actions such as sending mail.

				## Example: review model responses

				Save this as `pipeline.json` in the model engine's assets folder, set `PIPELINE pipeline.json` in that model engine's SMSS, and restart or reload the model engine:

				```json
				{
				  "pipelines": {
				    "askRoom": {
				      "output": [
				        {
				          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailOutputReactor",
				          "params": {
				            "guardrailEngineId": "%s",
				            "inputMapping": {
				              "prompt": "result"
				            },
				            "directParameters": {
				              "policy": "Classify the response as UNSAFE if it exposes secrets, invents unsupported claims, or gives instructions outside the approved scope."
				            },
				            "blockOnGuardrailFailure": true,
				            "blockErrorMessage": "The response did not pass policy review."
				          }
				        }
				      ]
				    }
				  }
				}
				```

				The output reactor maps the completed model response to `prompt`, supplies a use-case-specific policy, and withholds the result if the judge returns `UNSAFE`. Omit `directParameters` to use the engine's default policy. Do not attach a pipeline that invokes this guardrail to its judge model, because that would recurse.
				"""
				.formatted(getEngineId());
	}
}
