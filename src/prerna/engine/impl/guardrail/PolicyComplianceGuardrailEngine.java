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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Utility;

/**
 * Guardrail that classifies text against a configurable content policy by
 * calling a configured LLM with a classification system prompt. The model is
 * expected to respond with a single word {@code SAFE} or {@code UNSAFE}.
 * Typically mounted on an {@code output} pipeline to check a model's response,
 * but works identically on a user prompt when mounted on {@code input}.
 *
 * Required SMSS keys:
 *   {@code MODEL_ENGINE_ID} engine ID of the LLM to use for classification.
 *    Must not itself be guarded by this (or any) output pipeline.
 *   {@code POLICY_DESCRIPTION} the behaviors that make the text UNSAFE
 * Optional SMSS keys:
 *   {@code BLOCKED_MESSAGE} canned replacement text; only takes effect
 *       when mounted on an {@code input} pipeline with
 *   {@code maskOnGuardrailFailure=true}. Output pipelines can only block a
 *       failing response today, not rewrite it.
 * Optional per-call parameter {@code policy} overrides {@code POLICY_DESCRIPTION}.
 */
public class PolicyComplianceGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(PolicyComplianceGuardrailEngine.class);

	public static final String MODEL_ENGINE_ID_KEY = "MODEL_ENGINE_ID";
	public static final String POLICY_DESCRIPTION_KEY = "POLICY_DESCRIPTION";
	public static final String BLOCKED_MESSAGE_KEY = "BLOCKED_MESSAGE";

	private static final String PROMPT_PARAM = "prompt";
	private static final String POLICY_PARAM = "policy";

	private static final String SYSTEM_PROMPT_TEMPLATE =
			"You are a strict content-policy classifier. You review a piece of text - typically an AI "
			+ "assistant's RESPONSE, reviewed here before it is shown to the end user - and decide whether "
			+ "it violates the policy below.\n\n"
			+ "POLICY - the text is UNSAFE if it does any of the following:\n"
			+ "${POLICY_DESCRIPTION}\n\n"
			+ "The text is SAFE if it does not do any of the above - including when it factually reports "
			+ "negative, critical, or unfavorable information that is explicitly documented in the source "
			+ "material, WITHOUT adding the assistant's own opinion, characterization, or advice. Faithfully "
			+ "reporting documented facts is required, not prohibited, even when those facts are unflattering.\n\n"
			+ "Respond with ONLY one word:\n"
			+ "- UNSAFE if the text violates the policy above\n"
			+ "- SAFE   if it does not\n\n"
			+ "Do not explain your reasoning. Output only SAFE or UNSAFE.";

	private String modelEngineId;
	private String defaultPolicyDescription;
	private String blockedMessage;

	public PolicyComplianceGuardrailEngine() {
		this.keysToGet = new String[] { PROMPT_PARAM, POLICY_PARAM };
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelEngineId = this.smssProp.getProperty(MODEL_ENGINE_ID_KEY);
		if (this.modelEngineId == null || (this.modelEngineId = this.modelEngineId.trim()).isEmpty()) {
			throw new IllegalArgumentException(MODEL_ENGINE_ID_KEY + " is required for PolicyComplianceGuardrailEngine");
		}

		this.defaultPolicyDescription = this.smssProp.getProperty(POLICY_DESCRIPTION_KEY);
		if (this.defaultPolicyDescription == null || (this.defaultPolicyDescription = this.defaultPolicyDescription.trim()).isEmpty()) {
			throw new IllegalArgumentException(POLICY_DESCRIPTION_KEY + " is required for PolicyComplianceGuardrailEngine");
		}

		String blockedMessageStr = this.smssProp.getProperty(BLOCKED_MESSAGE_KEY);
		if (blockedMessageStr != null && !(blockedMessageStr = blockedMessageStr.trim()).isEmpty()) {
			this.blockedMessage = blockedMessageStr;
		}

		this.functionDescription = "Classifies text against a configurable content policy using an LLM judge, "
				+ "returning SAFE/UNSAFE.";
		this.parameters = new ArrayList<>();
		this.parameters.add(new FunctionParameter("prompt", "String", "The text to evaluate against the policy"));
		this.parameters.add(new FunctionParameter("policy", "String", "Optional override of POLICY_DESCRIPTION for this call"));
		this.requiredParameters = new ArrayList<>(Arrays.asList("prompt"));
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		Object rawPrompt = getRawNounValue(ns, curRow, PROMPT_PARAM, 0);
		String textToJudge = extractText(rawPrompt);
		if (textToJudge == null || textToJudge.isEmpty()) {
			Map<String, Object> details = new HashMap<>();
			details.put("classification", "SKIPPED_NO_TEXT");
			return new GuardrailNounMetadata(true, textToJudge, details);
		}

		String policyDescription = keyValue.containsKey(POLICY_PARAM) && !keyValue.get(POLICY_PARAM).isEmpty()
				? keyValue.get(POLICY_PARAM)
				: this.defaultPolicyDescription;

		classLogger.info("PolicyComplianceGuardrail: classifying text (length={}) via model={}",
				textToJudge.length(), this.modelEngineId);

		String classification;
		try {
			classification = classify(textToJudge, policyDescription);
		} catch (Exception e) {
			classLogger.error("PolicyComplianceGuardrail: judge call failed, passing by default (fail-open): {}",
					e.getMessage());
			Map<String, Object> details = new HashMap<>();
			details.put("classification", "ERROR_FAIL_OPEN");
			details.put("error", e.getMessage());
			return new GuardrailNounMetadata(true, textToJudge, details);
		}

		boolean pass = !classification.toUpperCase().contains("UNSAFE");
		classLogger.info("PolicyComplianceGuardrail: classification='{}', pass={}", classification, pass);

		Map<String, Object> details = new HashMap<>();
		details.put("classification", classification);
		details.put("modelEngineId", this.modelEngineId);

		String returnPrompt = pass ? textToJudge : (this.blockedMessage != null ? this.blockedMessage : textToJudge);
		return new GuardrailNounMetadata(pass, returnPrompt, details);
	}

	private String classify(String textToJudge, String policyDescription) throws Exception {
		IModelEngine judgeEngine = Utility.getModel(this.modelEngineId);
		if (judgeEngine == null) {
			throw new IllegalStateException("Could not find model engine with id: " + this.modelEngineId);
		}

		// Room/model calls require an Insight to be registered, this one is never tied to a real user session.
		Insight classificationInsight = new Insight();
		InsightStore.getInstance().put(classificationInsight);
		try {
			Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), classificationInsight,
					judgeEngine, textToJudge);
			Map<String, Object> params = new HashMap<>();
			params.put("use_history", false);
			InputMessage msg = InputMessage.builder(room)
					.withSystemPrompt(buildSystemPrompt(policyDescription))
					.withText(textToJudge)
					.withModelType(judgeEngine.getModelType())
					.withParamMap(params)
					.build();
			ResponseMessage response = room.ask(msg, judgeEngine);
			Object responseObj = response.getModelEngineResponse().toMap().get("response");
			return responseObj != null ? responseObj.toString().trim() : "";
		} finally {
			InsightStore.getInstance().remove(classificationInsight.getInsightId());
		}
	}

	private String buildSystemPrompt(String policyDescription) {
		return SYSTEM_PROMPT_TEMPLATE.replace("${POLICY_DESCRIPTION}", policyDescription);
	}

	@SuppressWarnings("unchecked")
	private String extractText(Object rawValue) {
		if (rawValue == null) {
			return null;
		}
		if (rawValue instanceof String) {
			return (String) rawValue;
		}
		if (rawValue instanceof AbstractModelEngineResponse) {
			Object resp = ((AbstractModelEngineResponse) rawValue).toMap().get("response");
			return resp != null ? resp.toString() : null;
		}
		if (rawValue instanceof Map) {
			Object resp = ((Map<String, Object>) rawValue).get("response");
			return resp != null ? resp.toString() : null;
		}
		return rawValue.toString();
	}

	private Object getRawNounValue(NounStore ns, GenRowStruct curRow, String key, int positionalIndexIfMissing) {
		if (ns != null) {
			GenRowStruct grs = ns.getGenRowStruct(key);
			if (grs != null && !grs.isEmpty()) {
				return grs.get(0);
			}
		}
		if (curRow != null && !curRow.isEmpty() && positionalIndexIfMissing < curRow.size()) {
			return curRow.get(positionalIndexIfMissing);
		}
		return null;
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_POLICY_COMPLIANCE;
	}
}