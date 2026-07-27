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
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Utility;

/**
 * Guardrail that detects aggressive or self-harm content by calling a
 * configured LLM with a classification system prompt. The model is expected to
 * respond with a single word — {@code SAFE} or {@code UNSAFE}.
 *
 * <p>Required SMSS keys:
 * <ul>
 *   <li>{@code MODEL_ENGINE_ID} — engine ID of the LLM to use for classification</li>
 * </ul>
 * Optional SMSS keys:
 * <ul>
 *   <li>{@code SYSTEM_PROMPT} — override the default classification instruction</li>
 * </ul>
 */
public class AggressiveSelfHarmGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AggressiveSelfHarmGuardrailEngine.class);

	public static final String MODEL_ENGINE_ID_KEY = "MODEL_ENGINE_ID";

	private static final String SYSTEM_PROMPT =
			"You are a content safety classifier. Your sole task is to detect whether a user message "
			+ "expresses first-person intent to harm themselves or others, suicidal ideation, or aggressive threats.\n\n"
			+ "Respond with ONLY one word:\n"
			+ "- UNSAFE if the message expresses a wish or intent to hurt oneself (e.g. 'I want to hurt myself', "
			+ "'I want to end my life', 'I hate myself and want to disappear'), threatens violence toward others, "
			+ "or contains aggressive language directed at a person\n"
			+ "- SAFE   for everything else, including clinical descriptions of past self-harm, "
			+ "third-party reports, or general medical questions\n\n"
			+ "Do not explain your reasoning. Output only SAFE or UNSAFE.";

	private String modelEngineId;

	public AggressiveSelfHarmGuardrailEngine() {
		this.keysToGet = new String[] { "prompt" };
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelEngineId = this.smssProp.getProperty(MODEL_ENGINE_ID_KEY);
		if (this.modelEngineId == null || this.modelEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException(MODEL_ENGINE_ID_KEY + " is required for AggressiveSelfHarmGuardrailEngine");
		}
		this.modelEngineId = this.modelEngineId.trim();

		this.functionDescription = "Detects aggressive or self-harm content by asking a configured LLM to classify "
				+ "the prompt as SAFE or UNSAFE.";
		this.parameters = new ArrayList<>();
		this.parameters.add(new FunctionParameter("prompt", "String", "The prompt to evaluate"));
		this.requiredParameters = new ArrayList<>(Arrays.asList("prompt"));
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		String prompt = keyValue.get("prompt");

		classLogger.info("AggressiveSelfHarmGuardrail: classifying prompt (length={}) via model={}",
				prompt.length(), this.modelEngineId);

		IModelEngine modelEngine = Utility.getModel(this.modelEngineId);
		if (modelEngine == null) {
			throw new IllegalStateException("Could not find model engine with id: " + this.modelEngineId);
		}

		Insight classificationInsight = new Insight();
		InsightStore.getInstance().put(classificationInsight);

		String classification;
		try {
			Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), classificationInsight, modelEngine, prompt);
			Map<String, Object> params = new HashMap<>();
			params.put("use_history", false);
			InputMessage msg = InputMessage.builder(room)
					.withSystemPrompt(SYSTEM_PROMPT)
					.withText(prompt)
					.withModelType(modelEngine.getModelType())
					.withParamMap(params)
					.build();
			ResponseMessage response = room.ask(msg, modelEngine);
			Object responseObj = response.getModelEngineResponse().toMap().get("response");
			classification = responseObj != null ? responseObj.toString().trim() : "";
		} finally {
			InsightStore.getInstance().remove(classificationInsight.getInsightId());
		}

		boolean pass = !classification.toUpperCase().contains("UNSAFE");

		classLogger.info("AggressiveSelfHarmGuardrail: classification='{}', pass={}", classification, pass);

		Map<String, Object> details = new HashMap<>();
		details.put("classification", classification);
		details.put("modelEngineId", this.modelEngineId);

		String returnPrompt = pass ? prompt
				: "Respond only with the following message, word for word: "
				+ "'I'm sorry, I'm not able to help with that. "
				+ "If you or someone you know is in crisis, please call or text 988 to reach the Veterans Crisis Line, "
				+ "or call 911 for immediate emergency assistance.'";

		return new GuardrailNounMetadata(pass, returnPrompt, details);
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_AGGRESSIVE_SELF_HARM;
	}
}
