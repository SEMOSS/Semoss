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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.util.Utility;

/**
 * Base for guardrails that ask a model to classify text as SAFE or UNSAFE.
 */
public abstract class PromptGuardrailEngine extends AbstractGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(PromptGuardrailEngine.class);

	public static final String MODEL_ENGINE_ID_KEY = "MODEL_ENGINE_ID";
	public static final String SYSTEM_PROMPT_KEY = "SYSTEM_PROMPT";
	public static final String BLOCKED_MESSAGE_KEY = "BLOCKED_MESSAGE";
	public static final String FAIL_OPEN_KEY = "FAIL_OPEN";

	/** Parameter key carrying an explicitly supplied conversation for screening. */
	static final String FULL_PROMPT_PARAM = AbstractModelEngine.FULL_PROMPT;

	protected static final String PROMPT_PARAM = "prompt";

	private String modelEngineId;
	private String systemPrompt;
	private String blockedMessage;
	private boolean failOpen;

	protected PromptGuardrailEngine(String... additionalParameters) {
		this.keysToGet = new String[additionalParameters.length + 1];
		this.keysToGet[0] = PROMPT_PARAM;
		System.arraycopy(additionalParameters, 0, this.keysToGet, 1, additionalParameters.length);
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelEngineId = getRequiredSmssProperty(MODEL_ENGINE_ID_KEY);
		configurePromptGuardrail();
		this.systemPrompt = getPropertyOrDefault(SYSTEM_PROMPT_KEY, getDefaultSystemPrompt());
		this.blockedMessage = getPropertyOrDefault(BLOCKED_MESSAGE_KEY, getDefaultBlockedMessage());
		this.failOpen = getBooleanProperty(FAIL_OPEN_KEY, isFailOpenByDefault());

		this.functionDescription = getPromptGuardrailDescription();
		this.parameters = new ArrayList<>(getPromptGuardrailParameters());
		this.requiredParameters = new ArrayList<>(getRequiredPromptGuardrailParameters());
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		String textToJudge = extractText(getRawNounValue(ns, curRow, PROMPT_PARAM, 0));
		if (textToJudge == null || textToJudge.isEmpty()) {
			return handleMissingText(textToJudge);
		}

		classLogger.info("{}: classifying text (length={}) via model={}", getClass().getSimpleName(),
				textToJudge.length(), this.modelEngineId);

		String classification;
		try {
			classification = classify(textToJudge, resolveSystemPrompt(keyValue));
		} catch (Exception e) {
			classLogger.error("{}: judge call failed, failing {}: {}", getClass().getSimpleName(),
					this.failOpen ? "open" : "closed", e.getMessage(), e);
			Map<String, Object> details = new HashMap<>();
			details.put("classification", this.failOpen ? "ERROR_FAIL_OPEN" : "ERROR_FAIL_CLOSED");
			details.put("error", e.getMessage());
			details.put("modelEngineId", this.modelEngineId);
			return new GuardrailNounMetadata(this.failOpen, textToJudge, details);
		}

		boolean pass = !classification.toUpperCase().contains("UNSAFE");
		classLogger.info("{}: classification='{}', pass={}", getClass().getSimpleName(), classification, pass);

		Map<String, Object> details = new HashMap<>();
		details.put("classification", classification);
		details.put("modelEngineId", this.modelEngineId);

		String returnPrompt = pass || this.blockedMessage == null ? textToJudge : this.blockedMessage;
		return new GuardrailNounMetadata(pass, returnPrompt, details);
	}

	protected void configurePromptGuardrail() {
		// Most prompt guardrails need no additional configuration.
	}

	protected abstract String getDefaultSystemPrompt();

	protected String getDefaultBlockedMessage() {
		return null;
	}

	protected boolean isFailOpenByDefault() {
		return false;
	}

	protected abstract String getPromptGuardrailDescription();

	protected List<FunctionParameter> getPromptGuardrailParameters() {
		return List.of(new FunctionParameter(PROMPT_PARAM, "String", "The text to evaluate"));
	}

	protected List<String> getRequiredPromptGuardrailParameters() {
		return List.of(PROMPT_PARAM);
	}

	protected String resolveSystemPrompt(Map<String, String> keyValue) {
		return this.systemPrompt;
	}

	protected String getConfiguredSystemPrompt() {
		return this.systemPrompt;
	}

	protected GuardrailNounMetadata handleMissingText(String textToJudge) {
		throw new IllegalArgumentException("No prompt has been defined");
	}

	private String classify(String textToJudge, String classificationPrompt) throws Exception {
		IModelEngine judgeEngine = Utility.getModel(this.modelEngineId);
		if (judgeEngine == null) {
			throw new IllegalStateException("Could not find model engine with id: " + this.modelEngineId);
		}

		Insight classificationInsight = new Insight();
		InsightStore.getInstance().put(classificationInsight);
		String savedJobId = ThreadStore.getJobId();
		ThreadStore.setJobId(null);
		try {
			Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), classificationInsight,
					judgeEngine, textToJudge);
			Map<String, Object> params = new HashMap<>();
			params.put("use_history", false);
			InputMessage msg = InputMessage.builder(room).withSystemPrompt(classificationPrompt).withText(textToJudge)
					.withModelType(judgeEngine.getModelType()).withParamMap(params).build();
			ResponseMessage response = room.ask(msg, judgeEngine);
			Object responseObj = response.getModelEngineResponse().toMap().get("response");
			return responseObj != null ? responseObj.toString().trim() : "";
		} finally {
			ThreadStore.setJobId(savedJobId);
			InsightStore.getInstance().remove(classificationInsight.getInsightId());
		}
	}

	protected String getRequiredSmssProperty(String key) {
		String value = this.smssProp.getProperty(key);
		if (value == null || (value = value.trim()).isEmpty()) {
			throw new IllegalArgumentException(key + " is required for " + getClass().getSimpleName());
		}
		return value;
	}

	private String getPropertyOrDefault(String key, String defaultValue) {
		String value = this.smssProp.getProperty(key);
		return value != null && !(value = value.trim()).isEmpty() ? value : defaultValue;
	}

	private boolean getBooleanProperty(String key, boolean defaultValue) {
		String value = this.smssProp.getProperty(key);
		return value != null && !(value = value.trim()).isEmpty() ? Boolean.parseBoolean(value) : defaultValue;
	}

	@SuppressWarnings("unchecked")
	static String extractText(Object rawValue) {
		if (rawValue == null) {
			return null;
		}
		if (rawValue instanceof String) {
			return (String) rawValue;
		}
		if (rawValue instanceof InputMessage) {
			InputMessage inputMessage = (InputMessage) rawValue;
			// a passthrough route sends the conversation as the full_prompt
			// parameter and a placeholder as the message text; screen the
			// conversation the engine will actually run, not the placeholder
			String fullPromptText = fullPromptText(inputMessage);
			if (fullPromptText != null) {
				return fullPromptText;
			}
			return inputMessage.getFullInputPrompt();
		}
		if (rawValue instanceof AbstractModelEngineResponse) {
			Object response = ((AbstractModelEngineResponse) rawValue).toMap().get("response");
			return response != null ? response.toString() : null;
		}
		if (rawValue instanceof Map) {
			Map<String, Object> valueMap = (Map<String, Object>) rawValue;
			Object response = valueMap.get("response");
			if (response != null) {
				return extractText(response);
			}
			Object body = valueMap.get("body");
			Object subject = valueMap.get("subject");
			if (body != null || subject != null) {
				return joinText(Arrays.asList(subject, body));
			}
			String fullPrompt = chatFullPromptText(valueMap);
			if (fullPrompt != null) {
				return fullPrompt;
			}
			return extractText(valueMap.get("messages"));
		}
		if (rawValue instanceof Collection) {
			return joinText((Collection<?>) rawValue);
		}
		return rawValue.toString();
	}

	/**
	 * The conversation a passthrough route sends as the message's full_prompt
	 * parameter. The final user turn is what a new request adds to the
	 * conversation, so its contents are what an input guardrail screens; a
	 * full_prompt without a user turn screens everything. A full_prompt entry is
	 * a chat-shaped map ({@code role} + one of {@code content}/{@code message}).
	 *
	 * @param inputMessage the intercepted message
	 * @return the full prompt text to screen, or null when none is present
	 */
	public static String fullPromptText(InputMessage inputMessage) {
		Map<String, Object> parameters = inputMessage.getParamMap();
		if (parameters == null) {
			return null;
		}
		Object fullPrompt = parameters.get(FULL_PROMPT_PARAM);
		if (fullPrompt == null) {
			return null;
		}
		if (fullPrompt instanceof String) {
			try {
				fullPrompt = new Gson().fromJson((String) fullPrompt, List.class);
			} catch (RuntimeException e) {
				return null;
			}
		}
		if (!(fullPrompt instanceof Collection)) {
			return null;
		}
		List<String> userTexts = new ArrayList<>();
		List<String> allTexts = new ArrayList<>();
		for (Object item : (Collection<?>) fullPrompt) {
			if (!(item instanceof Map)) {
				continue;
			}
			Map<?, ?> entry = (Map<?, ?>) item;
			Object content = entry.containsKey("message") ? entry.get("message") : entry.get("content");
			if (!(content instanceof String) || ((String) content).isEmpty()) {
				continue;
			}
			allTexts.add((String) content);
			if ("user".equalsIgnoreCase(String.valueOf(entry.get("role")))) {
				userTexts.add((String) content);
			}
		}
		if (userTexts.isEmpty() && allTexts.isEmpty()) {
			return null;
		}
		StringBuilder combined = new StringBuilder();
		for (String text : userTexts.isEmpty() ? allTexts : userTexts) {
			if (combined.length() > 0) {
				combined.append('\n');
			}
			combined.append(text);
		}
		return combined.toString();
	}

	/**
	 * A full_prompt parameter embedded in a plain map of engine parameters (the
	 * engine layer removes and converts it just before inference).
	 *
	 * @param valueMap a mapped parameter that resolved to a map
	 * @return the full prompt text to screen, or null when the map carries none
	 */
	private static String chatFullPromptText(Map<String, Object> valueMap) {
		Object fullPrompt = valueMap.get(FULL_PROMPT_PARAM);
		if (!(fullPrompt instanceof Collection)) {
			return null;
		}
		StringBuilder combined = new StringBuilder();
		for (Object item : (Collection<?>) fullPrompt) {
			if (item instanceof Map) {
				Object content = ((Map<?, ?>) item).get("content");
				if (content instanceof String && !((String) content).isEmpty()) {
					if (combined.length() > 0) {
						combined.append('\n');
					}
					combined.append(content);
				}
			}
		}
		return combined.length() > 0 ? combined.toString() : null;
	}

	private static String joinText(Collection<?> values) {
		StringBuilder combined = new StringBuilder();
		for (Object value : values) {
			String text = extractText(value);
			if (text == null || text.isEmpty()) {
				continue;
			}
			if (combined.length() > 0) {
				combined.append('\n');
			}
			combined.append(text);
		}
		return combined.toString();
	}

	private Object getRawNounValue(NounStore ns, GenRowStruct curRow, String key, int positionalIndexIfMissing) {
		if (ns != null) {
			GenRowStruct grs = ns.getGenRowStruct(key);
			if (grs != null && !grs.isEmpty()) {
				if (grs.size() == 1) {
					return grs.get(0);
				}
				List<Object> values = new ArrayList<>();
				for (int i = 0; i < grs.size(); i++) {
					values.add(grs.get(i));
				}
				return values;
			}
		}
		if (curRow != null && !curRow.isEmpty() && positionalIndexIfMissing < curRow.size()) {
			return curRow.get(positionalIndexIfMissing);
		}
		return null;
	}
}
