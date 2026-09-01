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
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.ds.py.PyUtils;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

public class PromptInjectionGuardrailEngine extends AbstractPythonGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractPythonModelEngine.class);

	private static final String MODEL_NAME_KEY = "MODEL_NAME";
	private static final String LABEL_DECISION_MAP_KEY = "LABEL_DECISION_MAP";
	private static final String DEFAULT_THRESHOLD_KEY = "DEFAULT_THRESHOLD";
	private static final String DEFAULT_DECISION_KEY = "DEFAULT_DECISION";
	private static final String DEFAULT_MAX_LENGTH_KEY = "DEFAULT_MAX_LENGTH";
	private static final String TRUST_REMOTE_CODE_KEY = "TRUST_REMOTE_CODE";
	private static final String USE_CUDA_KEY = "USE_CUDA";
	private static final String DEVICE_KEY = "DEVICE";

	private static final double DEFAULT_THRESHOLD = 0.7;
	private static final int DEFAULT_MAX_LENGTH = 512;
	private static final boolean DEFAULT_DECISION_ALLOW = false; // fail closed by default

	private String modelName = null;
	private Map<String, Boolean> labelDecisionMap = Collections.emptyMap();
	private double defaultThreshold = DEFAULT_THRESHOLD;
	private boolean defaultDecisionAllow = DEFAULT_DECISION_ALLOW;
	private int defaultMaxLength = DEFAULT_MAX_LENGTH;
	private boolean trustRemoteCode = false;
	private Boolean useCuda = null;
	private String device = null;

	public PromptInjectionGuardrailEngine() {
		this.keysToGet = new String[] { "prompt", "threshold", "maxLength" };
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelName = this.smssProp.getProperty(MODEL_NAME_KEY);
		if (this.modelName == null || (this.modelName = this.modelName.trim()).isEmpty()) {
			classLogger.warn("Must define the prompt injection classifier MODEL_NAME");
			throw new IllegalArgumentException("Must define the prompt injection classifier MODEL_NAME");
		}

		String labelDecisionMapStr = this.smssProp.getProperty(LABEL_DECISION_MAP_KEY);
		this.labelDecisionMap = parseLabelDecisionMap(labelDecisionMapStr);

		String defaultThresholdStr = this.smssProp.getProperty(DEFAULT_THRESHOLD_KEY);
		if (defaultThresholdStr != null && !(defaultThresholdStr = defaultThresholdStr.trim()).isEmpty()) {
			try {
				this.defaultThreshold = Double.parseDouble(defaultThresholdStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid default threshold value " + defaultThresholdStr
						+ ". Revert to default value of " + this.defaultThreshold, e);
			}
		}

		String defaultMaxLengthStr = this.smssProp.getProperty(DEFAULT_MAX_LENGTH_KEY);
		if (defaultMaxLengthStr != null && !(defaultMaxLengthStr = defaultMaxLengthStr.trim()).isEmpty()) {
			try {
				this.defaultMaxLength = Integer.parseInt(defaultMaxLengthStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid default maxLength value " + defaultMaxLengthStr
						+ ". Revert to default value of " + this.defaultMaxLength, e);
			}
		}

		String defaultDecisionStr = this.smssProp.getProperty(DEFAULT_DECISION_KEY);
		if (defaultDecisionStr != null && !(defaultDecisionStr = defaultDecisionStr.trim()).isEmpty()) {
			this.defaultDecisionAllow = parseAllowDecision(defaultDecisionStr);
		}

		String trustRemoteCodeStr = this.smssProp.getProperty(TRUST_REMOTE_CODE_KEY);
		if (trustRemoteCodeStr != null && !(trustRemoteCodeStr = trustRemoteCodeStr.trim()).isEmpty()) {
			this.trustRemoteCode = Boolean.parseBoolean(trustRemoteCodeStr);
			if (this.trustRemoteCode) {
				classLogger.warn("Prompt injection guardrail engine "
						+ SmssUtilities.getUniqueName(this.engineName, this.engineId)
						+ " is configured with TRUST_REMOTE_CODE=true. This executes model repository code at load time.");
			}
		}

		String useCudaStr = this.smssProp.getProperty(USE_CUDA_KEY);
		if (useCudaStr != null && !(useCudaStr = useCudaStr.trim()).isEmpty()) {
			this.useCuda = Boolean.parseBoolean(useCudaStr);
		}

		String deviceStr = this.smssProp.getProperty(DEVICE_KEY);
		if (deviceStr != null && !(deviceStr = deviceStr.trim()).isEmpty()) {
			this.device = deviceStr;
		}
		this.functionDescription = "Classifies a prompt for prompt-injection attempts using a HuggingFace text-classification model.";
		this.parameters = new ArrayList<>();
		this.parameters
				.add(new FunctionParameter("prompt", "String", "This is the prompt we are applying the guardrail to"));
		this.parameters.add(new FunctionParameter("threshold", "Double",
				"Number between 0-1. If any label mapped to BLOCK has score >= threshold, the prompt fails. Default is "
						+ this.defaultThreshold));
		this.parameters.add(new FunctionParameter("maxLength", "Integer",
				"Max tokenized length for the classifier truncation. Default is " + this.defaultMaxLength));
		this.requiredParameters = new ArrayList<>(Arrays.asList("prompt"));
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		checkSocketStatus();
		Map<String, String> keyValue = organizeKeys(ns, curRow);

		String prompt = keyValue.get(this.keysToGet[0]);
		if (prompt == null) {
			throw new IllegalArgumentException("No prompt has been defined");
		}

		double threshold = this.defaultThreshold;
		if (keyValue.containsKey(this.keysToGet[1])) {
			threshold = Double.parseDouble(keyValue.get(this.keysToGet[1]));
		}

		int maxLength = this.defaultMaxLength;
		if (keyValue.containsKey(this.keysToGet[2])) {
			maxLength = Integer.parseInt(keyValue.get(this.keysToGet[2]));
		}

		String script = "classifier.classify(" + PyUtils.determineStringType(prompt) + ", max_length="
				+ PyUtils.determineStringType(maxLength) + ")";
		Map<String, Object> predictions = (Map<String, Object>) pyTranslator.runDirectPyNoCancelTrace(script);

		Map<String, Double> scoresByLabel = extractScoresByLabel(predictions);
		DecisionResult decision = evaluate(scoresByLabel, this.labelDecisionMap, threshold, this.defaultDecisionAllow);

		Map<String, Object> retValue = new HashMap<>();
		retValue.put("modelName", this.modelName);
		retValue.put("threshold", threshold);
		retValue.put("maxLength", maxLength);
		retValue.put("topLabel", predictions.get("top_label"));
		retValue.put("topScore", predictions.get("top_score"));
		retValue.put("blockLabel", decision.blockLabel);
		retValue.put("blockScore", decision.blockScore);
		retValue.put("scoresByLabel", scoresByLabel);
		retValue.put("rawReturn", predictions);

		return new GuardrailNounMetadata(decision.pass, prompt, retValue);
	}

	static class DecisionResult {
		final boolean pass;
		final String blockLabel;
		final Double blockScore;

		private DecisionResult(boolean pass, String blockLabel, Double blockScore) {
			this.pass = pass;
			this.blockLabel = blockLabel;
			this.blockScore = blockScore;
		}
	}

	static DecisionResult evaluate(Map<String, Double> scoresByLabel, Map<String, Boolean> labelDecisionMap,
			double threshold, boolean defaultDecisionAllow) {
		if (scoresByLabel == null || scoresByLabel.isEmpty()) {
			return new DecisionResult(defaultDecisionAllow, null, null);
		}
		if (labelDecisionMap == null || labelDecisionMap.isEmpty()) {
			return new DecisionResult(defaultDecisionAllow, null, null);
		}

		boolean pass = true;
		String blockLabel = null;
		Double blockScore = null;

		for (Map.Entry<String, Double> entry : scoresByLabel.entrySet()) {
			String label = entry.getKey();
			Double score = entry.getValue();
			if (label == null || score == null) {
				continue;
			}

			Boolean allow = getDecisionForLabel(labelDecisionMap, label);
			if (allow == null) {
				allow = defaultDecisionAllow;
			}

			if (!allow && score >= threshold) {
				if (blockScore == null || score > blockScore) {
					blockScore = score;
					blockLabel = label;
				}
				pass = false;
			}
		}

		return new DecisionResult(pass, blockLabel, blockScore);
	}

	private static Boolean getDecisionForLabel(Map<String, Boolean> labelDecisionMap, String label) {
		Boolean direct = labelDecisionMap.get(label);
		if (direct != null) {
			return direct;
		}
		return labelDecisionMap.get(label.toLowerCase(Locale.ROOT));
	}

	static Map<String, Boolean> parseLabelDecisionMap(String labelDecisionMapStr) {
		if (labelDecisionMapStr == null || (labelDecisionMapStr = labelDecisionMapStr.trim()).isEmpty()) {
			throw new IllegalArgumentException(
					"Must define " + LABEL_DECISION_MAP_KEY + " as a JSON object mapping label->ALLOW/BLOCK");
		}

		Map<String, Object> raw = new Gson().fromJson(labelDecisionMapStr, Map.class);
		if (raw == null || raw.isEmpty()) {
			throw new IllegalArgumentException("Invalid " + LABEL_DECISION_MAP_KEY + ": empty map");
		}

		Map<String, Boolean> out = new HashMap<>();
		for (Map.Entry<String, Object> entry : raw.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key == null || (key = key.trim()).isEmpty()) {
				continue;
			}
			boolean allow = parseAllowValue(value);
			out.put(key, allow);
			out.put(key.toLowerCase(Locale.ROOT), allow);
		}

		if (out.isEmpty()) {
			throw new IllegalArgumentException("Invalid " + LABEL_DECISION_MAP_KEY + ": no valid mappings");
		}

		return out;
	}

	private static boolean parseAllowValue(Object value) {
		if (value == null) {
			throw new IllegalArgumentException("Invalid label mapping value: null");
		}
		if (value instanceof Boolean) {
			return ((Boolean) value).booleanValue();
		}
		String str = (value + "").trim();
		if (str.equalsIgnoreCase("allow") || str.equalsIgnoreCase("allowed") || str.equalsIgnoreCase("safe")
				|| str.equalsIgnoreCase("pass") || str.equalsIgnoreCase("true")) {
			return true;
		}
		if (str.equalsIgnoreCase("block") || str.equalsIgnoreCase("blocked") || str.equalsIgnoreCase("injection")
				|| str.equalsIgnoreCase("fail") || str.equalsIgnoreCase("false")) {
			return false;
		}
		throw new IllegalArgumentException("Invalid label mapping value: " + value);
	}

	private static boolean parseAllowDecision(String defaultDecisionStr) {
		String str = defaultDecisionStr.trim();
		if (str.equalsIgnoreCase("allow") || str.equalsIgnoreCase("allowed") || str.equalsIgnoreCase("safe")
				|| str.equalsIgnoreCase("pass") || str.equalsIgnoreCase("true")) {
			return true;
		}
		if (str.equalsIgnoreCase("block") || str.equalsIgnoreCase("blocked") || str.equalsIgnoreCase("injection")
				|| str.equalsIgnoreCase("fail") || str.equalsIgnoreCase("false")) {
			return false;
		}
		throw new IllegalArgumentException("Invalid " + DEFAULT_DECISION_KEY + " value: " + defaultDecisionStr);
	}

	private static Map<String, Double> extractScoresByLabel(Map<String, Object> predictions) {
		if (predictions == null) {
			return Collections.emptyMap();
		}
		Object scoresObj = predictions.get("scores_by_label");
		if (!(scoresObj instanceof Map)) {
			return Collections.emptyMap();
		}

		Map<String, Object> raw = (Map<String, Object>) scoresObj;
		Map<String, Double> out = new HashMap<>();
		for (Map.Entry<String, Object> entry : raw.entrySet()) {
			String label = entry.getKey();
			Object scoreObj = entry.getValue();
			if (label == null || scoreObj == null) {
				continue;
			}
			double score;
			if (scoreObj instanceof Number) {
				score = ((Number) scoreObj).doubleValue();
			} else {
				score = Double.parseDouble(scoreObj + "");
			}
			out.put(label, score);
		}
		return out;
	}

	@Override
	protected String getStartupScript() {
		String deviceLiteral = PyUtils.determineStringType(this.device);
		String useCudaLiteral = PyUtils.determineStringType(this.useCuda);
		// @formatter:off
		return "from smss_util.PromptInjection import PromptInjectionClassifier\n"
				+ "classifier = PromptInjectionClassifier(model_id="
				+ PyUtils.determineStringType(this.modelName)
				+ ", trust_remote_code="
				+ PyUtils.determineStringType(this.trustRemoteCode) + ", use_cuda=" + useCudaLiteral
				+ ", device=" + deviceLiteral + ")";
		// @formatter:on
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_PROMPT_INJECTION;
	}

	@Override
	public String getDefaultMarkdown() {
		return """
				# Prompt-injection guardrail

				This guardrail runs a local Hugging Face text-classification model and evaluates its label scores using `LABEL_DECISION_MAP`. A label mapped to `BLOCK` fails when its score is at or above the threshold.

				`MODEL_NAME` and `LABEL_DECISION_MAP` are required in the guardrail SMSS. Match the map keys to the exact labels produced by that model, for example `{"SAFE":"ALLOW","INJECTION":"BLOCK"}`. `DEFAULT_DECISION` is used for unmapped or missing labels and defaults to block. Optional settings include `DEFAULT_THRESHOLD`, `DEFAULT_MAX_LENGTH`, `DEVICE`, `USE_CUDA`, and `TRUST_REMOTE_CODE`. Enable remote code only for a model repository you trust.

				## Example: block injection attempts before model inference

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
				            "directParameters": {
				              "threshold": 0.75,
				              "maxLength": 512
				            },
				            "blockOnGuardrailFailure": true,
				            "blockErrorMessage": "The request was blocked because it may contain prompt-injection instructions."
				          }
				        }
				      ]
				    }
				  }
				}
				```

				The classifier sees the text from the actual `InputMessage`, and a failed check prevents the model call. This guardrail returns the original prompt, so use it to block rather than mask.
				"""
				.formatted(getEngineId());
	}

}
