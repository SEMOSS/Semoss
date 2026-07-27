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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

public class GLiNERGuardrailEngine extends AbstractPythonGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractPythonModelEngine.class);

	private static final String MODEL_NAME = "MODEL_NAME";
	private static final String NER_LABELS = "NER_LABELS";
	private static final String DEFAULT_THRESHOLD_KEY = "DEFAULT_THRESHOLD";
	private static final String MASK_TEMPLATE_KEY = "MASK_TEMPLATE";

	private String modelName = null;
	private List<String> defaultLabels = null;
	private Double defaultThreshold = .7;
	// template used when masking a matched entity; {label} is replaced with the entity label
	private String maskTemplate = "[{label}]";

	public GLiNERGuardrailEngine() {
		this.keysToGet = new String[] { "prompt", "labels", "threshold" };
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.modelName = this.smssProp.getProperty(MODEL_NAME);
		if (this.modelName == null || (this.modelName = this.modelName.trim()).isEmpty()) {
			classLogger.warn("Must define the GLiNER model name");
			throw new IllegalArgumentException("Must define the GLiNER model name");
		}

		String defaultLabelsStr = this.smssProp.getProperty(NER_LABELS);
		if (defaultLabelsStr != null && !(defaultLabelsStr = defaultLabelsStr.trim()).isEmpty()) {
			this.defaultLabels = new Gson().fromJson(defaultLabelsStr, java.util.List.class);
		}

		String defaultThresholdStr = this.smssProp.getProperty(DEFAULT_THRESHOLD_KEY);
		if (defaultThresholdStr != null && !(defaultThresholdStr = defaultThresholdStr.trim()).isEmpty()) {
			try {
				defaultThreshold = Double.parseDouble(defaultThresholdStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid default threshold value " + defaultThresholdStr
						+ ". Revert to default value of " + defaultThreshold, e);
			}
		}

		String maskTemplateStr = this.smssProp.getProperty(MASK_TEMPLATE_KEY);
		if (maskTemplateStr != null && !(maskTemplateStr = maskTemplateStr.trim()).isEmpty()) {
			this.maskTemplate = maskTemplateStr;
		}

		this.functionDescription = "Applying Named Entity Recognition based on provided user labels";
		this.parameters = new ArrayList<>();
		this.parameters
				.add(new FunctionParameter("prompt", "String", "This is the prompt we are applying the guardrail to"));
		this.parameters.add(new FunctionParameter("labels", "List<String>",
				"List of named entity lables to apply against the prompt"));
		this.parameters.add(new FunctionParameter("threshold", "Double",
				"Number between 0-1 for the probability threshold to apply across the categories to reject a prompt. The larger the value, the higher the probability of the prompt containing the entity. The default value is "
						+ defaultThreshold));

		if (this.defaultLabels != null && !this.defaultLabels.isEmpty()) {
			this.requiredParameters = new ArrayList<>(Arrays.asList("labels"));
		} else {
			this.requiredParameters = new ArrayList<>(Arrays.asList("prompt", "labels"));
		}
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		checkSocketStatus();
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		String prompt = keyValue.get(this.keysToGet[0]);
		if (prompt == null) {
			throw new IllegalArgumentException("No prompt has been defined");
		}
		List<String> labels = getNounAsStringList(ns, this.keysToGet[1]);
		if (labels == null || labels.isEmpty()) {
			labels = defaultLabels;
		}
		if (labels == null) {
			throw new IllegalArgumentException("No named entity recognition lables have been defined");
		}
		double threshold = this.defaultThreshold;
		if (keyValue.containsKey(this.keysToGet[2])) {
			threshold = Double.parseDouble(keyValue.get(this.keysToGet[2]));
		}

		String script = "model.predict_entities(\"\"\"" + prompt + "\"\"\", " + new Gson().toJson(labels) + ")";
		List<Map<String, Object>> predictions = (List<Map<String, Object>>) pyTranslator
				.runDirectPyNoCancelTrace(script);
		boolean pass = true;
		// collect the entities that breach the threshold so we can build a masked
		// variant of the prompt for interceptors that mask rather than block
		List<Map<String, Object>> flagged = new ArrayList<>();
		for (Map<String, Object> category : predictions) {
			// account if the type is return
			Object categoryScore = category.get("score");
			double score = 0;
			if (categoryScore instanceof Number) {
				score = ((Number) categoryScore).doubleValue();
			} else {
				score = Double.parseDouble(categoryScore + "");
			}

			if (score > threshold) {
				pass = false;
				flagged.add(category);
			}
		}

		// Build a masked copy of the prompt (each flagged entity span replaced with the
		// mask template). When nothing breaches the threshold this equals the original
		// prompt. The interceptor decides whether to use this (mask) or reject (block).
		String returnPrompt = buildMaskedPrompt(prompt, flagged);

		Map<String, Object> retValue = new HashMap<>();
		retValue.put("threshold", threshold);
		retValue.put("return", predictions);
		return new GuardrailNounMetadata(pass, returnPrompt, retValue);
	}

	/**
	 * Build a masked copy of the prompt where every flagged entity span is replaced
	 * with the configured mask template (default {@code [label]}). Spans are replaced
	 * from right to left so the character offsets returned by GLiNER stay valid as the
	 * string is rewritten. Overlapping spans are skipped defensively. When there are no
	 * flagged entities the original prompt is returned unchanged.
	 *
	 * @param prompt  the original prompt
	 * @param flagged the entity predictions (each a map with start/end/label) that
	 *                breached the threshold
	 * @return the masked prompt
	 */
	private String buildMaskedPrompt(String prompt, List<Map<String, Object>> flagged) {
		if (prompt == null || flagged == null || flagged.isEmpty()) {
			return prompt;
		}
		// sort a copy by start offset descending so right-to-left splicing keeps offsets valid
		List<Map<String, Object>> ordered = new ArrayList<>(flagged);
		ordered.sort((a, b) -> Integer.compare(getInt(b.get("start")), getInt(a.get("start"))));

		StringBuilder masked = new StringBuilder(prompt);
		// tracks the left edge of the last span we replaced; the next span must end at
		// or before this to be a non-overlapping, still-valid region of the original text
		int lastStart = prompt.length();
		for (Map<String, Object> entity : ordered) {
			int start = getInt(entity.get("start"));
			int end = getInt(entity.get("end"));
			if (start < 0 || start >= end || end > lastStart) {
				continue;
			}
			Object label = entity.get("label");
			String replacement = this.maskTemplate.replace("{label}", label == null ? "" : label.toString());
			masked.replace(start, end, replacement);
			lastStart = start;
		}
		return masked.toString();
	}

	/**
	 * Coerce a value returned from the python translator (Number, or a stringified
	 * number) into an int, returning -1 when it cannot be parsed.
	 *
	 * @param value the raw value
	 * @return the int value, or -1 if not parseable
	 */
	private static int getInt(Object value) {
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		if (value == null) {
			return -1;
		}
		try {
			return (int) Double.parseDouble(value.toString());
		} catch (NumberFormatException e) {
			return -1;
		}
	}

	@Override
	protected String getStartupScript() {
		// @formatter:off
		return "from gliner import GLiNER\n" 
				+ "model = GLiNER.from_pretrained(\"" + this.modelName + "\")";
		// @formatter:on
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_GLINER;
	}
}
