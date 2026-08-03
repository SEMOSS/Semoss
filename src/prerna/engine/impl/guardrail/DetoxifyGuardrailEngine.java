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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.impl.function.FunctionParameter;
import prerna.engine.impl.model.AbstractPythonModelEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;

public class DetoxifyGuardrailEngine extends AbstractPythonGuardrailReactorFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractPythonModelEngine.class);

	private static final String DEFAULT_THRESHOLD_KEY = "DEFAULT_THRESHOLD";
	private Double defaultThreshold = .7;

	public DetoxifyGuardrailEngine() {
		this.keysToGet = new String[] { "prompt", "threshold" };
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String defaultThresholdStr = this.smssProp.getProperty(DEFAULT_THRESHOLD_KEY);
		if (defaultThresholdStr != null && !(defaultThresholdStr = defaultThresholdStr.trim()).isEmpty()) {
			try {
				defaultThreshold = Double.parseDouble(defaultThresholdStr);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid default threshold value " + defaultThresholdStr
						+ ". Revert to default value of " + defaultThreshold, e);
			}
		}

		this.functionDescription = "Applying toxicity analysis on the following categoires ['toxicity', 'severe_toxicity', 'obscene', 'threat', 'insult', 'identity_attack']";
		this.parameters = new ArrayList<>();
		this.parameters
				.add(new FunctionParameter("prompt", "String", "This is the prompt we are applying the guardrail to"));
		this.parameters.add(new FunctionParameter("threshold", "Double",
				"Number between 0-1 for the probability threshold to apply across the categories to reject a prompt. The larger the value, the higher the probability of the prompt containing the category. The default value is "
						+ defaultThreshold));
		this.requiredParameters = new ArrayList<>(Arrays.asList("prompt"));
	}

	@Override
	public GuardrailNounMetadata execute(NounStore ns, GenRowStruct curRow) {
		checkSocketStatus();
		Map<String, String> keyValue = organizeKeys(ns, curRow);
		String prompt = keyValue.get(this.keysToGet[0]);
		double threshold = this.defaultThreshold;
		if (keyValue.containsKey("threshold")) {
			threshold = Double.parseDouble(keyValue.get("threshold"));
		}
		String script = "model.predict(\"\"\"" + prompt + "\"\"\")";
		Map<String, Object> value = (Map<String, Object>) pyTranslator.runDirectPyNoCancelTrace(script);

		boolean pass = true;
		for (String category : value.keySet()) {
			// account if the type is return
			Object categoryScore = value.get(category);
			double score = 0;
			if (categoryScore instanceof Number) {
				score = ((Number) categoryScore).doubleValue();
			} else {
				score = Double.parseDouble(categoryScore + "");
			}

			if (score > threshold) {
				pass = false;
			}
		}

		Map<String, Object> retValue = new HashMap<>();
		retValue.put("threshold", threshold);
		retValue.put("return", value);
		return new GuardrailNounMetadata(pass, prompt, retValue);
	}

	@Override
	protected String getStartupScript() {
		// @formatter:off
		return "from detoxify import Detoxify\n" 
				+ "model = Detoxify('original')";
		// @formatter:on
	}

	@Override
	public GuardrailTypeEnum getGuardrailType() {
		return GuardrailTypeEnum.EMBEDDED_DETOXIFY;
	}
}
