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
package prerna.reactor.algorithms;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Evaluates JSON Logic rules against provided data using the base Semoss Python
 * implementation.
 * 
 * JSON Logic is a declarative way to express complex rules that can be
 * serialized as JSON.
 * 
 * Usage: JsonLogic(rule=["{\">=\": [{\"var\": \"age\"}, 21]}"],
 * data=["{\"age\": 25}"])
 * Returns: true (no additional return)
 *
 * Failed rule: JsonLogic(rule=["{\">=\": [{\"var\": \"age\"}, 21]}"],
 * data=["{\"age\": 18}"])
 * Returns: false, with additional return "Condition failed for age: 18.0 >=
 * 21.0"
 */
public class JsonLogicReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JsonLogicReactor.class);

	public JsonLogicReactor() {
		this.keysToGet = new String[] { "rule", "data" };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String ruleJson = this.keyValue.get("rule");
		if (ruleJson == null || ruleJson.trim().isEmpty()) {
			throw new SemossPixelException("Rule parameter is required and cannot be empty");
		}

		String dataJson = this.keyValue.get("data");

		try {
			classLogger.info("Evaluating JSON Logic rule");
			Map<?, ?> evaluation = evaluateWithPython(ruleJson, dataJson);
			Object result = evaluation.get("result");
			classLogger.info("JSON Logic evaluation completed successfully");
			PixelDataType returnType = determineReturnType(result);
			NounMetadata noun = new NounMetadata(result, returnType);
			if (evaluation.get("reason") instanceof String) {
				noun.addAdditionalReturn(
						new NounMetadata(evaluation.get("reason"), PixelDataType.CONST_STRING));
			}
			return noun;
		} catch (com.google.gson.JsonSyntaxException e) {
			classLogger.error("Invalid JSON syntax in rule or data: {}", e.getMessage());
			throw new SemossPixelException("Invalid JSON syntax: " + e.getMessage(), e);
		} catch (Exception e) {
			classLogger.error("Error evaluating JSON Logic rule", e);
			throw new SemossPixelException("Failed to evaluate JSON Logic rule: " + e.getMessage(), e);
		}
	}

	/**
	 * Evaluates JSON Logic using the base Semoss Python implementation
	 * (utils/json_logic.py)
	 * 
	 * @param ruleJson The JSON Logic rule as a JSON string
	 * @param dataJson The data to evaluate against as a JSON string (can be null)
	 * @return The evaluation result
	 */
	private Map<?, ?> evaluateWithPython(String ruleJson, String dataJson) {
		try {
			PyTranslator pt = this.insight.getPyTranslator();

			// Build Python script to import and call the diagnostic evaluator
			StringBuilder script = new StringBuilder();
			script.append("from utils.json_logic import evaluate_json_with_reason\n");
			script.append("evaluate_json_with_reason(");
			script.append(PyUtils.determineStringType(ruleJson));
			script.append(", ");
			script.append(
					dataJson != null && !dataJson.trim().isEmpty() ? PyUtils.determineStringType(dataJson) : "None");
			script.append(")");

			// Execute the script and get the result
			Object pyResponse = pt.runScript(script.toString());

			if (pyResponse instanceof String) {
				String resultJson = (String) pyResponse;
				Object parsed = GSON.fromJson(resultJson, Object.class);
				if (parsed instanceof Map) {
					return (Map<?, ?>) parsed;
				}
			}

			throw new IllegalStateException("Python JSON Logic evaluator returned an invalid response");

		} catch (Exception e) {
			classLogger.error("Error calling Python JSON Logic evaluator", e);
			throw new SemossPixelException("Python evaluation failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Determines the appropriate PixelDataType based on the result object
	 */
	private PixelDataType determineReturnType(Object result) {
		if (result == null) {
			return PixelDataType.NULL_VALUE;
		} else if (result instanceof Boolean) {
			return PixelDataType.BOOLEAN;
		} else if (result instanceof Number) {
			return PixelDataType.CONST_DECIMAL;
		} else if (result instanceof String) {
			return PixelDataType.CONST_STRING;
		} else if (result instanceof java.util.Map) {
			return PixelDataType.MAP;
		} else if (result instanceof Iterable) {
			return PixelDataType.VECTOR;
		} else {
			return PixelDataType.CUSTOM_DATA_STRUCTURE;
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Evaluates JSON Logic rules against provided data. \
				JSON Logic provides a declarative way to express complex conditional logic that can be serialized as JSON. \
				Supports standard operations (comparisons, arithmetic, logic, arrays) plus Semoss extensions \
				(regex matching, fuzzy string comparison, date operations, type casting, and collection helpers).\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("rule")) {
			return "The JSON Logic rule to evaluate (as a JSON string)";
		} else if (key.equals("data")) {
			return "The data context to evaluate the rule against (as a JSON string, optional)";
		}
		return super.getDescriptionForKey(key);
	}

}
