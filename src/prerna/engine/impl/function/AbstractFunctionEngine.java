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
package prerna.engine.impl.function;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.Strictness;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.AbstractEngine;

public abstract class AbstractFunctionEngine extends AbstractEngine implements IFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractFunctionEngine.class);

	protected Gson gson = new GsonBuilder().setStrictness(Strictness.LENIENT).create();

	protected String functionName;
	protected String functionDescription;
	protected List<FunctionParameter> parameters;
	protected List<String> requiredParameters;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		if (!smssProp.containsKey(IFunctionEngine.NAME_KEY)) {
			throw new IllegalArgumentException("Must have key " + IFunctionEngine.NAME_KEY + " in SMSS");
		}
		if (!smssProp.containsKey(IFunctionEngine.DESCRIPTION_KEY)) {
			throw new IllegalArgumentException("Must have key " + IFunctionEngine.DESCRIPTION_KEY + " in SMSS");
		}

		this.functionName = smssProp.getProperty(IFunctionEngine.NAME_KEY);
		this.functionDescription = smssProp.getProperty(IFunctionEngine.DESCRIPTION_KEY);

		if (smssProp.containsKey(IFunctionEngine.PARAMETER_KEY)) {
			try {
				this.parameters = gson.fromJson(smssProp.getProperty(IFunctionEngine.PARAMETER_KEY),
						new TypeToken<List<FunctionParameter>>() {
						}.getType());
			} catch (Exception e) {
				classLogger.error("Invalid json format for {} key, value used {}", IFunctionEngine.PARAMETER_KEY,
						smssProp.getProperty(IFunctionEngine.PARAMETER_KEY));
			}
		}

		if (smssProp.containsKey(IFunctionEngine.REQUIRED_PARAMETER_KEY)) {
			try {
				this.requiredParameters = gson.fromJson(smssProp.getProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY),
						new TypeToken<List<String>>() {
						}.getType());
			} catch (Exception e) {
				classLogger.error("Invalid json format for {} key, value used {}",
						IFunctionEngine.REQUIRED_PARAMETER_KEY,
						smssProp.getProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY));
			}
		}
	}

	/**
	 * Throw when a caller left out a parameter this engine declared as required.
	 *
	 * <p>
	 * Every implementation of {@link #execute(Map)} owes its caller this check, and
	 * {@link #requiredParameters} is owned here, so the check belongs here too
	 * rather than being restated by each engine.
	 *
	 * @param parameterValues the runtime parameters for this call
	 */
	protected void validateRequiredParameters(Map<String, Object> parameterValues) {
		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			return;
		}
		Set<String> missingPs = new HashSet<>();
		for (String requiredP : this.requiredParameters) {
			if (parameterValues == null || !parameterValues.containsKey(requiredP)) {
				missingPs.add(requiredP);
			}
		}
		if (!missingPs.isEmpty()) {
			throw new IllegalArgumentException("Must define required keys = " + missingPs);
		}
	}

	/**
	 * Build the trailing sentence that tells a caller what value is used when they
	 * leave a parameter out. Used when composing the parameter descriptions that
	 * {@link #getFunctionDefintionJson()} publishes.
	 *
	 * @param defaultValue the default this engine was opened with
	 * @return the sentence to append, or an empty string when there is no default
	 */
	protected static String defaultText(String defaultValue) {
		if (defaultValue == null || defaultValue.isEmpty()) {
			return "";
		}
		return " Defaults to " + defaultValue + ".";
	}

	/**
	 * Pull a runtime parameter as a trimmed string, falling back to a default when
	 * it is missing or blank.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @param key             the parameter to read
	 * @param defaultValue    value to use when the parameter is not set
	 * @return the parameter value as a string or the default
	 */
	protected static String getParameterValue(Map<String, Object> parameterValues, String key, String defaultValue) {
		Object value = parameterValues == null ? null : parameterValues.get(key);
		if (value == null) {
			return defaultValue;
		}
		String stringValue = value.toString().trim();
		if (stringValue.isEmpty()) {
			return defaultValue;
		}
		return stringValue;
	}

	/**
	 * Pull a runtime parameter as an int. A value that is not a number is logged
	 * and the default used, since a model passing "five" should still get a result
	 * back rather than an error.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @param key             the parameter to read
	 * @param defaultValue    value to use when the parameter is not set or not a
	 *                        number
	 * @return the parameter value as an int or the default
	 */
	protected static int getIntParameterValue(Map<String, Object> parameterValues, String key, int defaultValue) {
		Object value = parameterValues == null ? null : parameterValues.get(key);
		if (value == null) {
			return defaultValue;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		String stringValue = value.toString().trim();
		if (stringValue.isEmpty()) {
			return defaultValue;
		}
		try {
			// a model routinely sends an integer as "5" or even "5.0"
			return (int) Double.parseDouble(stringValue);
		} catch (NumberFormatException e) {
			classLogger.warn("Invalid number '{}' for the {} parameter, using {} instead", stringValue, key,
					defaultValue);
			return defaultValue;
		}
	}

	@Override
	public JSONObject getFunctionDefintionJson() {
		JSONObject json = new JSONObject();
		json.put("name", this.functionName);
		json.put("description", this.functionDescription);

		JSONObject parameterJSON = new JSONObject();
		if (this.parameters != null && !this.parameters.isEmpty()) {
			parameterJSON.put("type", "object");
			JSONObject propertiesJSON = new JSONObject();
			for (FunctionParameter fParam : this.parameters) {
				JSONObject thisPropJSON = new JSONObject();
				thisPropJSON.put("type", fParam.getParameterType());
				thisPropJSON.put("description", fParam.getParameterDescription());
				propertiesJSON.put(fParam.getParameterName(), thisPropJSON);
			}
			parameterJSON.put("properties", propertiesJSON);
		}
		json.put("parameters", parameterJSON);

		JSONArray requiredJSON = new JSONArray();
		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			requiredJSON.put(this.requiredParameters);
		}
		json.put("required", requiredJSON);

		return json;
	}

	@Override
	public String getFunctionName() {
		return functionName;
	}

	@Override
	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	@Override
	public String getFunctionDescription() {
		return functionDescription;
	}

	@Override
	public void setFunctionDescription(String functionDescription) {
		this.functionDescription = functionDescription;
	}

	@Override
	public List<FunctionParameter> getParameters() {
		return parameters;
	}

	@Override
	public void setParameters(List<FunctionParameter> parameters) {
		this.parameters = parameters;
	}

	@Override
	public List<String> getRequiredParameters() {
		return this.requiredParameters;
	}

	@Override
	public void setRequiredParameters(List<String> requiredParameters) {
		this.requiredParameters = requiredParameters;
	}

	@Override
	public CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.FUNCTION;
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

}
