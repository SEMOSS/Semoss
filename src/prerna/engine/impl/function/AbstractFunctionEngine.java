/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.function;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.AbstractEngine;

public abstract class AbstractFunctionEngine extends AbstractEngine implements IFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractFunctionEngine.class);

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
			this.parameters = new Gson().fromJson(smssProp.getProperty(IFunctionEngine.PARAMETER_KEY),
					new TypeToken<List<FunctionParameter>>() {
					}.getType());
		}

		if (smssProp.containsKey(IFunctionEngine.REQUIRED_PARAMETER_KEY)) {
			this.requiredParameters = new Gson().fromJson(smssProp.getProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY),
					new TypeToken<List<String>>() {
					}.getType());
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
	public Map<String, Object> buildOpenAIFunctionEngineToolMap() {
		// Fetch metadata for the engine
		Map<String, Object> metadata = SecurityEngineUtils.getAggregateEngineMetadata(this.getEngineId(),
				Arrays.asList("description"), true);

		// Extract the description from metadata
		String description = (String) metadata.get("description");
		if (description == null) {
			description = "No description available.";
		}

		// Create the main map
		Map<String, Object> toolMap = new HashMap<>();
		toolMap.put("type", "function");

		// Create the function map
		Map<String, Object> functionMap = new HashMap<>();
		functionMap.put("name", "function_engine");
		functionMap.put("description", description);

		// Create the parameters map
		Map<String, Object> parametersMap = new HashMap<>();
		parametersMap.put("type", "object");

		// Create the properties map
		Map<String, Object> propertiesMap = new HashMap<>();

		// Add the id property
		Map<String, Object> idMap = new HashMap<>();
		idMap.put("type", "string");
		idMap.put("description", "The unique identifier for this function_engine used to call this specific engine");
		idMap.put("enum", Arrays.asList(this.getEngineId()));
		propertiesMap.put("id", idMap);

		// Add the map property
		Map<String, Object> mapMap = new HashMap<>();
		mapMap.put("type", "object");

		// Create the map properties map
		Map<String, Object> mapPropertiesMap = new HashMap<>();
		for (FunctionParameter param : this.getParameters()) {
			Map<String, Object> paramMap = new HashMap<>();
			paramMap.put("type", param.getParameterType().toLowerCase());
			paramMap.put("description", param.getParameterDescription());
			mapPropertiesMap.put(param.getParameterName(), paramMap);
		}
		mapMap.put("properties", mapPropertiesMap);
		mapMap.put("required", this.getRequiredParameters());
		mapMap.put("description", "A map containing the parameters to pass into the function_engine call.");

		propertiesMap.put("map", mapMap);

		// Finalize parameters map
		parametersMap.put("properties", propertiesMap);
		parametersMap.put("required", Arrays.asList("id", "map"));

		// Add parameters to function map
		functionMap.put("parameters", parametersMap);

		// Add function map to main map
		toolMap.put("function", functionMap);

		return toolMap;
	}

	@Override
	public Map<String, Object> buildBedrockToolSpec() {
		// Fetch metadata/description
		Map<String, Object> metadata = SecurityEngineUtils.getAggregateEngineMetadata(this.getEngineId(),
				Arrays.asList("description"), true);
		String description = (String) metadata.get("description");
		if (description == null) {
			description = "No description available.";
		}

		// Build properties for schema
		Map<String, Object> propertiesMap = new HashMap<>();
		for (FunctionParameter param : this.getParameters()) {
			Map<String, Object> property = new HashMap<>();
			property.put("type", param.getParameterType().toLowerCase());
			property.put("description", param.getParameterDescription());
			propertiesMap.put(param.getParameterName(), property);
		}

		// Build inputSchema.json
		Map<String, Object> inputSchemaJson = new HashMap<>();
		inputSchemaJson.put("type", "object");
		inputSchemaJson.put("properties", propertiesMap);
		inputSchemaJson.put("required", this.getRequiredParameters());

		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("json", inputSchemaJson);

		// toolSpec map (this is what you want to return)
		Map<String, Object> toolSpecMap = new HashMap<>();
		toolSpecMap.put("name", this.getEngineId()); // or assign function/tool name you want
		toolSpecMap.put("description", description);
		toolSpecMap.put("inputSchema", inputSchema);

		// Wrap as {"toolSpec": ...}
		Map<String, Object> wrapper = new HashMap<>();
		wrapper.put("toolSpec", toolSpecMap);

		return wrapper;
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

	@Deprecated
	/** Will be deleted for buildOpenAIFunctionEngineToolMap */
	public Map<String, Object> buildFunctionEngineToolMap() {
		return buildOpenAIFunctionEngineToolMap();
	}
}
