package prerna.engine.impl.function;

import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.AbstractEngine;

/**
 * Abstract base class providing common functionality for function engines in the SEMOSS platform.
 * 
 * <p>This class extends {@link AbstractEngine} and implements {@link IFunctionEngine} to provide
 * a comprehensive foundation for all function engine implementations. It handles the common
 * aspects of function management including parameter validation, metadata management, and
 * configuration parsing from SMSS properties.</p>
 * 
 * <p>Key responsibilities include:</p>
 * <ul>
 *   <li><strong>Function Metadata:</strong> Managing function names, descriptions, and signatures</li>
 *   <li><strong>Parameter Management:</strong> Handling parameter definitions and validation</li>
 *   <li><strong>Configuration Loading:</strong> Parsing function definitions from SMSS files</li>
 *   <li><strong>JSON Serialization:</strong> Converting function definitions to JSON format</li>
 * </ul>
 * 
 * <p>Subclasses must implement the {@link #execute(java.util.Map)} method to provide
 * specific function execution logic. Common function engine types include:</p>
 * <ul>
 *   <li>Python function execution engines</li>
 *   <li>REST API function engines</li>
 *   <li>Custom embedding generation engines</li>
 *   <li>OCR and document processing engines</li>
 *   <li>Audio transcription engines</li>
 * </ul>
 * 
 * @see {@link AbstractEngine} for base engine functionality
 * @see {@link IFunctionEngine} for function engine interface
 * @see {@link FunctionParameter} for parameter definitions
 * @author SEMOSS
 */
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
