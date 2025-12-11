package prerna.engine.api;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.engine.impl.function.FunctionParameter;
import prerna.logging.IgnoreEngineLogging;

public interface IFunctionEngine extends IEngine {

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String FUNCTION_TYPE = "FUNCTION_TYPE";

	String NAME_KEY = "FUNCTION_NAME";
	String DESCRIPTION_KEY = "FUNCTION_DESCRIPTION";
	String PARAMETER_KEY = "FUNCTION_PARAMETERS";
	String REQUIRED_PARAMETER_KEY = "FUNCTION_REQUIRED_PARAMETERS";
	String PYTHON_FILE_NAME = "PYTHON_FILE_NAME";

	/**
	 * 
	 * @param args
	 * @return
	 */
	Object execute(Map<String, Object> parameterValues);

	/**
	 * Unique name of the function
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getFunctionName();

	/**
	 * 
	 */
	@IgnoreEngineLogging
	void setFunctionName(String functionName);

	/**
	 * Description of what this function does
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getFunctionDescription();

	/**
	 * 
	 * @param description
	 */
	@IgnoreEngineLogging
	void setFunctionDescription(String description);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	List<FunctionParameter> getParameters();

	/**
	 * 
	 * @param parameters
	 */
	@IgnoreEngineLogging
	void setParameters(List<FunctionParameter> parameters);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	List<String> getRequiredParameters();

	/**
	 * 
	 * @param requiredParameters
	 */
	@IgnoreEngineLogging
	void setRequiredParameters(List<String> requiredParameters);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	JSONObject getFunctionDefintionJson();

}
