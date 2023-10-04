package prerna.engine.api;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.function.FunctionParameter;

public interface IFunctionEngine extends IEngine {

	String NAME_KEY = "FUNCTION_NAME";
	String DESCRIPTION_KEY = "FUNCTION_DESCRIPTION";
	String PARAMETER_KEY = "FUNCTION_PARAMETERS";
	String REQUIRED_PARAMETER_KEY = "FUNCTION_REQUIRED_PARAMETERS";

	/**
	 * 
	 * @param args
	 * @return
	 */
	Object execute(Map<String, Object> parameterValues);
	
	/**
	 * Unique name of the function
	 * @return
	 */
	String getFunctionName();
	
	/**
	 * 
	 */
	void setFunctionName(String functionName);
	
	/**
	 * Description of what this function does
	 * @return
	 */
	String getFunctionDescription();
	
	/**
	 * 
	 * @param description
	 */
	void setFunctionDescription(String description);
	
	/**
	 * 
	 * @return
	 */
	List<FunctionParameter> getParameters();
	
	/**
	 * 
	 * @param parameters
	 */
	void setParameters(List<FunctionParameter> parameters);
	
	/**
	 * 
	 * @return
	 */
	List<String> getRequiredParameters();
	
	/**
	 * 
	 * @param requiredParameters
	 */
	void setRequiredParameters(List<String> requiredParameters);
	
	/**
	 * 
	 * @return
	 */
	org.json.JSONObject getFunctionDefintionJson();
	
}
