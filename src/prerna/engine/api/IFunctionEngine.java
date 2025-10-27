package prerna.engine.api;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.function.FunctionParameter;

/**
 * Interface for function engines that provide executable operations within the SEMOSS platform.
 * 
 * <p>Function engines encapsulate reusable computational logic that can be executed
 * with parameters and return results. They extend the base {@link IEngine} interface
 * to provide standardized function execution capabilities across different programming
 * languages and execution environments.</p>
 * 
 * <p>Key capabilities include:</p>
 * <ul>
 *   <li><strong>Function Execution:</strong> Execute functions with typed parameters</li>
 *   <li><strong>Parameter Management:</strong> Define and validate function parameters</li>
 *   <li><strong>Metadata Exposure:</strong> Provide function names, descriptions, and signatures</li>
 *   <li><strong>Type Safety:</strong> Support parameter validation and type checking</li>
 * </ul>
 * 
 * <p>Function engines support various execution environments including:</p>
 * <ul>
 *   <li>Local Python execution</li>
 *   <li>REST API function calls</li>
 *   <li>Custom embedding generation</li>
 *   <li>OCR and document processing</li>
 *   <li>Audio transcription services</li>
 * </ul>
 * 
 * @see {@link IEngine} for base engine functionality
 * @see {@link FunctionParameter} for parameter definitions
 * @see {@link FunctionTypeEnum} for available function types
 * @see {@link ICustomEmbeddingsFunctionEngine} for specialized embedding functions
 * @author SEMOSS
 */
public interface IFunctionEngine extends IEngine {

	/** Configuration key for the function type used in engine initialization */
	String FUNCTION_TYPE = "FUNCTION_TYPE";

	/** Configuration key for the function name */
	String NAME_KEY = "FUNCTION_NAME";
	
	/** Configuration key for the function description */
	String DESCRIPTION_KEY = "FUNCTION_DESCRIPTION";
	
	/** Configuration key for function parameters list */
	String PARAMETER_KEY = "FUNCTION_PARAMETERS";
	
	/** Configuration key for required parameters list */
	String REQUIRED_PARAMETER_KEY = "FUNCTION_REQUIRED_PARAMETERS";
	
	/** Configuration key for Python file name in Python-based function engines */
	String PYTHON_FILE_NAME = "PYTHON_FILE_NAME";

	/**
	 * Executes the function with the provided parameter values.
	 * 
	 * <p>This method performs the core computational logic of the function
	 * using the provided parameters. Parameter validation should be performed
	 * to ensure required parameters are present and types are correct.</p>
	 * 
	 * @param parameterValues Map of parameter names to their values for execution
	 * @return The result of the function execution, type depends on the specific function
	 * @throws RuntimeException If parameter validation fails or execution encounters errors
	 */
	Object execute(Map<String, Object> parameterValues);

	/**
	 * Gets the unique name identifier for this function.
	 * 
	 * <p>The function name serves as a unique identifier for the function
	 * within the SEMOSS platform and is used for function discovery and invocation.</p>
	 * 
	 * @return The unique function name
	 */
	String getFunctionName();

	/**
	 * Sets the unique name identifier for this function.
	 * 
	 * @param functionName The unique function name to set
	 */
	void setFunctionName(String functionName);

	/**
	 * Gets the human-readable description of what this function does.
	 * 
	 * <p>The description should clearly explain the function's purpose,
	 * behavior, and expected outcomes to help users understand when
	 * and how to use the function.</p>
	 * 
	 * @return The function description text
	 */
	String getFunctionDescription();

	/**
	 * Sets the human-readable description for this function.
	 * 
	 * @param description The descriptive text explaining the function's purpose
	 */
	void setFunctionDescription(String description);

	/**
	 * Gets the list of parameters that this function accepts.
	 * 
	 * <p>This method returns the complete parameter definitions including
	 * parameter names, types, default values, and validation constraints.</p>
	 * 
	 * @return List of {@link FunctionParameter} objects defining the function signature
	 * @see {@link FunctionParameter} for parameter structure
	 */
	List<FunctionParameter> getParameters();

	/**
	 * Sets the list of parameters that this function accepts.
	 * 
	 * @param parameters List of {@link FunctionParameter} objects defining the function signature
	 * @see {@link FunctionParameter} for parameter structure
	 */
	void setParameters(List<FunctionParameter> parameters);

	/**
	 * Gets the list of parameter names that are required for function execution.
	 * 
	 * <p>Required parameters must be provided when executing the function,
	 * while optional parameters may use default values if not specified.</p>
	 * 
	 * @return List of required parameter names
	 */
	List<String> getRequiredParameters();

	/**
	 * Sets the list of parameter names that are required for function execution.
	 * 
	 * @param requiredParameters List of parameter names that must be provided
	 */
	void setRequiredParameters(List<String> requiredParameters);

	/**
	 * Gets the function definition as a JSON object for API exposure.
	 * 
	 * <p>This method returns a structured JSON representation of the function
	 * including its name, description, parameters, and other metadata. This
	 * format is suitable for API documentation and client-side function discovery.</p>
	 * 
	 * @return JSON object containing the complete function definition
	 */
	org.json.JSONObject getFunctionDefintionJson();

}
