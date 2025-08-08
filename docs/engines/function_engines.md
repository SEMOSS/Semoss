# `FUNCTION` Engines

Function engines in SEMOSS provide a powerful way to encapsulate custom logic, external API calls, or even other SEMOSS operations (like Reactors) as well-defined, callable functions. These are particularly useful for extending SEMOSS's capabilities, integrating with third-party services, and for use in advanced AI workflows, such as function calling with Large Language Models (LLMs). Function engines typically extend `prerna.engine.impl.function.AbstractFunctionEngine` and implement `prerna.engine.api.IFunctionEngine`.

## Core Concepts for Function Engines

### `prerna.engine.api.IFunctionEngine` Interface

This interface defines the contract for all function engines:

*   `execute(Map<String, Object> parameterValues)`: This is the primary method to invoke the function. It takes a map where keys are parameter names and values are the arguments for those parameters. The method returns an `Object` which is the result of the function's execution.
*   `getFunctionName()`: Returns the unique name of the function this engine represents.
*   `getFunctionDescription()`: Returns a human-readable description of what the function does.
*   `getParameters()`: Returns a `List<FunctionParameter>` objects. Each `FunctionParameter` (from `prerna.engine.impl.function.FunctionParameter`) describes an expected input parameter, including its name, type (e.g., "string", "number", "boolean", "object", "array"), and description.
*   `getRequiredParameters()`: Returns a `List<String>` of parameter names that are mandatory for the function execution.
*   `getFunctionDefintionJson()`: Generates a `org.json.JSONObject` that describes the function's signature (name, description, parameters, and required parameters) in a format often compatible with OpenAI's function/tool specification. This allows LLMs to understand how to call the function.
*   `buildOpenAIFunctionEngineToolMap()`: Returns a `Map<String, Object>` specifically structured for OpenAI's "tool" (function calling) definition.
*   `buildBedrockToolSpec()`: Returns a `Map<String, Object>` structured for AWS Bedrock's tool specification.

### `prerna.engine.impl.function.AbstractFunctionEngine` Class

This abstract class provides a base for concrete function engine implementations.

*   **Metadata Loading**: Its primary responsibility is to load the function's metadata (name, description, parameters, and required parameters) from the engine's SMSS properties file during the `open()` method.
    *   `FUNCTION_NAME`: Property key for the function's name.
    *   `FUNCTION_DESCRIPTION`: Property key for the description.
    *   `FUNCTION_PARAMETERS`: Property key for a JSON string that defines the list of `FunctionParameter` objects (each having `name`, `type`, `description`).
    *   `FUNCTION_REQUIRED_PARAMETERS`: Property key for a JSON string array listing the names of mandatory parameters.
*   **SMSS Configuration**: It expects the function's signature and any other necessary configurations to be defined within the `.smss` file.
*   **Secrets Integration**: Like other abstract engines, it integrates with `SecretsFactory` to load sensitive configurations if needed.

### Extending for a New Function Source

To create a new `FUNCTION` engine:

1.  **Implement `IFunctionEngine`**.
2.  **Extend `AbstractFunctionEngine`**: This is highly recommended to leverage the standardized loading of function metadata from SMSS properties.
3.  **Implement `execute(Map<String, Object> parameterValues)`**: This is the core method where the actual logic of your function resides.
    *   Retrieve and validate input parameters from the `parameterValues` map based on the definitions loaded from SMSS.
    *   Perform the function's action. This could involve:
        *   Calling an external REST API using an HTTP client.
        *   Executing a local script (e.g., Python, R) via `PyTranslator` or `RJavaTranslator`.
        *   Invoking other Java methods or SEMOSS Reactors.
        *   Performing complex calculations or data transformations.
    *   Return the result of the function as an `Object`. This result will be wrapped in `NounMetadata` by the calling Pixel/Reactor.
4.  **Define SMSS Properties**: In the `.smss` file for your new function engine:
    *   Specify `ENGINE_CLASS` as your new engine's class name.
    *   Define `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`.
    *   Define `FUNCTION_PARAMETERS` as a JSON string representing a list of objects, where each object has "name", "type" (e.g., "string", "number", "object", "array", "boolean"), and "description" keys.
    *   Define `FUNCTION_REQUIRED_PARAMETERS` as a JSON string array of required parameter names.
    *   Add any other custom properties your engine needs for its operation (e.g., API endpoints, script paths, default values).
5.  **Define `FunctionTypeEnum`**: Add a new value to `prerna.engine.api.FunctionTypeEnum` for your new function type if it represents a distinct category.

## Example Implementations

### `prerna.engine.impl.function.LocalPythonFunctionEngine`
*   **Purpose**: Enables the execution of specific functions within local Python scripts, making them callable as SEMOSS function engines.
*   **Implementation Highlights**:
    *   The `execute()` method typically uses `prerna.ds.py.PyTranslator` to construct and send a Python command to the Python TCP server. This command would import the specified script and call the target function with the provided parameters.
    *   It handles the translation of input parameters from the Java `Map` to a format suitable for the Python function and translates the Python function's return value back to a Java `Object`.
*   **SMSS Configuration**:
    *   `PYTHON_SCRIPT_PATH`: The file system path to the `.py` script.
    *   `PYTHON_FUNCTION_NAME`: The name of the function to call within the Python script.
    *   `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`, `FUNCTION_PARAMETERS`, `FUNCTION_REQUIRED_PARAMETERS` defining the function's signature as seen by SEMOSS.
    *   May also include Python environment details if not globally managed.

### `prerna.engine.impl.function.RESTFunctionEngine`
*   **Purpose**: Allows SEMOSS to make calls to external REST APIs and treat these API endpoints as callable functions.
*   **Implementation Highlights**:
    *   The `execute()` method would use an HTTP client library (e.g., Apache HttpClient, OkHttp) to construct an HTTP request (GET, POST, PUT, etc.).
    *   It maps the input `parameterValues` to HTTP query parameters, path parameters, request headers, or the request body (often JSON) based on how the function engine is configured in its SMSS file.
    *   Handles sending the request, receiving the response, and parsing the response body (e.g., from JSON into a Java Map or List).
*   **SMSS Configuration**:
    *   `API_ENDPOINT_URL`: The base URL of the REST API.
    *   `HTTP_METHOD`: The HTTP method to use (e.g., "GET", "POST").
    *   Authentication details: This could involve properties for API keys (e.g., `API_KEY_HEADER_NAME`, `API_KEY_VALUE`), OAuth2 token URLs and credentials, or other auth mechanisms.
    *   `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`, `FUNCTION_PARAMETERS` (defining how these map to the API request), `FUNCTION_REQUIRED_PARAMETERS`.
    *   Potentially, mappings for request/response content types.

### `prerna.engine.impl.function.AbstractReactorFunctionEngine`
*   **Purpose**: An interesting meta-engine that can wrap an existing SEMOSS Reactor, exposing its functionality as if it were a standard function engine.
*   **Implementation Highlights**:
    *   The `execute()` method would internally instantiate and invoke the specified Reactor, passing the function parameters as nouns to the Reactor.
    *   It translates the `NounMetadata` returned by the Reactor into a simple `Object` to fit the `IFunctionEngine.execute()` signature.
*   **SMSS Configuration**:
    *   `REACTOR_CLASS_NAME`: The fully qualified class name of the SEMOSS Reactor to wrap.
    *   `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`, `FUNCTION_PARAMETERS` (mapping to the Reactor's expected nouns), `FUNCTION_REQUIRED_PARAMETERS`.
```
