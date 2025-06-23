# GAAS Function Execution (`gaas_gpt_function.py`)

The `py/gaas_gpt_function.py` module provides the `FunctionEngine` class, which acts as a Python proxy to execute SEMOSS `FUNCTION` engines. This enables Python-based Generative AI Agent Services (GAAS) or other Python scripts to invoke predefined functions or tools that are managed by the SEMOSS backend.

## `FunctionEngine` Class

*   **Purpose**: The `FunctionEngine` class allows Python code to trigger the execution of a specific SEMOSS `FUNCTION` engine. These backend `FUNCTION` engines can encapsulate a wide range of operations, such as calling external APIs, running specific Python or R scripts, or even executing complex SEMOSS Pixel recipes. This class does not define the functions themselves but provides the means to call them.
*   **Inheritance**: It extends `gaas_server_proxy.ServerProxy`, which is responsible for the underlying communication with the SEMOSS Java backend.

### Initialization

The constructor `__init__(self, engine_id: str, insight_id: Optional[str] = None)`:
*   `engine_id` (str): **Required**. The ID of the target SEMOSS `FUNCTION` engine (an `IFunctionEngine` instance) that is configured in the Java backend. This ID specifies which function or tool will be executed.
*   `insight_id` (Optional[str]): The ID of the current insight. This can be used for context by the backend `FUNCTION` engine, such as accessing insight-specific variables or resources if the function is designed to do so.
*   The constructor asserts that `engine_id` is provided and prints an initialization message.

### Key Methods and Functionality

*   **`execute(self, parameterMap: dict, insight_id: Optional[str] = None) -> Any`**:
    *   **Purpose**: This is the primary method to call the configured SEMOSS `FUNCTION` engine.
    *   **Inputs**:
        *   `parameterMap` (dict): A Python dictionary where keys are the parameter names expected by the backend `FUNCTION` engine, and values are the arguments for those parameters. This map is serialized to JSON when constructing the Pixel command.
        *   `insight_id` (Optional[str]): Overrides the instance's `insight_id` if provided, passing this specific insight context to the backend.
    *   **Core Logic**:
        1.  Generates a unique `epoc` ID for the transaction using `super().get_next_epoc()`.
        2.  Constructs a Pixel script string: `ExecuteFunctionEngine(engine = "<engine_id>", map=[<json_serialized_parameterMap>]);`
            *   The `engine_id` is the ID of the `FUNCTION` engine to be executed.
            *   The `parameterMap` is converted into a JSON string.
        3.  Calls `super().callReactor(...)` to send this Pixel script to the SEMOSS Java backend for execution. The `callReactor` method (from `ServerProxy`) handles the communication.
    *   **Outputs**:
        *   The method returns the `"output"` field from the first `pixelReturn` structure returned by the `callReactor` method. The nature and format of this output are determined by the specific backend `FUNCTION` engine that was executed. It could be a simple value, a JSON string, a list, a dictionary, or any data type that the backend function returns and can be serialized.
        *   If the Pixel execution does not return the expected structure, it might return `None` or the raw `pixelReturn` object.

### Interaction with SEMOSS `FUNCTION` Engines

*   The `FunctionEngine` Python class is a client or proxy to a `FUNCTION` engine (an implementation of `prerna.engine.api.IFunctionEngine`) that is already defined and configured within the SEMOSS Java backend.
*   The actual logic of the function (e.g., calling an external API, running a script, performing a calculation) resides in the Java implementation of that backend `FUNCTION` engine.
*   The `parameterMap` provided to the `execute` method is passed to the backend `IFunctionEngine`'s `execute(Map<String, Object> parameterValues)` method.

### Error Handling

*   The constructor asserts that `engine_id` is provided.
*   Errors related to the communication with the SEMOSS backend would be handled by the `ServerProxy` superclass.
*   Errors originating from the execution of the `FUNCTION` engine on the backend (e.g., issues within the function's logic, API call failures) would typically be propagated back through the `pixelReturn` structure, potentially as an error message within the output or by raising an exception if the `callReactor` method is designed to do so for certain error types.

### Example Usage (Conceptual)

```python
# Assuming gaas_server_proxy is configured and SEMOSS backend is running
# And a FUNCTION engine with ID "my_custom_api_caller" is configured in SEMOSS

function_engine_id = "my_custom_api_caller"
insight_context_id = "some_active_insight_id" # Optional

# Initialize the FunctionEngine client
function_tool = FunctionEngine(engine_id=function_engine_id, insight_id=insight_context_id)

# Define parameters for the backend FUNCTION engine
# This depends on what parameters "my_custom_api_caller" expects
params_for_function = {
    "api_endpoint_param": "users/123",
    "http_method": "GET",
    "query_params": {"include_details": True}
}

try:
    # Execute the function
    result = function_tool.execute(parameterMap=params_for_function)

    if result is not None:
        print("Function execution successful. Result:")
        print(result)
    else:
        print("Function execution might have failed or returned no specific output.")
except Exception as e:
    print(f"Error executing function engine: {e}")

```

This `FunctionEngine` class provides a straightforward way for Python-based GAAS components to leverage the extensible functionality offered by SEMOSS `FUNCTION` engines, enabling agents to perform a wide variety of pre-defined actions and tools managed by the core platform.
