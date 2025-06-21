# Model Limits (`model_limits.py`)

The `py/genai_client/model_limits.py` file is responsible for defining and managing operational limits for various language models, primarily focusing on context window size and maximum completion tokens. This is crucial for ensuring that prompts do not exceed model capabilities and for controlling the length of generated responses.

## Key Components

*   **`IndividualModelLimits(pydantic.BaseModel)`**:
    *   A Pydantic data model used to represent the limits for a single model.
    *   Attributes:
        *   `context_window` (int): The maximum number of tokens the model can consider from the input prompt (including history and current input).
        *   `max_completion_tokens` (int): The maximum number of tokens the model can generate in a single response.

*   **`MODEL_LIMITS_CONFIG` (dict)**:
    *   A dictionary that hardcodes `IndividualModelLimits` for a predefined set of popular models.
    *   Keys are model name strings (e.g., "gpt-4o", "gpt-3.5-turbo", "meta-llama/Meta-Llama-3.1-8B-Instruct").
    *   Values are dictionaries specifying `{"context_window": <value>, "max_completion_tokens": <value>}`.
    *   Example entry: `"gpt-4-turbo": {"context_window": 128000, "max_completion_tokens": 4096}`

*   **`FALLBACK_CONFIG` (dict)**:
    *   Defines default `context_window` and `max_completion_tokens` to be used if a requested model is not found in `MODEL_LIMITS_CONFIG`.
    *   Example: `{"context_window": 8192, "max_completion_tokens": 2048}`

*   **`get_model_limits(model_name: str) -> IndividualModelLimits`**:
    *   A utility function that retrieves the `IndividualModelLimits` for a given `model_name`.
    *   It first checks `MODEL_LIMITS_CONFIG`. If the model is not found, it returns limits based on `FALLBACK_CONFIG`.

*   **`OPENAI_MODELS` (List[str])**:
    *   A predefined list of model identifiers, primarily for OpenAI models. Its specific usage within this module might be for validation or categorization elsewhere in the `genai_client`.

*   **`ModelLimits` Class**:
    *   This class provides a mechanism to determine the effective context window and maximum completion tokens for a given model by considering multiple potential sources of configuration in a specific order of precedence.
    *   **Constructor `__init__(...)`**:
        *   Takes `model_name` as a required argument.
        *   Accepts optional arguments for limits that might come from:
            *   Dynamic call parameters (e.g., passed during a specific API call to generate text): `max_tokens_call_param`, `max_completion_tokens_call_param`.
            *   SEMOSS system/engine configuration (SMSS properties): `context_window_smss`, `max_tokens_smss`, `max_completion_tokens_smss`.
    *   **`_resolve_model_limits(...)` Method**: This private method implements the logic to determine the final limits:
        1.  It starts with the hardcoded limits for the `model_name` from `MODEL_LIMITS_CONFIG` (or `FALLBACK_CONFIG`).
        2.  **Max Completion Tokens**:
            *   It prioritizes `max_completion_tokens_call_param` if provided.
            *   Then `max_tokens_call_param` (seems like an alternative name for the same concept).
            *   Then `max_completion_tokens_smss` if provided.
            *   Then `max_tokens_smss`.
            *   Finally, defaults to the value from the model's config (`MODEL_LIMITS_CONFIG` or `FALLBACK_CONFIG`).
        3.  **Context Window**:
            *   It prioritizes `context_window_smss` if provided.
            *   Otherwise, it defaults to the value from the model's config.
    *   **Instance Attributes**:
        *   `self.context_window` (int): The resolved context window size.
        *   `self.max_completion_tokens` (int): The resolved maximum number of tokens for the model's response.

## Usage Example (Conceptual)

```python
# from py.genai_client.model_limits import ModelLimits

# # Example: Limits defined only by the internal config
# limits_gpt4_default = ModelLimits(model_name="gpt-4")
# print(f"GPT-4 Default Context: {limits_gpt4_default.context_window}, Max Completion: {limits_gpt4_default.max_completion_tokens}")

# # Example: Overriding with SMSS-level configurations
# limits_gpt4_smss = ModelLimits(model_name="gpt-4", context_window_smss=4096, max_completion_tokens_smss=1024)
# print(f"GPT-4 SMSS Context: {limits_gpt4_smss.context_window}, Max Completion: {limits_gpt4_smss.max_completion_tokens}")

# # Example: Overriding with call-specific parameters (highest precedence for max_completion_tokens)
# limits_gpt4_call = ModelLimits(model_name="gpt-4",
#                                context_window_smss=4096, # SMSS value
#                                max_completion_tokens_call_param=500) # Call parameter
# print(f"GPT-4 Call Context: {limits_gpt4_call.context_window}, Max Completion: {limits_gpt4_call.max_completion_tokens}")
```

This system allows SEMOSS to manage LLM token limits flexibly, providing global defaults, system-level overrides via SMSS configurations, and fine-grained control through parameters passed during specific model invocations. This is essential for preventing errors from exceeding model token capacities and for managing operational costs.
