# OpenAI and Azure OpenAI Text Generation Clients

This document covers the clients designed to interact with OpenAI's language models, both directly via the OpenAI API and through Microsoft's Azure OpenAI Service.

## `OpenAITextGenerationClient` (from `openai_chat_completion_client.py`)

This class provides a client for OpenAI's chat completion models (e.g., GPT-3.5 Turbo, GPT-4).

*   **Purpose**: It acts as an interface to OpenAI's chat models, handling request construction, API interaction, and response parsing. It standardizes these operations within the SEMOSS `genai_client` framework.
*   **Relationship to Framework**: It extends `AbstractOpenAiClient` (which likely extends `AbstractTextGenerationClient`), inheriting common OpenAI client setup, template management, and model limit considerations. It implements the core `ask_call()` via its `Chat` operation delegate.

### Initialization

The `OpenAITextGenerationClient` (via its parent `AbstractOpenAiClient`) is initialized with:
*   `api_key` (str): The OpenAI API key.
*   `model_name` (str): The specific OpenAI model ID (e.g., "gpt-4", "gpt-3.5-turbo").
*   `base_url` (Optional[str]): For OpenAI-compatible APIs that are not hosted by OpenAI (e.g., local vLLM server, NVIDIA NIMs). If provided, `api_key` might be set to "EMPTY".
*   `timeout` (Optional[float]): Request timeout.
*   `max_retries` (Optional[int]): Number of retries for API calls.
*   `model_type` (Optional[str]): Can be "OPEN_AI" or "VLLM" to adjust for minor API differences, especially for structured output/JSON mode.
*   `use_max_tokens_param` (Optional[bool]): If true, uses `max_tokens` in requests; otherwise, uses `max_completion_tokens`. Defaults to `False`.
*   `**kwargs`: Passed to `AbstractTextGenerationClient` for template and model limit setup.

The constructor initializes `openai.OpenAI` client and instantiates `Instruct` and `Chat` operation classes.

### Key Methods and Functionality

*   **`ask_call(self, **kwargs) -> AskModelEngineResponse`**:
    *   This method is the primary interface for chat-like interactions.
    *   It delegates the actual API call to `self.chat_operation.ask(**kwargs)`.
    *   The `Chat.ask()` method (detailed under `AbstractOpenAiClient` documentation, but core logic resides in `inference_call`) handles:
        *   Preparing the `messages` payload in the format OpenAI expects (list of dictionaries with "role" and "content").
        *   Managing various parameters like `temperature`, `max_tokens` (or `max_completion_tokens`), `top_p`, `stream`, `tools`, `tool_choice`.
        *   Calling `self.client.chat.completions.create()`.
        *   Handling streaming responses by iterating through chunks and concatenating content.
        *   Parsing non-streaming responses, including handling `tool_calls` if present.
        *   Returning an `AskModelEngineResponse` with the text response, token counts, and message type ("CHAT" or "TOOL").

*   **`inference_call(self, prefix: str, **kwargs) -> Tuple[str, int, str]`**: (Defined in `AbstractOpenAiClient` but executed by `OpenAiChatCompletion` instance)
    *   This is a central method used by `Chat.ask()`.
    *   **Structured Output/JSON Mode**: If `schema` is provided in `kwargs`, it calls `_structured_output_call()` to attempt to get JSON output adhering to the schema.
    *   **Tool Handling**: If `tools` are provided, it sets `tool_choice` to "auto" if not specified and disables streaming.
    *   **Parameter Naming**: Calls `resolve_token_param_naming()` to use either `max_tokens` or `max_completion_tokens` based on `use_max_tokens_param`.
    *   **Model-Specific Kwargs**: Calls `_update_model_specific_kwargs()` to adjust parameters for compatibility with specific models like "o1-mini" (e.g., forcing temperature to 1.0, disabling streaming, converting system messages).
    *   Makes the call to `self.client.chat.completions.create()`.
    *   Parses the response, handling both regular text and `tool_calls`.
    *   Returns the final text/tool result, response tokens, and message type.

*   **Structured Output Helpers**:
    *   `_validate_structured_input()`: Validates if a schema is a JSON string, dict, or Pydantic model.
    *   `_create_structured_response_format()`: Creates the `response_format` or `guided_json` parameter based on `model_type` (OpenAI vs. vLLM) and schema type.
    *   `_get_structured_output_response()`: Makes the API call for structured output.
    *   `_structured_output_call()`: Orchestrates the structured output process.

*   **Token Limit Handling**:
    *   `_truncate_by_tokens()`: Truncates messages (oldest non-system first) if total tokens exceed `safe_window`.
    *   `check_token_limits()`: Calculates prompt tokens, truncates if necessary, and adjusts `max_completion_tokens` to fit the model's context window.

*   **Image Handling (`_handle_image_params`)**: (Defined in `AbstractOpenAiClient`)
    *   Formats image inputs (URL or base64) into the structure expected by OpenAI's multimodal chat completion API.

### Interaction with OpenAI API

*   Uses the `openai.OpenAI` client from the official Python SDK.
*   Constructs requests for the `chat.completions.create` endpoint.
*   Handles parameters like `model`, `messages`, `temperature`, `max_tokens`, `stream`, `tools`, `tool_choice`, `response_format`.

## `AzureOpenAITextGenerationClient` (from `azure_openai_chat_completion.py`)

This class provides a client for OpenAI models deployed via Microsoft Azure OpenAI Service.

*   **Purpose**: To enable interaction with OpenAI models using Azure-specific endpoints, API keys, and deployment names, while maintaining a consistent interface with other OpenAI clients.
*   **Relationship to Framework**: It extends `OpenAITextGenerationClient`. This means it inherits most of the functionality for request preparation, response handling, streaming, tool use, and structured output.
*   **Key Differences**:
    *   **Initialization**:
        *   `endpoint` (str): The Azure OpenAI service endpoint URL (e.g., `https://your-resource-name.openai.azure.com/`). **Required.**
        *   `model_name` (str, optional): While passed to the parent, for Azure, the `azure_deployment` name (passed as `deployment_id` in `kwargs` to the parent, or implicitly the `model_name`) is often more critical for identifying the deployed model.
        *   `api_key` (str, default: "EMPTY"): The Azure OpenAI API key.
        *   `api_version` (str, default: "2023-07-01-preview"): The API version for Azure OpenAI.
        *   The constructor calls the parent `OpenAiChatCompletion` constructor, passing along these Azure-specific parameters which are then used in `_get_client`.
    *   **Client Instantiation (`_get_client`)**:
        *   This method is overridden to initialize and return an `openai.AzureOpenAI` client instance.
        *   It uses the `api_key`, `azure_endpoint`, and `api_version` for configuration.
    *   **Tokenizer Initialization (`_get_tokenizer`)**:
        *   It attempts to get a tokenizer for `self.model_name`. If this fails (e.g., if `model_name` is a deployment ID not directly recognized by `tiktoken`), it defaults to using a tokenizer for "gpt-3.5-turbo" (or an `openai_model_name` if provided in `init_args`). This is important because Azure deployment names can be custom.

*   **Leveraging Parent Class**: Most other functionalities (API call logic, streaming, tool handling, structured output, token limit checks) are inherited directly from `OpenAITextGenerationClient` and `AbstractOpenAiClient`. The key change is that these methods will operate using the `AzureOpenAI` client instance.

These clients provide a standardized way to leverage OpenAI and Azure OpenAI models for advanced text generation tasks within SEMOSS, including chat, instruction following, tool use, and structured data extraction.
