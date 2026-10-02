# Anthropic Text Generation Client

The `py/genai_client/text_generation/anthropic_client/anthropic_text_client.py` module provides the `AnthropicTextClient` class for interacting with Anthropic's language models (e.g., Claude series), potentially via different providers like Google Cloud Vertex AI.

## `AnthropicTextClient`

*   **Purpose**: This class serves as a specialized client for Anthropic's text generation models. It adapts the common interface defined by `AbstractTextGenerationClient` to the specific API requirements of Anthropic models, including message formatting, handling of system prompts, and tool usage.
*   **Relationship to Framework**: It extends `AbstractTextGenerationClient`, inheriting its template management, model limit handling, and the main `ask()` public method. It implements the crucial `ask_call()` method to make requests to an Anthropic model.

### Initialization

The client is initialized with the following parameters:

*   `provider` (str): Specifies the platform through which the Anthropic model is accessed. Supported values are `"anthropic"` (first party API), `"bedrock"` (Amazon Bedrock), `"azure"` (Microsoft Foundry), and `"google"` (Anthropic models on Google Cloud Vertex AI).
*   `use_beta_header` (str or bool, optional): Routes requests through `client.beta.messages` and sends the `anthropic-beta` header. Defaults to `False`. When enabled, `beta_feature_name` is required.
*   `prompt_caching` (str or bool, optional): Enables Anthropic prompt caching. Defaults to `True`.
*   `cache_ttl` (str, optional): Lifetime of a cache entry, either `"5m"` or `"1h"`. Defaults to `None`, which leaves the field off the request and gets Anthropic's 5 minute default. See [Prompt Caching](#prompt-caching) below.
*   `**kwargs`: A dictionary of keyword arguments.
    *   **Common from `AbstractTextGenerationClient`**:
        *   `model_name` (str): The identifier for the specific Anthropic model (e.g., "claude-3-opus-20240229").
        *   `template` (Optional[Dict]): A dictionary of prompt templates.
        *   `template_name` (Optional[str]): The default template name to use.
        *   Model limit parameters (e.g., `context_window`, `max_completion_tokens`).
    *   **Provider-Specific (for "google" provider)**:
        *   `service_account_credentials` (Dict, optional): Google service account key as a dictionary.
        *   `service_account_key_file` (str, optional): Path to a service account JSON key file.
        *   `region` (str, optional): Google Cloud region.
        *   `project` (str, optional): Google Cloud Project ID.
        *   `api_key` (str, optional): Though less common for Vertex AI, an API key might be relevant for other Anthropic access methods if supported in the future.

The constructor initializes the underlying client (e.g., `anthropic.AnthropicVertex` if `provider="google"`) via the `_get_client` method, which uses `GoogleClientConfig` and `GoogleClient` from `py.genai_client.clients.google_clients`.

### Prompt Caching

Prompt caching lets Anthropic reuse a previously processed prompt prefix. Cache reads are billed at roughly 0.1x the base input price, while the write that seeds the cache is billed at 1.25x for a 5 minute TTL and 2x for a 1 hour TTL. A 5 minute entry pays for itself on the second request, a 1 hour entry on the third, so `cache_ttl='1h'` is the right choice when traffic is bursty with idle gaps longer than five minutes and the same prefix is hit at least a few times per hour.

Caching is controlled by two init parameters:

*   `prompt_caching`: master switch. When false, no `cache_control` markers are attached at all.
*   `cache_ttl`: `"5m"` or `"1h"`. The value is validated in `_normalize_cache_ttl` against `AnthropicCacheTTL` in `py/genai_client/message_builders/anthropic/anthropic_models.py`; anything else raises a `ValueError` at engine startup. An unset or blank value means the `ttl` key is omitted from `cache_control` and Anthropic applies its 5 minute default.

`_cache_control()` builds the payload attached at each breakpoint, which is `{"type": "ephemeral"}` when no TTL was requested and `{"type": "ephemeral", "ttl": "1h"}` when one was.

Where the markers land depends on the provider:

*   `anthropic` and `azure` support the top level automatic `cache_control` field, so `ask_call` sets `request_config.cache_control` once and Anthropic places the breakpoint on the last cacheable block.
*   `bedrock` and `google` only support block level `cache_control`, so the client places the breakpoints itself in Anthropic's evaluation order: `_apply_cache_to_tools` marks the last tool definition, `_apply_cache_to_system` marks the last text block of the system prompt, and `_apply_cache_to_last_block` marks the last cacheable block (`text`, `tool_result`, `image`, or `document`) of the last message.

Cache activity is reported back through `AskModelEngineResponse2` as `cache_read_tokens` and `cache_creation_tokens`, and logged as a `[prompt_caching] ttl=... cache_read_tokens=... cache_creation_tokens=...` line. `prompt_tokens` is normalized to the total billed input, meaning Anthropic's `input_tokens` plus both cache counters, so it lines up with the OpenAI and Gemini clients.

Note that the minimum cacheable prefix is model dependent and ranges from 512 to 4096 tokens. A prompt shorter than the model's minimum will not cache and produces no error, just `cache_creation_tokens` of zero.

#### Configuring it on an engine

`cache_ttl` is a plain constructor argument, so it is set on the `INIT_MODEL_ENGINE` line of the model SMSS that `AbstractPythonModelEngine` runs at startup:

```
INIT_MODEL_ENGINE	import genai_client;${VAR_NAME} = genai_client.AnthropicClient(model_name='${MODEL}', provider='${PROVIDER}', endpoint='${ENDPOINT}', api_key='${API_KEY}', context_window=${CONTEXT_WINDOW}, max_tokens=${MAX_TOKENS}, prompt_caching=True, cache_ttl='1h')
```

### Key Methods and Functionality

*   **`_get_client(self, **kwargs)`**:
    *   Based on the `provider` string, it configures and returns the appropriate native client.
    *   If `provider == "google"`, it sets up a `GoogleClient` of type `ANTHROPIC` for Vertex AI.
    *   Raises a `ValueError` if the provider is not supported.

*   **`ask_call(self, question: str = None, context: str = None, use_history: bool = True, history: List[Dict] = None, prefix: str = "", **kwargs)`**:
    *   This is the core method implementing the logic for making a request to the Anthropic model.
    *   **History Conversion**: Calls `_convert_history()` to transform the input `history` (and the current `question`) into Anthropic's required message format (a list of `Message` Pydantic models with "user" and "assistant" roles). It also extracts any system prompt found within the history.
    *   **System Prompt**: The `context` parameter is treated as the system prompt for Anthropic models. If a system prompt is also derived from history, the explicit `context` usually takes precedence or is combined.
    *   **Request Configuration**: Uses `_convert_args_to_provider_config()` to prepare the request payload (`AnthropicRequestConfig`) for the Anthropic API, including model name, system prompt, formatted messages, max tokens, temperature, tools, etc.
    *   **API Call**:
        *   If `streaming` is enabled (default), it calls `_handle_streaming()`.
        *   Otherwise, it calls `self.client.messages.create(...)` for a non-streaming response.
    *   **Tool Use Handling**: If the model's response indicates tool use (`response.stop_reason == "tool_use"`), it calls `_parse_tools_call_response()` to format the tool call information.
    *   **Response Packaging**: Returns an `AskModelEngineResponse` containing the model's text response (or tool calls), and token usage information.

*   **`_convert_history(self, question: str = None) -> Tuple[List[Message], str]`**:
    *   Processes the `self.ask_settings.history` (and an optional new `question`) into a list of `Message` objects.
    *   Handles complex content within user messages, including text and images (converting image URLs or base64 data into `ImageContentPart` objects).
    *   Correctly formats messages for "user" and "assistant" roles, and extracts a "system" message if present in the history.
    *   Supports `ToolUseContentPart` and `ToolResultContentPart` for representing tool interactions in the history.
    *   Calls `_filter_incomplete_tool_conversations()` to ensure that there are no trailing `tool_use` messages without corresponding `tool_result` messages, as this is disallowed by the Anthropic API.

*   **`_filter_incomplete_tool_conversations(self, messages: List[Message]) -> List[Message]`**:
    *   Removes any final assistant message that consists only of `tool_use` parts, as Anthropic requires a `tool_result` to follow.

*   **`_create_image_part(self, image_type: str, data: str) -> ImageContentPart`**:
    *   A helper to construct `ImageContentPart` Pydantic models from image URLs or base64 data.
    *   If `provider` is "google" (Vertex AI), it fetches URL-based images and converts them to base64, as Vertex AI's Anthropic integration often expects base64.
    *   Determines the correct `media_type` (e.g., "image/jpeg", "image/png").

*   **`_convert_args_to_provider_config(self, context: str = None, history: List[Message] = None, **kwargs) -> AnthropicRequestConfig`**:
    *   Maps generic parameters (like `max_tokens`, `temperature`) and Anthropic-specific parameters from `kwargs` to an `AnthropicRequestConfig` Pydantic model.
    *   Handles `tools` conversion using `_handle_tools_conversion()`. If tools are present, streaming is typically disabled.

*   **`_handle_tools_conversion(self, tools: List[Dict]) -> List[ToolCall]`**:
    *   Converts a list of tool definitions (often in OpenAI-compatible format) into a list of Anthropic `ToolCall` Pydantic models.

*   **`_handle_streaming(self, prefix: str = "", converted_history: List[Message] = None) -> StreamingResponse`**:
    *   Manages streaming responses from `self.client.messages.stream(...)`.
    *   Concatenates text chunks from `stream.text_stream`.
    *   Prints the streamed text to `stdout` (prefixed with `prefix`).
    *   Calculates input and output tokens using `_count_tokens()` after the stream is complete.
    *   Returns a `StreamingResponse` Pydantic model containing the full text and token usage.

*   **`_count_tokens(self, converted_history: List[Message] = None, response_string: str = None) -> int`**:
    *   Counts tokens for either a list of `Message` objects (for prompts) or a response string.
    *   Uses `self.client.messages.count_tokens(...)` for this purpose. This method is available when the client is an `anthropic.AnthropicVertex` instance.

### Data Structures

The client makes extensive use of Pydantic models for structuring requests and responses, ensuring type safety and clear data contracts. Key Pydantic models include:
*   `Message`: Represents a single message in a conversation, with a role and content (which can be a string or a list of content parts).
*   `TextContentPart`, `ImageContentPart`, `ToolUseContentPart`, `ToolResultContentPart`: Define different types of content within a message.
*   `AnthropicRequestConfig`: Structures the request payload for the Anthropic API.
*   `Usage`, `StreamingResponse`: Structure parts of the response from the Anthropic API.

### Error Handling & Unique Features

*   **Provider Abstraction**: A key feature is its ability to route to Anthropic models via different providers (currently Google Vertex AI).
*   **Streaming Support**: Implements handling for streaming responses.
*   **Tool Use**: Supports Anthropic's tool use (function calling) feature, including parsing tool use requests from the model and formatting tool definitions.
*   **Complex Content Handling**: Manages messages with mixed content types (text and images).
*   **Token Counting**: Integrates Anthropic's token counting.
*   **Error Handling**: Relies on the underlying Anthropic SDK or Google Client SDK for API errors. ValueErrors are raised for unsupported configurations (e.g., unsupported provider).

The `AnthropicTextClient` provides a robust and feature-rich interface for leveraging Anthropic's models within the SEMOSS ecosystem, with a focus on adapting to the specific requirements of the Anthropic Messages API, including its structured message format and tool usage capabilities.
