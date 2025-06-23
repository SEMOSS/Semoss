# AWS Bedrock Text Generation Client

The `py/genai_client/text_generation/bedrock_client.py` module provides the `BedrockClient` class for interacting with various text generation models hosted on Amazon Web Services (AWS) Bedrock.

## `BedrockClient`

*   **Purpose**: This class acts as a specialized client for text generation models available through AWS Bedrock. It standardizes the interaction with these models, handling the specifics of the Bedrock API, including request/response formatting for different underlying model providers (like Anthropic, AI21, Cohere, Meta, Amazon).
*   **Relationship to Framework**: It extends `AbstractTextGenerationClient`, inheriting template management, model limit considerations, and the public `ask()` method. It implements the core `ask_call()` method to communicate with the Bedrock service.

### Initialization

The `BedrockClient` is initialized with the following parameters:

*   `template` (Optional[Dict]): A dictionary of prompt templates.
*   `service_name` (str, default: `"bedrock-runtime"`): The AWS service name for Bedrock runtime.
*   `modelId` (str, default: `"anthropic.claude-instant-v1"`): The identifier for the specific Bedrock model to be used (e.g., "anthropic.claude-v2", "ai21.j2-grande-instruct", "cohere.command-text-v14", "meta.llama2-13b-chat-v1", "amazon.titan-text-express-v1").
*   `access_key` (Optional[str]): AWS access key ID. If not provided, the client relies on environment credentials (e.g., IAM role).
*   `secret_key` (Optional[str]): AWS secret access key.
*   `region` (Optional[str]): The AWS region where the Bedrock service is being used (e.g., "us-east-1").
*   `template_name` (Optional[str]): Default prompt template name.
*   `response_stream` (Optional[bool]): Global flag to indicate if responses should be streamed by default. (Note: Streaming behavior is also controllable per `ask_call`).
*   `guardrail_identifier` (Optional[str]): The identifier of a Bedrock Guardrail to use.
*   `guardrail_version` (Optional[str]): The version of the Bedrock Guardrail.
*   `**kwargs`: Additional keyword arguments, including those for model limits (`MAX_TOKENS`, `MAX_INPUT_TOKENS`) which are passed to the parent class and used to initialize its `tokenizer` (a `HuggingfaceTokenizer` by default, with tokenizer name often mapped from `modelId`).

The constructor initializes a `boto3.client` for the `bedrock-runtime` service using the provided AWS credentials and region.

### Key Methods and Functionality

*   **`_get_client(self)`**:
    *   Returns a `boto3.client` instance for `bedrock-runtime`. It uses provided `access_key`, `secret_key`, and `region`. If keys are not provided, it assumes credentials are set in the environment (e.g., via IAM roles or AWS CLI configuration).

*   **`ask_call(self, question=None, context=None, template_name=None, history=None, max_new_tokens=500, temperature=None, top_p=None, stop_sequences=None, prefix="", stream=True, use_history=True, **kwargs)`**:
    *   The core method for making requests to the Bedrock model.
    *   **Message Preparation**:
        *   Uses `_prepare_message_payload()` to format the `question`, `context`, and `history` into a list of messages.
        *   The `context` is typically treated as a system prompt if provided.
        *   Image handling: If `IMAGE_ENCODED` or `IMAGE_URL` are in `kwargs`, `_handle_image_params()` is called to format the image data into the Bedrock-compatible message structure. This involves converting images to bytes and specifying their format.
        *   Uses `_format_messages_for_model()` to adapt the message structure for specific model families (e.g., Anthropic Claude models require a specific "Human:"/"Assistant:" turn-based format within a single user message content block).
    *   **Inference Configuration**: `create_inference_config()` prepares model-specific inference parameters like `maxTokens`, `temperature`, `topP`.
    *   **Guardrails**: `_get_guardrail_config()` adds guardrail configuration if `guardrail_identifier` and `guardrail_version` are set.
    *   **Tool Handling**: If `tools` and optionally `tool_choice` are provided in `kwargs`, it configures the `toolConfig` for the Bedrock request. Streaming is automatically disabled if tools are used.
    *   **API Call**:
        *   If `stream` is true (and no tools are used), it calls `client.converse_stream(**request_params)` and processes the stream using `_handle_stream_response()`.
        *   Otherwise, it calls `client.converse(**request_params)` for a non-streaming response.
    *   **Response Parsing**:
        *   For streaming, `_handle_stream_response()` concatenates text chunks and extracts token counts from metadata events.
        *   For non-streaming, it parses the `response.get("output", {}).get("message", {}).get("content", [])`. It checks for `toolUse` blocks and formats them, or concatenates `text` blocks.
    *   **Response Packaging**: Returns an `AskModelEngineResponse` with the response text (or tool calls), `prompt_tokens`, `response_tokens`, and `messageType` ("CHAT" or "TOOL").

*   **Helper Methods for Request/Response Formatting**:
    *   `_prepare_message_payload()`: Constructs the initial list of messages.
    *   `_format_message_content()`: Formats a single message dictionary.
    *   `_format_messages_for_model()`: Adapts the message list to the specific format expected by different model providers on Bedrock (e.g., Anthropic's specific turn structure).
    *   `_handle_image_params()`: Converts image URLs or base64 strings into the byte-based format Bedrock expects for multimodal inputs. Includes helpers like `_get_image_extension_from_url`, `_get_bytes_from_encoded`, `_get_bytes_from_url`.
    *   `decode_image_bytes_in_messages()`: A utility that seems intended to handle potential base64 strings within image byte fields, ensuring they are decoded before sending to Bedrock (though Bedrock typically expects raw bytes for images in `converse` API).

*   **Streaming**: The `_handle_stream_response` method iterates through events in the stream from `client.converse_stream`, accumulating text from `contentBlockDelta` events and extracting token usage from `metadata` events.

*   **Tokenizer**: Uses `HuggingfaceTokenizer` by default. The `_get_default_tokenizer()` method provides a basic mapping for some models (e.g., "anthropic.claude-instant-v1" to "bert-base-uncased"), but for many Bedrock models, the exact tokenizer for precise client-side token counting might need to be aligned with the model provider's recommendations. Token counts from the API response are generally preferred.

### Error Handling & Unique Features

*   **Provider Versatility**: The client is designed to be adaptable to the various model providers available through Bedrock by formatting the `messages` and `inferenceConfig` differently based on `modelId` patterns (though the provided code primarily shows detailed formatting for Anthropic models via the `converse` API).
*   **Guardrails**: Supports AWS Bedrock Guardrails for responsible AI.
*   **Tool Use (Function Calling)**: Supports Bedrock's `toolConfig` for function calling capabilities.
*   **Multimodal Inputs**: Capable of handling image inputs for models that support them on Bedrock.
*   **Error Handling**: Includes basic try-except blocks for API calls and logs errors. Specific `botocore.exceptions.ParamValidationError` is caught during streaming attempts with a retry mechanism that converts message content to string, suggesting potential issues with complex message structures in some streaming scenarios.

The `BedrockClient` provides a comprehensive interface for leveraging a wide range of foundation models through AWS Bedrock, abstracting many of the provider-specific request nuances.
