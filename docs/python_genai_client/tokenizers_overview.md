# Tokenizers (`py/genai_client/tokenizers/`)

The `py/genai_client/tokenizers/` sub-package provides utilities for counting tokens and encoding/decoding text according to different models' tokenization schemes. This is essential for managing context window limits, estimating costs, and ensuring prompts are correctly formatted for various large language models.

## Core Concepts

The primary goal of this sub-package is to offer a consistent way to handle tokenization, abstracting the specifics of underlying libraries like `tiktoken` (for OpenAI models) or tokenizers from the `transformers` library (for Hugging Face models).

### `abstract_tokenizer.py` - `AbstractTokenizer`

This file defines the `AbstractTokenizer` class, an abstract base class (ABC) that all concrete tokenizer implementations inherit from.

*   **Purpose**: To establish a common interface for tokenizer functionalities.
*   **Key Attributes**:
    *   `encoder_name` (str): The name of the model or encoding scheme used to initialize the tokenizer (e.g., "gpt-4", "cl100k_base", "bert-base-uncased").
    *   `max_tokens` (Optional[int]): The overall maximum token limit the model can handle (context + completion).
    *   `max_input_tokens` (Optional[int]): The maximum number of tokens allowed for the input prompt. If not provided, it might be derived from `max_tokens` or a model's default.
    *   `context_window` (Optional[int]): Often used interchangeably with `max_input_tokens` or `max_tokens` to refer to the model's total context capacity.
    *   `max_completion_tokens` (Optional[int]): The maximum number of tokens that can be generated in a response.
    *   `tokenizer`: The actual underlying tokenizer object (e.g., from `tiktoken` or `transformers`).
*   **Abstract Methods (to be implemented by subclasses)**:
    *   `_get_tokenizer(self, encoder_name: str)`: Must load and return the specific tokenizer instance.
    *   `count_tokens(self, input_data: Union[List[Dict], str]) -> int`: Must return the number of tokens for the given input (string or list of chat messages).
    *   `get_tokens_ids(self, input_data: Union[List[Dict], str]) -> List[int]`: Must return a list of token IDs for the input.
    *   `decode_token_ids(self, token_ids: List[int]) -> str`: Must decode a list of token IDs back into a string.
*   **Utility Methods (often implemented in AbstractTokenizer or overridden)**:
    *   `get_tokens(self, input_data: Union[List[Dict], str]) -> List[str]`: Returns a list of token strings.
    *   `get_max_token_length(self) -> int`: Returns the effective maximum token limit.
    *   `get_max_input_token_length(self) -> int`: Returns the maximum input token limit.
    *   `_safe_encode(self, text: str) -> List[int]`: Encodes text to token IDs, often handling special tokens.
    *   `_safe_decode(self, tokens: List[int]) -> str`: Decodes token IDs to text.

## `openai_tokenizer.py` - `OpenAiTokenizer`

This class, extending `AbstractTokenizer`, is specifically designed for models that use `tiktoken` based tokenization, primarily OpenAI models (GPT series) and models that are compatible with OpenAI's tokenization (like some Anthropic models when calculating prompt size for them).

*   **Purpose**: To provide accurate token counting and encoding/decoding for OpenAI-compatible models using the `tiktoken` library.
*   **Initialization `__init__(...)`**:
    *   `encoder_name` (str): The model name (e.g., "gpt-4", "gpt-3.5-turbo") or a specific `tiktoken` encoding name (e.g., "cl100k_base").
    *   `max_tokens` (int): Overall maximum token limit for the model.
    *   `max_input_tokens` (Optional[int]): Maximum tokens for the input prompt.
    *   `context_window` (Optional[int]): Alias for `max_input_tokens` or total model capacity.
    *   `max_completion_tokens` (Optional[int]): Maximum tokens for the generated output.
    *   It calls `_set_token_adjustments()` to set `tokens_per_message` and `tokens_per_name` based on the `encoder_name`, which are specific to how OpenAI calculates tokens for chat messages.

### Key Methods and Functionality

*   **`_set_token_adjustments(self, encoder_name: str)`**:
    *   Sets `self.tokens_per_message` and `self.tokens_per_name` based on the OpenAI model. For "gpt-4" or "gpt-3.5-turbo", these are (3, 1) respectively. For "gpt-3.5-turbo-0301", they are (4, -1). Otherwise, they are (0, 0). These adjustments are used in `count_tokens` for chat messages.

*   **`_get_tokenizer(self, encoder_name: str)`**:
    *   Uses `tiktoken.get_encoding(encoder_name)` if `encoder_name` is a known encoding string (e.g., "cl100k_base", "o200k_base").
    *   Otherwise, it tries `tiktoken.encoding_for_model(encoder_name)`.
    *   Includes a fallback for "gpt-4o" to "o200k_base" if the specific model name isn't recognized by the `tiktoken` version.
    *   If the model is still not found, it logs a warning and defaults to a `WordCountTokenizer` as a very basic fallback.

*   **`format_with_chat_template(self, messages: List[Dict]) -> str`**:
    *   Applies OpenAI's chat template to a list of messages if the tokenizer supports it (i.e., if it has `apply_chat_template` method, and `chat_template` is set, defaulting to "chatml").
    *   Crucially, it returns an empty string if the message content includes images (structured list content), as `apply_chat_template` is typically for text-only messages. This means token counting for image-containing messages relies on the more manual approach in `count_tokens`.

*   **`count_tokens(self, input_data: Union[List[Dict], str]) -> int`**:
    *   If `input_data` is a list of chat message dictionaries (OpenAI format):
        *   It iterates through each message, adding `self.tokens_per_message`.
        *   For each key-value pair in a message:
            *   If the key is "content":
                *   If the content is a list (structured content, e.g., for multimodal models with text and images), it iterates through the list. If a part is "text", it tokenizes and counts that text. Image parts are *not* tokenized by this method (OpenAI API handles image tokenization internally).
                *   If the content is a string, it tokenizes and counts it.
            *   If the key is "name" (used for function/tool names in some contexts), it adds `self.tokens_per_name`.
        *   Finally, it adds 3 to the total (representing the priming tokens for an assistant's reply).
    *   If `input_data` is a string, it simply encodes it using `self.get_tokens_ids()` and returns the length.

*   **`get_tokens_ids(self, input_data: Union[List[Dict], str]) -> List[int]`**:
    *   If `input_data` is a list of messages, it concatenates the "content" or "text" fields of all messages into a single string before encoding.
    *   If `input_data` is a dictionary, it extracts the "content" field.
    *   Encodes the resulting string using `self.tokenizer.encode(input_data)`.

*   **`get_tokens(self, input_data: Union[List[Dict], str]) -> List[str]`**:
    *   Converts the input data to token IDs using `get_tokens_ids` and then decodes each token ID individually to get a list of token strings.

*   **`get_max_token_length(self) -> int`**: Returns `self.max_tokens` if set, otherwise defaults to `self.tokenizer.max_token_value` (though this attribute might not be standard on all `tiktoken.Encoding` objects).
*   **`get_max_input_token_length(self) -> int`**: Returns `self.max_input_tokens`.
*   **`decode_token_ids(self, token_ids: List[int]) -> str`**: Decodes a list of token IDs back to a string using `self.tokenizer.decode(token_ids)`.
*   **`_safe_encode(self, text: str) -> List[int]` / `_safe_decode(self, tokens: List[int]) -> str`**: Wrapper methods for `self.tokenizer.encode` and `self.tokenizer.decode`.

### Role of `tiktoken`

The `OpenAiTokenizer` heavily relies on the `tiktoken` library, which is OpenAI's open-source tokenizer. `tiktoken` is used to:
*   Load the correct encoding scheme based on the model name or encoding name.
*   Perform the actual conversion of text to token IDs (`encode()`).
*   Perform the conversion of token IDs back to text (`decode()`).

### Supported Models

This tokenizer is primarily intended for:
*   OpenAI models (GPT-3.5, GPT-4, GPT-4o, text embedding models, etc.).
*   Models that explicitly state compatibility with `tiktoken` encodings like "cl100k_base" or "o200k_base".
*   It might be used by other clients (e.g., for Anthropic models on some platforms) to *estimate* prompt size, even if the final tokenization is done by the model provider's endpoint, because `tiktoken` often provides a close approximation for models with similar tokenization strategies.

The specific logic for handling `tokens_per_message` and `tokens_per_name` in `count_tokens` is particular to how OpenAI calculates token counts for its chat completion API.
