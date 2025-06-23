# Text Generation WebUI (Oobabooga) Client (`textgen_client.py`)

The `py/genai_client/text_generation/textgen_client.py` module provides the `TextGenClient` class, which is designed to interact with a Text Generation WebUI (often referred to as Oobabooga) server. This server typically hosts open-source language models and exposes an API for text generation.

## `TextGenClient`

*   **Purpose**: This class acts as a client for language models served via Oobabooga's Text Generation WebUI. It provides a standardized interface within SEMOSS to send prompts and receive generated text from these models, abstracting the direct API calls to the WebUI.
*   **Relationship to Framework**: It extends `AbstractTextGenerationClient`, inheriting functionalities like prompt template management and the public `ask()` method. The core generation logic is implemented in its `ask_call()` method.

### Initialization

The `TextGenClient` is initialized with the following parameters:

*   `endpoint` (str): **Required**. The base URL of the Text Generation WebUI server (e.g., `http://localhost:5000`).
*   `template` (Optional[Union[Dict, str]]): A dictionary of prompt templates or a path to a JSON file containing templates.
*   `model_name` (Optional[str]): The name or identifier of the model being served by the Text Generation WebUI. This is primarily used to initialize a `HuggingfaceTokenizer` for client-side token counting and truncation.
*   `template_name` (Optional[str]): The default prompt template name to use.
*   `stop_sequences` (Optional[List[str]], default: `[]`): A list of strings that, if generated, will cause the model to stop generating further text. If empty, it defaults to the tokenizer's `eos_token` if available.
*   `timeout` (Optional[int], default: `30`): Timeout in seconds for API requests to the WebUI.
*   `**kwargs`: Additional keyword arguments.
    *   `MAX_TOKENS`, `MAX_INPUT_TOKENS`: Passed to `HuggingfaceTokenizer` for setting token limits.

The constructor initializes a `text_generation.Client` instance (from the `text-generation` library by Hugging Face, which is a common client for TGI and similar backends) using the provided `endpoint`. It also sets up a `HuggingfaceTokenizer` based on the `model_name`.

### Key Methods and Functionality

*   **`ask_call(self, question: str = None, context: Optional[str] = None, ..., **kwargs) -> AskModelEngineResponse`**:
    *   This is the primary method for making requests to the Text Generation WebUI.
    *   **Prompt Construction**:
        *   If `FULL_PROMPT` is in `kwargs`, it uses this as the complete prompt (can be a string or a list of role/content dicts which it formats into a single string).
        *   Otherwise, it uses `_fill_params()` and `_process_history()` to construct the prompt string from `question`, `context`, `template_name`, `history`, and other `kwargs` (for template filling).
        *   `_fill_params()`: Fills templates using `self.fill_context()` or `self.fill_template()`.
        *   `_process_history()`: Formats chat history into the prompt string, typically in a "Role: Content\n\n" format. It tries to place history before the main content if a template substitution occurred, otherwise it appends history and then the question.
    *   **Token Limit Handling**: Calls `_check_token_limits()` to truncate the prompt if it exceeds model/tokenizer limits and adjust `max_new_tokens` accordingly.
    *   **API Parameters**: Prepares a `parameters` dictionary for the `self.client.generate()` or `self.client.generate_stream()` call. This includes:
        *   `prompt` (str): The fully constructed prompt.
        *   `do_sample` (bool, default: `False`)
        *   `max_new_tokens` (int, default: `1000`)
        *   `repetition_penalty` (Optional[float])
        *   `return_full_text` (bool, default: `False`)
        *   `seed` (Optional[int])
        *   `stop_sequences` (List[str])
        *   `temperature` (Optional[float])
        *   `top_k` (Optional[int])
        *   `top_p` (Optional[float])
        *   `truncate` (Optional[int]): Note: Client-side truncation is also performed by `_check_token_limits`.
        *   `typical_p` (Optional[float])
        *   `watermark` (bool, default: `False`)
    *   **API Call**:
        *   If `stream` is `True` (passed in `kwargs`, defaults to `True`), it calls `self.client.generate_stream(**parameters)`. It iterates through the streamed response tokens, concatenates them, and prints them with the `prefix`.
        *   If `stream` is `False`, it calls `self.client.generate(**parameters)`.
    *   **Logprobs & Token Details**: If `decoder_input_details=True` is passed (for non-streaming) or `include_logprobs=True` (for streaming, though TGI streaming often doesn't provide detailed token logprobs per chunk in the same way as a non-streaming detailed response), it attempts to capture token texts and log probabilities.
    *   **Response Packaging**: Returns an `AskModelEngineResponse` containing:
        *   `response`: The generated text.
        *   `prompt_tokens`: Calculated by `_check_token_limits`.
        *   `response_tokens`: Calculated by counting the tokens in the generated text (for streaming) or from `response.details.tokens` (for non-streaming if details are available).
        *   `tokens` and `logprobs` lists if requested and available.

*   **`_process_full_prompt(self, full_prompt: Union[List, str]) -> str`**:
    *   Formats a `full_prompt` (either a list of OpenAI-style message dicts or a raw string) into a single string suitable for the Text Generation WebUI.

*   **`_check_token_limits(self, prompt_payload: str, max_new_tokens: int) -> Tuple[str, int, AskModelEngineResponse]`**:
    *   Uses the initialized `HuggingfaceTokenizer` to count tokens in `prompt_payload`.
    *   Truncates `prompt_payload` from the beginning if it exceeds `max_input_tokens` (or `max_tokens` as a fallback from tokenizer settings).
    *   Adjusts `max_new_tokens` to ensure `prompt_tokens + max_new_tokens` does not exceed the overall `max_tokens` limit of the tokenizer, applying a 5% buffer.
    *   Populates `prompt_tokens` and any `warning` in an `AskModelEngineResponse` object.

### Interaction with Text Generation WebUI API

*   The client uses the `text_generation.Client` library, which is designed to interact with Hugging Face's Text Generation Inference (TGI) toolkit. Oobabooga's Text Generation WebUI often exposes a TGI-compatible API.
*   **Endpoints Used**: The `text_generation.Client` typically interacts with endpoints like:
    *   `/generate` (for non-streaming)
    *   `/generate_stream` (for streaming)
*   **Payload Structure**: The JSON payload sent to these endpoints includes parameters like `prompt`, `do_sample`, `max_new_tokens`, `temperature`, `top_p`, `top_k`, `stop_sequences`, etc., as detailed in the `parameters` dictionary within `ask_call`.

### Error Handling & Unique Features

*   **Client-Side Tokenization & Truncation**: Proactively checks and truncates prompt length based on the configured tokenizer and model limits before sending the request.
*   **Flexible Prompt Construction**: Offers multiple ways to build the final prompt: using `question`/`context`, leveraging prompt templates, or providing a `FULL_PROMPT`.
*   **Streaming Support**: Capable of handling streamed responses from the API.
*   **Parameter Mapping**: Maps common LLM parameters to the specific names expected by the `text_generation.Client` library.

The `TextGenClient` enables SEMOSS to utilize a wide variety of open-source models hosted via the Text Generation WebUI by providing a compatible client interface.
