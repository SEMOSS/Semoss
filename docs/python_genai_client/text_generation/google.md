# Google Vertex AI Text Generation Client (`vertex_generative_model.py`)

The `py/genai_client/text_generation/google_clients/vertex_generative_model.py` module provides the `VertexGenerativeModelClient` class, designed to interact with Google's generative models (like Gemini) hosted on Google Cloud Vertex AI.

## `VertexGenerativeModelClient`

*   **Purpose**: This class serves as a client for text generation tasks using models deployed on Vertex AI, particularly focusing on the `GenerativeModel` class from the `vertexai.preview.generative_models` SDK, which is often used for Gemini models. It handles chat-based interactions, including history management and streaming.
*   **Relationship to Framework**: It extends `AbstractVertextAiTextGeneration` (which in turn likely extends `AbstractTextGenerationClient`), inheriting common functionalities for Google Cloud authentication, template management, and model limit considerations. It implements the core `ask_call()` method to make requests to the Vertex AI model.

### Initialization

The client is initialized via its parent class `AbstractVertextAiTextGeneration` and ultimately `AbstractTextGenerationClient`. Key parameters relevant at this level or passed through `**kwargs` include:

*   `model_name` (str): The identifier for the specific Vertex AI model (e.g., "gemini-1.0-pro").
*   `project_id` (str): Your Google Cloud Project ID.
*   `location` (str): The Google Cloud region where the model is hosted (e.g., "us-central1").
*   `creds` (Optional[`google.auth.credentials.Credentials`]): Pre-configured Google Cloud credentials. If not provided, the client attempts to use Application Default Credentials.
*   `template` (Optional[Dict]): A dictionary of prompt templates.
*   `template_name` (Optional[str]): The default template name.
*   `safety_settings` (Optional[Dict]): A dictionary to configure safety settings for blocking harmful content. Keys are `HarmCategory` (e.g., "HARM_CATEGORY_SEXUALLY_EXPLICIT") and values are `HarmBlockThreshold` (e.g., "BLOCK_LOW_AND_ABOVE").
*   Model limit parameters (e.g., `max_tokens`, `max_input_tokens`) are passed to the parent for tokenizer initialization.

The constructor initializes the underlying Vertex AI client by calling `self._get_client()`.

### Key Methods and Functionality

*   **`_get_client(self)`**:
    *   Returns an instance of `vertexai.preview.generative_models.GenerativeModel(self.model_name)`. This is the actual client object used to interact with the specified Vertex AI model.

*   **`ask_call(self, question: str = None, context: Optional[str] = None, history: Optional[List] = [], max_new_tokens: Optional[int] = 500, temperature: Optional[float] = None, top_p: Optional[float] = None, top_k: Optional[int] = None, candidate_count: Optional[int] = None, stop_sequences: Optional[List[str]] = None, prefix: str = "", stream: Optional[bool] = True, use_history: Optional[bool] = True, **kwargs)`**:
    *   This is the core method for making requests to the Vertex AI model.
    *   **Full Prompt Handling**: If `FULL_PROMPT` is in `kwargs`, it expects a list where the last item is the current question, and preceding items form the history.
    *   **History Management**: If `use_history` is true, it converts the provided `history` (a list of dictionaries with "role"/"author" and "content") into a list of Vertex AI `Content` objects.
    *   **Context Handling**: If `context` is provided, it's prepended to the chat history as a user message, followed by a model message "Understood." to simulate setting a system context (as Gemini models via `start_chat` primarily use user/model turns).
    *   **Chat Initialization**: Calls `self.client.start_chat(history=historyChat)` to create a chat session object.
    *   **Generation Configuration**: Prepares a `generation_config` dictionary with parameters like `temperature`, `max_output_tokens` (from `max_new_tokens`), `top_p`, `top_k`, `stop_sequences`, and `candidate_count`.
    *   **Safety Settings**: Converts the `self.safety_settings` dictionary (string keys/values) into the format expected by the Vertex AI SDK, using `gapic_content_types.HarmCategory` and `gapic_content_types.SafetySetting.HarmBlockThreshold` enums.
    *   **API Call**:
        *   Calls `chat.send_message(content=question, generation_config=..., stream=stream, safety_settings=...)`.
        *   If `stream` is true, it iterates through the response stream, concatenates `response.text` chunks, prints them with the given `prefix`, and extracts token counts from `response._raw_response.usage_metadata`.
        *   If `stream` is false, it gets the complete response directly.
    *   **Response Packaging**: Returns an `AskModelEngineResponse` containing:
        *   `response`: The concatenated text response.
        *   `prompt_tokens`: From `usage_metadata.prompt_token_count`.
        *   `response_tokens`: From `usage_metadata.candidates_token_count`.

### Interaction with Vertex AI SDK

*   The client primarily uses `vertexai.preview.generative_models.GenerativeModel` to get a model instance.
*   For chat interactions, it uses `model.start_chat()` to get a `ChatSession` object.
*   Messages are sent using `chat.send_message()`.
*   History messages are converted into `vertexai.preview.generative_models.Content` objects, with `Part.from_text()` used for the content.
*   Safety settings are mapped from string configurations to `gapic_content_types.HarmCategory` and `HarmBlockThreshold` enums.

### Model Parameters

The client supports common LLM parameters:
*   `max_new_tokens` (maps to `max_output_tokens`)
*   `temperature`
*   `top_p`
*   `top_k`
*   `candidate_count`
*   `stop_sequences`

These are passed within the `generation_config` object to the `send_message` method.

### Error Handling and Unique Features

*   **Safety Settings**: Explicitly supports configuring Vertex AI's safety settings.
*   **Streaming**: Supports streaming responses.
*   **Context Simulation**: Simulates a system prompt by prepending user/model turns for context, as the `start_chat` method for Gemini doesn't directly accept a system prompt in the same way some other APIs do.
*   **Error Handling**: Includes a `try-except KeyError` for history formatting and logs errors for invalid safety setting categories/thresholds. Other API errors would typically be raised by the Vertex AI SDK.

### Data Structures

*   Uses `vertexai.preview.generative_models.Content` and `Part` for structuring messages sent to the model.
*   Returns `AskModelEngineResponse` (from `py/genai_client/constants.py`) to standardize the output.

The `VertexGenerativeModelClient` provides a SEMOSS-friendly interface to Google's Gemini models on Vertex AI, adapting the `AbstractTextGenerationClient` framework to the specifics of the Vertex AI SDK.
