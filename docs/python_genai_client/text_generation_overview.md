# Text Generation Clients (`py/genai_client/text_generation/`)

The `py/genai_client/text_generation/` sub-package is dedicated to clients that interact with various Large Language Models (LLMs) for text generation, chat completions, and instruction-following tasks. It provides a standardized way to call different model providers and includes utilities for managing prompt templates.

## Core Concepts

### `abstract_text_generation_client.py` - `AbstractTextGenerationClient`

This abstract base class (ABC) defines the common interface and shared functionalities for all text generation clients within the `genai_client`.

*   **Purpose**: To provide a consistent structure for initializing clients, managing prompt templates, handling model limits, and defining the core `ask` functionality that subclasses must implement.
*   **Key Attributes**:
    *   `model_name` (str): The identifier of the specific LLM being used.
    *   `model_limits` (`ModelLimits` Pydantic model): Stores context window and max token information, often derived from SMSS configurations passed during initialization.
    *   `templates` (Dict): A dictionary holding loaded prompt templates.
    *   `template_name` (str, optional): The default template name to use if not specified in a call.
*   **Initialization `__init__(...)`**:
    *   Loads prompt templates from a specified JSON file path or a dictionary. If no template source is provided, it defaults to loading `chat_templates.json` located within the same directory.
    *   Parses `kwargs` for model limit parameters (`context_window`, `max_input_tokens`, `max_completion_tokens` or `max_tokens`) and stores them in `self.model_limits`.
*   **`AskSettings` (Inner Pydantic Class)**:
    *   Defines a structure to hold settings that influence how a model call is made, rather than parameters passed directly to the model. These include:
        *   `full_prompt` (Optional[List[Dict]]): Allows providing a complete, pre-formatted prompt structure (e.g., a list of user/assistant messages).
        *   `streaming` (bool): Whether to use streaming for the response (defaults to `True`).
        *   `use_history` (bool): Whether to incorporate conversation history (defaults to `True`).
        *   `history` (Optional[List[Dict]]): The actual conversation history.
        *   `image_url` (Optional[List[str]]), `image_encoded` (Optional[List[str]]): For multi-modal inputs.
    *   The `get_ask_settings(**kwargs)` method parses these from keyword arguments passed to the `ask` method.
*   **Template Management**:
    *   `get_template(template_name, **kwargs)`: Retrieves a template by its name. It includes a fallback mechanism to look for model-specific default templates (e.g., `f"{self.model_name}.default.context"` or `f"{self.model_name}.default.nocontext"`) if the exact `template_name` is not found.
    *   `add_template(template_name, template)`: Allows adding new templates programmatically.
    *   `write_templates(template_file)`: Saves the current set of templates to a specified JSON file.
    *   `fill_template(template_name, **kwargs)`: Fills a named template with provided keyword arguments.
    *   `fill_context(theContext, **kwargs)`: Uses Python's `string.Template` to substitute placeholders in a given template string.
*   **Abstract Method**:
    *   `ask_call(self, *args: Any, **kwargs: Any) -> AskModelEngineResponse`: This is the core method that concrete client implementations **must** override. It's responsible for making the actual API call to the specific LLM service and returning an `AskModelEngineResponse` (defined in `py/genai_client/constants.py`).
*   **Public Method**:
    *   `ask(*args: Any, **kwargs: Any) -> Dict`: The primary public method users call. It invokes the subclass's `ask_call` method and then converts the `AskModelEngineResponse` to a dictionary using its `to_dict()` method.
    *   `embeddings(...)`: Returns a default "This model does not support embeddings" message, as this class hierarchy is for text generation.

### `chat_templates.json`

*   **Purpose**: A JSON file located in the `py/genai_client/text_generation/` directory that stores a collection of predefined prompt templates.
*   **Structure**: A flat JSON object where keys are template names (e.g., `"orca.default.context"`, `"sql.default.context"`) and values are the corresponding template strings.
*   **Placeholders**: Template strings use `$`-prefixed placeholders (e.g., `$system`, `$question`, `$context`) which are substituted by the `fill_template` or `fill_context` methods of `AbstractTextGenerationClient`.
*   **Usage**: Provides default and model-specific prompt structures that can be easily loaded and used by text generation clients. This helps in maintaining consistency and adapting to the specific formatting requirements of different LLMs. For example, a template for a chat model might include roles like "System", "User", and "Assistant".

## Specific Client Implementations

The `text_generation` package contains various subdirectories and files for clients tailored to specific LLM providers or model families. These concrete clients extend `AbstractTextGenerationClient` and implement the `ask_call` method to handle the unique API requirements of each service.

Detailed documentation for each major provider can be found here:

*   **[Anthropic Clients (Claude)](./text_generation/anthropic.md)**
*   **[AWS Bedrock Clients](./text_generation/bedrock.md)**
*   **[Google Vertex AI & GenAI Clients](./text_generation/google.md)**
*   **[OpenAI & Azure OpenAI Clients](./text_generation/openai.md)**
*   **[Text Generation WebUI (Oobabooga)](./text_generation/textgen.md)**

These documents will cover initialization, specific parameters, and usage examples for each client type.
