# Python GenAI Client (`py/genai_client/`)

The `py/genai_client/` package provides a Python client library designed to offer a standardized and simplified interface for interacting with various Generative AI models and services. Its primary goal is to abstract the specific API details of different model providers (like OpenAI, Google Vertex AI, AWS Bedrock, Anthropic, etc.) and local model serving frameworks, allowing the broader SEMOSS platform to use them through a consistent API.

This client library is crucial for powering features such as:
*   Text generation and chat completions.
*   Generating text embeddings for semantic search and RAG.
*   Counting tokens for prompt engineering and cost estimation.

## Key Components

The `genai_client` package is organized into several key sub-modules:

*   **Clients (`./clients/`)**: Contains modules for initializing and configuring clients for specific GenAI providers or services.
*   **Embedders (`./embedders/`)**: Provides classes for generating vector embeddings from text or images using various embedding models. Each embedder typically wraps a specific model or service.
*   **Text Generation (`./text_generation/`)**: Includes clients for text and chat completion tasks, supporting different models and features like streaming.
*   **Tokenizers (`./tokenizers/`)**: Offers tools to count tokens and tokenize text according to different models' tokenization schemes, which is important for managing context windows and predicting costs.
*   **Model Keys (`./model_keys/`)**: Defines standardized keys or enums to refer to various supported models, ensuring consistency.
*   **Model Limits (`./model_limits.md`)**: Provides information or utilities related to the operational limits of different models, such as context window sizes.
*   **Constants and Utilities (`constants.py`, `utils.py`)**: Contain shared constants and helper functions used across the package.

This documentation will provide details on each of these components.

### Text Generation Clients

The `text_generation` sub-package provides a unified interface for interacting with various large language models for text and chat completion tasks. It includes an abstract base client and concrete implementations for providers like Anthropic, AWS Bedrock, Google Vertex AI, OpenAI, Azure OpenAI, and Oobabooga's Text Generation WebUI.

- [Text Generation Clients Overview](./text_generation_overview.md)

### Tokenizers

The `tokenizers` sub-package provides utilities for token counting, which is essential for managing context windows and estimating costs when interacting with large language models.

- [Tokenizers Overview](./tokenizers_overview.md)

## Core Files

At the root of the `py/genai_client/` package, there are a few key files providing shared constants, utility functions, and base data structures.

### `constants.py`

This file defines shared constants and base dataclasses for model responses.

*   **String Constants**:
    *   `MODEL_NAME`: Key for specifying a model's name.
    *   `MAX_TOKENS`: Key for maximum tokens in a response.
    *   `MAX_INPUT_TOKENS`: Key for maximum input tokens.
    *   `CHAT_TYPE`: Key for specifying a chat interaction type.
    *   `TEMPLATE`, `TEMPLATE_NAME`: Related to prompt templating.
    *   `FULL_PROMPT`: Key for the fully constructed prompt.
    *   `IMAGE_ENCODED`, `IMAGE_URL`, `IMAGE_EXTENSION`: Related to image inputs.

*   **Response Dataclasses**: These provide a standardized structure for responses received from model engines.
    *   `AbstractModelEngineResponse`: A base dataclass including:
        *   `response` (Any): The actual content of the response.
        *   `response_tokens` (int): Number of tokens in the generated response.
        *   `prompt_tokens` (int): Number of tokens in the input prompt.
        *   `to_dict()`: Method to convert the response object to a dictionary, often for serialization or logging.
    *   `AskModelEngineResponse(AbstractModelEngineResponse)`: For chat or question-answering model responses.
        *   Inherits from `AbstractModelEngineResponse`.
        *   `response` (Any): Typically a string with the model's textual answer.
        *   `messageType` (str): e.g., "CHAT".
        *   `warning` (str): Optional warning messages.
        *   `tokens` (List[str]): List of generated tokens (if available).
        *   `logprobs` (List[float]): Log probabilities for tokens (if available).
    *   `EmbeddingsModelEngineResponse(AbstractModelEngineResponse)`: For embedding model responses.
        *   `response` (List[float]): The numerical vector embedding.

### `utils.py`

This file contains various utility functions used throughout the `genai_client` package, particularly for handling URLs and image data.

*   **`StringEnum(Enum)`**: A custom Enum base class that allows direct comparison of enum members with string values (e.g., `MyEnum.VALUE == "value_string"`).
*   **URL Classification and Parsing**:
    *   `is_base64_image_url(url: str) -> bool`: Checks if a given string is a `data:image/...;base64,...` URL.
    *   `is_standard_web_url(url: str) -> bool`: Checks if a string is a standard HTTP or HTTPS URL.
    *   `URLClassification(StringEnum)`: An enum with values `BASE64_IMAGE`, `WEB_URL`, `UNKNOWN`.
    *   `classify_url(url: str) -> str`: Returns the `URLClassification` for a given URL string.
    *   `get_image_extension(url_or_base64: str) -> Optional[str]`: Attempts to extract the image file extension (e.g., "jpeg", "png") from either a base64 data URL or a standard web URL. It normalizes "jpg" to "jpeg".
*   **Image Fetching**:
    *   `fetch_and_encode_image(url: str) -> Tuple[str, str]`: Given a standard web URL pointing to an image, this function fetches the image content, base64 encodes it, and returns a tuple containing the base64 encoded image string and its determined media type (e.g., "image/jpeg").

These core files provide foundational elements and helper utilities that support the various client, embedder, and text generation modules within the package.
