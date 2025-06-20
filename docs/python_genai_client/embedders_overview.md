# Embedders (`py/genai_client/embedders/`)

The `py/genai_client/embedders/` sub-package provides classes for generating vector embeddings from textual (and sometimes image) data. These embeddings are numerical representations that capture semantic meaning, making them suitable for tasks like semantic search, clustering, and as input to various machine learning models. Each embedder class typically wraps a specific embedding model or service provider.

## Core Concepts

### `abstract_embedder.py`

This file defines the `AbstractEmbedder` class, an abstract base class (ABC) that all concrete embedder implementations inherit from.

*   **Purpose**: To establish a common interface and share foundational functionalities for all embedders.
*   **Key Attributes**:
    *   `model_name` (str): The identifier for the specific embedding model being used.
    *   `tokenizer` (`AbstractTokenizer`): An instance of a tokenizer compatible with the embedding model, used for tasks like counting tokens to respect context window limits, especially when batching inputs.
*   **Abstract Methods (to be implemented by subclasses)**:
    *   `_get_tokenizer(self, init_args: Dict) -> AbstractTokenizer`: Must be implemented to return an instance of `AbstractTokenizer` suitable for the specific embedding model.
    *   `embeddings_call(self, strings_to_embed: List[str], **kwargs: Any) -> EmbeddingsModelEngineResponse`: The core method that takes a list of strings and returns an `EmbeddingsModelEngineResponse` containing the list of numerical embeddings and token counts.
    *   `image_embeddings_call(self, images_to_embed: List[str], **kwargs: Any) -> EmbeddingsModelEngineResponse`: Similar to `embeddings_call`, but for generating embeddings from a list of images (e.g., image file paths or base64 encoded strings).
*   **Public Methods**:
    *   `embeddings(self, strings_to_embed: List[str], **kwargs: Any) -> Dict`: A public wrapper around `embeddings_call` that returns the response as a dictionary.
    *   `image_embeddings(self, images_to_embed: List[str], **kwargs: Any) -> Dict`: A public wrapper around `image_embeddings_call`.
    *   `ask(...)`: By default, raises a "model does not support text generation" message, as embedders are specialized for embedding generation, not chat/text generation.
*   **KeyBERT Integration**:
    *   `to_keybert_embedder(self) -> keybert.backend.BaseEmbedder`: A utility method that wraps the `AbstractEmbedder` instance into a format compatible with the [KeyBERT](https://github.com/MaartenGr/KeyBERT) library for keyword extraction. This allows SEMOSS embedders to be seamlessly used as the embedding backend for KeyBERT.

### Concrete Embedder Implementations

Each concrete embedder class in this package implements the `AbstractEmbedder` and provides the logic to interact with a specific embedding service or model type.

Common responsibilities include:
*   Initializing the client for the target service (e.g., OpenAI API client, AWS Bedrock client).
*   Implementing `_get_tokenizer()` to provide the correct tokenizer for the model.
*   Implementing `embeddings_call()` (and `image_embeddings_call()` if applicable) by:
    *   Formatting the input strings/images according to the service's API requirements.
    *   Making the API call to the embedding service.
    *   Parsing the response to extract the numerical embeddings and token information.
    *   Returning an `EmbeddingsModelEngineResponse`.
*   Handling potential batching of inputs if the API has limits on the number or total size of inputs per request.

## Available Embedders

*(This list will be populated based on the files found in the directory)*

### `openai_embedder.py` - `OpenAiEmbedder`
*   **Purpose**: Generates embeddings using OpenAI's embedding models (e.g., `text-embedding-ada-002`).
*   **Initialization**: Requires `model_name` and `api_key`. Can also take `max_tokens` for the tokenizer and other `openai.OpenAI` client parameters.
*   **Functionality**:
    *   Uses `OpenAiTokenizer`.
    *   Implements `embeddings_call` by calling the `client.embeddings.create()` method of the OpenAI Python SDK.
    *   Supports batching of input texts if the total token count exceeds the model's limit, splitting the input into multiple API calls.
    *   Also implements `image_embeddings_call`, suggesting it can handle image inputs for compatible OpenAI models.

### `bedrock_embedder.py` - `BedrockEmbedder`
*   **Purpose**: Generates embeddings using models hosted on AWS Bedrock (e.g., Amazon Titan embedding models, Cohere embedding models via Bedrock).
*   **Initialization**: Requires `model_name` (or `modelId`), AWS `access_key`, `secret_key`, and `region`. For Cohere models, `cohere_input_type` (e.g., "search_document") can be specified.
*   **Functionality**:
    *   Uses the `boto3` AWS SDK to interact with the "bedrock-runtime" service.
    *   `embeddings_call` iterates through input texts, constructs a model-specific JSON request body (different for Titan vs. Cohere), and calls `client.invoke_model()`.
    *   `_get_tokenizer()` currently returns `None`, implying token counting relies on information from the API response or is handled differently.
    *   `image_embeddings_call` is not implemented (raises `NotImplementedError`).

### `azure_openai_embedder.py` - `AzureOpenAiEmbedder`
*   *(To be detailed after reading the file - likely similar to `OpenAiEmbedder` but configured for Azure OpenAI service endpoints and authentication.)*

### `local_embedder.py` - `LocalEmbedder`
*   *(To be detailed after reading the file - likely uses sentence-transformers or similar libraries to run embedding models locally.)*

### `textgen_embedder.py` - `TextgenEmbedder`
*   *(To be detailed after reading the file - likely interacts with a Text Generation Inference (TGI) server or a similar local/remote model serving endpoint that provides embeddings.)*

### `vertex_embedder.py` - `VertexEmbedder`
*   *(To be detailed after reading the file - likely for models on Google Cloud Vertex AI, using the Vertex AI SDK.)*

---

*Note: The specific parameters and initialization methods for each embedder will vary. Refer to the individual class documentation (once created or by inspecting the source) for precise details.*
