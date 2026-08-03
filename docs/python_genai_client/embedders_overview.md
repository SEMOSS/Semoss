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
*   **Public Methods**:
    *   `embeddings(self, strings_to_embed: List[str], **kwargs: Any) -> Dict`: A public wrapper around `embeddings_call` that returns the response as a dictionary.
    *   `multi_modal_embeddings(self, text: List[str] = None, image: List[str] = None, video: List[str] = None, **kwargs: Any) -> Dict`: Optional capability for embedding text/image/video together, broken out by modality. Embedders that don't override it report "not implemented".
    *   `ask(...)`: By default, raises a "model does not support text generation" message, as embedders are specialized for embedding generation, not chat/text generation.
*   **KeyBERT Integration**:
    *   `to_keybert_embedder(self) -> keybert.backend.BaseEmbedder`: A utility method that wraps the `AbstractEmbedder` instance into a format compatible with the [KeyBERT](https://github.com/MaartenGr/KeyBERT) library for keyword extraction. This allows SEMOSS embedders to be seamlessly used as the embedding backend for KeyBERT.

### Concrete Embedder Implementations

Each concrete embedder class in this package implements the `AbstractEmbedder` and provides the logic to interact with a specific embedding service or model type.

Common responsibilities include:
*   Initializing the client for the target service (e.g., OpenAI API client, AWS Bedrock client).
*   Implementing `_get_tokenizer()` to provide the correct tokenizer for the model.
*   Implementing `embeddings_call()` by:
    *   Formatting the input strings/images according to the service's API requirements.
    *   Making the API call to the embedding service.
    *   Parsing the response to extract the numerical embeddings and token information.
    *   Returning an `EmbeddingsModelEngineResponse`.
*   Handling potential batching of inputs if the API has limits on the number or total size of inputs per request.

## Available Embedders

*(This list will be populated based on the files found in the directory)*

### `openai_embedder.py` - `OpenAiEmbedder`
*   **Purpose**: Generates embeddings using OpenAI's embedding models (e.g., `text-embedding-ada-002`).
*   **Initialization**: Requires `model_name` (OpenAI model ID) and `api_key`. Can also take `max_tokens` for the tokenizer and other `openai.OpenAI` client parameters (like `base_url`).
*   **Functionality**:
    *   Uses `OpenAiTokenizer`.
    *   Implements `embeddings_call` by calling the `client.embeddings.create()` method of the OpenAI Python SDK.
    *   Supports batching of input texts if the total token count exceeds the model's limit, splitting the input into multiple API calls.

### `bedrock_embedder.py` - `BedrockEmbedder`
*   **Purpose**: Generates embeddings using models hosted on AWS Bedrock (e.g., Amazon Titan embedding models, Cohere embedding models via Bedrock).
*   **Initialization**: Requires `model_name` (or `modelId` - the Bedrock model identifier), AWS `access_key`, `secret_key`, and `region`. For Cohere models, `cohere_input_type` (e.g., "search_document", "search_query") can be specified.
*   **Functionality**:
    *   Uses the `boto3` AWS SDK to interact with the "bedrock-runtime" service.
    *   `embeddings_call` iterates through input texts one by one, constructs a model-specific JSON request body (different for Titan vs. Cohere via `createJsonObjForModel`), and calls `client.invoke_model()`.
    *   `_get_tokenizer()` returns `None`; token counting often relies on information from the Bedrock API response itself or is managed differently.

### `azure_openai_embedder.py` - `AzureOpenAiEmbedder`
*   **Purpose**: Generates embeddings using OpenAI models deployed via the Azure OpenAI service.
*   **Inheritance**: Extends `OpenAiEmbedder`.
*   **Initialization**: Requires `model_name` (this is the Azure deployment ID for the embedding model), `api_key` (Azure OpenAI API key), `endpoint` (Azure OpenAI service endpoint URL), and `api_version`. An optional `openai_model_name` can be provided if the deployment name doesn't map to a standard tiktoken model name for tokenizer initialization (defaults to "text-embedding-ada-002" for tokenizer if direct mapping fails).
*   **Functionality**:
    *   Overrides `_get_client()` to return an `openai.AzureOpenAI` client instance, configured with Azure-specific parameters (`azure_endpoint`, `api_version`).
    *   The core embedding logic, including batching and API calls via `client.embeddings.create()`, is inherited from `OpenAiEmbedder`.

### `local_embedder.py` - `LocalEmbedder`
*   **Purpose**: Generates embeddings using sentence-transformer models running locally on the SEMOSS server.
*   **Initialization**:
    *   `model_name` (str): Hugging Face model identifier (e.g., "sentence-transformers/all-MiniLM-L6-v2").
    *   `model_path` (Optional[str]): Path to a locally downloaded model directory. If not provided, the class attempts to download/load from Hugging Face Hub cache via `try_to_load_from_cache` or `snapshot_download`.
    *   `device_number` (Optional[Union[int, float]]): Specifies the GPU device number if CUDA is available (e.g., 0, 1). Defaults to CPU or "auto" device mapping if not specified or CUDA not available.
*   **Functionality**:
    *   `_get_tokenizer()`: Returns a `HuggingfaceTokenizer` initialized with `model_name`.
    *   `get_embedder()`: Loads the model using `sentence_transformers.SentenceTransformer(self.model_folder, device=self.device)`. Falls back to `transformers.AutoModel.from_pretrained(...)` if the first attempt fails.
    *   `embeddings_call()`: Uses the loaded model's `encode()` method (standard for sentence-transformers) to generate embeddings.
    *   **Keyword Extraction**: Includes methods (`keyword_extraction`, `get_key_bert_model`, `get_text_keywords`) to integrate with the KeyBERT library, using the loaded local embedding model as the backend for KeyBERT to extract keywords from text.

### `textgen_embedder.py` - `TextEmbeddingsInference`
*   **Purpose**: Generates embeddings by making API calls to a running Text Generation Inference (TGI) server endpoint. TGI is a Hugging Face tool for serving transformer models.
*   **Inheritance**: Extends `LocalEmbedder` (primarily for tokenizer initialization structure, though it targets a remote TGI server).
*   **Initialization**:
    *   `endpoint` (str): The URL of the TGI server's embedding endpoint (often `/embed`).
    *   `model_name` (str): The Hugging Face model name that the TGI server is serving (used to initialize a compatible `HuggingfaceTokenizer`).
    *   `batch_size` (int, optional): Batch size for sending requests to the TGI server (default 32).
*   **Functionality**:
    *   `_get_tokenizer()`: Returns a `HuggingfaceTokenizer` based on `model_name`.
    *   `get_embedder()`: Returns an instance of an inner class `TextGenEmbedder`. This inner class's `encode()` method:
        *   Batches the input sentences according to `batch_size`.
        *   Makes POST requests to the TGI `endpoint` with a JSON payload like `{"inputs": sentences_batch, "truncate": True}`.
        *   Collects and returns the embeddings from the TGI server responses.
    *   `embeddings_call()`: Uses the `TextGenEmbedder.encode()` method.

### `vertex_embedder.py` - `VertexAiEmbedder`
*   **Purpose**: Generates embeddings using models available on Google Cloud Vertex AI (e.g., "textembedding-gecko").
*   **Initialization**:
    *   `model_name` (str): The Vertex AI model identifier (e.g., "textembedding-gecko@001").
    *   `region` (str): The Google Cloud region.
    *   `service_account_credentials` (Dict, optional) or `service_account_key_file` (str, optional): For authentication.
    *   `project` (str, optional): Google Cloud project ID.
    *   Calls `google_initializer` to set up the global `google.cloud.aiplatform` context.
*   **Functionality**:
    *   `_get_tokenizer()`: Returns `None`; the `self.tokenizer` attribute is later set to the Vertex AI `TextEmbeddingModel` client itself, as it provides a `count_tokens` method.
    *   `embeddings_call()`:
        *   Uses a helper method `_encode_text_to_embedding_batched` to handle batching and API calls.
        *   This helper uses `self.client.get_embeddings(sentences)` from the `vertexai.preview.language_models.TextEmbeddingModel` SDK.
        *   Implements batching (default batch size 5) and rate limiting (default 10 API calls per second using `ThreadPoolExecutor` and `time.sleep`) when calling the Vertex AI API.

---

*Note: The specific parameters and initialization methods for each embedder will vary. Refer to the individual class documentation (once created or by inspecting the source) for precise details.*
