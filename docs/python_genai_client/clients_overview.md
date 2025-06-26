# GenAI Client Initializers and Wrappers (`py/genai_client/clients/`)

The `py/genai_client/clients/` sub-package contains modules responsible for initializing connections and providing client objects for various Generative AI services. This typically involves handling authentication, setting up service-specific configurations, and wrapping the provider's native SDK client.

## 1. `client_initializer.py`

This module provides utility functions for global or foundational client initializations, particularly for services that require a shared context or project setup before specific service clients can be used.

### `google_initializer()`

*   **Purpose**: Initializes the Google Cloud AI Platform context using the `google.cloud.aiplatform` SDK. This setup is often a prerequisite for using various Vertex AI services.
*   **Key Parameters**:
    *   `region` (str): The Google Cloud region where the project is hosted (e.g., "us-central1").
    *   `service_account_credentials` (Dict, optional): A Python dictionary containing Google service account key information.
    *   `service_account_key_file` (str, optional): Path to a JSON file containing the service account key.
    *   `project` (str, optional): The Google Cloud Project ID. If not provided, it attempts to infer it from the service account credentials.
*   **Functionality**:
    1.  Loads Google service account credentials, either from a dictionary or a JSON key file.
    2.  Obtains scoped credentials suitable for AI Platform interactions.
    3.  Initializes the AI Platform globally for the current process using `aiplatform.init(project=..., location=..., credentials=...)`.
*   **Usage**: This function would typically be called once before interacting with Vertex AI-based clients (like those for Vertex AI text generation or embeddings) to ensure the environment is correctly configured to communicate with the specified Google Cloud project and region.

## 2. `google_clients.py`

This module provides a client wrapper, `GoogleClient`, for interacting with Google's Generative AI services, including models available directly via the Google Generative AI SDK (e.g., Gemini API with an API key) and models accessible through Google Cloud Vertex AI (which can include both Google's models and models from providers like Anthropic).

### `GoogleClientType(StringEnum)`

An enumeration defining the type of Google-hosted client being configured:
*   `GOOGLE`: For Google's own models (e.g., Gemini, PaLM accessed via `genai` SDK or Vertex AI).
*   `ANTHROPIC`: For Anthropic's models (e.g., Claude) when accessed via the Google Cloud Vertex AI platform.

### `GoogleClientConfig(pydantic.BaseModel)`

A Pydantic model for specifying the configuration for the `GoogleClient`.
*   `type` (`GoogleClientType`): Specifies whether the client is for `GOOGLE` or `ANTHROPIC` (via Vertex).
*   `service_account_credentials` (Dict, optional): Service account JSON key as a dictionary.
*   `service_account_key_file` (str, optional): Path to the service account JSON key file. (Required for Vertex AI access).
*   `region` (str, optional): The Google Cloud region (e.g., "us-central1"). (Required for Vertex AI access).
*   `project` (str, optional): The Google Cloud Project ID. (Required for Vertex AI access).
*   `api_key` (str, optional): An API key for directly accessing Google's Generative AI services (e.g., Gemini API without Vertex AI).

### `GoogleClient` Class

*   **Purpose**: Acts as a factory and wrapper for Google's `genai.Client` or Anthropic's `AnthropicVertex` client.
*   **Initialization `__init__(self, config: GoogleClientConfig)`**:
    *   Takes a `GoogleClientConfig` object.
    *   If service account details are provided, it loads them using `_load_credentials` to obtain scoped `google.oauth2.service_account.Credentials`.
    *   Calls `_get_client()` to instantiate the appropriate underlying client.
*   **`_get_google_client()`**:
    *   If `config.api_key` is present, it initializes `genai.Client(api_key=...)`. This is typically used for direct access to APIs like the Gemini API.
    *   If `config.project`, `config.region`, and service account credentials are provided, it initializes `genai.Client(credentials=..., vertexai=True, location=..., project=...)`. This configures the `genai` client to interact with models hosted on Vertex AI.
*   **`_get_anthropic_client()`**:
    *   Requires `config.project`, `config.region`, and service account credentials.
    *   Initializes an `anthropic.AnthropicVertex` client, configuring it to use the specified Google Cloud project and region for accessing Anthropic models on Vertex AI.
*   **Usage**: An instance of `GoogleClient` would hold the configured native client (`self.client`), which can then be used by other components like text generation clients or embedders.

**Conceptual Example**:

```python
# from py.genai_client.clients.google_clients import GoogleClient, GoogleClientConfig, GoogleClientType
# from py.genai_client.clients.client_initializer import google_initializer

# # Scenario 1: Using Google's GenAI SDK directly with an API Key (e.g., for Gemini)
# gemini_config = GoogleClientConfig(type=GoogleClientType.GOOGLE, api_key="YOUR_GEMINI_API_KEY")
# gemini_client_wrapper = GoogleClient(config=gemini_config)
# native_gemini_client = gemini_client_wrapper.client # This is a genai.Client instance

# # Scenario 2: Using a Google model via Vertex AI
# # First, ensure AI Platform is initialized (might be done globally by a calling process)
# # google_initializer(region="us-central1", service_account_key_file="/path/to/sa-key.json", project="my-gcp-project")
# vertex_google_config = GoogleClientConfig(
#     type=GoogleClientType.GOOGLE,
#     service_account_key_file="/path/to/sa-key.json", # or service_account_credentials
#     region="us-central1",
#     project="my-gcp-project"
# )
# vertex_google_client_wrapper = GoogleClient(config=vertex_google_config)
# native_vertex_google_client = vertex_google_client_wrapper.client # This is a genai.Client configured for Vertex

# # Scenario 3: Using an Anthropic model via Vertex AI
# # AI Platform should be initialized as above
# vertex_anthropic_config = GoogleClientConfig(
#     type=GoogleClientType.ANTHROPIC,
#     service_account_key_file="/path/to/sa-key.json",
#     region="us-central1",
#     project="my-gcp-project"
# )
# vertex_anthropic_client_wrapper = GoogleClient(config=vertex_anthropic_config)
# native_vertex_anthropic_client = vertex_anthropic_client_wrapper.client # This is an AnthropicVertex instance
```

This structure allows the `genai_client` to support different Google-provided GenAI services and models accessed through various authentication and hosting mechanisms (direct API vs. Vertex AI).
