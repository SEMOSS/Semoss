# `MODEL` Engines

Model engines in SEMOSS serve as connectors to various machine learning models, including Large Language Models (LLMs), traditional ML models, and embedding generators. They provide a standardized way for the SEMOSS backend to invoke these models for tasks like question answering, text generation, instruction following, and creating vector embeddings. These engines typically extend `prerna.engine.impl.model.AbstractModelEngine` and implement `prerna.engine.api.IModelEngine`.

## Core Concepts for Model Engines

### `prerna.engine.api.IModelEngine` Interface

This interface defines the primary operations supported by model engines:

*   `ask(String question, String context, Insight insight, Map<String, Object> parameters)`: Used for conversational AI or question-answering. Takes a user's question, optional context, the current `Insight` (for session and user information), and model-specific hyperparameters. Returns an `AskModelEngineResponse`.
*   `embeddings(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters)`: Generates numerical vector embeddings for a list of input texts. Returns an `EmbeddingsModelEngineResponse`.
*   `imageEmbeddings(List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters)`: Generates embeddings for a list of images (often represented as paths or URLs). Returns an `EmbeddingsModelEngineResponse`.
*   `getModelType()`: Returns a `prerna.engine.api.ModelTypeEnum` (e.g., `OPEN_AI`, `BEDROCK`, `VERTEX_AI`, `LOCAL_PYTHON`, `TEXT_EMBEDDINGS_INFERENCE`) identifying the kind or provider of the model.
*   `buildOpenAIFunctionEngineToolMap()`: Generates a map structure representing the engine's capabilities, suitable for use as a "tool" in OpenAI's function calling or assistant APIs.

### `prerna.engine.impl.model.AbstractModelEngine` Class

This abstract class provides a common foundation for most model engine implementations.

*   **Lifecycle and Configuration**:
    *   Handles the `open(Properties smssProp)` method to load SMSS properties.
    *   Integrates with `prerna.io.connector.secrets.SecretsFactory` and `ISecrets` to securely retrieve API keys or other sensitive credentials defined in the SMSS file or an external secret store.
*   **Usage Tracking and Logging**:
    *   Manages flags like `keepConversationHistory` and `keepInputOutput` (or the combined `KEEP_CONTEXT`) from SMSS properties to control data retention.
    *   If model inference logging is enabled (`Utility.isModelInferenceLogsEnabled()`), it wraps the core model invocation calls (see below) with logic to record interaction details (prompt, response, tokens, timestamps, etc.) to the `ModelInferenceLogsDatabase` via `prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker`.
*   **User Usage Restrictions**:
    *   Integrates with `prerna.engine.impl.model.ModelUsageRestrictionUtility` to check if the user associated with the `Insight` has any usage restrictions (e.g., token limits, time limits) for the specific model engine or for models in general. It also updates the usage after a call.
*   **Abstract `*Call` Methods**:
    *   Defines protected abstract methods:
        *   `askCall(...)`
        *   `embeddingsCall(...)`
        *   `imageEmbeddingsCall(...)`
    *   Concrete subclasses must implement these methods to perform the actual, direct interaction with the model provider's API or the local model execution environment. The public-facing methods (`ask`, `instruct`, etc.) in `AbstractModelEngine` call these internal `*Call` methods after handling logging and restrictions.

### Extending for a New Model Provider/Type

To create a new `MODEL` engine for a different provider or a new type of local model:

1.  **Implement `IModelEngine`**.
2.  **Extend `AbstractModelEngine`**: This provides the boilerplate for configuration, logging, and usage restrictions.
3.  **Implement Core `*Call` Methods**:
    *   Provide concrete implementations for `askCall`, `embeddingsCall`, and/or `imageEmbeddingsCall` relevant to the new model's capabilities.
    *   Inside these methods:
        *   Initialize the specific API client for the model service (e.g., using REST clients, provider-specific SDKs).
        *   Retrieve necessary API keys or configurations from `this.smssProp`.
        *   Format the input parameters (question, context, task, hyperparameters) into the request structure expected by the model's API.
        *   Make the actual API call to the model service.
        *   Parse the response from the model service.
        *   Package the parsed response into the appropriate `prerna.engine.impl.model.responses.*ModelEngineResponse` object (e.g., `AskModelEngineResponse`, `EmbeddingsModelEngineResponse`), including details like the actual response content and token counts if available.
4.  **Define `ModelTypeEnum`**: If it's a new category of model, add a corresponding value to `prerna.engine.api.ModelTypeEnum`. The `getModelType()` method in your new engine should return this enum.
5.  **SMSS Configuration**: Define the necessary properties that users will need to set in the `.smss` file for your engine (e.g., API endpoint URL, API key property name, default model variant).
6.  **OpenAI Tool Definition (Optional)**: If the engine is intended to be used as a tool by OpenAI assistants or similar frameworks, implement `buildOpenAIFunctionEngineToolMap()` to return a valid tool/function definition.

## Example Implementations

### `prerna.engine.impl.model.OpenAiEngine`
*   **Purpose**: Integrates with OpenAI's suite of models, including GPT models for text generation and chat, and embedding models like Ada.
*   **Implementation Highlights**:
    *   Implements `askCall` and `embeddingsCall`.
    *   Uses HTTP client libraries to make REST API calls to the OpenAI API endpoints (e.g., `/v1/chat/completions`, `/v1/embeddings`).
    *   Constructs JSON request bodies according to OpenAI's API specifications and parses the JSON responses.
*   **SMSS Configuration**:
    *   `API_KEY` or `OPEN_AI_KEY`: The OpenAI API key.
    *   `MODEL_NAME`: The default OpenAI model ID to use (e.g., "gpt-3.5-turbo", "text-embedding-ada-002").
    *   `ORG_ID` (optional): OpenAI organization ID.
    *   Other model-specific parameters can often be passed through the `parameters` map in the `ask`/`embeddings` methods.

### `prerna.engine.impl.model.BedrockEngine`
*   **Purpose**: Connects to AWS Bedrock, a service that provides access to foundation models from various AI companies (e.g., Anthropic Claude, AI21 Labs Jurassic, Stability AI Stable Diffusion).
*   **Implementation Highlights**:
    *   Uses the AWS SDK for Java to interact with the Bedrock runtime service.
    *   The `askCall` (or equivalent) method needs to handle different request and response JSON structures because each foundation model on Bedrock has its own specific API schema for invocation.
*   **SMSS Configuration**:
    *   AWS credentials (access key, secret key, session token – often managed via environment variables or EC2 instance profiles when deployed on AWS).
    *   `REGION`: The AWS region where the Bedrock service is being used.
    *   `MODEL_ID`: The specific Amazon Bedrock model ARN (e.g., "anthropic.claude-v2", "amazon.titan-embed-text-v1").
    *   `CONTENT_TYPE`, `ACCEPT_TYPE`: Typically "application/json".

#### OpenAI models on Bedrock (Mantle)

AWS exposes OpenAI models on Bedrock through the OpenAI-compatible "Mantle" endpoint at `https://bedrock-mantle.<region>.api.aws/openai/v1`. The Java engine class is still `prerna.engine.impl.model.BedrockEngine` — the only difference is the `INIT_MODEL_ENGINE` template, which routes to `genai_client.OpenAiClient` with `provider='bedrock-mantle'` instead of `genai_client.AnthropicClient`.

The Python `OpenAiClient` wraps the standard OpenAI SDK with an httpx auth class that SigV4-signs each request. No separate Bedrock API key is required.

Credentials resolution follows the standard boto3 default chain when `AWS_ACCESS_KEY` / `AWS_SECRET_KEY` are omitted from the .smss: environment variables, `~/.aws/credentials`, AWS SSO, then EC2/ECS/Lambda instance role. Refreshable credentials (EC2 role, assumed role, SSO) auto-renew between requests, so the engine survives credential rotation without restart. `AWS_REGION` similarly falls back to the session's region (env var `AWS_REGION` / `AWS_DEFAULT_REGION` or EC2 instance metadata) if not set in the .smss.

*   **Example SMSS** (Responses API, `openai.gpt-5.4`):

    ```
    #Base Properties
    ENGINE_TYPE         prerna.engine.impl.model.BedrockEngine
    NAME                GPT 5.4 Bedrock
    MODEL_TYPE          BEDROCK
    PROVIDER            bedrock-mantle
    MODEL               openai.gpt-5.4
    AWS_REGION          us-east-2
    AWS_ACCESS_KEY      <access-key>
    AWS_SECRET_KEY      <secret-key>
    VAR_NAME            gpt54Model
    CHAT_TYPE           responses
    MAX_TOKENS          4096
    CONTEXT_WINDOW      400000

    INIT_MODEL_ENGINE   import genai_client;${VAR_NAME} = genai_client.OpenAiClient(is_azure=False, api_key='unused', model_name='${MODEL}', provider='${PROVIDER}', aws_region='${AWS_REGION}', aws_access_key='${AWS_ACCESS_KEY}', aws_secret_key='${AWS_SECRET_KEY}', chat_type='${CHAT_TYPE}', max_tokens=${MAX_TOKENS}, context_window=${CONTEXT_WINDOW})
    ```

*   **IAM prerequisites on the principal** (IAM user, EC2 role, or assumed role):
    *   `bedrock-mantle:CreateInference` on the relevant Mantle project resources.
    *   For `openai.gpt-5.4` specifically: an AWS Marketplace subscription to the OpenAI GPT-5.4 listing in the target region (subscribe via the AWS Marketplace console, or grant `aws-marketplace:CreateAgreementRequest` + `aws-marketplace:Subscribe` to let the principal auto-subscribe on first invoke).
    *   The principal does *not* need `bedrock:InvokeModel` for Mantle calls — Mantle is a separate IAM action namespace from the classic Bedrock runtime.

### `prerna.engine.impl.model.EmbeddedModelEngine`
*   **Purpose**: Designed to work with models that are hosted locally or managed directly by the SEMOSS instance. This is often used for Python-based models (e.g., from Hugging Face Transformers, scikit-learn).
*   **Implementation Highlights**:
    *   The `askCall` or other `*Call` methods would typically use `prerna.ds.py.PyTranslator` to invoke a local Python script.
    *   This Python script would be responsible for loading the model files and performing inference.
    *   Data and parameters are passed to the Python script, and results are returned, via the TCP communication channel managed by `PyTranslator`.
*   **SMSS Configuration**:
    *   `PYTHON_SCRIPT_PATH`: Path to the Python script that serves the model.
    *   `MODEL_FILE_PATH` (or similar): Path to the actual model files (e.g., weights, tokenizer files).
    *   Details about the Python virtual environment to use, if applicable.
    *   Any specific parameters required by the local model serving script.

### `prerna.engine.impl.model.TextEmbeddingsEngine`
*   **Purpose**: A specialized engine focused purely on generating text embeddings.
*   **Implementation Highlights**: While it implements `IModelEngine`, its primary role is through the `embeddingsCall` method. It might act as a wrapper or facade that delegates the actual embedding generation to another underlying model engine (like an `OpenAiEngine` configured for an embedding model, or a local `EmbeddedModelEngine` running a sentence transformer).
*   **SMSS Configuration**: Would typically include an `EMBEDDER_ENGINE_ID` property pointing to the ID of another configured `MODEL` engine that actually performs the embedding computation.
```
