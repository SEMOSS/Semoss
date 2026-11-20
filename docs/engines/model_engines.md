# `MODEL` Engines

Model engines in SEMOSS serve as connectors to various machine learning models, including Large Language Models (LLMs), traditional ML models, and embedding generators. They provide a standardized way for the SEMOSS backend to invoke these models for tasks like question answering, text generation, instruction following, and creating vector embeddings. These engines typically extend `prerna.engine.impl.model.AbstractModelEngine` and implement `prerna.engine.api.IModelEngine`.

## Models, rooms, and agents

A model engine supplies generation or embedding capabilities. A `Room` manages persistent conversation and tool-result continuation. An agent WORKSPACE supplies reusable instructions and resources, and the SEMOSS harness manages the model/tool loop. These are separate roles.

See [AskRoom](../pixel/ask_room.md) for a caller-managed turn, [the native harness](../agents/semoss_harness.md) for server-managed execution, and [agent configuration](../agents/agent_configuration.md#model-selection) for model selection precedence. Persistent room and agent services depend on the [model-inference database](../platform_services/internal_databases.md#model-inference-database).

The current [IModelEngine](../../src/prerna/engine/api/IModelEngine.java) includes `askRoom(InputMessage, Room, parameters)` for room-based calls and `getContextWindow()` for context sizing. [AbstractModelEngine](../../src/prerna/engine/impl/model/AbstractModelEngine.java) integrates room persistence and model invocation. The native harness uses room calls for tool continuation and uses the configured context window when deciding whether to compact history.

## Core Concepts for Model Engines

### `prerna.engine.api.IModelEngine` Interface

This interface defines the primary operations supported by model engines:

*   `askRoom(InputMessage inputMessage, Room room, Map<String, Object> parameters)`: Runs a model turn in a persistent room and returns an `AskModelEngineResponse`. Use this entry point for conversation and tool-result continuation.
*   `ask(String question, String context, Insight insight, Map<String, Object> parameters)`: Deprecated compatibility entry point for older callers.
*   `embeddings(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters)`: Generates numerical vector embeddings for a list of input texts. Returns an `EmbeddingsModelEngineResponse`.
*   `multiModalEmbeddings(text, image, video, insight, parameters)`: Optional text, image, and video embedding support. The default returns a response indicating that the operation is not implemented.
*   `getModelType()`: Returns a [ModelTypeEnum](../../src/prerna/engine/api/ModelTypeEnum.java), such as `OPEN_AI`, `BEDROCK`, `VERTEX`, `EMBEDDED`, `TEXT_EMBEDDINGS`, or `MODEL_ROUTER`.
*   `getContextWindow()` and `keepsConversationHistory()`: Expose context sizing and the configured conversation-history behavior.
*   `validateInputModalities(...)`: Validates outbound media where the implementation enforces configured input capabilities.
*   `supportsBatch()` and the batch submission/status/results operations: Optional native provider batch support. Check the capability before using these methods.

### `prerna.engine.impl.model.AbstractModelEngine` Class

This abstract class provides a common foundation for most model engine implementations.

*   **Lifecycle and Configuration**:
    *   Handles the `open(Properties smssProp)` method to load SMSS properties.
    *   Integrates with `prerna.io.connector.secrets.SecretsFactory` and `ISecrets` to securely retrieve API keys or other sensitive credentials defined in the SMSS file or an external secret store.
*   **Usage Tracking and Logging**:
    *   Reads `KEEP_CONVERSATION_HISTORY`; the shared engine base also reads `KEEP_INPUT_OUTPUT`. Persistent room messages and inference usage records are distinct stores; see [internal databases](../platform_services/internal_databases.md#model-inference-database).
    *   If model inference logging is enabled (`Utility.isModelInferenceLogsEnabled()`), it wraps the core model invocation calls (see below) with logic to record interaction details (prompt, response, tokens, timestamps, etc.) to the `ModelInferenceLogsDatabase` via `prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker`.
*   **User Usage Restrictions**:
    *   Integrates with `prerna.engine.impl.model.ModelUsageRestrictionUtility` to check if the user associated with the `Insight` has any usage restrictions (e.g., token limits, time limits) for the specific model engine or for models in general. It also updates the usage after a call.
*   **Abstract `*Call` Methods**:
    *   Defines protected abstract methods:
        *   `askCall(InputMessage inputMessage, Insight insight, String roomId, Map<String, Object> parameters)`
        *   `embeddingsCall(...)`
    *   Concrete subclasses implement these methods to interact with the provider or model execution environment. The public `askRoom` and `embeddings` methods apply the shared lifecycle around those calls. The older string-based `askCall` overload is deprecated.
*   **Model Metadata**:
    *   Resolves token limits from security-database model metadata, with legacy SMSS fallback when applicable, and exposes the resulting context window to callers.
    *   Applies configured input modalities, reasoning settings, and other model capabilities when preparing requests.

### Extending for a New Model Provider/Type

To create a new `MODEL` engine for a different provider or a new type of local model:

1.  **Implement `IModelEngine`**.
2.  **Extend `AbstractModelEngine`**: This provides the boilerplate for configuration, logging, and usage restrictions.
3.  **Implement Core `*Call` Methods**:
    *   Implement the current `askCall` and `embeddingsCall` signatures, explicitly handling unsupported operations. Add optional multimodal or batch operations only when the provider supports them.
    *   Inside these methods:
        *   Initialize the specific API client for the model service (e.g., using REST clients, provider-specific SDKs).
        *   Retrieve necessary API keys or configurations from `this.smssProp`.
        *   Format the input parameters (question, context, task, hyperparameters) into the request structure expected by the model's API.
        *   Make the actual API call to the model service.
        *   Parse the response from the model service.
        *   Package the parsed response into the appropriate `prerna.engine.impl.model.responses.*ModelEngineResponse` object (e.g., `AskModelEngineResponse`, `EmbeddingsModelEngineResponse`), including details like the actual response content and token counts if available.
4.  **Define `ModelTypeEnum`**: If it's a new category of model, add a corresponding value to `prerna.engine.api.ModelTypeEnum`. The `getModelType()` method in your new engine should return this enum.
5.  **SMSS Configuration**: Define the necessary properties that users will need to set in the `.smss` file for your engine (e.g., API endpoint URL, API key property name, default model variant).
6.  **Verify Agent Compatibility**: For generation engines used by the harness, verify room history, tool-call IDs and results, configured modalities, context sizing, and cancellation/error handling. Register accurate model capabilities so an embedding-only engine is not selected for an agent run.

## Example Implementations

### `prerna.engine.impl.model.OpenAiEngine`

[OpenAiEngine](../../src/prerna/engine/impl/model/OpenAiEngine.java) extends [AbstractPythonModelEngine](../../src/prerna/engine/impl/model/AbstractPythonModelEngine.java). The shared Java implementation invokes a configured Python client; request formatting and provider SDK calls live in `py/genai_client`. `INIT_MODEL_ENGINE` initializes that client, using properties substituted from the engine configuration. Credentials, model names, endpoints, and supported operations depend on the selected client and template. See the [GenAI client documentation](../python_genai_client/README.md).

### `prerna.engine.impl.model.BedrockEngine`

[BedrockEngine](../../src/prerna/engine/impl/model/BedrockEngine.java) also extends `AbstractPythonModelEngine`. Its initialization template selects the Python provider client and passes the configured model, region, and credentials. Provider-specific request construction is handled by that client. See the [Bedrock client guide](../python_genai_client/text_generation/bedrock.md) and the existing Mantle configuration example below.

#### OpenAI models on Bedrock (Mantle)

AWS exposes OpenAI models on Bedrock through the OpenAI-compatible "Mantle" endpoint at `https://bedrock-mantle.<region>.api.aws/openai/v1`. The Java engine class is still `prerna.engine.impl.model.BedrockEngine` — the only difference is the `INIT_MODEL_ENGINE` template, which routes to `genai_client.OpenAiClient` with `provider='bedrock'` instead of `genai_client.AnthropicClient`.

The Python `OpenAiClient` wraps the standard OpenAI SDK with an httpx auth class that SigV4-signs each request. No separate Bedrock API key is required.

Credentials resolution follows the standard boto3 default chain when `AWS_ACCESS_KEY` / `AWS_SECRET_KEY` are omitted from the .smss: environment variables, `~/.aws/credentials`, AWS SSO, then EC2/ECS/Lambda instance role. Refreshable credentials (EC2 role, assumed role, SSO) auto-renew between requests, so the engine survives credential rotation without restart. `AWS_REGION` similarly falls back to the session's region (env var `AWS_REGION` / `AWS_DEFAULT_REGION` or EC2 instance metadata) if not set in the .smss.

*   **Example SMSS** (Responses API, `openai.gpt-5.4`):

    ```
    #Base Properties
    ENGINE_TYPE         prerna.engine.impl.model.BedrockEngine
    NAME                GPT 5.4 Bedrock
    MODEL_TYPE          BEDROCK
    PROVIDER            bedrock
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

[EmbeddedModelEngine](../../src/prerna/engine/impl/model/EmbeddedModelEngine.java) uses the shared Python engine lifecycle and returns model type `EMBEDDED`. Configure its Python client through `INIT_MODEL_ENGINE`, including the model-specific loading and inference options that client accepts. The shared base handles process startup, the client variable, and Java/Python calls; see [Java–Python communication](../platform_services/java_python_communication.md).

### `prerna.engine.impl.model.TextEmbeddingsEngine`

[TextEmbeddingsEngine](../../src/prerna/engine/impl/model/TextEmbeddingsEngine.java) extends `AbstractRESTModelEngine`. It requires `ENDPOINT` and accepts `BATCH_SIZE` (default `32`). The engine sends batches of text as JSON containing `inputs` and `truncate: true`, then parses a list of embedding vectors. It does not support text generation and should not be selected as an agent's generation model.
