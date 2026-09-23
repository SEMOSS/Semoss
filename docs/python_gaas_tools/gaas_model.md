# GAAS Model Interaction (`gaas_gpt_model.py`)

The `py/gaas_gpt_model.py` module provides classes for interacting with various language models from a Python environment that is integrated with the SEMOSS backend (often running within a Tomcat server environment) or potentially using locally defined models. It defines an abstraction for model operations and provides implementations that can proxy requests to the SEMOSS Java backend or load models directly in Python.

## `AbstractModelEngine` (ABC)

*   **Purpose**: This abstract base class defines a common interface for all model engine implementations within this module. It ensures that different model interaction methods are consistently available.
*   **Abstract Methods**: Subclasses are required to implement:
    *   `get_model_type(*args, **kwargs) -> str`: Returns a string indicating the type of API or model being used.
    *   `ask(*args, **kwargs) -> List[Dict]`: For text generation or chat-like interactions. Expected to return a list of dictionaries containing the response and metadata.
    *   `embeddings(*args, **kwargs) -> List[Dict]`: For generating vector embeddings from input strings.
    *   `keyword_extraction(*args, **kwargs) -> List[Any]`: For extracting keywords from text.
    *   `do_call(method_name: str, input: Any, **kwargs) -> Any`: A generic method to call other specific, uniquely named methods on the engine.
    *   `get_model_engine_id() -> str`: Returns the SEMOSS engine ID if applicable.

## `TomcatModelEngine`

*   **Purpose**: This class implements `AbstractModelEngine` and acts as a proxy to a model engine configured within the SEMOSS Java backend (running on Tomcat). It allows Python code (often executed via SEMOSS's Python integration) to leverage models that are managed by the main SEMOSS platform.
*   **Inheritance**: Extends `AbstractModelEngine` and `gaas_server_proxy.ServerProxy`. The `ServerProxy` handles the communication back to the Java backend.
*   **Initialization `__init__(self, engine_id: str, insight_id: Optional[str] = None, **kwargs)`**:
    *   `engine_id` (str): **Required**. The ID of the target SEMOSS model engine (an `IModelEngine` instance) configured in the Java backend.
    *   `insight_id` (Optional[str]): The ID of the current insight, used for context in backend operations.
*   **Key Methods**:
    *   `get_model_type(insight_id: Optional[str] = None) -> str`:
        *   Executes a Pixel script `GetModelAPI(model="<engine_id>");` via `super().callReactor()` to ask the SEMOSS backend for the API type of the configured engine.
    *   `ask(question: str, context: Optional[str] = None, use_history: Optional[bool] = True, param_dict: Optional[Dict] = None, insight_id: Optional[str] = None) -> List[Dict]`:
        *   Constructs a Pixel script: `LLM(engine="<engine_id>", command="<question>", useHistory=<use_history>, context=["<context>"], paramValues=[<param_dict>]);`.
        *   Executes this Pixel script via `super().callReactor()`.
        *   Returns the response from the LLM, typically including the generated text and token counts.
    *   `ner(text: str, entities: List[str], mask_entities: List[str] = [], param_dict: Optional[Dict] = None, insight_id: Optional[str] = None)`:
        *   Executes an `NER(...)` Pixel command for Named Entity Recognition.
    *   `get_conversation_history(insight_id: Optional[str] = None) -> List[Dict]`:
        *   Executes a `GetRoomMessages(roomId="<insight_id>");` Pixel script to retrieve chat history from the `ModelInferenceLogsDatabase`.
    *   `embeddings(strings_to_embed: List[str], param_dict: Optional[Dict] = None, insight_id: Optional[str] = None) -> List[Dict]`:
        *   Constructs an `Embeddings(engine="<engine_id>", values=<strings_to_embed>, paramValues=[<param_dict>]);` Pixel script.
        *   Executes it via `super().callReactor()`.
    *   `keyword_extraction(input: List[str], param_dict: Optional[Dict] = None, insight_id: Optional[str] = None)`:
        *   Executes an `EmbedderKeywordExtraction(...)` Pixel command.
    *   `get_model_engine_id() -> str`: Returns the `self.engine_id`.
*   **Interaction**: All operations are translated into Pixel scripts and executed on the SEMOSS Java backend through the `ServerProxy`. This means the actual model interaction (e.g., calling OpenAI, Bedrock) is handled by the Java `IModelEngine` implementation corresponding to `engine_id`.

## `HuggingFacePipelineModelEngine`

*   **Purpose**: Implements `AbstractModelEngine` for using Hugging Face `transformers` pipelines directly within the Python environment where this code is running (potentially a local Python interpreter or a Python environment managed by SEMOSS).
*   **Initialization `__init__(self, engine_id: str, pipeline_type: Optional[str] = None, **kwargs)`**:
    *   `engine_id` (str): The Hugging Face model identifier (e.g., "distilbert-base-uncased-finetuned-sst-2-english") or path to a local model.
    *   `pipeline_type` (Optional[str]): The type of Hugging Face pipeline to create (e.g., "text-generation", "feature-extraction" for embeddings, "question-answering").
*   **Key Methods**:
    *   `get_model_type(...)`: Returns `self.pipeline_type`.
    *   `ask(...)`: Raises `NotImplementedError`, indicating this class might be more focused on other tasks like embeddings or specific pipeline operations.
    *   `embeddings(strings_to_embed: List[str], ...)`: If the pipeline is for feature extraction, it likely uses `self.pipe.model.encode(strings_to_embed)` to generate embeddings.
    *   `keyword_extraction(input: Any, ...)`: Directly uses `self.pipe(input)` if the pipeline is suited for this (e.g., a feature-extraction pipeline might be adapted, or it might expect a specific keyword extraction pipeline).
*   **Interaction**: Loads and runs Hugging Face models locally using the `transformers` library.

## `LocalModelEngine`

*   **Purpose**: A wrapper class that can load and use a Python-based model engine locally. It can initialize the model engine either from an existing instance or by dynamically loading it based on an SMSS file configuration.
*   **Initialization `__init__(self, model_engine: Any = None, engine_id: Optional[str] = None, engine_smss_file_path: Optional[str] = None, semoss_dev_path: Optional[str] = ...)`**:
    *   If `model_engine` (an already instantiated model engine object) is provided, it uses that directly.
    *   If `engine_id` or `engine_smss_file_path` is provided, it attempts to:
        1.  Find the SMSS file using `get_model_smss_file()`.
        2.  Read the SMSS properties using `read_smss_file()`.
        3.  Construct and execute a Python command string (from `INIT_MODEL_ENGINE` property in SMSS, with placeholders like `${API_KEY}` substituted) to instantiate the model engine. The instantiated object is expected to be assigned to a variable named by the `VAR_NAME` property in the SMSS.
*   **Key Methods**: Mostly delegates calls (`get_model_type`, `ask`, `embeddings`, `keyword_extraction`) to the underlying `self.local_model_engine` instance.
*   **Static Helper Methods**:
    *   `get_model_smss_file()`: Locates an engine's SMSS file in a SEMOSS model directory.
    *   `read_smss_file()`: Parses an SMSS file into a dictionary.
    *   `get_init_model_commads()`: Formats the `INIT_MODEL_ENGINE` string from SMSS by substituting placeholders.
*   **Interaction**: This class is designed to make locally defined Python model engines (which might themselves use `genai_client` or other libraries) conform to the `AbstractModelEngine` interface.

## `ModelEngine` (Factory Class)

*   **Purpose**: Acts as a factory to provide an instance of a model engine, primarily choosing between `TomcatModelEngine` (for interacting with the SEMOSS backend) and `LocalModelEngine`.
*   **Initialization `__init__(self, model_engine_class: Optional[str] = "TOMCAT", **kwargs)`**:
    *   `model_engine_class` (str, default: "TOMCAT"): Determines the type of engine to create. Can be "TOMCAT" or "LOCAL". "HF_PIPELINE" is mentioned but not fully implemented in the constructor logic shown.
    *   `**kwargs`: Passed to the constructor of the chosen model engine class (e.g., `engine_id` for `TomcatModelEngine`).
*   **Key Methods**: All methods (`get_model_type`, `ask`, `instruct`, `embeddings`, `keyword_extraction`, `ner`, `do_call`, `get_model_engine_id`, `get_conversation_history`) are wrappers that delegate the call to the underlying `self.model_engine` instance.
*   **Langchain Integration**:
    *   `to_langchain_embedder()`: Wraps the `ModelEngine` to make its `embeddings` method compatible with Langchain's `Embeddings` interface.
    *   `to_langchain_chat_model()`: Wraps the `ModelEngine` to make its `ask` and `get_conversation_history` methods compatible with Langchain's `BaseChatModel` interface. This includes converting message formats.

### Overall Relationship

The `ModelEngine` factory class is the primary entry point.
- If configured for "TOMCAT" (default), it uses `TomcatModelEngine`, which then uses `ServerProxy` to send Pixel commands to the SEMOSS Java backend. The Java backend would then use its own `IModelEngine` implementations (which might internally use the `py/genai_client` for specific providers like OpenAI, Bedrock, etc., or call local Java models).
- If configured for "LOCAL", it uses `LocalModelEngine`, which loads a Python model engine based on SMSS configurations. This local Python engine could be an instance from the `py/genai_client` library or any other custom Python model class.

This structure provides flexibility, allowing GAAS tools to interact with models managed by the main SEMOSS platform or with models defined and executed purely within the Python environment where the GAAS tool is running.
