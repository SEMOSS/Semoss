# SEMOSS Engine Implementations Deep Dive

This document provides an in-depth look at the various `IEngine` implementations within SEMOSS, categorized by their `CATALOG_TYPE`. It covers their purpose, key abstract parent classes, important methods for specific functionalities, and guidance on how to extend them for new, custom implementations.

## 1. `DATABASE` Engines

Engines of this type provide connectivity to various database systems, enabling data querying, manipulation, and metadata discovery. They typically extend `prerna.engine.impl.AbstractDatabaseEngine` and implement `prerna.engine.api.IDatabaseEngine`.

### 1.1. Core Concepts for Database Engines
*   **`prerna.engine.api.IDatabaseEngine` Interface**:
    *   `execQuery(String query)`: Executes a native query (e.g., SQL, SPARQL) against the database and returns results, often wrapped in an `IRawSelectWrapper`.
    *   `insertData(String query)` / `removeData(String query)`: Executes data modification queries.
    *   `commit()`: Commits the current transaction if the database supports transactional operations.
    *   `getDatabaseType()`: Returns a `DATABASE_TYPE` enum (e.g., RDBMS, JENA, NEO4J) identifying the specific kind of database.
    *   `getOWLEngineFactory()`: Provides access to the `OWLEngineFactory` which manages the engine's metadata (OWL file). This is crucial for engines that are "explorable" and have a semantic layer.
    *   `query(SelectQueryStruct qs)` (from `IExplorable`): Executes a SEMOSS `SelectQueryStruct` against the database, requiring translation to the native query language.
*   **`prerna.engine.impl.AbstractDatabaseEngine` Class**:
    *   **OWL Metadata Management**: Handles loading and managing the associated OWL file (via `RDFFileSesameEngine` instance stored in `baseDataEngine` and managed by `owlEnginefactory`). This OWL file stores the semantic model (concepts, relationships, data types) of the database.
    *   **SMSS Configuration**: Loads database connection details and OWL file location from the `.smss` properties. Handles encryption/decryption of sensitive properties like passwords.
    *   **Connection Management**: Concrete implementations are responsible for managing actual database connections (e.g., JDBC connections).
    *   **Query Interpretation**: While `AbstractDatabaseEngine` itself doesn't execute `SelectQueryStruct`s directly, it provides the framework. Concrete engines (like `RDBMSNativeEngine`) will have their own query interpreters (e.g., SQL interpreters) to translate `SelectQueryStruct` into native queries.
*   **Extending for a New Database**:
    *   Implement `IDatabaseEngine`.
    *   Extend `AbstractDatabaseEngine` if an OWL-based semantic layer is required, or manage metadata differently if it's a "basic" engine.
    *   Implement methods for connecting to the database, executing queries (translating `SelectQueryStruct` if necessary), inserting/deleting data, and retrieving schema information.
    *   If it's an RDBMS, you might extend `RDBMSNativeEngine` and primarily provide a new `AbstractSqlQueryUtil` for dialect-specific SQL.
    *   Ensure proper handling of database transactions and connection pooling if applicable.
    *   Define necessary properties in the `.smss` file for connection and behavior.

### 1.2. Example: `prerna.engine.impl.rdbms.RDBMSNativeEngine`
*   **Purpose**: Provides connectivity to a wide range of relational databases via JDBC.
*   **Implementation Highlights**: Uses a specific `prerna.util.sql.AbstractSqlQueryUtil` implementation based on the database type (e.g., for H2, Oracle, PostgreSQL) to generate dialect-specific SQL from `SelectQueryStruct`s. Manages JDBC connections.
*   **SMSS Configuration**: JDBC URL, driver class, username, password, fetch size, database zone ID.

### 1.3. Example: `prerna.engine.impl.rdf.InMemoryJenaEngine` / `RDFFileJenaEngine`
*   **Purpose**: Manages RDF data using Apache Jena, in-memory or file-backed.
*   **Implementation Highlights**: Uses Jena API for SPARQL execution and graph manipulation. The `OWLFile` is the primary data store itself for these engines.
*   **SMSS Configuration**: File paths (for `RDFFileJenaEngine`), Jena-specific settings like graph name.

### 1.4. Example: `prerna.engine.impl.neo4j.Neo4jEngine`
*   **Purpose**: Connects to Neo4j graph databases.
*   **Implementation Highlights**: Uses the Neo4j Java driver to execute Cypher queries. Translates `SelectQueryStruct` (or parts of it) into Cypher.
*   **SMSS Configuration**: Neo4j Bolt URI, username, password.

### 1.5. Example: `prerna.engine.impl.tinker.JanusEngine`
*   **Purpose**: Interfaces with JanusGraph, a distributed graph database, typically using the TinkerPop Gremlin query language.
*   **Implementation Highlights**: Translates `SelectQueryStruct` to Gremlin queries for execution against JanusGraph via its TinkerPop interface.
*   **SMSS Configuration**: JanusGraph connection properties (e.g., storage backend configuration path, graph name).

## 2. `STORAGE` Engines

Storage engines provide access to file systems and object storage solutions, for reading and writing files and managing assets. They typically extend `prerna.engine.impl.storage.AbstractStorageEngine` and implement `prerna.engine.api.IStorageEngine`.

### 2.1. Core Concepts for Storage Engines
*   **`prerna.engine.api.IStorageEngine` Interface**:
    *   `list(String path)`: Lists assets (files/folders) at a given path.
    *   `listDetails(String path)`: Lists assets with more details (size, modified date).
    *   `syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)`: Syncs local files/folders to the storage.
    *   `syncStorageToLocal(String storagePath, String localPath)`: Syncs from storage to local.
    *   `copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)`: Copies a local file to storage.
    *   `copyToLocal(String storageFilePath, String localFolderPath)`: Copies a file from storage to local.
    *   `deleteFromStorage(String storagePath)` / `deleteFolderFromStorage(String storageFolderPath)`: Deletes assets from storage.
    *   `getStorageType()`: Returns a `StorageTypeEnum` (e.g., `LOCAL_FS`, `AWS_S3`).
*   **`prerna.engine.impl.storage.AbstractStorageEngine` Class**:
    *   Provides common initialization for engine ID, name, and SMSS properties, including integration with secret stores for credentials.
    *   Concrete implementations must handle the actual interaction with the specific storage system's API.
*   **Extending for a New Storage System**:
    *   Implement `IStorageEngine`.
    *   Extend `AbstractStorageEngine` for basic property setup.
    *   Implement all methods defined in `IStorageEngine` using the target storage system's SDK or API (e.g., AWS S3 SDK, Azure Blob Storage SDK, Google Cloud Storage SDK).
    *   Manage authentication and credentials securely, often by reading them from SMSS properties (which can be backed by a secret store).
    *   Handle path normalization and differences between local file system paths and object storage paths.

### 2.2. Example: `prerna.engine.impl.storage.LocalFileSystemStorageEngine`
*   **Purpose**: Interacts with the local file system of the server where SEMOSS is running.
*   **Implementation Highlights**: Uses standard Java `java.io.File` and `java.nio.file.Path` operations for file/directory listing, copying, and deletion.
*   **SMSS Configuration**: Might define a `ROOT_DIR` to restrict access to a specific base path on the server.

### 2.3. Example: `prerna.engine.impl.storage.S3StorageEngine`
*   **Purpose**: Connects to Amazon S3 (Simple Storage Service) for object storage.
*   **Implementation Highlights**: Uses the AWS SDK for Java to perform S3 operations (listing objects, uploading, downloading, deleting).
*   **SMSS Configuration**: AWS credentials (access key, secret key, session token if applicable), S3 bucket name, AWS region.

## 3. `MODEL` Engines

Model engines facilitate interaction with machine learning models for tasks like predictions, text generation, or embedding creation. They typically extend `prerna.engine.impl.model.AbstractModelEngine` and implement `prerna.engine.api.IModelEngine`.

### 3.1. Core Concepts for Model Engines
*   **`prerna.engine.api.IModelEngine` Interface**:
    *   `ask(String question, String context, Insight insight, Map<String, Object> parameters)`: For conversational AI or question-answering tasks.
    *   `instruct(String task, String context, List<Map<String, Object>> projectData, Insight insight, Map<String, Object> parameters)`: For models that follow instructions to generate structured output or perform tasks.
    *   `embeddings(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters)`: For generating numerical vector embeddings from text.
    *   `imageEmbeddings(List<String> imagesToEmbed, Insight insight, Map<String, Object> parameters)`: For generating embeddings from images.
    *   `getModelType()`: Returns a `ModelTypeEnum` identifying the kind of model (e.g., `OPEN_AI`, `BEDROCK`, `LOCAL_PYTHON`).
*   **`prerna.engine.impl.model.AbstractModelEngine` Class**:
    *   **Lifecycle and Configuration**: Handles opening SMSS properties and integrating with `ISecrets` for secure API key management.
    *   **Usage Tracking**: Manages flags like `keepConversationHistory` and `keepInputOutput`. Wraps the core model calls to log interactions via `ModelEngineInferenceLogsWorker` if `inferenceLogsEnbaled` is true.
    *   **User Restrictions**: Integrates with `ModelUsageRestrictionUtility` to check and update user-specific usage limits (e.g., token counts, time limits).
    *   **Abstract `*Call` Methods**: Defines protected abstract methods (e.g., `askCall`, `instructCall`, `embeddingsCall`) that concrete subclasses must implement to perform the actual interaction with the model provider's API or local model.
*   **Extending for a New Model Provider/Type**:
    *   Implement `IModelEngine`.
    *   Extend `AbstractModelEngine`.
    *   Implement the required abstract `*Call` methods (e.g., `askCall`, `embeddingsCall`). This involves:
        *   Setting up the API client for the specific model service.
        *   Formatting the input parameters (question, context, hyperparameters) into the request structure expected by the model's API.
        *   Making the API call.
        *   Parsing the API response and packaging it into the appropriate `ModelEngineResponse` object (e.g., `AskModelEngineResponse`, `EmbeddingsModelEngineResponse`).
    *   Define a new `ModelTypeEnum` value if necessary.
    *   Specify necessary SMSS properties for configuration (e.g., API endpoint, API key property name).

### 3.2. Example: `prerna.engine.impl.model.OpenAiEngine`
*   **Purpose**: Integrates with OpenAI's models (e.g., GPT series for text generation, Ada for embeddings).
*   **Implementation Highlights**: Implements `askCall`, `instructCall`, `embeddingsCall` by making HTTP requests to the OpenAI API endpoints using an HTTP client. It handles constructing the JSON request bodies and parsing the JSON responses.
*   **SMSS Configuration**: `API_KEY` (for OpenAI API key), `MODEL_NAME` (default model to use, e.g., "gpt-3.5-turbo"), `ORG_ID` (optional OpenAI organization ID).

### 3.3. Example: `prerna.engine.impl.model.BedrockEngine`
*   **Purpose**: Connects to AWS Bedrock, providing access to various foundation models from different providers (e.g., Anthropic Claude, AI21 Labs Jurassic).
*   **Implementation Highlights**: Uses the AWS SDK for Java to interact with the Bedrock runtime. It needs to handle different request/response formats depending on the specific foundation model being invoked via Bedrock.
*   **SMSS Configuration**: AWS credentials (typically managed via environment or instance profiles), `REGION`, `MODEL_ID` (the specific Bedrock model ARN), `CONTENT_TYPE`, `ACCEPT_TYPE`.

### 3.4. Example: `prerna.engine.impl.model.EmbeddedModelEngine`
*   **Purpose**: Designed to work with models that are hosted locally or managed directly by the SEMOSS instance. This often involves Python-based models.
*   **Implementation Highlights**: May use `prerna.ds.py.PyTranslator` to invoke a local Python script that loads and runs the model. The communication would go through the TCP server mechanism.
*   **SMSS Configuration**: Could include `PYTHON_SCRIPT_PATH`, `MODEL_FILE_PATH`, Python environment details, or specific parameters for the local model.

## 4. `VECTOR` Engines

Vector engines connect to vector databases or libraries for performing similarity searches, typically on text embeddings. They are crucial for Retrieval-Augmented Generation (RAG) and other embedding-based AI applications. They generally extend `prerna.engine.impl.vector.AbstractVectorDatabaseEngine` and implement `prerna.engine.api.IVectorDatabaseEngine`.

### 4.1. Core Concepts for Vector Engines
*   **`prerna.engine.api.IVectorDatabaseEngine` Interface**:
    *   `addDocument(List<String> filePaths, Map<String, Object> parameters)`: Ingests documents, which involves text extraction, chunking the text, generating embeddings for chunks (usually via an associated `IModelEngine`), and storing these embeddings along with metadata.
    *   `addEmbeddings(String vectorCsvFile, Insight insight, Map<String, Object> parameters)`: Allows adding pre-computed embeddings and associated text/metadata from a CSV file.
    *   `nearestNeighbor(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters)`: Takes a search query, generates its embedding, and finds the most similar items in the vector store.
    *   `removeDocument(List<String> filePaths, Map<String, Object> parameters)`: Deletes documents and their corresponding embeddings from the store.
    *   `listDocuments(Map<String, Object> parameters)`: Lists the source documents currently indexed.
    *   `getVectorDatabaseType()`: Returns a `VectorDatabaseTypeEnum`.
*   **`prerna.engine.impl.vector.AbstractVectorDatabaseEngine` Class**:
    *   **Embedder Integration**: Manages a connection to an `IModelEngine` (specified by `EMBEDDER_ENGINE_ID` in SMSS) used for generating text embeddings.
    *   **Text Processing**: Often relies on a Python backend (via `PyTranslator` and scripts like `vector_database.py`) for:
        *   Text extraction from various file types (PDF, DOCX, etc.).
        *   Text chunking strategies (e.g., recursive character splitting, token-based splitting).
    *   **Local File Management**: Manages local storage for original documents, extracted text, and chunked data, typically within the engine's specific folder (`/<BASE_ENGINE_FOLDER>/vector/<ENGINE_ID>/py/schema/<INDEX_CLASS>/`).
    *   **Configuration**: Handles SMSS properties for `CONTENT_LENGTH` (chunk size), `CONTENT_OVERLAP`, `DEFAULT_CHUNK_UNIT`, `EXTRACTION_METHOD`, `DISTANCE_METHOD`, and `DEFAULT_INDEX_CLASS` (collection/index name).
    *   **Python Server Management**: May start and manage its own Python TCP server process via `ClientProcessWrapper` for text processing tasks.
*   **Extending for a New Vector Database**:
    *   Implement `IVectorDatabaseEngine`.
    *   Extend `AbstractVectorDatabaseEngine`.
    *   Implement methods for connecting to the specific vector database.
    *   Implement how embeddings and metadata are inserted, updated, deleted, and queried (similarity search) using the database's client API.
    *   Define necessary SMSS properties for connection (host, port, API keys, index/collection names).
    *   If the database has specific Python dependencies for optimal interaction, you might need to adjust the Python server environment or scripts.

### 4.2. Example: `prerna.engine.impl.vector.FaissDatabaseEngine`
*   **Purpose**: Interfaces with FAISS (Facebook AI Similarity Search), a library for efficient local similarity search. Typically used for in-memory or file-backed indexes.
*   **Implementation Highlights**: Manages FAISS index files. Operations like adding embeddings and searching are often delegated to Python scripts that use the FAISS library.
*   **SMSS Configuration**: `INDEX_PATH` (path to the FAISS index file), `EMBEDDER_ENGINE_ID`.

### 4.3. Example: `prerna.engine.impl.vector.ChromaVectorDatabaseEngine`
*   **Purpose**: Connects to ChromaDB, an open-source embedding database.
*   **Implementation Highlights**: Uses the ChromaDB client API (often via Python) to create collections, add embeddings/documents, and perform similarity queries.
*   **SMSS Configuration**: ChromaDB host, port, collection name, `EMBEDDER_ENGINE_ID`.

## 5. `FUNCTION` Engines

Function engines allow SEMOSS to treat external services, custom scripts, or even other internal SEMOSS reactors as callable functions. This is particularly useful for integrating with LLM function-calling capabilities or for creating reusable, parameterized operations. They typically extend `prerna.engine.impl.function.AbstractFunctionEngine` and implement `prerna.engine.api.IFunctionEngine`.

### 5.1. Core Concepts for Function Engines
*   **`prerna.engine.api.IFunctionEngine` Interface**:
    *   `execute(Map<String, Object> parameterValues)`: The core method that invokes the function with a map of parameter names to their values.
    *   `getFunctionName()`, `getFunctionDescription()`: Return metadata about the function.
    *   `getParameters()`: Returns a list of `FunctionParameter` objects, describing the expected inputs.
    *   `getRequiredParameters()`: Lists which parameters are mandatory.
    *   `getFunctionDefintionJson()`: Generates a JSON schema representing the function's signature (name, description, parameters), often formatted to be compatible with OpenAI's function/tool specification.
    *   `buildOpenAIFunctionEngineToolMap()` / `buildBedrockToolSpec()`: Methods to generate tool specifications for specific LLM providers.
*   **`prerna.engine.impl.function.AbstractFunctionEngine` Class**:
    *   **Metadata Loading**: Handles loading the function's metadata (`functionName`, `functionDescription`, `parameters`, `requiredParameters`) from the engine's SMSS properties. These are usually defined using keys like `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`, `FUNCTION_PARAMETERS` (a JSON string defining the parameters), and `FUNCTION_REQUIRED_PARAMETERS`.
    *   **SMSS Configuration**: Expects function signature details to be defined in the `.smss` file.
*   **Extending for a New Function Source**:
    *   Implement `IFunctionEngine`.
    *   Extend `AbstractFunctionEngine` to leverage metadata loading.
    *   Implement the `execute(Map<String, Object> parameterValues)` method. This is where the core logic resides:
        *   Retrieve and validate parameters from the `parameterValues` map.
        *   Perform the function's action (e.g., call an external API, run a local script, invoke another Java method).
        *   Return the result of the function.
    *   Define the function's signature (name, description, parameters, required parameters) in the SMSS properties for the new engine.

### 5.2. Example: `prerna.engine.impl.function.LocalPythonFunctionEngine`
*   **Purpose**: Enables the execution of Python scripts or specific functions within those scripts, making them callable as SEMOSS functions.
*   **Implementation Highlights**: The `execute()` method likely uses `prerna.ds.py.PyTranslator` to send the script path/function name and parameters to the Python TCP server for execution.
*   **SMSS Configuration**: Would include `PYTHON_SCRIPT_PATH` (path to the .py file), `PYTHON_FUNCTION_NAME` (name of the function to call within the script), `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`, and `FUNCTION_PARAMETERS` (JSON defining the inputs the Python function expects).

### 5.3. Example: `prerna.engine.impl.function.RESTFunctionEngine`
*   **Purpose**: Allows SEMOSS to make calls to external REST APIs and treat them as callable functions.
*   **Implementation Highlights**: The `execute()` method would use an HTTP client library (like Apache HttpClient or OkHttp) to construct and send the HTTP request. It maps the input `parameterValues` to query parameters, path parameters, or the request body based on the SMSS configuration. It then parses the API's response.
*   **SMSS Configuration**: `API_ENDPOINT_URL`, `HTTP_METHOD` (GET, POST, etc.), authentication details (e.g., API key header name, token URL if OAuth2), `FUNCTION_NAME`, `FUNCTION_DESCRIPTION`, and `FUNCTION_PARAMETERS` (JSON defining how input parameters map to the API request).

## 6. `PROJECT` Engines

Project engines represent SEMOSS projects themselves, acting as containers or high-level orchestrators for other assets like engines, insights, and datasets.

### 6.1. Example: `prerna.engine.impl.app.AppEngine`
*   **Purpose**: Represents a SEMOSS "Project" (often referred to as an "App" in older contexts or internal code). It doesn't connect to an external data source in the traditional sense but rather manages the metadata and assets associated with a specific analytical workspace or application built within SEMOSS.
*   **Implementation Highlights**:
    *   Likely interacts heavily with `SecurityProjectUtils` and potentially `MasterDatabaseUtility` to retrieve project metadata, associated engine IDs, insight IDs, and user permissions for the project.
    *   May provide methods to list project contents or manage project-level settings.
    *   Its `open()` method would load project-specific metadata from its SMSS file.
*   **SMSS Configuration**: Typically contains `PROJECT_ID`, `PROJECT_NAME`, paths or references to associated engine configurations, version information, and user/group access control lists for the project itself.

```
