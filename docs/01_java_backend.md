# SEMOSS Java Backend Deep Dive

This document provides a more detailed look into the core Java components of the SEMOSS platform.

## 1. Pixel: The SEMOSS Query Language & Execution Engine (`src/prerna/sablecc2`)

"Pixel" is the custom query and scripting language of SEMOSS. It allows users to express complex data operations, analytical workflows, and interactions with various parts of the SEMOSS platform. The `src/prerna/sablecc2/` package and its sub-packages are responsible for parsing, translating, and executing these Pixel scripts.

The name `sablecc2` suggests that the SableCC parser generator tool was used to create the underlying parsing infrastructure. This is evident from the presence of files like `lexer.dat`, `parser.dat`, and the typical structure of sub-packages:
*   `lexer/`: Contains the lexical analyzer (`Lexer.java`) which breaks down the input Pixel string into a stream of tokens.
*   `parser/`: Contains the parser (`Parser.java`) which constructs an Abstract Syntax Tree (AST) from the token stream, based on the Pixel grammar.
*   `node/`: Defines the classes representing the various nodes in the AST (e.g., `AAssignRoutine`, `ASelectExpr`).
*   `analysis/`: Provides visitor pattern implementations (`Analysis.java`, `DepthFirstAdapter.java`) for traversing and manipulating the AST.

**Execution Flow:**

1.  **Preprocessing:** The input Pixel string is first pre-processed by `PixelPreProcessor.java`. This step might involve tasks like encoding/decoding parts of the script.
2.  **Parsing:**
    *   The `PixelRunner.java` class is a key orchestrator for executing Pixel scripts.
    *   Inside its `runPixel()` method, it instantiates a `Lexer` and `Parser` with the input Pixel string.
    *   The `parser.parse()` method is called, which generates an AST (rooted by a `Start` node).
3.  **Translation and Execution:**
    *   A `GreedyTranslation` object (from `GreedyTranslation.java`) is created and applied to the AST (`tree.apply(translation)`).
    *   The `GreedyTranslation` class appears to be the primary interpreter that walks the AST and executes the operations defined by the Pixel script. It likely uses a `PixelPlanner` internally to manage the execution steps and intermediate data structures.
    *   The `src/prerna/sablecc2/om/` package (Object Model) contains important classes like `NounStore.java` and `VarStore.java`, which are likely used by the `GreedyTranslation` to manage variables, data, and state during execution.
    *   `PixelPlanner.java` (used by `GreedyTranslation`) plays a crucial role in planning the execution of the operations, managing resources, and possibly optimizing the Pixel query.
4.  **Result Handling:**
    *   As Pixel commands are executed, results are captured as `NounMetadata` objects.
    *   `PixelRunner.addResult()` is called to store these results. It associates the output with a `Pixel` object, which is then added to the current `Insight` (`insight.getPixelList().addPixel()`). An `Insight` object represents a specific analysis session or workspace for the user.

**Key Classes:**

*   `PixelRunner.java`: Orchestrates the parsing and execution of Pixel scripts.
*   `PixelPreProcessor.java`: Handles initial processing of the Pixel string.
*   `Lexer.java`: Generated lexical analyzer.
*   `Parser.java`: Generated parser.
*   `GreedyTranslation.java`: Primary class responsible for interpreting the AST and executing Pixel logic. It interacts with `PixelPlanner`.
*   `PixelPlanner.java`: (Located in `prerna.reactor` package) Assists `GreedyTranslation` in planning and executing the operations.
*   `NounStore.java` / `VarStore.java`: Manage data and variables during Pixel execution.
*   `Insight.java`: (Located in `prerna.om` package) Represents the context (e.g., session, workspace) in which Pixel scripts are run, holding the list of executed Pixels and their results.

This architecture allows SEMOSS to have a flexible and powerful way to define and execute a wide range of operations, from simple data queries to complex multi-step analytical workflows.

## 2. Reactor Framework (`src/prerna/reactor`)

The Reactor framework is a cornerstone of SEMOSS's Java backend, providing a flexible and extensible way to define and execute specific operations or commands, often as part of a Pixel script. Each "reactor" is a Java class that encapsulates a particular piece of logic.

### 2.1. Core Reactor Interfaces/Classes

*   **`IReactor.java`**: This is the primary interface that all reactors must implement (typically by extending `AbstractReactor`). It defines the contract for reactor behavior, including methods for:
    *   Execution: `NounMetadata execute()` is the main method where the reactor performs its logic.
    *   Input/Output Definition: `getInputs()` and `getOutputs()` (though often managed by `AbstractReactor` conventions).
    *   Lifecycle and Chaining: `setParentReactor()`, `getChildReactors()`, `mergeUp()` for integrating into a larger execution flow.
    *   Interaction with Pixel Execution: `setPixelPlanner()`, `setNounStore()`.
    *   Metadata: `getName()`, `getSignature()`, `getHelp()`.

*   **`AbstractReactor.java`**: This abstract class provides a robust base implementation of `IReactor`. Most concrete reactors in SEMOSS extend `AbstractReactor`. Key functionalities it provides include:
    *   **Noun Management**:
        *   `NounStore store`: Each reactor instance has a `NounStore` to hold its input parameters (nouns).
        *   `keysToGet`: Concrete reactors define an array of strings (`keysToGet`) specifying the names of the input nouns they expect (e.g., `"value"`, `"column"`, `"expression"`). These often correspond to keys from `ReactorKeysEnum.java`.
        *   `organizeKeys()`: A crucial method called typically at the start of `execute()`. It populates the `store` (and a convenience map `keyValue`) from the actual inputs provided in the Pixel script, matching them against `keysToGet`.
        *   `curRow`: A `GenRowStruct` representing the current data row or context, especially when reactors are chained or process input streams.
    *   **Planner and Insight**: Access to the `PixelPlanner` and the current `Insight` object.
    *   **Signature and Naming**: Storing the reactor's operation name and Pixel signature.
    *   **Error Handling and Logging**: Utility methods for standardized error reporting and logging.
    *   **Input Retrieval**: Helper methods like `getNounAsStringList(String key)` to easily access input values from the `NounStore`.

### 2.2. Key Reactor Examples and Roles

Reactors are used for a vast array of tasks in SEMOSS. The behavior and integration of a reactor can vary:

*   **General Purpose Reactors**: These perform a distinct operation and their results are typically used by subsequent reactors or returned to the user.
    *   **Example: `EchoReactor.java`**
        *   **Purpose**: A simple reactor that returns its primary input.
        *   **Inputs**: Expects a single noun, typically specified by the key `ReactorKeysEnum.VALUE.getKey()` (e.g., `Pixel: Echo(value=["Hello World"]);`).
        *   **Execution**: Calls `organizeKeys()` to load its inputs. Retrieves the specified noun from its `NounStore` and returns it as a `NounMetadata` object.
        *   **Role**: Useful for debugging, simple assignments, or as a basic building block.

*   **Data Structure Providers / Configuration Reactors**: Some reactors don't perform a final action themselves but rather construct an object or configure a state that is then used by a parent or subsequent reactor in the execution chain.
    *   **Example: `FilterReactor.java`**
        *   **Purpose**: To define a filter condition based on a left operand (column), a comparator, and a right operand (value or another column).
        *   **Inputs**: Expects nouns named "LCOL", "COMPARATOR", and "RCOL".
        *   **Execution**: Its `execute()` method creates a `prerna.sablecc2.om.Filter` object using these inputs.
        *   **Integration**:
            *   It overrides `mergeUp()` to add the created `Filter` object (as `NounMetadata`) directly into its parent reactor's current processing row (`parentReactor.getCurRow().add(filterNoun)`).
            *   Its `getInputs()` method returns `null`, indicating it's not treated as a standalone step in the `PixelPlanner` but rather contributes to a consuming reactor (e.g., a data query reactor like `SelectReactor` would consume this `Filter` object).
        *   **Role**: Defines a filter that will be applied by another reactor that actually performs data retrieval or manipulation.

*   **Control Flow Reactors**:
    *   **Example: `IfReactor.java`**
        *   **Purpose**: Implements conditional logic (if-then-else).
        *   **Inputs**: Typically takes a condition, a then-expression (reactor or value), and an optional else-expression.
        *   **Execution**: Evaluates the condition. Based on the result, it then executes either the "then" part or the "else" part.
        *   **Role**: Allows for branching logic within Pixel scripts.

*   **Data Operation Reactors (e.g., in `src/prerna/reactor/frame/` or `src/prerna/reactor/qs/`)**:
    *   Many reactors are dedicated to specific data operations like selecting columns, joining data, grouping, pivoting, etc. These often interact heavily with the `ITableDataFrame` (via `PixelPlanner` or directly) or build up components of a query structure (`QueryStruct`). For example, a hypothetical `SelectReactor` would take column names as input and modify the current frame or query to only include those columns.

The Reactor pattern, combined with the Pixel language, gives SEMOSS a highly modular and powerful way to define and execute a wide variety of operations. Developers can add new functionality by creating new reactor classes.

## 3. Engine Abstraction (`src/prerna/engine`)

## 3. Engine Abstraction (`src/prerna/engine`)

The "Engine" is a fundamental concept in SEMOSS, representing a connection to an external data source, a service, or a computational environment. The `src/prerna/engine/api/IEngine.java` interface is the root of the engine abstraction.

### 3.1. `IEngine.java`: The Core Engine Interface

`IEngine.java` defines the basic contract for all engine types within SEMOSS. It focuses on the lifecycle, configuration, and identification of an engine.

**Key Responsibilities and Methods:**

*   **Identity:**
    *   `getEngineId()` / `setEngineId(String engineId)`: Manages a unique identifier for the engine instance.
    *   `getEngineName()` / `setEngineName(String engineName)`: Manages a user-friendly name for the engine, which might be the same as its ID or a more descriptive alias.

*   **Lifecycle Management:**
    *   `open(String smssFilePath)` / `open(Properties smssProp)`: This is the core initialization method. Engines are configured via `.smss` files (which are Java Properties files). These files contain key-value pairs defining connection strings, credentials, paths, and other parameters specific to the engine type. The `open()` method reads this configuration and establishes the connection or prepares the engine for use.
    *   `close()`: Inherited from `java.io.Closeable`, this method is responsible for releasing any resources held by the engine (e.g., database connections, file handles).
    *   `delete()`: Allows for the removal of an engine, including its configuration files and any associated resources.

*   **Configuration Access:**
    *   `getSmssFilePath()` / `setSmssFilePath(String smssFilePath)`: Provides access to the path of the `.smss` configuration file.
    *   `getSmssProp()` / `setSmssProp(Properties smssProp)`: Allows direct access to the loaded `Properties` object containing the engine's configuration.
    *   `getOrigSmssProp()`: Returns the original properties from the SMSS file, before any potential runtime modifications.

*   **Engine Typing:**
    *   `getCatalogType()`: Returns an `IEngine.CATALOG_TYPE` enum value. This is a crucial piece of metadata that categorizes the engine's primary function. The defined types include:
        *   `DATABASE`: For connections to relational, graph, or other types of databases.
        *   `STORAGE`: For connections to file systems or object storage (e.g., S3, local disk).
        *   `MODEL`: For managing and interacting with machine learning models.
        *   `VECTOR`: For vector databases used in similarity search and AI.
        *   `FUNCTION`: For engines that provide access to custom functions or APIs.
        *   `GUARDRAIL`: For engines that enforce policies or rules.
        *   `PROJECT`: A special type representing a SEMOSS project itself.
        *   `VENV`: (Likely related to Python virtual environments, though noted as not heavily used anymore).
    *   `getCatalogSubType(Properties smssProp)`: Allows for a more granular classification of the engine within its main `CATALOG_TYPE`.

*   **Other Utilities:**
    *   `holdsFileLocks()`: Indicates if the engine's operation might result in file locks, which is important for coordinating operations like exports or backups.
    *   `buildOpenAIFunctionEngineToolMap()`: Suggests a mechanism for integrating engines with AI function-calling capabilities, particularly with OpenAI models, allowing AI agents to potentially interact with or query these engines.

The `IEngine` interface itself is quite generic. Specific data processing capabilities (like executing queries for a `DATABASE` engine, or reading/writing files for a `STORAGE` engine) are typically defined by more specialized interfaces (e.g., `IDatabaseEngine`, `IStorageEngine`) which concrete engine implementations will also implement in addition to `IEngine`.

### 3.2. Engine Implementations (`src/prerna/engine/impl/`)

SEMOSS provides a wide array of concrete engine implementations, catering to different data sources, services, and functionalities. These classes extend base abstract classes like `prerna.engine.impl.AbstractEngine` or more specialized ones like `prerna.engine.impl.AbstractDatabaseEngine`, `prerna.engine.impl.function.AbstractFunctionEngine`, etc., and implement `IEngine` along with other necessary interfaces (e.g., `IDatabaseEngine`, `IStorageEngine`).

Here are some notable examples categorized by their `CATALOG_TYPE`:

**`DATABASE` Engines:**
These engines connect to various database systems, enabling data querying and manipulation. They typically implement `prerna.engine.api.IDatabaseEngine`.
*   **`prerna.engine.impl.rdbms.RDBMSNativeEngine`**:
    *   **Purpose**: Provides connectivity to a wide range of relational databases via JDBC (Java Database Connectivity).
    *   **SMSS Configuration**: Would include JDBC URL, driver class name, username, password.
*   **`prerna.engine.impl.rdf.InMemoryJenaEngine` / `prerna.engine.impl.rdf.RDFFileJenaEngine`**:
    *   **Purpose**: Manages RDF data using the Apache Jena framework, either in-memory or backed by files.
    *   **SMSS Configuration**: Might include file paths for `RDFFileJenaEngine`, or specific Jena settings.
*   **`prerna.engine.impl.neo4j.Neo4jEngine`**:
    *   **Purpose**: Connects to Neo4j graph databases.
    *   **SMSS Configuration**: URI for the Neo4j instance, authentication details.
*   **`prerna.engine/impl/tinker/JanusEngine.java`**:
    *   **Purpose**: Interface for JanusGraph, a distributed graph database, likely using the TinkerPop framework.
    *   **SMSS Configuration**: JanusGraph connection properties (e.g., storage backend, indexing).

**`STORAGE` Engines:**
These engines provide access to file systems and object storage solutions. They typically implement `prerna.engine.api.IStorageEngine`.
*   **`prerna.engine.impl.storage.LocalFileSystemStorageEngine`**:
    *   **Purpose**: Allows SEMOSS to interact with the local file system of the server it's running on.
    *   **SMSS Configuration**: Might define a base path or allowed directories.
*   **`prerna.engine.impl.storage.S3StorageEngine`**:
    *   **Purpose**: Connects to Amazon S3 (Simple Storage Service) for object storage.
    *   **SMSS Configuration**: AWS credentials, bucket name, region.

**`MODEL` Engines:**
These engines facilitate interaction with machine learning models, often for predictions or other AI tasks. They might implement `prerna.engine.api.IModelEngine`.
*   **`prerna.engine.impl.model.OpenAiEngine`**:
    *   **Purpose**: Integrates with OpenAI's models (e.g., GPT series for text generation, embeddings).
    *   **SMSS Configuration**: OpenAI API key, possibly default model names.
*   **`prerna.engine.impl.model.BedrockEngine`**:
    *   **Purpose**: Connects to AWS Bedrock, providing access to various foundation models.
    *   **SMSS Configuration**: AWS credentials, region, specific Bedrock model IDs.
*   **`prerna.engine.impl.model.EmbeddedModelEngine`**:
    *   **Purpose**: Likely designed to work with models that are hosted locally or managed directly by the SEMOSS instance (e.g., Python models running in the local environment).
    *   **SMSS Configuration**: Could include paths to model files, Python environment details.

**`VECTOR` Engines:**
These engines connect to vector databases, crucial for similarity searches, retrieval-augmented generation (RAG), and other embedding-based AI applications. They might implement `prerna.engine.api.IVectorEngine`.
*   **`prerna.engine.impl.vector.FaissDatabaseEngine`**:
    *   **Purpose**: Interfaces with FAISS (Facebook AI Similarity Search), a library for efficient similarity search.
    *   **SMSS Configuration**: Path to FAISS index files, or parameters for an in-memory FAISS index.
*   **`prerna.engine.impl.vector.ChromaVectorDatabaseEngine`**:
    *   **Purpose**: Connects to ChromaDB, an open-source embedding database.
    *   **SMSS Configuration**: ChromaDB connection details (e.g., host, port, collection name).

**`FUNCTION` Engines:**
These engines allow SEMOSS to execute custom code or call external APIs as functions.
*   **`prerna.engine.impl.function.LocalPythonFunctionEngine`**:
    *   **Purpose**: Enables the execution of Python scripts or functions stored locally.
    *   **SMSS Configuration**: Might specify Python interpreter paths, script directories, or environment variables.
*   **`prerna.engine.impl.function.RESTFunctionEngine`**:
    *   **Purpose**: Allows SEMOSS to make calls to external REST APIs and use their responses.
    *   **SMSS Configuration**: Base URL of the API, authentication methods (e.g., API keys), endpoint definitions.

**`PROJECT` Engines:**
*   **`prerna.engine.impl.app.AppEngine`**:
    *   **Purpose**: Represents a SEMOSS "Project" (often referred to as an "App"). It acts as a container or a high-level orchestrator for other engines, insights, and assets related to a specific analytical endeavor.
    *   **SMSS Configuration**: Contains metadata about the project, its constituent engines, and user access.

Each implementation translates the generic `IEngine` contract (like `open()`, `close()`, `getEngineType()`) into actions specific to its underlying technology. The `.smss` property files play a vital role in providing the necessary configuration details for each engine instance.

### 3.3. Engine Management

*(Detailed explanation to be added: Configuration, instantiation, selection/routing)*

## 4. Data Source Layer (`src/prerna/ds`)

*(Detailed explanation to be added: Role of this package, `TinkerFrame.java`, interaction with IEngine)*

## 5. Authentication and Authorization (`src/prerna/auth`)

*(Detailed explanation to be added)*

## 6. High-Level API Request Flow

*(Detailed explanation to be added: From web request to Pixel/command, reactor processing, and engine interaction)*

```
