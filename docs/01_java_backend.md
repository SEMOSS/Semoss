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

Managing the lifecycle, configuration, and accessibility of engines is a critical aspect of SEMOSS. This involves several components working together:

**1. Configuration (`.smss` files):**
*   Each engine instance is defined by a `.smss` file. This is a Java Properties file (`.properties` format) that contains all the necessary configuration for that specific engine.
*   Key properties typically include:
    *   `ENGINE_CLASS` (or `Constants.ENGINE_TYPE` in code): The fully qualified Java class name of the `IEngine` implementation (e.g., `prerna.engine.impl.rdbms.RDBMSNativeEngine`).
    *   `ENGINE_ALIAS` (or `Constants.ENGINE_ALIAS`): The user-friendly name of the engine.
    *   `ENGINE_ID` (or `Constants.ENGINE`): The unique ID for the engine.
    *   ...and other properties specific to the engine type (JDBC URLs, API keys, file paths, etc.).
*   These `.smss` files are typically stored in the `db/` directory or a configured asset location.

**2. Registration and Metadata Storage (`SecurityEngineUtils` and `securityDb`):**
*   The `prerna.auth.utils.SecurityEngineUtils.java` class plays a central role in managing engine metadata and permissions. It interacts with a dedicated security database (often an H2 database referred to as `securityDb`).
*   **Engine Creation/Registration**: When a new engine is added to SEMOSS (e.g., through the UI or an API call):
    *   Its `.smss` file is created/provided.
    *   `SecurityEngineUtils.addEngine(String engineId, boolean global, User user)` is called.
    *   This method reads the `.smss` file, determines the engine's catalog type and subtype (by temporarily instantiating the engine class to call its `getCatalogType()` methods).
    *   It then records the engine's ID, name, type, subtype, and other metadata into an `ENGINE` table within the `securityDb`.
    *   User permissions for this new engine (e.g., ownership for the creator) are also stored in tables like `ENGINEPERMISSION`.
*   **Metadata Access**: `SecurityEngineUtils` provides methods to retrieve engine details, list available engines for a user (respecting permissions), and get/set additional metadata (stored in `ENGINEMETA`).

**3. Instantiation and Runtime Access:**
*   **`DIHelper.java` (`prerna.util.DIHelper`)**: This singleton utility class acts as a runtime cache and provider for various global properties and object instances.
    *   When an engine is loaded and its `.smss` file path is known, this path can be registered with `DIHelper` (e.g., `DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFilePath)`).
    *   Other parts of the system can then retrieve this path or other engine-related properties from `DIHelper`.
    *   `DIHelper` can also store actual instantiated engine objects via `setLocalProperty(String key, Object value)` if they are meant to be singletons or cached.
*   **Engine Instantiation**:
    *   When a Pixel script or another part of SEMOSS needs to use a specific engine, it typically starts with the engine's ID.
    *   The system retrieves the engine's class name and SMSS properties (either directly from the file system using the path from `DIHelper` or from a cache).
    *   The engine class is instantiated using `Class.forName(engineClassName).newInstance()`.
    *   The `engine.open(smssProperties)` method is called on the new instance to initialize it.
*   **Caching/Pooling**: For performance, frequently used engines, especially database engines, might be cached or managed in connection pools. The specifics would depend on the engine type.

**4. Engine Selection and Usage in Pixel:**
*   Pixel scripts typically refer to engines by their ID or alias (e.g., `Database("myRdbmsEngine") | Select(...)`).
*   The Pixel execution engine (`PixelRunner` and `PixelPlanner`):
    1.  Parses the Pixel script and identifies the target engine ID.
    2.  Likely interacts with `SecurityEngineUtils` to verify the current user's permission to access that engine.
    3.  Retrieves the engine's configuration (SMSS properties).
    4.  Ensures the engine is instantiated and opened (as described above).
    5.  Passes the subsequent operations (like `Select`, `Filter`) to the appropriate methods of the instantiated `IEngine` object (or a more specific interface it implements, like `IDatabaseEngine`).

**In summary:**
*   `.smss` files define individual engine configurations.
*   `SecurityEngineUtils` and its associated database track engine metadata and user permissions.
*   `DIHelper` helps manage runtime access to engine configurations and instances.
*   The Pixel execution framework uses this infrastructure to instantiate and interact with the correct engine based on user commands and permissions.

## 4. Data Source Layer (`src/prerna/ds`)

The `src/prerna/ds` package provides abstractions and concrete implementations for representing and manipulating datasets within SEMOSS's Java memory, once data has been fetched from an external `IEngine` or created through transformations. This layer is crucial for in-memory analytics, data manipulation, and serving data to visualization components.

**Core Concepts:**

*   **`ITableDataFrame` Interface (`prerna.algorithm.api.ITableDataFrame`)**:
    *   This is the primary interface for all in-memory frame-like data structures in SEMOSS.
    *   It defines common operations expected from a data frame, such as accessing metadata, iterating data, applying filters, and saving/loading.
*   **`AbstractTableDataFrame.java` (`prerna.ds.shared.AbstractTableDataFrame`)**:
    *   A key abstract class that provides a base implementation for most `ITableDataFrame`s.
    *   **Metadata (`OwlTemporalEngineMeta metaData`)**: Each frame instance holds an `OwlTemporalEngineMeta` object. This metadata object describes the frame's structure: column names (headers), data types, relationships between columns (if applicable, especially for graph frames), and potentially other semantic information.
    *   **Filtering (`GenRowFilters grf`)**: `AbstractTableDataFrame` includes a `GenRowFilters` object. This allows filters to be applied directly to the in-memory frame. Operations on the frame (like iteration or querying) will respect these filters.
    *   **Querying**: It defines a `query(SelectQueryStruct qs)` method (often abstract or overridden by concrete classes) that allows the frame to be queried using SEMOSS's internal `SelectQueryStruct` representation. This enables a consistent way to query data regardless of whether it's in an external engine or an in-memory frame.
    *   **Caching**: Implements mechanisms for caching metrics about the data (e.g., column uniqueness, min/max values) to improve performance.
    *   **Persistence**: Provides methods (`saveMeta`, `openCacheMeta`) for saving and loading the frame's metadata and filter state, often used in conjunction with caching the frame's actual data.

**Key Frame Implementations:**

The `src/prerna/ds/` package and its sub-packages contain various concrete implementations of `ITableDataFrame`:

*   **`TinkerFrame.java`**:
    *   **Purpose**: Represents graph data within SEMOSS, built upon Apache TinkerPop's `TinkerGraph` (an in-memory graph database).
    *   **Functionality**: Stores nodes and edges, allows graph traversals and queries using Gremlin (via `GremlinInterpreter` which translates `SelectQueryStruct` to Gremlin). It's used for graph-based analytics and visualizations.
    *   **DataFrameType**: `GRAPH`.

*   **`H2Frame.java` (`prerna.ds.rdbms.h2.H2Frame` or `prerna.ds.h2.H2Frame`)**:
    *   **Purpose**: Represents tabular data backed by an in-memory H2 database instance.
    *   **Functionality**: Allows SQL querying of the in-memory data. This is often used as a high-performance backend for tabular data that has been imported or transformed within SEMOSS.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`NativeFrame.java` (`prerna.ds.nativeframe.NativeFrame`)**:
    *   **Purpose**: A purely Java-based in-memory representation of tabular data (e.g., using Lists of Lists or similar structures).
    *   **Functionality**: Suitable for smaller datasets or when direct Java manipulation is needed without the overhead of a database like H2.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`PandasFrame.java` (`prerna.ds.py.PandasFrame`)**:
    *   **Purpose**: Acts as a wrapper or interface to a Pandas DataFrame in a Python environment managed by SEMOSS.
    *   **Functionality**: Allows SEMOSS to leverage Pandas' powerful data manipulation capabilities. Operations might be delegated to the Python environment.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`RDataTable.java` (`prerna.ds.r.RDataTable`)**:
    *   **Purpose**: Similar to `PandasFrame`, but for R, likely wrapping R's `data.table` or `data.frame`.
    *   **Functionality**: Enables SEMOSS to utilize R for statistical analysis and data manipulation on data held in R.
    *   **DataFrameType**: `GRID` or `TABLE`.

*   **`SparkDataFrame.java` (`prerna.ds.spark.SparkDataFrame`)**:
    *   **Purpose**: Represents a Spark DataFrame, allowing SEMOSS to work with distributed datasets managed by Apache Spark.
    *   **Functionality**: Operations are typically translated into Spark actions and transformations.
    *   **DataFrameType**: `GRID` or `TABLE`.

**Interaction with `IEngine` and `QueryStruct`:**

*   **Loading Data into Frames**: Data is typically loaded into these `ITableDataFrame` implementations from an external `IEngine`.
    *   A query (often a `SelectQueryStruct`) is executed by an `IEngine` (e.g., `RDBMSNativeEngine` executing SQL).
    *   The results from the `IEngine` are then streamed or bulk-loaded into an appropriate frame type (e.g., data from an RDBMS might be loaded into an `H2Frame` for further in-memory work).
*   **Frames as Queryable Sources**: Once data is in an `ITableDataFrame`, the frame itself can often be queried using the same `SelectQueryStruct` mechanism that is used for external engines. This is achieved by each frame type providing its own implementation for `query(SelectQueryStruct qs)`, which translates the QS into operations on its specific backend (e.g., Gremlin for `TinkerFrame`, SQL for `H2Frame`, direct Java operations for `NativeFrame`).
*   **`QueryStruct.java` (`prerna.ds.QueryStruct`)**: This class is a pivotal data structure that represents a query in an abstract, engine-agnostic way. It includes selectors, filters, joins, group by clauses, order by clauses, etc.
    *   Reactors and other parts of SEMOSS often build or modify `QueryStruct` objects.
    *   These `QueryStruct`s are then passed to either an `IEngine` (to query external data) or an `ITableDataFrame` (to query in-memory data).
    *   Interpreters (like `GremlinInterpreter` for `TinkerFrame`, or SQL generators for RDBMS engines/frames) are responsible for translating the `QueryStruct` into the native query language of the target data store.

The data source layer (`src/prerna/ds`) thus provides a flexible way for SEMOSS to handle data both from external sources and within its own memory, using a common query abstraction (`QueryStruct`) and a set of versatile in-memory data frame representations.

## 5. Authentication and Authorization (`src/prerna/auth`)

The `src/prerna/auth/` package and its sub-packages are responsible for managing user identity, authentication, and access control throughout the SEMOSS platform.

**Core Security Objects:**

*   **`User.java`**: Represents an authenticated user within SEMOSS. It typically stores user identifiers (from various authentication providers), profile information, and potentially a collection of their permissions or roles.
*   **`AccessToken.java`**: Represents a security token (e.g., a JWT or an opaque token) issued to a user upon successful authentication. This token is then used to authenticate subsequent API requests and manage user sessions. `ReadOnlyAccessToken` might be a specialized version for read-only operations.
*   **`AuthProvider.java`**: An enumeration that defines the different methods by which a user can be authenticated (e.g., `NATIVE` for users stored in SEMOSS's own database, `LDAP`, `SAML`, `OIDC` for integration with external identity providers).
*   **`AccessPermissionEnum.java`**: Defines the various levels of access rights a user can have on a resource (e.g., `READ_ONLY`, `EDIT`, `OWNER`).

**Authentication Process (High-Level):**

1.  **Login Request**: A user initiates a login request, typically providing credentials or being redirected from an external Identity Provider (IdP).
2.  **Provider Determination**: SEMOSS identifies the `AuthProvider` being used for the login attempt.
3.  **Credential Validation**:
    *   For `NATIVE` users, `prerna.auth.utils.SecurityNativeUserUtils` likely handles the validation of credentials against a user store within SEMOSS's security database.
    *   For external providers (LDAP, SAML, OIDC), SEMOSS would interact with the respective IdP according to the protocol's specifications. This might involve validating assertions or tokens provided by the IdP.
4.  **User Object Creation**: Upon successful authentication, a `User` object is created or retrieved, populating it with identity information.
5.  **Token Issuance**: `prerna.auth.utils.SecurityTokenUtils` is responsible for generating an `AccessToken` for the authenticated `User`. This token encapsulates the user's authenticated state.
6.  **Session Management**: The `AccessToken` is used to manage the user's session, typically sent with each subsequent request to the backend.

**Authorization Process (High-Level):**

SEMOSS employs a role-based or permission-based access control model, primarily managed through its security database.

1.  **Resource Access Request**: A user, identified by their `AccessToken` and associated `User` object, attempts to access or modify a resource (e.g., an Engine, Project, Insight, or perform a specific action).
2.  **Permission Check**:
    *   Utility classes within `prerna.auth.utils/` are invoked to check permissions. For example:
        *   `SecurityEngineUtils.userCanViewEngine(User user, String engineId)`
        *   `SecurityProjectUtils.userCanEditProject(User user, String projectId)`
    *   These methods query the security database, which stores relationships between users (or groups they belong to) and resources, along with the `AccessPermissionEnum` level granted.
3.  **Decision**: Based on the permissions found in the security database, the system either grants or denies access to the resource or action. Both direct user permissions and permissions derived from group memberships are typically considered.
4.  **External Authorization**: The `prerna.auth.external.ExternalAuthorizationHelper` class suggests that SEMOSS can also integrate with external systems for making authorization decisions, potentially augmenting or overriding its internal permission model.

**Key Utility Packages/Classes:**

*   **`prerna.auth.utils`**: This package is central to security operations.
    *   `AbstractSecurityUtils`: Provides base functionality and access to the security database (often an H2 database instance).
    *   `SecurityEngineUtils`, `SecurityProjectUtils`, `SecurityInsightUtils`: Manage permissions and metadata for Engines, Projects, and Insights, respectively. They handle storing and retrieving these entities along with their associated user/group permissions from the security database.
    *   `SecurityAdminUtils`: Provides functions for administrative security tasks.
    *   `SecurityGroup*Utils`: A set of classes for managing security groups and their permissions on various resources.

The authentication and authorization mechanisms are designed to be comprehensive, supporting both native user management and integration with enterprise identity systems, while providing granular control over access to SEMOSS resources.

## 6. High-Level API Request Flow

Understanding how an API request flows through the SEMOSS Java backend helps tie together the various components discussed. While specifics can vary, a general request lifecycle often looks like this:

1.  **HTTP Request Reception:**
    *   An HTTP request (e.g., from the SEMOSS frontend UI, a custom application, or an API tool) arrives at the Java web server (typically Apache Tomcat) hosting the SEMOSS application.
    *   Standard Java EE components like Servlets (e.g., `prerna.servlets.ApiServlet`, `prerna.servlets.PixelServlet`) or Filters (e.g., `prerna.security.HttpFilter`) are the initial entry points.

2.  **Authentication and Authorization:**
    *   Security filters intercept the request.
    *   The user's `AccessToken` (usually in an HTTP header or cookie) is validated using `SecurityTokenUtils`.
    *   A `User` object is reconstituted.
    *   For protected resources, an authorization check is performed using utility classes like `SecurityEngineUtils`, `SecurityProjectUtils`, etc., to ensure the user has the necessary permissions for the requested resource or action. If authorization fails, an appropriate HTTP error (e.g., 401 Unauthorized, 403 Forbidden) is returned.

3.  **Request Parsing and Dispatching:**
    *   The servlet or controller extracts parameters from the HTTP request. This might include:
        *   A Pixel script to be executed.
        *   The name of a specific Reactor to invoke and its input parameters.
        *   Identifiers for resources like engine IDs, project IDs, or insight IDs.
    *   Based on the endpoint or request parameters, the request is dispatched to the appropriate handler.

4.  **Pixel Execution / Reactor Invocation:**
    *   **If the request contains a Pixel script (e.g., to `PixelServlet`):**
        *   An `Insight` object (representing the user's session or workspace) is retrieved or created.
        *   The `PixelRunner.runPixel(String expression, Insight insight)` method is called.
        *   The `PixelRunner` parses the script (using `sablecc2` components) and uses `GreedyTranslation` (which in turn uses `PixelPlanner`) to execute the script.
        *   This execution may involve a chain of Reactors, interactions with `ITableDataFrame`s (in-memory data), or queries to external `IEngine`s.
    *   **If the request is for a specific Reactor:**
        *   The `ReactorFactory` might be used to get an instance of the target `IReactor`.
        *   Input parameters (nouns) are loaded into the reactor's `NounStore`.
        *   The reactor's `execute()` method is called.
        *   The reactor performs its specific logic, which could involve calculations, data transformations, or calls to `IEngine`s or `ITableDataFrame`s.

5.  **Data Retrieval and Processing (Interaction with Engines/Frames):**
    *   If the operation requires data from an external source, the `PixelPlanner` or the invoked `IReactor` will:
        *   Identify the target `IEngine` ID.
        *   Ensure the user has permissions for this engine.
        *   Obtain an instance of the `IEngine` (potentially from a cache or by instantiating it using its SMSS configuration, often facilitated by `DIHelper` for configuration paths and `SecurityEngineUtils` for metadata).
        *   Formulate a `QueryStruct` representing the query.
        *   The `IEngine` executes the query (translating the `QueryStruct` to its native language like SQL, SPARQL, etc.) and returns the data.
        *   This data might then be loaded into an `ITableDataFrame` (e.g., `H2Frame`, `NativeFrame`) for further in-memory processing or transformation.
    *   If the operation acts on data already in an in-memory `ITableDataFrame` (like `TinkerFrame`, `H2Frame`), the frame's own `query(SelectQueryStruct qs)` method or other manipulation methods are called.

6.  **Result Aggregation and Formatting:**
    *   The `PixelRunner` (for Pixel scripts) or the top-level `IReactor` aggregates the results. Results are typically packaged as `NounMetadata` objects.
    *   For API responses, these results are often converted into a JSON format suitable for consumption by the client. Utility classes (e.g., within `prerna.util.gson`) might be used for this serialization.

7.  **HTTP Response Generation:**
    *   The servlet sends the formatted results (e.g., JSON data, success messages, or error messages) back to the client in the HTTP response, along with an appropriate HTTP status code.

This flow demonstrates how SEMOSS uses a combination of Pixel scripts, a modular Reactor framework, and a versatile Engine/DataFrame abstraction to handle a wide variety of requests, all while enforcing security and permissions.

```
