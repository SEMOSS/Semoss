# SEMOSS Backend Architecture Overview

The SEMOSS backend is a sophisticated system responsible for orchestrating data processing, user requests, and interactions with a multitude of services. While this documentation primarily details the Java-based implementation of these backend concepts, the architecture is designed to be modular.

## Key Backend Components (Java Implementation Focus)

The core of the platform, currently implemented primarily in Java, revolves around several key components:

*   **Pixel Query Engine**: At the heart of SEMOSS is a powerful engine (leveraging components from `src/prerna/sablecc2`) responsible for parsing and executing "Pixel," SEMOSS's custom query and scripting language. This allows for complex data manipulations and workflow definitions across the platform.
*   **Reactor Framework**: SEMOSS employs a Reactor pattern (`src/prerna/reactor`) enabling extensible and modular functionality. Reactors are Java classes that handle specific commands or operations defined in Pixel scripts, making it easy to add new features.
*   **Engine Abstraction (`IEngine`)**: The `prerna.engine.api.IEngine` interface provides a standardized way for the core system to interact with various data sources, AI models, storage systems, and custom functions. Different implementations of `IEngine` handle the specifics of each backend service.
*   **Data Source & DataFrame Layer (`src/prerna/ds`)**: This layer contains abstractions (like `ITableDataFrame`) and wrappers that allow SEMOSS to work seamlessly with different structures of data in memory, often in conjunction with the `IEngine` implementations or as results of Pixel operations.
*   **Authentication and Authorization (`src/prerna/auth`)**: Manages user identity, access control to resources (engines, projects, insights), and security policies within the platform.
*   **Inter-Process Communication**: Mechanisms for Java to interact with other language environments, notably Python (detailed in a separate document), for leveraging external libraries and functionalities.

## Data Storage and Interaction

The SEMOSS backend interacts with several types of data stores:

*   **Primary Data Sources (via Engines)**: These are the external databases, storage systems, model APIs, etc., that users connect to for their analytics and operations. These are accessed via the `IEngine` interface.
*   **Internal Databases (`db/` directory)**: SEMOSS uses a set of internal databases (typically H2) for its own operational metadata:
    *   **`LocalMasterDatabase`**: Stores metadata about SEMOSS projects, configured engines, user insights, and their relationships.
    *   **`SecurityDB`**: Manages user credentials, roles, and permissions for accessing SEMOSS assets.
    *   **`PromptDatabase`**: Stores and manages prompts for Large Language Model interactions.
    *   **`ModelInferenceLogsDatabase`**: Logs interactions with model engines for auditing and usage tracking.
    *   **`UserTrackingDatabase`**: Records user activity and system events.
    *   Others for themes, scheduling, etc.

The backend reads various configuration files (e.g., `RDF_Map.prop`, `social.properties`, individual engine `.smss` files) to manage these components, connections, and overall behavior.

## High-Level API Request Flow

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
