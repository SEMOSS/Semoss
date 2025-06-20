# SEMOSS Monolith Interaction

This document outlines the interaction between the main SEMOSS platform (presumably the components developed in the current repository, referred to as `semoss/semoss` for clarity) and the `semoss/monolith` web application.

## Overview of `semoss/monolith`

The `semoss/monolith` repository houses a Java-based web application packaged as a WAR (Web Application Archive). Its primary role is to serve as the main backend server for SEMOSS, exposing APIs and handling client requests. It builds upon a core `semoss` library (artifact `org.semoss:semoss`), which is likely produced by the `semoss/semoss` repository.

Key characteristics of the Monolith:
- **Technology**: Java, Maven, RESTEasy (for JAX-RS RESTful services), WebSockets.
- **Packaging**: Deployed as a WAR file on a Java web server (e.g., Tomcat).
- **Core Logic**: Leverages a base `semoss.jar` for fundamental SEMOSS functionalities like Pixel processing, engine management, and data utilities.
- **Configuration**: Shares configuration mechanisms with the core `semoss` library, such as `RDF_Map.prop` and `log4j2.properties`.

## Key Interaction Points

### 1. Core Library Dependency

The most fundamental interaction is that `semoss/monolith` includes the `org.semoss:semoss` artifact (the core SEMOSS library) as a Maven dependency. This means the Monolith uses the classes and methods from this core library to perform its operations. The Monolith essentially provides the web-facing layer (APIs, request handling, session management) on top of the functionalities provided by the core `semoss` library.

### 2. RESTful API via RESTEasy

The Monolith exposes a comprehensive set of RESTful API endpoints using JAX-RS, with RESTEasy as the implementation. These endpoints are primarily accessible under the `/api/*` URL path, as configured in its `web.xml` file via the `prerna.semoss.web.app.MonolithApplication` JAX-RS application class.

**The `runPixel` Endpoint:**

A critical endpoint for the platform is `runPixel`. While its exact path under `/api/` and specific signature are defined within the Java JAX-RS resource classes in the Monolith's `prerna.semoss.web.app` package, its general behavior involves:

- **Request**: Clients (e.g., SEMOSS UIs, external applications) send HTTP requests (typically POST) to this endpoint. The request payload contains:
    - The Pixel code to be executed.
    - Optionally, parameters such as target database/engine IDs, variable assignments, and context information.
- **Processing**:
    1. The Monolith's JAX-RS resource class receives the request.
    2. It authenticates and authorizes the request based on the configured security filters (e.g., session tokens, SAML assertions, API keys).
    3. It then utilizes the embedded `semoss.jar` (core library) to:
        - Parse the Pixel code.
        - Generate an execution plan.
        - Run the query against the appropriate data engines (databases, storage, models, etc.).
    4. Results from the Pixel execution are returned from the core library to the JAX-RS resource.
- **Response**: The JAX-RS resource formats the results (typically as JSON) and sends them back in the HTTP response to the client. This can include data, messages, errors, or status information.

Other API endpoints under `/api/*` handle various other platform functions like engine management, data uploads, user authentication, scheduling tasks (`/api/schedule/*`), and OpenAI model interactions (`/api/model/openai/*`).

### 3. WebSocket Communication

The Monolith is equipped for WebSocket communication (indicated by `javax.websocket/javax.websocket-api` in its `pom.xml`). This allows for real-time, bidirectional communication channels between clients and the server. Use cases might include:
- Live updates for dashboards or data visualizations.
- Collaborative editing or interaction sessions.
- Streaming of large query results or logs.
The specific WebSocket endpoints and their protocols are defined within Java classes in the Monolith.

### 4. Shared Configuration Files

The Monolith relies on configuration files like `RDF_Map.prop` (for semantic mappings) and `log4j2.properties` (for logging), which are also central to the core SEMOSS platform. This ensures consistency in how both the core library and the Monolith web application interpret semantic data and manage logging.

### 5. Authentication and Authorization

The Monolith is responsible for securing its exposed endpoints. It employs a suite of configurable security filters (defined in `web.xml` and implemented in `prerna.web.conf.*` classes) to handle:
- User authentication (SAML, trusted tokens, user access keys, etc.).
- Session management.
- Authorization checks.
Clients interacting with the Monolith's API must adhere to these security protocols.

## Typical Interaction Flow (Example: UI running a Pixel query)

1.  A user interacts with a SEMOSS web interface, triggering a Pixel query.
2.  The UI client constructs an HTTP request (e.g., POST) to the Monolith's `/api/.../runPixel` endpoint, including the Pixel code and any necessary session tokens or authentication headers.
3.  The Monolith receives the request. Its security filters validate the user's session and authorization.
4.  The designated JAX-RS resource class for `runPixel` is invoked.
5.  This resource class calls methods within the core `semoss.jar` to execute the Pixel query.
6.  The core library processes the query (connects to databases, performs computations, etc.) and returns the results.
7.  The JAX-RS resource packages the results into a JSON response.
8.  The Monolith sends the HTTP response back to the UI client.
9.  The UI client renders the results for the user.

## Development and Troubleshooting

- When changes are made to the core `semoss` library (in `semoss/semoss`), a new version of the `org.semoss:semoss` artifact must be built and published.
- The `semoss/monolith` project then needs to be updated to use this new version, rebuilt into a WAR, and redeployed.
- Troubleshooting issues often involves checking logs from both the Monolith application (Tomcat logs, application logs configured via log4j2) and potentially any client-side logs if the interaction originates from another application. Understanding the flow through the RESTEasy endpoints and the underlying Pixel execution logic is key.

This documentation provides a high-level overview of the interaction. For precise details of API endpoint signatures, request/response structures, and internal logic, developers would need to consult the Java source code within the `semoss/monolith` repository, particularly the JAX-RS resource classes in the `prerna.semoss.web.app` package and filter configurations in `web.xml`.
