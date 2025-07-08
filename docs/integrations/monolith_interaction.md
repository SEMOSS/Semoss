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

## Servlet Filters and Their Roles

The `semoss/monolith` web application utilizes a series of servlet filters defined in its `web.xml` to process incoming requests and outgoing responses. These filters handle various concerns, including security, session management, character encoding, and request routing. Below is a breakdown of the key active filters:

### Standard Tomcat Filters

These are common filters provided by Apache Tomcat.

1.  **`SetCharacterEncoding`**
    *   **Class**: `org.apache.catalina.filters.SetCharacterEncodingFilter`
    *   **Purpose**: Ensures that requests and responses are processed using UTF-8 encoding, crucial for correct international character handling.
    *   **Configuration**: `encoding = UTF-8`
    *   **Applies to**: All requests (`/*`).

2.  **`HeaderSecurityFilter`**
    *   **Class**: `org.apache.catalina.filters.HttpHeaderSecurityFilter`
    *   **Purpose**: Adds HTTP security headers to responses to mitigate common web vulnerabilities.
        *   HSTS (HTTP Strict Transport Security): Instructs browsers to only use HTTPS.
        *   X-Frame-Options: Prevents clickjacking (set to `SAMEORIGIN`).
        *   X-Content-Type-Options: Prevents MIME-sniffing.
    *   **Applies to**: All requests (`/*`).

### SEMOSS Custom Filters (from `prerna.web.conf.*` package)

These filters are specific to the SEMOSS application and handle core application logic, security, and session management.

#### Application Initialization & Health

3.  **`StartUpSuccessFilter`**
    *   **Class**: `prerna.web.conf.StartUpSuccessFilter`
    *   **Purpose**: Likely verifies that essential application components (e.g., database connections, core services) initialized correctly during startup. If not, it might block requests or redirect to an error page.
    *   **Applies to**: All requests (`/*`).

#### Security, Session, and User Management

4.  **`NoUserExistsFilter`**
    *   **Class**: `prerna.web.conf.NoUserExistsFilter`
    *   **Purpose**: Checks if any administrative user is configured. If not (e.g., on a fresh install), it may redirect to a setup page (like `/adminconfig/*`) to allow the creation of the first admin user.
    *   **Applies to**: API requests (`/api/*`).

5.  **`SessionCounterExceededFilter`**
    *   **Class**: `prerna.web.conf.SessionCounterExceededFilter`
    *   **Purpose**: Designed to limit concurrent user sessions. (Note: In the analyzed `web.xml`, `sessionLimit` is `-1`, meaning this filter is inactive by default).
    *   **Applies to**: All requests (`/*`).

6.  **`MemoryCheckFilter`**
    *   **Class**: `prerna.web.conf.MemoryCheckFilter`
    *   **Purpose**: Likely monitors memory usage (per-user, per-session, or overall). If usage exceeds thresholds, it might block new requests or terminate expensive operations to prevent OutOfMemoryErrors.
    *   **Applies to**: API requests (`/api/*`).

7.  **`ShareSessionFilter`**
    *   **Class**: `prerna.web.conf.ShareSessionFilter`
    *   **Purpose**: Potentially involved in enabling session sharing across different web contexts or managing session propagation for specific integrations.
    *   **Applies to**: All requests (`/*`).

8.  **`TrustedTokenFilter`**
    *   **Class**: `prerna.web.conf.TrustedTokenFilter`
    *   **Purpose**: Part of a custom token-based authentication scheme. It inspects requests for a "trusted token" and validates it, often used for server-to-server communication.
    *   **Applies to**: All requests (`/*`).

9.  **`UserAccessKeyFilter`**
    *   **Class**: `prerna.web.conf.UserAccessKeyFilter`
    *   **Purpose**: Implements authentication based on user-specific access keys. Requests must provide a valid key for verification.
    *   **Applies to**: All requests (`/*`).

10. **`NoUserInSessionTrustedTokenFilter`**
    *   **Class**: `prerna.web.conf.NoUserInSessionTrustedTokenFilter`
    *   **Purpose**: A specialized token authentication, possibly for specific clients (e.g., Sencha UI, given `trustedTokenPrefix="sencha"`). Allows API access via a valid token even without an existing user session. Accepts tokens from any domain (`trustedTokenDomain="*"`).
    *   **Applies to**: API requests (`/api/*`).

11. **`NoUserInSessionFilter`**
    *   **Class**: `prerna.web.conf.NoUserInSessionFilter`
    *   **Purpose**: A primary security filter for the API. Checks for a valid user session. If absent, it typically blocks API access or redirects to login, unless authentication is handled by a preceding filter.
    *   **Applies to**: API requests (`/api/*`).

12. **`AdminStartupFilter`**
    *   **Class**: `prerna.web.conf.AdminStartupFilter`
    *   **Purpose**: Secures the `/adminconfig/*` path, ensuring only authenticated admins can access these configuration endpoints (handled by `AdminApplication`).
    *   **Applies to**: Admin configuration requests (`/adminconfig/*`).

13. **`PublicHomeCheckFilter`**
    *   **Class**: `prerna.web.conf.PublicHomeCheckFilter`
    *   **Purpose**: Manages access to publicly shared resources or dashboards under `/public_home/*`. Checks if a resource is marked public or if specific access rules apply.
    *   **Applies to**: Public home requests (`/public_home/*`).

#### Request-Specific Processing Filters

14. **`OpenAIFilter`**
    *   **Class**: `prerna.web.conf.OpenAIFilter`
    *   **Purpose**: Specific to OpenAI model interactions (`/api/model/openai/*`). Could handle API key injection, request/response modification for OpenAI, logging, or policy enforcement for OpenAI usage.
    *   **Applies to**: OpenAI model requests (`/api/model/openai/*`).

15. **`SchedulerFilter`**
    *   **Class**: `prerna.web.conf.SchedulerFilter`
    *   **Purpose**: Applied to scheduler API calls (`/api/schedule/*`). May handle auth specific to scheduling or ensure correct request routing/formatting.
    *   **Applies to**: Scheduler API requests (`/api/schedule/*`).

16. **`APIFilter`**
    *   **Class**: `prerna.web.conf.APIFilter`
    *   **Purpose**: Mapped to `/data/*` URLs (handled by `APIApplication` servlet). Might set up context for data-centric API interactions, possibly related to external applications or API users.
    *   **Applies to**: Data API requests (`/data/*`).

It's important to note that several other filters related to specific authentication mechanisms (SAML, Waffle, CAC/PIV, Anonymous User) were found commented out in the `web.xml`. This indicates that these are optional features that can be enabled if needed.
