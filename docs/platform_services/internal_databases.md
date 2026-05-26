# SEMOSS Internal Databases and Java Interaction

SEMOSS utilizes several internal databases for its operational needs, including metadata storage, user tracking, prompt management, and security. This document outlines how Java components interact with these key internal databases. Most of these databases are typically H2 file-based databases, configured via `.smss` files found in the `db/` directory.

## 1. Overview of Internal Databases

SEMOSS leverages a set of specialized databases, often H2 instances, for managing its core metadata and operational data:

*   **`LocalMasterDatabase`**: Stores metadata about user-created and system-level assets like projects, engines (data sources), insights, and their relationships. It's central to organizing and retrieving user work.
*   **`PromptDatabase`**: Specifically designed to store and manage prompts used with Large Language Models (LLMs) and other GenAI features within SEMOSS.
*   **`UserTrackingDatabase`**: Records user activity, system events, and audit trails.
*   **`SecurityDB`**: (Often named `security.smss`) Stores user credentials, roles, permissions for projects, engines, insights, and other assets. This database is fundamental to SEMOSS's access control mechanisms. (Covered in more detail in the Authentication & Authorization section).
*   **`ThemesDatabase`**: (Often named `themes.smss`) Stores theme configurations for customizing the SEMOSS UI appearance.
*   **`SchedulerDatabase`**: (Often named `scheduler.smss`) Manages scheduled tasks and jobs within SEMOSS.
*   **Other Utility Databases**: Depending on the configuration, there might be other small, special-purpose databases.

Java components interact with these databases primarily through JDBC, often abstracted by utility classes or specific data access objects (DAOs).

## 2. `LocalMasterDatabase`

The `LocalMasterDatabase` is arguably one of the most critical internal databases, as it holds the metadata for user assets and system configurations.

### 2.1. Purpose and Schema (Conceptual)

*   **Purpose**: To catalog projects, engines (data sources), insights (analyses/dashboards), and the relationships between them. It also stores metadata about global vs. user-specific assets.
*   **Key Information Stored (Conceptual Tables)**:
    *   `PROJECT`: Information about projects (ID, name, type, creator, visibility, etc.).
    *   `ENGINE`: Information about data engines (ID, name, type, configuration path (SMSS file), creator, global status, etc.). This table is also managed by `SecurityEngineUtils` for consistency in the security database.
    *   `INSIGHT`: Information about insights (ID, name, project associations, creator, etc.).
    *   `PROJECT_ENGINE_RELATION`: Links projects to the engines they use.
    *   `PROJECT_INSIGHT_RELATION`: Links projects to the insights they contain.
    *   `USER_PROJECT_PERMISSIONS`, `USER_ENGINE_PERMISSIONS`, `USER_INSIGHT_PERMISSIONS`: While primarily managed in the `SecurityDB`, there might be some denormalized or cached permission information here, or this data might solely reside in `SecurityDB`.
    *   Metadata tables for storing additional key-value properties for projects, engines, and insights.

### 2.2. Java Interaction (`src/prerna/masterdatabase/`)

The `src/prerna/masterdatabase/` package contains Java classes responsible for interacting with the `LocalMasterDatabase`.
*   **`prerna.masterdatabase.utility.MasterDatabaseUtility`**: This class likely provides high-level methods to query and manipulate metadata stored in `LocalMasterDatabase`. It might offer functions to:
    *   Retrieve lists of projects, engines, or insights for a user.
    *   Get detailed metadata for a specific project, engine, or insight.
    *   Add, update, or delete metadata entries.
*   **Specific Reactors**: Reactors within sub-packages like `src/prerna/reactor/masterdatabase/` (e.g., `GetProjectListReactor`, `GetEngineListReactor`, `GetInsightListReactor`) would use `MasterDatabaseUtility` or direct JDBC calls to fetch information required by Pixel scripts.
*   **OWL Representation**: Often, the metadata for these assets (especially engines and projects) is also stored or cached as OWL (Web Ontology Language) files (e.g., `db/LocalMasterDatabase/MasterDatabase_OWL.OWL`). Java classes like `prerna.masterdatabase.utility.MasterDatabaseOwlCreatorHelper` might be involved in generating or updating these OWL files from the database or vice-versa. These OWL files can provide a semantic representation of the assets and their relationships.

## 3. `PromptDatabase`

With the integration of GenAI capabilities, managing prompts effectively is crucial.

### 3.1. Purpose and Schema

*   **Purpose**: To store, categorize, and manage prompts that can be used with various LLMs integrated into SEMOSS. This allows users to save, reuse, and share effective prompts with access control via a `GLOBAL` flag.
*   **Tables**:
    *   `PROMPT`: Stores the core prompt data. Supports versioning — updates create a new row with an incremented `VERSION` and the previous row's `IS_LATEST` is set to `false`.
        *   `ID` (VARCHAR) — Unique prompt identifier (UUID)
        *   `TITLE` (VARCHAR) — Prompt name
        *   `CONTEXT` (CLOB) — The prompt text/template
        *   `VERSION` (INTEGER) — Version number, starting at 0
        *   `INTENT` (VARCHAR) — Optional description of the prompt's purpose
        *   `CREATED_BY` (VARCHAR) — User ID of the creator
        *   `DATE_CREATED` (TIMESTAMP) — Creation timestamp
        *   `IS_LATEST` (BOOLEAN) — Whether this is the current version
        *   `GLOBAL` (BOOLEAN) — Whether the prompt is visible to all users. When `false`, only the creator can see it.
    *   `PROMPTMETA`: Stores tags and arbitrary key-value metadata for prompts. Tags are stored with `METAKEY='tag'`; other metadata uses the actual key name.
        *   `PROMPT_ID` (VARCHAR) — Foreign key to `PROMPT.ID`
        *   `METAKEY` (VARCHAR) — The metadata category (e.g., `"tag"`, `"department"`, `"region"`)
        *   `METAVALUE` (VARCHAR) — The metadata value
        *   `METAORDER` (INTEGER) — Ordering within a given metakey
    *   `PROMPTMETAKEYS`: Registry of available metadata keys, synced from the security database's `USERMETAKEYS` table on first use. Stores display configuration for each metakey.
        *   `METAKEY` (VARCHAR) — The metadata key name
        *   `SINGLEMULTI` (VARCHAR) — Whether the key accepts single or multiple values
        *   `DISPLAYORDER` (INTEGER) — Display ordering
        *   `DISPLAYOPTIONS` (VARCHAR) — Display configuration
        *   `DEFAULTVALUES` (VARCHAR) — Default values for the key

### 3.2. Access Control

Prompt visibility and modification are governed by the `GLOBAL` flag and the `CREATED_BY` field:

*   **Listing/Viewing**: Users see prompts where `GLOBAL = true` OR `CREATED_BY = <their user ID>`. This applies uniformly to regular users and admins.
*   **Updating**: Regular users can only update prompts they created. Admins can update prompts they created or any global prompt, but cannot update another user's non-global prompt.
*   **Deleting**: Same authorization rules as updating.
*   **GetPromptMetaValues**: Restricted to admin users only.

### 3.3. Java Interaction

*   **`prerna.prompt.PromptUtils.java`**: Core utility class in `src/prerna/prompt/` providing all CRUD operations for prompts. Key methods:
    *   `addPrompt(...)` — Creates a new prompt, inserts tags and metadata, returns the generated UUID.
    *   `editPrompt(...)` — Versions an existing prompt (marks old as not latest, inserts new row) with authorization checks.
    *   `deletePrompt(...)` — Removes a prompt and its metadata from `PROMPT` and `PROMPTMETA` tables after authorization.
    *   `getPrompt(...)` — Retrieves a single prompt by ID with access control, including tags and metadata.
    *   `getPrompts(...)` — Lists prompts with visibility filtering, optional metadata-based filtering, and pagination.
    *   `checkPromptTitle(...)` — Checks if an accessible prompt with the given title exists.
    *   `getAvailableMetaValues(...)` — Returns distinct metadata values with usage counts, grouped by metakey.
    *   `updatePromptMetadata(...)` — Replaces specific metadata fields for a prompt.
*   **`prerna.prompt.AbstractPromptUtils.java`**: Handles database initialization and schema migration, including adding new columns (e.g., `GLOBAL`) to existing tables.
*   **`prerna.prompt.PromptOwlCreator.java`**: Defines the OWL representation of the prompt database schema.
*   **Reactors** (in `src/prerna/reactor/prompt/`):
    *   `AddPromptReactor` — Creates a prompt, returns the new prompt UUID.
    *   `UpdatePromptReactor` — Updates an existing prompt with authorization.
    *   `DeletePromptReactor` — Deletes a prompt, returns the deleted prompt UUID.
    *   `GetPromptReactor` — Retrieves a single prompt by ID.
    *   `ListPromptReactor` — Lists prompts with filtering and pagination.
    *   `CheckPromptTitleReactor` — Checks title availability.
    *   `GetPromptMetaValuesReactor` — Returns metadata value counts (admin only).

See each reactor class for Pixel usage, parameters, and return types.

## 4. Database Configuration and Access

*   **SMSS Files**: Each internal database (LocalMaster, PromptDB, SecurityDB, etc.) has its connection details defined in a respective `.smss` file located in the `db/` directory (e.g., `db/LocalMasterDatabase.smss`, `db/PromptDatabase.smss`). These files specify the JDBC driver, connection URL (often pointing to an H2 file like `database.mv.db` within its respective subdirectory), username, and password.
*   **`DIHelper.java`**: As discussed previously, `DIHelper` plays a role in managing access to the *paths* of these `.smss` files or the loaded `Properties` objects.
    *   `DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE)` is a common pattern to get the SMSS file path for an engine (where `engineId` would be `Constants.LOCAL_MASTER_DB`, `Constants.PROMPT_DB`, etc.).
*   **`prerna.engine.impl.SmssUtilities.java`**: This utility class contains methods like `getEngine(String engineId)` which can take an engine ID (like `Constants.LOCAL_MASTER_DB`), retrieve its SMSS properties (likely via `DIHelper`), instantiate the correct `IEngine` implementation (usually an `RDBMSNativeEngine` for these H2 databases), and return the opened engine.
*   **JDBC and Query Utilities**:
    *   Once an `IEngine` instance for an internal database is obtained, interactions often boil down to standard JDBC operations.
    *   `prerna.util.sql.AbstractSqlQueryUtil` and its database-specific implementations (e.g., for H2) provide helper methods for executing queries, preparing statements, and processing results.
    *   Higher-level utility classes (like `MasterDatabaseUtility` or `PromptUtils`) would use these JDBC utilities to perform their tasks.

By using this setup, SEMOSS maintains a clear separation of concerns, with dedicated databases for different types of operational data, all accessed through a consistent mechanism of SMSS configuration files and Java database interaction utilities.

## 5. `ModelInferenceLogsDatabase`

The `ModelInferenceLogsDatabase` is a specialized internal database dedicated to tracking and auditing interactions with Large Language Models (LLMs) and other model engines within SEMOSS.

### 5.1. Purpose and Schema (Conceptual)

*   **Purpose**: To provide a comprehensive log of all requests and responses to model engines, capture usage metrics (like token counts and response times), associate interactions with users, insights (conversations/rooms), and projects, and store user feedback on model responses.
*   **Initialization**:
    *   The database schema (tables, columns, keys, indexes) is programmatically defined and managed by `prerna.engine.impl.model.inferencetracking.ModelInferenceLogsOwlCreator`.
    *   It's typically an H2 database, configured via an SMSS file (e.g., `db/ModelInferenceLogsDatabase.smss`).
*   **Key Tables (Conceptual Names - actual names might have prefixes/suffixes like `MESSAGE__`)**:
    *   `MESSAGE`: Stores individual LLM interactions.
        *   Columns: `MESSAGE_ID` (Primary Key), `TRANSACTION_ID` (id for llm interaction), `MESSAGE_TYPE` (e.g., "INPUT", "OUTPUT"), `MESSAGE_DATA` (the actual prompt or response, potentially large, may be stored as CLOB/BLOB), `MESSAGE_METHOD` (e.g., "ask", "instruct", "embeddings"), `MESSAGE_TOKENS` (token count for the message), `RESPONSE_TIME`, `DATE_CREATED`, `AGENT_ID` (FK to AGENT table), `INSIGHT_ID` (FK to ROOM table, representing the conversation/room ID), `SESSIONID`, `USER_ID`, `USER_NAME`, `USER_EMAIL_ID`.
    *   `AGENT`: Stores information about the LLM agents or model engines being used.
        *   Columns: `AGENT_ID` (Primary Key), `AGENT_NAME`, `DESCRIPTION`, `AGENT_TYPE` (e.g., "OpenAI", "Bedrock"), `AUTHOR`, `DATE_CREATED`.
    *   `ROOM`: Represents conversation sessions or contexts in which LLM interactions occur. Often, an "Insight" in SEMOSS serves as a "room".
        *   Columns: `INSIGHT_ID` (Primary Key, also the Room ID), `ROOM_NAME`, `ROOM_CONTEXT` (overall context for the conversation), `USER_ID`, `USER_NAME`, `USER_EMAIL_ID`, `AGENT_ID` (FK to AGENT table, the primary model for the room), `IS_ACTIVE`, `DATE_CREATED`, `PROJECT_ID`, `PROJECT_NAME`.
    *   `FEEDBACK`: Stores user-provided feedback on model responses.
        *   Columns: `MESSAGE_ID` (FK to MESSAGE table), `MESSAGE_TYPE` (e.g., "RESPONSE"), `FEEDBACK_TEXT`, `FEEDBACK_DATE`, `RATING` (e.g., thumbs up/down as boolean or integer).

### 5.2. Java Interaction

*   **`prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils.java`**: This is the primary utility class for all interactions with the `ModelInferenceLogsDatabase`.
    *   It handles database initialization, including schema creation and updates.
    *   Provides methods like:
        *   `doRecordMessage()`: To log a new model interaction (called by `AbstractModelEngine` and `AbstractVectorDatabaseEngine` after an LLM call).
        *   `doCreateNewConversation()`: To create a new room/conversation record.
        *   `doCreateNewAgent()`: To register a new model agent if it's not already present.
        *   `recordFeedback()`, `updateFeedback()`, `deleteFeedbackEntry()`: To manage user feedback on model responses.
        *   Various retrieval methods for fetching conversation histories (`doRetrieveConversation`), listing user conversations (`getUserConversations`), and generating usage reports (e.g., `getOverAllEngineUsageFromModelInferenceLogs`, `getTokenUsagePerProjectForEngine`).
*   **`prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker.java`**: This class, typically run in a separate thread, is responsible for asynchronously calling `ModelInferenceLogsUtils.doRecordMessage()` to ensure that logging model interactions does not block the main execution flow of model calls.
*   **Reactors for Usage and History**: Reactors in `src/prerna/engine/impl/model/inferencetracking/reactors/` (e.g., `GetRoomMessagesReactor`, `GetUserConversationRoomsReactor`) use `ModelInferenceLogsUtils` to expose conversation history and usage data via Pixel scripts.

This database is essential for monitoring LLM usage, understanding costs, gathering data for potential fine-tuning, and providing users with access to their interaction histories.
