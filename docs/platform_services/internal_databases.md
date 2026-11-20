# Internal Databases and Agent Persistence

SEMOSS separates platform metadata into system databases. They can use embedded or external database configurations; the checked-in Docker examples use PostgreSQL. Resource definitions and startup settings determine the backend, so H2 is not a universal deployment requirement.

## System database responsibilities

| Service | Responsibility |
| --- | --- |
| Local master | Database metadata and semantic/query relationships used by core data services |
| Security | Users, engine/project/insight catalogs, metadata, dependencies, and permissions |
| Scheduler | Scheduled job state |
| Themes | Theme configuration |
| Prompt | Reusable prompts, metadata, and prompt access |
| Model inference | Model records, persistent rooms, agent workspaces, runs, and pending actions |
| User tracking | User activity records |
| Audit logs | Configured audit records |
| Notifications | Notification state where enabled/configured |

[SystemEngineRegistry](../../src/prerna/util/SystemEngineRegistry.java) provides controlled access to internal engines. Core services should use their existing authorized helpers rather than treat these databases as user-created catalog engines.

## Local master and security

[MasterDatabaseUtility](../../src/prerna/masterdatabase/utility/MasterDatabaseUtility.java) and [LocalMasterOwlCreator](../../src/prerna/masterdatabase/utility/LocalMasterOwlCreator.java) define the local-master interaction and schema. Do not infer project permission tables from its name: security catalog and permission behavior belongs to the security services.

[SecurityEngineUtils](../../src/prerna/auth/utils/SecurityEngineUtils.java) and [SecurityProjectUtils](../../src/prerna/auth/utils/SecurityProjectUtils.java) manage engine/project metadata and access. Agent workspaces and skills are projects of types `WORKSPACE` and `SKILL`, so their reusable identity and permissions participate in this catalog.

External connector state also lives in the security database: `GITHUB_APP` holds the configured app, `GITHUB_PROJECT_LINK` maps projects to repositories, and `MS_GRAPH_SUBSCRIPTION` holds Microsoft change-notification subscriptions and subscriber credentials. [SecurityExternalConnectorsUtils](../../src/prerna/auth/utils/SecurityExternalConnectorsUtils.java) manages these records. See [Monolith webhooks](../integrations/monolith_webhooks.md) for setup, delivery, and renewal behavior.

## Prompt database

With the integration of GenAI capabilities, managing prompts effectively is crucial.

### Purpose and Schema

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

### Access Control

Prompt visibility and modification are governed by the `GLOBAL` flag and the `CREATED_BY` field:

*   **Listing/Viewing**: Users see prompts where `GLOBAL = true` OR `CREATED_BY = <their user ID>`. This applies uniformly to regular users and admins.
*   **Updating**: Regular users can only update prompts they created. Admins can update prompts they created or any global prompt, but cannot update another user's non-global prompt.
*   **Deleting**: Same authorization rules as updating.
*   **GetPromptMetaValues**: Restricted to admin users only.

### Java Interaction

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

## Model-inference database

The model-inference database supports the persistent AI application, not only usage reporting. It must be initialized for the room, workspace, and durable agent services described in these docs. Its schema is defined in [ModelInferenceLogsOwlCreator](../../src/prerna/engine/impl/model/inferencetracking/ModelInferenceLogsOwlCreator.java).

| Table | Role |
| --- | --- |
| `AGENT` | Model/inference metadata retained by the logging schema; distinct from a reusable WORKSPACE agent |
| `MESSAGE` | Inference records, model/room associations, timing, and token usage |
| `FEEDBACK` | Feedback associated with model messages |
| `ROOM` | Conversation identity, user/project/model associations, options, and serialized `MESSAGES` |
| `WORKSPACE` | Agent identity, authored prompt, active state, and `CONFIG_JSON` |
| `WORKSPACE_RESOURCE` | Attached resources such as MCP projects, skills, and prompts |
| `AGENT_RUN` | Run ID, parent run, room/workspace/model/harness, request, progress, status, final output, and errors |
| `AGENT_RUN_ACTION` | Pending tool action, arguments, decision/execution state, and result |

### Conversation versus inference records

[RoomMessageStore](../../src/prerna/engine/impl/model/RoomMessageStore.java) treats `ROOM.MESSAGES` as the durable conversation projection. Optional Redis integration provides a hot copy and coordination. The `MESSAGE` inference log serves a different purpose; it is not interchangeable with the room's message tree and tool state.

A room can be associated with an Insight while remaining a separate persisted conversation. Room IDs, Insight IDs, model engine IDs, workspace IDs, and agent run IDs should not be treated as synonyms.

### Workspaces and skills

Workspace setter reactors maintain both resource rows and configuration JSON. [AgentConfigLoader](../../src/prerna/reactor/agent/config/AgentConfigLoader.java) resolves the effective configuration and room additions. Skills themselves remain project assets; the workspace stores references, not a second authoritative copy of the skill body.

[SystemAgentSeeder](../../src/prerna/util/SystemAgentSeeder.java) reconciles built-in workspace definitions after their platform projects are cataloged. It skips seeding when model-inference storage is unavailable. See [agent configuration](../agents/agent_configuration.md) and [skills](../agents/skills/skills_doc.md).

### Durable runs and decisions

[AgentRunStore](../../src/prerna/reactor/agent/run/AgentRunStore.java) persists run lifecycle state. [AgentRunActionStore](../../src/prerna/reactor/agent/run/AgentRunActionStore.java) persists approval/input boundaries. Normal access is scoped to the owning user; specialized automation operations apply their explicit authorization paths.

The persisted state supports status queries and continuation logic. Live stream events remain process-local, and ordinary active user runs retain live credentials on the submitting node. See [run lifecycle and durability](../agents/agent_runs.md).

## Configuration and initialization

The [Compose initialization SQL](../../docker-compose-examples/init.sql) creates example PostgreSQL databases. Matching `CUSTOM_*` connection settings in the [Compose files](../../docker-compose-examples/README.md) point the application at them. Startup services initialize/migrate their own tables through the configured engines.

Database initialization SQL runs only against a fresh PostgreSQL data directory. Changing environment values or `init.sql` does not rewrite an existing volume's contents. Confirm feature flags and database startup logs before investigating missing rooms or system agents.

See [local Docker configuration](../deployment/docker_configuration.md) for local volumes and initialization. For Kubernetes and semoss-artifacts property configuration, use [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment).
