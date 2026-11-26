# Authentication and Authorization

SEMOSS authenticates requests through the configured Monolith web layer and enforces access to engines, projects, insights, rooms, and runs in the corresponding services. The two concerns are related but separate: a signed-in user still needs permission to use a resource.

## Identity and resource access

| Component | Role |
| --- | --- |
| [User](../../src/prerna/auth/User.java) | Current user and associated login identities |
| [AccessToken](../../src/prerna/auth/AccessToken.java) | Provider identity/token information used by authentication integrations; not a claim that every request uses a JWT |
| [AuthProvider](../../src/prerna/auth/AuthProvider.java) | Supported provider identifiers |
| [AccessPermissionEnum](../../src/prerna/auth/AccessPermissionEnum.java) | Resource levels `OWNER`, `EDIT`, and `READ_ONLY` |
| [SecurityEngineUtils](../../src/prerna/auth/utils/SecurityEngineUtils.java) | Engine access and metadata |
| [SecurityProjectUtils](../../src/prerna/auth/utils/SecurityProjectUtils.java) | Project access and metadata, including WORKSPACE and SKILL projects |
| [SecurityInsightUtils](../../src/prerna/auth/utils/SecurityInsightUtils.java) | Saved-insight access |

The enabled providers and filters depend on `social.properties`, Monolith configuration, and deployment settings. See [configuration](../development_guides/configuration_and_environment.md) and [Monolith integration](../integrations/monolith_interaction.md).

## Request handling

Monolith establishes the configured session or integration identity. REST resources resolve the user's execution context. Reactors and service methods then validate the specific operation, such as viewing an engine, editing a target project, or changing ownership settings.

Custom reactors should check access before loading and operating on a user-selected resource. A resource identifier supplied by a client or model is not proof of permission. Use `CustomReactorSecurity` for what the signed in user may do and to find people, and the platform's existing authorized operations, instead of introducing a parallel permission model; see [What custom reactors use instead](project_reactor_sandbox.md#what-custom-reactors-use-instead).

## App portals

Monolith serves a project's portal under `public_home/<projectId>/portals/` through `PublicHomeCheckFilter`. A signed in user gets it when they can view the project now: it is public, or they have a grant in effect on it, given to them directly or to a team they are in. A signed out visitor gets it when the project has a login page of its own (the `HAS_CUSTOM_LOGIN` column of `PROJECT`), so they can sign in there; the REST endpoints the app then calls still need a signed in user. Every project starts without one. Its owners turn it on or off with `/api/auth/project/setProjectCustomLogin`, and admins with `/api/auth/admin/project/setProjectCustomLogin`, each taking `projectId` and `customLogin`; a Digital Twin never has one. Every refusal is recorded in the audit trail as a failed `PROJECT_VIEW`, and every change as `PROJECT_CUSTOM_LOGIN_UPDATE`.

## Session ids

A session id is all a browser needs to act as its user, so the platform writes its hash, never the id, everywhere people or other processes can read it: the insight cache folder of the session, the log4j `sessionId` context and the `AUDIT_LOGS` and `SERVER_LOGS` rows written from it, `USER_TRACKING`, the user audit trail, the inference logs, an agent room's insight id, and the OpenShell sandbox leases. [SessionIds](../../src/prerna/security/SessionIds.java) makes the hash: an HMAC-SHA256 keyed by the RDF_Map setting `LOG_HASH_SALT`, as 64 hex characters, so the same session has the same hash in every folder, log and table. Every server of a cluster has the same `LOG_HASH_SALT`, so a session's hash is the same on each of them; without the setting, the hash is a plain SHA-256 of the id.

The id itself stays with the parts that find or become the live session: the HTTP session and its cookie, the headless browser cookie an export sets, the `InsightStore` and `ThreadStore` session lookups, a share-session token, which becomes the browser's cookie when it is used, and the user's own Python process, which hands the id back on its calls. A share token that only signs its holder in keeps no session id.

## Changes to engines, projects and insights

The write methods of the security utilities check the acting user themselves, so a caller that skips its own check is still refused. Each takes the signed in user and needs a level on the target:

| Change | Needs |
| --- | --- |
| Creating an engine or a project: `addEngine`, `addProject` | The creation rules: the publisher rule, the admin only rule for the type, for a public one the admin only rule for making it public, and an id that no project has (for an engine) or no engine has (for a project), for admins too |
| Making its creator the owner: `addEngineOwner`, `addProjectOwner`, `addUserInsightCreator` | The user created the engine or project and no one has been given access to it yet; for an insight, edit access on its project and no one given access to the insight yet |
| Changing an engine's or project's metadata, dependencies, portal and reactor records, and an insight: `updateEngineMetadata`, `setDefaultEngineMarkdown`, `updateProjectMetadata`, `updateProjectDependencies`, `setPortalPublish`, `setReactorCompilation`, `createInsightsDatabase`, `addInsight`, the `updateInsight` methods, `deleteInsight` | Edit access on the engine, project or insight; an owner of a project can edit its insights |
| Deleting an engine or a project, and marking a Digital Twin: `deleteEngine`, `deleteProject`, `markDigitalTwin` | Ownership |
| Syncing model metadata from the catalog: `syncModelMetadataFromCatalog` | An admin in an admin operation |
| Replacing a person's engine permissions from an external permission service, and making a platform project public: `updateEngineUserPermissions`, `setProjectCompletelyGlobal` | The platform's own work |

The user who created an engine or a project also passes the edit and owner checks until someone is given access to it, which covers the steps of creating one and cleaning up after a creation that fails. An admin passes every check in an admin operation: an admin reactor or endpoint. The platform's own work for no user runs inside a `PlatformOperation`; registering an engine or a project from its smss passes no user and gives no one access to it. An engine and a project never share an id (`requireNoProjectWithId` and `requireNoEngineWithId` in `AbstractSecurityUtils`): uploading an engine or a project, and creating a project, check the id before any files are written, and every engine a user creates is checked when it is registered. A call with no user outside a platform operation is refused and logged. The checks live in `ResourceWriteAccess` and are tested in `ResourceWriteAccessUnitTests`.

A reactor that does other work before the write, such as writing files or another database, also checks first, so it refuses before it changes anything.

## Shared authentication

`GetShareAuthToken()` records the caller's user ID and login type for authentication in another browser session. Monolith's `ShareSessionFilter` reconstructs the matching account's token through `SecurityShareSessionUtils.generateAccessTokenForShareAuth`. Reconstruction preserves the canonical user ID independently of email or username; an API user does not need an email address to retain its identity for model access, usage limits, or room ownership. Missing identity fields or an account that cannot be resolved are rejected rather than returning an incomplete token.

This does not change `GetShareSessionToken()` semantics: sharing authentication establishes a login in a separate session, while sharing a session reuses the existing session.

## Agents, skills, and approvals

Agent execution has several access boundaries:

- The selected model must be accessible and appropriate for the requested model operation.
- A reusable workspace agent is governed by project access; editing its configuration needs edit access.
- Editing files in a project target needs the target's edit permission, independently of the agent workspace.
- Skill attachment needs workspace edit access and access to the skill project. Reading skill content follows project asset access rules.
- Tool calls enforce access to the engines or projects they operate on.
- Ordinary run reads, streaming, cancellation, and pending-action decisions use the run's owner identity. Specialized automation paths have their own explicit authorization logic.

A tool approval is permission to perform the recorded action within the caller's existing authority; it does not grant new engine or project privileges. Decisions are validated against persisted `AGENT_RUN_ACTION` records. Skills, prompts, and working-directory instruction files describe behavior and do not grant permissions.

Filesystem containment, restricted built-in commands, and optional process sandboxing are additional runtime controls. Their scope depends on the selected tool and deployment configuration; do not equate them with resource authorization or assume universal process isolation.

## Related guides

- [Agent configuration](../agents/agent_configuration.md)
- [Approval and input pauses](../agents/agent_runs.md#approval-and-input-pauses)
- [Skill content and access](../agents/skills/skills_doc.md)
- [Internal databases](internal_databases.md)
