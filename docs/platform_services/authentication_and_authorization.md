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

Custom reactors should check access before loading and operating on a user-selected resource. A resource identifier supplied by a client or model is not proof of permission. Use the platform's security utilities and existing authorized operations instead of introducing a parallel permission model.

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
