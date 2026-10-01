# Configuring SEMOSS Agents

A reusable agent is a project of type `WORKSPACE` plus a workspace record in the model-inference database. Its configuration combines authored instructions, MCP resources, skills, model defaults, execution budgets, hooks, and subagent settings.

The model-inference database must be initialized. The caller needs access to the model and agent workspace, and the appropriate access to the execution target and tools. System agents are seeded from the distribution; use a separate custom workspace for your own configuration.

## Create an agent

Run the following Pixel in an authenticated SEMOSS context. The `database` and `python` skill projects must be available in the installation:

```pixel
AddWorkspace(
    name=["Data Assistant"],
    description=["Explains datasets and produces reproducible analysis"],
    systemPrompt=["Ground conclusions in returned data and save reusable analysis when requested."],
    skills=["database", "python"],
    mcp=[]
);
```

[AddWorkspace](../../src/prerna/engine/impl/model/inferencetracking/reactors/workspaces/AddWorkspaceReactor.java) creates the underlying project and workspace metadata. Use its returned workspace identifier in subsequent calls. Name validation requires a leading letter and allows letters, numbers, and spaces.

```pixel
GetWorkspace(workspaceId=["<workspace-id>"]);
AttachSkillToWorkspace(workspaceId=["<workspace-id>"], skillId=["exports"]);
```

Attached skills provide instructions. To query a particular database, also expose an appropriate permitted tool or integration for that resource. Workbenches can add tools and context for the engine currently being viewed.

## Run it against a target

```pixel
RunAgent(
    roomId=["<room-id>"],
    command=["Inspect the project files and explain the analysis workflow."],
    engine=["<model-engine-id>"],
    workspaceId=["<workspace-id>"],
    harnessType=["semoss"],
    space=["<target-project-id>"],
    maxTurns=[12],
    wait=[false]
);
```

Use an accessible room identifier. When an explicit engine is supplied, the runner can initialize a room if needed; otherwise it loads the existing room. The response contains the `runId` used for monitoring. See [run lifecycle](agent_runs.md).

### Agent identity and file target

| Input | Purpose |
| --- | --- |
| `workspaceId` | Selects the agent configuration; overrides the room's workspace for this run |
| `space=["INSIGHT"]` or omitted | Uses the default room/insight target, subject to inherited run context |
| `space=["USER"]` | Uses the authenticated user's asset project |
| `space=["<project-id>"]` | Uses an editable target project's assets |
| `paramValues=[{"subdir":"public"}]` | Selects a relative subdirectory within the resolved target |
| `paramValues=[{"project":"<project-id>"}]` | Supported project-target form when top-level `space` is omitted |

Pass `space` at the top level, not inside `paramValues`. Do not combine `space` with `paramValues.project`. Relative subdirectories must stay inside the authorized target. The legacy absolute `filePath` input is ignored; it is not a supported way to select arbitrary host files.

An explicit workspace is applied as a temporary room overlay for the run and restored afterward. Persisted workbench selection is a separate frontend/room-options behavior.

### Model selection

[AgentRunner](../../src/prerna/reactor/agent/AgentRunner.java) selects the model in this order:

1. Explicit `engine` on the request.
2. The room's `MODEL_ID`.
3. Legacy model identifiers in room options.
4. The workspace's `CONFIG_JSON.model_id` default.

The resolved engine must be accessible to the user and be a model suitable for text generation. A workspace default does not override an explicit runtime selection or an existing room model.

## Update configuration

[EditWorkspace](../../src/prerna/engine/impl/model/inferencetracking/reactors/workspaces/EditWorkspaceReactor.java) supports more configuration fields than `AddWorkspace`:

| Pixel input | Stored configuration / behavior |
| --- | --- |
| `systemPrompt` | Authored `system_prompt` |
| `modelId` | Default `model_id`; omit to preserve, blank to clear |
| `mcp` | Full resource list, using maps such as `{id, name, type}` |
| `skills` | Full skill-project ID list |
| `maxTurns`, `maxReflections`, `maxSeconds` | `budgets.max_turns`, `max_reflections`, `max_seconds` |
| `maxSubagentDepth`, `maxSubagentsPerRun`, `maxSpawnsPerTurn` | `spawn_policy` limits |
| `subagents` | Named subagent specifications |
| `hooks` | Registered lifecycle/tool hook configuration |
| `useDefaultAgentTools`, `disabledDefaultTools` | Default-tool exposure and dispatch policy |
| `greeting`, `greetingEnabled` | UI greeting configuration; not model instructions |

Editing requires workspace edit access. Built-in system agents reject this edit path. `mcp` and `skills` are full replacement lists: read the current configuration first and include every attachment you intend to retain. Use attach/detach reactors for an incremental skill change.

```pixel
EditWorkspace(
    workspaceId=["<workspace-id>"],
    name=["Data Assistant"],
    description=["Explains datasets and produces reproducible analysis"],
    systemPrompt=["Ground conclusions in returned data and save reusable analysis when requested."],
    mcp=[],
    skills=["database", "python", "exports"],
    modelId=["<model-engine-id>"],
    maxTurns=[20],
    maxSeconds=[180],
    maxSubagentDepth=[0]
);
```

This example intentionally defines an empty MCP attachment list. Supply your complete resource list when adapting it. Inspect `GetWorkspace` afterward to confirm the effective configuration.

## Prompt and skill resolution

[AgentConfigLoader](../../src/prerna/reactor/agent/config/AgentConfigLoader.java) combines workspace configuration with room additions and runtime context. Skills are merged by project ID from workspace resource rows, `CONFIG_JSON.skills`, and room `options.skills`. MCP resources have their own corresponding merge path.

Room `instructions` append to the authored agent prompt when `overrideSystemPrompt=false`; true or omitted replaces that authored layer. Working-directory `AGENTS.md`/`CLAUDE.md`, harness instructions, skill summaries, and runtime context are handled separately. See [prompt composition](semoss_harness.md#prompt-composition).

Use [GetWorkspace](../../src/prerna/engine/impl/model/inferencetracking/reactors/workspaces/GetWorkspaceReactor.java) to inspect the catalog/configuration view and [ListSkills](skills/skills_doc.md#listskills) to inspect physical skill files. An attachment in the database and a successfully staged skill are different checks.

## Deployment and verification

Ship the backend and required `project/platform__*` assets together. [SystemAgentSeeder](../../src/prerna/util/SystemAgentSeeder.java) reconciles platform workspace configuration after project cataloging; model-inference storage must be available for that step.

For a custom agent, verify the selected model, workspace, and target; submit a bounded run; inspect `GetAgentRun`; and confirm tool results and written files support the reported result. Check an approval-required tool through pause and resumption, and check that inaccessible resources remain inaccessible. Live quality checks complement the [unit tests](../development_guides/java_developer_onboarding.md#verification).

See [workbench defaults](workbench-default-agents.md), [skills](skills/skills_doc.md), and [the native harness](semoss_harness.md).
