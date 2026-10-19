# Claude Code Harness Integration

SEMOSS registers `claude_code` as an alternate implementation of `IAgentHarness`. It uses the same `RunAgent` submission and durable run services as other harnesses, while delegating its model/tool loop to the Claude Code integration.

For the native Java loop, see [the SEMOSS harness](../agents/semoss_harness.md). The two runtimes do not have identical tool, budget, media, or approval behavior.

## Submit through RunAgent

Use accessible room, model, workspace, and target project identifiers:

```pixel
RunAgent(
    roomId=["<room-id>"],
    engine=["<model-engine-id>"],
    workspaceId=["<workspace-id>"],
    harnessType=["claude_code"],
    space=["<target-project-id>"],
    command=["Inspect the application and explain its structure."],
    wait=[false]
);
```

The public entry point is `RunAgent` with a harness selection. Older examples using a standalone `ClaudeCode(...)` reactor do not describe this path.

## Components

| Component | Responsibility |
| --- | --- |
| [ClaudeCodeAgentHarness](../../src/prerna/reactor/agent/ClaudeCodeAgentHarness.java) | Adapts `AgentRunContext` to the external runtime |
| [ClaudeCodeManager](../../src/prerna/engine/impl/model/ClaudeCodeManager.java) | Configures the managed Python process, model proxy credentials, working target, and CLI integration |
| [ClaudeCodeClient](../../py/genai_client/agents/claude_code/claude_code_client.py) | Wraps the Claude Agent SDK and configures session/resume behavior |
| [AgentRunStreamService](../../src/prerna/reactor/agent/stream/AgentRunStreamService.java) | Canonical run event buffering and polling |

The manager resolves the CLI path from deployment configuration, an SDK-bundled binary where available, and its supported fallback lookup. The Python environment and CLI must actually be present in the deployed runtime; a source checkout alone does not install them.

## Model, files, and skills

The integration routes model requests through the configured SEMOSS model endpoint and uses the caller's permitted model context. The working directory comes from the agent runner's authorized target; there is no automatic `client/` suffix. Select a relative subdirectory explicitly when needed.

Workspace MCP resources are passed to the adapter. Attached skills are staged under `.claude/skills/` before execution, using the shared [skill lifecycle](../agents/skills/skills_doc.md). The external runtime then applies its own instruction/tool loading behavior.

## Sessions and limits

The Python wrapper supports session resume keyed to room history; it is not limited to independent one-off conversations. Continuing a room requires the corresponding runtime/session assets and configuration.

The adapter owns a separate loop. Native `maxTurns`, reflection counts, and native tool-approval behavior should not be interpreted as external-runtime guarantees. The current Python wrapper also sets permission behavior and turn options internally; inspect its implementation before relying on a requested override. The harness does not advertise the native media-input capability.

Canonical run events are supported for this adapter, while the returned native-loop iteration/tool trace fields do not represent its complete external transcript. Use [durable run status and streaming](../agents/agent_runs.md) and the associated conversation/transcript representation when diagnosing a run.

## Deployment checks

Confirm the managed Python environment, SDK/CLI availability, selected model endpoint, target project permissions, staged skills, and configured sandbox support. Keep adapter code, Python runtime assets, and Monolith model proxy routes on compatible versions.

See [agent configuration](../agents/agent_configuration.md), [local Docker setup](../cloud_and_cluster/docker_deployment.md), and [Monolith integration](../integrations/monolith_interaction.md). For Kubernetes and semoss-artifacts property configuration, use [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment).
