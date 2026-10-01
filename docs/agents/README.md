# Agents, Harnesses, and Skills

SEMOSS agents combine persistent conversations, reusable workspace configuration, model engines, executable tools, and skills. The default `semoss` harness runs the model/tool loop in Java and uses the same engine and project services as the rest of the platform.

## Concepts

| Term | Meaning | Lifetime |
| --- | --- | --- |
| Harness | Implementation of `IAgentHarness` that executes an agent request | Registered runtime implementation |
| Agent / workspace | A `WORKSPACE` project plus instructions, attachments, model defaults, and limits | Reusable configuration |
| Room | Model conversation with messages, options, and user ownership | Persistent across turns |
| Run | One invocation submitted through `RunAgent` | Tracked by `runId` and status |
| Skill | A `SKILL` project with `SKILL.md` and supporting files | Reusable instructional package |
| Tool | Executable operation exposed through built-in handlers or configured MCP resources | Available according to run configuration and permissions |
| Execution target | Room/insight, user assets, or project files the run works on | Resolved for each run |

An agent is not a model engine, a skill is not a tool, and the agent's workspace ID is not necessarily the ID of the project it edits.

## Choose an execution path

| Need | Entry point |
| --- | --- |
| A persistent model turn whose tool results your client controls | [AskRoom](../pixel/ask_room.md), followed by `AddToolExecution` when needed |
| A server-managed agent loop with durable status and approval pauses | [RunAgent](agent_runs.md), normally with `harnessType=["semoss"]` |
| The Claude Code runtime through SEMOSS | [RunAgent with `claude_code`](../claude_code/claude_code.md) |
| A stateless model call | `LLM` with history disabled; see [model engines](../engines/model_engines.md) |

## Guide map

1. [Agent configuration](agent_configuration.md): define a workspace, select a model and target, attach resources, and set limits.
2. [The SEMOSS harness](semoss_harness.md): understand prompt composition, tools, compaction, hooks, and subagents.
3. [Run lifecycle and streaming](agent_runs.md): submit, monitor, pause, resume, and cancel runs.
4. [Agent skills](skills/skills_doc.md): author, catalog, attach, stage, discover, and load skills.
5. [Workbench default agents](workbench-default-agents.md): how frontend defaults and system agents are deployed and selected.
6. [PowerPoint visual inspection](pptx-visual-inspection.md): a specialized agent workflow.

## Requirements

Persistent rooms, workspaces, and agent run records use the model-inference database. It must be enabled and successfully initialized. Users also need access to the selected model, agent workspace, target project, and any resources invoked by tools.

The native harness uses a configured text-generation model. Tool use depends on the selected model's capabilities and provider integration. Python, Node, external harnesses, and specialized tools additionally depend on their deployment's runtimes and feature settings.

Platform agents and skills are shipped as project assets and cataloged at startup. `ProjectWatcher`, `SystemDefaultEngines`, and `SystemAgentSeeder` connect the distribution's project folders to the catalogs and workspace configuration. Deploy backend code and matching project assets together; see [system agent deployment](workbench-default-agents.md#registration-and-deployment).

## Source map

| Area | Source |
| --- | --- |
| Submission and registry | [RunAgentReactor](../../src/prerna/reactor/agent/RunAgentReactor.java), [AgentHarnessRegistry](../../src/prerna/reactor/agent/AgentHarnessRegistry.java) |
| Context resolution | [AgentRunner](../../src/prerna/reactor/agent/AgentRunner.java), [AgentConfigLoader](../../src/prerna/reactor/agent/config/AgentConfigLoader.java) |
| Native loop and tools | [SemossAgentHarness](../../src/prerna/reactor/agent/runtime/SemossAgentHarness.java), [HarnessToolExecutor](../../src/prerna/reactor/agent/runtime/HarnessToolExecutor.java) |
| Runs and events | [Run services](../../src/prerna/reactor/agent/run/), [stream services](../../src/prerna/reactor/agent/stream/) |
| Skill lifecycle | [Skill reactors and helpers](../../src/prerna/reactor/agent/skill/) |
| Built-in agent definitions | [SystemAgentSeeder](../../src/prerna/util/SystemAgentSeeder.java), [SystemDefaultEngines](../../src/prerna/util/SystemDefaultEngines.java) |
