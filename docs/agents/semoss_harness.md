# The SEMOSS Agent Harness

The native harness, registered as `semoss`, executes an agent request using SEMOSS rooms, model engines, tools, and resource permissions. It is the default when `RunAgent` omits `harnessType` or passes a blank value.

[SemossAgentHarness](../../src/prerna/reactor/agent/runtime/SemossAgentHarness.java) implements [IAgentHarness](../../src/prerna/reactor/agent/IAgentHarness.java). [AgentRunner](../../src/prerna/reactor/agent/AgentRunner.java) resolves the execution context first; [AgentRunService](../../src/prerna/reactor/agent/run/AgentRunService.java) and its worker own scheduling and durable status.

## What happens during a run

1. **Resolve context.** Load the room, select and authorize the model, resolve the agent workspace and file target, load configuration, and stage attached skills.
2. **Prepare the harness.** Apply default-tool policy, collect explicit and room/workspace tools, expose eligible subagent tools, and compose the system prompt.
3. **Call the model.** Send the user turn through `Room.ask(...)` with model context and tool schemas. Supported current-turn media is included in this initial request.
4. **Execute tools.** Dispatch requested tools, record results, and continue the model. Tool batches can run concurrently; consumers should not assume model-listed tool order guarantees serialized effects.
5. **Pause when needed.** Persist approval-required actions and yield `INPUT_REQUIRED` rather than keeping a tool waiting indefinitely on an HTTP request.
6. **Finish or continue.** A response without more tool calls ends the loop unless configured reflection requests another pass. Cancellation, errors, and limits also end execution.
7. **Persist and clean up.** Tag room messages with the run, report the outcome, run lifecycle cleanup, restore temporary room options, and synchronize configured target assets.

Approval resumption continues from the recorded tool results without adding the original user command a second time. See [run lifecycle](agent_runs.md).

## Prompt composition

The native harness builds a prompt from these sources:

- Platform behavior from [SemossHarnessPrompts](../../src/prerna/reactor/agent/runtime/SemossHarnessPrompts.java).
- Delegation instructions for the subagent tools actually exposed to the run.
- Available skill names, descriptions, and paths discovered in the working directory.
- Resolved agent context: working-directory instructions, workspace/room authored instructions, and selected-engine context when applicable.
- Runtime context, including identifiers and the execution target.
- Specialized workflow instructions when configured, such as the presentation workflow.

The working-directory loader checks `AGENTS.md`, then `CLAUDE.md`, **in that exact directory**. It does not search parent directories. The first readable supported file is used, with a 100 KiB limit. See [AgentsMdLoader](../../src/prerna/reactor/agent/runtime/AgentsMdLoader.java).

Workspace `system_prompt` supplies the authored agent instructions. Nonblank room `instructions` append when `overrideSystemPrompt=false`; true or omitted retains replacement behavior for that authored layer. Neither mode removes the native harness instructions or the skill catalog. The current workbench sends the flag explicitly; see [workbench selection](workbench-default-agents.md#selection-and-saved-conversations).

The available-skills block contains summaries, not every skill's full contents. `LoadSkill` retrieves the relevant instructions and references when needed.

## Tools and execution policy

[PlatformAgentTools](../../src/prerna/reactor/agent/runtime/PlatformAgentTools.java) resolves the default tool provider and applies workspace policy. [PlatformAgentToolHandlers](../../src/prerna/reactor/agent/runtime/PlatformAgentToolHandlers.java) defines native tool schemas and handlers.

| Tool group | Examples | Notes |
| --- | --- | --- |
| File operations | `ReadFile`, `WriteFile`, `EditFile`, `MultiEdit`, `MoveFile`, `DeleteFile` | Paths resolve within the working target; tool policy can protect configured paths |
| File discovery | `ListDirectory`, `GlobFiles`, `GrepFiles` | Operate on the working target |
| Task state | `TodoRead`, `TodoWrite` | Track the agent's task list |
| Skill discovery and loading | `ListSkill`, `LoadSkill` | Load instructions and supporting files on demand |
| Managed code | `ExecutePythonCode`, `ExecuteNodeCode` | Availability depends on runtime settings; state lasts while the worker remains alive |
| Shell operations | `BashCommand` | Optional restricted command handler; consult its current schema and allowlist |
| Resource integrations | Workspace/room MCP tools | Run against configured engines and projects with the caller's access |
| Delegation | Spawn, check, wait, and named subagent tools | Subject to workspace policy and run-tree limits |

`useDefaultAgentTools` and `disabledDefaultTools` control the agent's general built-in tools. Default-tool availability can also be supplied by a deployment-configured MCP project. Explicit resource attachments and their permission checks still matter; a skill does not grant an engine, filesystem, or tool permission.

Tool execution mode such as `SMSS_MCP_EXECUTION=ask` requires an approval decision. The executor persists that decision point as a run action. [Tool hooks](../../src/prerna/reactor/agent/IToolHook.java) participate before and after dispatch; run hooks surround the overall harness lifecycle.

## Budgets and limits

[AgentConfig](../../src/prerna/reactor/agent/config/AgentConfig.java) defines these native defaults:

| Limit | Default | Meaning |
| --- | --- | --- |
| `maxTurns` | `30` | Maximum tool-call rounds, not individual tool calls |
| `maxReflections` | `0` | Optional self-review rounds after an answer |
| `max_seconds` in `paramValues` | `0` | No wall-clock cap when zero |
| `max_subagent_depth` | `1` | Root can spawn children; those children cannot spawn another level |
| `max_subagents_per_run` | `10` | Spawn count cap across the root's delegation tree |
| `max_spawns_per_turn` | `5` | Spawn cap for one tool batch |

Workspace `CONFIG_JSON.budgets` caps caller-requested turns, reflections, and time. A caller can request a smaller limit, not enlarge a configured cap. Workspace `spawn_policy` controls delegation depth and counts; a depth of zero disables spawning.

Wall-clock budget checks occur at harness execution boundaries. They are not a universal deadline that can instantly terminate every blocking provider or tool operation. Waiting timeouts are separate from execution limits and do not imply cancellation.

## Context compaction

The native harness monitors active-branch token usage against the model's configured context window. The current trigger is 80%. At eligible boundaries it invokes the existing room compaction operation, then continues from the resulting context.

If the context window is missing or nonpositive, automatic compaction is disabled for that run and logged. An input leaf or unresolved tool calls can make compaction ineligible; errors or unavailable strategies are surfaced by the harness. Compaction changes the context sent to the model, not the identity of the durable run.

## Subagents

The harness synthesizes tools from `CONFIG_JSON.subagents[]` and exposes general spawn/check/wait capabilities while the run remains below its depth limit. Children use their own rooms and durable run IDs, with a parent-run association. The shared run-tree policy bounds total and per-turn spawning.

Completion modes are defined by [SubAgentRunCompletionMode](../../src/prerna/reactor/agent/run/SubAgentRunCompletionMode.java):

| Mode | Parent behavior |
| --- | --- |
| `WAIT` | Parent collects the result through the wait tool; default |
| `POST` | Child completion is posted to the parent room |
| `POST_AND_CONTINUE` | Completion is posted and a new parent-room continuation run is scheduled |

A wait timeout leaves the child running. Delegation also needs a clear task boundary: child work can share target files, so independent rooms are not a guarantee of independent filesystem changes. See [SubAgentDispatcher](../../src/prerna/reactor/agent/subagent/SubAgentDispatcher.java) and [AgentSubAgentRegistry](../../src/prerna/reactor/agent/subagent/AgentSubAgentRegistry.java).

## Lifecycle hooks and alternate harnesses

[IAgentRunHook](../../src/prerna/reactor/agent/IAgentRunHook.java) provides `onRoomCreation`, `beforeRun`, `afterAgentInit`, `afterRun`, and `beforeAgentDeInit`. `beforeRun` can veto execution; failures in the observation/cleanup hooks are logged. Configuration resolves registered hook implementations through [AgentHookRegistry](../../src/prerna/reactor/agent/hooks/AgentHookRegistry.java).

The registry also includes `claude_code` and `github_copilot_py`. Explicit unknown harness names are rejected before run submission. These adapters own different runtime loops: native reflection, tool budgets, media, and event-stream behavior are not automatically portable to them. Canonical event sessions currently cover `semoss` and `claude_code`.

Continue with [agent configuration](agent_configuration.md), [skills](skills/skills_doc.md), and [durable runs](agent_runs.md).
