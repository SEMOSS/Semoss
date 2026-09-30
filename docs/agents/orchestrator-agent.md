# Playground Orchestrator

The built-in **Orchestrator Agent** has workspace ID `orchestrator-agent`. New Playground rooms select it when the caller does not explicitly choose another workspace. Existing rooms and explicit workspace selections are unchanged.

The Orchestrator answers ordinary requests itself. Each room has an `options.agents[]` participant roster, and the harness gives the room's default agent one named `transfer_to_*` tool for every eligible participant. New Orchestrator rooms initially copy `pptx-agent` and `app-builder` from the Orchestrator workspace's configured default roster into `options.agents[]`.

A transfer is not a subagent run. Calling a transfer tool starts a linked, top-level successor run in the same room and working directory. The receiving agent responds directly in the conversation. After that run finishes, the next user request is handled by the room's default Orchestrator again.

## Editing room participants

Users may add or remove accessible Agent workspaces through the room options UI. The client persists the roster through `UpdateRoomOptions`:

```pixel
UpdateRoomOptions(
    roomId=["<room-id>"],
    options=[{"agents":[{"workspaceId":"pptx-agent"}]}]
);
```

Targets must be active, distinct from the room's default agent, unique within the roster, and viewable by the editor. Normal execution-time authorization still applies to the user who starts a run.

The Orchestrator workspace also keeps an editable default roster used only to initialize new rooms. That workspace-level setting is currently persisted through the existing `EditWorkspace.subagents` compatibility field; it does not make transfers subagent runs. Changing it does not rewrite existing room rosters.

## Transfer UI contract

The parent Orchestrator run completes after requesting the transfer. The successor run is linked by `transferFromRunId` and `transferRootRunId`, remains a top-level run in the same room, and streams through the normal agent-run APIs. The Playground watches the linked run immediately and reconciles missing linked runs from the room's durable run history after refresh or reconnect.

The Orchestrator requires a packaged global WORKSPACE project named `platform__orchestrator-agent` so it can be cataloged and authorized like the other built-in system agents.
