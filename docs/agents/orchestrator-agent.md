# Playground Orchestrator

The built-in **Orchestrator Agent** has workspace ID `orchestrator-agent`. New Playground rooms select it when the caller does not explicitly choose another workspace. Existing rooms and explicit workspace selections are unchanged.

The Orchestrator answers ordinary requests itself and receives one named tool for every workspace in its `CONFIG_JSON.subagents[]` allowlist. Its initial roster contains the built-in `pptx-agent`. The managed prompt tells it to delegate file-producing work with `inherit_parent_workdir=true` and `completionMode=POST`, allowing the parent chat to remain available while the durable child run completes without launching a follow-up Orchestrator run in the parent room.

## Editing the roster

Users with edit permission on the Orchestrator project may replace the roster through `EditWorkspace`:

```pixel
EditWorkspace(
    workspaceId=["orchestrator-agent"],
    name=["Orchestrator Agent"],
    subagents=[
        {"workspaceId":"pptx-agent"}
    ]
);
```

Only `subagents` may be changed on this built-in workspace. Targets must be active, distinct from the Orchestrator, unique within the roster, and viewable by the editor. Normal execution-time authorization still applies to the user who starts a run.

Startup reconciliation restores the managed prompt and spawn policy but preserves an existing roster. If the workspace has never been seeded, PowerPoint is installed as its initial specialist.

## Child-run UI contract

Live agent events use `kind="subagent"` and include `childRunId`, `roomId`, `workspaceId`, `displayName`, and status. Terminal events may also contain `resultPreview` or `error`. `GetSubagentRuns(runId=["<parent-run-id>"])` provides the durable refresh path and returns `executorType="AGENT"` plus the specialist name in `executorLabel`.

The Orchestrator requires a packaged global WORKSPACE project named `platform__orchestrator-agent` so it can be cataloged and authorized like the other built-in system agents.
