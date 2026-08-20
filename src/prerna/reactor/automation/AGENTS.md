# Automation Python — Agent Guide

Automation projects persist a typed graph and one Python source file per non-start node. The graph
is canonical: Java traverses its sequential control edges, and Python executes each node module
in the authenticated user's Python insight.

## Frontend Pixel contract

| Pixel | Request | Response / behavior |
| --- | --- | --- |
| `CreateAutomation` | `projectName` | Creates a project with a starter graph. |
| `GetAutomation` | `project` | Returns the definition as the top-level map, including `trigger.start.config.globals`, plus `nodeSources: { nodeId: source }`. |
| `SaveAutomation` | `project`, `json`, optional `nodeSources` | `json` and the `nodeSources` JSON map may be raw or Base64. Trigger source belongs in `trigger.start.config.pythonSource`; legacy `python` and trigger source entries are migrated. |
| `TriggerAutomation` | `project`, optional `inputs`, `triggerType` | Java seeds configured trigger globals, executes trigger Python, then follows the canonical control path; returned scope and `globals` include resolved values. |
| `GetActiveAutomationRun` | `project` | Returns the active run lock or an empty map. |
| `GetAutomationRun` | `project`, `runId` | Returns live run state and per-node outputs. |
| `ListAutomationRuns` | `project`, optional `limit` | Returns run history, newest first. |
| `CancelAutomationRun` | `project`, `runId` | Requests cancellation using the DB flag and same-pod fast path. |

## MCP authoring

Saving or creating an automation generates project-scoped MCP tools in
`assets/mcp/pixel_mcp.json`. The Automation Workspace chat uses these tools to add a typed
generated node, reconfigure a generated node, or update an explicitly custom node with an
optimistic source-hash check. MCP tools never receive the whole graph or bypass Java-owned
control-flow validation.

## Persisted files

Workflow artifacts live at the project asset root:

| File | Purpose |
| --- | --- |
| `automation-workflow.json` | Canonical typed graph (`formatVersion: 2`). |
| `automation-nodes/<label_slug>_<uuid-prefix>.py` | One persisted `run(scope)` source file per non-start node. |

`automation-workflow.py` is not used. `SaveAutomation` versions and synchronizes the graph and all current node-source files with the project. Legacy portal-based and Base64-named node files are read as a compatibility fallback and migrated on the next save.

## Runtime behavior

```
TriggerAutomation (virtual thread)
  -> validates and snapshots the graph
  -> claims AUTOMATION_ACTIVE_RUN
  -> inserts AUTOMATION_RUNS + AUTOMATION_NODE_OUTPUTS
  -> Java seeds trigger globals and executes trigger Python, then visits each control-edge node in order
  -> PyTranslator.runScriptWithExplicitAssetPaths(...) executes that node's source only
  -> Python module invokes its documented ai_server engine SDK or direct Pixel call
  -> releases the lock in finally
```

Java accepts one connected, acyclic, sequential control path rooted at `trigger.start`.
Supported native-Python runtime types are:

- `database.query`, `database.insert`, `database.update`
- `model.chat`, `model.embeddings`, `model.vision`, `model.ner`
- `storage.action`, `storage.list`, `storage.read`, `storage.upload`,
  `storage.download`, `storage.delete`
- `vector.action`, `vector.search`, `vector.add`, `vector.delete`
- `function.execute`, `app.pixel`, `control.wait`
- `agent.run`

Control branches/loops are rejected before execution. Trigger globals use the canonical
`trigger.start.config.globals` list: each entry is `{ name, defaultValue, description? }`, with a
non-private Python-identifier name. `trigger.start.config.pythonSource` is the canonical optional
setup source (`python` is a compatibility alias). Java puts defaults in the runtime scope unless
inputs override them; the globals are returned by Trigger and become Playground defaults. `developer.python`
and custom-code nodes execute their own persisted `run(scope)` source. Node source may return any
JSON-serializable value; Java persists it as the current node output. Generated sources import their documented
`ai_server` engine class and invoke it directly; wait nodes use `time.sleep`.

The bridge reloads the Java-bound node from the immutable run snapshot and retains the callback
insight's user/security context. It does not accept an arbitrary node definition, node id, engine
id, or Java object from Python. Cancellation sets the DB flag, signals the same-pod Python socket
job when possible, and is checked before each node and during waits.

## Shared infrastructure

| Class | Purpose |
| --- | --- |
| `AutomationDatabaseUtility` / `AutomationOwlCreator` | Run records, node outputs, active-run locking, and stale-run recovery. |
| `AutomationPythonRunRegistry` | Same-pod Python socket interruption, heartbeat, and cancellation state. |
| `AutomationRuntimeUtils` | JSON serialization, scope construction, and output previews. |

Do not bypass the active-run database lock, run snapshot, or DB/in-memory cancellation signal.
