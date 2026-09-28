# Automation Python — Agent Guide

Automation projects persist a typed graph and one Python source file per Python-backed node. The graph
is canonical: Java traverses its control edges, and Python executes each selected node module
in the authenticated user's Python insight.

## Frontend Pixel contract

| Pixel | Request | Response / behavior |
| --- | --- | --- |
| `CreateAutomation` | `projectName` | Creates a project with a starter graph. |
| `GetAutomation` | `project` | Returns the definition as the top-level map, including `trigger.start.config.globals`, `nodeSources: { nodeId: source }`, and server-derived `scopeVariables` by node ID. |
| `SaveAutomation` | `project`, `json`, optional `nodeSources` | `json` and the `nodeSources` JSON map may be raw or Base64. `nodeSources` holds one entry per Python-backed node; trigger setup source belongs in `trigger.start.config.pythonSource`. |
| `TriggerAutomation` | `project`, optional `inputs`, `triggerType` | Java seeds configured trigger globals, executes trigger Python, then follows the canonical control path; returned scope and `globals` include resolved values. |
| `GetAutomationRun` | `project`, `runId` | Returns live run state and per-node outputs. |
| `ListAutomationRuns` | `project`, optional `limit` | Returns run history, newest first. |
| `CancelAutomationRun` | `project`, `runId` | Requests cancellation using the DB flag and same-pod fast path. |
| `RemoveAutomationStep` | `project`, `nodeId`, optional `removeDownstream` | Removes one step and its attached edges; when `removeDownstream=true`, removes the selected step and every downstream step. Detached drafts can be saved but cannot run. |

## MCP authoring

Saving or creating an automation generates project-scoped MCP tools in
`assets/mcp/pixel_mcp.json`. The Automation Workspace chat uses these tools to add a typed
generated node, reconfigure a generated node, or update an explicitly custom node with an
optimistic source-hash check. MCP tools never receive the whole graph or bypass Java-owned
control-flow validation.

`scopeVariables` is keyed by node ID. Each descriptor includes `name`, `source`, `availability`,
the recommended `pythonExpression`, explicit `requiredPythonExpression` and
`optionalPythonExpression` forms, and `templateExpression`. Custom Python may choose required
`scope["name"]` or optional `scope.get("name")` access; generated configuration forms should insert
`templateExpression`. A conditional branch output is reported with `availability: "conditional"`
and recommends the safe optional expression.

The workbench runs the immutable `workflow-automation-builder` system agent and supplies the active
automation project's MCP through room options. That agent owns the authoring prompt and the
`workflow-automation` system skill; the skill owns reusable graph and node-selection guidance. The
project MCP remains project-scoped so its fixed project ID, authenticated Insight, approval
policy, and refresh events stay bound to the automation being edited.

## Persisted files

Workflow artifacts live at the project asset root:

| File | Purpose |
| --- | --- |
| `automation-workflow.json` | Canonical typed graph (`formatVersion: 2`). |
| `automation-nodes/<label_slug>_<uuid-prefix>.py` | One persisted `run(scope)` source file per Python-backed node. |

`SaveAutomation` versions and synchronizes the graph and all current node-source files with the
project. Both are read from the project asset root only, and a save that is interrupted mid-publish
is rolled back from the backup written under `.automation-save`.

## Runtime behavior

```
TriggerAutomation (virtual thread)
  -> validates and snapshots the graph
  -> inserts a SUBMITTED run, bounded effective inputs, immutable node sources, and pending node outputs
  -> atomically claims that run as RUNNING
  -> Java seeds trigger globals and executes trigger Python, then visits the selected control path
  -> PyTranslator.runScriptWithExplicitAssetPaths(...) executes that node's source only
  -> Python module invokes its documented ai_server engine SDK or direct Pixel call
  -> persists the terminal status for that run
```

Authoring may persist an acyclic draft with detached or incomplete control paths. Execution accepts
only a connected graph rooted at `trigger.start`; each run follows one deterministic path through
its routing nodes.
Supported native-Python runtime types are:

- `database.query`, `database.insert`, `database.update`, `database.delete`
- `model.chat`, `model.embeddings`, `model.vision`, `model.ner`
- `storage.list`, `storage.read`, `storage.upload`,
  `storage.download`, `storage.delete`
- `vector.search`, `vector.add`, `vector.delete`
- `function.execute`, `app.pixel`, `control.wait`, `control.if`, `control.jev`
- `agent.run`

`control.if` stores ordered `{ id, condition }` clauses evaluated only by the bounded Java
expression evaluator. The first match selects its `case:<clause-id>` edge; otherwise the final
`else` edge is selected. `control.jev` delegates a typed routing question to a TYPESAFE model.
`questionType: "choice"` selects among arbitrary described routes; `questionType: "noul"` maps the
model's Yes probability to exactly one `{ answer: true }` route or one `{ answer: false }` route.
Both modes retain stable route IDs for `case:<route-id>` edges and select `else` when confidence is
below the configured threshold. Arbitrary fan-out from one port, loops, and parallel execution are
rejected before execution; nonselected branch nodes are retained in history as `SKIPPED`. Trigger globals use the canonical
`trigger.start.config.globals` list: each entry is `{ name, defaultValue, description? }`, with a
non-private Python-identifier name. `trigger.start.config.pythonSource` holds the optional
setup source. Java puts defaults in the runtime scope unless
inputs override them; the globals are returned by Trigger and become Playground defaults. `developer.python`
and custom-code nodes execute their own persisted `run(scope)` source. Node source may return any
JSON-serializable value; Java persists it as the current node output. Generated sources import their documented
`ai_server` engine class and invoke it directly; wait nodes use `time.sleep`.
Each run reloads its persisted effective trigger-input snapshot before execution. Each node receives a read-only,
run-local `scope` mapping containing trigger inputs, globals, runtime metadata, and prior
outputs keyed by `outputVar`. Custom Python reads it directly; `${...}` references are reserved for supported
generated-node configuration fields and are not rewritten inside custom source. Generated nodes use the documented
engine SDK unless an existing Pixel reactor owns required server policy. Generated database reads use `SqlQuery`,
which retains SQL routing, authorization, configured engine-pipeline guardrails, and bounded row collection. Generated
database writes use the database SDK's `ExecQuery` path, which retains edit authorization, audit logging, commit
behavior, and configured `insertData` guardrails. Generated updates always require a `WHERE` clause; use custom Python
for an intentionally unbounded operation. Return a value so Java can store it under the node's `outputVar`.

The bridge reloads the Java-bound node from the immutable run snapshot and retains the callback
insight's user/security context. It does not accept an arbitrary node definition, node id, engine
id, or Java object from Python. Cancellation sets the DB flag, signals the same-pod Python socket
job when possible, and is checked before each node and during waits.

## Shared infrastructure

| Class | Purpose |
| --- | --- |
| `AutomationDatabaseUtility` | Physical run records, node outputs, per-run claiming, and stale-run recovery in the scheduler DB. |
| `SchedulerOwlCreator` | Authoritative OWL schema for both scheduler-owned and automation-owned tables in that DB. |
| `AutomationPythonRunRegistry` | Same-pod Python socket interruption, heartbeat, and cancellation state. |
| `AutomationRuntimeUtils` | JSON serialization, scope construction, and output previews. |

Do not bypass the per-run database claim, run snapshot, or DB/in-memory cancellation signal.
