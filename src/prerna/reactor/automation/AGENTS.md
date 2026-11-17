# Automation — Agent Guide

Automation is a Java-first execution product built on existing SEMOSS reactors,
engines, Insights, and project security. The persisted typed graph is canonical.
Java validates and snapshots it, owns traversal and run scope, invokes the
supported platform operation for each generated node, and records durable run
history. Python is an explicit custom-computation boundary, not the workflow
orchestrator.

## Frontend Pixel contract

| Pixel | Request | Response / behavior |
| --- | --- | --- |
| `CreateAutomation` | `projectName` | Creates a project with a starter graph. |
| `GetAutomation` | `project` | Returns the graph, developer-Python sources, source hashes, globals, and server-derived scope variables. |
| `SaveAutomation` | `project`, `json`, optional `nodeSources` | Saves a validated graph. `nodeSources` contains only `developer.python` source; generated nodes reject source. |
| `TriggerAutomation` | `project`, optional `inputs`, `triggerType` | Snapshots and starts the project automation as the authenticated caller. |
| `RunAutomationDefinition` | `json`, optional `nodeSources`, `inputs`, `triggerType` | Runs a validated inline definition under the caller's owner-scoped inline run ID. |
| `GetAutomationRun` | optional `project`, `runId` | Returns authorized durable run state, node history, and bounded output details. |
| `ListAutomationRuns` | optional `project`, optional `limit` | Returns authorized run history, newest first. |
| `CancelAutomationRun` | optional `project`, `runId` | Persists cancellation and signals the same-JVM fast path. |
| `RerunAutomationNodes` | `project`, `runId`, `nodeIds` or `wholeRun`, optional `loopNodeId` + `iteration` | Reprocesses a finished run as a new run, leaving the finished run untouched: the whole automation again (`wholeRun`), or the chosen top-level nodes chained in run order with every other node carried over; with `loopNodeId` and a zero-based `iteration`, the chosen body nodes of that one loop iteration. See "Reprocessing a run". |
| `RemoveAutomationStep` | `project`, `nodeId`, optional `removeDownstream` | Removes the requested step and validates the resulting draft. |

Inline runs use `SYSTEM__AUTOMATION_INLINE__<sha256(loginType:loginId)>` as
their persisted run scope. A project lookup must never accept this internal ID.

## Persisted definition

Workflow artifacts live at the project asset root:

| File | Purpose |
| --- | --- |
| `automation-workflow.json` | Canonical format-version 2 typed graph. |
| `automation-nodes/<label_slug>_<uuid-prefix>.py` | Source for each `developer.python` node. |

Generated nodes persist configuration, not generated Python. Trigger setup
Python remains in `trigger.start.config.pythonSource`. Saving synchronizes graph
and developer sources through the project service and uses the existing backup
and rollback behavior.

## Java-first runtime

```text
TriggerAutomation / RunAutomationDefinition
  -> validate definition and referenced SEMOSS resources
  -> persist immutable graph, inputs, and developer-source snapshot
  -> atomically claim the submitted run
  -> create one authenticated, run-owned execution Insight
  -> seed Java-owned AutomationExecutionContext
  -> walk the selected control path in Java
       generated node -> typed Java handler -> guarded Pixel/reactor/engine path
       control node   -> Java condition/Jev/loop/wait implementation
       developer node -> AutomationPythonExecutor -> run(read-only scope)
  -> add each successful business output to Java scope by outputVar
  -> persist bounded node output, trace metadata, and terminal state
```

`AutomationRunExecutionService` is the orchestration owner. Generated nodes are
dispatched by node type to explicit handlers; they are not rendered into a
Python workflow. `AutomationPixelExecutor` is the authenticated boundary for
existing SEMOSS Pixel/reactor operations. Keep permissions, routing, audit,
guardrails, and commit behavior in those existing owners.

`AutomationExecutionContext` owns live Java values for the run. Configuration
references such as `${query_result[0].ID}` are resolved by
`AutomationScopeResolver` while preserving the underlying Java value. The UI
and durable history receive bounded presentation contracts, not raw tasks,
engine objects, provider handles, Python variable names, or Insight internals.
`data.transform` is a pure scope transform resolved directly by
`AutomationDataTransformExecutor` - no Pixel call and no generated Python. It
recursively resolves a configured field tree (nested objects/arrays, literals,
and `${name.path}` references) into a brand-new JSON object built only from
that tree; it never merges in any prior scope value. `data.declare-variable`
builds its field tree the same way; besides its own `outputVar` entry, Java
also spreads the declared names into scope as individually named variables -
exactly like trigger globals - so later nodes can reference `${name}` directly
instead of a dotted path into one output object.

## Node execution

- Database reads use the guarded `SqlQuery` task path, import the task once into
  an Insight-owned SEMOSS frame, and carry an internal Java frame reference in
  scope. Java can page it for loops and indexed references. A developer-Python
  node receives the corresponding pandas binding under the same `outputVar`.
- Database writes, functions, apps, storage, vectors, models, and agents use
  their existing Pixel/reactor or engine path from a typed Java handler.
- `developer.python` is the only arbitrary per-node Python source. Its module
  must define `run(scope)`. Scope is read-only; the returned value becomes the
  node output.
- Trigger setup Python is optional initialization. It is not a second graph
  executor.
- Conversation identifiers and agent trace state stay in trace/history;
  business output stays in scope.

Do not replace these handlers with a generic Python dispatcher. Do not duplicate
base-engine authorization or SQL policy in Automation.

## Tables, history, and restart

An internal `RegisteredFrame` is an implementation detail for scalable live
table access. It is not the public node contract. Database results and supported
Python frame results are captured into bounded, chunked table history with
schema, row counts, byte counts, and checksums. Live consumers use the run
Insight frame; reopened history uses the durable capture.

Never infer a table/frame from user-controlled JSON fields. Persist the explicit
output kind. Ordinary dictionaries that contain `dataType`, `rowCount`, or
`columnCount` remain ordinary JSON.

Table limits are product guardrails. Do not solve a large-data problem by
serializing the full table into every API response or every Python invocation.
Do not change shared frame/import behavior for an Automation-only defect without
reproducing the same behavior through the standard SEMOSS path.

## Control flow

- `control.if` evaluates ordered bounded expressions in Java. First match wins;
  `else` is the fallback.
- `control.jev` calls the TypeSafe model, retains the typed response, and routes
  through stable `case:<route-id>` or `else` ports.
- `control.loop` owns a nested acyclic body. Java executes bounded sequential
  `forEach`, `repeat`, or `while` passes and creates child history rows.
- A for-each loop accepts a list/array or an internal table value. Table pages
  must set `distinct=false` so duplicate physical rows remain duplicate items.
- Loop bodies use the same generated and developer handlers as the parent graph,
  including database query nodes. Nested loops, parallel passes, arbitrary graph
  cycles, and agent waits inside loops remain unsupported unless current
  validation explicitly says otherwise.

## Scope and output contract

Each selected node reads a snapshot of the current run scope and publishes one
value under its unique `outputVar`. Scope contains trigger inputs, runtime
metadata, and successful upstream outputs.

- Generated configuration uses exact `${name.path}` references in supported
  fields. Resolution preserves maps, lists, table rows, scalars, and nulls.
- Developer Python uses `scope["name"]` or `scope.get("name")`; `${...}` is not
  rewritten inside Python source.
- Java scope is authoritative. Python receives only the values required for
  the custom computation. Retained Python bindings may avoid repeatedly
  serializing a prior large JSON value, but they do not own graph state.
- All JSON inputs, outputs, aggregate scope, loop results, previews, and table
  captures remain bounded by `AutomationConstants`.

## Run lifecycle

- `AutomationRunStore` owns scheduler-database run records, immutable snapshots,
  node history, waits, table captures, claims, and stale-run recovery.
- `AutomationRunRegistry` is only a same-JVM heartbeat/cancellation fast path.
  The database is authoritative.
- The execution Insight supplies the authenticated SEMOSS runtime and live
  resources for one run. It is not a durable-history database.
- Resume reconstructs Java scope only from authorized persisted inputs and
  successful outputs. It must never rerun a successful node to recreate data.

## Reprocessing a run

`RerunAutomationNodes` reprocesses a finished run as a **new run**. The finished
run is never modified, so its history stays exactly as it ran. It is the one
explicit exception to "never rerun a successful node": the caller either asks for
the whole automation again or names the nodes, so nothing runs that they did not
ask for.

- Only a finished run (SUCCESS, FAILED, CANCELLED, INTERRUPTED) can be
  reprocessed, and only by an editor of the project.
- The new run records the run it came from in `REPROCESSED_FROM_RUN_ID`, and its
  own `STARTED_AT`/`COMPLETED_AT` describe the reprocess.
- Both paths use the automation as it is saved now, never the original run's
  snapshot.

### Whole run (`wholeRun=["true"]`)

`reprocessWholeRun` starts the automation from its trigger through the normal
`start` path, with the automation's current defaults. Only inputs the finished run
was given beyond its own defaults (`inputOverrides`) are carried over, so editing
a trigger variable's default takes effect.

### Chosen nodes

`reprocessNodes` copies the finished run (`AutomationRunStore.cloneRunForReprocess`:
run row, node rows, node sources, and saved results with their chunks under new
result IDs; every copied node row has `CARRIED_OVER` set) and then runs the chosen
nodes on the copy with `rerunNodes`, which resets only those rows (clearing
`CARRIED_OVER`). Nodes that were not chosen keep their recorded result, shown as
carried over, and the chosen nodes read it as upstream scope
(`reconstructScope` with the chosen nodes excluded).

- The selection is validated (`planNodeRerun`) before anything is copied, and a
  copy refused before it started (for example a table that left memory) is
  deleted again, so history never shows a phantom run.
- Tables the finished run still holds in its execution Insight are made
  available to the new run's Insight (`carryOverFrames`); a table that has left
  memory refuses the reprocess untouched.
- The chosen nodes' graph and sources are patched with their current settings
  and recorded on the new run. A chosen node must still exist in the current
  automation; the patched graph is validated as a whole.
- Trigger, wait, and agent nodes cannot be reprocessed on their own (a whole
  run covers them). A chosen loop re-runs entirely.
- A failing chosen node stops the chain, later chosen nodes are skipped, and the
  run settles through the normal `finishRun`: FAILED if any node, chosen or
  carried over, is failed. Unchosen nodes downstream of a reprocessed node are
  not recomputed and may be stale; choose them too.

### One loop iteration

Passing `loopNodeId` and a zero-based `iteration` reprocesses the chosen body
nodes (`nodeIds` are the loop body's source node ids) of that single iteration,
as a new run created the same way (`rerunLoopIterationNodes` on the copy).

- Scope is rebuilt with the whole loop excluded, then the iteration's loop
  variables are recomputed (`planLoop`). A while loop's carried outputs come from
  the previous iteration's recorded rows.
- The body runs once in order: chosen nodes run and publish their output;
  unchosen successful nodes publish their recorded output so chosen nodes read
  the same inputs they did originally.
- Only the chosen iteration rows are reset (`reopenRunForLoopNodeRerun`); other
  iterations, the loop node row, and its siblings stay carried over.
- The loop's own collected `results` are not recomputed. Wait and agent nodes are
  refused.
- The loop must be a loop of the run, the iteration must exist, and the chosen
  nodes must still exist in the current automation's loop body.

## Package layout

| Package | Ownership |
| --- | --- |
| `prerna.reactor.automation` | Shared constants and the narrow Python invocation bridge. |
| `prerna.reactor.automation.definition` | Graph/catalog validation, definition persistence, and authoring reactors. |
| `prerna.reactor.automation.project` | Project permissions, persistence coordination, and derived MCP assets. |
| `prerna.reactor.automation.run` | Java execution, typed handlers, run scope, durable history, cancellation, waits, and run reactors. |
| `prerna.reactor.automation.agent` | Trace-linked child-agent access and actions. |
| `prerna.reactor.automation.utils` | Cross-boundary serialization and preview utilities only. |

Keep additions with the capability that owns their lifecycle. Do not add a
generic service/helpers layer, another workflow runtime, or a persistence
interface without a real second implementation.

## Verification

Test the contract at its owner:

- graph/catalog validation and authoring under `definition` tests;
- handler rendering, scope resolution, loops, waits, table capture, status, and
  cleanup under `run` tests;
- Python bridge behavior in focused Python tests;
- history/presentation in package UI tests and an authenticated browser run.

Do not claim a build, restart, browser test, or end-to-end run passed unless it
actually ran against the current checkout.
