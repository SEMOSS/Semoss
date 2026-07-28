# Automation Engine

Executes sequential node pipelines against SEMOSS engines. Users build a pipeline in the form editor (FE), save it as `automation.json`, then trigger it manually. Each run is tracked in the DB and the FE polls for progress.

## How it works

```
FE calls runPixelAsync("TriggerAutomation(...)")
  → Monolith spawns a virtual thread, returns jobId immediately
  → FE polls GetActiveAutomationRun every 500ms (up to 10×) to get runId

TriggerAutomationReactor (virtual thread, synchronous)
  → reads automation.json (nodes in order)
  → claims single-run slot (AUTOMATION_ACTIVE_RUN) → runId visible to FE
  → inserts AUTOMATION_RUNS + AUTOMATION_NODE_OUTPUTS rows
  → calls AutomationRunEngine.run() synchronously
  → returns completed run result to jobId slot when done

AutomationRunEngine (same virtual thread)
  → iterates nodes in saved order
  → dispatches each node to its IAutomationNodeExecutor
  → writes node output + status to AUTOMATION_NODE_OUTPUTS after each node
  → FE polls GetAutomationRun every 3s while runId is known
```

## Reactors

| Reactor | Pixel | What it does |
| --- | --- | --- |
| `TriggerAutomationReactor` | `TriggerAutomation(project=["id"])` | Starts a run synchronously; returns completed run result |
| `GetActiveAutomationRunReactor` | `GetActiveAutomationRun(project=["id"])` | Returns `{RUN_ID, PROJECT_ID}` from active-run lock table; empty map when idle |
| `GetAutomationReactor` | `GetAutomation(project=["id"])` | Returns saved pipeline definition (automation.json) |
| `GetAutomationConfigReactor` | `GetAutomationConfig(project=["id"])` | Returns env var/secret config; masks sensitive values |
| `GetAutomationRunReactor` | `GetAutomationRun(project=["id"], runId=["id"])` | Returns live run state for FE polling |
| `ListAutomationRunsReactor` | `ListAutomationRuns(project=["id"])` | Returns run history |
| `CancelAutomationRunReactor` | `CancelAutomationRun(project=["id"], runId=["id"])` | Cancels an in-progress run |
| `SaveAutomationReactor` | `SaveAutomation(project=["id"], config=["{}"])` | Persists pipeline definition |
| `SaveAutomationConfigReactor` | `SaveAutomationConfig(project=["id"], config=["[]"])` | Persists env var config |
| `RunAutomationNodeReactor` | `RunAutomationNode(project=["id"], nodeId=["id"])` | Single-node test run; result not persisted |

## Node types

Each node type has a corresponding `IAutomationNodeExecutor` in `nodes/`:

| Type | Executor | What it does |
| --- | --- | --- |
| `trigger` | (no executor) | Seed node; provides `triggered_at`, `date`, `run_id` scope vars |
| `database-engine` | `DatabaseEngineNodeExecutor` | Runs SQL via `SqlQuery` pixel |
| `model-engine` | `ModelEngineNodeExecutor` | LLM ask or embeddings via `IModelEngine` |
| `vector-engine` | `VectorEngineNodeExecutor` | Search, add, delete, list via `IVectorDatabaseEngine` |
| `storage-engine` | `StorageEngineNodeExecutor` | File operations via `IStorageEngine` |
| `function-engine` | `FunctionEngineNodeExecutor` | Function invocation via `IFunctionEngine` |
| `app` | `AppEngineNodeExecutor` | Arbitrary pixel, optionally scoped to a project |
| `wait` | `WaitNodeExecutor` | Sleep N seconds; cancel-aware |

## DB tables

| Table | Purpose |
| --- | --- |
| `AUTOMATION_ACTIVE_RUN` | PK on `PROJECT_ID` — enforces one concurrent run per project |
| `AUTOMATION_RUNS` | One row per run: status, timing, node counts |
| `AUTOMATION_NODE_OUTPUTS` | One row per node per run: status, output, preview, duration |

## Key utilities

| Class | Purpose |
| --- | --- |
| `AutomationExecutionUtils` | Shared statics: GSON, scope building, variable resolution, output transforms, preview generation |
| `AutomationDatabaseUtility` | All DB reads/writes for runs and node outputs |
| `PixelExecutionUtils` | Timeout-enforced pixel execution with ThreadStore propagation |
| `AutomationConstants` | String constants for all keys, statuses, and file names |
