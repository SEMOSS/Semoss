# Automation Engine — Agent Guide

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

## Key classes

| Class | Purpose |
| --- | --- |
| `AutomationRunEngine` | Orchestrates a full pipeline run end-to-end |
| `AutomationExecutionUtils` | Shared statics: GSON, scope building, variable resolution, output transforms, preview generation |
| `AutomationGenerationUtils` | LLM/generation helpers: engine discovery, prompt building, response extraction |
| `AutomationDatabaseUtility` | All DB reads/writes for runs and node outputs |
| `PixelExecutionUtils` | Timeout-enforced pixel execution with ThreadStore propagation |
| `AutomationConstants` | String constants for all keys, statuses, and file names |

---

## Adding a new node type

1. **Create the executor** in `nodes/` implementing `IAutomationNodeExecutor`:
   - Declare `private static final Logger classLogger = LogManager.getLogger(YourExecutor.class);`
   - Log execution at `DEBUG` level with node label and key params before dispatching
   - Use `AutomationExecutionUtils.resolve(value, scope, configMap)` for all `${var}` substitution
   - Throw `IllegalArgumentException` for missing required config fields
   - Use `AutomationExecutionUtils.GSON` — do not declare a local `Gson` instance

2. **Register it** in the `EXECUTORS` map on `IAutomationNodeExecutor` (static field on the interface; use `Map.ofEntries` if adding an 11th entry — `Map.of` only supports 10)

3. **Add the type constant** to `AutomationConstants` (e.g. `NODE_TYPE_FOO = "foo-engine"`)

4. **Wire the FE** — add the node type to `automation.types.ts` and `automation.constants.ts` in `SemossWeb`

## Logging rules

Follow the platform standard — SLF4J `{}` placeholders, exception as the last argument:

```java
// ✅
classLogger.debug("Foo node \"{}\" executing operation={}", nodeLabel, operation);
classLogger.error("Foo node \"{}\" failed: {}", nodeLabel, e.getMessage(), e);

// ❌ — never concatenate strings in log calls
classLogger.error("Foo node " + nodeLabel + " failed: " + e.getMessage());
```

## Exception conventions

```java
// Missing/invalid user input — use IllegalArgumentException
throw new IllegalArgumentException("Foo node \"" + nodeLabel + "\": 'engineId' is required");

// User-facing reactor errors — use SemossPixelException
throw new SemossPixelException("Project does not exist or user does not have access");
```

## GSON

Use the shared instance — never declare your own:

```java
// ✅
AutomationExecutionUtils.GSON.fromJson(json, AutomationExecutionUtils.MAP_TYPE);

// ❌
private static final Gson GSON = new GsonBuilder().create();
```

## Reactor conventions

- Keep reactors thin: parse params, auth-check, delegate, return. Business logic belongs in `AutomationRunEngine`, `AutomationExecutionUtils`, or an executor.
- Always call `organizeKeys()` before reading `this.keyValue`
- Use `SecurityProjectUtils.testUserProjectIdForAlias` to resolve alias → UUID before any lookup
- Add `getReactorDescription()` and `getDescriptionForKey()` to every reactor
- MCP-destructive reactors (save, trigger, cancel) must override `getMcpToolMetadata()` to `MCPExecution.ASK`

## What not to change

- `AutomationDatabaseUtility` — DB access is intentional; use `setNullableString`, `SelectQueryStruct`, try-with-resources
- `PixelExecutionUtils` — timeout + ThreadStore propagation; do not bypass
- `CancelAutomationRunReactor` — dual-signal cancel (DB flag + in-memory); both signals are required for cluster safety
- `claimActiveRun` — PK-violation is the concurrency guard; do not add a separate lock
