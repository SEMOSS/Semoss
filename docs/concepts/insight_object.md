# The Insight Object (`prerna.om.Insight`)

The `prerna.om.Insight` class provides the live execution context for Pixel, reactors, data frames, and language workers. It holds the current user's context, variables, data, and execution history for a unit of work.

## Purpose and Key Responsibilities

*   **Contextual Hub**: Acts as a central context for operations. When a user executes Pixel scripts or interacts with data, these actions occur within the scope of an `Insight`.
*   **State Management**: Maintains the state of an analysis, including variables, loaded data frames, and applied filters.
*   **Recipe of Operations**: Records the sequence of Pixel commands executed, forming a "recipe" that can potentially be replayed or saved.
*   **Data Management**: Holds references to in-memory data frames (`ITableDataFrame`) and manages their lifecycle within its scope.
*   **Session Association**: Is associated with a specific `User` and can be linked to a `Project`.

## Key Information Held by an Insight

An `Insight` object holds various pieces of information critical to its function:

*   **`insightId` (String)**: A unique UUID automatically generated for each insight instance.
*   **`user` (User)**: The user object who owns or is interacting with this insight.
*   **`insightName` (String)**: A user-defined name for the insight, especially if it's saved.
*   **Project Association**:
    *   `projectId` (String): The ID of the project this insight belongs to (if saved).
    *   `projectName` (String): The name of the project.
    *   `rdbmsId` (String): An identifier for the saved insight definition within the project.
*   **`pixelList` (PixelList)**:
    *   A crucial transient field that stores an ordered list of `Pixel` objects. Each `Pixel` object represents a command executed (the Pixel script itself) and often the `NounMetadata` result of that command.
    *   This list effectively forms the "recipe" or history of operations performed within the insight.
*   **`varStore` (VarStore)**:
    *   A transient `prerna.sablecc2.om.VarStore` instance. This is a key-value store for variables created and used during Pixel script execution within the insight.
    *   It holds user-defined variables, intermediate results, and importantly, references to active data frames (`ITableDataFrame`) using keys (e.g., `Insight.CUR_FRAME_KEY` often points to the most recently used frame).
*   **`taskStore` (TaskStore)**: Manages ongoing tasks or iterators, such as those for streaming data from frames.
*   **File System Context**:
    *   `insightFolder` (String): The path to a dedicated directory on the file system for this insight. Used for storing temporary files, scripts, cached data, etc. The location depends on whether the insight is saved and the overall SEMOSS configuration.
*   **External Process Integration**:
    *   `rJavaTranslator` (AbstractRJavaTranslator): Manages communication with an R environment for the insight.
    *   `pyTranslator` (PyTranslator): Uses the user's Python connection and identifies the globals namespace for this insight.
*   **Caching Configuration**: `cacheable`, `cacheMinutes`, `cacheCron`, `cacheEncrypt` for defining how and if the insight's state or results should be cached.
*   **`pragmap` (Map)**: Stores pragma directives (e.g., `#CACHE=TRUE`, `#RAW_DATA=TRUE`) encountered during Pixel execution, which can modify the behavior of subsequent operations.

## Python variable scope within the user's process

In the normal user-code execution path, the `User` owns a Python process through its `ClientProcessWrapper`. Multiple Insights associated with that `User` can reuse the same Python connection. **Each Insight's runtime `insightId` selects a separate Python globals dictionary inside that process.** The process is reused while top-level variable names are scoped to the selected Insight.

### How the namespace is selected

1. [Insight.getPyTranslator()](../../src/prerna/om/Insight.java) obtains `user.getPythonSocketClient(true)` and constructs `new PyTranslator(sc, this)`. The user manages the shared connection; the Insight supplies the namespace identity.
2. [PyTranslator](../../src/prerna/ds/py/PyTranslator.java) sends `PayloadStruct.insightId = globalStoreInsight.getInsightId()` with the Python command.
3. [TCPServerHandler.handle_python()](../../py/gaas_tcp_server_handler.py) reads that ID and asks `InsightGlobalStore.get_insight_globals(insight_id)` for the corresponding dictionary. The store creates and initializes the dictionary on first use, then reuses it for subsequent calls with the same ID.
4. The handler's `execute_and_capture()` executes statements and evaluates expressions using that dictionary. Assignments, imports, and function/class definitions therefore remain available to later calls in the same Insight namespace.

The Python-side selection is conceptually:

```python
# Simplified view of the handler; output capture is omitted.
store = InsightGlobalStore()
namespace = store.get_insight_globals(insight_id)
exec(code, namespace)
```

For example, Insights A and B can use the same variable name in one user process:

| Execution | Python code | Effect |
| --- | --- | --- |
| Insight A, first call | `counter = 10` | A's dictionary holds `counter = 10` |
| Insight B, first call | `counter = 100` | B's dictionary holds `counter = 100` |
| Insight A, later call | `counter += 1` | A now holds `11`; B still holds `100` |
| Insight B, later call | `counter` | Evaluates to `100` |

The same applies to names bound to DataFrames, lists, and user-defined functions. `globals()` in executed code refers to the selected namespace. The lookup uses the runtime Insight ID, rather than the project ID, saved insight `rdbmsId`, HTTP session ID, or individual job ID. Two callers deliberately using the same namespace ID in the same Python process share its variables.

### What this scope covers

The globals dictionary controls Python name bindings. Java's `Insight.varStore` is a separate store used by Pixel; assigning a Python variable does not automatically create a matching Pixel variable. Frame and reactor integrations explicitly bridge the two where needed.

The dictionaries share a Python interpreter and its process resources. Imported module objects and other process-wide state can be shared, and filesystem access is governed separately. Project-asset imports have additional path-based handling in the Python server. Insight-level variable scoping provides namespace separation; process isolation and concurrent access to shared objects require their own mechanisms.

### Namespace identity and execution identity

[PayloadStruct](../../src/prerna/tcp/PayloadStruct.java) can carry two Insight IDs:

- **`insightId`** selects the Python globals dictionary, taken from the translator's `globalStoreInsight`.
- **`executionInsightId`** carries the calling Insight's execution/user context when it differs, including context used for callbacks into Java.

For ordinary Insight Python execution, the translator is bound to that Insight. Engine-owned Python workers can instead bind their translator to an internal process Insight while passing the caller as `executionInsightId`. This keeps the engine's persistent Python objects under its own namespace while carrying the caller's context for backend operations. See [Java–Python communication](../platform_services/java_python_communication.md).

### Lifetime and cleanup

Namespace state remains in Python memory between calls. [InsightUtility](../../src/prerna/util/insight/InsightUtility.java) connects its cleanup to the Insight lifecycle:

- Clearing an Insight sends `CLEAR_NON_MODULE_GLOBALS` through `clearInsightGlobals()`. The Python store removes ordinary variables while retaining imported modules and selected runtime entries, including the recorded working directory.
- Dropping an Insight sends `REMOVE_INSIGHT_GLOBALS` through `removeInsightGlobals()`, deleting that Insight's dictionary and requesting garbage collection. Objects still referenced elsewhere can remain alive.
- These lifecycle calls require Python to be enabled and `deletePythonGlobalsOnDropInsight` to be true; the flag defaults to true and is also checked by the clear path.
- Restarting the Python process loses its in-memory namespaces. Reusing the same Insight ID afterward creates a fresh dictionary; the ID alone cannot restore previous Python values. Persist outputs or use the appropriate save/cache/replay workflow when state must survive the process.

## How Insights are Used

*   **Pixel Execution (`PixelRunner`)**:
    *   All Pixel scripts are executed within the context of an `Insight` object.
    *   `PixelRunner` uses the insight's `varStore` to resolve variables and store new ones.
    *   Results of Pixel operations (`NounMetadata`) are often added to the insight's `pixelList`.
*   **Reactors (`IReactor`)**:
    *   Many reactors receive the current `Insight` object as part of their execution context.
    *   They can access the `varStore` to get input parameters or data frames, and they can update the `varStore` with their results.
*   **Model Engines (`AbstractModelEngine`, `AbstractVectorDatabaseEngine`)**:
    *   Model and Vector engines often require an `Insight` object when their methods (e.g., `ask()`, `embeddings()`, `nearestNeighbor()`) are called.
    *   The insight provides user context for logging (`ModelInferenceLogsDatabase`), session context for conversation history (if the model engine supports it via `keepConversationHistory`), and access to data frames or variables from the `varStore` that might be needed as input to the models.
*   **Data Frames (`ITableDataFrame`)**:
    *   Data frames loaded or created within an insight are typically stored in its `varStore`.
    *   The insight effectively "owns" these in-memory data representations for the duration of the session or until they are explicitly cleared.
*   **Saving and Loading**:
    *   Saved insight definitions retain the Pixel recipe and metadata associated with a `Project`. Use the appropriate save, cache, or replay workflow for the state that needs to survive an execution session.
    *   Loading a saved insight involves recreating the `Insight` object and replaying its Pixel recipe or restoring its state to bring the user back to their previous analysis.

## Insights, rooms, and agent runs

An Insight is the live execution context used by Pixel, frames, language workers, and the current user. A model Room is a separately persisted conversation, and an agent run is a tracked invocation with its own run ID and lifecycle.

Agent submission captures an execution context for the background worker. Durable run records do not serialize arbitrary live Insight state, active language-worker variables, or user tokens. See [backend architecture](../01_backend_architecture_overview.md) and [agent durability](../agents/agent_runs.md#durability-and-cluster-behavior) before relying on restart or cross-node behavior.
