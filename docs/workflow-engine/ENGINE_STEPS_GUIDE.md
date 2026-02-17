# Engine Steps in Workflow Engine

> How the FE exposes SEMOSS engines (Vector, Storage, Function, etc.) as workflow steps.

## Key Insight

**No new backend code is needed.** Every engine that has been MCP-enabled already exposes tools via the existing `GetMCPTools` reactor. The existing `RUN_TOOL` step type in the workflow engine already calls `MCPFactory.build(engine).callTool(toolName, params, insight)`.

The FE just needs a guided selection flow: **Engine Type → Engine → Tool → Params**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Workflow Canvas — "Engine Step" Configuration Panel    │
│                                                         │
│  1. Select Engine Type:  [ VECTOR ▼ ]                   │
│  2. Select Engine:       [ my-pinecone-db ▼ ]           │
│  3. Select Tool:         [ VectorDatabaseQuery ▼ ]      │
│  4. Configure Params:    (auto-generated from schema)   │
└─────────────────────────────────────────────────────────┘
```

### What happens at each step:

| # | User Action | Pixel Call | Returns |
|---|------------|-----------|---------|
| 1 | Pick engine type | `MyEngines(engineTypes=["VECTOR"], limit=[50], offset=[0])` | List of engines the user has access to, filtered by type |
| 2 | Pick specific engine | (no call — just selecting from the list above) | Engine ID + name |
| 3 | Pick tool | `GetMCPTools(engine=["<engineId>"])` | JSON with `tools` array, each with `name`, `description`, `inputSchema` |
| 4 | Fill params | (no call — FE renders a form from `inputSchema.properties`) | User provides param values |

---

## Engine Types Available

These are the `CATALOG_TYPE` values from `IEngine.java`:

| Type | Description | Typical Tools (when MCP-enabled) |
|------|------------|----------------------------------|
| `VECTOR` | Vector databases (Pinecone, Weaviate, FAISS, etc.) | `VectorDatabaseQuery`, `CreateEmbeddingsFromDocuments`, `ListDocumentsInVectorDatabase`, `RemoveDocumentFromVectorDatabase`, `VectorFileDownload` |
| `STORAGE` | File storage (S3, Azure Blob, etc.) | `ListStoragePath`, `ListStoragePathDetails`, `PullFromStorage`, `PushToStorage`, `DeleteFromStorage` |
| `FUNCTION` | Custom function engines | `ExecuteFunctionEngine` |
| `DATABASE` | Relational databases | ⏳ Deferred — no standard MCP tools yet |
| `MODEL` | LLM / ML model engines | Use `LLM_ASK` or `LLM_AGENT` step types instead |
| `GUARDRAIL` | Content guardrails | Use `GUARDRAIL` step type instead |

> **Note:** `DATABASE` and `MODEL` are excluded from the engine step type picker for now. Databases don't have standard MCP tools built yet. Models are handled by dedicated `LLM_ASK`/`LLM_AGENT` step types.

---

## Existing Pixel APIs (No Changes Needed)

### 1. List engines by type
```
MyEngines(engineTypes=["VECTOR"], limit=[50], offset=[0]);
```
**Returns:** Array of engine objects:
```json
[
  {
    "database_id": "abc123-...",
    "database_name": "My Pinecone DB",
    "database_type": "VECTOR",
    "permission": 1,
    ...
  }
]
```

Optional filters:
- `filterWord=["search term"]` — search by name
- `onlyFavorites=[true]` — favorites only
- `permissionFilters=[1,2]` — by permission level

### 2. Get tools for an engine
```
GetMCPTools(engine=["abc123-..."]);
```
**Returns:**
```json
{
  "tools": [
    {
      "name": "VectorDatabaseQuery",
      "description": "Query a vector database with a search string",
      "title": "Vector Database Query",
      "inputSchema": {
        "type": "object",
        "properties": {
          "engine": {
            "description": "The vector database engine ID",
            "type": "string",
            "enum": ["abc123-..."]
          },
          "searchStatement": {
            "description": "The search query string",
            "type": "string"
          },
          "limit": {
            "description": "Max number of results",
            "type": "integer"
          }
        },
        "required": ["engine", "searchStatement"]
      },
      "_meta": {
        "SMSS_FUNCTION_NAME": "VectorDatabaseQuery",
        "SMSS_MCP_EXECUTION": "auto"
      }
    }
  ],
  "_meta": {
    "SMSS_ENGINE_ID": "abc123-...",
    "SMSS_ENGINE_NAME": "My Pinecone DB",
    "SMSS_ENGINE_TYPE": "VECTOR"
  }
}
```

### 3. How `RUN_TOOL` step executes it

The existing `RunToolStepHandler` takes this config in `workflow.json`:

```json
{
  "stepId": "query-vector",
  "type": "RUN_TOOL",
  "name": "Search Knowledge Base",
  "config": {
    "engineId": "abc123-...",
    "toolName": "VectorDatabaseQuery",
    "params": {
      "engine": "abc123-...",
      "searchStatement": "{{user-input.output}}",
      "limit": 5
    }
  },
  "next": ["ask-llm"]
}
```

At runtime, `WorkflowExecutor`:
1. Resolves templates in `params` (e.g., `{{user-input.output}}` → actual value)
2. Calls `MCPFactory.build(engine).callTool("VectorDatabaseQuery", params, insight)`
3. Stores result in `WorkflowContext` as `StepResult`

---

## FE Implementation Details

### Step Type in UI

The FE should present `RUN_TOOL` steps as **"Engine"** or **"Use Engine"** in the step palette. This is NOT a new step type — it maps to `RUN_TOOL` in the workflow JSON.

### Configuration Panel Flow

```tsx
// Pseudo-code for the Engine step config panel

// Step 1: Engine type selector (only types with MCP tools)
const ENGINE_TYPES = ["VECTOR", "STORAGE", "FUNCTION"];

// Step 2: On type select, fetch engines
const engines = await runPixel(`MyEngines(engineTypes=["${selectedType}"], limit=[50], offset=[0])`);

// Step 3: On engine select, fetch tools
const toolsResponse = await runPixel(`GetMCPTools(engine=["${selectedEngineId}"])`);
const tools = toolsResponse.tools;

// Step 4: On tool select, render param form from inputSchema
const selectedTool = tools.find(t => t.name === selectedToolName);
const schema = selectedTool.inputSchema;

// Render form fields from schema.properties
// Pre-fill engine param with the selected engine ID
// Allow template syntax in string fields (e.g., {{prevStep.output}})
```

### Auto-filling the Engine ID param

Most engine tools have an `engine` param with an `enum` constraint (set to the engine ID). The FE should:
1. Auto-fill this param with the selected engine ID
2. Hide it from the user (or show as read-only)
3. Include it in the saved config

### Template Support in Params

String params should support template syntax for chaining:
- `{{previousStep.output}}` — output from a prior step
- `{{previousStep.output.fieldName}}` — nested field access

This enables patterns like: Vector Search → LLM Ask, where the LLM's prompt includes search results.

---

## Example: Vector Search → LLM Summarize

```json
{
  "steps": [
    {
      "stepId": "search",
      "type": "RUN_TOOL",
      "name": "Search Knowledge Base",
      "config": {
        "engineId": "pinecone-abc123",
        "toolName": "VectorDatabaseQuery",
        "params": {
          "engine": "pinecone-abc123",
          "searchStatement": "What are the benefits eligibility requirements?",
          "limit": 5
        }
      },
      "next": ["summarize"]
    },
    {
      "stepId": "summarize",
      "type": "LLM_ASK",
      "name": "Summarize Results",
      "config": {
        "modelId": "gpt4-model-id",
        "systemPrompt": "You are a helpful assistant. Summarize the following search results.",
        "userPrompt": "Here are the search results:\n\n{{search.output}}\n\nPlease provide a concise summary."
      },
      "next": ["output"]
    },
    {
      "stepId": "output",
      "type": "OUTPUT",
      "name": "Final Answer",
      "config": {}
    }
  ]
}
```

---

## Important Notes

1. **Only MCP-enabled engines have tools.** If `GetMCPTools` returns empty, the engine hasn't been set up with `MakeEngineMCP` yet. Show a message like "This engine has no tools configured."

2. **`MakeEngineMCP`** is the admin reactor that generates `pixel_mcp.json` for an engine. For VECTOR/STORAGE/FUNCTION types, it auto-generates standard tools from built-in reactor definitions. Custom reactors can also be added.

3. **The `_meta.SMSS_MCP_EXECUTION` field** on each tool indicates execution mode:
   - `"auto"` — tool can run without user confirmation
   - `"ask"` — tool should ask for confirmation before running
   - In workflow context, all tools run automatically (the workflow definition IS the user's intent).

4. **Engine access is security-checked.** `MCPFactory.build()` checks `isMCPEnabled()`. `GetMCPTools` checks `userCanEditEngine`. The workflow executor runs tools as the user who triggered the execution.

5. **Projects are also engines.** `MCPFactory.build()` treats `IProject` instances differently (the project itself implements `IMCP`). Project-based MCP tools (custom Python/Pixel tools) also work with `RUN_TOOL` — the user just selects a project instead of an engine.
