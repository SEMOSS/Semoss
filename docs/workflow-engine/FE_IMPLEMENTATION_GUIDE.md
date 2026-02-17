# Workflow Engine — Frontend Implementation Guide

> This document describes the backend API surface, data contracts, and implementation guidance for building the workflow canvas editor UI.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Pixel API Reference](#pixel-api-reference)
3. [Data Schema — workflow.json](#data-schema--workflowjson)
4. [Step Types & Config Schemas](#step-types--config-schemas)
5. [Template Expressions](#template-expressions)
6. [UI Components](#ui-components)
7. [User Flows](#user-flows)
8. [Important Implementation Notes](#important-implementation-notes)

---

## Architecture Overview

Workflows are stored as **SEMOSS projects** with `projectType = "WORKFLOW"`. Each workflow project contains a single `workflow.json` file at `assets/workflow/workflow.json` that describes the full DAG.

**The FE owns all mutation logic.** The backend is a dumb document store + executor. There are no incremental `AddStep`/`DeleteStep` APIs. The FE loads the full JSON, edits it in-memory, and saves the entire document on every save.

```
┌──────────────────────────────────────────────────────────────┐
│                     Frontend Canvas                          │
│                                                              │
│  Load workflow ──► Edit in memory ──► Save full JSON         │
│       ▲                                    │                 │
│       │              GetWorkflowStatus     │  SaveWorkflow   │
│       └────────────────────────────────────┘                 │
│                                                              │
│  Run workflow ──► Poll/display result                        │
│       │              RunWorkflow                             │
└───────┼──────────────────────────────────────────────────────┘
        ▼
┌──────────────────────────────────────────────────────────────┐
│                     Backend Engine                            │
│                                                              │
│  WorkflowExecutor: parse JSON → validate DAG → walk steps    │
│  Handlers: LLM_ASK, LLM_AGENT, RUN_TOOL, RUN_PIXEL, etc.   │
│  Logging: saves execution results to executions/ folder      │
└──────────────────────────────────────────────────────────────┘
```

---

## Pixel API Reference

### 1. Create a Workflow Project

```
CreateProject(project=["My Workflow Name"], type=["WORKFLOW"]);
```

**Returns:** Map with `project_id`, `project_name`, `project_type`

**What it does:** Creates a new SEMOSS project with type `WORKFLOW`, scaffolds `assets/workflow/workflow.json` with an empty default definition, creates `assets/workflow/executions/` directory, and initializes git.

---

### 2. Get Workflow Status (Load)

```
GetWorkflowStatus(project=["<project-uuid>"]);
```

**Returns:**
```json
{
  "projectId": "abc-123",
  "projectName": "My Workflow",
  "projectType": "WORKFLOW",
  "workflow": {
    "workflowId": "abc-123",
    "name": "My Workflow",
    "version": 1,
    "steps": [ ... ],
    "variables": { ... },
    "settings": { ... }
  },
  "executions": [
    {
      "executionId": "exec-uuid-1",
      "status": "SUCCESS",
      "durationMs": 4523,
      "triggeredBy": "manual",
      "startTimeMs": 1708000000000,
      "endTimeMs": 1708000004523
    },
    {
      "executionId": "exec-uuid-2",
      "status": "ERROR",
      "durationMs": 1200,
      "triggeredBy": "schedule",
      "startTimeMs": 1707990000000,
      "endTimeMs": 1707990001200,
      "error": "Step 'step-3' failed: ..."
    }
  ]
}
```

**Notes:**
- `workflow` is `null` if workflow.json hasn't been saved yet (freshly created project)
- `executions` is sorted newest-first, capped at 50 entries
- Executions contain only summary fields (not full step-by-step results)
- Requires `userCanViewProject` permission

---

### 3. Save Workflow (Persist)

```
SaveWorkflow(project=["<project-uuid>"], json=[<workflow-json-object>], comment=["optional commit message"]);
```

**Parameters:**
| Key | Required | Type | Description |
|-----|----------|------|-------------|
| `project` | Yes | String | Project UUID |
| `json` | Yes | Map/Object | The full `workflow.json` object |
| `comment` | No | String | Git commit message (defaults to "Update workflow definition") |

**Returns:** `true` on success

**What it does:**
1. Validates the user has `userCanEditProject` permission
2. Validates the project is type `WORKFLOW`
3. **Validates the workflow DAG** — rejects saves with:
   - Dangling step references (next/ifTrue/ifFalse pointing to nonexistent stepIds)
   - Orphaned steps (unreachable from any entry point)
   - Cycles in the DAG
   - Duplicate step IDs
   - No entry points (every step has incoming edges — nothing can start)
4. Writes `workflow.json`
5. Git add + commit
6. Syncs to cluster (if clustered)

**Error responses:** If validation fails, the error message lists all issues:
```
"Workflow validation failed: Step 'step-2' references non-existent step 'deleted-step'; Steps [step-5] are orphaned (unreachable from any entry point)"
```

**Important:** Always send the ENTIRE workflow.json, not a partial update. The BE overwrites the whole file.

---

### 4. Run Workflow (Execute)

```
RunWorkflow(project=["<project-uuid>"], variables=[{"key": "value"}], trigger=["manual"]);
```

**Parameters:**
| Key | Required | Type | Description |
|-----|----------|------|-------------|
| `project` | Yes | String | Project UUID |
| `variables` | No | Map | Runtime variable overrides (merged with workflow-level defaults) |
| `trigger` | No | String | Trigger source label: `"manual"`, `"schedule"`, `"webhook"`, `"api"` (defaults to `"manual"`) |

**Returns:**
```json
{
  "executionId": "exec-uuid",
  "workflowId": "abc-123",
  "status": "SUCCESS",
  "durationMs": 4523,
  "triggeredBy": "manual",
  "finalOutput": "The workflow's last step output value",
  "error": null
}
```

**Status values:** `SUCCESS`, `ERROR`, `TIMEOUT`

**Notes:**
- Execution is **synchronous** — the call blocks until the workflow completes
- For long-running workflows, consider running in a background thread and polling
- The execution result is also automatically persisted to `executions/{executionId}.json` for history
- Requires `userCanViewProject` permission

---

### 5. Schedule a Workflow

```
ScheduleJob(
  jobName=["Weekly Report Generator"],
  jobGroup=["<project-uuid>"],
  cronExpression=["0 0 9 ? * MON"],
  recipe=["RunWorkflow(project=[\"<project-uuid>\"], trigger=[\"schedule\"]);"],
  uiState=["<serialized-ui-state>"]
);
```

This uses the existing SEMOSS scheduler — no new APIs needed. The `recipe` is a Pixel string containing the `RunWorkflow` call.

---

### 6. List Workflow Projects

Use the existing project listing APIs to get all projects, filter by `projectType === "WORKFLOW"` on the FE side. SEMOSS project list APIs already support type filtering.

---

## Data Schema — workflow.json

This is the complete schema the FE must produce when saving:

```jsonc
{
  // Unique identifier — typically matches the project ID
  "workflowId": "abc-123-uuid",

  // Human-readable name
  "name": "Customer Onboarding Flow",

  // Increment on each save (optional, FE-managed)
  "version": 1,

  // Workflow-level variables — defaults that can be overridden at runtime
  "variables": {
    "database": "db-engine-uuid",
    "notifyEmail": "admin@example.com",
    "threshold": 0.8
  },

  // Execution settings
  "settings": {
    "maxSteps": 50,        // Max step executions before abort (prevents infinite loops)
    "timeoutMs": 300000,   // 5 minute timeout
    "onError": "stop"      // "stop" = abort on first error, "skip" = skip failed step's successors
  },

  // The DAG — array of step nodes
  "steps": [
    {
      "stepId": "step-1",              // Unique ID — generate UUID on FE
      "type": "LLM_ASK",              // Step type — must match STEP_TYPE enum
      "name": "Summarize Data",        // Display label on canvas
      "description": "optional desc",  // Tooltip/detail text

      // Canvas position — used only by FE, ignored by BE
      "position": { "x": 100, "y": 200 },

      // Type-specific configuration (see Step Types section below)
      "config": {
        "modelId": "model-engine-uuid",
        "systemPrompt": "You are a data analyst.",
        "userPrompt": "Summarize: {{step-0.output}}"
      },

      // Input mappings — template expressions resolved before execution
      "inputs": {
        "data": "{{step-0.output}}",
        "format": "{{variables.outputFormat}}"
      },

      // Edges — default successors
      "next": ["step-2"],

      // Conditional edges — only used by CONDITION type
      "ifTrue": null,
      "ifFalse": null
    }
  ]
}
```

### Key Rules

1. **`stepId`** must be unique across all steps. Use UUIDs (e.g., `crypto.randomUUID()`).
2. **`type`** must be one of the valid `STEP_TYPE` values (see below).
3. **`next`**, **`ifTrue`**, **`ifFalse`** are arrays of `stepId` strings referencing other steps.
4. **Entry steps** are steps with no incoming edges (no other step points to them via next/ifTrue/ifFalse). These execute first.
5. **`position`** is entirely FE-owned. The BE ignores it.
6. **`config`** schema varies by step type (documented below).
7. **`inputs`** are key-value pairs where values are `{{template}}` expressions.

---

## Step Types & Config Schemas

### `STATIC` — Static Value

Outputs a fixed value. Useful for constants and test data.

```json
{
  "type": "STATIC",
  "config": {
    "value": "any value — string, number, object, array"
  }
}
```

**Output:** Whatever `value` is.

---

### `LLM_ASK` — Single LLM Call

Sends a prompt to an LLM and returns the text response.

```json
{
  "type": "LLM_ASK",
  "config": {
    "modelId": "<model-engine-uuid>",       // Required — LLM engine ID
    "systemPrompt": "You are a...",         // Optional — system message
    "userPrompt": "Analyze this: {{step-1.output}}",  // Required — user message
    "paramMap": {                           // Optional — model parameters
      "temperature": 0.7,
      "max_tokens": 2000
    }
  }
}
```

**Output:** String — the LLM's text response.

**FE config form should provide:**
- Model engine dropdown (fetch available models from existing SEMOSS APIs)
- System prompt textarea
- User prompt textarea (with `{{...}}` autocomplete)
- Optional: temperature slider, max_tokens input

---

### `LLM_AGENT` — Agentic LLM with Tool Use

Sends a prompt to an LLM with access to MCP tools. The agent loops: LLM → tool call → feed result back → repeat until the LLM gives a final text answer.

```json
{
  "type": "LLM_AGENT",
  "config": {
    "modelId": "<model-engine-uuid>",       // Required — LLM engine ID
    "systemPrompt": "You are a...",         // Optional
    "userPrompt": "Find and summarize...",  // Required
    "toolEngineIds": [                      // Required — list of MCP engine IDs to give the agent
      "mcp-engine-uuid-1",
      "mcp-engine-uuid-2"
    ],
    "paramMap": { "temperature": 0 },       // Optional
    "maxIterations": 10                     // Optional — max tool-use loops (default: 10)
  }
}
```

**Output:** String — the LLM's final text response after all tool calls are resolved.

**FE config form should provide:**
- Model engine dropdown
- System/user prompt textareas
- Multi-select for tool engine IDs (fetch from `GetAllEngines` or similar)
- Max iterations slider (1–20, default 10)

---

### `RUN_TOOL` — Direct MCP Tool Call

Calls a specific MCP tool directly (no LLM involved).

```json
{
  "type": "RUN_TOOL",
  "config": {
    "engineId": "<mcp-engine-uuid>",    // Required
    "toolName": "search_database",      // Required — tool method name
    "params": {                         // Required — tool input parameters
      "query": "{{step-1.output}}",
      "limit": 10
    }
  }
}
```

**Output:** Whatever the tool returns (typically a Map or String).

**FE config form should provide:**
- Engine dropdown → on select, fetch available tools (use `GetMCPTools(engine=["<id>"])` or similar)
- Tool dropdown → on select, show auto-generated param form from the tool's `inputSchema`
- Param inputs with `{{...}}` autocomplete support

---

### `RUN_PIXEL` — Execute Pixel Recipe

Runs an arbitrary Pixel expression.

```json
{
  "type": "RUN_PIXEL",
  "config": {
    "recipe": "Database(\"<db-id>\") | Select(columns) | Collect(500);"
  }
}
```

**Output:** The value of the last NounMetadata from the Pixel execution (can be a frame, map, list, string, etc.).

**FE config form:** Pixel expression code editor (monospace textarea with syntax highlighting if possible).

---

### `CONDITION` — Conditional Branch

Evaluates a boolean expression and routes to `ifTrue` or `ifFalse` edges.

```json
{
  "type": "CONDITION",
  "config": {
    "left": "{{step-1.output}}",       // Left operand (supports template resolution)
    "operator": ">",                    // Comparison operator
    "right": 100                        // Right operand
  }
}
```

**Supported operators:** `==`, `!=`, `>`, `<`, `>=`, `<=`, `contains`, `empty`, `notEmpty`

**Edge wiring:** CONDITION steps use `ifTrue` and `ifFalse` instead of `next`:
```json
{
  "stepId": "check-threshold",
  "type": "CONDITION",
  "config": { "left": "{{score.output}}", "operator": ">=", "right": 0.8 },
  "ifTrue": ["approve-step"],
  "ifFalse": ["reject-step"],
  "next": null
}
```

**Output:** Boolean (true/false). The routing is automatic based on which branch is taken.

**FE canvas rendering:**
- Render CONDITION nodes with a diamond shape (decision node)
- Two outgoing edges: green for `ifTrue`, red for `ifFalse`
- Edge labels: "True" / "False"

**FE config form:**
- Left value input (with `{{...}}` autocomplete)
- Operator dropdown
- Right value input (with `{{...}}` autocomplete)

---

### `OUTPUT` — Terminal Output

Marks a terminal step. Simply passes through its input as the final workflow output.

```json
{
  "type": "OUTPUT",
  "config": {
    "value": "{{step-5.output}}"
  }
}
```

**Output:** Whatever `value` resolves to.

**Note:** The workflow's `finalOutput` in the execution result is the output of the last successfully executed step, regardless of whether it's an OUTPUT step. But using OUTPUT makes the intent explicit.

---

### `RUN_PYTHON` — (Not yet implemented)

Placeholder for future Python script execution.

### `LOOP` — (Not yet implemented)

Placeholder for future loop/iteration support.

### `TRANSFORM` — (Not yet implemented)

Placeholder for future data transformation support.

### `HUMAN_INPUT` — (Not yet implemented)

Placeholder for future human-in-the-loop support (workflow pauses and waits for user input).

### `GUARDRAIL` — (Not yet implemented)

Placeholder for future guardrail/safety-check support.

---

## Template Expressions

The backend resolves `{{...}}` expressions in step `config` and `inputs` before executing each step. The FE is responsible for baking the correct expressions into the JSON at design time.

### Syntax

| Expression | Resolves To |
|---|---|
| `{{stepId.output}}` | The output of a previously executed step |
| `{{stepId.output.fieldName}}` | A nested field from a step's output (if output is a Map) |
| `{{stepId.output.field.nested}}` | Deep nested field access |
| `{{stepId.metadata.key}}` | Step metadata (e.g., `{{check.metadata.branch}}` → `"ifTrue"`) |

### Type Preservation

- If the entire string is a single `{{expression}}`, the resolved value preserves its original type (Map, List, Number, etc.)
- If `{{expression}}` is embedded in a larger string like `"Hello {{name.output}}"`, it's string-interpolated

### Autocomplete UX

The FE should provide `{{...}}` autocomplete in text inputs:

1. When the user types `{{`, show a dropdown of:
   - All step IDs that appear **before** the current step in the DAG (i.e., steps that could have executed already)
   - Each step shows: `stepId` → `stepName` (`stepType`)
2. After selecting a step, auto-insert `.output` (most common)
3. Optionally show `.metadata` for advanced users

---

## UI Components

### 1. Workflow List View

A shared view (not per-project) that shows all WORKFLOW projects the user has access to.

| Column | Source |
|---|---|
| Name | `projectName` |
| Last Run Status | `executions[0].status` (from `GetWorkflowStatus`) |
| Last Run Time | `executions[0].startTimeMs` |
| Triggered By | `executions[0].triggeredBy` |
| Actions | Edit, Run, Schedule, Share, Delete |

**Actions:**
- **Create Workflow** → `CreateProject(project=["name"], type=["WORKFLOW"])` → navigate to canvas
- **Edit** → Navigate to canvas editor for this project
- **Run** → `RunWorkflow(project=["uuid"])` → show result toast
- **Delete** → Use existing `DeleteProject` Pixel
- **Share** → Use existing project sharing (permissions are the same as any SEMOSS project)

---

### 2. Canvas Editor (Main View)

**Layout:**
```
┌─────────────────────────────────────────────────────────────────┐
│  Toolbar: [Save] [Run] [Schedule] [Undo] [Redo]  [Zoom +/-]   │
├──────────┬──────────────────────────────────────┬───────────────┤
│          │                                      │               │
│  Step    │                                      │  Config       │
│  Palette │        Canvas (react-flow)           │  Panel        │
│          │                                      │               │
│  ────    │     ┌────┐    ┌────┐    ┌────┐      │  (shows when  │
│  LLM_ASK │     │ A  │───►│ B  │───►│ C  │      │   a node is   │
│  RUN_TOOL│     └────┘    └────┘    └────┘      │   selected)   │
│  COND.   │                                      │               │
│  ...     │                                      │               │
│          │                                      │               │
├──────────┴──────────────────────────────────────┴───────────────┤
│  Execution History (collapsible drawer)                         │
│  exec-1 | SUCCESS | 4.5s | manual | Feb 17 09:30              │
│  exec-2 | ERROR   | 1.2s | schedule | Feb 16 09:00            │
└─────────────────────────────────────────────────────────────────┘
```

---

### 3. Step Palette (Left Sidebar)

Draggable step types grouped by category:

| Category | Step Types |
|---|---|
| **AI** | `LLM_ASK`, `LLM_AGENT` |
| **Data** | `RUN_TOOL`, `RUN_PIXEL` |
| **Logic** | `CONDITION` |
| **I/O** | `STATIC`, `OUTPUT` |
| **Future** | `LOOP`, `TRANSFORM`, `HUMAN_INPUT`, `GUARDRAIL`, `RUN_PYTHON` (show as disabled/coming-soon) |

**Drag-to-create:**
1. User drags a step type from the palette onto the canvas
2. FE creates a new step object with:
   - `stepId`: `crypto.randomUUID()`
   - `type`: the dragged type
   - `name`: default name like "LLM Ask 1" (auto-increment)
   - `position`: drop coordinates
   - `config`: empty `{}`
   - `next`, `ifTrue`, `ifFalse`: `null`
3. Step appears as an unconnected node on the canvas

---

### 4. Config Panel (Right Sidebar)

Shows when a node is selected. The form fields depend on the step's `type`:

- See [Step Types & Config Schemas](#step-types--config-schemas) for what fields each type needs
- All text fields should support `{{...}}` autocomplete
- Model/engine dropdowns should fetch options from existing SEMOSS engine listing APIs
- Tool dropdowns should fetch available tools when an engine is selected

**Common fields (all step types):**
- Name (text input)
- Description (optional textarea)

---

### 5. Variables Panel

A top bar or collapsible section for workflow-level variables:

| Field | Description |
|---|---|
| Name | Variable key (used in templates as `{{variables.name}}` — but note: variables are merged into context, template syntax is `{{stepId.output}}` for step outputs) |
| Default Value | Value used if no runtime override is provided |
| Description | Optional documentation |

---

### 6. Execution History Drawer

Bottom drawer or tab. Populated from `GetWorkflowStatus` response's `executions` array.

| Column | Field |
|---|---|
| Status | `status` — render as badge (green=SUCCESS, red=ERROR, yellow=TIMEOUT) |
| Duration | `durationMs` — format as "4.5s" or "2m 30s" |
| Triggered By | `triggeredBy` |
| Time | `startTimeMs` — format as relative or absolute time |
| Error | `error` — show in tooltip or expandable row |

---

## User Flows

### Flow 1: Create and Build a Workflow

```
1. User clicks "Create Workflow"
2. FE calls: CreateProject(project=["My Workflow"], type=["WORKFLOW"]);
3. FE receives project_id, navigates to canvas editor
4. FE calls: GetWorkflowStatus(project=["<id>"]);
5. FE receives empty workflow (default scaffold)
6. User drags steps onto canvas, connects edges, configures each step
7. User clicks "Save"
8. FE assembles full workflow.json from canvas state
9. FE calls: SaveWorkflow(project=["<id>"], json=[<workflow-object>]);
10. If validation fails → show error toast with details
11. If success → show success toast, update local state
```

### Flow 2: Edit an Existing Workflow

```
1. User clicks "Edit" on a workflow in the list
2. FE calls: GetWorkflowStatus(project=["<id>"]);
3. FE parses workflow.json → renders steps as nodes, edges from next/ifTrue/ifFalse
4. User makes changes (add/delete/move steps, rewire edges, edit configs)
5. User clicks "Save"
6. FE rebuilds full workflow.json from canvas state
7. FE calls: SaveWorkflow(project=["<id>"], json=[<workflow-object>]);
```

### Flow 3: Run a Workflow

```
1. User clicks "Run" (from list or canvas toolbar)
2. FE calls: RunWorkflow(project=["<id>"], variables=[{...}]);
3. FE shows loading state
4. FE receives result: { executionId, status, durationMs, finalOutput, error }
5. FE shows result toast/modal:
   - SUCCESS: green, show finalOutput
   - ERROR: red, show error message
   - TIMEOUT: yellow, show timeout info
6. FE refreshes execution history
```

### Flow 4: Delete a Step

```
1. User selects a step node on canvas, presses Delete
2. FE removes the step from the in-memory steps array
3. FE scans ALL other steps and removes any references to the deleted stepId:
   - Remove from other steps' `next` arrays
   - Remove from other steps' `ifTrue` arrays
   - Remove from other steps' `ifFalse` arrays
4. FE optionally auto-rewires: if A→B→C and B is deleted, wire A→C
5. Canvas re-renders
6. Changes are NOT persisted until user clicks "Save"
```

### Flow 5: Insert a Step Between Two Steps

```
1. User drags a new step type from palette onto an existing edge (A→C)
2. FE creates new step B with a UUID
3. FE rewires: A.next = ["B"], B.next = ["C"]  (was: A.next = ["C"])
4. Canvas re-renders showing A→B→C
5. User clicks the new node to configure it
6. User clicks "Save" to persist
```

### Flow 6: Schedule a Workflow

```
1. User clicks "Schedule" in toolbar
2. FE shows a scheduling dialog:
   - Cron expression builder (or presets: daily, weekly, monthly)
   - Schedule name
3. FE calls:
   ScheduleJob(
     jobName=["<name>"],
     jobGroup=["<project-uuid>"],
     cronExpression=["<cron>"],
     recipe=["RunWorkflow(project=[\"<project-uuid>\"], trigger=[\"schedule\"]);"],
     uiState=["<serialized-state>"]
   );
4. FE shows confirmation
```

---

## Important Implementation Notes

### 1. Whole-Document Save

The FE must always send the **entire** `workflow.json` on save. There is no partial/delta update API. This is intentional — it keeps the architecture simple, enables easy undo/redo (just keep a stack of JSON snapshots), and makes git diffs meaningful.

### 2. Backend Validates on Save

The `SaveWorkflow` reactor validates the DAG before writing. If the FE sends invalid JSON (dangling references, cycles, orphans), the save is **rejected** with a descriptive error. The FE can optionally do client-side validation for faster feedback, but the BE is the authoritative check.

### 3. FE Owns Step IDs

The FE generates `stepId` values (use `crypto.randomUUID()`). The BE never generates or modifies step IDs. When deleting a step, the FE is responsible for cleaning up all references to that step's ID in other steps' `next`/`ifTrue`/`ifFalse` arrays.

### 4. FE Owns Position

The `position` field (`{ x, y }`) is purely for the canvas layout. The BE stores it but never reads it. The FE should set positions when creating or moving nodes.

### 5. Undo / Redo

Since the workflow is a single JSON document, undo/redo is trivially implemented by maintaining a local history stack of JSON snapshots. Push a snapshot before each mutation, pop on undo.

### 6. Edge Rendering Rules

- **Normal steps** (all except CONDITION): render edges from `next` array. Single outgoing edge per target.
- **CONDITION steps**: render TWO sets of edges:
  - `ifTrue` edges → green, labeled "True"
  - `ifFalse` edges → red, labeled "False"
  - `next` should be `null` for CONDITION steps
- **Entry steps** (no incoming edges): render with a distinct style (e.g., rounded border, "START" label)
- **OUTPUT steps**: render with a distinct style (e.g., double border, "END" label)

### 7. Execution is Synchronous

`RunWorkflow` blocks until the workflow completes. For workflows expected to run >30s, consider:
- Running the Pixel call in a web worker or with a long timeout
- Showing a spinner with elapsed time
- In the future, we may add async execution with polling

### 8. Template Expression Autocomplete

When building the `{{...}}` autocomplete:
- Only show steps that are **upstream** of the current step (i.e., that will have executed before the current step runs)
- This requires traversing the DAG backwards from the current node
- Show the step name and type as context in the dropdown

### 9. Existing SEMOSS APIs You'll Need

| Purpose | How to call |
|---|---|
| List all projects | Existing project listing APIs, filter by `project_type == "WORKFLOW"` |
| Get model engines | Existing engine listing APIs, filter by model type |
| Get MCP tool engines | Existing engine listing APIs, filter by function type |
| Get tools for an engine | `GetMCPTools(engine=["<id>"])` or similar |
| Delete a project | Existing `DeleteProject` Pixel |
| Share a project | Existing project permission APIs |
| Schedule a job | `ScheduleJob(...)` — see Flow 6 |
| List scheduled jobs | `ListAllJobs(...)` or similar existing scheduler APIs |

---

## Example: Complete workflow.json

```json
{
  "workflowId": "proj-abc-123",
  "name": "Customer Sentiment Analysis",
  "version": 3,
  "variables": {
    "databaseId": "db-engine-uuid",
    "modelId": "gpt4-engine-uuid",
    "threshold": 0.7
  },
  "settings": {
    "maxSteps": 50,
    "timeoutMs": 300000,
    "onError": "stop"
  },
  "steps": [
    {
      "stepId": "fetch-reviews",
      "type": "RUN_PIXEL",
      "name": "Fetch Recent Reviews",
      "description": "Load last 100 customer reviews from database",
      "position": { "x": 100, "y": 300 },
      "config": {
        "recipe": "Database(\"{{variables.databaseId}}\") | Select(review_text, rating, date) | Sort(date, desc) | Collect(100);"
      },
      "inputs": {},
      "next": ["analyze-sentiment"],
      "ifTrue": null,
      "ifFalse": null
    },
    {
      "stepId": "analyze-sentiment",
      "type": "LLM_ASK",
      "name": "Analyze Sentiment",
      "description": "Use LLM to classify sentiment of reviews",
      "position": { "x": 400, "y": 300 },
      "config": {
        "modelId": "{{variables.modelId}}",
        "systemPrompt": "You are a sentiment analysis expert. Classify each review as positive, negative, or neutral. Return a JSON array.",
        "userPrompt": "Analyze the sentiment of these reviews:\n\n{{fetch-reviews.output}}"
      },
      "inputs": {},
      "next": ["check-negative-ratio"],
      "ifTrue": null,
      "ifFalse": null
    },
    {
      "stepId": "check-negative-ratio",
      "type": "CONDITION",
      "name": "High Negative Rate?",
      "description": "Check if more than 30% of reviews are negative",
      "position": { "x": 700, "y": 300 },
      "config": {
        "left": "{{analyze-sentiment.output}}",
        "operator": "contains",
        "right": "negative"
      },
      "inputs": {},
      "next": null,
      "ifTrue": ["alert-team"],
      "ifFalse": ["generate-report"]
    },
    {
      "stepId": "alert-team",
      "type": "RUN_TOOL",
      "name": "Send Alert",
      "description": "Notify the team about high negative sentiment",
      "position": { "x": 1000, "y": 150 },
      "config": {
        "engineId": "slack-mcp-engine-uuid",
        "toolName": "send_message",
        "params": {
          "channel": "#customer-alerts",
          "message": "⚠️ High negative sentiment detected in recent reviews."
        }
      },
      "inputs": {},
      "next": ["generate-report"],
      "ifTrue": null,
      "ifFalse": null
    },
    {
      "stepId": "generate-report",
      "type": "LLM_ASK",
      "name": "Generate Summary Report",
      "position": { "x": 1000, "y": 450 },
      "config": {
        "modelId": "{{variables.modelId}}",
        "systemPrompt": "You are a business analyst.",
        "userPrompt": "Create a concise executive summary of customer sentiment based on this analysis:\n\n{{analyze-sentiment.output}}"
      },
      "inputs": {},
      "next": ["final-output"],
      "ifTrue": null,
      "ifFalse": null
    },
    {
      "stepId": "final-output",
      "type": "OUTPUT",
      "name": "Report Output",
      "position": { "x": 1300, "y": 450 },
      "config": {
        "value": "{{generate-report.output}}"
      },
      "inputs": {},
      "next": null,
      "ifTrue": null,
      "ifFalse": null
    }
  ]
}
```

This workflow: fetches reviews → analyzes sentiment → branches on negative rate → optionally alerts → generates summary → outputs report.
