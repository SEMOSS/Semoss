# Workflow Engine: Feature Overview & Architecture

## Table of Contents

- [Overview](#overview)
- [Core Concept: Workflow-as-an-App](#core-concept-workflow-as-an-app)
- [Existing SEMOSS Building Blocks](#existing-semoss-building-blocks)
- [Project Structure](#project-structure)
- [Workflow Definition Schema](#workflow-definition-schema)
- [Step Types](#step-types)
- [Backend Architecture](#backend-architecture)
- [Frontend Architecture](#frontend-architecture)
- [Workflow Composability](#workflow-composability)
- [Security & Access Control](#security--access-control)
- [Scheduling & Triggers](#scheduling--triggers)
- [Execution Logging](#execution-logging)

---

## Overview

The Workflow Engine enables users to build **multi-step agent workflows** through a visual canvas UI. Each workflow is a **directed acyclic graph (DAG)** of steps — LLM calls, MCP tool executions, Pixel recipes, conditionals, loops, and more — that can be executed on demand, on a cron schedule, or via an event trigger.

The key architectural decision is that **a workflow is a SEMOSS project (app)**. This means every workflow automatically inherits the platform's identity, access control, file storage, versioning, scheduling, and MCP capabilities with zero new infrastructure.

---

## Core Concept: Workflow-as-an-App

In SEMOSS, a **project (app)** is the fundamental unit of:

| Concern | How Projects Handle It |
|---|---|
| **Identity** | UUID + name, registered in the catalog |
| **Access control** | `SecurityProjectUtils.userCanViewProject()`, `userCanEditProject()`, per-user/group permissions |
| **File storage** | `{project}/assets/` directory tree |
| **MCP** | `IProject extends IMCP` — the project itself is an MCP server |
| **Python runtime** | Can have its own `PyTranslator` |
| **Scheduling** | Quartz jobs use `jobGroup = projectId` |
| **Git sync** | Projects can be backed by a git repository |
| **Export/import** | Built-in project export mechanisms |
| **Audit** | `PipelineInvocationHandler` + engine audit logs |

Rather than building a parallel system for workflows, we introduce a new `PROJECT_TYPE.WORKFLOW`. The workflow definition lives in the project's assets folder, the canvas editor is the project's portal, and all existing project infrastructure applies directly.

**Users never need to think of it as an "app"** — the UI presents it as a workflow. But under the hood, it's a project, which means sharing, permissions, scheduling, and everything else just works.

---

## Existing SEMOSS Building Blocks

These are the existing platform capabilities that the workflow engine leverages:

### MCP (Model Context Protocol)

| File | Purpose |
|---|---|
| `src/prerna/engine/api/IMCP.java` | Interface: `initMCP()`, `getMCPTools()`, `callTool()` |
| `src/prerna/engine/impl/MCPFactory.java` | Factory: builds `IMCP` from an engine or project |
| `src/prerna/engine/impl/InternalMCP.java` | Implementation: reads tool definitions from `assets/mcp/py_mcp.json` and `pixel_mcp.json`, dispatches to Python or Pixel |
| `src/prerna/reactor/agent/mcp/MCPUtility.java` | Utilities: `runPythonTool()`, `runPixelTool()`, tool name management, execution modes |
| `src/prerna/reactor/agent/mcp/RunMCPToolReactor.java` | Reactor: standalone MCP tool execution |

**How it works:** Tools are defined declaratively in JSON files (`py_mcp.json` / `pixel_mcp.json`) in the engine's assets folder. Each tool has a name, description, and `inputSchema`. When invoked, `InternalMCP.callTool()` looks up the tool definition, then delegates to `MCPUtility.runPythonTool()` (which loads `mcp_driver.py` and calls the function) or `MCPUtility.runPixelTool()` (which builds and runs a Pixel expression).

**Execution modes** (`MCPUtility.MCPExecution`):
- `AUTO` — tool runs without user confirmation
- `ASK` — user must approve
- `DISABLED` — tool is hidden from selection

### LLM Conversation & Agent Loop

| File | Purpose |
|---|---|
| `src/prerna/playground/reactors/AskPlaygroundReactor.java` | Sends a prompt to a model, returns input + response messages |
| `src/prerna/playground/reactors/AddPlaygroundToolExecutionReactor.java` | Feeds tool execution results back into the Room and re-invokes the LLM |
| `src/prerna/engine/impl/model/Room.java` | Manages conversation message history, tool call tracking, `ask()` and `addToolExecutionResult()` |
| `src/prerna/engine/impl/model/AbstractModelEngine.java` | `askRoom()` — invokes the model with conversation context |

**Agent loop flow:**
```
User prompt → Room.ask() → LLM responds
    ├─ RESPONSE_TEXT → done, return to user
    └─ RESPONSE_TOOL → for each tool_call:
         ├─ Execute tool (RunMCPToolReactor / IMCP.callTool)
         ├─ Room.addToolExecutionResult()
         ├─ All tools answered? → Re-invoke LLM → loop
         └─ More tools pending → wait
```

### Quartz Scheduler

| File | Purpose |
|---|---|
| `src/prerna/reactor/scheduler/ScheduleJobReactor.java` | Creates Quartz cron jobs that execute Pixel recipes |
| `src/prerna/reactor/scheduler/ExecuteScheduledJobReactor.java` | Manually triggers a scheduled job |
| `src/prerna/rpa/quartz/jobs/insight/RunPixelJobFromDB.java` | The Quartz `Job` class — makes an HTTP POST to the SEMOSS API to run the Pixel |
| `src/prerna/reactor/scheduler/SchedulerDatabaseUtility.java` | DB operations for job storage, history, execution tracking |
| `src/prerna/reactor/scheduler/SchedulerFactorySingleton.java` | Singleton Quartz scheduler instance |

**How it works:** `ScheduleJobReactor` takes a `recipe` (Pixel expression), `cronExpression`, `jobGroup` (= project ID), and user credentials. It creates a Quartz `JobDetail` with a `CronTrigger`. When the trigger fires, `RunPixelJobFromDB` makes an HTTP POST to `/api/schedule/executePixel` with the stored Pixel recipe.

### Pipeline / Interceptor System

| File | Purpose |
|---|---|
| `src/prerna/engine/impl/pipeline/PipelineInvocationHandler.java` | Dynamic proxy that intercepts engine method calls, runs input/output pipeline stages |
| `src/prerna/reactor/interceptor/PipelineReactorUtils.java` | Constants for pipeline data passing |
| `src/prerna/reactor/interceptor/GenericGuardrailInputReactor.java` | Guardrail interceptor for input validation |

### Control Flow

| File | Purpose |
|---|---|
| `src/prerna/reactor/IfReactor.java` | `IF(condition, trueCase, falseCase)` — conditional branching in Pixel |

### Project Management

| File | Purpose |
|---|---|
| `src/prerna/project/api/IProject.java` | Interface: `PROJECT_TYPE` enum, `IMCP`, file paths |
| `src/prerna/project/impl/Project.java` | Implementation |
| `src/prerna/reactor/project/CreateProjectReactor.java` | Creates a new project with type, portal, git config |
| `src/prerna/reactor/project/GetAppBlocksJsonReactor.java` | Reads `blocks.json` from project portal folder |
| `src/prerna/reactor/project/SaveAppBlocksJsonReactor.java` | Writes `blocks.json` |

---

## Project Structure

### Existing app structure (for reference)

```
{projectId}/
  assets/
    mcp/
      py_mcp.json             ← MCP tool definitions (Python)
      pixel_mcp.json          ← MCP tool definitions (Pixel)
    py/
      mcp_driver.py           ← Python tool implementations
  portals/
    blocks.json               ← UI blocks definition (BLOCKS-type projects)
  version/
```

### Workflow project structure

```
{projectId}/                              ← PROJECT_TYPE.WORKFLOW
  assets/
    mcp/
      py_mcp.json                         ← Tools this workflow exposes as an MCP server (optional)
      pixel_mcp.json
    py/
      mcp_driver.py                       ← Python implementations for exposed tools
    workflow/
      workflow.json                       ← THE workflow definition (steps, edges, variables, trigger)
      executions/
        {executionId}.json                ← Per-run execution logs
  portals/
    ...                                   ← Canvas editor UI (react-flow based)
```

The workflow definition at `assets/workflow/workflow.json` is the single source of truth. The canvas UI reads and writes this file via the existing `GetAppAssetsReactor` / `SaveAppAssetsReactor` APIs — no new CRUD endpoints needed.

---

## Workflow Definition Schema

```json
{
  "workflowId": "{{projectId}}",
  "name": "Weekly Report Agent",
  "version": 1,

  "steps": [
    {
      "stepId": "step-1",
      "type": "LLM_ASK",
      "name": "Generate SQL query",
      "description": "Asks GPT-4 to write a SQL query for last week's sales",
      "position": { "x": 100, "y": 200 },
      "config": {
        "modelId": "gpt-4-engine-uuid",
        "systemPrompt": "You are a SQL expert...",
        "userPrompt": "Write a query to get last week's sales from {{variables.database}}"
      },
      "inputs": {},
      "next": ["step-2"]
    },
    {
      "stepId": "step-2",
      "type": "RUN_TOOL",
      "name": "Execute the query",
      "position": { "x": 300, "y": 200 },
      "config": {
        "engineId": "db-engine-uuid",
        "toolName": "run_query",
        "params": {
          "query": "{{step-1.output}}"
        }
      },
      "next": ["step-3"]
    },
    {
      "stepId": "step-3",
      "type": "CONDITION",
      "name": "Has results?",
      "position": { "x": 500, "y": 200 },
      "config": {
        "expression": "{{step-2.rowCount}} > 0"
      },
      "ifTrue": ["step-4"],
      "ifFalse": ["step-5"]
    },
    {
      "stepId": "step-4",
      "type": "LLM_ASK",
      "name": "Summarize results",
      "position": { "x": 700, "y": 100 },
      "config": {
        "modelId": "gpt-4-engine-uuid",
        "userPrompt": "Summarize this data: {{step-2.output}}"
      },
      "next": ["step-6"]
    },
    {
      "stepId": "step-5",
      "type": "STATIC",
      "name": "No data message",
      "position": { "x": 700, "y": 300 },
      "config": {
        "value": "No sales data found for last week."
      },
      "next": ["step-6"]
    },
    {
      "stepId": "step-6",
      "type": "OUTPUT",
      "name": "Send email",
      "position": { "x": 900, "y": 200 },
      "config": {
        "action": "email",
        "to": ["team@company.com"],
        "subject": "Weekly Sales Report",
        "body": "{{step-4.output || step-5.output}}"
      }
    }
  ],

  "variables": {
    "database": {
      "type": "string",
      "default": "sales-db-engine-uuid",
      "description": "The database engine to query"
    }
  },

  "trigger": {
    "type": "cron",
    "expression": "0 0 9 ? * MON",
    "timezone": "America/New_York"
  },

  "settings": {
    "maxSteps": 50,
    "timeoutMs": 300000,
    "onError": "stop",
    "retryPolicy": {
      "maxRetries": 0,
      "backoffMs": 1000
    }
  }
}
```

### Field Reference

| Field | Type | Description |
|---|---|---|
| `workflowId` | string | Matches the project ID |
| `version` | number | Schema version for migration support |
| `steps` | array | Ordered list of step definitions |
| `steps[].stepId` | string | Unique identifier within the workflow |
| `steps[].type` | enum | Step type (see [Step Types](#step-types)) |
| `steps[].position` | object | Canvas coordinates — ignored by backend |
| `steps[].config` | object | Type-specific configuration |
| `steps[].next` | array | Step IDs to execute after this step completes |
| `steps[].ifTrue` / `ifFalse` | array | Branch targets (CONDITION type only) |
| `variables` | object | Workflow-level variables with types and defaults |
| `trigger` | object | How/when the workflow is invoked |
| `settings` | object | Execution limits and error handling |

### Template Syntax

Step configs support `{{...}}` template expressions:
- `{{variables.name}}` — workflow variable
- `{{step-1.output}}` — full output of a prior step
- `{{step-1.output.fieldName}}` — nested field access
- `{{step-1.rowCount}}` — metadata from a prior step result

---

## Step Types

These are the node types available in the canvas palette:

| Type | Description | Backend Handler | Backed By |
|---|---|---|---|
| `LLM_ASK` | Send a prompt to a model, get text back | `LLMAskStepHandler` | `Room.ask()` / `AskPlaygroundReactor` logic |
| `LLM_AGENT` | Full agent loop — LLM + auto tool execution until text response | `LLMAgentStepHandler` | `Room.ask()` + `IMCP.callTool()` + `Room.addToolExecutionResult()` in a loop |
| `RUN_TOOL` | Execute a single MCP tool directly | `RunToolStepHandler` | `MCPFactory.build(engine).callTool()` |
| `RUN_PIXEL` | Run an arbitrary Pixel expression | `RunPixelStepHandler` | `PixelRunner` |
| `RUN_PYTHON` | Run a Python script | `RunPythonStepHandler` | `PyTranslator` |
| `QUERY_DB` | Query a database engine | `QueryDbStepHandler` | Existing database reactors |
| `CONDITION` | Branch based on an expression (if/else) | `ConditionStepHandler` | Expression evaluation (mirrors `IfReactor`) |
| `LOOP` | Iterate over a list, run sub-steps per item | `LoopStepHandler` | New — iterates with context injection |
| `TRANSFORM` | Map/filter/reshape data between steps | `TransformStepHandler` | JSON path / expression-based |
| `STATIC` | Return a static value | `StaticStepHandler` | Direct value pass-through |
| `HUMAN_INPUT` | Pause and wait for user input (non-auto workflows) | `HumanInputStepHandler` | New — suspends execution |
| `GUARDRAIL` | Run a guardrail check on flowing data | `GuardrailStepHandler` | `GenericGuardrailInputReactor` |
| `OUTPUT` | Terminal node — email, store, webhook, return | `OutputStepHandler` | Existing email / HTTP reactors |

### Step Type: `LLM_ASK`

Sends a single prompt to a model and returns the text response. Does NOT handle tool calls — if the model returns a tool_call, it's passed through as output (use `LLM_AGENT` for auto tool handling).

```json
{
  "type": "LLM_ASK",
  "config": {
    "modelId": "engine-uuid",
    "roomId": "optional-room-uuid",
    "systemPrompt": "You are a helpful assistant.",
    "userPrompt": "Summarize: {{step-1.output}}",
    "paramMap": {
      "temperature": 0.7,
      "max_tokens": 2000
    }
  }
}
```

### Step Type: `LLM_AGENT`

Full agentic loop. Sends a prompt to a model with MCP tools attached. If the model requests tool calls, executes them automatically and feeds results back until the model returns a text response or `maxIterations` is reached.

```json
{
  "type": "LLM_AGENT",
  "config": {
    "modelId": "engine-uuid",
    "roomId": "optional-room-uuid",
    "systemPrompt": "You are a data analyst with access to databases.",
    "userPrompt": "Find last week's top customers.",
    "toolEngineIds": ["db-engine-uuid", "api-engine-uuid"],
    "maxIterations": 10,
    "mcpExecution": "auto"
  }
}
```

### Step Type: `RUN_TOOL`

Directly executes a single MCP tool without involving an LLM.

```json
{
  "type": "RUN_TOOL",
  "config": {
    "engineId": "engine-uuid",
    "toolName": "run_query",
    "params": {
      "query": "SELECT * FROM sales WHERE date > '2026-02-09'"
    }
  }
}
```

### Step Type: `CONDITION`

Evaluates an expression and routes to different branches.

```json
{
  "type": "CONDITION",
  "config": {
    "expression": "{{step-2.rowCount}} > 0"
  },
  "ifTrue": ["step-4"],
  "ifFalse": ["step-5"]
}
```

### Step Type: `OUTPUT`

Terminal step that routes the final result to an external destination.

```json
{
  "type": "OUTPUT",
  "config": {
    "action": "email",
    "to": ["user@example.com"],
    "subject": "Report: {{variables.reportName}}",
    "body": "{{step-4.output}}"
  }
}
```

Supported actions: `email`, `webhook`, `store_db`, `write_file`, `return` (default — just returns the value).

---

## Backend Architecture

### Core Classes

```
src/prerna/
├── project/api/
│   └── IProject.java                          ← Add WORKFLOW to PROJECT_TYPE enum
│
├── workflow/
│   ├── WorkflowDefinition.java                ← POJO: parsed workflow.json
│   ├── WorkflowStep.java                      ← POJO: single step definition
│   ├── WorkflowContext.java                    ← Runtime state: variable values, step results
│   ├── WorkflowExecutionResult.java            ← Final result: status, outputs, logs
│   ├── WorkflowExecutor.java                   ← Core engine: walks DAG, dispatches steps
│   ├── WorkflowTemplateEngine.java             ← Resolves {{...}} expressions
│   │
│   └── handlers/                               ← One handler per step type
│       ├── IWorkflowStepHandler.java           ← Interface
│       ├── LLMAskStepHandler.java
│       ├── LLMAgentStepHandler.java
│       ├── RunToolStepHandler.java
│       ├── RunPixelStepHandler.java
│       ├── ConditionStepHandler.java
│       ├── LoopStepHandler.java
│       ├── TransformStepHandler.java
│       ├── StaticStepHandler.java
│       ├── OutputStepHandler.java
│       └── GuardrailStepHandler.java
│
├── reactor/workflow/
│   └── RunWorkflowReactor.java                 ← Pixel: RunWorkflow(project=["uuid"])
```

### WorkflowExecutor Flow

```
RunWorkflowReactor.execute()
  │
  ├─ Load IProject (security check)
  ├─ Read assets/workflow/workflow.json
  ├─ Parse → WorkflowDefinition
  ├─ Merge variable overrides
  │
  ├─ Create WorkflowContext (variables, step results map)
  │
  ├─ Find entry steps (no incoming edges)
  ├─ For each step in execution order:
  │    ├─ Resolve {{...}} templates in step config
  │    ├─ Lookup handler by step.type
  │    ├─ handler.execute(config, context, insight)
  │    ├─ Store StepResult in context
  │    ├─ If CONDITION → follow ifTrue or ifFalse branch
  │    ├─ If LOOP → iterate, execute sub-steps per item
  │    ├─ Check maxSteps / timeout
  │    └─ On error → stop or skip based on settings.onError
  │
  ├─ Save execution log to assets/workflow/executions/
  └─ Return WorkflowExecutionResult
```

### IWorkflowStepHandler Interface

```java
public interface IWorkflowStepHandler {

    /**
     * Execute a single workflow step.
     *
     * @param stepId   The unique step identifier
     * @param config   The step's config object (type-specific)
     * @param context  The workflow runtime context (variables, prior results)
     * @param insight  The SEMOSS insight for the execution
     * @return The result of this step
     */
    StepResult execute(String stepId, Map<String, Object> config,
                       WorkflowContext context, Insight insight);
}
```

### StepResult

```java
public class StepResult {
    private String stepId;
    private String status;          // "success", "error", "skipped"
    private Object output;          // The primary output value
    private Map<String, Object> metadata;  // rowCount, duration, etc.
    private String error;           // Error message if failed
    private long durationMs;
}
```

---

## Frontend Architecture

### Views

| View | Description |
|---|---|
| **Workflow List** | Filtered project list (`projectType = WORKFLOW`). Shows name, last run status, schedule, actions (edit/run/delete). |
| **Canvas Editor** | React-flow based DAG editor. Reads/writes `workflow.json` via asset APIs. |
| **Run History** | Table of past executions with status, duration, trigger type. Click to see step-level details. |
| **Run Detail** | Step-by-step execution view — each node colored by status, click to see input/output. |

### Canvas Editor Components

```
┌──────────────────────────────────────────────────────────────┐
│  [Save] [Run Now] [Schedule]              Workflow Settings ▼│
├──────────┬───────────────────────────────────┬───────────────┤
│          │                                   │               │
│  Step    │         Canvas Area               │   Config      │
│  Palette │                                   │   Panel       │
│          │    ┌─────┐    ┌─────┐    ┌─────┐  │               │
│  LLM Ask │    │Step1├───►│Step2├───►│Step3│  │  Model: ▼     │
│  Agent   │    └─────┘    └─────┘    └──┬──┘  │  Prompt: ___  │
│  Tool    │                          ┌──┴──┐  │  Temp:  0.7   │
│  Pixel   │                     ┌────┤ If  │  │               │
│  Python  │                     │    └──┬──┘  │               │
│  Branch  │                ┌────▼┐  ┌───▼──┐  │               │
│  Loop    │                │Yes  │  │ No   │  │               │
│  Output  │                └─────┘  └──────┘  │               │
│          │                                   │               │
├──────────┴───────────────────────────────────┴───────────────┤
│  Variables: database = [sales-db ▼]   reportName = [______]  │
├──────────────────────────────────────────────────────────────┤
│  Run History   │ #1 ✅ 2m3s  │ #2 ❌ 45s  │ #3 ✅ 1m52s    │
└──────────────────────────────────────────────────────────────┘
```

### API Calls (All Existing)

| Action | API | Notes |
|---|---|---|
| Create workflow | `CreateProject(type=["WORKFLOW"])` | Existing reactor |
| Load workflow.json | `GetAppAssets` or direct file read | Existing reactor |
| Save workflow.json | `SaveAppAssets` | Existing reactor |
| Run workflow | `RunWorkflow(project=["uuid"])` | New reactor |
| Schedule workflow | `ScheduleJob(recipe=["RunWorkflow(...)"], ...)` | Existing reactor |
| List workflows | `MyProjects` with type filter | Existing reactor |
| Share | `GrantProjectPermission` / `SetProjectGlobal` | Existing reactors |

---

## Workflow Composability

Since `IProject extends IMCP`, a workflow project can expose the **entire workflow as an MCP tool**. This enables:

### Workflow-as-a-Tool

A workflow project's `py_mcp.json` can define:

```json
{
  "tools": [{
    "name": "run_weekly_report",
    "description": "Runs the weekly sales report workflow and returns the summary",
    "inputSchema": {
      "type": "object",
      "properties": {
        "database": {
          "type": "string",
          "description": "The database engine ID to query"
        }
      },
      "required": ["database"]
    },
    "_meta": {
      "SMSS_FUNCTION_NAME": "run_weekly_report",
      "SMSS_MCP_EXECUTION": "auto"
    }
  }]
}
```

The implementation (in `mcp_driver.py` or as a Pixel tool) simply calls:
```
RunWorkflow(project=["this-project-uuid"], variables=[{"database": inputDatabase}]);
```

### Nesting

This means:
- **Workflow A** can have an `LLM_AGENT` step with Workflow B's project armed as a tool engine
- The LLM in Workflow A can decide to call Workflow B as a tool
- Workflow B executes its full step graph and returns the result
- Workflow A continues with the result

This provides **hierarchical composition** — complex workflows built from simpler, reusable sub-workflows.

---

## Security & Access Control

All handled by existing project security — no new code needed:

| Operation | Permission Required | Check |
|---|---|---|
| View workflow | Project viewer | `SecurityProjectUtils.userCanViewProject()` |
| Edit workflow (canvas) | Project editor | `SecurityProjectUtils.userCanEditProject()` |
| Run workflow | Project viewer | `SecurityProjectUtils.userCanViewProject()` |
| Schedule workflow | Project editor + admin OR project editor | `SecurityAdminUtils.userIsAdmin()` or `SecurityProjectUtils.userCanEditProject()` |
| Delete workflow | Project owner | Project ownership check |
| Share workflow | Project owner | Project ownership check |

**Cross-engine access:** When a workflow step accesses another engine (e.g., `RUN_TOOL` against a database engine), the executing user must also have access to that engine. This is already enforced by `checkEngineEditSecurity()` / `SecurityEngineUtils.userCanViewEngine()` in the existing reactors.

---

## Scheduling & Triggers

### Cron Schedule

Uses the existing Quartz scheduler. The scheduled Pixel recipe is simply:

```
RunWorkflow(project=["workflow-project-uuid"]);
```

Configured via `ScheduleJobReactor` with `jobGroup = projectId`. All existing scheduling features apply:
- Cron expression + timezone
- Trigger on load
- Pause / resume
- Execution history

### Webhook Trigger (Future)

A new REST endpoint:

```
POST /api/workflow/trigger/{projectId}
Body: { "variables": { "database": "other-db-uuid" } }
```

- Validates user access
- Calls `RunWorkflowReactor` with the provided variable overrides
- Returns the execution result (sync) or execution ID (async)

### Event Triggers (Future)

Hook into SEMOSS internal events:
- Database row insert → trigger workflow
- File upload to project → trigger workflow
- Another scheduled job completing → chain to workflow
- Implemented via Quartz `JobListener` or internal pub/sub

---

## Execution Logging

Each workflow run produces an execution log saved to `assets/workflow/executions/`:

```json
{
  "executionId": "uuid",
  "workflowId": "project-uuid",
  "startTime": "2026-02-16T09:00:00Z",
  "endTime": "2026-02-16T09:02:03Z",
  "status": "success",
  "triggeredBy": "cron",
  "userId": "user-id",
  "variables": { "database": "sales-db-uuid" },
  "steps": [
    {
      "stepId": "step-1",
      "name": "Generate SQL query",
      "type": "LLM_ASK",
      "status": "success",
      "startTime": "2026-02-16T09:00:00Z",
      "endTime": "2026-02-16T09:00:12Z",
      "durationMs": 12000,
      "output": "SELECT customer_name, SUM(amount) ..."
    },
    {
      "stepId": "step-2",
      "name": "Execute the query",
      "type": "RUN_TOOL",
      "status": "success",
      "durationMs": 3400,
      "output": { "rows": [...], "rowCount": 42 }
    }
  ]
}
```

The execution log is stored as a file in the project's assets folder, meaning:
- It's included in project exports
- It's versioned if the project uses git
- Access is controlled by project permissions
- It can be queried via asset browsing APIs
