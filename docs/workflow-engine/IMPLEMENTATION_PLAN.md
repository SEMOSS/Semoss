# Workflow Engine: Implementation Plan

> **Status:** Not Started
> **Last Updated:** 2026-02-16
> **Feature Doc:** [FEATURE_OVERVIEW.md](./FEATURE_OVERVIEW.md)

This document tracks all implementation tasks for the Workflow Engine feature. Each task has a status, description, and relevant file paths. Update the status as work progresses.

**Status Legend:**
- ⬜ Not Started
- 🔲 In Progress
- ✅ Completed
- ⏸️ Blocked

---

## Phase 1: Foundation — Project Type & Data Model

> Goal: Establish WORKFLOW as a project type and define the core data model classes.

### Task 1.1 — Add `WORKFLOW` to `PROJECT_TYPE` enum

- **Status:** ✅
- **File:** `src/prerna/project/api/IProject.java`
- **Description:** Add `WORKFLOW` to the `PROJECT_TYPE` enum alongside `BLOCKS`, `CODE`, `WORKSPACE`, `INSIGHTS`.
- **Changes:**
  ```java
  enum PROJECT_TYPE {
      BLOCKS, CODE, WORKSPACE, INSIGHTS, WORKFLOW
  }
  ```
- **Validation:** Ensure no existing code breaks — search for `PROJECT_TYPE.values()` or switch statements on `PROJECT_TYPE`.
- **Notes:** Also define the workflow folder constant:
  ```java
  String WORKFLOW_FOLDER = "workflow";
  String WORKFLOW_FILE_NAME = "workflow.json";
  ```

### Task 1.2 — Update `CreateProjectReactor` to handle WORKFLOW type

- **Status:** ✅
- **File:** `src/prerna/reactor/project/CreateProjectReactor.java`
- **Description:** When `projectType = WORKFLOW`:
  - Set `hasPortal = true` (the canvas editor)
  - Auto-create the `assets/workflow/` directory
  - Create a default empty `workflow.json` scaffold:
    ```json
    {
      "workflowId": "{{projectId}}",
      "name": "{{projectName}}",
      "version": 1,
      "steps": [],
      "variables": {},
      "trigger": null,
      "settings": {
        "maxSteps": 50,
        "timeoutMs": 300000,
        "onError": "stop"
      }
    }
    ```
- **Validation:** Create a WORKFLOW project and verify folder structure is correct.

### Task 1.3 — Create `WorkflowDefinition` POJO

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowDefinition.java` (new)
- **Description:** Java class representing the parsed `workflow.json`. Fields:
  - `String workflowId`
  - `String name`
  - `int version`
  - `List<WorkflowStep> steps`
  - `Map<String, WorkflowVariable> variables`
  - `WorkflowTrigger trigger`
  - `WorkflowSettings settings`
- **Notes:** Use Gson-friendly structure. Include a static `parse(String json)` factory method.

### Task 1.4 — Create `WorkflowStep` POJO

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowStep.java` (new)
- **Description:** Represents a single step node. Fields:
  - `String stepId`
  - `String type` (enum: `LLM_ASK`, `LLM_AGENT`, `RUN_TOOL`, `RUN_PIXEL`, `RUN_PYTHON`, `CONDITION`, `LOOP`, `TRANSFORM`, `STATIC`, `HUMAN_INPUT`, `GUARDRAIL`, `OUTPUT`)
  - `String name`
  - `String description`
  - `Map<String, Object> position` (UI-only, ignored by backend)
  - `Map<String, Object> config` (type-specific)
  - `Map<String, String> inputs` (template expressions mapping)
  - `List<String> next`
  - `List<String> ifTrue` (CONDITION only)
  - `List<String> ifFalse` (CONDITION only)

### Task 1.5 — Create `WorkflowContext` runtime class

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowContext.java` (new)
- **Description:** Holds runtime state during execution:
  - `Map<String, Object> variables` — merged variable values
  - `Map<String, StepResult> stepResults` — results keyed by stepId
  - `List<StepExecutionLog> executionLog` — ordered log of step executions
  - `long startTimeMs`
  - `int stepsExecuted` — counter for maxSteps enforcement
  - Methods: `putResult(stepId, result)`, `getResult(stepId)`, `getVariable(name)`, `resolveTemplate(String template)`

### Task 1.6 — Create `StepResult` POJO

- **Status:** ✅
- **File:** `src/prerna/workflow/StepResult.java` (new)
- **Description:** Result of a single step execution:
  - `String stepId`
  - `String status` — `"success"`, `"error"`, `"skipped"`
  - `Object output` — the primary output value
  - `Map<String, Object> metadata` — additional data (rowCount, model used, etc.)
  - `String error` — error message if failed
  - `long durationMs`

### Task 1.7 — Create `WorkflowExecutionResult` POJO

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowExecutionResult.java` (new)
- **Description:** Final result of a workflow run:
  - `String executionId`
  - `String workflowId`
  - `String status` — `"success"`, `"error"`, `"timeout"`
  - `long startTimeMs`, `endTimeMs`, `durationMs`
  - `String triggeredBy` — `"manual"`, `"cron"`, `"webhook"`, `"api"`
  - `Map<String, StepResult> stepResults`
  - `Object finalOutput` — output of the last step

---

## Phase 2: Template Engine

> Goal: Implement the `{{...}}` template resolution system for step configs.

### Task 2.1 — Create `WorkflowTemplateEngine`

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowTemplateEngine.java` (new)
- **Description:** Resolves `{{...}}` expressions in step config strings. Supported patterns:
  - `{{variables.name}}` → looks up in `WorkflowContext.variables`
  - `{{step-1.output}}` → looks up `StepResult.output` for step-1
  - `{{step-1.output.fieldName}}` → nested field access (dot-notation JSON path)
  - `{{step-1.metadata.rowCount}}` → metadata access
  - `{{step-1.output || step-2.output}}` → fallback/coalesce
- **Implementation approach:**
  - Use regex to find `\{\{(.+?)\}\}` patterns
  - Parse the expression inside
  - Look up values in `WorkflowContext`
  - Support recursive resolution for maps (walk all string values in a config map)
- **Method signatures:**
  ```java
  public static String resolve(String template, WorkflowContext context);
  public static Map<String, Object> resolveMap(Map<String, Object> configMap, WorkflowContext context);
  ```

---

## Phase 3: Step Handlers

> Goal: Implement handlers for each step type. Start with the most critical ones.

### Task 3.1 — Create `IWorkflowStepHandler` interface

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/IWorkflowStepHandler.java` (new)
- **Description:**
  ```java
  public interface IWorkflowStepHandler {
      StepResult execute(String stepId, Map<String, Object> config,
                         WorkflowContext context, Insight insight);
  }
  ```

### Task 3.2 — Create `WorkflowStepHandlerRegistry`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/WorkflowStepHandlerRegistry.java` (new)
- **Description:** Maps step type strings to handler instances:
  ```java
  public class WorkflowStepHandlerRegistry {
      private static final Map<String, IWorkflowStepHandler> HANDLERS = new HashMap<>();
      static {
          HANDLERS.put("LLM_ASK", new LLMAskStepHandler());
          HANDLERS.put("RUN_TOOL", new RunToolStepHandler());
          HANDLERS.put("RUN_PIXEL", new RunPixelStepHandler());
          HANDLERS.put("CONDITION", new ConditionStepHandler());
          HANDLERS.put("STATIC", new StaticStepHandler());
          HANDLERS.put("OUTPUT", new OutputStepHandler());
          // ... etc
      }
      public static IWorkflowStepHandler getHandler(String type) { ... }
  }
  ```

### Task 3.3 — Implement `StaticStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/StaticStepHandler.java` (new)
- **Description:** Simplest handler — returns `config.value` as the output. Good starting point to validate the pipeline.

### Task 3.4 — Implement `RunPixelStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/RunPixelStepHandler.java` (new)
- **Description:**
  - Reads `config.recipe` (Pixel expression string)
  - Resolves templates in the recipe
  - Runs via `PixelRunner` (same as `RunPixelJobFromDB`)
  - Captures output as `StepResult.output`
- **Reference:** `src/prerna/rpa/quartz/jobs/insight/RunPixelJobFromDB.java`

### Task 3.5 — Implement `RunToolStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/RunToolStepHandler.java` (new)
- **Description:**
  - Reads `config.engineId`, `config.toolName`, `config.params`
  - Resolves templates in params
  - Calls `MCPFactory.build(engine).callTool(toolName, params, insight)`
  - Returns tool output as `StepResult.output`
- **Reference:** `src/prerna/reactor/agent/mcp/RunMCPToolReactor.java`, `src/prerna/engine/impl/InternalMCP.java`

### Task 3.6 — Implement `LLMAskStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/LLMAskStepHandler.java` (new)
- **Description:**
  - Reads `config.modelId`, `config.systemPrompt`, `config.userPrompt`, `config.paramMap`
  - Creates or reuses a `Room`
  - Builds an `InputMessage`
  - Calls `room.ask(msg, modelEngine, null)`
  - Returns the response text as `StepResult.output`
- **Reference:** `src/prerna/playground/reactors/AskPlaygroundReactor.java`

### Task 3.7 — Implement `ConditionStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/ConditionStepHandler.java` (new)
- **Description:**
  - Reads `config.expression` (e.g., `"42 > 0"` after template resolution)
  - Evaluates the expression to a boolean
  - Returns `StepResult` with `metadata.branch = "ifTrue"` or `"ifFalse"`
  - The `WorkflowExecutor` uses this to determine which `next` steps to follow
- **Expression evaluation:** Can start simple with basic comparisons (`>`, `<`, `==`, `!=`, `contains`), expand later.

### Task 3.8 — Implement `LLMAgentStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/LLMAgentStepHandler.java` (new)
- **Description:** Full agentic loop:
  1. Load model engine and tool engines from config
  2. Gather MCP tool definitions from all specified tool engines
  3. Create a Room, build InputMessage with tools attached
  4. Call `room.ask()`
  5. If response is `RESPONSE_TOOL`:
     - For each tool call: `MCPFactory.build(engine).callTool()`
     - `room.addToolExecutionResult()`
     - Loop until `RESPONSE_TEXT` or `maxIterations`
  6. Return final text as `StepResult.output`
- **Reference:** `src/prerna/playground/reactors/AskPlaygroundReactor.java`, `src/prerna/playground/reactors/AddPlaygroundToolExecutionReactor.java`
- **Notes:** This is the most complex handler. Consider implementing after the simpler ones are working.

### Task 3.9 — Implement `OutputStepHandler`

- **Status:** ✅
- **File:** `src/prerna/workflow/handlers/OutputStepHandler.java` (new)
- **Description:** Routes the final result based on `config.action`:
  - `return` — just passes through (default)
  - `email` — sends email via existing email infrastructure
  - `webhook` — HTTP POST to a URL
  - `write_file` — writes to the project's assets folder
  - `store_db` — inserts into a database engine
- **Notes:** Start with `return` only. Add other actions incrementally.

### Task 3.10 — Implement `LoopStepHandler` (Deferred)

- **Status:** ⬜
- **File:** `src/prerna/workflow/handlers/LoopStepHandler.java` (new)
- **Description:**
  - Reads `config.items` (template expression resolving to a list)
  - Reads `config.subSteps` (step IDs to execute per item)
  - For each item: injects `{{loop.item}}`, `{{loop.index}}` into context, executes sub-steps
  - Collects results into a list
- **Notes:** Defer until basic linear + branching workflows work.

### Task 3.11 — Implement `RunPythonStepHandler` (Deferred)

- **Status:** ⬜
- **File:** `src/prerna/workflow/handlers/RunPythonStepHandler.java` (new)
- **Description:**
  - Reads `config.script` (Python code string) or `config.file` (path in assets)
  - Executes via `PyTranslator`
  - Returns output
- **Notes:** Defer until core steps work.

### Task 3.12 — Implement `GuardrailStepHandler` (Deferred)

- **Status:** ⬜
- **File:** `src/prerna/workflow/handlers/GuardrailStepHandler.java` (new)
- **Description:**
  - Runs a guardrail check on the input data
  - If passes, continues; if fails, routes to error handling
- **Reference:** `src/prerna/reactor/interceptor/GenericGuardrailInputReactor.java`
- **Notes:** Defer.

---

## Phase 4: Workflow Executor

> Goal: Build the core engine that walks the DAG and orchestrates step execution.

### Task 4.1 — Implement `WorkflowExecutor`

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowExecutor.java` (new)
- **Description:** The orchestrator:
  ```java
  public class WorkflowExecutor {
      public WorkflowExecutionResult execute(
          IProject project,
          Map<String, Object> variableOverrides,
          Insight insight,
          String triggeredBy
      ) { ... }
  }
  ```
- **Core logic:**
  1. Read `workflow.json` from project assets
  2. Parse into `WorkflowDefinition`
  3. Merge `variableOverrides` with definition defaults
  4. Build adjacency graph from steps' `next`/`ifTrue`/`ifFalse` edges
  5. Find entry steps (steps with no incoming edges)
  6. Execute steps using a queue/stack:
     - Pop next step
     - Resolve templates in `step.config`
     - Get handler from registry
     - Execute handler
     - Store result
     - If CONDITION: enqueue the appropriate branch
     - Else: enqueue `step.next` steps
  7. Enforce `maxSteps` and `timeoutMs`
  8. Handle errors based on `settings.onError`
  9. Build and return `WorkflowExecutionResult`
- **Edge cases:**
  - Steps with multiple incoming edges (join/merge) — execute when ALL predecessors complete
  - Steps with no next — terminal nodes
  - Circular reference detection (validate at parse time)

### Task 4.2 — Add DAG validation

- **Status:** ✅
- **File:** `src/prerna/workflow/WorkflowDefinition.java`
- **Description:** Add a `validate()` method that checks:
  - No circular references (cycle detection via DFS)
  - All `next`/`ifTrue`/`ifFalse` references point to existing step IDs
  - At least one step exists
  - At least one entry step (no incoming edges)
  - No orphaned steps (unreachable from any entry)
  - Required config fields present for each step type

---

## Phase 5: Reactor & API Layer

> Goal: Expose the workflow executor via Pixel and REST.

### Task 5.1 — Create `RunWorkflowReactor`

- **Status:** ✅
- **File:** `src/prerna/reactor/workflow/RunWorkflowReactor.java` (new)
- **Description:**
  - **Pixel:** `RunWorkflow(project=["workflow-project-uuid"], variables=[{"key": "value"}]);`
  - **Keys:** `project` (required), `variables` (optional map)
  - **Logic:**
    1. Load `IProject` by ID
    2. Security check: `userCanViewProject`
    3. Confirm `projectType == WORKFLOW`
    4. Create `WorkflowExecutor`
    5. Call `executor.execute(project, variables, insight, "manual")`
    6. Return result
- **Return type:** `PixelDataType.MAP` with execution result

### Task 5.2 — Register workflow reactors in `ReactorFactory`

- **Status:** ✅
- **File:** `src/prerna/reactor/ReactorFactory.java`
- **Description:** Added entries for all three workflow reactors:
  ```java
  reactorHash.put("RunWorkflow", RunWorkflowReactor.class);
  reactorHash.put("SaveWorkflow", SaveWorkflowReactor.class);
  reactorHash.put("GetWorkflowStatus", GetWorkflowStatusReactor.class);
  ```

### Task 5.3 — Create `SaveWorkflowReactor`

- **Status:** ✅
- **File:** `src/prerna/reactor/workflow/SaveWorkflowReactor.java` (new)
- **Description:**
  - **Pixel:** `SaveWorkflow(project=["uuid"], json=[{...}], comment=["optional message"]);`
  - **Keys:** `project` (required), `json` (required — workflow definition map), `comment` (optional)
  - Validates WORKFLOW project type, writes workflow.json, git add+commit, cluster sync
  - Security check: `userCanEditProject`
- **Return type:** `PixelDataType.BOOLEAN` (true on success)

### Task 5.4 — Create `GetWorkflowStatusReactor`

- **Status:** ✅
- **File:** `src/prerna/reactor/workflow/GetWorkflowStatusReactor.java` (new)
- **Description:**
  - **Pixel:** `GetWorkflowStatus(project=["uuid"]);`
  - Returns project metadata + current workflow.json contents
  - Security check: `userCanViewProject`
  - Pulls from cloud if clustered
- **Return type:** `PixelDataType.MAP` with projectId, projectName, projectType, workflow

---

## Phase 6: Execution Logging

> Goal: Persist execution results for history and debugging.

### Task 6.1 — Implement execution log saving

- **Status:** ✅
- **File:** `src/prerna/reactor/workflow/engine/WorkflowExecutor.java`
- **Description:** After execution completes (success, error, or timeout):
  - Serializes `WorkflowExecutionResult` to JSON via `saveExecutionLog()`
  - Writes to `{project}/assets/workflow/executions/{executionId}.json`
  - Creates the `executions/` directory if it doesn't exist
  - Best-effort — log failures do not abort the workflow result
  - All exit paths (success, timeout, maxSteps, error in step, missing handler) save logs

### Task 6.2 — Add execution log cleanup/retention

- **Status:** ✅
- **File:** `src/prerna/reactor/workflow/engine/WorkflowExecutor.java`
- **Description:** Implemented `cleanupOldExecutions()`:
  - Keeps the latest 100 execution logs per project (configurable via `MAX_EXECUTION_LOGS`)
  - Sorts by last-modified time, deletes oldest files when count exceeds threshold
  - Called automatically after every log save

### Task 6.3 — Add execution history to `GetWorkflowStatusReactor`

- **Status:** ✅
- **File:** `src/prerna/reactor/workflow/GetWorkflowStatusReactor.java`
- **Description:** Enhanced the reactor to include an `executions` array in the return map:
  - Reads execution log files from `assets/workflow/executions/`
  - Returns summaries (executionId, status, durationMs, triggeredBy, startTimeMs, endTimeMs, error)
  - Sorted newest-first, capped at 50 entries
  - Does not include full step results (lightweight for list views)

---

## Phase 7: Scheduling Integration

> Goal: Enable cron-based and trigger-based workflow execution.

### Task 7.1 — Verify scheduling works with `RunWorkflow` Pixel

- **Status:** ✅
- **Files:** Existing scheduler reactors, `RunWorkflowReactor.java` (updated)
- **Description:** Verified compatibility by tracing the full scheduling path:
  1. `ScheduleJobReactor` stores a Pixel `recipe` + encrypted user credentials in a Quartz `JobDataMap`
  2. When the cron fires, `RunPixelJobFromDB` makes an HTTP POST to `/api/schedule/executePixel` with the stored Pixel and user credentials
  3. The server-side endpoint reconstructs a `User` + `Insight`, then runs the Pixel
  4. `RunWorkflow(project=["uuid"])` is a standard Pixel string — works with zero changes
  5. **Added** optional `trigger` key to `RunWorkflowReactor` so scheduled runs can self-identify:
     ```
     ScheduleJob(
       jobName=["Weekly Report"],
       jobGroup=["workflow-project-uuid"],
       cronExpression=["0 0 9 ? * MON"],
       recipe=["RunWorkflow(project=[\"workflow-project-uuid\"], trigger=[\"schedule\"]);"],
       uiState=["..."]
     );
     ```
  6. User credentials pass through via the scheduler's `providerInfo` mechanism — security checks in `RunWorkflowReactor` work correctly

### Task 7.2 — Webhook trigger (No dedicated endpoint needed)

- **Status:** ✅ (N/A)
- **Description:** No dedicated webhook endpoint is needed in the engine layer. The existing Pixel execution REST API (in the web module) already accepts arbitrary Pixel strings. External systems can trigger workflow execution by calling:
  ```
  POST /api/engine/runPixel
  Body: { "expression": "RunWorkflow(project=[\"uuid\"], trigger=[\"webhook\"], variables=[{\"key\": \"value\"}]);" }
  ```
  If a purpose-built webhook endpoint is ever needed (simpler URL, API key auth), it belongs in the **web module**, not this engine repo.

### Task 7.3 — Implement event-based triggers (Deferred)

- **Status:** ⬜
- **Description:** Future work — hook into internal SEMOSS events to trigger workflows.

---

## Phase 8: Frontend — Canvas Editor

> Goal: Build the visual workflow editor UI.

### Task 8.1 — Create workflow list view

- **Status:** ⬜
- **Description:**
  - Filter projects by `projectType = WORKFLOW`
  - Display as cards or table: name, last run status, schedule, created by
  - Actions: create new, edit, run, delete, share
  - "Create Workflow" button → calls `CreateProject(type=["WORKFLOW"])`

### Task 8.2 — Set up react-flow canvas

- **Status:** ⬜
- **Description:**
  - Integrate react-flow (or similar DAG editor library)
  - Load `workflow.json` on mount via `GetAppAssets`
  - Render steps as nodes, `next`/`ifTrue`/`ifFalse` as edges
  - Save on change via `SaveAppAssets`

### Task 8.3 — Build step palette (left sidebar)

- **Status:** ⬜
- **Description:**
  - Draggable list of step types with icons
  - Grouped by category: AI (LLM_ASK, LLM_AGENT), Data (RUN_TOOL, QUERY_DB, RUN_PIXEL), Logic (CONDITION, LOOP, TRANSFORM), I/O (OUTPUT, HUMAN_INPUT)
  - Drag onto canvas creates a new node with default config

### Task 8.4 — Build config panel (right sidebar)

- **Status:** ⬜
- **Description:**
  - Shows when a node is selected
  - Auto-generated form based on step type
  - LLM_ASK: model dropdown, system prompt textarea, user prompt textarea
  - RUN_TOOL: engine dropdown → tool dropdown (fetched from `GetMCPTools`) → auto-generated param form from `inputSchema`
  - CONDITION: expression input
  - All text fields support `{{...}}` autocomplete with available variables/step outputs

### Task 8.5 — Build variables panel

- **Status:** ⬜
- **Description:**
  - Top bar or collapsible section
  - Add/edit/delete workflow-level variables
  - Each variable: name, type, default value, description

### Task 8.6 — Build run controls

- **Status:** ⬜
- **Description:**
  - "Run Now" button → calls `RunWorkflow` → shows live progress
  - Step nodes light up green (success), red (error), yellow (in-progress), gray (pending)
  - Click completed node → see input/output in a modal or panel
  - "Schedule" button → opens cron expression builder → calls `ScheduleJob`

### Task 8.7 — Build execution history panel

- **Status:** ⬜
- **Description:**
  - Bottom drawer or tab
  - Table: execution ID, status, duration, triggered by, timestamp
  - Click row → opens run detail view (step-by-step breakdown)

---

## Phase 9: Advanced Features (Deferred)

### Task 9.1 — Workflow-as-MCP-tool (Composability)

- **Status:** ⬜
- **Description:**
  - Allow a WORKFLOW project to expose itself as an MCP tool
  - Auto-generate `py_mcp.json` with a tool entry based on workflow variables as input schema
  - Implementation calls `RunWorkflow` internally
  - Enables nesting: one workflow calling another via `LLM_AGENT` step

### Task 9.2 — `HUMAN_INPUT` step type

- **Status:** ⬜
- **Description:**
  - Suspends workflow execution
  - Stores execution state to disk
  - Presents a form to the user in the UI
  - On submit → resumes execution from the suspended step
  - Requires: execution state serialization/deserialization

### Task 9.3 — Workflow versioning

- **Status:** ⬜
- **Description:**
  - Track version history of `workflow.json`
  - Allow reverting to a previous version
  - Show diff between versions
  - Can leverage git-backed projects or manual versioning

### Task 9.4 — Workflow templates / marketplace

- **Status:** ⬜
- **Description:**
  - Pre-built workflow templates that users can clone
  - "Start from template" in the create workflow flow
  - Templates stored as project exports

### Task 9.5 — Parallel step execution

- **Status:** ⬜
- **Description:**
  - When a step has multiple `next` targets with no dependencies between them, execute in parallel
  - Requires thread-safe `WorkflowContext`
  - Join/merge step to wait for all parallel branches

### Task 9.6 — RAG composite step

- **Status:** ⬜
- **Description:**
  - A convenience composite step type that combines Vector Search → LLM Ask in a single node
  - Config: `vectorEngineId`, `searchQuery`, `topK`, `modelId`, `systemPrompt`
  - Under the hood: runs `VectorDatabaseQuery` tool, then passes results into an LLM ask
  - Simplifies the most common multi-step pattern into a single drag-and-drop node
  - Can be built as a handler that internally chains `RunToolStepHandler` + `LLMAskStepHandler`

### Task 9.7 — Database engine MCP tools

- **Status:** ⬜
- **Description:**
  - Build standard MCP tools for DATABASE engine type (similar to VECTOR/STORAGE/FUNCTION)
  - Tools: `QueryDatabase`, `GetDatabaseSchema`, `GetDatabaseTables`, etc.
  - Add DATABASE to `MakeEngineMCPReactor.STANDARD_ENGINE_TOOLS` map
  - Once available, DATABASE engines become usable in workflow `RUN_TOOL` steps

---

## Testing Plan

### Unit Tests

| Test | Status |
|---|---|
| `WorkflowDefinition` parsing from JSON | ⬜ |
| `WorkflowDefinition` validation (cycles, missing refs, orphans) | ⬜ |
| `WorkflowTemplateEngine` — variable resolution | ⬜ |
| `WorkflowTemplateEngine` — step output resolution | ⬜ |
| `WorkflowTemplateEngine` — nested field access | ⬜ |
| `StaticStepHandler` | ⬜ |
| `ConditionStepHandler` — true/false branching | ⬜ |
| `WorkflowExecutor` — linear workflow (3 steps) | ⬜ |
| `WorkflowExecutor` — branching workflow (if/else) | ⬜ |
| `WorkflowExecutor` — maxSteps enforcement | ⬜ |
| `WorkflowExecutor` — timeout enforcement | ⬜ |
| `WorkflowExecutor` — error handling (onError=stop) | ⬜ |

### Integration Tests

| Test | Status |
|---|---|
| Create WORKFLOW project → verify folder structure | ⬜ |
| Save and load `workflow.json` via asset APIs | ⬜ |
| `RunWorkflowReactor` — simple linear workflow | ⬜ |
| `RunWorkflowReactor` — workflow with LLM_ASK step | ⬜ |
| `RunWorkflowReactor` — workflow with RUN_TOOL step | ⬜ |
| Schedule workflow via `ScheduleJobReactor` | ⬜ |
| Execution logging — verify log file written | ⬜ |
| Security — user without access cannot run workflow | ⬜ |

---

## Dependencies & Risks

| Risk | Mitigation |
|---|---|
| `Room.ask()` is tightly coupled to the playground FE flow | Extract headless execution path; the `AskPlaygroundReactor` already does most of this inline |
| Quartz scheduler passes user credentials — workflow steps accessing other engines need proper auth | Reuse `RunPixelJobFromDB`'s credential-passing pattern |
| Large workflow executions could produce big log files | Implement retention policy (Task 6.2) |
| Complex template expressions (nested, fallback) could be fragile | Start with simple variable/output resolution, add complexity incrementally |
| react-flow or canvas library choice affects feature scope | Evaluate react-flow, elkjs, dagre for layout capabilities before committing |

---

## Quick Start (Phase 1-4 Minimum Viable Feature)

For the earliest working version, implement only these tasks:

1. ⬜ Task 1.1 — `WORKFLOW` enum
2. ⬜ Task 1.2 — `CreateProjectReactor` update
3. ⬜ Task 1.3 — `WorkflowDefinition`
4. ⬜ Task 1.4 — `WorkflowStep`
5. ⬜ Task 1.5 — `WorkflowContext`
6. ⬜ Task 1.6 — `StepResult`
7. ⬜ Task 2.1 — `WorkflowTemplateEngine`
8. ⬜ Task 3.1 — `IWorkflowStepHandler`
9. ⬜ Task 3.2 — `WorkflowStepHandlerRegistry`
10. ⬜ Task 3.3 — `StaticStepHandler`
11. ⬜ Task 3.5 — `RunToolStepHandler`
12. ⬜ Task 3.6 — `LLMAskStepHandler`
13. ⬜ Task 3.7 — `ConditionStepHandler`
14. ⬜ Task 4.1 — `WorkflowExecutor`
15. ⬜ Task 4.2 — DAG validation
16. ⬜ Task 5.1 — `RunWorkflowReactor`
17. ⬜ Task 5.2 — Register in `ReactorFactory`

This gives you a **fully functional headless workflow engine** that can be invoked via Pixel:
```
RunWorkflow(project=["uuid"], variables=[{"database": "db-uuid"}]);
```
