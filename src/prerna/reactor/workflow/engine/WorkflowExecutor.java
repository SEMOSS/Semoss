package prerna.reactor.workflow.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.workflow.engine.handlers.IWorkflowStepHandler;
import prerna.reactor.workflow.engine.handlers.WorkflowStepHandlerRegistry;
import prerna.util.AssetUtility;
import prerna.util.gson.GsonUtility;

/**
 * The core DAG executor that walks a workflow definition and orchestrates
 * step execution. Entry point for all workflow runs — manual, scheduled, or triggered.
 *
 * Execution algorithm:
 *   1. Load and parse workflow.json from the project's assets
 *   2. Validate the DAG structure
 *   3. Build an in-degree map to determine execution readiness
 *   4. Seed the ready queue with entry steps (in-degree == 0)
 *   5. While queue is not empty:
 *      a. Dequeue a step
 *      b. Resolve {{template}} expressions in its config via WorkflowTemplateEngine
 *      c. Look up the handler from WorkflowStepHandlerRegistry
 *      d. Execute the handler
 *      e. Record the StepResult in the WorkflowContext
 *      f. Determine successors (branch-aware for CONDITION steps)
 *      g. Decrement in-degree for successors; enqueue those that reach 0
 *   6. Enforce maxSteps and timeoutMs guardrails
 *   7. Handle errors per settings.onError ("stop" or "skip")
 *   8. Build and return WorkflowExecutionResult
 */
public class WorkflowExecutor {

	private static final Logger classLogger = LogManager.getLogger(WorkflowExecutor.class);

	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

	/** Maximum number of execution logs to retain per project. */
	private static final int MAX_EXECUTION_LOGS = 100;

	/** Subfolder under workflow/ for execution logs. */
	private static final String EXECUTIONS_FOLDER = "executions";

	/**
	 * Execute a workflow project.
	 *
	 * @param project           the WORKFLOW project containing workflow.json
	 * @param variableOverrides runtime variable overrides (merged with definition defaults)
	 * @param insight           the current insight context
	 * @param triggeredBy       who/what triggered this run (e.g., "manual", "schedule", "api")
	 * @return the execution result
	 */
	public WorkflowExecutionResult execute(IProject project,
			Map<String, Object> variableOverrides,
			Insight insight, String triggeredBy) {

		// ── 1. Load workflow.json ───────────────────────────────────────
		String projectId = project.getProjectId();
		String assetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		String executionsDir = assetFolder
				+ File.separator + IProject.WORKFLOW_FOLDER
				+ File.separator + EXECUTIONS_FOLDER;
		File workflowFile = new File(assetFolder
				+ File.separator + IProject.WORKFLOW_FOLDER
				+ File.separator + IProject.WORKFLOW_FILE_NAME);

		if (!workflowFile.exists()) {
			throw new IllegalStateException(
					"workflow.json not found for project " + projectId
					+ " at path: " + workflowFile.getAbsolutePath());
		}

		WorkflowDefinition definition;
		try {
			definition = WorkflowDefinition.fromFile(workflowFile);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to parse workflow.json: " + e.getMessage(), e);
		}

		// ── 2. Validate ────────────────────────────────────────────────
		List<String> validationErrors = definition.validate();
		if (!validationErrors.isEmpty()) {
			throw new IllegalArgumentException(
					"Workflow validation failed: " + String.join("; ", validationErrors));
		}

		// ── 3. Build context ───────────────────────────────────────────
		Map<String, Object> mergedVars = new HashMap<>();
		if (definition.getVariables() != null) {
			mergedVars.putAll(definition.getVariables());
		}
		if (variableOverrides != null) {
			mergedVars.putAll(variableOverrides);
		}
		WorkflowContext context = new WorkflowContext(mergedVars);

		WorkflowSettings settings = definition.getSettings();
		if (settings == null) {
			settings = new WorkflowSettings();
		}

		// ── 4. Build adjacency structures ──────────────────────────────
		Map<String, WorkflowStep> stepMap = definition.getStepMap();
		Map<String, Integer> inDegree = buildInDegreeMap(definition);

		// ── 5. Seed queue with entry steps ─────────────────────────────
		Queue<String> readyQueue = new ArrayDeque<>();
		for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
			if (entry.getValue() == 0) {
				readyQueue.add(entry.getKey());
			}
		}

		String onError = settings.getOnError() != null ? settings.getOnError() : "stop";
		Object lastOutput = null;

		classLogger.info("Starting workflow '{}' with {} steps, {} entry points",
				definition.getName(), stepMap.size(), readyQueue.size());

		// ── 6. Execute loop ────────────────────────────────────────────
		while (!readyQueue.isEmpty()) {
			// Timeout check
			if (context.getElapsedMs() > settings.getTimeoutMs()) {
				classLogger.warn("Workflow '{}' timed out after {}ms",
						definition.getName(), context.getElapsedMs());
				WorkflowExecutionResult result = WorkflowExecutionResult.timeout(
						context, definition.getWorkflowId(), triggeredBy);
				result.setFinalOutput(lastOutput);
				saveExecutionLog(result, executionsDir);
				return result;
			}

			// Max steps check
			if (context.getStepsExecuted() >= settings.getMaxSteps()) {
				classLogger.warn("Workflow '{}' hit max steps limit of {}",
						definition.getName(), settings.getMaxSteps());
				WorkflowExecutionResult result = WorkflowExecutionResult.from(
						context, definition.getWorkflowId(), triggeredBy);
				result.setStatus(WorkflowExecutionResult.Status.ERROR);
				result.setError("Max steps limit reached: " + settings.getMaxSteps());
				result.setFinalOutput(lastOutput);
				saveExecutionLog(result, executionsDir);
				return result;
			}

			String stepId = readyQueue.poll();
			WorkflowStep step = stepMap.get(stepId);
			if (step == null) {
				classLogger.warn("Step '{}' not found in step map, skipping", stepId);
				continue;
			}

			// Skip if already executed (safety guard for join nodes)
			if (context.hasResult(stepId)) {
				continue;
			}

			classLogger.debug("Executing step '{}' (type={})", stepId, step.getType());

			// Resolve templates in config
			Map<String, Object> resolvedConfig = WorkflowTemplateEngine.resolveMap(
					step.getConfig(), context);

			// Also resolve input mappings and merge into config
			if (step.getInputs() != null && !step.getInputs().isEmpty()) {
				for (Map.Entry<String, String> inputEntry : step.getInputs().entrySet()) {
					Object resolvedValue = WorkflowTemplateEngine.resolve(
							inputEntry.getValue(), context);
					resolvedConfig.put(inputEntry.getKey(), resolvedValue);
				}
			}

			// Look up handler
			String stepType = step.getType();
			IWorkflowStepHandler handler = WorkflowStepHandlerRegistry.getHandler(stepType);
			if (handler == null) {
				StepResult errorResult = StepResult.error(stepId,
						"No handler registered for step type: " + stepType, 0);
				context.putResult(stepId, errorResult);
				context.addLogEntry(stepId, stepType, StepResult.Status.ERROR, 0);

				if ("stop".equalsIgnoreCase(onError)) {
					return buildErrorResult(context, definition, triggeredBy,
							"No handler for step type: " + stepType, lastOutput, executionsDir);
				}
				continue;
			}

			// Execute
			StepResult stepResult;
			try {
				stepResult = handler.execute(stepId, resolvedConfig, context, insight);
			} catch (Exception e) {
				classLogger.error("Unhandled exception in step '{}'", stepId, e);
				stepResult = StepResult.error(stepId,
						"Unhandled exception: " + e.getMessage(), 0);
			}

			// Record result
			context.putResult(stepId, stepResult);
			context.addLogEntry(stepId, stepType, stepResult.getStatus(), stepResult.getDurationMs());

			if (stepResult.getStatus() == StepResult.Status.SUCCESS) {
				lastOutput = stepResult.getOutput();
			}

			classLogger.debug("Step '{}' completed: status={}, duration={}ms",
					stepId, stepResult.getStatus(), stepResult.getDurationMs());

			// Handle error
			if (stepResult.getStatus() == StepResult.Status.ERROR) {
				if ("stop".equalsIgnoreCase(onError)) {
					return buildErrorResult(context, definition, triggeredBy,
							"Step '" + stepId + "' failed: " + stepResult.getError(),
							lastOutput, executionsDir);
				}
				// "skip" — don't enqueue successors, continue with other ready steps
				classLogger.warn("Step '{}' errored but onError=skip, continuing", stepId);
				continue;
			}

			// ── Determine successors ───────────────────────────────────
			List<String> successors = getSuccessors(step, stepResult);
			for (String successorId : successors) {
				if (!stepMap.containsKey(successorId)) continue;
				int remaining = inDegree.getOrDefault(successorId, 0) - 1;
				inDegree.put(successorId, remaining);
				if (remaining <= 0) {
					readyQueue.add(successorId);
				}
			}
		}

		// ── 7. Build final result ──────────────────────────────────────
		WorkflowExecutionResult result = WorkflowExecutionResult.from(
				context, definition.getWorkflowId(), triggeredBy);
		result.setFinalOutput(lastOutput);

		classLogger.info("Workflow '{}' completed: status={}, duration={}ms, steps={}",
				definition.getName(), result.getStatus(),
				result.getDurationMs(), context.getStepsExecuted());

		saveExecutionLog(result, executionsDir);
		return result;
	}

	/**
	 * Build an in-degree map from the workflow definition.
	 * In-degree = number of predecessor steps that must complete before a step can run.
	 * Entry steps have in-degree of 0.
	 */
	private Map<String, Integer> buildInDegreeMap(WorkflowDefinition definition) {
		Map<String, Integer> inDegree = new HashMap<>();

		// Initialize all steps to 0
		for (WorkflowStep step : definition.getSteps()) {
			inDegree.put(step.getStepId(), 0);
		}

		// Count incoming edges
		for (WorkflowStep step : definition.getSteps()) {
			for (String successorId : step.getAllSuccessorIds()) {
				inDegree.merge(successorId, 1, Integer::sum);
			}
		}

		return inDegree;
	}

	/**
	 * Determine which successor steps to activate based on the step type and result.
	 * For CONDITION steps, only the matching branch (ifTrue/ifFalse) is returned.
	 * For all other steps, the normal 'next' list is returned.
	 */
	private List<String> getSuccessors(WorkflowStep step, StepResult result) {
		WorkflowStep.STEP_TYPE type = step.getStepType();

		if (type == WorkflowStep.STEP_TYPE.CONDITION) {
			String branch = (String) result.getMetadata().get("branch");
			if ("ifTrue".equals(branch) && step.getIfTrue() != null) {
				return step.getIfTrue();
			} else if ("ifFalse".equals(branch) && step.getIfFalse() != null) {
				return step.getIfFalse();
			}
			// Fallback to next if branch metadata is missing
			return step.getNext() != null ? step.getNext() : List.of();
		}

		// Non-condition steps: use "next"
		return step.getNext() != null ? step.getNext() : List.of();
	}

	/**
	 * Build an error execution result.
	 */
	/**
	 * Build an error execution result and save the execution log.
	 */
	private WorkflowExecutionResult buildErrorResult(WorkflowContext context,
			WorkflowDefinition definition, String triggeredBy,
			String errorMessage, Object lastOutput, String executionsDir) {
		WorkflowExecutionResult result = WorkflowExecutionResult.from(
				context, definition.getWorkflowId(), triggeredBy);
		result.setStatus(WorkflowExecutionResult.Status.ERROR);
		result.setError(errorMessage);
		result.setFinalOutput(lastOutput);
		if (executionsDir != null) {
			saveExecutionLog(result, executionsDir);
		}
		return result;
	}

	// ── Execution log persistence ──────────────────────────────────────

	/**
	 * Persist a WorkflowExecutionResult to the executions directory as
	 * {executionId}.json. Also enforces retention cleanup.
	 */
	private void saveExecutionLog(WorkflowExecutionResult result, String executionsDir) {
		try {
			File dir = new File(executionsDir);
			if (!dir.exists()) {
				dir.mkdirs();
			}

			File logFile = new File(dir, result.getExecutionId() + ".json");
			GsonUtility.writeObjectToJsonFile(logFile, GSON, result);
			classLogger.debug("Saved execution log to {}", logFile.getAbsolutePath());

			// Enforce retention — keep only the latest N logs
			cleanupOldExecutions(dir);
		} catch (IOException e) {
			// Log but do not fail the workflow — execution log is best-effort
			classLogger.warn("Failed to save execution log for {}: {}",
					result.getExecutionId(), e.getMessage());
		}
	}

	/**
	 * Delete oldest execution log files if the count exceeds MAX_EXECUTION_LOGS.
	 * Files are sorted by last-modified time; the oldest are removed first.
	 */
	private void cleanupOldExecutions(File executionsDir) {
		File[] logFiles = executionsDir.listFiles((dir, name) -> name.endsWith(".json"));
		if (logFiles == null || logFiles.length <= MAX_EXECUTION_LOGS) {
			return;
		}

		// Sort ascending by last modified (oldest first)
		Arrays.sort(logFiles, Comparator.comparingLong(File::lastModified));

		int toDelete = logFiles.length - MAX_EXECUTION_LOGS;
		for (int i = 0; i < toDelete; i++) {
			if (logFiles[i].delete()) {
				classLogger.debug("Deleted old execution log: {}", logFiles[i].getName());
			}
		}
	}
}
