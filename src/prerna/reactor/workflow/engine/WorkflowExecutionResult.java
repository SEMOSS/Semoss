package prerna.reactor.workflow.engine;

import java.util.Map;
import java.util.UUID;

/**
 * Final result of a complete workflow execution.
 */
public class WorkflowExecutionResult {

	private String executionId;
	private String workflowId;
	private Status status;
	private long startTimeMs;
	private long endTimeMs;
	private long durationMs;
	private String triggeredBy;
	private Map<String, StepResult> stepResults;
	private Object finalOutput;
	private String error;

	public enum Status {
		SUCCESS, ERROR, TIMEOUT
	}

	// ── Factory helpers ─────────────────────────────────────────────────

	public static WorkflowExecutionResult from(WorkflowContext context, String workflowId, String triggeredBy) {
		WorkflowExecutionResult result = new WorkflowExecutionResult();
		result.executionId = UUID.randomUUID().toString();
		result.workflowId = workflowId;
		result.startTimeMs = context.getStartTimeMs();
		result.endTimeMs = System.currentTimeMillis();
		result.durationMs = result.endTimeMs - result.startTimeMs;
		result.triggeredBy = triggeredBy;
		result.stepResults = context.getStepResults();

		// Determine overall status from step results
		boolean hasError = context.getStepResults().values().stream()
				.anyMatch(r -> r.getStatus() == StepResult.Status.ERROR);
		result.status = hasError ? Status.ERROR : Status.SUCCESS;

		return result;
	}

	public static WorkflowExecutionResult timeout(WorkflowContext context, String workflowId, String triggeredBy) {
		WorkflowExecutionResult result = from(context, workflowId, triggeredBy);
		result.status = Status.TIMEOUT;
		result.error = "Workflow exceeded timeout of " + context.getElapsedMs() + "ms";
		return result;
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getExecutionId() { return executionId; }
	public void setExecutionId(String executionId) { this.executionId = executionId; }

	public String getWorkflowId() { return workflowId; }
	public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }

	public Status getStatus() { return status; }
	public void setStatus(Status status) { this.status = status; }

	public long getStartTimeMs() { return startTimeMs; }
	public void setStartTimeMs(long startTimeMs) { this.startTimeMs = startTimeMs; }

	public long getEndTimeMs() { return endTimeMs; }
	public void setEndTimeMs(long endTimeMs) { this.endTimeMs = endTimeMs; }

	public long getDurationMs() { return durationMs; }
	public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

	public String getTriggeredBy() { return triggeredBy; }
	public void setTriggeredBy(String triggeredBy) { this.triggeredBy = triggeredBy; }

	public Map<String, StepResult> getStepResults() { return stepResults; }
	public void setStepResults(Map<String, StepResult> stepResults) { this.stepResults = stepResults; }

	public Object getFinalOutput() { return finalOutput; }
	public void setFinalOutput(Object finalOutput) { this.finalOutput = finalOutput; }

	public String getError() { return error; }
	public void setError(String error) { this.error = error; }
}
