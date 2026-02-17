package prerna.reactor.workflow.engine;

/**
 * Result of a single step execution.
 */
public class StepResult {

	private String stepId;
	private Status status;
	private Object output;
	private java.util.Map<String, Object> metadata = new java.util.HashMap<>();
	private String error;
	private long durationMs;

	public enum Status {
		SUCCESS, ERROR, SKIPPED
	}

	// ── Factory helpers ─────────────────────────────────────────────────

	public static StepResult success(String stepId, Object output, long durationMs) {
		StepResult r = new StepResult();
		r.stepId = stepId;
		r.status = Status.SUCCESS;
		r.output = output;
		r.durationMs = durationMs;
		return r;
	}

	public static StepResult error(String stepId, String errorMsg, long durationMs) {
		StepResult r = new StepResult();
		r.stepId = stepId;
		r.status = Status.ERROR;
		r.error = errorMsg;
		r.durationMs = durationMs;
		return r;
	}

	public static StepResult skipped(String stepId) {
		StepResult r = new StepResult();
		r.stepId = stepId;
		r.status = Status.SKIPPED;
		r.durationMs = 0;
		return r;
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getStepId() { return stepId; }
	public void setStepId(String stepId) { this.stepId = stepId; }

	public Status getStatus() { return status; }
	public void setStatus(Status status) { this.status = status; }

	public Object getOutput() { return output; }
	public void setOutput(Object output) { this.output = output; }

	public java.util.Map<String, Object> getMetadata() { return metadata; }
	public void setMetadata(java.util.Map<String, Object> metadata) { this.metadata = metadata; }

	public String getError() { return error; }
	public void setError(String error) { this.error = error; }

	public long getDurationMs() { return durationMs; }
	public void setDurationMs(long durationMs) { this.durationMs = durationMs; }
}
