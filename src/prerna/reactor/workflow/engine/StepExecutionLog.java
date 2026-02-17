package prerna.reactor.workflow.engine;

/**
 * A single log entry recording the execution of one step.
 */
public class StepExecutionLog {

	private final String stepId;
	private final String stepType;
	private final StepResult.Status status;
	private final long durationMs;
	private final long timestamp;

	public StepExecutionLog(String stepId, String stepType, StepResult.Status status, long durationMs) {
		this.stepId = stepId;
		this.stepType = stepType;
		this.status = status;
		this.durationMs = durationMs;
		this.timestamp = System.currentTimeMillis();
	}

	public String getStepId() { return stepId; }
	public String getStepType() { return stepType; }
	public StepResult.Status getStatus() { return status; }
	public long getDurationMs() { return durationMs; }
	public long getTimestamp() { return timestamp; }
}
