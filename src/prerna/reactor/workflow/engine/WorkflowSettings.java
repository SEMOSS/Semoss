package prerna.reactor.workflow.engine;

/**
 * Settings for a workflow execution — max steps, timeout, error handling.
 */
public class WorkflowSettings {

	private int maxSteps = 50;
	private long timeoutMs = 300000;
	private String onError = "stop"; // "stop" | "skip" | "retry"

	public int getMaxSteps() { return maxSteps; }
	public void setMaxSteps(int maxSteps) { this.maxSteps = maxSteps; }

	public long getTimeoutMs() { return timeoutMs; }
	public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }

	public String getOnError() { return onError; }
	public void setOnError(String onError) { this.onError = onError; }
}
