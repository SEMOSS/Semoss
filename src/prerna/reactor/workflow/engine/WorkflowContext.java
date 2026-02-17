package prerna.reactor.workflow.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds runtime state during workflow execution.
 * Passed through to each step handler so they can read prior results and variables.
 */
public class WorkflowContext {

	private final Map<String, Object> variables = new HashMap<>();
	private final Map<String, StepResult> stepResults = new HashMap<>();
	private final List<StepExecutionLog> executionLog = new ArrayList<>();
	private final long startTimeMs;
	private int stepsExecuted = 0;

	public WorkflowContext() {
		this.startTimeMs = System.currentTimeMillis();
	}

	public WorkflowContext(Map<String, Object> initialVariables) {
		this();
		if (initialVariables != null) {
			this.variables.putAll(initialVariables);
		}
	}

	// ── Result tracking ─────────────────────────────────────────────────

	public void putResult(String stepId, StepResult result) {
		stepResults.put(stepId, result);
		stepsExecuted++;
	}

	public StepResult getResult(String stepId) {
		return stepResults.get(stepId);
	}

	public boolean hasResult(String stepId) {
		return stepResults.containsKey(stepId);
	}

	public void addLogEntry(String stepId, String stepType, StepResult.Status status, long durationMs) {
		executionLog.add(new StepExecutionLog(stepId, stepType, status, durationMs));
	}

	// ── Variable access ─────────────────────────────────────────────────

	public Object getVariable(String name) {
		return variables.get(name);
	}

	public void setVariable(String name, Object value) {
		variables.put(name, value);
	}

	// ── Getters ─────────────────────────────────────────────────────────

	public Map<String, Object> getVariables() { return variables; }
	public Map<String, StepResult> getStepResults() { return stepResults; }
	public List<StepExecutionLog> getExecutionLog() { return executionLog; }
	public long getStartTimeMs() { return startTimeMs; }
	public int getStepsExecuted() { return stepsExecuted; }

	/**
	 * Returns elapsed time since the workflow started.
	 */
	public long getElapsedMs() {
		return System.currentTimeMillis() - startTimeMs;
	}
}
