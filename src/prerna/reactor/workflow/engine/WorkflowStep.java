package prerna.reactor.workflow.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a single step (node) in a workflow DAG.
 */
public class WorkflowStep {

	// ── Step type enum ──────────────────────────────────────────────────

	public enum STEP_TYPE {
		LLM_ASK,
		LLM_AGENT,
		RUN_TOOL,
		RUN_PIXEL,
		RUN_PYTHON,
		CONDITION,
		LOOP,
		TRANSFORM,
		STATIC,
		HUMAN_INPUT,
		GUARDRAIL,
		OUTPUT
	}

	// ── Fields ──────────────────────────────────────────────────────────

	private String stepId;
	private String type;
	private String name;
	private String description;

	/** UI position — ignored by the backend executor */
	private Map<String, Object> position;

	/** Type-specific configuration (model id, prompt, recipe, etc.) */
	private Map<String, Object> config = new HashMap<>();

	/** Input mappings using {{template}} expressions */
	private Map<String, String> inputs = new HashMap<>();

	/** Default successor step IDs */
	private List<String> next;

	/** Successor step IDs when condition evaluates to true (CONDITION type only) */
	private List<String> ifTrue;

	/** Successor step IDs when condition evaluates to false (CONDITION type only) */
	private List<String> ifFalse;

	// ── Convenience ─────────────────────────────────────────────────────

	/**
	 * Returns the parsed STEP_TYPE enum, or null if the type string is invalid.
	 */
	public STEP_TYPE getStepType() {
		if (type == null) return null;
		try {
			return STEP_TYPE.valueOf(type);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Returns all successor step IDs (combines next, ifTrue, ifFalse).
	 * Used by DAG validation and cycle detection.
	 */
	public List<String> getAllSuccessorIds() {
		List<String> successors = new ArrayList<>();
		if (next != null) successors.addAll(next);
		if (ifTrue != null) successors.addAll(ifTrue);
		if (ifFalse != null) successors.addAll(ifFalse);
		return successors;
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getStepId() { return stepId; }
	public void setStepId(String stepId) { this.stepId = stepId; }

	public String getType() { return type; }
	public void setType(String type) { this.type = type; }

	public String getName() { return name; }
	public void setName(String name) { this.name = name; }

	public String getDescription() { return description; }
	public void setDescription(String description) { this.description = description; }

	public Map<String, Object> getPosition() { return position; }
	public void setPosition(Map<String, Object> position) { this.position = position; }

	public Map<String, Object> getConfig() { return config; }
	public void setConfig(Map<String, Object> config) { this.config = config; }

	public Map<String, String> getInputs() { return inputs; }
	public void setInputs(Map<String, String> inputs) { this.inputs = inputs; }

	public List<String> getNext() { return next; }
	public void setNext(List<String> next) { this.next = next; }

	public List<String> getIfTrue() { return ifTrue; }
	public void setIfTrue(List<String> ifTrue) { this.ifTrue = ifTrue; }

	public List<String> getIfFalse() { return ifFalse; }
	public void setIfFalse(List<String> ifFalse) { this.ifFalse = ifFalse; }
}
