package prerna.reactor.workflow.engine;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Represents a parsed workflow.json definition.
 * This is the top-level POJO for the workflow engine.
 */
public class WorkflowDefinition {

	private String workflowId;
	private String name;
	private int version;
	private List<WorkflowStep> steps = new ArrayList<>();
	private Map<String, Object> variables = new HashMap<>();
	private Map<String, Object> trigger;
	private WorkflowSettings settings = new WorkflowSettings();

	// ── Factory methods ─────────────────────────────────────────────────

	private static final Gson GSON = new GsonBuilder()
			.setPrettyPrinting()
			.disableHtmlEscaping()
			.create();

	/**
	 * Parse a workflow definition from a JSON string.
	 */
	public static WorkflowDefinition parse(String json) {
		return GSON.fromJson(json, WorkflowDefinition.class);
	}

	/**
	 * Parse a workflow definition from a file.
	 */
	public static WorkflowDefinition fromFile(File file) throws IOException {
		try (FileReader reader = new FileReader(file)) {
			return GSON.fromJson(reader, WorkflowDefinition.class);
		}
	}

	/**
	 * Serialize this definition back to JSON.
	 */
	public String toJson() {
		return GSON.toJson(this);
	}

	// ── Validation ──────────────────────────────────────────────────────

	/**
	 * Validates the workflow definition for structural correctness.
	 * 
	 * @return list of validation error messages (empty if valid)
	 */
	public List<String> validate() {
		List<String> errors = new ArrayList<>();

		if (steps == null || steps.isEmpty()) {
			errors.add("Workflow must contain at least one step");
			return errors;
		}

		// Collect all step IDs
		Set<String> stepIds = new HashSet<>();
		for (WorkflowStep step : steps) {
			if (step.getStepId() == null || step.getStepId().isEmpty()) {
				errors.add("Step is missing a stepId");
				continue;
			}
			if (!stepIds.add(step.getStepId())) {
				errors.add("Duplicate stepId: " + step.getStepId());
			}
		}

		// Validate references and detect cycles
		Set<String> referencedIds = new HashSet<>();
		for (WorkflowStep step : steps) {
			if (step.getStepId() == null) continue;

			// Validate 'next' references
			if (step.getNext() != null) {
				for (String nextId : step.getNext()) {
					if (!stepIds.contains(nextId)) {
						errors.add("Step '" + step.getStepId() + "' references unknown step: " + nextId);
					}
					referencedIds.add(nextId);
				}
			}

			// Validate 'ifTrue'/'ifFalse' references (CONDITION steps)
			if (step.getIfTrue() != null) {
				for (String id : step.getIfTrue()) {
					if (!stepIds.contains(id)) {
						errors.add("Step '" + step.getStepId() + "' ifTrue references unknown step: " + id);
					}
					referencedIds.add(id);
				}
			}
			if (step.getIfFalse() != null) {
				for (String id : step.getIfFalse()) {
					if (!stepIds.contains(id)) {
						errors.add("Step '" + step.getStepId() + "' ifFalse references unknown step: " + id);
					}
					referencedIds.add(id);
				}
			}
		}

		// Check for entry points (steps with no incoming edges)
		Set<String> entrySteps = new HashSet<>(stepIds);
		entrySteps.removeAll(referencedIds);
		if (entrySteps.isEmpty()) {
			errors.add("No entry steps found — possible circular reference (all steps are referenced by other steps)");
		}

		// Cycle detection via DFS
		if (errors.isEmpty()) {
			Map<String, WorkflowStep> stepMap = getStepMap();
			Set<String> visited = new HashSet<>();
			Set<String> inStack = new HashSet<>();
			for (String entryId : entrySteps) {
				if (hasCycle(entryId, stepMap, visited, inStack)) {
					errors.add("Circular reference detected in workflow");
					break;
				}
			}

			// Orphan detection — steps not reachable from any entry point
			if (errors.isEmpty() && visited.size() < stepIds.size()) {
				Set<String> orphans = new HashSet<>(stepIds);
				orphans.removeAll(visited);
				for (String orphanId : orphans) {
					errors.add("Step '" + orphanId + "' is unreachable from any entry point");
				}
			}
		}

		return errors;
	}

	/**
	 * DFS cycle detection helper.
	 */
	private boolean hasCycle(String stepId, Map<String, WorkflowStep> stepMap,
			Set<String> visited, Set<String> inStack) {
		if (inStack.contains(stepId)) return true;
		if (visited.contains(stepId)) return false;

		visited.add(stepId);
		inStack.add(stepId);

		WorkflowStep step = stepMap.get(stepId);
		if (step != null) {
			List<String> successors = step.getAllSuccessorIds();
			for (String nextId : successors) {
				if (hasCycle(nextId, stepMap, visited, inStack)) {
					return true;
				}
			}
		}

		inStack.remove(stepId);
		return false;
	}

	// ── Convenience methods ─────────────────────────────────────────────

	/**
	 * Returns a map of stepId -> WorkflowStep for quick lookup.
	 */
	public Map<String, WorkflowStep> getStepMap() {
		Map<String, WorkflowStep> map = new HashMap<>();
		if (steps != null) {
			for (WorkflowStep step : steps) {
				if (step.getStepId() != null) {
					map.put(step.getStepId(), step);
				}
			}
		}
		return map;
	}

	/**
	 * Returns the step IDs that have no incoming edges (entry points).
	 */
	public List<String> getEntryStepIds() {
		Set<String> allIds = new HashSet<>();
		Set<String> referenced = new HashSet<>();
		for (WorkflowStep step : steps) {
			allIds.add(step.getStepId());
			if (step.getNext() != null) referenced.addAll(step.getNext());
			if (step.getIfTrue() != null) referenced.addAll(step.getIfTrue());
			if (step.getIfFalse() != null) referenced.addAll(step.getIfFalse());
		}
		allIds.removeAll(referenced);
		return new ArrayList<>(allIds);
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getWorkflowId() { return workflowId; }
	public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }

	public String getName() { return name; }
	public void setName(String name) { this.name = name; }

	public int getVersion() { return version; }
	public void setVersion(int version) { this.version = version; }

	public List<WorkflowStep> getSteps() { return steps; }
	public void setSteps(List<WorkflowStep> steps) { this.steps = steps; }

	public Map<String, Object> getVariables() { return variables; }
	public void setVariables(Map<String, Object> variables) { this.variables = variables; }

	public Map<String, Object> getTrigger() { return trigger; }
	public void setTrigger(Map<String, Object> trigger) { this.trigger = trigger; }

	public WorkflowSettings getSettings() { return settings; }
	public void setSettings(WorkflowSettings settings) { this.settings = settings; }
}
