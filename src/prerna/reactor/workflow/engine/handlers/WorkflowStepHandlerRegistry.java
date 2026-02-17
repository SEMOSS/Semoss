package prerna.reactor.workflow.engine.handlers;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.workflow.engine.WorkflowStep;

/**
 * Registry that maps step type strings to their handler implementations.
 */
public class WorkflowStepHandlerRegistry {

	private static final Map<String, IWorkflowStepHandler> HANDLERS = new HashMap<>();

	static {
		HANDLERS.put(WorkflowStep.STEP_TYPE.STATIC.name(), new StaticStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.CONDITION.name(), new ConditionStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.OUTPUT.name(), new OutputStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.RUN_PIXEL.name(), new RunPixelStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.RUN_TOOL.name(), new RunToolStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.LLM_ASK.name(), new LLMAskStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.LLM_AGENT.name(), new LLMAgentStepHandler());
	}

	/**
	 * Get the handler for a given step type.
	 * 
	 * @param type the step type string (e.g., "LLM_ASK", "RUN_TOOL")
	 * @return the handler, or null if no handler is registered for this type
	 */
	public static IWorkflowStepHandler getHandler(String type) {
		return HANDLERS.get(type);
	}

	/**
	 * Register a custom handler for a step type.
	 * Can be used to override built-in handlers or add new types.
	 */
	public static void register(String type, IWorkflowStepHandler handler) {
		HANDLERS.put(type, handler);
	}

	/**
	 * Check if a handler exists for the given type.
	 */
	public static boolean hasHandler(String type) {
		return HANDLERS.containsKey(type);
	}
}
