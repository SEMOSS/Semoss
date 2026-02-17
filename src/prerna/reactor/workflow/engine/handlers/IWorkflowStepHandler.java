package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;

/**
 * Interface for workflow step handlers.
 * Each step type (LLM_ASK, RUN_TOOL, CONDITION, etc.) has a handler
 * that knows how to execute that type of step.
 */
public interface IWorkflowStepHandler {

	/**
	 * Execute a workflow step.
	 * 
	 * @param stepId   the unique ID of the step being executed
	 * @param config   the step's resolved configuration (templates already replaced)
	 * @param context  the workflow runtime context (variables, prior step results)
	 * @param insight  the SEMOSS insight for engine/resource access
	 * @return the result of executing this step
	 */
	StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight);
}
