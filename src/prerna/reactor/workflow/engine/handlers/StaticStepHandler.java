package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;

/**
 * Simplest handler — returns a static value from config.
 * Useful for injecting constants, default values, or test data into a workflow.
 * 
 * Config:
 *   value — the value to output (any type)
 */
public class StaticStepHandler implements IWorkflowStepHandler {

	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();
		Object value = config.get("value");
		return StepResult.success(stepId, value, System.currentTimeMillis() - start);
	}
}
