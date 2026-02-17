package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;

/**
 * Terminal step that passes through its input as the workflow's final output.
 * 
 * Config:
 *   value — the value to output (typically a template reference like {{prev-step.output}})
 */
public class OutputStepHandler implements IWorkflowStepHandler {

	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();
		Object value = config.get("value");
		return StepResult.success(stepId, value, System.currentTimeMillis() - start);
	}
}
