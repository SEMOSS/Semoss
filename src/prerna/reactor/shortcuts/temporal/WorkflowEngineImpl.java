package prerna.reactor.shortcuts.temporal;

import java.time.Duration;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

public class WorkflowEngineImpl implements WorkflowEngine {

	@Override
	public void start(String workflowKey, String workflowJson, String triggerType, String filePath) {
		// Configure retry policy for activities
		RetryOptions retryOptions = RetryOptions.newBuilder()
				.setInitialInterval(Duration.ofSeconds(1))
				.setMaximumInterval(Duration.ofSeconds(30))
				.setBackoffCoefficient(2.0)
				.setMaximumAttempts(5)
				.build();

		ActivityOptions activityOptions = ActivityOptions.newBuilder()
				.setStartToCloseTimeout(Duration.ofMinutes(15))
				.setRetryOptions(retryOptions)
				.setHeartbeatTimeout(Duration.ofSeconds(30))
				.build();

		WorkflowActivity activity = Workflow.newActivityStub(WorkflowActivity.class, activityOptions);

		try {
			// Generate UUID deterministically for workflow context
			String uniqueId = workflowKey + "_" + System.nanoTime();

			// Call activities directly - Temporal automatically yields control on activity calls
			// Step 1: Load workflow definition
			WorkflowDefinition definition = activity.loadDefinition(workflowKey);
			
			// Explicit yield point
			Workflow.sleep(Duration.ofMillis(100));

			// Step 2: Save workflow execution
			long executionId = activity.saveWorkflowExecution(workflowKey, triggerType, filePath, "IN_PROGRESS");
			
			// Explicit yield point
			Workflow.sleep(Duration.ofMillis(100));

			// Step 3: Execute workflow - this activity contains the main loop
			activity.executeWorkflow(definition, executionId, workflowKey, filePath, uniqueId);

		} catch (Exception e) {
			throw new RuntimeException("Workflow failed: " + e.getMessage(), e);
		}
	}
}
