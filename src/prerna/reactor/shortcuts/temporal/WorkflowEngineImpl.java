/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.shortcuts.temporal;

import java.time.Duration;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

public class WorkflowEngineImpl implements WorkflowEngine {

	@Override
	public void start(String workflowKey, String workflowJson, String triggerType, String filePath) {
		// Wait for workflow context to initialize
		Workflow.sleep(Duration.ofMillis(100));

		// Configure retry policy for activities
		RetryOptions retryOptions = RetryOptions.newBuilder()
				.setInitialInterval(Duration.ofSeconds(1))
				.setMaximumInterval(Duration.ofSeconds(30))
				.setBackoffCoefficient(2.0)
				.setMaximumAttempts(3)
				.build();

		ActivityOptions activityOptions = ActivityOptions.newBuilder()
				.setStartToCloseTimeout(Duration.ofMinutes(5))
				.setRetryOptions(retryOptions)
				.build();

		WorkflowActivity activity = Workflow.newActivityStub(WorkflowActivity.class, activityOptions);

		try {
			// Generate UUID deterministically for workflow context
			String uniqueId = workflowKey + "_" + System.nanoTime();

			// Load definition
			WorkflowDefinition definition = activity.loadDefinition(workflowKey);

			// Save execution
			long executionId = activity.saveWorkflowExecution(workflowKey, triggerType, filePath, "IN_PROGRESS");

			// Execute workflow
			activity.executeWorkflow(definition, executionId, workflowKey, filePath, uniqueId);

		} catch (Exception e) {
			throw new RuntimeException("Workflow failed: " + e.getMessage(), e);
		}
	}
}
