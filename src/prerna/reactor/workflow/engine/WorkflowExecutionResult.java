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
package prerna.reactor.workflow.engine;

import java.util.Map;
import java.util.UUID;

/**
 * Final result of a complete workflow execution.
 */
public class WorkflowExecutionResult {

	private String executionId;
	private String workflowId;
	private Status status;
	private long startTimeMs;
	private long endTimeMs;
	private long durationMs;
	private String triggeredBy;
	private Map<String, StepResult> stepResults;
	private Object finalOutput;
	private String error;

	public enum Status {
		SUCCESS, ERROR, TIMEOUT
	}

	// ── Factory helpers ─────────────────────────────────────────────────

	public static WorkflowExecutionResult from(WorkflowContext context, String workflowId, String triggeredBy) {
		WorkflowExecutionResult result = new WorkflowExecutionResult();
		result.executionId = UUID.randomUUID().toString();
		result.workflowId = workflowId;
		result.startTimeMs = context.getStartTimeMs();
		result.endTimeMs = System.currentTimeMillis();
		result.durationMs = result.endTimeMs - result.startTimeMs;
		result.triggeredBy = triggeredBy;
		result.stepResults = context.getStepResults();

		// Determine overall status from step results
		boolean hasError = context.getStepResults().values().stream()
				.anyMatch(r -> r.getStatus() == StepResult.Status.ERROR);
		result.status = hasError ? Status.ERROR : Status.SUCCESS;

		return result;
	}

	public static WorkflowExecutionResult timeout(WorkflowContext context, String workflowId, String triggeredBy) {
		WorkflowExecutionResult result = from(context, workflowId, triggeredBy);
		result.status = Status.TIMEOUT;
		result.error = "Workflow exceeded timeout of " + context.getElapsedMs() + "ms";
		return result;
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getExecutionId() { return executionId; }
	public void setExecutionId(String executionId) { this.executionId = executionId; }

	public String getWorkflowId() { return workflowId; }
	public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }

	public Status getStatus() { return status; }
	public void setStatus(Status status) { this.status = status; }

	public long getStartTimeMs() { return startTimeMs; }
	public void setStartTimeMs(long startTimeMs) { this.startTimeMs = startTimeMs; }

	public long getEndTimeMs() { return endTimeMs; }
	public void setEndTimeMs(long endTimeMs) { this.endTimeMs = endTimeMs; }

	public long getDurationMs() { return durationMs; }
	public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

	public String getTriggeredBy() { return triggeredBy; }
	public void setTriggeredBy(String triggeredBy) { this.triggeredBy = triggeredBy; }

	public Map<String, StepResult> getStepResults() { return stepResults; }
	public void setStepResults(Map<String, StepResult> stepResults) { this.stepResults = stepResults; }

	public Object getFinalOutput() { return finalOutput; }
	public void setFinalOutput(Object finalOutput) { this.finalOutput = finalOutput; }

	public String getError() { return error; }
	public void setError(String error) { this.error = error; }
}
