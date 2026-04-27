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
