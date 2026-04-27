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

import java.util.HashMap;
import java.util.Map;

/**
 * Result of a single step execution.
 */
public class StepResult {

	private String stepId;
	private Status status;
	private Object output;
	private Map<String, Object> metadata = new HashMap<>();
	private String error;
	private long durationMs;

	public enum Status {
		SUCCESS, ERROR, SKIPPED
	}

	// ── Factory helpers ─────────────────────────────────────────────────

	public static StepResult success(String stepId, Object output, long durationMs) {
		StepResult r = new StepResult();
		r.stepId = stepId;
		r.status = Status.SUCCESS;
		r.output = output;
		r.durationMs = durationMs;
		return r;
	}

	public static StepResult error(String stepId, String errorMsg, long durationMs) {
		StepResult r = new StepResult();
		r.stepId = stepId;
		r.status = Status.ERROR;
		r.error = errorMsg;
		r.durationMs = durationMs;
		return r;
	}

	public static StepResult skipped(String stepId) {
		StepResult r = new StepResult();
		r.stepId = stepId;
		r.status = Status.SKIPPED;
		r.durationMs = 0;
		return r;
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getStepId() { return stepId; }
	public void setStepId(String stepId) { this.stepId = stepId; }

	public Status getStatus() { return status; }
	public void setStatus(Status status) { this.status = status; }

	public Object getOutput() { return output; }
	public void setOutput(Object output) { this.output = output; }

	public Map<String, Object> getMetadata() { return metadata; }
	public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

	public String getError() { return error; }
	public void setError(String error) { this.error = error; }

	public long getDurationMs() { return durationMs; }
	public void setDurationMs(long durationMs) { this.durationMs = durationMs; }
}
