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
package prerna.reactor.agent.run;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Projects code-owned delivery evidence without equating execution with fulfillment. */
final class AgentTaskOutcome {

	private AgentTaskOutcome() {
	}

	static Map<String, Object> fromRun(Map<String, Object> run) {
		Map<String, Object> outcome = new HashMap<>();
		outcome.put("status", "UNKNOWN");
		String status = String.valueOf(run.get("status"));
		Map<?, ?> workflow = run.get("progress") instanceof Map<?, ?> progress
				&& progress.get("workflow") instanceof Map<?, ?> value ? value : Map.of();
		boolean failed = "FAILED".equals(status) || "CANCELLED".equals(status);
		boolean completed = "COMPLETED".equals(status);
		if (!failed && !completed) {
			return outcome;
		}
		List<String> limitations = new ArrayList<>();
		addLimitation(limitations, run.get("errorMessage"));
		addLimitation(limitations, workflow.get("error"));
		addLimitation(limitations, workflow.get("warning"));
		boolean artifactVerified = workflow.get("artifact") instanceof Map<?, ?> artifact
				&& "available".equals(artifact.get("status"))
				&& "passed".equals(artifact.get("structuralValidation"))
				&& artifact.get("sourceHash") instanceof String hash && !hash.isBlank();
		if (failed) {
			boolean partial = "FAILED".equals(status) && "pptx".equals(workflow.get("workflow")) && artifactVerified;
			outcome.put("status", partial ? "PARTIAL" : "BLOCKED");
			outcome.put("summary", partial ? "A validated presentation was saved, but the task remains incomplete."
					: "CANCELLED".equals(status) ? "The run was cancelled."
							: "The run ended before the task could be completed.");
			if (limitations.isEmpty()) {
				limitations.add("Task completion could not be verified.");
			}
		} else if ("pptx".equals(workflow.get("workflow")) && artifactVerified
				&& ("delivered".equals(workflow.get("phase")) || "delivered_with_warnings".equals(workflow.get("phase")))) {
			boolean reviewed = workflow.get("reviewOutcome") instanceof Map<?, ?> review
					&& "complete".equals(review.get("status")) && "pass".equals(review.get("verdict"));
			boolean cleanPresentation = reviewed && limitations.isEmpty() && "delivered".equals(workflow.get("phase"));
			// Artifact checks only prove this deliverable; the user may also have requested
			// other actions (for example, emailing it). Full request fulfillment is unknown.
			outcome.put("status", cleanPresentation ? "UNKNOWN" : "PARTIAL");
			outcome.put("summary", cleanPresentation ? "The presentation was saved and its checks passed."
					: "A validated presentation was saved with limitations.");
			if (!reviewed) {
				addLimitation(limitations, "The presentation review has not verified that all requirements were met.");
			}
		}
		if (!limitations.isEmpty()) {
			outcome.put("limitations", limitations);
		}
		return outcome;
	}

	private static void addLimitation(List<String> values, Object value) {
		if (value instanceof String text && !text.isBlank() && !values.contains(text)) {
			values.add(text);
		}
	}
}
