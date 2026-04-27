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
 * Represents a single step (node) in a workflow DAG.
 */
public class WorkflowStep {

	// ── Step type enum ──────────────────────────────────────────────────

	public enum STEP_TYPE {
		LLM_ASK,
		LLM_AGENT,
		RUN_TOOL,
		RUN_PIXEL,
		RUN_PYTHON,
		CONDITION,
		LOOP,
		TRANSFORM,
		STATIC,
		HUMAN_INPUT,
		GUARDRAIL,
		OUTPUT
	}

	// ── Fields ──────────────────────────────────────────────────────────

	private String stepId;
	private String type;
	private String name;
	private String description;

	/** UI position — ignored by the backend executor */
	private Map<String, Object> position;

	/** Type-specific configuration (model id, prompt, recipe, etc.) */
	private Map<String, Object> config = new HashMap<>();

	/** Input mappings using {{template}} expressions */
	private Map<String, String> inputs = new HashMap<>();

	/** Default successor step IDs */
	private List<String> next;

	/** Successor step IDs when condition evaluates to true (CONDITION type only) */
	private List<String> ifTrue;

	/** Successor step IDs when condition evaluates to false (CONDITION type only) */
	private List<String> ifFalse;

	// ── Convenience ─────────────────────────────────────────────────────

	/**
	 * Returns the parsed STEP_TYPE enum, or null if the type string is invalid.
	 */
	public STEP_TYPE getStepType() {
		if (type == null) return null;
		try {
			return STEP_TYPE.valueOf(type);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Returns all successor step IDs (combines next, ifTrue, ifFalse).
	 * Used by DAG validation and cycle detection.
	 */
	public List<String> getAllSuccessorIds() {
		List<String> successors = new ArrayList<>();
		if (next != null) successors.addAll(next);
		if (ifTrue != null) successors.addAll(ifTrue);
		if (ifFalse != null) successors.addAll(ifFalse);
		return successors;
	}

	// ── Getters / Setters ───────────────────────────────────────────────

	public String getStepId() { return stepId; }
	public void setStepId(String stepId) { this.stepId = stepId; }

	public String getType() { return type; }
	public void setType(String type) { this.type = type; }

	public String getName() { return name; }
	public void setName(String name) { this.name = name; }

	public String getDescription() { return description; }
	public void setDescription(String description) { this.description = description; }

	public Map<String, Object> getPosition() { return position; }
	public void setPosition(Map<String, Object> position) { this.position = position; }

	public Map<String, Object> getConfig() { return config; }
	public void setConfig(Map<String, Object> config) { this.config = config; }

	public Map<String, String> getInputs() { return inputs; }
	public void setInputs(Map<String, String> inputs) { this.inputs = inputs; }

	public List<String> getNext() { return next; }
	public void setNext(List<String> next) { this.next = next; }

	public List<String> getIfTrue() { return ifTrue; }
	public void setIfTrue(List<String> ifTrue) { this.ifTrue = ifTrue; }

	public List<String> getIfFalse() { return ifFalse; }
	public void setIfFalse(List<String> ifFalse) { this.ifFalse = ifFalse; }
}
