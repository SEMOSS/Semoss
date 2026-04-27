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
package prerna.reactor.workflow.engine.handlers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import prerna.reactor.workflow.engine.WorkflowStep;

/**
 * Registry that maps step type strings to their handler implementations.
 */
public final class WorkflowStepHandlerRegistry {

	private static final Map<String, IWorkflowStepHandler> HANDLERS = new ConcurrentHashMap<>();

	private WorkflowStepHandlerRegistry() {}

	static {
		HANDLERS.put(WorkflowStep.STEP_TYPE.STATIC.name(), new StaticStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.CONDITION.name(), new ConditionStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.OUTPUT.name(), new OutputStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.RUN_PIXEL.name(), new RunPixelStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.RUN_TOOL.name(), new RunToolStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.LLM_ASK.name(), new LLMAskStepHandler());
		HANDLERS.put(WorkflowStep.STEP_TYPE.LLM_AGENT.name(), new LLMAgentStepHandler());
	}

	/**
	 * Get the handler for a given step type.
	 * 
	 * @param type the step type string (e.g., "LLM_ASK", "RUN_TOOL")
	 * @return the handler, or null if no handler is registered for this type
	 */
	public static IWorkflowStepHandler getHandler(String type) {
		return HANDLERS.get(type);
	}

	/**
	 * Register a custom handler for a step type.
	 * Can be used to override built-in handlers or add new types.
	 */
	public static void register(String type, IWorkflowStepHandler handler) {
		HANDLERS.put(type, handler);
	}

	/**
	 * Check if a handler exists for the given type.
	 */
	public static boolean hasHandler(String type) {
		return HANDLERS.containsKey(type);
	}
}
