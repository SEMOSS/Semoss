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
