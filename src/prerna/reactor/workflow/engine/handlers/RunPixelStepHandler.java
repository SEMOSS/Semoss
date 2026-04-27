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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Executes a Pixel recipe string and returns its result.
 * 
 * Config:
 *   recipe — the Pixel expression to execute (e.g., "Query(engine=\"uuid\", query=\"SELECT ...\");")
 */
public class RunPixelStepHandler implements IWorkflowStepHandler {

	private static final Logger classLogger = LogManager.getLogger(RunPixelStepHandler.class);

	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		String recipe = (String) config.get("recipe");
		if (recipe == null || recipe.trim().isEmpty()) {
			return StepResult.error(stepId, "RunPixel step requires a 'recipe' in config",
					System.currentTimeMillis() - start);
		}

		// Block runtime template expressions in recipe to prevent Pixel injection via LLM output.
		// recipe values must be static strings set at workflow design time.
		if (recipe.contains("{{") && recipe.contains("}}")) {
			return StepResult.error(stepId,
					"RunPixel recipe must be a static Pixel expression. Dynamic template references are not permitted.",
					System.currentTimeMillis() - start);
		}

		try {
			PixelRunner runner = insight.runPixel(recipe);
			List<NounMetadata> results = runner.getResults();

			Object output = null;
			if (results != null && !results.isEmpty()) {
				NounMetadata lastResult = results.get(results.size() - 1);
				output = lastResult.getValue();
			}

			return StepResult.success(stepId, output, System.currentTimeMillis() - start);
		} catch (Exception e) {
			classLogger.error("RunPixel step '{}' failed", stepId, e);
			return StepResult.error(stepId, "Pixel execution failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}
	}
}
