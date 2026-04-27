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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;
import prerna.util.Utility;

/**
 * Executes an MCP tool on an engine and returns the result.
 * 
 * Config:
 *   engineId  — the engine/project ID that hosts the tool
 *   toolName  — the tool name to invoke
 *   params    — Map of parameters to pass to the tool
 */
public class RunToolStepHandler implements IWorkflowStepHandler {

	private static final Logger classLogger = LogManager.getLogger(RunToolStepHandler.class);

	@SuppressWarnings("unchecked")
	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		String engineId = (String) config.get("engineId");
		String toolName = (String) config.get("toolName");
		Map<String, Object> params = (Map<String, Object>) config.get("params");

		if (engineId == null || engineId.isEmpty()) {
			return StepResult.error(stepId, "RunTool step requires 'engineId' in config",
					System.currentTimeMillis() - start);
		}
		if (toolName == null || toolName.isEmpty()) {
			return StepResult.error(stepId, "RunTool step requires 'toolName' in config",
					System.currentTimeMillis() - start);
		}

		try {
			if (!SecurityEngineUtils.userCanViewEngine(insight.getUser(), engineId)) {
				return StepResult.error(stepId, "User does not have access to engine: " + engineId,
						System.currentTimeMillis() - start);
			}
			IEngine engine = Utility.getEngine(engineId);
			if (engine == null) {
				return StepResult.error(stepId, "Engine not found: " + engineId,
						System.currentTimeMillis() - start);
			}

			IMCP mcp = MCPFactory.build(engine);
			Object output = mcp.callTool(toolName, params, insight);

			return StepResult.success(stepId, output, System.currentTimeMillis() - start);
		} catch (Exception e) {
			classLogger.error("RunTool step '{}' failed", stepId, e);
			return StepResult.error(stepId, "Tool execution failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}
	}
}
