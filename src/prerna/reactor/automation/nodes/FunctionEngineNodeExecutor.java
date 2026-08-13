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
package prerna.reactor.automation.nodes;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.util.Utility;

/**
 * Executes a function-engine automation node. Delegates to the platform
 * {@link prerna.engine.api.IFunctionEngine} API to run arbitrary server-side functions.
 *
 * <p>Config fields (from {@code node.config}):
 * <ul>
 *   <li>{@code engineId} (required) — UUID or alias of the target function engine</li>
 *   <li>{@code params} (optional) — JSON object of parameters to pass to the function; supports
 *       {@code ${var}} substitution; defaults to {@code {}}</li>
 * </ul>
 *
 * <p>Requires edit access to {@code engineId} because function execution runs arbitrary
 * server-side code, which is a mutating/write operation.
 */
public final class FunctionEngineNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(FunctionEngineNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = NodeConfigHelper.required(config, AutomationConstants.CONFIG_ENGINE_ID, nodeLabel);
		String params = NodeConfigHelper.optional(config, AutomationConstants.CONFIG_PARAMS, AutomationConstants.EMPTY_JSON_OBJECT);

		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);
		String resolvedParams = AutomationExecutionUtils.resolve(params, scope, configMap);

		resolvedEngineId = SecurityQueryUtils.testUserEngineIdForAlias(ctx.insight().getUser(), resolvedEngineId);
		if (!SecurityEngineUtils.userCanEditEngine(ctx.insight().getUser(), resolvedEngineId)) {
			throw new IllegalArgumentException(
					"Function-engine node \"" + nodeLabel + "\": engine does not exist or user does not have edit access: " + resolvedEngineId);
		}

		IFunctionEngine engine = Utility.getFunctionEngine(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Function-engine node \"" + nodeLabel + "\": engine not found: " + resolvedEngineId);
		}

		classLogger.debug("Function-engine node \"{}\" executing via engine {}", nodeLabel, resolvedEngineId);
		Map<String, Object> paramMap = parseParams(resolvedParams, nodeLabel);
		return engine.execute(paramMap);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseParams(String json, String nodeLabel) {
		if (json == null || json.isBlank()) return Map.of();
		try {
			Map<String, Object> parsed = AutomationExecutionUtils.GSON.fromJson(json,
					AutomationExecutionUtils.MAP_TYPE);
			return parsed != null ? parsed : Map.of();
		} catch (Exception e) {
			throw new IllegalArgumentException("Function-engine node \"" + nodeLabel + "\": params is not valid JSON: " + e.getMessage(), e);
		}
	}

}
