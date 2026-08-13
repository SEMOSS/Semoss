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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.util.Utility;

/**
 * Executes a model-engine automation node. Supports LLM ({@code llm}) and embeddings
 * ({@code embeddings}) operations via the platform {@link prerna.engine.api.IModelEngine} API.
 */
public final class ModelEngineNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(ModelEngineNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = NodeConfigHelper.required(config, AutomationConstants.CONFIG_ENGINE_ID, nodeLabel);
		String operation = NodeConfigHelper.optional(config, AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_LLM);
		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);

		resolvedEngineId = SecurityQueryUtils.testUserEngineIdForAlias(ctx.insight().getUser(), resolvedEngineId);
		if (!SecurityEngineUtils.userCanViewEngine(ctx.insight().getUser(), resolvedEngineId)) {
			throw new IllegalArgumentException(
					"Model-engine node \"" + nodeLabel + "\": engine does not exist or user does not have access: " + resolvedEngineId);
		}

		IModelEngine engine = Utility.getModel(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Model-engine node \"" + nodeLabel + "\": engine not found: " + resolvedEngineId);
		}

		classLogger.debug("Model-engine node \"{}\" executing operation={} via engine {}", nodeLabel, operation, resolvedEngineId);
		switch (operation) {
			case AutomationConstants.OP_EMBEDDINGS: {
				String values = NodeConfigHelper.required(config, AutomationConstants.CONFIG_VALUES, nodeLabel);
				String resolvedValues = AutomationExecutionUtils.resolve(values, scope, configMap);
				List<String> valueList = Arrays.stream(resolvedValues.split(","))
						.map(String::trim)
						.filter(s -> !s.isEmpty())
						.collect(Collectors.toList());
				EmbeddingsModelEngineResponse response = engine.embeddings(valueList, ctx.insight(), null);
				return response.getResponse();
			}
			default: {
				// llm (and vision/ner as fallback — both use ask() with the primary command field)
				String command = NodeConfigHelper.required(config, AutomationConstants.CONFIG_COMMAND, nodeLabel);
				String resolvedCommand = AutomationExecutionUtils.resolve(command, scope, configMap);
				String context = NodeConfigHelper.optional(config, AutomationConstants.CONFIG_CONTEXT);
				String resolvedContext = (context != null)
						? AutomationExecutionUtils.resolve(context, scope, configMap) : null;
				Map<String, Object> params = parseParams(config, scope, configMap, nodeLabel);
				@SuppressWarnings("rawtypes")
				AskModelEngineResponse response = engine.ask(resolvedCommand, resolvedContext, ctx.insight(), params);
				return response.getResponse();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseParams(Map<String, Object> config,
			Map<String, String> scope, Map<String, String> configMap, String nodeLabel) {
		String paramValues = NodeConfigHelper.optional(config, AutomationConstants.CONFIG_PARAM_VALUES);
		if (paramValues == null) return null;
		String resolved = AutomationExecutionUtils.resolve(paramValues, scope, configMap);
		try {
			return AutomationExecutionUtils.GSON.fromJson(resolved, AutomationExecutionUtils.MAP_TYPE);
		} catch (Exception e) {
			throw new IllegalArgumentException("Model-engine node \"" + nodeLabel + "\": paramValues is not valid JSON: " + e.getMessage(), e);
		}
	}

}
