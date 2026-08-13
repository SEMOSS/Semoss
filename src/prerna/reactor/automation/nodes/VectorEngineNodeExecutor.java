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
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.util.Utility;

/**
 * Executes a vector-engine automation node. Supports document ingestion ({@code add-file},
 * {@code add-csv}), deletion ({@code delete}), listing ({@code list}), and semantic search
 * ({@code search}) via the platform {@link prerna.engine.api.IVectorDatabaseEngine} API.
 *
 * <p>Config fields (from {@code node.config}):
 * <ul>
 *   <li>{@code engineId} (required) — UUID or alias of the target vector engine</li>
 *   <li>{@code operation} (optional) — one of {@code search} (default), {@code add-file},
 *       {@code add-csv}, {@code delete}, {@code list}</li>
 *   <li>{@code filePath} (required for add-file/add-csv) — comma-separated file paths</li>
 *   <li>{@code fileNames} (required for delete) — comma-separated document names to remove</li>
 *   <li>{@code command} (required for search) — the query string</li>
 *   <li>{@code limit} (optional for search) — max results; default {@value AutomationConstants#DEFAULT_VECTOR_SEARCH_LIMIT}</li>
 * </ul>
 */
public final class VectorEngineNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(VectorEngineNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = NodeConfigHelper.required(config, AutomationConstants.CONFIG_ENGINE_ID, nodeLabel);
		String operation = NodeConfigHelper.optional(config, AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_SEARCH);
		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);

		resolvedEngineId = SecurityQueryUtils.testUserEngineIdForAlias(ctx.insight().getUser(), resolvedEngineId);
		boolean mutating = AutomationConstants.OP_ADD_FILE.equals(operation)
				|| AutomationConstants.OP_ADD_CSV.equals(operation)
				|| AutomationConstants.OP_DELETE.equals(operation);
		boolean authorized = mutating
				? SecurityEngineUtils.userCanEditEngine(ctx.insight().getUser(), resolvedEngineId)
				: SecurityEngineUtils.userCanViewEngine(ctx.insight().getUser(), resolvedEngineId);
		if (!authorized) {
			throw new IllegalArgumentException(
					"Vector-engine node \"" + nodeLabel + "\": engine does not exist or user does not have access: " + resolvedEngineId);
		}

		IVectorDatabaseEngine engine = Utility.getVectorDatabase(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Vector-engine node \"" + nodeLabel + "\": engine not found: " + resolvedEngineId);
		}

		classLogger.debug("Vector-engine node \"{}\" executing operation={} via engine {}", nodeLabel, operation, resolvedEngineId);
		switch (operation) {
			case AutomationConstants.OP_ADD_FILE:
			case AutomationConstants.OP_ADD_CSV: {
				String filePaths = NodeConfigHelper.required(config, AutomationConstants.CONFIG_FILE_PATH, nodeLabel);
				String resolvedPaths = AutomationExecutionUtils.resolve(filePaths, scope, configMap);
				List<String> paths = Arrays.stream(resolvedPaths.split(","))
						.map(String::trim)
						.filter(s -> !s.isEmpty())
						.collect(Collectors.toList());
				engine.addDocument(paths, null);
				return "Added " + paths.size() + " file(s)";
			}
			case AutomationConstants.OP_LIST: {
				List<Map<String, Object>> docs = engine.listDocuments(null);
				return docs;
			}
			case AutomationConstants.OP_DELETE: {
				String fileNames = NodeConfigHelper.required(config, AutomationConstants.CONFIG_FILE_NAMES, nodeLabel);
				String resolvedNames = AutomationExecutionUtils.resolve(fileNames, scope, configMap);
				List<String> names = Arrays.stream(resolvedNames.split(","))
						.map(String::trim)
						.filter(s -> !s.isEmpty())
						.collect(Collectors.toList());
				engine.removeDocument(names, null);
				return "Deleted " + names.size() + " file(s)";
			}
			default: {
				// search
				String command = NodeConfigHelper.required(config, AutomationConstants.CONFIG_COMMAND, nodeLabel);
				String resolvedCommand = AutomationExecutionUtils.resolve(command, scope, configMap);
				int limit = NodeConfigHelper.optionalInt(config, AutomationConstants.CONFIG_LIMIT, AutomationConstants.DEFAULT_VECTOR_SEARCH_LIMIT);
				List<Map<String, Object>> results = engine.nearestNeighbor(ctx.insight(), resolvedCommand, limit, null);
				return results;
			}
		}
	}

}
