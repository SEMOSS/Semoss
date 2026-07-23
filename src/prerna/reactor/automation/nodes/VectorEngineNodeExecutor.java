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

import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.automation.AutomationExecutionUtils;
import prerna.util.Utility;

public final class VectorEngineNodeExecutor implements IAutomationNodeExecutor {

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = required(config, "engineId", nodeLabel);
		String operation = optional(config, "operation", "search");
		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);

		IVectorDatabaseEngine engine = Utility.getVectorDatabase(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Vector-engine node \"" + nodeLabel + "\": engine not found: " + resolvedEngineId);
		}

		switch (operation) {
			case "add-file":
			case "add-csv": {
				String filePaths = required(config, "filePath", nodeLabel);
				String resolvedPaths = AutomationExecutionUtils.resolve(filePaths, scope, configMap);
				List<String> paths = Arrays.asList(resolvedPaths.split(","));
				engine.addDocument(paths, null);
				return "Added " + paths.size() + " file(s)";
			}
			case "list": {
				List<Map<String, Object>> docs = engine.listDocuments(null);
				return docs;
			}
			case "delete": {
				String fileNames = required(config, "fileNames", nodeLabel);
				String resolvedNames = AutomationExecutionUtils.resolve(fileNames, scope, configMap);
				List<String> names = Arrays.asList(resolvedNames.split(","));
				engine.removeDocument(names, null);
				return "Deleted " + names.size() + " file(s)";
			}
			default: {
				// search
				String command = required(config, "command", nodeLabel);
				String resolvedCommand = AutomationExecutionUtils.resolve(command, scope, configMap);
				int limit = optionalInt(config, "limit", 5);
				List<Map<String, Object>> results = engine.nearestNeighbor(ctx.insight(), resolvedCommand, limit, null);
				return results;
			}
		}
	}

	private static String required(Map<String, Object> config, String key, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException("Vector-engine node \"" + nodeLabel + "\": '" + key + "' is required");
		}
		return v.toString();
	}

	private static String optional(Map<String, Object> config, String key, String def) {
		Object v = config.get(key);
		return (v == null || v.toString().isBlank()) ? def : v.toString();
	}

	private static int optionalInt(Map<String, Object> config, String key, int def) {
		Object v = config.get(key);
		if (v == null) return def;
		try { return Integer.parseInt(v.toString().trim()); }
		catch (NumberFormatException e) { return def; }
	}
}
