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

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationExecutionUtils;
import prerna.reactor.automation.PixelExecutionUtils;

/**
 * Executor for {@code database-engine} nodes. Runs a SQL query against a configured database engine
 * using the {@code SqlQuery} reactor — the same engine abstraction used elsewhere in the platform.
 *
 * <p>Config fields:
 * <ul>
 *   <li>{@code engineId} (required) — UUID or alias of the target database engine</li>
 *   <li>{@code expression} (required) — SQL expression; supports {@code ${var}} substitution</li>
 *   <li>{@code operation} (optional) — {@code "read"} (default) or {@code "write"}; informational
 *       only since {@code SqlQuery} auto-detects SELECT vs DML from the SQL text</li>
 *   <li>{@code limit} (optional) — max rows for SELECT results; default 50</li>
 * </ul>
 */
public final class DatabaseEngineNodeExecutor implements IAutomationNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(DatabaseEngineNodeExecutor.class);

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = required(config, AutomationConstants.CONFIG_ENGINE_ID, nodeLabel);
		String sql = required(config, AutomationConstants.CONFIG_EXPRESSION, nodeLabel);
		String operation = optional(config, AutomationConstants.CONFIG_OPERATION, AutomationConstants.OP_READ);
		int limit = optionalInt(config, AutomationConstants.CONFIG_LIMIT, AutomationConstants.DEFAULT_DB_QUERY_LIMIT);

		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);
		String resolvedSql = AutomationExecutionUtils.resolve(sql, scope, configMap);

		classLogger.debug("Database-engine node \"{}\" executing operation={} via engine {}", nodeLabel, operation, resolvedEngineId);

		// Escape double quotes in the SQL and engine id for the pixel string literal, then
		// delegate to SqlQuery which uses the engine abstraction (HardSelectQueryStruct) and
		// enforces the caller's database-level permissions automatically.
		String escapedEngineId = resolvedEngineId.replace("\"", "\\\"");
		String escapedSql = resolvedSql.replace("\"", "\\\"");
		String pixel = "SqlQuery(database=[\"" + escapedEngineId + "\"], query=[\"" + escapedSql + "\"], limit=[" + limit + "]);";

		int timeout = AutomationExecutionUtils.getNodeTimeout(ctx.node());
		return PixelExecutionUtils.runAndCollect(ctx.insight(), pixel, timeout);
	}

	private static String required(Map<String, Object> config, String key, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException("Database-engine node \"" + nodeLabel + "\": '" + key + "' is required");
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
