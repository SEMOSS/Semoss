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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.automation.AutomationExecutionUtils;
import prerna.util.Utility;

public final class DatabaseEngineNodeExecutor implements IAutomationNodeExecutor {

	@Override
	public Object execute(AutomationNodeContext ctx) throws Exception {
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();

		String engineId = required(config, "engineId", nodeLabel);
		String sql = required(config, "expression", nodeLabel);
		String operation = optional(config, "operation", "read");

		String resolvedEngineId = AutomationExecutionUtils.resolve(engineId, scope, configMap);
		String resolvedSql = AutomationExecutionUtils.resolve(sql, scope, configMap);

		IRDBMSEngine engine = (IRDBMSEngine) Utility.getEngine(resolvedEngineId);
		if (engine == null) {
			throw new IllegalArgumentException("Database-engine node \"" + nodeLabel + "\": engine not found: " + resolvedEngineId);
		}

		if ("write".equals(operation)) {
			try (Connection conn = engine.getConnection();
				 PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
				int rowsAffected = ps.executeUpdate();
				return Map.of("rowsAffected", rowsAffected);
			} catch (SQLException e) {
				throw new IllegalStateException("Database-engine node \"" + nodeLabel + "\": write failed: " + e.getMessage(), e);
			}
		} else {
			int limit = optionalInt(config, "limit", 50);
			try (Connection conn = engine.getConnection();
				 PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
				try (ResultSet rs = ps.executeQuery()) {
					ResultSetMetaData meta = rs.getMetaData();
					int colCount = meta.getColumnCount();
					List<Map<String, Object>> rows = new ArrayList<>();
					int count = 0;
					while (rs.next() && count < limit) {
						Map<String, Object> row = new LinkedHashMap<>();
						for (int i = 1; i <= colCount; i++) {
							row.put(meta.getColumnLabel(i), rs.getObject(i));
						}
						rows.add(row);
						count++;
					}
					return rows;
				}
			} catch (SQLException e) {
				throw new IllegalStateException("Database-engine node \"" + nodeLabel + "\": query failed: " + e.getMessage(), e);
			}
		}
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
