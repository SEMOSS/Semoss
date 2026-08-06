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
package prerna.reactor.automation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Uses an LLM to generate a SQL SELECT query for a given database engine from a plain-English description.
 *
 * <p>Pixel: {@code GenerateSQL(database=["engineId"], description=["base64EncodedDescription"])}
 *
 * <p>Fetches the engine's schema, finds the first available MODEL engine, and returns a SQL string.
 * The caller is expected to review and edit the result before running it.
 */
public final class GenerateSQLReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateSQLReactor.class);

	private static final String SYSTEM_PROMPT = """
You are a SQL expert. Given a database schema and a plain-English description, write a single SQL SELECT query that retrieves the requested data.

Rules:
- Use only the tables and columns that exist in the schema
- Use column types to write type-safe SQL — don't compare VARCHAR columns to integers
- Do not add conditions or filters not implied by the description
- Include a LIMIT clause appropriate to the request (e.g. LIMIT 100 for "get all X" queries)
- Return ONLY the SQL statement — no markdown, no explanation, no code fences

## Database schema
""";

	public GenerateSQLReactor() {
		this.keysToGet = new String[] {
			ReactorKeysEnum.DATABASE.getKey(),
			AutomationConstants.DOC_DESCRIPTION,
			ReactorKeysEnum.ENGINE.getKey()
		};
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in.");
		}

		String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
		String description = this.keyValue.get(AutomationConstants.DOC_DESCRIPTION);
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		if (databaseId == null || databaseId.trim().isEmpty()) {
			throw new IllegalArgumentException("A database engine ID is required.");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
			throw new IllegalArgumentException("Database engine does not exist or user does not have access.");
		}

		if (description == null || description.trim().isEmpty()) {
			throw new IllegalArgumentException("A description of the data to retrieve is required.");
		}
		// Decode base64-encoded description from FE
		try {
			description = new String(
				java.util.Base64.getDecoder().decode(description.trim()),
				java.nio.charset.StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			// Not base64-encoded — use as-is
		}

		// Resolve model engine
		if (engineId == null || engineId.trim().isEmpty()) {
			engineId = findFirstModelEngine(user);
		}
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"No AI model engine is available. Add a model engine connection to use SQL generation.");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model engine does not exist or user does not have access.");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException("Model engine could not be loaded.");
		}

		// Build schema section
		String schema = buildSchemaForEngine(databaseId);

		// Call LLM
		String prompt = SYSTEM_PROMPT + schema + "\n## Request\n" + description.trim();
		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("use_history", false);

		Map<String, Object> response;
		try {
			response = modelEngine.ask(prompt, null, this.insight, paramMap).toMap();
		} catch (Exception e) {
			classLogger.error("LLM call failed for GenerateSQL on database {}", databaseId, e);
			throw new RuntimeException("SQL generation failed: " + e.getMessage(), e);
		}

		String sql = extractResponseText(response);
		if (sql == null || sql.isBlank()) {
			throw new IllegalStateException("The AI model did not return a response. Try again.");
		}
		sql = stripCodeFences(sql).trim();

		return new NounMetadata(sql, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	private static String buildSchemaForEngine(String engineId) {
		StringBuilder sb = new StringBuilder();
		try {
			IDatabaseEngine dbEngine = Utility.getDatabase(engineId);
			if (!(dbEngine instanceof IRDBMSEngine rdbms)) {
				return "(non-relational engine — no table schema available)\n";
			}
			List<String> tables = rdbms.getPixelConcepts();
			if (tables == null || tables.isEmpty()) {
				return "(no tables found)\n";
			}
			for (String table : tables) {
				List<String> columns = rdbms.getPixelSelectors(table);
				sb.append(table).append(" (");
				if (columns != null && !columns.isEmpty()) {
					StringBuilder cols = new StringBuilder();
					for (String col : columns) {
						String colName = col.contains("__") ? col.split("__")[1] : col;
						String dataType = null;
						try {
							String physUri = rdbms.getPhysicalUriFromPixelSelector(col);
							if (physUri != null) dataType = rdbms.getDataTypes(physUri);
						} catch (Exception ignored) { }
						if (cols.length() > 0) cols.append(", ");
						cols.append(colName);
						if (dataType != null && !dataType.isBlank()) {
							cols.append(": ").append(dataType.trim());
						}
					}
					sb.append(cols);
				}
				sb.append(")\n");
			}
		} catch (Exception e) {
			classLogger.warn("Failed to fetch schema for engine {}", engineId, e);
			return "(schema unavailable)\n";
		}
		return sb.toString();
	}

	private static String findFirstModelEngine(User user) {
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, List.of("MODEL"), null, false, null, null, null, "1", "0", null);
			if (engines != null && !engines.isEmpty()) {
				Object id = engines.get(0).get("database_id");
				return id != null ? String.valueOf(id) : null;
			}
		} catch (Exception e) {
			classLogger.warn("Failed to auto-discover model engine for GenerateSQL", e);
		}
		return null;
	}

	private static String extractResponseText(Map<String, Object> response) {
		if (response == null) return null;
		Object resp = response.get("response");
		if (resp instanceof String s && !s.isBlank()) return s;
		Object output = response.get("output");
		if (output instanceof String s && !s.isBlank()) return s;
		Object content = response.get("content");
		if (content instanceof String s && !s.isBlank()) return s;
		return null;
	}

	private static String stripCodeFences(String raw) {
		String trimmed = raw.strip();
		if (trimmed.startsWith("```")) {
			int firstNewline = trimmed.indexOf('\n');
			if (firstNewline != -1) trimmed = trimmed.substring(firstNewline + 1);
			if (trimmed.endsWith("```")) trimmed = trimmed.substring(0, trimmed.lastIndexOf("```")).strip();
		}
		return trimmed;
	}

	@Override
	public String getReactorDescription() {
		return "Generates a SQL SELECT query from a plain-English description using the database schema and an AI model.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.DATABASE.getKey().equals(key)) return "ID of the database engine to query.";
		if (AutomationConstants.DOC_DESCRIPTION.equals(key)) return "Plain-English description of what data to retrieve (base64-encoded).";
		if (ReactorKeysEnum.ENGINE.getKey().equals(key)) return "Optional model engine ID. Defaults to the first available MODEL engine.";
		return super.getDescriptionForKey(key);
	}
}
