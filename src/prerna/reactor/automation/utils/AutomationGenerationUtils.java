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
package prerna.reactor.automation.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.reactor.automation.AutomationConstants;

/**
 * LLM/generation helpers used exclusively by the AI reactor group:
 * {@link prerna.reactor.automation.BuildAutomationReactor},
 * {@link prerna.reactor.automation.ExplainAutomationReactor}, {@link prerna.reactor.automation.GenerateNodeLabelReactor},
 * {@link prerna.reactor.automation.GenerateRunSummaryReactor}, {@link prerna.reactor.automation.QuickEditAutomationReactor},
 * {@link prerna.reactor.automation.AutomationAskRoomReactor}, and {@link prerna.reactor.automation.GetReactorSignatureReactor}.
 *
 * <p>Execution-time helpers (resolve, scope building, output transforms, etc.)
 * live in {@link AutomationExecutionUtils}.
 */
public final class AutomationGenerationUtils {

	private static final Logger classLogger = LogManager.getLogger(AutomationGenerationUtils.class);

	/** Engine types listed in the generation prompt so the LLM can populate engineId fields. */
	static final List<String> GENERATION_ENGINE_TYPES = Arrays.asList("DATABASE", "MODEL", "VECTOR", "STORAGE", "FUNCTION");

	private AutomationGenerationUtils() {}

	/**
	 * Returns the ID of the first MODEL-type engine the user has access to, or {@code null} if none.
	 */
	public static String findFirstModelEngine(User user) {
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, List.of("MODEL"), null, false, null, null, null, "1", "0", null);
			if (engines != null && !engines.isEmpty()) {
				Object id = engines.get(0).get("database_id");
				return id != null ? String.valueOf(id) : null;
			}
		} catch (Exception e) {
			classLogger.warn("Failed to auto-discover model engine", e);
		}
		return null;
	}

	/**
	 * Extracts the text content from a model engine response map.
	 * Checks {@code response}, {@code output}, and {@code content} keys in order.
	 */
	public static String extractResponseText(Map<String, Object> response) {
		if (response == null) return null;
		Object resp = response.get("response");
		if (resp instanceof String s && !s.isBlank()) return s;
		Object output = response.get("output");
		if (output instanceof String s && !s.isBlank()) return s;
		Object content = response.get("content");
		if (content instanceof String s && !s.isBlank()) return s;
		return null;
	}

	/**
	 * Builds the Room options map for RunAgent - registers all user-accessible engines
	 * as MCP tools and includes the room's built-in tools.
	 */
	public static Map<String, Object> buildEngineMcpOptions(User user, String systemPrompt) {
		List<Map<String, Object>> mcpList = new ArrayList<>();
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, GENERATION_ENGINE_TYPES, null, false, null, null, null, "50", "0", null);
			if (engines != null) {
				for (Map<String, Object> engine : engines) {
					String id = engine.getOrDefault("database_id", "").toString();
					String name = engine.getOrDefault("database_name", "").toString();
					if (id.isBlank()) continue;
					try {
						IEngine engineObj = Utility.getEngine(id);
						if (engineObj == null || !engineObj.isMCPEnabled()) continue;
					} catch (Exception e) {
						classLogger.debug("Skipping engine {} from MCP list - could not load: {}", id, e.getMessage());
						continue;
					}
					Map<String, Object> entry = new HashMap<>();
					entry.put("id", id);
					entry.put("name", name);
					entry.put("type", "ENGINE");
					mcpList.add(entry);
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to build MCP engine list for automation room", e);
		}
		Map<String, Object> dbMakerEntry = new HashMap<>();
		dbMakerEntry.put("id", Constants.MCP_DATABASE_MAKER);
		dbMakerEntry.put("name", "Database Tools");
		dbMakerEntry.put("type", "PROJECT");
		mcpList.add(dbMakerEntry);
		Map<String, Object> roomEntry = new HashMap<>();
		roomEntry.put("id", MCPUtility.ROOM_MCP_ID);
		roomEntry.put("name", MCPUtility.ROOM_MCP_NAME);
		roomEntry.put("type", MCPUtility.ROOM_MCP_TYPE);
		roomEntry.put("fromRoom", true);
		mcpList.add(roomEntry);
		Map<String, Object> options = new HashMap<>();
		if (systemPrompt != null && !systemPrompt.isBlank()) {
			options.put("instructions", systemPrompt);
		}
		options.put("mcp", mcpList);
		return options;
	}

	/**
	 * Builds the "## Available engines" prompt section for generation - lists each engine the
	 * current user can access so the LLM can assign engineId fields correctly.
	 */
	public static String buildAvailableEnginesSection(User user) {
		StringBuilder sb = new StringBuilder("## Available engines\n");
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, GENERATION_ENGINE_TYPES, null, false, null, null, null, "50", "0", null);
			if (engines == null || engines.isEmpty()) {
				sb.append("None available - leave engineId as empty string.\n");
				return sb.toString();
			}
			for (Map<String, Object> engine : engines) {
				String id = engine.getOrDefault("database_id", "").toString();
				String name = engine.getOrDefault("database_name", "").toString();
				String type = engine.getOrDefault("engine_type", "").toString().toUpperCase();
				sb.append("- type=").append(type)
				  .append(" id=\"").append(id)
				  .append("\" name=\"").append(name).append("\"");
				// For relational databases, include the table list so the model can match
				// the user's intent to the right DB without needing to call get_db_schema first.
				if ("DATABASE".equals(type) && !id.isBlank()) {
					try {
						IDatabaseEngine dbEngine = Utility.getDatabase(id);
						if (dbEngine instanceof IRDBMSEngine rdbms) {
							List<String> tables = rdbms.getPixelConcepts();
							if (tables != null && !tables.isEmpty()) {
								sb.append(" tables=[");
								for (int i = 0; i < tables.size(); i++) {
									String table = tables.get(i);
									// Strip the "ConceptName__" prefix that pixel concepts sometimes include
									int sep = table.lastIndexOf("__");
									sb.append(sep >= 0 ? table.substring(sep + 2) : table);
									if (i < tables.size() - 1) sb.append(", ");
								}
								sb.append("]");
							}
						}
					} catch (Exception e) {
						// Table list is best-effort; the model can still call get_db_schema if needed
					}
				}
				sb.append("\n");
			}
		} catch (Exception e) {
			classLogger.warn("Failed to build engine list for generation prompt", e);
			sb.append("(engine list unavailable - leave engineId as empty string)\n");
		}
		return sb.toString();
	}

	/**
	 * Fetches the table/column schema (with data types) for the given list of database engine IDs.
	 * Formatted as a prompt section for LLM use.
	 */
	public static String buildSchemaForEngineIds(List<String> engineIds) {
		StringBuilder sb = new StringBuilder("## Database schema\n");
		for (String engineId : engineIds) {
			try {
				IDatabaseEngine dbEngine = Utility.getDatabase(engineId);
				if (!(dbEngine instanceof IRDBMSEngine rdbms)) {
					sb.append("Database id=\"").append(engineId).append("\": (non-relational - no table schema)\n");
					continue;
				}
				sb.append("Database id=\"").append(engineId).append("\":\n");
				List<String> tables = rdbms.getPixelConcepts();
				if (tables == null || tables.isEmpty()) {
					sb.append("  (no tables found)\n");
					continue;
				}
				for (String table : tables) {
					List<String> columns = rdbms.getPixelSelectors(table);
					sb.append("  - ").append(table).append(" (");
					if (columns != null && !columns.isEmpty()) {
						StringBuilder cols = new StringBuilder();
						for (String col : columns) {
							int sep = col.lastIndexOf("__");
							String colName = sep >= 0 ? col.substring(sep + 2) : col;
							String dataType = null;
							try {
								String physicalUri = rdbms.getPhysicalUriFromPixelSelector(col);
								if (physicalUri != null) {
									dataType = rdbms.getDataTypes(physicalUri);
								}
							} catch (Exception ignored) {
								// Data type fetch is best-effort; column name alone is still useful
							}
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
				sb.append("Database id=\"").append(engineId).append("\": (schema unavailable)\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Returns the list of custom reactors available in the given project, formatted as a prompt
	 * section for LLM use. Used to fill in app-node pixel expressions.
	 */
	public static String buildReactorListSection(String projectId) {
		StringBuilder sb = new StringBuilder("## Available custom reactors in this project\n");
		try {
			IProject project = Utility.getProject(projectId);
			if (project == null) {
				sb.append("(project not found - leave app node pixel as-is)\n");
				return sb.toString();
			}
			java.util.TreeSet<String> reactors = project.getAvailableReactors();
			if (reactors == null || reactors.isEmpty()) {
				sb.append("(none - leave app node pixel as a comment placeholder)\n");
				return sb.toString();
			}
			for (String name : reactors) {
				sb.append("- ").append(name).append("\n");
			}
		} catch (Exception e) {
			classLogger.warn("Failed to build reactor list for project {}", projectId, e);
			sb.append("(reactor list unavailable - leave app node pixel as a comment placeholder)\n");
		}
		return sb.toString();
	}

	/**
	 * Strips leading/trailing markdown code fences ({@code ```json ... ```} or {@code ``` ... ```})
	 * that models sometimes add despite being instructed not to.
	 */
	public static String stripCodeFences(String raw) {
		String trimmed = raw.strip();
		if (trimmed.startsWith("```")) {
			int firstNewline = trimmed.indexOf('\n');
			if (firstNewline != -1) {
				trimmed = trimmed.substring(firstNewline + 1);
			}
			if (trimmed.endsWith("```")) {
				trimmed = trimmed.substring(0, trimmed.lastIndexOf("```")).strip();
			}
		}
		return trimmed;
	}

	/**
	 * Validates that a generated document string is parseable JSON and contains the minimum
	 * required structure ({@code graph.nodes} must be non-empty). Throws on failure.
	 *
	 * <p>Uses Gson for parsing (consistent with the rest of the automation package) rather than
	 * {@code org.json}. Gson deserializes JSON objects as {@code Map<String, Object>} and arrays
	 * as {@code List<?>}, so the casts below are safe for well-formed JSON.
	 */
	public static void validateGeneratedDoc(String raw) {
		Map<String, Object> doc;
		try {
			doc = AutomationExecutionUtils.GSON.fromJson(raw, AutomationExecutionUtils.MAP_TYPE);
		} catch (Exception e) {
			classLogger.warn("Generated automation doc is not valid JSON (truncated): {}",
					raw.length() > 500 ? raw.substring(0, 500) + "..." : raw, e);
			throw new IllegalStateException(
					"The AI model returned an invalid response. Please try again with a different description.", e);
		}
		if (doc == null || !(doc.get("graph") instanceof Map<?, ?>)) {
			throw new IllegalStateException("Generated document is missing the 'graph' field.");
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
		if (!(graph.get("nodes") instanceof List<?>)) {
			throw new IllegalStateException("Generated graph is missing the 'nodes' array.");
		}
		@SuppressWarnings("unchecked")
		List<Object> nodes = (List<Object>) graph.get("nodes");
		if (nodes.isEmpty()) {
			throw new IllegalStateException("Generated graph has no nodes.");
		}
	}
}
