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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Uses an LLM to scaffold a starter {@link AutomationDocument} from a plain-English description.
 *
 * <p>Pixel: {@code GenerateAutomation(project=["appId"], description=["what it should do"], engine=["modelEngineId"])}
 *
 * <p>Generation is two-pass: pass 1 builds the workflow structure and picks engines; pass 2 fetches
 * the real DB schema for any chosen database engines and rewrites SQL expressions and model commands
 * with accurate table/column names. Pass 2 is skipped when no database-engine nodes exist.
 *
 * <p>Returns the generated JSON as a string (same shape as {@code GetAutomation}). The caller is
 * expected to display it for review and save it via {@code SaveAutomation} — this reactor does NOT
 * persist anything.
 */
public final class GenerateAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateAutomationReactor.class);

	private static final int DESCRIPTION_MAX_CHARS = 1000;
	private static final String PENDING_SQL = "PENDING_SQL_GENERATION";

	/** Engine types used to fetch context for the prompt. */
	private static final List<String> SUPPORTED_ENGINE_TYPES = Arrays.asList(
			"DATABASE", "MODEL", "VECTOR", "STORAGE", "FUNCTION");

	/**
	 * Pass 1 system prompt: structural generation only.
	 * The LLM picks node types, engines, labels, and output vars.
	 * DB expressions are set to PENDING_SQL_GENERATION for pass 2 to fill in.
	 */
	private static final String SYSTEM_PROMPT_STRUCTURAL = """
You are a workflow builder assistant. Generate an automation graph JSON document from the user's plain-English description.

## Available node types
Each node has these fields: id (string), type (string), label (string), position ({x:0,y:0}), outputVar (string), config (object).

Node types and their config shapes:
- trigger: config={"mode":"manual"} — always the first node, outputVar="trigger_out"
- database-engine: config={"engineId":"","operation":"query","expression":"PENDING_SQL_GENERATION","limit":50,"commit":false} — SQL queries, outputVar="db_out"
- model-engine: config={"engineId":"","operation":"llm","command":"","context":"","paramValues":"","values":"","image":"","prompt":"","entities":""} — LLM calls, outputVar="model_out"
- vector-engine: config={"engineId":"","operation":"search","command":"","limit":5,"filters":"","metaFilters":"","filePath":"","source":"","space":"","filePaths":"","paramValues":"","fileNames":""} — semantic search, outputVar="vector_out"
- storage-engine: config={"engineId":"","operation":"list","storagePath":"/","filePath":"","metadata":""} — file storage, outputVar="storage_out"
- function-engine: config={"engineId":"","operation":"execute","params":""} — custom functions, outputVar="fn_out"
- app: config={"pixel":"PENDING_PIXEL_EXPRESSION","appId":""} — run a custom reactor or arbitrary Pixel, outputVar="pixel_out"
- wait: config={"seconds":"5"} — pause between steps, outputVar="wait_out"

## Variable substitution
Reference upstream node outputs in config fields using ${outputVar} (e.g. ${db_out}, ${model_out}).
NEVER use SQL parameterized syntax ($1, $2, ?, :param) — those are not supported and will cause runtime errors.
In SQL expressions, always wrap ${outputVar} in single quotes for string/UUID values: WHERE col = '${varName}'. NEVER use double quotes — PostgreSQL treats double-quoted values as column names.

## Rules
1. Always start with a trigger node (id="trigger-1").
2. Use only node types from the list above.
3. Set engineId from the available engines listed below. Leave empty string "" if no suitable engine exists.
4. Keep outputVar names unique and descriptive of what the node produces.
5. Make node labels action-oriented and specific to what the node does.
6. Build a realistic, useful graph — don't add unnecessary nodes.
7. For database-engine nodes: set expression to exactly "PENDING_SQL_GENERATION" — a second pass will fill in real SQL using the actual schema.
8. For app nodes: set pixel to exactly "PENDING_PIXEL_EXPRESSION" — a second pass will fill in the reactor call using the project's available reactors.
9. For model-engine nodes: "command" is the plain instruction to the LLM (e.g. "Summarize these cases and highlight urgent items"). Put the actual data by setting "context" to the upstream outputVar (e.g. "context":"${db_out}"). NEVER describe the data structure in prose inside "command" — the LLM will receive the real data at runtime via ${outputVar} substitution, not a description of it.
10. Respond with ONLY valid JSON. No markdown, no code fences, no explanation.

## Response format
{"version":1,"description":"<one-sentence description>","graph":{"nodes":[...],"edges":[]}}
""";

	/**
	 * Edit/iterate mode system prompt: same structure rules as pass 1, but instructs the LLM to
	 * treat the input as an existing document to modify rather than generate from scratch.
	 */
	private static final String SYSTEM_PROMPT_EDIT = """
You are a workflow builder assistant. You are given an existing automation graph JSON document. Modify it based on the user's request. Preserve all steps and structure that are not directly affected by the request. You may add, remove, or change nodes as needed. Always keep a trigger node as the first node.

## Available node types
Each node has these fields: id (string), type (string), label (string), position ({x:0,y:0}), outputVar (string), config (object).

Node types and their config shapes:
- trigger: config={"mode":"manual"} — always the first node, outputVar="trigger_out"
- database-engine: config={"engineId":"","operation":"query","expression":"PENDING_SQL_GENERATION","limit":50,"commit":false} — SQL queries, outputVar="db_out"
- model-engine: config={"engineId":"","operation":"llm","command":"","context":"","paramValues":"","values":"","image":"","prompt":"","entities":""} — LLM calls, outputVar="model_out"
- vector-engine: config={"engineId":"","operation":"search","command":"","limit":5,"filters":"","metaFilters":"","filePath":"","source":"","space":"","filePaths":"","paramValues":"","fileNames":""} — semantic search, outputVar="vector_out"
- storage-engine: config={"engineId":"","operation":"list","storagePath":"/","filePath":"","metadata":""} — file storage, outputVar="storage_out"
- function-engine: config={"engineId":"","operation":"execute","params":""} — custom functions, outputVar="fn_out"
- app: config={"pixel":"PENDING_PIXEL_EXPRESSION","appId":""} — run a custom reactor or arbitrary Pixel, outputVar="pixel_out"
- wait: config={"seconds":"5"} — pause between steps, outputVar="wait_out"

## Variable substitution
Reference upstream node outputs in config fields using ${outputVar} (e.g. ${db_out}, ${model_out}).
NEVER use SQL parameterized syntax ($1, $2, ?, :param) — those are not supported and will cause runtime errors.
In SQL expressions, always wrap ${outputVar} in single quotes for string/UUID values: WHERE col = '${varName}'. NEVER use double quotes — PostgreSQL treats double-quoted values as column names.

## Rules
1. Always start with a trigger node (id="trigger-1").
2. Use only node types from the list above.
3. Set engineId from the available engines listed below. Leave empty string "" if no suitable engine exists.
4. Keep outputVar names unique and descriptive of what the node produces.
5. Make node labels action-oriented and specific to what the node does.
6. Build a realistic, useful graph — don't add unnecessary nodes.
7. For database-engine nodes: set expression to exactly "PENDING_SQL_GENERATION" — a second pass will fill in real SQL using the actual schema.
8. For app nodes: set pixel to exactly "PENDING_PIXEL_EXPRESSION" — a second pass will fill in the reactor call using the project's available reactors.
9. For model-engine nodes: "command" is the plain instruction to the LLM (e.g. "Summarize these cases and highlight urgent items"). Put the actual data by setting "context" to the upstream outputVar (e.g. "context":"${db_out}"). NEVER describe the data structure in prose inside "command" — the LLM will receive the real data at runtime via ${outputVar} substitution, not a description of it.
10. Respond with ONLY valid JSON. No markdown, no code fences, no explanation.

## Response format
{"version":1,"description":"<one-sentence description>","graph":{"nodes":[...],"edges":[]}}
""";

	/**
	 * Pass 2 system prompt: schema-aware refinement.
	 * Rewrites PENDING_SQL_GENERATION, PENDING_PIXEL_EXPRESSION, and model commands using actual schema and reactor list.
	 */
	private static final String SYSTEM_PROMPT_REFINE = """
You are given a workflow automation document along with context gathered after pass 1: the actual database schemas for chosen database engines, and the list of custom reactors available in this project.

Your task: return a corrected copy of the document with these updates:
1. database-engine nodes: replace the "expression" value "PENDING_SQL_GENERATION" with a real SQL SELECT query using the actual table and column names from the schema.
   - Write SQL that matches what the user asked for — use appropriate joins, filters, and columns based on the user's intent
   - Do not add conditions or filters that aren't implied by the user's request
   - Use column types to write type-safe SQL — don't compare VARCHAR columns to integers or assume a column's semantics from its name alone
   - Use a reasonable LIMIT to avoid returning unbounded result sets
   - NEVER use SQL parameterized placeholders ($1, $2, ?, :param) — the execution engine does not support bound parameters. For runtime values from upstream nodes, use ${outputVar} inline in the SQL string wrapped in single quotes (e.g. WHERE id = '${db_out}'). NEVER wrap ${outputVar} in double quotes — double quotes are SQL identifier delimiters and will cause a "column does not exist" error. For literal filters, hardcode the value directly.
   - If no relevant table exists in the schema, set expression to "-- [replace with your SQL query]" and update the label to "Review: update this query"
2. model-engine nodes: ensure the "context" field contains the upstream outputVar reference (e.g. "${db_out}") so the actual data is passed to the LLM at runtime. The "command" must be a plain instruction only — NEVER describe the data structure or column names in prose inside "command". The LLM will see the real data via the context field; it does not need to be told what columns exist.
3. app nodes: replace the "pixel" value "PENDING_PIXEL_EXPRESSION" with a call to the most appropriate reactor from the available reactors list.
   - Use the format: ReactorName(param="${varName}") referencing upstream outputVars where relevant
   - If no reactor in the list clearly matches the intent, set pixel to "-- [describe the Pixel expression to write here]"

Keep ALL other fields (ids, types, labels, outputVars, engineIds, edges, etc.) EXACTLY as they are.

Respond with ONLY the complete updated JSON document. No markdown, no code fences, no explanation.
""";

	/** Key for an optional base64-encoded current doc to modify (edit/iterate mode). */
	private static final String CURRENT_DOC_KEY = "currentDoc";

	public GenerateAutomationReactor() {
		this.keysToGet = new String[] {
			ReactorKeysEnum.PROJECT.getKey(),
			AutomationConstants.DOC_DESCRIPTION,
			ReactorKeysEnum.ENGINE.getKey(),
			CURRENT_DOC_KEY
		};
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in.");
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String description = this.keyValue.get(AutomationConstants.DOC_DESCRIPTION);
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		// View access is intentional — this reactor is read-only (no persistence).
		// The caller saves the result via SaveAutomation, which enforces edit access.
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access.");
		}

		if (description == null || description.trim().isEmpty()) {
			throw new IllegalArgumentException("A description of what the automation should do is required.");
		}
		// Decode base64-encoded description sent by the FE to prevent Pixel injection
		try {
			description = new String(
				java.util.Base64.getDecoder().decode(description.trim()),
				java.nio.charset.StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			// Not base64-encoded (e.g. direct API call) — use the value as-is
		}
		if (description.length() > DESCRIPTION_MAX_CHARS) {
			description = description.substring(0, DESCRIPTION_MAX_CHARS);
		}

		// Resolve model engine — use provided ID or fall back to first available MODEL engine
		if (engineId == null || engineId.trim().isEmpty()) {
			engineId = findFirstModelEngine(user);
		}
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"No AI model engine is available. Add a model engine connection to generate an automation.");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model engine " + engineId + " does not exist or user does not have access.");
		}

		IModelEngine modelEngine = Utility.getModel(engineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException(
					"Model engine " + engineId + " could not be loaded. It may no longer exist.");
		}

		// Decode optional current doc (edit/iterate mode)
		String currentDocRaw = this.keyValue.get(CURRENT_DOC_KEY);
		String currentDoc = null;
		if (currentDocRaw != null && !currentDocRaw.trim().isEmpty()) {
			try {
				currentDoc = new String(
					java.util.Base64.getDecoder().decode(currentDocRaw.trim()),
					java.nio.charset.StandardCharsets.UTF_8);
			} catch (IllegalArgumentException e) {
				// Not base64-encoded (e.g. direct API call) — use the value as-is
				currentDoc = currentDocRaw;
			}
			if (currentDoc != null && currentDoc.length() > 50_000) {
				currentDoc = currentDoc.substring(0, 50_000);
			}
		}

		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("use_history", false);

		// -- Pass 1: structural generation (or edit if current doc provided) -------------
		String enginesSection = buildAvailableEnginesSection(user);
		String pass1Message;
		String systemPrompt1;
		if (currentDoc != null) {
			classLogger.info("GenerateAutomation edit mode: project={}, docLength={}", projectId, currentDoc.length());
			systemPrompt1 = SYSTEM_PROMPT_EDIT;
			pass1Message = enginesSection
				+ "\n## Existing automation\n```json\n" + currentDoc + "\n```"
				+ "\n## User's modification request\n" + description.trim();
		} else {
			systemPrompt1 = SYSTEM_PROMPT_STRUCTURAL;
			pass1Message = enginesSection + "\n## User's request\n" + description.trim();
		}
		String raw = callLlm(modelEngine, systemPrompt1, pass1Message, paramMap, projectId);
		raw = stripCodeFences(raw);
		validateGeneratedDoc(raw);

		// -- Pass 2: schema-aware refinement (when DB nodes or app nodes are present) ----
		List<String> dbEngineIds = extractDatabaseEngineIds(raw);
		boolean hasAppNodes = extractHasAppNodes(raw);
		if (!dbEngineIds.isEmpty() || hasAppNodes) {
			classLogger.info("GenerateAutomation pass 2: db engines={}, appNodes={}, project={}",
					dbEngineIds.size(), hasAppNodes, projectId);
			StringBuilder pass2Message = new StringBuilder("## Workflow document from pass 1\n").append(raw).append("\n");
			if (!dbEngineIds.isEmpty()) {
				pass2Message.append("\n").append(buildSchemaForEngineIds(dbEngineIds));
			}
			if (hasAppNodes) {
				pass2Message.append("\n").append(buildReactorListSection(projectId));
			}
			try {
				String refined = callLlm(modelEngine, SYSTEM_PROMPT_REFINE, pass2Message.toString(), paramMap, projectId);
				refined = stripCodeFences(refined);
				validateGeneratedDoc(refined);
				raw = refined;
			} catch (Exception e) {
				classLogger.warn("GenerateAutomation pass 2 failed — returning pass 1 result. Reason: {}", e.getMessage());
				// Fall through: return pass 1 result unchanged
			}
		}

		return new NounMetadata(raw, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	// -- Private helpers -------------------------------------------------------------

	/** Calls the model and returns the response text, throwing on failure. */
	private String callLlm(IModelEngine modelEngine, String systemPrompt, String userMessage,
			Map<String, Object> paramMap, String projectId) {
		Map<String, Object> response;
		try {
			response = modelEngine.ask(systemPrompt + "\n\n" + userMessage, null, this.insight, paramMap).toMap();
		} catch (Exception e) {
			classLogger.error("LLM call failed for GenerateAutomation on project {}", projectId, e);
			throw new RuntimeException("AI generation failed: " + e.getMessage(), e);
		}
		String text = extractResponseText(response);
		if (text == null || text.isBlank()) {
			throw new IllegalStateException(
					"The AI model did not return a response. Try again or start with a blank automation.");
		}
		return text;
	}

	/**
	 * Parses a generated document and returns the engineIds of all database-engine nodes
	 * that have a non-blank engineId. Used to decide whether pass 2 is needed.
	 */
	private static List<String> extractDatabaseEngineIds(String docJson) {
		List<String> ids = new ArrayList<>();
		try {
			JSONObject doc = new JSONObject(docJson);
			JSONObject graph = doc.optJSONObject("graph");
			if (graph == null) return ids;
			JSONArray nodes = graph.optJSONArray("nodes");
			if (nodes == null) return ids;
			for (int i = 0; i < nodes.length(); i++) {
				JSONObject node = nodes.optJSONObject(i);
				if (node == null) continue;
				if ("database-engine".equals(node.optString("type"))) {
					JSONObject config = node.optJSONObject("config");
					if (config != null) {
						String dbId = config.optString("engineId", "").trim();
						if (!dbId.isEmpty() && !ids.contains(dbId)) {
							ids.add(dbId);
						}
					}
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to extract database engine IDs from generated doc", e);
		}
		return ids;
	}

	/**
	 * Returns true if the document contains any app-type nodes whose pixel is still PENDING_PIXEL_EXPRESSION.
	 * Used to decide whether pass 2 needs to include the reactor list.
	 */
	private static boolean extractHasAppNodes(String docJson) {
		try {
			JSONObject doc = new JSONObject(docJson);
			JSONObject graph = doc.optJSONObject("graph");
			if (graph == null) return false;
			JSONArray nodes = graph.optJSONArray("nodes");
			if (nodes == null) return false;
			for (int i = 0; i < nodes.length(); i++) {
				JSONObject node = nodes.optJSONObject(i);
				if (node == null) continue;
				if ("app".equals(node.optString("type"))) {
					JSONObject config = node.optJSONObject("config");
					if (config != null && "PENDING_PIXEL_EXPRESSION".equals(config.optString("pixel"))) {
						return true;
					}
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to check for app nodes in generated doc", e);
		}
		return false;
	}

	/**
	 * Returns the list of custom reactors available in the given project, formatted as a
	 * bulleted list for the pass 2 prompt. Uses the same source as the FE reactor browser.
	 */
	private static String buildReactorListSection(String projectId) {
		StringBuilder sb = new StringBuilder("## Available custom reactors in this project\n");
		try {
			IProject project = Utility.getProject(projectId);
			if (project == null) {
				sb.append("(project not found — leave app node pixel as-is)\n");
				return sb.toString();
			}
			java.util.TreeSet<String> reactors = project.getAvailableReactors();
			if (reactors == null || reactors.isEmpty()) {
				sb.append("(none — leave app node pixel as a comment placeholder)\n");
				return sb.toString();
			}
			for (String name : reactors) {
				sb.append("- ").append(name).append("\n");
			}
		} catch (Exception e) {
			classLogger.warn("Failed to build reactor list for project {}", projectId, e);
			sb.append("(reactor list unavailable — leave app node pixel as a comment placeholder)\n");
		}
		return sb.toString();
	}

	/**
	 * Fetches the table/column schema (with data types) for the given engine IDs.
	 * Uses the same metamodel API as {@code TextToSQLReactor}. Data types prevent the LLM
	 * from generating type-mismatched WHERE clauses (e.g. comparing VARCHAR to an integer).
	 */
	private static String buildSchemaForEngineIds(List<String> engineIds) {
		StringBuilder sb = new StringBuilder("## Database schema\n");
		for (String engineId : engineIds) {
			try {
				IDatabaseEngine dbEngine = Utility.getDatabase(engineId);
				if (!(dbEngine instanceof IRDBMSEngine rdbms)) {
					sb.append("Database id=\"").append(engineId).append("\": (non-relational — no table schema)\n");
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
	 * Returns the ID of the first MODEL-type engine the user has access to, or null if none.
	 */
	private static String findFirstModelEngine(User user) {
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
	 * Builds the "## Available engines" section appended to the pass 1 user message.
	 * Lists each engine the user has access to so the LLM can populate engineId fields.
	 */
	private static String buildAvailableEnginesSection(User user) {
		StringBuilder sb = new StringBuilder("## Available engines\n");
		try {
			List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(
					user, SUPPORTED_ENGINE_TYPES, null, false, null, null, null, "50", "0", null);
			if (engines == null || engines.isEmpty()) {
				sb.append("None available — leave engineId as empty string.\n");
				return sb.toString();
			}
			for (Map<String, Object> engine : engines) {
				String id = engine.getOrDefault("database_id", "").toString();
				String name = engine.getOrDefault("database_name", "").toString();
				String type = engine.getOrDefault("engine_type", "").toString().toUpperCase();
				sb.append("- type=").append(type)
				  .append(" id=\"").append(id)
				  .append("\" name=\"").append(name).append("\"\n");
			}
		} catch (Exception e) {
			classLogger.warn("Failed to build engine list for generation prompt", e);
			sb.append("(engine list unavailable — leave engineId as empty string)\n");
		}
		return sb.toString();
	}

	/**
	 * Extracts the text content from the model's response map.
	 * Handles both {@code response} string and {@code output}/{@code content} keys.
	 */
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

	/** Strips leading/trailing markdown code fences (```json ... ``` or ``` ... ```). */
	private static String stripCodeFences(String raw) {
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
	 * Throws if the generated doc is not parseable JSON or missing required structure.
	 * Does not enforce strict schema — just enough to avoid crashing the FE.
	 */
	private static void validateGeneratedDoc(String raw) {
		try {
			JSONObject doc = new JSONObject(raw);
			if (!doc.has("graph")) {
				throw new IllegalStateException("Generated document is missing the 'graph' field.");
			}
			JSONObject graph = doc.getJSONObject("graph");
			if (!graph.has("nodes")) {
				throw new IllegalStateException("Generated graph is missing the 'nodes' array.");
			}
			JSONArray nodes = graph.getJSONArray("nodes");
			if (nodes.length() == 0) {
				throw new IllegalStateException("Generated graph has no nodes.");
			}
		} catch (org.json.JSONException e) {
			classLogger.warn("Generated automation doc is not valid JSON: {}", raw, e);
			throw new IllegalStateException(
					"The AI model returned an invalid response. Please try again with a different description.", e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Uses an AI model to scaffold a starter automation graph from a plain-English description. "
				+ "Returns the generated document JSON — the caller must save it via SaveAutomation.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (AutomationConstants.DOC_DESCRIPTION.equals(key)) {
			return "Plain-English description of what the automation should do (max 1000 characters).";
		} else if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Project ID that will own this automation.";
		} else if (ReactorKeysEnum.ENGINE.getKey().equals(key)) {
			return "Optional model engine ID to use for generation. Defaults to the first available MODEL engine.";
		} else if (CURRENT_DOC_KEY.equals(key)) {
			return "Optional base64-encoded JSON of an existing automation document. When provided, the LLM modifies the existing document rather than generating from scratch.";
		}
		return super.getDescriptionForKey(key);
	}
}
