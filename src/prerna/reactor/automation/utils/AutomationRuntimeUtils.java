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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.util.AssetUtility;
import prerna.reactor.automation.AutomationConstants;

/**
 * Shared static utilities for the automation execution engine.
 *
 * <p>Centralizes logic shared by the graph runtime and its engine executors.
 */
public final class AutomationRuntimeUtils {

	private static final Logger classLogger = LogManager.getLogger(AutomationRuntimeUtils.class);

	/** Prefix for config-map lookups within a template, e.g. {@code ${config.API_KEY}}. */
	private static final String CONFIG_VAR_PREFIX = "config.";

	/** Reusable {@link TypeToken} type for {@code Map<String, Object>} deserialization. */
	public static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

	/** Reusable {@link TypeToken} type for {@code List<Map<String, Object>>} deserialization. */
	private static final Type LIST_OBJ_MAP_TYPE = new TypeToken<List<Map<String, Object>>>() {}.getType();

	/**
	 * Shared Gson instance for the whole automation engine  - public so the
	 * {@code nodes} sub-package has one shared instance to reuse instead of each
	 * file declaring its own.
	 */
	public static final Gson GSON = new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeHierarchyAdapter(ZoneId.class,
					(JsonSerializer<ZoneId>) (src, t, ctx) -> new JsonPrimitive(src.getId()))
			.registerTypeAdapter(ZonedDateTime.class,
					(JsonSerializer<ZonedDateTime>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(OffsetDateTime.class,
					(JsonSerializer<OffsetDateTime>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(LocalDateTime.class,
					(JsonSerializer<LocalDateTime>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(LocalDate.class,
					(JsonSerializer<LocalDate>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeAdapter(Instant.class,
					(JsonSerializer<Instant>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.registerTypeHierarchyAdapter(Throwable.class,
					(JsonSerializer<Throwable>) (src, t, ctx) -> new JsonPrimitive(src.toString()))
			.create();

	private AutomationRuntimeUtils() {}

	/**
	 * Resolves {@code ${varName}} and {@code ${config.KEY}} placeholders in a template string
	 * using {@link StringSubstitutor} (the platform's standard {@code ${...}} templating helper,
	 * also used by {@code AbstractPythonModelEngine.fillVars}). Recursive substitution is
	 * disabled so a resolved value is never itself re-scanned for placeholders, and unresolved
	 * placeholders are left untouched rather than throwing.
	 */
	public static String resolve(String template, Map<String, String> scope, Map<String, String> configMap) {
		if (template == null) return "";

		Map<String, String> vars = new HashMap<>();
		for (Map.Entry<String, String> e : configMap.entrySet()) {
			vars.put(CONFIG_VAR_PREFIX + e.getKey(), e.getValue());
		}
		for (Map.Entry<String, String> e : scope.entrySet()) {
			if (e.getValue() != null) {
				vars.put(e.getKey(), e.getValue());
			}
		}

		StringSubstitutor sub = new StringSubstitutor(vars);
		sub.setEnableUndefinedVariableException(false);
		sub.setDisableSubstitutionInValues(true);
		return sub.replace(template);
	}

	/**
	 * Returns the per-node timeout from the node definition, defaulting to
	 * {@link AutomationConstants#DEFAULT_TIMEOUT_SECONDS}.
	 */
	public static int getNodeTimeout(Map<String, Object> node) {
		Object timeout = node.get(AutomationConstants.CONFIG_TIMEOUT_SECONDS);
		if (timeout instanceof Number) {
			return ((Number) timeout).intValue();
		}
		return AutomationConstants.DEFAULT_TIMEOUT_SECONDS;
	}

	/**
	 * Loads {@code automation-config.json} for a project and returns key->value pairs.
	 * Returns an empty map if the file does not exist or cannot be parsed.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, String> loadConfig(String projectId) {
		Map<String, String> map = new HashMap<>();
		try {
			String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
			File f = Paths.get(portalsFolder, AutomationConstants.AUTOMATION_CONFIG_FILE_NAME).toFile();
			if (!f.exists()) return map;
			String json = Files.readString(f.toPath(), StandardCharsets.UTF_8);
			List<Map<String, Object>> entries = GSON.fromJson(json, LIST_OBJ_MAP_TYPE);
			if (entries != null) {
				for (Map<String, Object> entry : entries) {
					String key = (String) entry.get(AutomationConstants.CONFIG_ENTRY_KEY);
					String value = (String) entry.get(AutomationConstants.CONFIG_ENTRY_VALUE);
					if (key != null && value != null) map.put(key, value);
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to load automation config for project {}", projectId, e);
		}
		return map;
	}

	/**
	 * Applies an output transform to a raw pixel result.
	 *
	 * <p>Supported modes: {@code rows-as-objects}, {@code first-row}, {@code column},
	 * {@code jsonpath}. Returns raw serialized JSON when config is null or mode is {@code raw}.
	 */
	@SuppressWarnings("unchecked")
	public static String applyOutputTransform(Object rawResult, Map<String, Object> transformConfig) {
		String rawStr = serializeRaw(rawResult);
		if (transformConfig == null) return rawStr;

		String mode = (String) transformConfig.getOrDefault(AutomationConstants.TRANSFORM_MODE, AutomationConstants.TRANSFORM_MODE_RAW);
		switch (mode) {
			case AutomationConstants.TRANSFORM_MODE_ROWS_AS_OBJECTS: return transformRowsAsObjects(rawStr);
			case AutomationConstants.TRANSFORM_MODE_FIRST_ROW: return transformFirstRow(rawStr);
			case AutomationConstants.TRANSFORM_MODE_COLUMN: return transformColumn(rawStr, (String) transformConfig.get(AutomationConstants.TRANSFORM_COLUMN));
			case AutomationConstants.TRANSFORM_MODE_JSONPATH: return transformJsonPath(rawStr, (String) transformConfig.get(AutomationConstants.TRANSFORM_PATH));
			default:                return rawStr;
		}
	}

	/** Serializes a raw pixel result to a JSON string. */
	public static String serializeRaw(Object rawResult) {
		if (rawResult == null) return "";
		if (rawResult instanceof String) return (String) rawResult;
		return GSON.toJson(rawResult);
	}

	// -- Private transform helpers -------------------------------------------------

	@SuppressWarnings("unchecked")
	private static String transformRowsAsObjects(String rawStr) {
		Map<String, Object> data = extractDataset(parseJsonAny(rawStr));
		if (data == null) return rawStr;
		List<String> headers = (List<String>) data.get(AutomationConstants.DATASET_HEADERS);
		List<List<Object>> rows = (List<List<Object>>) data.get(AutomationConstants.DATASET_VALUES);
		if (headers == null || rows == null) return rawStr;
		List<Map<String, Object>> result = new ArrayList<>();
		for (List<Object> row : rows) {
			Map<String, Object> rowMap = new HashMap<>();
			for (int i = 0; i < headers.size() && i < row.size(); i++) {
				rowMap.put(headers.get(i), row.get(i));
			}
			result.add(rowMap);
		}
		return GSON.toJson(result);
	}

	@SuppressWarnings("unchecked")
	private static String transformFirstRow(String rawStr) {
		Map<String, Object> data = extractDataset(parseJsonAny(rawStr));
		if (data == null) return rawStr;
		List<String> headers = (List<String>) data.get(AutomationConstants.DATASET_HEADERS);
		List<List<Object>> rows = (List<List<Object>>) data.get(AutomationConstants.DATASET_VALUES);
		if (headers == null || rows == null || rows.isEmpty()) return rawStr;
		Map<String, Object> rowMap = new HashMap<>();
		List<Object> first = rows.get(0);
		for (int i = 0; i < headers.size() && i < first.size(); i++) {
			rowMap.put(headers.get(i), first.get(i));
		}
		return GSON.toJson(rowMap);
	}

	@SuppressWarnings("unchecked")
	private static String transformColumn(String rawStr, String colName) {
		if (colName == null || colName.isEmpty()) return rawStr;
		Map<String, Object> data = extractDataset(parseJsonAny(rawStr));
		if (data == null) return rawStr;
		List<String> headers = (List<String>) data.get(AutomationConstants.DATASET_HEADERS);
		List<List<Object>> rows = (List<List<Object>>) data.get(AutomationConstants.DATASET_VALUES);
		if (headers == null || rows == null) return rawStr;
		int colIdx = headers.indexOf(colName);
		if (colIdx < 0) return rawStr;
		List<Object> col = new ArrayList<>();
		for (List<Object> row : rows) col.add(colIdx < row.size() ? row.get(colIdx) : null);
		return GSON.toJson(col);
	}

	@SuppressWarnings("unchecked")
	private static String transformJsonPath(String rawStr, String path) {
		if (path == null || path.isEmpty()) return rawStr;
		try {
			Object current = parseJsonAny(rawStr);
			for (String segment : path.split("\\.")) {
				if (!(current instanceof Map)) break;
				current = ((Map<String, Object>) current).get(segment);
			}
			if (current == null) return "";
			return current instanceof String ? (String) current : GSON.toJson(current);
		} catch (Exception e) {
			return rawStr;
		}
	}

	/**
	 * Normalises any of the three dataset formats into a canonical {@code {headers, values}} map:
	 *   1. List&lt;Map&gt; (rows-as-objects)  - produced by {@link DatabaseEngineNodeExecutor}
	 *   2. {@code {data: {headers, values}}}  - SEMOSS wrapped envelope
	 *   3. {@code {headers, values}}  - SEMOSS direct
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> extractDataset(Object parsed) {
		if (parsed == null) return null;

		// Format 1: rows-as-objects list from DatabaseEngineNodeExecutor
		if (parsed instanceof List) {
			List<Object> list = (List<Object>) parsed;
			if (list.isEmpty()) return null;
			Object first = list.get(0);
			if (!(first instanceof Map)) return null;
			List<String> headers = new ArrayList<>(((Map<String, Object>) first).keySet());
			List<List<Object>> values = new ArrayList<>();
			for (Object item : list) {
				if (item instanceof Map) {
					Map<String, Object> row = (Map<String, Object>) item;
					List<Object> rowVals = new ArrayList<>();
					for (String h : headers) rowVals.add(row.get(h));
					values.add(rowVals);
				}
			}
			Map<String, Object> result = new HashMap<>();
			result.put(AutomationConstants.DATASET_HEADERS, headers);
			result.put(AutomationConstants.DATASET_VALUES, values);
			return result;
		}

		if (!(parsed instanceof Map)) return null;
		Map<String, Object> map = (Map<String, Object>) parsed;

		// Format 2: {data: {headers, values}}
		if (map.containsKey(AutomationConstants.DATASET_DATA) && map.get(AutomationConstants.DATASET_DATA) instanceof Map) {
			return (Map<String, Object>) map.get(AutomationConstants.DATASET_DATA);
		}
		// Format 3: {headers, values}
		if (map.containsKey(AutomationConstants.DATASET_HEADERS) && map.containsKey(AutomationConstants.DATASET_VALUES)) return map;
		return null;
	}

	/** Parses JSON to Object  - returns List for arrays, Map for objects (handles all executor output shapes). */
	private static Object parseJsonAny(String json) {
		if (json == null || json.isBlank()) return null;
		try {
			return GSON.fromJson(json, Object.class);
		} catch (Exception e) {
			return null;
		}
	}

	private static Map<String, Object> parseJson(String json) {
		if (json == null || json.isBlank()) return null;
		try {
			return GSON.fromJson(json, MAP_TYPE);
		} catch (Exception e) {
			return null;
		}
	}

	// -- Scope building ------------------------------------------------------------

	/**
	 * Builds the initial variable scope for an automation run, seeded with {@code date},
	 * {@code triggered_at}, and {@code run_id} (when non-blank).
	 *
	 * @param runId the run ID to seed into scope, or {@code null} for test runs
	 * @param user  the triggering user  - used to localise {@code date} and {@code triggered_at}
	 *              to the user's configured timezone; falls back to UTC when {@code null} or
	 *              when no zone has been set on the user
	 */
	public static Map<String, String> buildInitialScope(String runId, User user) {
		Map<String, String> scope = new HashMap<>();
		ZoneId zone = (user != null && user.getZoneId() != null) ? user.getZoneId() : ZoneId.of("UTC");
		ZonedDateTime now = ZonedDateTime.now(zone);
		scope.put(AutomationConstants.SCOPE_DATE, now.format(DateTimeFormatter.ISO_LOCAL_DATE));
		scope.put(AutomationConstants.SCOPE_TRIGGERED_AT, now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		if (runId != null && !runId.isBlank()) scope.put(AutomationConstants.SCOPE_RUN_ID, runId);
		return scope;
	}

	/** Truncates a string to {@link AutomationConstants#OUTPUT_PREVIEW_MAX_LENGTH} chars. */
	public static String generatePreview(String s) {
		if (s == null) return null;
		return s.length() <= AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH
				? s : s.substring(0, AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH);
	}

	// -- Runtime input helpers -----------------------------------------------------

	/**
	 * Extracts caller-supplied input values from the Pixel MAP noun and overwrites the matching
	 * config field on each node before execution. Nodes without a {@code playgroundFillable}
	 * list or whose listed fields are absent from the node config are skipped with a warning.
	 *
	 * @param nodes     ordered node list from the automation document (mutated in place)
	 * @param inputsMap raw MAP noun value from the {@code inputs} Pixel parameter, or {@code null}
	 * @return the flattened {@code String → String} inputs map (empty when no inputs were passed)
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, String> applyRuntimeInputs(List<Map<String, Object>> nodes, Map<String, Object> inputsMap) {
		Map<String, String> runtimeInputs = new HashMap<>();
		if (inputsMap != null) {
			for (Map.Entry<String, Object> entry : inputsMap.entrySet()) {
				if (entry.getValue() != null) {
					runtimeInputs.put(entry.getKey(), entry.getValue().toString());
				}
			}
		}
		if (!runtimeInputs.isEmpty()) {
			for (Map<String, Object> node : nodes) {
				List<String> fillable = (List<String>) node.get("playgroundFillable");
				if (fillable == null || fillable.isEmpty()) continue;
				String nodeLabel = (String) node.get(AutomationConstants.NODE_FIELD_LABEL);
				Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
				if (config == null) continue;
				for (String fieldName : fillable) {
					String paramName = buildRuntimeInputName(nodeLabel, fieldName);
					String value = runtimeInputs.get(paramName);
					if (value != null) {
						if (config.containsKey(fieldName)) {
							config.put(fieldName, value);
						} else {
							classLogger.warn("Runtime input '{}' targets field '{}' which does not exist in node '{}' config - skipping",
									paramName, fieldName, nodeLabel);
						}
					}
				}
			}
		}
		return runtimeInputs;
	}

	/**
	 * Builds a stable parameter name for a runtime-fillable node field.
	 * Slugifies the node label (lowercase, non-alphanumeric chars -> underscore, collapsed)
	 * and appends the field name. Falls back to {@code "input_" + fieldName} if label is blank.
	 *
	 * @param nodeLabel the node's display label (may be null or empty)
	 * @param fieldName the config field name (e.g. "expression", "command")
	 * @return a deterministic, URL-safe parameter name
	 */
	public static String buildRuntimeInputName(String nodeLabel, String fieldName) {
		if (nodeLabel != null && !nodeLabel.trim().isEmpty()) {
			String slug = nodeLabel.trim()
					.toLowerCase()
					.replaceAll("[^a-z0-9]+", "_")
					.replaceAll("^_+|_+$", "");
			if (!slug.isEmpty()) {
				return slug + "_" + fieldName;
			}
		}
		return "input_" + fieldName;
	}

	// -- Config value coercion -----------------------------------------------------

	/**
	 * Returns the trimmed string form of a node config value, or {@code null} if the value is
	 * {@code null} or blank.
	 */
	public static String strCfg(Object v) {
		return (v != null && !v.toString().isBlank()) ? v.toString() : null;
	}

	/**
	 * Normalizes a config value that may arrive as either an already-parsed {@code Map} or a
	 * raw JSON string (e.g. {@code inputMapping}, {@code paramValues}). Returns an empty map
	 * if absent or unparseable.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> coerceToMap(Object raw) {
		if (raw instanceof Map) {
			return (Map<String, Object>) raw;
		}
		if (raw instanceof String str && !str.isBlank()) {
			Map<String, Object> parsed = parseJson(str);
			if (parsed != null) return parsed;
		}
		return new HashMap<>();
	}

	// -- Node config access helpers -----------------------------------------------
	// -- Node config access helpers ------------------------------------------------

	/**
	 * Returns the string value of {@code key} from {@code config}, throwing if absent or blank.
	 *
	 * @param config    node config map
	 * @param key       config key to look up
	 * @param nodeLabel display label of the containing node (used in the error message)
	 */
	public static String required(Map<String, Object> config, String key, String nodeLabel) {
		Object v = config.get(key);
		if (v == null || v.toString().isBlank()) {
			throw new IllegalArgumentException(
					"Node \"" + nodeLabel + "\": '" + key + "' is required");
		}
		return v.toString();
	}

	/**
	 * Returns the string value of {@code key} from {@code config}, or {@code def} if absent or blank.
	 *
	 * @param config node config map
	 * @param key    config key to look up
	 * @param def    value to return when key is absent or blank
	 */
	public static String optional(Map<String, Object> config, String key, String def) {
		Object v = config.get(key);
		return (v == null || v.toString().isBlank()) ? def : v.toString();
	}

	/**
	 * Returns the string value of {@code key} from {@code config}, or {@code null} if absent or blank.
	 *
	 * @param config node config map
	 * @param key    config key to look up
	 */
	public static String optional(Map<String, Object> config, String key) {
		return optional(config, key, null);
	}

	/**
	 * Returns the integer value of {@code key} from {@code config}, or {@code def} if absent, blank,
	 * or not parseable as an integer.
	 *
	 * @param config node config map
	 * @param key    config key to look up
	 * @param def    default value when key is absent, blank, or unparseable
	 */
	public static int optionalInt(Map<String, Object> config, String key, int def) {
		Object v = config.get(key);
		if (v == null) return def;
		try {
			return Integer.parseInt(v.toString().trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}

}
