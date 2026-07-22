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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.util.AssetUtility;

/**
 * Shared static utilities for the automation execution engine.
 *
 * <p>Centralizes logic shared across {@link TriggerAutomationReactor},
 * {@link RunAutomationNodeReactor}, and
 * {@link prerna.reactor.automation.foreach.ForEachNodeExecutor}.
 */
public final class AutomationExecutionUtils {

	private static final Logger classLogger = LogManager.getLogger(AutomationExecutionUtils.class);

	/**
	 * Shared Gson instance for the whole automation engine — public so the
	 * {@code nodes} sub-package has one shared instance to reuse instead of each
	 * file declaring its own.
	 */
	public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private AutomationExecutionUtils() {}

	/**
	 * Resolves {@code ${varName}} and {@code ${config.KEY}} placeholders in a template string
	 * via plain {@link String#replace} — no validation or escaping is applied.
	 *
	 * <p>Any substitution slot whose value can carry user-supplied or LLM-generated text MUST
	 * be wrapped in {@code <encode>...</encode>} in the Pixel template before this is called —
	 * {@code PixelPreProcessor} handles decoding after parsing, preventing injected content from
	 * breaking the surrounding Pixel grammar.
	 */
	public static String resolve(String template, Map<String, String> scope, Map<String, String> configMap) {
		if (template == null) return "";
		String result = template;
		for (Map.Entry<String, String> e : configMap.entrySet()) {
			result = result.replace("${config." + e.getKey() + "}", e.getValue());
		}
		for (Map.Entry<String, String> e : scope.entrySet()) {
			if (e.getValue() != null) {
				result = result.replace("${" + e.getKey() + "}", e.getValue());
			}
		}
		return result;
	}

	/**
	 * Returns the per-node timeout from the node definition, defaulting to
	 * {@link AutomationConstants#DEFAULT_TIMEOUT_SECONDS}.
	 */
	public static int getNodeTimeout(Map<String, Object> node) {
		Object timeout = node.get("timeoutSeconds");
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
			File f = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_CONFIG_FILE_NAME);
			if (!f.exists()) return map;
			String json = Files.readString(f.toPath(), StandardCharsets.UTF_8);
			List<Map<String, Object>> entries = GSON.fromJson(json,
					new TypeToken<List<Map<String, Object>>>() {}.getType());
			if (entries != null) {
				for (Map<String, Object> entry : entries) {
					String key = (String) entry.get("key");
					String value = (String) entry.get("value");
					if (key != null && value != null) map.put(key, value);
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to load automation config for project {}: {}", projectId, e.getMessage(), e);
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

		String mode = (String) transformConfig.getOrDefault("mode", "raw");
		switch (mode) {
			case "rows-as-objects": return transformRowsAsObjects(rawStr);
			case "first-row":       return transformFirstRow(rawStr);
			case "column":          return transformColumn(rawStr, (String) transformConfig.get("column"));
			case "jsonpath":        return transformJsonPath(rawStr, (String) transformConfig.get("path"));
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
		Map<String, Object> data = extractDataset(parseJson(rawStr));
		if (data == null) return rawStr;
		List<String> headers = (List<String>) data.get("headers");
		List<List<Object>> rows = (List<List<Object>>) data.get("values");
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
		Map<String, Object> data = extractDataset(parseJson(rawStr));
		if (data == null) return rawStr;
		List<String> headers = (List<String>) data.get("headers");
		List<List<Object>> rows = (List<List<Object>>) data.get("values");
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
		Map<String, Object> data = extractDataset(parseJson(rawStr));
		if (data == null) return rawStr;
		List<String> headers = (List<String>) data.get("headers");
		List<List<Object>> rows = (List<List<Object>>) data.get("values");
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
			Object current = parseJson(rawStr);
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

	@SuppressWarnings("unchecked")
	private static Map<String, Object> extractDataset(Map<String, Object> parsed) {
		if (parsed == null) return null;
		if (parsed.containsKey("data") && parsed.get("data") instanceof Map) {
			return (Map<String, Object>) parsed.get("data");
		}
		if (parsed.containsKey("headers") && parsed.containsKey("values")) return parsed;
		return null;
	}

	private static Map<String, Object> parseJson(String json) {
		if (json == null || json.isBlank()) return null;
		try {
			return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
		} catch (Exception e) {
			return null;
		}
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

	// -- Automation document loading -----------------------------------------------

	/**
	 * Loads and parses a project's {@code automation.json} (graph + trigger config).
	 * Throws if the file is missing or unreadable.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> loadAutomationDoc(String projectId) {
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File f = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_FILE_NAME);
		if (!f.exists()) {
			throw new IllegalArgumentException("No automation.json found for this project. Save an automation first.");
		}
		try {
			String json = Files.readString(f.toPath(), StandardCharsets.UTF_8);
			return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
		} catch (IOException e) {
			throw new IllegalStateException("Failed to read automation.json: " + e.getMessage(), e);
		}
	}

	// -- Topological sort ----------------------------------------------------------

	/**
	 * Topologically sorts a node/edge graph (Kahn's algorithm). Nodes with no incoming edges
	 * are seeded in node-array order, so a linear automation with no edges still runs
	 * top-to-bottom in the order the nodes were defined.
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> topoSort(List<Map<String, Object>> nodes,
			List<Map<String, Object>> edges) {
		if (nodes == null || nodes.isEmpty()) return new ArrayList<>();

		Map<String, Integer> inDegree = new HashMap<>();
		Map<String, List<String>> adj = new HashMap<>();

		for (Map<String, Object> n : nodes) {
			String id = (String) n.get("id");
			inDegree.put(id, 0);
			adj.put(id, new ArrayList<>());
		}
		if (edges != null) {
			for (Map<String, Object> e : edges) {
				String src = (String) e.get("source");
				String tgt = (String) e.get("target");
				adj.computeIfAbsent(src, k -> new ArrayList<>()).add(tgt);
				inDegree.merge(tgt, 1, Integer::sum);
			}
		}

		Queue<String> queue = new LinkedList<>();
		for (Map<String, Object> n : nodes) {
			String id = (String) n.get("id");
			if (inDegree.getOrDefault(id, 0) == 0) queue.add(id);
		}

		Map<String, Map<String, Object>> nodeById = new HashMap<>();
		for (Map<String, Object> n : nodes) nodeById.put((String) n.get("id"), n);

		List<Map<String, Object>> sorted = new ArrayList<>();
		while (!queue.isEmpty()) {
			String id = queue.poll();
			sorted.add(nodeById.get(id));
			for (String neighbor : adj.getOrDefault(id, new ArrayList<>())) {
				int deg = inDegree.merge(neighbor, -1, Integer::sum);
				if (deg == 0) queue.add(neighbor);
			}
		}
		return sorted;
	}
}
