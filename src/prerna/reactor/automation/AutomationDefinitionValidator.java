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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.JsonParseException;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Validates the typed-graph automation document before Python source is generated.
 *
 * <p>The graph structure remains separate from implementation source. The graph is
 * the business-editable contract; generated Python and developer-owned blocks implement that
 * contract without changing its node, port, or capability declarations.
 */
public final class AutomationDefinitionValidator {

	private static final Set<String> SUPPORTED_NODE_TYPES = Set.of(
			AutomationConstants.NODE_START,
			AutomationConstants.NODE_DATABASE_QUERY,
			AutomationConstants.NODE_DATABASE_INSERT,
			AutomationConstants.NODE_DATABASE_UPDATE,
			AutomationConstants.NODE_MODEL_CHAT,
			AutomationConstants.NODE_MODEL_EMBEDDINGS,
			AutomationConstants.NODE_MODEL_VISION,
			AutomationConstants.NODE_MODEL_NER,
			AutomationConstants.NODE_STORAGE_ACTION,
			AutomationConstants.NODE_STORAGE_LIST,
			AutomationConstants.NODE_STORAGE_READ,
			AutomationConstants.NODE_STORAGE_UPLOAD,
			AutomationConstants.NODE_STORAGE_DOWNLOAD,
			AutomationConstants.NODE_STORAGE_DELETE,
			AutomationConstants.NODE_VECTOR_ACTION,
			AutomationConstants.NODE_VECTOR_SEARCH,
			AutomationConstants.NODE_VECTOR_ADD,
			AutomationConstants.NODE_VECTOR_DELETE,
			AutomationConstants.NODE_FUNCTION_EXECUTE,
			AutomationConstants.NODE_APP_PIXEL,
			AutomationConstants.NODE_AGENT_RUN,
			AutomationConstants.NODE_CONTROL_WAIT,
			AutomationConstants.NODE_DEVELOPER_PYTHON);

	private AutomationDefinitionValidator() {
	}

	/**
	 * Parses and validates an automation graph document.
	 *
	 * @param json graph document JSON
	 * @return immutable-by-convention validated graph metadata
	 */
	public static ValidatedDefinition parseAndValidate(String json) {
		if (json == null || json.isBlank()) {
			throw new IllegalArgumentException("Python automation definition must be a nonblank JSON object.");
		}
		try {
			Map<String, Object> definition = AutomationRuntimeUtils.GSON.fromJson(json,
					AutomationRuntimeUtils.MAP_TYPE);
			return validate(definition);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException("Python automation definition must be valid JSON.", e);
		}
	}

	/**
	 * Validates the graph shape, typed-node identity, edge endpoints, and code ownership state.
	 *
	 * @param definition parsed graph document
	 * @return validated definition and deterministic provenance fields
	 */
	public static ValidatedDefinition validate(Map<String, Object> definition) {
		if (definition == null) {
			throw new IllegalArgumentException("Python automation definition must be a JSON object.");
		}
		validateVersion(definition.get(AutomationConstants.DOC_FORMAT_VERSION));
		Map<String, Object> graph = requireMap(definition.get(AutomationConstants.DOC_GRAPH), "graph");
		List<Map<String, Object>> nodes = requireMapList(graph.get(AutomationConstants.DOC_NODES), "graph.nodes");
		List<Map<String, Object>> edges = requireMapList(graph.get(AutomationConstants.DOC_EDGES), "graph.edges");

		Map<String, String> nodeTypes = validateNodes(nodes);
		validateEdges(edges, nodeTypes);
		validateTriggerBindings(definition.get(AutomationConstants.DOC_TRIGGER_BINDINGS));

		String snapshot = AutomationRuntimeUtils.GSON.toJson(canonicalize(definition));
		return new ValidatedDefinition(definition, nodes, edges, snapshot, sha256(snapshot));
	}

	private static void validateVersion(Object value) {
		if (!(value instanceof Number number) || number.intValue() != AutomationConstants.PYTHON_DOC_CURRENT_VERSION
				|| number.doubleValue() != number.intValue()) {
			throw new IllegalArgumentException("Python automation definition formatVersion must be "
					+ AutomationConstants.PYTHON_DOC_CURRENT_VERSION + ".");
		}
	}

	private static Map<String, String> validateNodes(List<Map<String, Object>> nodes) {
		Map<String, String> nodeTypes = new LinkedHashMap<>();
		Set<String> outputVariables = new HashSet<>();
		int startCount = 0;
		for (int index = 0; index < nodes.size(); index++) {
			Map<String, Object> node = nodes.get(index);
			String nodeId = requireNonblankString(node.get(AutomationConstants.NODE_FIELD_ID),
					"graph.nodes[" + index + "].id");
			String nodeType = requireNonblankString(node.get(AutomationConstants.NODE_FIELD_TYPE),
					"graph.nodes[" + index + "].type");
			if (!SUPPORTED_NODE_TYPES.contains(nodeType)) {
				throw new IllegalArgumentException("Unsupported Python automation node type: " + nodeType + ".");
			}
			if (nodeTypes.putIfAbsent(nodeId, nodeType) != null) {
				throw new IllegalArgumentException("Python automation definition has duplicate node id: " + nodeId + ".");
			}
			if (AutomationConstants.NODE_START.equals(nodeType)) {
				startCount++;
			} else if (node.containsKey(AutomationConstants.NODE_FIELD_OUTPUT_VAR)) {
				String outputVar = requireNonblankString(
						node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR),
						"graph.nodes[" + index + "].outputVar");
				if (!outputVariables.add(outputVar)) {
					throw new IllegalArgumentException(
							"Python automation definition has duplicate outputVar: " + outputVar + ".");
				}
			}
			Object config = node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (config != null && !(config instanceof Map<?, ?>)) {
				throw new IllegalArgumentException("Node '" + nodeId + "' config must be an object.");
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> nodeConfig = config instanceof Map<?, ?> map
					? (Map<String, Object>) map : Map.of();
			validateNodeConfig(nodeId, nodeType, nodeConfig);
			Object codeMode = node.get(AutomationConstants.NODE_FIELD_CODE_MODE);
			if (codeMode != null && !AutomationConstants.NODE_CODE_MODE_GENERATED.equals(codeMode)
					&& !AutomationConstants.NODE_CODE_MODE_CUSTOM.equals(codeMode)) {
				throw new IllegalArgumentException("Node '" + nodeId + "' has unsupported codeMode: " + codeMode + ".");
			}
			if (AutomationConstants.NODE_CODE_MODE_CUSTOM.equals(codeMode)
					&& !(config instanceof Map<?, ?>)) {
				throw new IllegalArgumentException("Custom node '" + nodeId + "' must declare a config object.");
			}
		}
		if (startCount != 1) {
			throw new IllegalArgumentException("Python automation definition must contain exactly one start node.");
		}
		return nodeTypes;
	}

	private static void validateNodeConfig(String nodeId, String nodeType, Map<String, Object> config) {
		if (AutomationConstants.NODE_START.equals(nodeType)) {
			validateTriggerConfig(nodeId, config);
			return;
		}
		if (AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType)) {
			return;
		}
		if (requiresEngine(nodeType)) {
			requireConfigString(nodeId, config, AutomationConstants.CONFIG_ENGINE_ID);
		}

		switch (nodeType) {
			case AutomationConstants.NODE_DATABASE_QUERY,
					AutomationConstants.NODE_DATABASE_INSERT,
					AutomationConstants.NODE_DATABASE_UPDATE -> requireConfigString(nodeId, config, "query");
			case AutomationConstants.NODE_MODEL_CHAT -> requireConfigString(nodeId, config, "prompt");
			case AutomationConstants.NODE_MODEL_EMBEDDINGS,
					AutomationConstants.NODE_MODEL_NER -> requireConfigString(nodeId, config, "text");
			case AutomationConstants.NODE_MODEL_VISION -> {
				requireConfigString(nodeId, config, "prompt");
				requireConfigString(nodeId, config, "image");
			}
			case AutomationConstants.NODE_STORAGE_READ,
					AutomationConstants.NODE_STORAGE_UPLOAD,
					AutomationConstants.NODE_STORAGE_DOWNLOAD,
					AutomationConstants.NODE_STORAGE_DELETE -> requireConfigString(nodeId, config, "path");
			case AutomationConstants.NODE_VECTOR_SEARCH,
					AutomationConstants.NODE_VECTOR_ADD,
					AutomationConstants.NODE_VECTOR_DELETE -> requireConfigString(nodeId, config, "value");
			case AutomationConstants.NODE_FUNCTION_EXECUTE -> requireConfigString(nodeId, config, "arguments");
			case AutomationConstants.NODE_APP_PIXEL -> requireConfigString(nodeId, config, "pixel");
			case AutomationConstants.NODE_AGENT_RUN -> {
				requireConfigString(nodeId, config, AutomationConstants.CONFIG_ROOM_ID);
				requireConfigString(nodeId, config, AutomationConstants.CONFIG_COMMAND);
			}
			case AutomationConstants.NODE_CONTROL_WAIT -> validateWaitConfig(nodeId, config);
			default -> {
				// All supported node types are covered above or require only an engine ID.
			}
		}
	}

	private static void validateTriggerConfig(String nodeId, Map<String, Object> config) {
		validateOptionalSource(nodeId, config, AutomationConstants.CONFIG_PYTHON_SOURCE);
		validateOptionalSource(nodeId, config, AutomationConstants.CONFIG_PYTHON);
		Object canonical = config.get(AutomationConstants.CONFIG_PYTHON_SOURCE);
		Object legacy = config.get(AutomationConstants.CONFIG_PYTHON);
		if (canonical instanceof String canonicalSource && !canonicalSource.isBlank()
				&& legacy instanceof String legacySource && !legacySource.isBlank()
				&& !canonicalSource.equals(legacySource)) {
			throw new IllegalArgumentException("Trigger node '" + nodeId
					+ "' cannot provide different pythonSource and python values.");
		}
		if (!config.containsKey(AutomationConstants.CONFIG_GLOBALS)) {
			return;
		}
		Object rawGlobals = config.get(AutomationConstants.CONFIG_GLOBALS);
		if (!(rawGlobals instanceof List<?> globals)) {
			throw new IllegalArgumentException("Trigger node '" + nodeId + "' config.globals must be an array.");
		}
		Set<String> names = new HashSet<>();
		for (int index = 0; index < globals.size(); index++) {
			Map<String, Object> global = requireMap(globals.get(index),
					"Trigger node '" + nodeId + "' config.globals[" + index + "]");
			String name = requireNonblankString(global.get("name"),
					"Trigger node '" + nodeId + "' config.globals[" + index + "].name");
			if (!name.matches("[A-Za-z][A-Za-z0-9_]*")) {
				throw new IllegalArgumentException("Trigger global '" + name
						+ "' must be a non-private Python identifier.");
			}
			if (!names.add(name)) {
				throw new IllegalArgumentException("Trigger node '" + nodeId
						+ "' has duplicate global name: " + name + ".");
			}
			if (!global.containsKey(AutomationConstants.CONFIG_DEFAULT_VALUE)) {
				throw new IllegalArgumentException("Trigger global '" + name + "' must provide defaultValue.");
			}
			Object description = global.get(AutomationConstants.CONFIG_DESCRIPTION);
			if (description != null && !(description instanceof String)) {
				throw new IllegalArgumentException("Trigger global '" + name + "' description must be a string.");
			}
		}
	}

	private static void validateOptionalSource(String nodeId, Map<String, Object> config, String key) {
		Object source = config.get(key);
		if (source != null && !(source instanceof String)) {
			throw new IllegalArgumentException("Trigger node '" + nodeId + "' config." + key + " must be a string.");
		}
	}

	private static boolean requiresEngine(String nodeType) {
		return nodeType.startsWith("database.")
				|| nodeType.startsWith("model.")
				|| nodeType.startsWith("storage.")
				|| nodeType.startsWith("vector.")
				|| AutomationConstants.NODE_FUNCTION_EXECUTE.equals(nodeType);
	}

	private static void validateWaitConfig(String nodeId, Map<String, Object> config) {
		Object value = config.get("durationSeconds");
		if (value instanceof Number number
				&& number.doubleValue() >= AutomationConstants.WAIT_MIN_SECONDS
				&& number.doubleValue() <= AutomationConstants.WAIT_MAX_SECONDS) {
			return;
		}
		if (value instanceof String string) {
			try {
				double seconds = Double.parseDouble(string);
				if (seconds >= AutomationConstants.WAIT_MIN_SECONDS
						&& seconds <= AutomationConstants.WAIT_MAX_SECONDS) {
					return;
				}
			} catch (NumberFormatException ignored) {
				// Report the shared validation error below.
			}
		}
		throw new IllegalArgumentException("Node '" + nodeId + "' durationSeconds must be between "
				+ AutomationConstants.WAIT_MIN_SECONDS + " and " + AutomationConstants.WAIT_MAX_SECONDS + ".");
	}

	private static void requireConfigString(String nodeId, Map<String, Object> config, String key) {
		Object value = config.get(key);
		if (!(value instanceof String string) || string.isBlank()) {
			throw new IllegalArgumentException("Node '" + nodeId + "' config." + key
					+ " must be a nonblank string.");
		}
	}

	private static void validateEdges(List<Map<String, Object>> edges, Map<String, String> nodeTypes) {
		Set<String> edgeIds = new HashSet<>();
		for (int index = 0; index < edges.size(); index++) {
			Map<String, Object> edge = edges.get(index);
			String edgeId = requireNonblankString(edge.get(AutomationConstants.NODE_FIELD_ID),
					"graph.edges[" + index + "].id");
			if (!edgeIds.add(edgeId)) {
				throw new IllegalArgumentException("Python automation definition has duplicate edge id: " + edgeId + ".");
			}
			String kind = requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_KIND),
					"graph.edges[" + index + "].kind");
			if (!AutomationConstants.EDGE_KIND_CONTROL.equals(kind) && !AutomationConstants.EDGE_KIND_DATA.equals(kind)) {
				throw new IllegalArgumentException("Automation edge '" + edgeId + "' must have kind 'control' or 'data'.");
			}
			String source = requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_SOURCE),
					"graph.edges[" + index + "].source");
			String target = requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_TARGET),
					"graph.edges[" + index + "].target");
			if (!nodeTypes.containsKey(source) || !nodeTypes.containsKey(target)) {
				throw new IllegalArgumentException("Automation edge '" + edgeId + "' references an unknown node.");
			}
			if (source.equals(target)) {
				throw new IllegalArgumentException(
						"Automation edge '" + edgeId + "' cannot reference the same source and target node.");
			}
			requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_SOURCE_PORT),
					"graph.edges[" + index + "].sourcePort");
			requireNonblankString(edge.get(AutomationConstants.EDGE_FIELD_TARGET_PORT),
					"graph.edges[" + index + "].targetPort");
		}
	}

	private static void validateTriggerBindings(Object value) {
		if (value == null) {
			return;
		}
		List<Map<String, Object>> bindings = requireMapList(value, "triggerBindings");
		Set<String> ids = new HashSet<>();
		for (int index = 0; index < bindings.size(); index++) {
			String id = requireNonblankString(bindings.get(index).get(AutomationConstants.NODE_FIELD_ID),
					"triggerBindings[" + index + "].id");
			if (!ids.add(id)) {
				throw new IllegalArgumentException("Python automation definition has duplicate trigger binding id: " + id + ".");
			}
			requireNonblankString(bindings.get(index).get(AutomationConstants.NODE_FIELD_TYPE),
					"triggerBindings[" + index + "].type");
		}
	}

	private static Map<String, Object> requireMap(Object value, String field) {
		if (!(value instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException("Python automation definition field '" + field + "' must be an object.");
		}
		Map<String, Object> result = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			if (!(entry.getKey() instanceof String key)) {
				throw new IllegalArgumentException("Python automation definition field '" + field + "' has a non-string key.");
			}
			result.put(key, entry.getValue());
		}
		return result;
	}

	private static List<Map<String, Object>> requireMapList(Object value, String field) {
		if (!(value instanceof List<?> values)) {
			throw new IllegalArgumentException("Python automation definition field '" + field + "' must be an array.");
		}
		List<Map<String, Object>> result = new ArrayList<>();
		for (int index = 0; index < values.size(); index++) {
			result.add(requireMap(values.get(index), field + "[" + index + "]"));
		}
		return result;
	}

	private static String requireNonblankString(Object value, String field) {
		if (!(value instanceof String string) || string.isBlank()) {
			throw new IllegalArgumentException("Python automation definition field '" + field + "' must be a nonblank string.");
		}
		return string;
	}

	private static Object canonicalize(Object value) {
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> sorted = new TreeMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				sorted.put((String) entry.getKey(), canonicalize(entry.getValue()));
			}
			return sorted;
		}
		if (value instanceof List<?> list) {
			List<Object> values = new ArrayList<>();
			for (Object entry : list) {
				values.add(canonicalize(entry));
			}
			return values;
		}
		return value;
	}

	private static String sha256(String value) {
		try {
			byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			StringBuilder hash = new StringBuilder(digest.length * 2);
			for (byte entry : digest) {
				hash.append(String.format("%02x", entry));
			}
			return hash.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	/** Validated graph and deterministic provenance snapshot. */
	public record ValidatedDefinition(Map<String, Object> definition, List<Map<String, Object>> nodes,
			List<Map<String, Object>> edges, String snapshot, String hash) {
	}
}
