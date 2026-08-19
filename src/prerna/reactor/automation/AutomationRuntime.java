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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.reactor.automation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Java-side graph ordering and one-node Python invocation support. */
final class AutomationRuntime {

	private static final Pattern GLOBAL_ASSIGNMENT = Pattern.compile(
			"^([A-Za-z][A-Za-z0-9_]*)\\s*=\\s*(.+?)(?:\\s+#.*)?$");

	private AutomationRuntime() {
	}

	static List<Map<String, Object>> nodesForRun(AutomationDefinitionValidator.ValidatedDefinition definition) {
		List<Map<String, Object>> nodes = new ArrayList<>();
		for (Map<String, Object> original : controlOrderedNodes(definition)) {
			Map<String, Object> node = new LinkedHashMap<>(original);
			node.putIfAbsent(AutomationConstants.NODE_FIELD_LABEL,
					node.get(AutomationConstants.NODE_FIELD_ID));
			if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				node.putIfAbsent(AutomationConstants.NODE_FIELD_OUTPUT_VAR,
						defaultOutputVariable(node));
			}
			nodes.add(node);
		}
		return nodes;
	}

	private static String defaultOutputVariable(Map<String, Object> node) {
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		String id = (String) node.get(AutomationConstants.NODE_FIELD_ID);
		if (type == null || id == null || id.length() < 6) {
			return id;
		}
		return type.replace('.', '_') + "_" + id.substring(id.length() - 6);
	}

	/**
	 * Resolves the one supported sequential control path. This is intentionally Java-owned: Python
	 * sources cannot select nodes or alter graph order.
	 */
	static List<Map<String, Object>> controlOrderedNodes(AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, Map<String, Object>> nodes = new LinkedHashMap<>();
		String start = null;
		for (Map<String, Object> node : definition.nodes()) {
			String id = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			nodes.put(id, node);
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				start = id;
			}
		}
		Map<String, String> outgoing = new HashMap<>();
		for (Map<String, Object> edge : definition.edges()) {
			if (!AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))) {
				continue;
			}
			String source = (String) edge.get(AutomationConstants.EDGE_FIELD_SOURCE);
			String target = (String) edge.get(AutomationConstants.EDGE_FIELD_TARGET);
			if (outgoing.putIfAbsent(source, target) != null) {
				throw new IllegalArgumentException("Automation supports only one outgoing control edge per node.");
			}
		}
		List<Map<String, Object>> ordered = new ArrayList<>();
		Set<String> visited = new HashSet<>();
		String current = start;
		while (current != null) {
			if (!visited.add(current)) {
				throw new IllegalArgumentException("Automation control edges must not contain a cycle.");
			}
			ordered.add(nodes.get(current));
			current = outgoing.get(current);
		}
		if (ordered.size() != nodes.size()) {
			throw new IllegalArgumentException("Every automation node must be connected to trigger.start by control edges.");
		}
		return ordered;
	}

	/** Runs one node module with the workflow scope supplied by the Java scheduler. */
	static String buildNodeInvocationScript(String source, Map<String, String> scope) {
		return buildNodeInvocationScript(source, scope, Map.of());
	}

	static String buildNodeInvocationScript(String source, Map<String, String> scope,
			Map<String, String> config) {
		return """
				import base64 as _automation_b64
				import json as _automation_json
				import re as _automation_re
				_automation_scope = _automation_json.loads(
				    _automation_b64.urlsafe_b64decode("%s").decode("utf-8"))
				_automation_config = _automation_json.loads(
				    _automation_b64.urlsafe_b64decode("%s").decode("utf-8"))
				_automation_scope["_automation_config"] = _automation_config

				def resolve(value, scope):
				    if not isinstance(value, str):
				        return value

				    def _automation_replace(match):
				        key = match.group(1)
				        if key.startswith("config."):
				            return _automation_config.get(key[7:], match.group(0))
				        return scope.get(key, match.group(0))

				    return _automation_re.sub(r"\\$\\{([^}]+)\\}", _automation_replace, value)

				%s

				_automation_result = run(_automation_scope)
				_automation_json.loads(_automation_json.dumps(_automation_result, default=str))
				""".formatted(
						encode(AutomationRuntimeUtils.GSON.toJson(scope != null ? scope : Map.of())),
						encode(AutomationRuntimeUtils.GSON.toJson(config != null ? config : Map.of())),
						source);
	}

	/**
	 * Executes trigger Python in an isolated module and returns its non-private,
	 * JSON-compatible globals. A trigger may also return a map from {@code run(scope)}
	 * to define computed globals.
	 */
	static String buildTriggerInvocationScript(String source, Map<String, String> scope) {
		return """
				import base64 as _automation_b64
				import json as _automation_json

				_automation_scope = _automation_json.loads(
				    _automation_b64.urlsafe_b64decode("%s").decode("utf-8"))
				_automation_module = {}
				exec(_automation_b64.urlsafe_b64decode("%s").decode("utf-8"), _automation_module)
				_automation_run = _automation_module.get("run")
				_automation_result = _automation_run(_automation_scope) if callable(_automation_run) else None
				_automation_globals = {}
				for _automation_name, _automation_value in _automation_module.items():
				    if _automation_name.startswith("_") or callable(_automation_value):
				        continue
				    try:
				        _automation_json.dumps(_automation_value)
				        _automation_globals[_automation_name] = _automation_value
				    except (TypeError, ValueError):
				        pass
				if isinstance(_automation_result, dict):
				    for _automation_name, _automation_value in _automation_result.items():
				        if (isinstance(_automation_name, str)
				                and not _automation_name.startswith("_")):
				            try:
				                _automation_json.dumps(_automation_value)
				                _automation_globals[_automation_name] = _automation_value
				            except (TypeError, ValueError):
				                pass
				_automation_json.loads(_automation_json.dumps(_automation_globals))
				""".formatted(
						encode(AutomationRuntimeUtils.GSON.toJson(scope != null ? scope : Map.of())),
						encode(source != null ? source : ""));
	}

	/**
	 * Reads literal top-level globals for Get/Save and playground defaults without
	 * executing user Python. Non-literal values are available at trigger time only.
	 */
	static Map<String, Object> declaredGlobals(String source) {
		Map<String, Object> globals = new LinkedHashMap<>();
		if (source == null || source.isBlank()) {
			return globals;
		}
		for (String line : source.split("\\R")) {
			if (!line.isEmpty() && Character.isWhitespace(line.charAt(0))) {
				continue;
			}
			Matcher assignment = GLOBAL_ASSIGNMENT.matcher(line);
			if (!assignment.matches()) {
				continue;
			}
			Object value = parseLiteral(assignment.group(2).trim());
			if (value != UnparsedLiteral.INSTANCE) {
				globals.put(assignment.group(1), value);
			}
		}
		return globals;
	}

	static Map<String, Object> declaredGlobals(AutomationDefinitionValidator.ValidatedDefinition definition,
			Map<String, String> nodeSources) {
		Map<String, Object> globals = new LinkedHashMap<>();
		for (Map<String, Object> global : triggerGlobalDefinitions(definition, nodeSources)) {
			globals.put((String) global.get("name"), global.get(AutomationConstants.CONFIG_DEFAULT_VALUE));
		}
		return globals;
	}

	/**
	 * Returns the canonical trigger declarations from {@code config.globals}. Legacy trigger
	 * Python assignments remain a read-only fallback for pre-migration workflow definitions.
	 */
	@SuppressWarnings("unchecked")
	static List<Map<String, Object>> triggerGlobalDefinitions(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		for (Map<String, Object> node : definition.nodes()) {
			if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				continue;
			}
			Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (rawConfig instanceof Map<?, ?> config
					&& config.containsKey(AutomationConstants.CONFIG_GLOBALS)) {
				Object rawGlobals = config.get(AutomationConstants.CONFIG_GLOBALS);
				if (rawGlobals instanceof List<?> values) {
					List<Map<String, Object>> globals = new ArrayList<>();
					for (Object value : values) {
						if (value instanceof Map<?, ?> map) {
							globals.add(new LinkedHashMap<>((Map<String, Object>) map));
						}
					}
					return globals;
				}
			}
		}
		List<Map<String, Object>> globals = new ArrayList<>();
		for (Map.Entry<String, Object> entry : declaredGlobals(triggerSource(definition, nodeSources)).entrySet()) {
			Map<String, Object> global = new LinkedHashMap<>();
			global.put("name", entry.getKey());
			global.put(AutomationConstants.CONFIG_DEFAULT_VALUE, entry.getValue());
			globals.add(global);
		}
		return globals;
	}

	static String triggerSource(AutomationDefinitionValidator.ValidatedDefinition definition,
			Map<String, String> nodeSources) {
		for (Map<String, Object> node : definition.nodes()) {
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				return triggerSource(node, nodeSources.get((String) node.get(AutomationConstants.NODE_FIELD_ID)));
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	static String triggerSource(Map<String, Object> node, String legacySource) {
		Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		if (rawConfig instanceof Map<?, ?> raw) {
			Map<String, Object> config = (Map<String, Object>) raw;
			String source = sourceValue(config.get(AutomationConstants.CONFIG_PYTHON_SOURCE));
			if (source != null) {
				return source;
			}
			source = sourceValue(config.get(AutomationConstants.CONFIG_PYTHON));
			if (source != null) {
				return source;
			}
		}
		return legacySource;
	}

	@SuppressWarnings("unchecked")
	static Map<String, Object> triggerGlobalDefaults(Map<String, Object> node) {
		Map<String, Object> globals = new LinkedHashMap<>();
		Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		if (!(rawConfig instanceof Map<?, ?> raw)) {
			return globals;
		}
		Object rawGlobals = ((Map<String, Object>) raw).get(AutomationConstants.CONFIG_GLOBALS);
		if (!(rawGlobals instanceof List<?> values)) {
			return globals;
		}
		for (Object value : values) {
			if (value instanceof Map<?, ?> rawGlobal) {
				Map<String, Object> global = (Map<String, Object>) rawGlobal;
				Object name = global.get("name");
				if (name instanceof String stringName
						&& global.containsKey(AutomationConstants.CONFIG_DEFAULT_VALUE)) {
					globals.put(stringName, global.get(AutomationConstants.CONFIG_DEFAULT_VALUE));
				}
			}
		}
		return globals;
	}

	private static String sourceValue(Object value) {
		return value instanceof String source && !source.isBlank() ? source : null;
	}

	private static Object parseLiteral(String value) {
		if ("True".equals(value)) {
			return true;
		}
		if ("False".equals(value)) {
			return false;
		}
		if ("None".equals(value)) {
			return null;
		}
		if (value.isEmpty() || !isJsonLiteral(value)) {
			if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
				return value.substring(1, value.length() - 1).replace("\\'", "'").replace("\\\\", "\\");
			}
			return UnparsedLiteral.INSTANCE;
		}
		try {
			return AutomationRuntimeUtils.GSON.fromJson(value, Object.class);
		} catch (Exception ignored) {
			return UnparsedLiteral.INSTANCE;
		}
	}

	private static boolean isJsonLiteral(String value) {
		char first = value.charAt(0);
		return first == '"' || first == '{' || first == '[' || first == '-' || Character.isDigit(first)
				|| "true".equals(value) || "false".equals(value) || "null".equals(value);
	}

	private enum UnparsedLiteral {
		INSTANCE
	}

	@SuppressWarnings("unchecked")
	static Object normalizeNodeResult(Object output) {
		Object value = output;
		if (value instanceof String string) {
			try {
				value = AutomationRuntimeUtils.GSON.fromJson(string, Object.class);
			} catch (Exception e) {
				return string;
			}
		}
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> result = new LinkedHashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				if (!(entry.getKey() instanceof String key)) {
					throw new IllegalArgumentException("Python automation node result contains a non-string key.");
				}
				result.put(key, entry.getValue());
			}
			return result;
		}
		return value;
	}

	private static String encode(String value) {
		return Base64.getUrlEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
	}
}
