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

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Java-side graph ordering and one-node Python invocation support. */
final class AutomationRuntime {

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
				_automation_scope = _automation_json.loads(
				    _automation_b64.urlsafe_b64decode("%s").decode("utf-8"))
				_automation_config = _automation_json.loads(
				    _automation_b64.urlsafe_b64decode("%s").decode("utf-8"))
				_automation_scope["_automation_config"] = _automation_config

				%s

				_automation_result = run(_automation_scope)
				_automation_json.loads(_automation_json.dumps(_automation_result, default=str))
				""".formatted(
						encode(AutomationRuntimeUtils.GSON.toJson(scope != null ? scope : Map.of())),
						encode(AutomationRuntimeUtils.GSON.toJson(config != null ? config : Map.of())),
						source);
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
