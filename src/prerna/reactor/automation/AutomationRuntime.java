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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Provides Java-owned graph ordering and one-node Python invocation support.
 *
 * <p>
 * Runtime traversal operates on the validated graph, while Python is invoked
 * with only the source selected for the current node and a bounded JSON scope.
 * This class never discovers or executes arbitrary project files.
 */
final class AutomationRuntime {

	private AutomationRuntime() {
	}

	static List<Map<String, Object>> nodesForRun(AutomationDefinitionValidator.ValidatedDefinition definition) {
		List<Map<String, Object>> nodes = new ArrayList<>();
		for (Map<String, Object> original : controlOrderedNodes(definition)) {
			Map<String, Object> node = new LinkedHashMap<>(original);
			node.putIfAbsent(AutomationConstants.NODE_FIELD_LABEL, node.get(AutomationConstants.NODE_FIELD_ID));
			Object nodeType = node.get(AutomationConstants.NODE_FIELD_TYPE);
			if (AutomationConstants.NODE_CONTROL_IF.equals(nodeType)
					|| AutomationConstants.NODE_CONTROL_JEV.equals(nodeType)) {
				node.remove(AutomationConstants.NODE_FIELD_OUTPUT_VAR);
			} else if (!AutomationConstants.NODE_START.equals(nodeType)) {
				node.putIfAbsent(AutomationConstants.NODE_FIELD_OUTPUT_VAR, defaultOutputVariable(node));
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
	 * Returns every node in deterministic topological order for run-history
	 * initialization. Runtime traversal still selects only one condition path and
	 * remains Java-owned.
	 */
	static List<Map<String, Object>> controlOrderedNodes(AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, Map<String, Object>> nodes = new LinkedHashMap<>();
		Map<String, Integer> incoming = new LinkedHashMap<>();
		for (Map<String, Object> node : definition.nodes()) {
			String id = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			nodes.put(id, node);
			incoming.put(id, 0);
		}
		Map<String, List<String>> outgoing = new HashMap<>();
		for (Map<String, Object> edge : definition.edges()) {
			if (!AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))) {
				continue;
			}
			String source = (String) edge.get(AutomationConstants.EDGE_FIELD_SOURCE);
			String target = (String) edge.get(AutomationConstants.EDGE_FIELD_TARGET);
			outgoing.computeIfAbsent(source, ignored -> new ArrayList<>()).add(target);
			incoming.compute(target, (ignored, count) -> count + 1);
		}

		ArrayDeque<String> ready = new ArrayDeque<>();
		incoming.forEach((nodeId, count) -> {
			if (count == 0) {
				ready.add(nodeId);
			}
		});
		List<Map<String, Object>> ordered = new ArrayList<>();
		while (!ready.isEmpty()) {
			String current = ready.removeFirst();
			ordered.add(nodes.get(current));
			for (String target : outgoing.getOrDefault(current, List.of())) {
				int remaining = incoming.compute(target, (ignored, count) -> count - 1);
				if (remaining == 0) {
					ready.add(target);
				}
			}
		}
		if (ordered.size() != nodes.size()) {
			throw new IllegalArgumentException("Automation control edges must not contain a cycle.");
		}
		return ordered;
	}

	static String startNodeId(AutomationDefinitionValidator.ValidatedDefinition definition) {
		for (Map<String, Object> node : definition.nodes()) {
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				return (String) node.get(AutomationConstants.NODE_FIELD_ID);
			}
		}
		throw new IllegalArgumentException("Automation definition has no trigger.start node.");
	}

	static Map<String, Map<String, String>> controlTargets(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, Map<String, String>> targets = new LinkedHashMap<>();
		for (Map<String, Object> edge : definition.edges()) {
			if (!AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))) {
				continue;
			}
			String source = (String) edge.get(AutomationConstants.EDGE_FIELD_SOURCE);
			String sourcePort = (String) edge.get(AutomationConstants.EDGE_FIELD_SOURCE_PORT);
			String target = (String) edge.get(AutomationConstants.EDGE_FIELD_TARGET);
			targets.computeIfAbsent(source, ignored -> new LinkedHashMap<>()).put(sourcePort, target);
		}
		return targets;
	}

	/**
	 * Runs one node module with the workflow scope and run-local Insight folder
	 * supplied by the Java scheduler.
	 */
	static String buildNodeInvocationScript(String source, Map<String, Object> scope, String workspaceRoot) {
		if (workspaceRoot == null || workspaceRoot.isBlank()) {
			throw new IllegalArgumentException("Automation node execution requires a run-local Insight folder.");
		}
		return buildPythonInvocation("execute_node", source, scope, workspaceRoot);
	}

	/**
	 * Executes trigger Python in an isolated module and returns its non-private,
	 * JSON-compatible globals. A trigger may also return a map from
	 * {@code run(scope)} to define computed globals.
	 */
	static String buildTriggerInvocationScript(String source, Map<String, Object> scope) {
		return buildPythonInvocation("execute_trigger", source, scope, null);
	}

	private static String buildPythonInvocation(String function, String source, Map<String, Object> scope,
			String workspaceRoot) {
		Path runtimePath = Path.of(Utility.getBaseFolder(), Constants.PY_BASE_FOLDER, "semoss_automation_runtime.py")
				.toAbsolutePath().normalize();
		if (!Files.isRegularFile(runtimePath)) {
			throw new IllegalStateException("Automation Python runtime is unavailable: " + runtimePath);
		}
		String workspaceArgument = workspaceRoot == null ? ""
				: ", " + AutomationRuntimeUtils.GSON.toJson(workspaceRoot);
		return """
				import importlib.util as _automation_importlib
				_automation_spec = _automation_importlib.spec_from_file_location(
				    "_semoss_automation_runtime", %s)
				_automation_runtime = _automation_importlib.module_from_spec(_automation_spec)
				_automation_spec.loader.exec_module(_automation_runtime)
				_automation_runtime.%s("%s", "%s", %d%s)
				""".formatted(AutomationRuntimeUtils.GSON.toJson(runtimePath.toString()), function,
				encode(AutomationRuntimeUtils.toBoundedRuntimeJson(scope != null ? scope : Map.of(),
						AutomationConstants.RUN_SCOPE_MAX_BYTES, "Automation run scope")),
				encode(source != null ? source : ""), AutomationConstants.NODE_OUTPUT_MAX_BYTES, workspaceArgument);
	}

	/**
	 * Returns each trigger global's declared default for Get/Save responses and
	 * playground defaults.
	 */
	static Map<String, Object> declaredGlobals(AutomationDefinitionValidator.ValidatedDefinition definition) {
		for (Map<String, Object> node : definition.nodes()) {
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				return triggerGlobalDefaults(node);
			}
		}
		return Map.of();
	}

	/**
	 * Returns the trigger declarations held in
	 * {@code trigger.start.config.globals}.
	 */
	static List<Map<String, Object>> triggerGlobalDefinitions(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		for (Map<String, Object> node : definition.nodes()) {
			if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				return triggerGlobalDefinitions(node);
			}
		}
		return List.of();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> triggerGlobalDefinitions(Map<String, Object> node) {
		Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		if (!(rawConfig instanceof Map<?, ?> config)
				|| !(config.get(AutomationConstants.CONFIG_GLOBALS) instanceof List<?> values)) {
			return List.of();
		}
		List<Map<String, Object>> globals = new ArrayList<>();
		for (Object value : values) {
			if (value instanceof Map<?, ?> map) {
				globals.add(new LinkedHashMap<>((Map<String, Object>) map));
			}
		}
		return globals;
	}

	/**
	 * Returns the optional setup source held in
	 * {@code trigger.start.config.pythonSource}.
	 */
	@SuppressWarnings("unchecked")
	static String triggerSource(Map<String, Object> node) {
		Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		if (rawConfig instanceof Map<?, ?> raw) {
			return sourceValue(((Map<String, Object>) raw).get(AutomationConstants.CONFIG_PYTHON_SOURCE));
		}
		return null;
	}

	/**
	 * Returns the declared default for each trigger global the run should seed into
	 * scope.
	 */
	static Map<String, Object> triggerGlobalDefaults(Map<String, Object> node) {
		Map<String, Object> globals = new LinkedHashMap<>();
		for (Map<String, Object> global : triggerGlobalDefinitions(node)) {
			if (global.get("name") instanceof String name
					&& global.containsKey(AutomationConstants.CONFIG_DEFAULT_VALUE)) {
				globals.put(name, global.get(AutomationConstants.CONFIG_DEFAULT_VALUE));
			}
		}
		return globals;
	}

	/**
	 * Returns the agent workspace an agent node is bound to, or null for any other
	 * node type or a node that does not name one. Both the executor and the
	 * run-history reader compare a child agent's returned workspace against this
	 * value, so they resolve it the same way.
	 */
	static String configuredAgentWorkspaceId(Map<String, Object> node) {
		if (!AutomationConstants.NODE_AGENT_RUN.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
			return null;
		}
		if (!(node.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> config)) {
			return null;
		}
		Object workspaceId = config.get(AutomationConstants.CONFIG_WORKSPACE_ID);
		if (workspaceId == null) {
			return null;
		}
		String value = workspaceId.toString().trim();
		return value.isEmpty() ? null : value;
	}

	private static String sourceValue(Object value) {
		return value instanceof String source && !source.isBlank() ? source : null;
	}

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
