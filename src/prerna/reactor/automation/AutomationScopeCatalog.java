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
 ******************************************************************************/
package prerna.reactor.automation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes the run-scope values that are guaranteed to exist when each node
 * executes.
 *
 * <p>
 * The server derives this contract from the validated control graph so editors
 * can provide variable insertion and IntelliSense without duplicating graph
 * semantics. At a branch merge, outputs produced on only one branch are marked
 * conditional and use a safe {@code scope.get(...)} insertion expression.
 */
final class AutomationScopeCatalog {

	private AutomationScopeCatalog() {
	}

	/**
	 * Returns scope-variable descriptors keyed by node ID.
	 *
	 * @param definition validated Automation graph
	 * @return ordered variables available or conditionally available at each node
	 */
	static Map<String, List<Map<String, Object>>> variablesByNode(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		List<Map<String, Object>> orderedNodes = AutomationRuntime.nodesForRun(definition);
		Map<String, List<String>> predecessors = predecessors(definition);
		Map<String, Set<String>> ancestors = ancestors(orderedNodes, predecessors);
		Map<String, Set<String>> dominators = dominators(orderedNodes, predecessors);
		List<Map<String, Object>> sharedVariables = sharedVariables(definition);
		Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();

		for (Map<String, Object> target : orderedNodes) {
			String targetId = string(target.get(AutomationConstants.NODE_FIELD_ID));
			Map<String, Map<String, Object>> variablesByName = new LinkedHashMap<>();
			for (Map<String, Object> variable : sharedVariables) {
				variablesByName.put((String) variable.get("name"), variable);
			}
			Set<String> possibleNodes = ancestors.getOrDefault(targetId, Set.of());
			Set<String> guaranteedNodes = dominators.getOrDefault(targetId, Set.of());
			for (Map<String, Object> source : orderedNodes) {
				String sourceId = string(source.get(AutomationConstants.NODE_FIELD_ID));
				if (!possibleNodes.contains(sourceId)) {
					continue;
				}
				String outputVar = string(source.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR));
				if (outputVar == null || outputVar.isBlank()) {
					continue;
				}
				variablesByName.put(outputVar,
						variable(outputVar, "node", string(source.get(AutomationConstants.NODE_FIELD_LABEL)),
								"Output from an earlier step.", sourceId, null,
								guaranteedNodes.contains(sourceId) ? "guaranteed" : "conditional"));
			}
			result.put(targetId, new ArrayList<>(variablesByName.values()));
		}
		return result;
	}

	private static List<Map<String, Object>> sharedVariables(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		List<Map<String, Object>> variables = new ArrayList<>();
		variables.add(variable(AutomationConstants.SCOPE_DATE, "runtime", "Run date",
				"The run date in the triggering user's timezone.", null, null, "guaranteed"));
		variables.add(variable(AutomationConstants.SCOPE_TRIGGERED_AT, "runtime", "Triggered at",
				"The run timestamp in the triggering user's timezone.", null, null, "guaranteed"));
		variables.add(variable(AutomationConstants.SCOPE_RUN_ID, "runtime", "Run ID",
				"The durable identifier for this automation run.", null, null, "guaranteed"));

		for (Map<String, Object> node : definition.nodes()) {
			if (!AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				continue;
			}
			Object rawConfig = node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (!(rawConfig instanceof Map<?, ?> config)) {
				break;
			}
			Object rawGlobals = config.get(AutomationConstants.CONFIG_GLOBALS);
			if (!(rawGlobals instanceof List<?> globals)) {
				break;
			}
			for (Object value : globals) {
				if (!(value instanceof Map<?, ?> global)) {
					continue;
				}
				String name = string(global.get("name"));
				if (name == null || name.isBlank()) {
					continue;
				}
				String description = string(global.get(AutomationConstants.CONFIG_DESCRIPTION));
				variables.add(variable(name, "global", name,
						description == null ? "Automation input." : description, null,
						global.get(AutomationConstants.CONFIG_DEFAULT_VALUE), "guaranteed"));
			}
			break;
		}
		return variables;
	}

	private static Map<String, List<String>> predecessors(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Map<String, List<String>> result = new HashMap<>();
		for (Map<String, Object> edge : definition.edges()) {
			if (!AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))) {
				continue;
			}
			String source = string(edge.get(AutomationConstants.EDGE_FIELD_SOURCE));
			String target = string(edge.get(AutomationConstants.EDGE_FIELD_TARGET));
			result.computeIfAbsent(target, ignored -> new ArrayList<>()).add(source);
		}
		return result;
	}

	private static Map<String, Set<String>> ancestors(List<Map<String, Object>> orderedNodes,
			Map<String, List<String>> predecessors) {
		Map<String, Set<String>> result = new HashMap<>();
		for (Map<String, Object> node : orderedNodes) {
			String nodeId = string(node.get(AutomationConstants.NODE_FIELD_ID));
			Set<String> nodeAncestors = new HashSet<>();
			for (String parent : predecessors.getOrDefault(nodeId, List.of())) {
				nodeAncestors.add(parent);
				nodeAncestors.addAll(result.getOrDefault(parent, Set.of()));
			}
			result.put(nodeId, nodeAncestors);
		}
		return result;
	}

	private static Map<String, Set<String>> dominators(List<Map<String, Object>> orderedNodes,
			Map<String, List<String>> predecessors) {
		Map<String, Set<String>> result = new HashMap<>();
		for (Map<String, Object> node : orderedNodes) {
			String nodeId = string(node.get(AutomationConstants.NODE_FIELD_ID));
			List<String> parents = predecessors.getOrDefault(nodeId, List.of());
			Set<String> nodeDominators = new HashSet<>();
			if (!parents.isEmpty()) {
				nodeDominators.addAll(result.getOrDefault(parents.get(0), Set.of()));
				for (int index = 1; index < parents.size(); index++) {
					nodeDominators.retainAll(result.getOrDefault(parents.get(index), Set.of()));
				}
			}
			nodeDominators.add(nodeId);
			result.put(nodeId, nodeDominators);
		}
		return result;
	}

	private static Map<String, Object> variable(String name, String source, String label, String description,
			String sourceNodeId, Object defaultValue, String availability) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("name", name);
		result.put("source", source);
		result.put("label", label == null || label.isBlank() ? name : label);
		result.put("description", description);
		result.put("availability", availability);
		String requiredExpression = "scope[\"" + name + "\"]";
		String optionalExpression = "scope.get(\"" + name + "\")";
		result.put("pythonExpression",
				"conditional".equals(availability) ? optionalExpression : requiredExpression);
		result.put("requiredPythonExpression", requiredExpression);
		result.put("optionalPythonExpression", optionalExpression);
		result.put("templateExpression", "${" + name + "}");
		if (sourceNodeId != null) {
			result.put("sourceNodeId", sourceNodeId);
		}
		if (defaultValue != null) {
			result.put("defaultValue", defaultValue);
		}
		return result;
	}

	private static String string(Object value) {
		return value == null ? null : value.toString();
	}
}
