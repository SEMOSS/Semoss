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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Covers the server-owned scope metadata consumed by editor variable helpers. */
public class AutomationScopeCatalogUnitTests {

	@Test
	void returnsRuntimeGlobalsAndGuaranteedPriorOutputs() {
		Map<String, List<Map<String, Object>>> variables = AutomationScopeCatalog
				.variablesByNode(branchingDefinition());

		assertTrue(hasVariable(variables.get("left"), AutomationConstants.SCOPE_DATE));
		assertTrue(hasVariable(variables.get("left"), AutomationConstants.SCOPE_TRIGGERED_AT));
		assertTrue(hasVariable(variables.get("left"), AutomationConstants.SCOPE_RUN_ID));
		assertTrue(hasVariable(variables.get("left"), "region"));
		assertTrue(hasVariable(variables.get("left"), "query_result"));
		assertEquals("scope[\"query_result\"]",
				variable(variables.get("left"), "query_result").get("requiredPythonExpression"));
		assertEquals("scope.get(\"query_result\")",
				variable(variables.get("left"), "query_result").get("optionalPythonExpression"));
		assertEquals("${query_result}",
				variable(variables.get("left"), "query_result").get("templateExpression"));
	}

	@Test
	void marksBranchSpecificOutputsConditionalAtAMerge() {
		List<Map<String, Object>> mergeVariables = AutomationScopeCatalog.variablesByNode(branchingDefinition())
				.get("merge");

		assertTrue(hasVariable(mergeVariables, "query_result"));
		assertEquals("guaranteed", variable(mergeVariables, "query_result").get("availability"));
		assertEquals("conditional", variable(mergeVariables, "left_result").get("availability"));
		assertEquals("scope.get(\"right_result\")", variable(mergeVariables, "right_result").get("pythonExpression"));
	}

	private static AutomationDefinitionValidator.ValidatedDefinition branchingDefinition() {
		Map<String, Object> start = node("start", AutomationConstants.NODE_START, null,
				Map.of(AutomationConstants.CONFIG_GLOBALS,
						List.of(Map.of("name", "region", AutomationConstants.CONFIG_DEFAULT_VALUE, "east"))));
		Map<String, Object> query = node("query", AutomationConstants.NODE_DEVELOPER_PYTHON, "query_result",
				Map.of());
		Map<String, Object> decision = node("decision", AutomationConstants.NODE_CONTROL_IF, null,
				Map.of(AutomationConstants.CONFIG_CLAUSES, List.of(Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "route",
						AutomationConstants.CONFIG_CONDITION, "True"))));
		Map<String, Object> left = node("left", AutomationConstants.NODE_DEVELOPER_PYTHON, "left_result", Map.of());
		Map<String, Object> right = node("right", AutomationConstants.NODE_DEVELOPER_PYTHON, "right_result", Map.of());
		Map<String, Object> merge = node("merge", AutomationConstants.NODE_DEVELOPER_PYTHON, "merged_result", Map.of());

		Map<String, Object> definition = Map.of(AutomationConstants.DOC_FORMAT_VERSION,
				AutomationConstants.PYTHON_DOC_CURRENT_VERSION, AutomationConstants.DOC_TRIGGER_BINDINGS,
				List.of(Map.of("id", "manual", "type", "manual")), AutomationConstants.DOC_GRAPH,
				Map.of(AutomationConstants.DOC_NODES, List.of(start, query, decision, left, right, merge),
						AutomationConstants.DOC_EDGES,
						List.of(edge("start-query", "start", "out", "query"),
								edge("query-decision", "query", "out", "decision"),
								edge("decision-left", "decision", "case:route", "left"),
								edge("decision-right", "decision", "else", "right"),
								edge("left-merge", "left", "out", "merge"),
								edge("right-merge", "right", "out", "merge"))));
		return AutomationDefinitionValidator
				.parseAndValidate(AutomationRuntimeUtils.GSON.toJson(definition));
	}

	private static Map<String, Object> node(String id, String type, String outputVar, Map<String, Object> config) {
		java.util.LinkedHashMap<String, Object> node = new java.util.LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, id);
		node.put(AutomationConstants.NODE_FIELD_TYPE, type);
		node.put(AutomationConstants.NODE_FIELD_LABEL, id);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE,
				AutomationConstants.NODE_CONTROL_IF.equals(type) ? AutomationConstants.NODE_CODE_MODE_GENERATED
						: AutomationConstants.NODE_CODE_MODE_CUSTOM);
		if (outputVar != null) {
			node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, outputVar);
		}
		return node;
	}

	private static Map<String, Object> edge(String id, String source, String sourcePort, String target) {
		return Map.of(AutomationConstants.NODE_FIELD_ID, id, AutomationConstants.EDGE_FIELD_KIND,
				AutomationConstants.EDGE_KIND_CONTROL, AutomationConstants.EDGE_FIELD_SOURCE, source,
				AutomationConstants.EDGE_FIELD_SOURCE_PORT, sourcePort, AutomationConstants.EDGE_FIELD_TARGET, target,
				AutomationConstants.EDGE_FIELD_TARGET_PORT, AutomationConstants.CONTROL_PORT_IN);
	}

	private static boolean hasVariable(List<Map<String, Object>> variables, String name) {
		return variables.stream().anyMatch(variable -> name.equals(variable.get("name")));
	}

	private static Map<String, Object> variable(List<Map<String, Object>> variables, String name) {
		return variables.stream().filter(variable -> name.equals(variable.get("name"))).findFirst().orElseThrow();
	}
}
