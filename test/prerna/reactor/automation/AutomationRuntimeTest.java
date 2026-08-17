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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AutomationRuntimeTest {

	@Test
	void wrapsOneNodeWithScopeAndJsonResultContract() {
		String source = """
				def run(scope):
				    return {"value": scope["value"]}
				""";

		String script = AutomationRuntime.buildNodeInvocationScript(source, Map.of("value", "ok"));

		assertTrue(!script.contains("AutomationPythonNodeBridge"));
		assertTrue(!script.contains("def run_current_node("));
		assertTrue(script.contains("_automation_result = run(_automation_scope)"));
		assertTrue(script.contains(source.strip()));
	}

	@Test
	void normalizesOnlyJsonObjectOrListResults() {
		Map<String, Object> object = (Map<String, Object>) AutomationRuntime.normalizeNodeResult(
				"{\"value\":\"ok\"}");
		assertEquals("ok", object.get("value"));

		List<?> list = (List<?>) AutomationRuntime.normalizeNodeResult(List.of(Map.of("value", "ok")));
		assertEquals(1, list.size());
		assertEquals("not json", AutomationRuntime.normalizeNodeResult("not json"));
		assertEquals("ok", AutomationRuntime.normalizeNodeResult("ok"));
	}

	@Test
	void ordersNodesFromControlEdgesInJava() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.validate(definition("database.query", "generated"));

		assertEquals(List.of("start", "node"), AutomationRuntime.nodesForRun(definition).stream()
				.map(node -> node.get("id")).toList());
	}

	private static Map<String, Object> definition(String type, String codeMode) {
		return Map.of("formatVersion", 2, "graph", Map.of(
				"nodes", List.of(
						Map.of("id", "start", "type", "trigger.start", "config", Map.of()),
						Map.of("id", "node", "type", type, "codeMode", codeMode, "config", Map.of())),
				"edges", List.of(Map.of("id", "edge", "kind", "control", "source", "start",
						"sourcePort", "next", "target", "node", "targetPort", "in"))));
	}
}
