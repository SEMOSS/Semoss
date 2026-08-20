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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
		assertTrue(script.contains("def resolve(value, scope):"));
		assertTrue(!script.contains("_automation_config"));
		assertTrue(script.contains("_automation_module = {"));
		assertTrue(script.contains("\"resolve\": resolve"));
		assertTrue(script.contains("exec(_automation_b64.urlsafe_b64decode"));
		assertTrue(script.contains("_automation_run = _automation_module.get(\"run\")"));
		assertTrue(script.contains("_automation_result = _automation_run(_automation_scope)"));
		assertTrue(!script.contains(source.strip()));
	}

	@Test
	void requiresCallableRunFromIsolatedNodeModule() {
		String script = AutomationRuntime.buildNodeInvocationScript("value = 1", Map.of());

		assertTrue(script.contains("if not callable(_automation_run):"));
		assertTrue(script.contains("Automation node source must define callable run(scope)."));
		assertTrue(script.contains("\"__name__\": \"__automation_node__\""));
	}

	@Test
	void resolvesWholePlaceholderLiteralsInCustomSource() {
		String source = """
				def run(scope):
				    message = "${model_chat_3}"
				    return {"message": message}
				""";

		String script = AutomationRuntime.buildNodeInvocationScript(source,
				Map.of("model_chat_3", "resolved output"), true);

		String rewritten = """
				def run(scope):
				    message = resolve("${model_chat_3}", scope)
				    return {"message": message}
				""";
		String encodedRewritten = Base64.getUrlEncoder().encodeToString(
				rewritten.getBytes(StandardCharsets.UTF_8));
		assertTrue(script.contains(encodedRewritten));
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

	@Test
	void exposesOnlyLiteralNonPrivateTriggerGlobalsForDefaults() {
		Map<String, Object> globals = AutomationRuntime.declaredGlobals("""
				customer_id = "12345"
				max_results = 25
				enabled = True
				_private = "hidden"
				computed = make_value()
				    nested = "not global"
				""");

		assertEquals(Map.of("customer_id", "12345", "max_results", 25.0, "enabled", true), globals);
	}

	@Test
	void findsTriggerSourceByNodeTypeRatherThanFixedId() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.validate(Map.of("formatVersion", 2, "graph", Map.of(
						"nodes", List.of(Map.of("id", "manual-trigger", "type", "trigger.start", "config", Map.of())),
						"edges", List.of())));

		assertEquals(Map.of("region", "east"), AutomationRuntime.declaredGlobals(definition,
				Map.of("manual-trigger", "region = \"east\"")));
	}

	@Test
	void usesTriggerConfigGlobalsAndCanonicalPythonSource() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate("""
						{"formatVersion":2,"graph":{"nodes":[
						  {"id":"trigger","type":"trigger.start","config":{
						    "globals":[
						      {"name":"ticket","defaultValue":"INC-123","description":"Ticket to process"},
						      {"name":"limit","defaultValue":25}
						    ],
						    "pythonSource":"def run(scope):\\n    return {}"
						  }}
						],"edges":[]}}
						""");

		assertEquals(Map.of("ticket", "INC-123", "limit", 25.0),
				AutomationRuntime.declaredGlobals(definition, Map.of("trigger", "legacy = True")));
		assertEquals("def run(scope):\n    return {}",
				AutomationRuntime.triggerSource(definition, Map.of("trigger", "legacy = True")));
	}

	@Test
	void wrapsTriggerSourceInAnIsolatedModule() {
		String source = """
				customer_id = "12345"
				def run(scope):
				    return {"computed": scope["input"]}
				""";

		String script = AutomationRuntime.buildTriggerInvocationScript(source, Map.of("input", "value"));

		assertTrue(script.contains("exec(_automation_b64.urlsafe_b64decode"));
		assertTrue(script.contains("_automation_module.items()"));
		assertTrue(script.contains("_automation_globals"));
		assertTrue(!script.contains("customer_id = \"12345\""));
	}

	private static Map<String, Object> definition(String type, String codeMode) {
		Map<String, Object> config = switch (type) {
			case "database.query" -> Map.of("engineId", "database-id", "query", "SELECT 1");
			default -> Map.of();
		};
		return Map.of("formatVersion", 2, "graph", Map.of(
				"nodes", List.of(
						Map.of("id", "start", "type", "trigger.start", "config", Map.of()),
						Map.of("id", "node", "type", type, "codeMode", codeMode, "config", config)),
				"edges", List.of(Map.of("id", "edge", "kind", "control", "source", "start",
						"sourcePort", "next", "target", "node", "targetPort", "in"))));
	}
}
