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

import java.util.Map;

import org.junit.jupiter.api.Test;

class AutomationDefinitionValidatorTest {

	@Test
	void validatesTypedGraphWithControlAndDataEdges() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate("""
						{
						  "formatVersion": 2,
						  "triggerBindings": [{"id": "manual", "type": "manual"}],
						  "graph": {
						    "nodes": [
						      {"id": "start", "type": "trigger.start", "config": {}},
						      {"id": "query", "type": "database.query", "config": {}},
						      {"id": "model", "type": "model.chat", "config": {}}
						    ],
						    "edges": [
						      {"id": "control-start-query", "kind": "control", "source": "start", "sourcePort": "next", "target": "query", "targetPort": "in"},
						      {"id": "data-query-output", "kind": "data", "source": "query", "sourcePort": "rows", "target": "model", "targetPort": "context"}
						    ]
						  }
						}
						""");

		assertEquals(3, definition.nodes().size());
		assertEquals(2, definition.edges().size());
	}

	@Test
	void rejectsUnknownNodesAndNonWhileSelfEdges() {
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate("""
						{"formatVersion":2,"graph":{"nodes":[{"id":"start","type":"trigger.start"},{"id":"unknown","type":"unknown"}],"edges":[]}}
						"""));
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate("""
						{"formatVersion":2,"graph":{"nodes":[{"id":"start","type":"trigger.start"}],"edges":[{"id":"loop","kind":"control","source":"start","sourcePort":"next","target":"start","targetPort":"in"}]}}
						"""));
	}

	@Test
	void rendersDeterministicCurrentNodePythonSource() {
		AutomationDefinitionValidator.ValidatedDefinition definition =
				AutomationDefinitionValidator.parseAndValidate("""
						{"formatVersion":2,"graph":{"nodes":[
						  {"id":"start","type":"trigger.start","config":{}},
						  {"id":"node","type":"database.query","config":{}}
						],"edges":[
						  {"id":"edge","kind":"control","source":"start","sourcePort":"next","target":"node","targetPort":"in"}
						]}}
						""");

		String source = AutomationSourceRenderer.renderNode(definition.nodes().get(1));

		assertTrue(source.contains("def run(scope):"));
		assertTrue(source.contains("from ai_server import DatabaseEngine"));
		assertTrue(source.contains("database.execQuery(query=QUERY, return_pandas=False)"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationSourceRenderer.renderNode(definition.nodes().get(0)));
	}

	@Test
	void rendersNativePythonForEveryExecutableNodeType() {
		Map<String, String> expectedCalls = Map.ofEntries(
				Map.entry("database.query", "DatabaseEngine"),
				Map.entry("database.insert", "DatabaseEngine"),
				Map.entry("database.update", "DatabaseEngine"),
				Map.entry("model.chat", "ModelEngine"),
				Map.entry("model.embeddings", "ModelEngine"),
				Map.entry("model.vision", "ModelEngine"),
				Map.entry("model.ner", "ModelEngine"),
				Map.entry("storage.action", "StorageEngine"),
				Map.entry("storage.list", "StorageEngine"),
				Map.entry("storage.read", "StorageEngine"),
				Map.entry("storage.upload", "StorageEngine"),
				Map.entry("storage.download", "StorageEngine"),
				Map.entry("storage.delete", "StorageEngine"),
				Map.entry("vector.action", "VectorEngine"),
				Map.entry("vector.search", "VectorEngine"),
				Map.entry("vector.add", "VectorEngine"),
				Map.entry("vector.delete", "VectorEngine"),
				Map.entry("function.execute", "FunctionEngine"),
				Map.entry("app.pixel", "Insight"),
				Map.entry("control.wait", "time.sleep"),
				Map.entry("developer.python", "def run(scope):"));

		for (Map.Entry<String, String> expected : expectedCalls.entrySet()) {
			String source = AutomationSourceRenderer.renderNode(Map.of(
					"id", "node",
					"type", expected.getKey(),
					"config", Map.of()));
			assertTrue(source.contains(expected.getValue()), expected.getKey());
			assertTrue(!source.contains("automation.run_current_node"), expected.getKey());
		}
	}

	@Test
	void rejectsNonObjectNodeConfig() {
		assertThrows(IllegalArgumentException.class, () ->
				AutomationDefinitionValidator.parseAndValidate("""
						{"formatVersion":2,"graph":{"nodes":[{"id":"start","type":"trigger.start","config":"invalid"}],"edges":[]}}
						"""));
	}
}
