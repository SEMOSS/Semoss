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
package prerna.reactor.automation.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Covers building a runnable definition from inline JSON, without a project,
 * and detecting the Python its caller wrote.
 */
public class AutomationDefinitionServiceUnitTests {

	private static final String CUSTOM_SOURCE = "def run(scope):\n    return {'ok': True}\n";

	/** A GetAutomation export as a user pastes it, derived response keys included. */
	private static final String EXPORT = """
			{
			  "formatVersion": 2,
			  "description": "Pasted export",
			  "triggerBindings": [{ "id": "manual", "type": "manual" }],
			  "graph": {
			    "nodes": [
			      { "id": "start", "type": "trigger.start", "label": "Start", "position": { "x": 0, "y": 0 },
			        "codeMode": "generated", "config": { "globals": [{ "name": "owner_email", "defaultValue": "" }] } },
			      { "id": "wait", "type": "control.wait", "label": "Wait", "outputVar": "waited",
			        "position": { "x": 200, "y": 0 }, "codeMode": "generated", "config": { "durationSeconds": 1 } },
			      { "id": "custom", "type": "developer.python", "label": "Custom", "outputVar": "custom_result",
			        "position": { "x": 400, "y": 0 }, "codeMode": "custom", "config": {} },
			      { "id": "agent", "type": "agent.run", "label": "Agent", "outputVar": "agent_result",
			        "position": { "x": 600, "y": 0 }, "codeMode": "generated",
			        "config": { "workspaceId": "workspace-1", "engineId": "engine-1",
			          "command": "Summarize ${custom_result}" } }
			    ],
			    "edges": [
			      { "id": "e1", "kind": "control", "source": "start", "sourcePort": "out", "target": "wait",
			        "targetPort": "in" },
			      { "id": "e2", "kind": "control", "source": "wait", "sourcePort": "out", "target": "custom",
			        "targetPort": "in" },
			      { "id": "e3", "kind": "control", "source": "custom", "sourcePort": "out", "target": "agent",
			        "targetPort": "in" }
			    ]
			  },
			  "nodeGroups": [{ "id": "group-1", "name": "Work", "nodeIds": ["wait", "custom"] }],
			  "nodeSources": {
			    "custom": "def run(scope):\\n    return {'ok': True}\\n"
			  },
			  "sourceHashes": { "wait": "0", "custom": "1", "agent": "2" },
			  "revision": "revision-1",
			  "globals": { "owner_email": "" },
			  "scopeVariables": { "wait": [], "custom": [], "agent": [] }
			}
			""";

	@Test
	void runsAPastedExportWithoutItsDerivedKeys() {
		AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.fromInline(EXPORT, null);

		Map<String, Object> definition = parse(files.definition());
		for (String derivedKey : List.of("nodeSources", "sourceHashes", "revision", "scopeVariables", "globals")) {
			assertFalse(definition.containsKey(derivedKey), derivedKey);
		}
		assertTrue(definition.containsKey("nodeGroups"));
		assertEquals(Set.of("custom"), files.nodeSources().keySet());
		assertEquals(CUSTOM_SOURCE, files.nodeSources().get("custom"));
		assertTrue(AutomationDefinitionService.containsCallerCode(files));
	}

	@Test
	void suppliedSourcesReplaceTheEmbeddedMapForDeveloperNodes() {
		String replacement = "def run(scope):\n    return 2\n";
		AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.fromInline(EXPORT,
				Map.of("custom", replacement));

		assertEquals(replacement, files.nodeSources().get("custom"));
	}

	@Test
	void developerNodeWithoutSuppliedSourceIsRejected() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionService.fromInline(EXPORT, Map.of()));
		assertTrue(error.getMessage().contains("must be nonblank"));
	}

	@Test
	void generatedNodeRejectsSuppliedSource() {
		Map<String, Object> document = parse(EXPORT);

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionService.fromInline(AutomationRuntimeUtils.GSON.toJson(document),
						Map.of("wait", CUSTOM_SOURCE)));
		assertTrue(error.getMessage().contains("Only developer.python may own Python source"));
	}

	@Test
	void rejectsSourceForAnUnknownNode() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionService.fromInline(EXPORT, Map.of("ghost", CUSTOM_SOURCE)));
		assertEquals("Python source was supplied for an unknown node: ghost", error.getMessage());
	}

	@Test
	void rejectsEmbeddedSourcesThatAreNotStrings() {
		Map<String, Object> document = parse(EXPORT);
		document.put(AutomationConstants.DOC_NODE_SOURCES, Map.of("custom", 1));

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionService.fromInline(AutomationRuntimeUtils.GSON.toJson(document), null));
		assertEquals("nodeSources must map node ids to Python source strings.", error.getMessage());
	}

	@Test
	void rejectsCustomSourceWithoutARunEntryPoint() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionService.fromInline(EXPORT, Map.of("custom", "value = 1\n")));
		assertTrue(error.getMessage().contains("must define run(scope)"));
	}

	@Test
	void requiresAnExecutableGraph() {
		Map<String, Object> document = parse(EXPORT);
		graph(document).put(AutomationConstants.DOC_EDGES, new ArrayList<>());

		assertThrows(IllegalArgumentException.class,
				() -> AutomationDefinitionService.fromInline(AutomationRuntimeUtils.GSON.toJson(document), Map.of()));
	}

	@Test
	void generatedOnlyDefinitionContainsNoCallerCode() {
		Map<String, Object> document = parse(EXPORT);
		graph(document).put(AutomationConstants.DOC_NODES, new ArrayList<>(nodes(document).subList(0, 2)));
		graph(document).put(AutomationConstants.DOC_EDGES, new ArrayList<>(edges(document).subList(0, 1)));
		assertFalse(AutomationDefinitionService.containsCallerCode(
				AutomationDefinitionService.fromInline(AutomationRuntimeUtils.GSON.toJson(document), Map.of())));
	}

	@Test
	void findsACustomNodeInsideALoopBody() {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put(AutomationConstants.NODE_FIELD_ID, "body-python");
		body.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DEVELOPER_PYTHON);
		body.put(AutomationConstants.NODE_FIELD_LABEL, "Body");
		body.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, "body_result");
		body.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_CUSTOM);
		body.put(AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		// The loop takes the id "wait" so the export's start -> wait edge reaches it.
		Map<String, Object> loop = new LinkedHashMap<>();
		loop.put(AutomationConstants.NODE_FIELD_ID, "wait");
		loop.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_CONTROL_LOOP);
		loop.put(AutomationConstants.NODE_FIELD_LABEL, "Loop");
		loop.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		loop.put(AutomationConstants.NODE_FIELD_CONFIG,
				Map.of(AutomationConstants.CONFIG_LOOP_MODE, AutomationConstants.LOOP_MODE_FOR_EACH,
						AutomationConstants.CONFIG_LOOP_ITEMS, List.of(1, 2),
						AutomationConstants.CONFIG_LOOP_BATCH_SIZE, 1,
						AutomationConstants.CONFIG_LOOP_MAX_ITERATIONS, 10));
		loop.put(AutomationConstants.NODE_FIELD_BODY,
				Map.of(AutomationConstants.DOC_NODES, List.of(body), AutomationConstants.DOC_EDGES, List.of()));
		Map<String, Object> document = parse(EXPORT);
		graph(document).put(AutomationConstants.DOC_NODES, new ArrayList<>(List.of(nodes(document).get(0), loop)));
		graph(document).put(AutomationConstants.DOC_EDGES, new ArrayList<>(edges(document).subList(0, 1)));

		AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService
				.fromInline(AutomationRuntimeUtils.GSON.toJson(document), Map.of("body-python", CUSTOM_SOURCE));

		assertEquals(Map.of("body-python", CUSTOM_SOURCE), files.nodeSources());
		assertTrue(AutomationDefinitionService.containsCallerCode(files));
	}

	private static Map<String, Object> parse(String json) {
		return AutomationRuntimeUtils.GSON.fromJson(json, AutomationRuntimeUtils.MAP_TYPE);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> graph(Map<String, Object> document) {
		return (Map<String, Object>) document.get(AutomationConstants.DOC_GRAPH);
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> nodes(Map<String, Object> document) {
		return (List<Map<String, Object>>) graph(document).get(AutomationConstants.DOC_NODES);
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> edges(Map<String, Object> document) {
		return (List<Map<String, Object>>) graph(document).get(AutomationConstants.DOC_EDGES);
	}

	private static Map<String, Object> node(Map<String, Object> document, String nodeId) {
		return nodes(document).stream().filter(node -> nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID)))
				.findFirst().orElseThrow();
	}

}
