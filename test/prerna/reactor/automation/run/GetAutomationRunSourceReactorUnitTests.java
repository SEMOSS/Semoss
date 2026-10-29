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
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

class GetAutomationRunSourceReactorUnitTests {

	@Test
	void returnsDeveloperPythonSourceSnapshot() {
		String source = "def run(scope):\n    return scope\n";
		Map<String, Object> view = GetAutomationRunSourceReactor.buildSourceView("run-1", "python-1",
				definition(node("python-1", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of())), "hash-1",
				Map.of("python-1", source));

		assertEquals("PYTHON", view.get("viewKind"));
		assertEquals(false, view.get("legacyPython"));
		assertEquals("developer.python", view.get("sourceIdentity"));
		assertEquals(source, view.get(AutomationConstants.SOURCE_CODE));
	}

	@Test
	void returnsTypedJavaImplementationSnapshot() {
		Map<String, Object> view = GetAutomationRunSourceReactor.buildSourceView("run-1", "query-1",
				definition(node("query-1", AutomationConstants.NODE_DATABASE_QUERY,
						Map.of("engineId", "db-1", "query", "SELECT 1"))),
				"hash-1", Map.of());

		assertEquals("IMPLEMENTATION", view.get("viewKind"));
		assertEquals(false, view.get("legacyPython"));
		assertEquals(AutomationConstants.NODE_DATABASE_QUERY, view.get("nodeType"));
		assertEquals("java.database-query.v1", view.get("implementationId"));
		assertEquals("hash-1", view.get("definitionHash"));
		assertEquals("SELECT 1", ((Map<?, ?>) view.get("config")).get("query"));
	}

	@Test
	void resolvesLoopBodyNodeFromExecutedDefinition() {
		Map<String, Object> bodyNode = node("body-python", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of());
		Map<String, Object> loop = node("loop-1", AutomationConstants.NODE_CONTROL_LOOP, Map.of());
		loop.put(AutomationConstants.NODE_FIELD_BODY,
				Map.of(AutomationConstants.DOC_NODES, List.of(bodyNode), AutomationConstants.DOC_EDGES, List.of()));

		Map<String, Object> view = GetAutomationRunSourceReactor.buildSourceView("run-1", "body-python",
				definition(loop), "hash-1", Map.of("body-python", "def run(scope):\n    return 1\n"));

		assertEquals("body-python", view.get("sourceNodeId"));
		assertEquals("PYTHON", view.get("viewKind"));
	}

	@Test
	void labelsHistoricalTypedPythonAsCompatibilitySource() {
		Map<String, Object> view = GetAutomationRunSourceReactor.buildSourceView("run-1", "wait-1",
				definition(node("wait-1", AutomationConstants.NODE_CONTROL_WAIT, Map.of())), "hash-1",
				Map.of("wait-1", "def run(scope):\n    return None\n"));

		assertTrue((Boolean) view.get("legacyPython"));
		assertEquals("developer.python.compatibility", view.get("sourceIdentity"));
		assertEquals(AutomationConstants.NODE_CONTROL_WAIT, view.get("historicalNodeType"));
	}

	@Test
	void rejectsMissingNodeAndMissingDeveloperSource() {
		String snapshot = definition(node("python-1", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of()));

		assertThrows(IllegalArgumentException.class, () -> GetAutomationRunSourceReactor.buildSourceView("run-1",
				"missing", snapshot, "hash-1", Map.of()));
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> GetAutomationRunSourceReactor.buildSourceView("run-1", "python-1", snapshot, "hash-1",
						Map.of()));
		assertFalse(error.getMessage().isBlank());
	}

	private static Map<String, Object> node(String id, String type, Map<String, Object> config) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, id);
		node.put(AutomationConstants.NODE_FIELD_TYPE, type);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	private static String definition(Map<String, Object>... nodes) {
		return AutomationRuntimeUtils.GSON.toJson(Map.of(AutomationConstants.DOC_GRAPH,
				Map.of(AutomationConstants.DOC_NODES, List.of(nodes), AutomationConstants.DOC_EDGES, List.of())));
	}
}
