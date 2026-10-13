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
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *******************************************************************************/
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;

class AutomationExecutionContractUnitTests {

	private static final String PYTHON_SOURCE = "def run(scope):\n    return {}\n";

	@Test
	void submissionSelectsTypedImplementationsWithoutSourceProvenance() {
		var definition = definition(
				node("start", AutomationConstants.NODE_START, Map.of()),
				node("wait", AutomationConstants.NODE_CONTROL_WAIT, Map.of("durationSeconds", 0)),
				node("query", AutomationConstants.NODE_DATABASE_QUERY,
						Map.of("engineId", "db", "query", "SELECT 1", "limit", 50)),
				node("python", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of()));

		AutomationExecutionContract submitted = AutomationExecutionContract.forSubmission(
				definition, Map.of("python", PYTHON_SOURCE));
		AutomationExecutionContract loaded = AutomationExecutionContract.forExecution(
				new AutomationRunStore.PersistedExecutionContract(submitted.version(), submitted.snapshot()),
				definition);

		assertEquals(AutomationNodeDispatcher.Handler.START,
				handler(loaded, definition, "start"));
		assertEquals(AutomationNodeDispatcher.Handler.WAIT,
				handler(loaded, definition, "wait"));
		assertEquals(AutomationNodeDispatcher.Handler.DATABASE_QUERY,
				handler(loaded, definition, "query"));
		assertEquals(AutomationNodeDispatcher.Handler.PYTHON,
				handler(loaded, definition, "python"));
		assertEquals(3, submitted.version());
		assertTrue(submitted.snapshot().contains("java.database-query.direct-table.v2"));
		assertTrue(submitted.snapshot().contains("python.explicit-table-boundary.v2"));
		assertTrue(submitted.snapshot().contains("\"definitionHash\""));
		assertTrue(submitted.snapshot().contains("\"implementation\""));
	}

	@Test
	void typedNodesCannotSelectPythonSource() {
		var definition = definition(node("wait", AutomationConstants.NODE_CONTROL_WAIT,
				Map.of("durationSeconds", 0)));

		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationExecutionContract.forSubmission(definition, Map.of("wait", PYTHON_SOURCE)));

		assertTrue(error.getMessage().contains("cannot select Python source"));
	}

	@Test
	void durableItemsRequireSourceOnlyForSelectedPythonExecution() {
		var definition = definition(
				node("typed", AutomationConstants.NODE_CONTROL_WAIT, Map.of("durationSeconds", 0)),
				node("python", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of()));
		AutomationExecutionContract contract = AutomationExecutionContract.forSubmission(
				definition, Map.of("python", PYTHON_SOURCE));

		assertFalse(contract.requiresPythonSource("typed"));
		assertTrue(contract.requiresPythonSource("python"));
	}

	@Test
	void explicitPythonCannotSelectTypedExecutor() {
		var definition = definition(node("python", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of()));
		String invalid = "{\"version\":2,\"nodes\":{\"python\":{"
				+ "\"kind\":\"JAVA_CONTROL\",\"implementation\":\"java.wait.v1\","
				+ "\"nodeType\":\"developer.python\",\"config\":{},"
				+ "\"definitionHash\":\""
				+ AutomationExecutionContract.nodeDefinitionHash(definition.nodes().get(0))
				+ "\"}}}";

		assertThrows(IllegalStateException.class, () -> AutomationExecutionContract.forExecution(
				new AutomationRunStore.PersistedExecutionContract(2, invalid), definition));
	}

	@Test
	void immutableDescriptorRejectsConfigDrift() {
		var original = definition(node("wait", AutomationConstants.NODE_CONTROL_WAIT,
				Map.of("durationSeconds", 1)));
		AutomationExecutionContract submitted = AutomationExecutionContract.forSubmission(original, Map.of());
		var changed = definition(node("wait", AutomationConstants.NODE_CONTROL_WAIT,
				Map.of("durationSeconds", 2)));

		assertThrows(IllegalStateException.class, () -> AutomationExecutionContract.forExecution(
				new AutomationRunStore.PersistedExecutionContract(submitted.version(), submitted.snapshot()),
				changed));
	}

	@Test
	void immutableDescriptorAcceptsEquivalentDeserializedNumbers() {
		var submittedDefinition = definition(node("query", AutomationConstants.NODE_DATABASE_QUERY,
				Map.of("engineId", "db", "query", "SELECT 1", "limit", 10)));
		AutomationExecutionContract submitted =
				AutomationExecutionContract.forSubmission(submittedDefinition, Map.of());
		var deserializedDefinition = definition(node("query", AutomationConstants.NODE_DATABASE_QUERY,
				Map.of("engineId", "db", "query", "SELECT 1", "limit", 10.0d)));

		AutomationExecutionContract loaded = AutomationExecutionContract.forExecution(
				new AutomationRunStore.PersistedExecutionContract(submitted.version(), submitted.snapshot()),
				deserializedDefinition);

		assertEquals(AutomationNodeDispatcher.Handler.DATABASE_QUERY,
				handler(loaded, deserializedDefinition, "query"));
	}

	@Test
	void absentContractIsRejected() {
		var definition = definition(node("wait", AutomationConstants.NODE_CONTROL_WAIT,
				Map.of("durationSeconds", 0)));

		assertThrows(IllegalStateException.class, () -> AutomationExecutionContract.forExecution(
				new AutomationRunStore.PersistedExecutionContract(null, null), definition));
	}

	@Test
	@SuppressWarnings("unchecked")
	void reportContainsOnlyTypeOwnedAndExplicitPythonSelections() {
		var definition = definition(
				node("wait", AutomationConstants.NODE_CONTROL_WAIT, Map.of("durationSeconds", 0)),
				node("python", AutomationConstants.NODE_DEVELOPER_PYTHON, Map.of()));

		Map<String, Object> report = AutomationExecutionContract.submissionReport(
				definition, Map.of("python", PYTHON_SOURCE));
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) report.get("nodes");

		assertEquals("TYPE_OWNED_IMPLEMENTATION", nodes.get(0).get("reason"));
		assertEquals("EXPLICIT_DEVELOPER_PYTHON", nodes.get(1).get("reason"));
		assertEquals(64, String.valueOf(nodes.get(0).get("definitionHash")).length());
	}

	private static AutomationDefinitionValidator.ValidatedDefinition definition(Map<String, Object>... nodes) {
		return new AutomationDefinitionValidator.ValidatedDefinition(
				Map.of(), List.of(nodes), List.of(), "{}", "hash");
	}

	private static AutomationNodeDispatcher.Handler handler(AutomationExecutionContract contract,
			AutomationDefinitionValidator.ValidatedDefinition definition, String nodeId) {
		Map<String, Object> node = definition.nodes().stream()
				.filter(candidate -> nodeId.equals(candidate.get(AutomationConstants.NODE_FIELD_ID)))
				.findFirst()
				.orElseThrow();
		return AutomationNodeDispatcher.selectHandler(contract, nodeId,
				contract.executionDescriptor(nodeId, node));
	}

	private static Map<String, Object> node(String id, String type, Map<String, Object> config) {
		return Map.of(
				AutomationConstants.NODE_FIELD_ID, id,
				AutomationConstants.NODE_FIELD_TYPE, type,
				AutomationConstants.NODE_FIELD_CONFIG, config);
	}
}
