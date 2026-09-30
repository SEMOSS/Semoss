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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.om.InsightStore;

/**
 * Covers how the trigger node contributes to a run. The trigger is the only node
 * whose Python lives inside the definition rather than in its own file, so the
 * accessors that read it out of config are the whole contract.
 */
public class AutomationRuntimeUnitTests {

	private static Map<String, Object> triggerNode(Map<String, Object> config) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, "start");
		node.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_START);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		return node;
	}

	@Test
	void readsSetupSourceOutOfTriggerConfig() {
		String source = "def run(scope):\n    return {\"cutoff\": 1}\n";
		assertEquals(source, AutomationRuntime
				.triggerSource(triggerNode(Map.of(AutomationConstants.CONFIG_PYTHON_SOURCE, source))));
	}

	/** A blank or absent source means the trigger contributes no computed globals. */
	@Test
	void treatsBlankSetupSourceAsAbsent() {
		assertNull(AutomationRuntime.triggerSource(triggerNode(Map.of())));
		assertNull(AutomationRuntime
				.triggerSource(triggerNode(Map.of(AutomationConstants.CONFIG_PYTHON_SOURCE, "   "))));
		assertNull(AutomationRuntime.triggerSource(triggerNode(Map.of(AutomationConstants.CONFIG_PYTHON_SOURCE, 7))));
	}

	@Test
	void seedsDeclaredGlobalsFromTheirDefaults() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_GLOBALS,
				List.of(Map.of("name", "lookback_days", AutomationConstants.CONFIG_DEFAULT_VALUE, "7"),
						Map.of("name", "region", AutomationConstants.CONFIG_DEFAULT_VALUE, "east")));
		Map<String, Object> defaults = AutomationRuntime.triggerGlobalDefaults(triggerNode(config));
		assertEquals(2, defaults.size());
		assertEquals("7", defaults.get("lookback_days"));
		assertEquals("east", defaults.get("region"));
	}

	/** A global with no declared default must not seed a null into scope. */
	@Test
	void skipsGlobalsWithNoDeclaredDefault() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_GLOBALS, List.of(Map.of("name", "region")));
		assertTrue(AutomationRuntime.triggerGlobalDefaults(triggerNode(config)).isEmpty());
	}

	@Test
	void toleratesAMalformedGlobalsBlock() {
		assertTrue(AutomationRuntime.triggerGlobalDefaults(triggerNode(Map.of())).isEmpty());
		assertTrue(AutomationRuntime
				.triggerGlobalDefaults(triggerNode(Map.of(AutomationConstants.CONFIG_GLOBALS, "nonsense"))).isEmpty());
	}

	/**
	 * execute_node looks up a module-level run, so only an unindented binding
	 * counts. A run nested in a class or another function is not the entry point.
	 */
	@Test
	void recognisesTopLevelRunBindings() {
		assertTrue(AutomationDefinitionService.definesRunEntryPoint("def run(scope):\n    return {}\n"));
		assertTrue(AutomationDefinitionService.definesRunEntryPoint("async def run(scope):\n    return {}\n"));
		assertTrue(AutomationDefinitionService.definesRunEntryPoint("run = lambda scope: {}\n"));
		assertTrue(AutomationDefinitionService
				.definesRunEntryPoint("import os\n\n\ndef run(scope):\n    return os.getcwd()\n"));
	}

	@Test
	void rejectsNestedOrMissingRunBindings() {
		assertFalse(AutomationDefinitionService.definesRunEntryPoint(""));
		assertFalse(AutomationDefinitionService.definesRunEntryPoint("def helper(scope):\n    return {}\n"));
		assertFalse(AutomationDefinitionService
				.definesRunEntryPoint("class Job:\n    def run(self, scope):\n        return {}\n"));
		assertFalse(AutomationDefinitionService.definesRunEntryPoint("def outer():\n    run = 1\n    return run\n"));
	}

	@Test
	void generatedDatabaseQueriesUseTheCatalogDatasetCategory() {
		Map<String, Object> query = new LinkedHashMap<>();
		query.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_QUERY);
		query.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_GENERATED);
		assertEquals(AutomationValueType.DATASET, AutomationRuntime.resultValueType(query));

		query.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_CUSTOM);
		assertEquals(AutomationValueType.UNKNOWN, AutomationRuntime.resultValueType(query));
	}

	@Test
	void dataReferenceRoundTripsWithoutProviderDetails() {
		AutomationDataReference expected = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "opaque-reference", AutomationValueType.DATASET);
		Map<String, Object> value = expected.toMap();
		AutomationDataReference actual = AutomationDataReference.fromValue(value);

		assertEquals(expected, actual);
		assertNotNull(value.get(AutomationDataReference.MARKER));
		String serialized = prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(value);
		assertFalse(serialized.contains("engine"));
		assertFalse(serialized.contains("query"));
		assertFalse(serialized.contains("path"));
	}

	@Test
	void nodeHistoryDoesNotExposeOpaqueDataReference() {
		AutomationDataReference reference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "opaque-reference", AutomationValueType.DATASET);
		Map<String, Object> persistedValue = Map.of("summary", "bounded", "data", List.of(reference.toMap()));
		Map<String, Object> row = new LinkedHashMap<>();
		row.put(AutomationConstants.NODE_ID, "query");
		row.put(AutomationConstants.NODE_LABEL, "Query database");
		row.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		row.put(AutomationConstants.OUTPUT_VALUE,
				prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(persistedValue));
		row.put(AutomationConstants.OUTPUT_PREVIEW, AutomationDataReference.CLIENT_PREVIEW);

		Map<String, Object> result = AutomationDatabaseUtility.buildNodeResults(List.of(row), true).get(0);
		assertNull(result.get(AutomationConstants.OUTPUT_VALUE));
		assertEquals(AutomationDataReference.CLIENT_PREVIEW, result.get(AutomationConstants.OUTPUT_PREVIEW));
		assertFalse((Boolean) result.get(AutomationConstants.RESULT_DATA_AVAILABLE));
	}

	@Test
	void topLevelDataReferenceAdvertisesViewableRunDataWithoutItsIdentifier() {
		AutomationDataReference reference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "private-reference", AutomationValueType.DATASET);
		Map<String, Object> row = new LinkedHashMap<>();
		row.put(AutomationConstants.NODE_ID, "query");
		row.put(AutomationConstants.NODE_LABEL, "Query database");
		row.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		row.put(AutomationConstants.OUTPUT_VALUE,
				prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(reference.toMap()));
		row.put(AutomationConstants.OUTPUT_PREVIEW, AutomationDataReference.CLIENT_PREVIEW);

		Map<String, Object> result = AutomationDatabaseUtility.buildNodeResults(List.of(row), true).get(0);
		assertTrue((Boolean) result.get(AutomationConstants.RESULT_DATA_AVAILABLE));
		assertEquals("dataset", result.get(AutomationConstants.RESULT_DATA_VALUE_TYPE));
		assertFalse(prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(result)
				.contains("private-reference"));
	}

	@Test
	void topLevelDataReferenceIsUnavailableAfterItsExecutionInsightCloses() {
		AutomationDataReference reference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "private-reference", AutomationValueType.DATASET);
		Map<String, Object> row = new LinkedHashMap<>();
		row.put(AutomationConstants.NODE_ID, "query");
		row.put(AutomationConstants.OUTPUT_VALUE,
				prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(reference.toMap()));
		row.put(AutomationConstants.OUTPUT_PREVIEW, AutomationDataReference.CLIENT_PREVIEW);

		Map<String, Object> result = AutomationDatabaseUtility.buildNodeResults(List.of(row), false).get(0);

		assertFalse((Boolean) result.get(AutomationConstants.RESULT_DATA_AVAILABLE));
		assertEquals("dataset", result.get(AutomationConstants.RESULT_DATA_VALUE_TYPE));
	}

	@Test
	void runDataServiceReadsOnlyCanonicalTopLevelReferences() {
		AutomationDataReference reference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "private-reference", AutomationValueType.DATASET);
		Map<String, Object> row = Map.of(AutomationConstants.OUTPUT_VALUE,
				prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(reference.toMap()));

		assertEquals(reference, AutomationRunDataService.referenceFrom(row));
		assertNull(AutomationRunDataService.referenceFrom(
				Map.of(AutomationConstants.OUTPUT_VALUE, "{\"ordinary\":true}")));
		assertNull(AutomationRunDataService.referenceFrom(null));
	}

	@Test
	void dataOwnerRequiresTheCompleteImmutableTuple() {
		AutomationDataOwner owner = new AutomationDataOwner("project", "run", "user");

		assertEquals(Map.of("projectId", "project", "runId", "run", "userId", "user"), owner.toMap());
		assertThrows(IllegalArgumentException.class, () -> new AutomationDataOwner("project", "run", ""));
	}

	@Test
	void runInsightRegistryKeepsProviderDetailsOutOfScopeReferences() {
		String runId = UUID.randomUUID().toString();
		String projectId = "project";
		AutomationDataOwner owner = new AutomationDataOwner(projectId, runId, "user");
		AutomationDataReference taskReference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "task-reference", AutomationValueType.DATASET);
		Insight insight = new Insight();
		insight.setInsightId("automation-" + runId);
		insight.setProjectId(projectId);

		try {
			InsightStore.getInstance().put(insight);
			AutomationRunDataRegistry.register(insight, taskReference, owner,
					AutomationRunDataRegistry.Backing.TASK, "private-task-id");
			AutomationRunDataRegistry.registerRunMemoryReferences(insight, taskReference.toMap(), owner);

			AutomationRunDataRegistry.Entry entry = AutomationRunDataRegistry.require(insight, taskReference, owner);
			assertEquals(AutomationRunDataRegistry.Backing.TASK, entry.backing());
			assertEquals("private-task-id", entry.resourceId());
			assertFalse(taskReference.toMap().toString().contains("private-task-id"));
		} finally {
			InsightStore.getInstance().remove(insight.getInsightId());
		}
	}

	@Test
	void retainedDataPagesUseOneValidatedProviderNeutralShape() {
		Map<String, Object> raw = new LinkedHashMap<>();
		raw.put("available", true);
		raw.put("kind", "table");
		raw.put("valueType", "dataset");
		raw.put("offset", 0.0);
		raw.put("limit", 2.0);
		raw.put("count", 2.0);
		raw.put("total", 3.0);
		raw.put("hasMore", true);
		raw.put("headers", List.of("id", "value"));
		List<Object> firstRow = new java.util.ArrayList<>();
		firstRow.add(1.0);
		firstRow.add(null);
		raw.put("rows", List.of(firstRow, List.of(2.0, "two")));

		Map<String, Object> page = AutomationDataPage.fromValue(raw).toMap();

		assertEquals("table", page.get("kind"));
		assertEquals("dataset", page.get("valueType"));
		assertEquals(3L, page.get("total"));
		assertTrue((Boolean) page.get("hasMore"));
	}

	@Test
	void nestedDataReferencesAreRemovedFromClientValues() {
		AutomationDataReference reference = new AutomationDataReference(
				AutomationDataReference.CURRENT_SCHEMA_VERSION, "private-reference", AutomationValueType.FILE);
		Map<String, Object> value = Map.of("small", "visible", "nested", List.of(reference.toMap()));

		assertTrue(AutomationDataReference.containsReference(value));
		Object safe = AutomationDataReference.forClient(value);
		String serialized = prerna.reactor.automation.utils.AutomationRuntimeUtils.GSON.toJson(safe);
		assertTrue(serialized.contains("visible"));
		assertTrue(serialized.contains(AutomationDataReference.CLIENT_PREVIEW));
		assertFalse(serialized.contains("private-reference"));
	}
}
