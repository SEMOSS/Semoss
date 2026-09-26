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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Covers the node catalog the canvas builds its palette from. The catalog is the
 * only description of a node the client ever sees, so a node missing from it is
 * unreachable in the UI even though the runtime can execute it.
 */
public class AutomationNodeCatalogUnitTests {

	private static AutomationNodeDefinition find(String type) {
		for (AutomationNodeDefinition definition : AutomationNodeCatalog.getDefinitions()) {
			if (definition.toMap().get("type").equals(type)) {
				return definition;
			}
		}
		return null;
	}

	/** A node type with no catalog entry can never be added from the canvas. */
	@Test
	void everyNodeTypeIsInThePalette() {
		for (AutomationNodeType type : AutomationNodeType.values()) {
			assertNotNull(find(type.getType()), type.getType() + " is missing a catalog definition");
		}
	}

	@Test
	void nodeTypesAreUnique() {
		Set<Object> seen = new HashSet<>();
		for (AutomationNodeDefinition definition : AutomationNodeCatalog.getDefinitions()) {
			assertTrue(seen.add(definition.toMap().get("type")), "duplicate catalog entry");
		}
	}

	@Test
	void everyDatabaseOperationIsOffered() {
		for (String type : List.of(AutomationConstants.NODE_DATABASE_QUERY, AutomationConstants.NODE_DATABASE_INSERT,
				AutomationConstants.NODE_DATABASE_UPDATE, AutomationConstants.NODE_DATABASE_DELETE)) {
			assertEquals("database", find(type).toMap().get("category"), type + " belongs to the database category");
		}
	}

	@Test
	void storageDownloadAdvertisesItsStructuredResult() {
		AutomationNodeDefinition definition = find(AutomationConstants.NODE_STORAGE_DOWNLOAD);
		assertNotNull(definition);
		Map<String, Map<String, Object>> fieldsByKey = definition.outputFields().stream()
				.collect(java.util.stream.Collectors.toMap(AutomationNodeDefinition.OutputField::key,
						AutomationNodeDefinition.OutputField::toMap));
		assertEquals("string[]", fieldsByKey.get("files").get("type"));
		assertEquals(true, fieldsByKey.get("files").get("required"));
		assertEquals("string", fieldsByKey.get("filePath").get("type"));
		assertEquals(false, fieldsByKey.get("filePath").get("required"));
	}

	@Test
	void jevDecisionAdvertisesTypeSafeRoutingConfiguration() {
		AutomationNodeDefinition definition = find(AutomationConstants.NODE_CONTROL_JEV);
		assertNotNull(definition);
		assertEquals(AutomationNodeType.Permission.VIEW, definition.nodeType().getPermission());
		Map<String, Map<String, Object>> fieldsByKey = definition.configFields().stream()
				.collect(java.util.stream.Collectors.toMap(AutomationNodeDefinition.ConfigField::key,
						AutomationNodeDefinition.ConfigField::toMap));
		Map<String, Object> confidence = fieldsByKey.get("confidenceThreshold");
		assertEquals(0.0, confidence.get("minimum"));
		assertEquals(1.0, confidence.get("maximum"));
		assertEquals(AutomationConstants.JEV_QUESTION_TYPE_CHOICE,
				fieldsByKey.get(AutomationConstants.CONFIG_QUESTION_TYPE).get("defaultValue"));
		assertEquals(AutomationConstants.JEV_QUESTION_TYPE_CHOICE,
				definition.defaultConfig().get(AutomationConstants.CONFIG_QUESTION_TYPE));
	}

	@Test
	void loopAdvertisesBoundedSequentialConfiguration() {
		AutomationNodeDefinition definition = find(AutomationConstants.NODE_CONTROL_LOOP);
		assertNotNull(definition);
		assertEquals(AutomationConstants.LOOP_MODE_FOR_EACH,
				definition.defaultConfig().get(AutomationConstants.CONFIG_LOOP_MODE));
		Map<String, Map<String, Object>> fieldsByKey = definition.configFields().stream()
				.collect(java.util.stream.Collectors.toMap(AutomationNodeDefinition.ConfigField::key,
						AutomationNodeDefinition.ConfigField::toMap));
		assertEquals(AutomationConstants.LOOP_MAX_ITERATIONS,
				fieldsByKey.get(AutomationConstants.CONFIG_LOOP_MAX_ITERATIONS).get("maximum"));
		assertEquals(1, fieldsByKey.get(AutomationConstants.CONFIG_LOOP_BATCH_SIZE).get("minimum"));
	}

	/** Writes need edit rights on the engine; a read only needs view. */
	@Test
	void writeNodesRequireEditPermission() {
		assertEquals(AutomationNodeType.Permission.VIEW, AutomationNodeType.DATABASE_QUERY.getPermission());
		for (AutomationNodeType type : List.of(AutomationNodeType.DATABASE_INSERT, AutomationNodeType.DATABASE_UPDATE,
				AutomationNodeType.DATABASE_DELETE)) {
			assertEquals(AutomationNodeType.Permission.EDIT, type.getPermission(),
					type.getType() + " must require edit access");
		}
	}

	@Test
	void resolvesEveryTypeBackToItsEnum() {
		for (AutomationNodeType type : AutomationNodeType.values()) {
			assertEquals(type, AutomationNodeType.fromType(type.getType()));
			assertTrue(AutomationNodeType.isSupported(type.getType()));
		}
	}
}
