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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

class AutomationDataTransformExecutorUnitTests {

	@Test
	void transformBuildsFreshObjectFromScratchWithoutMergingPriorScope() {
		Map<String, Object> scope = Map.of("name", "Ada", "priorStepLeftoverKey", "should-not-appear");
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_FIELDS,
				List.of(field("greeting", valueNode("${name}")), field("enabled", booleanNode(true)),
						field("note", nullNode())));

		Map<String, Object> result = AutomationDataTransformExecutor.transform(config, scope, "transform-1");

		assertEquals(Map.of("greeting", "Ada", "enabled", true), Map.of("greeting", result.get("greeting"), "enabled",
				result.get("enabled")));
		assertTrue(result.containsKey("note"));
		assertNull(result.get("note"));
		assertFalse(result.containsKey("priorStepLeftoverKey"));
	}

	@Test
	void transformRecursesNestedObjectsAndArrays() {
		Map<String, Object> scope = Map.of("id", 7);
		Map<String, Object> nestedObject = Map.of("kind", "object", "fields",
				List.of(field("id", valueNode("${id}"))));
		Map<String, Object> arrayNode = Map.of("kind", "array", "items",
				List.of(valueNode("one"), valueNode("two")));
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_FIELDS,
				List.of(field("nested", nestedObject), field("items", arrayNode)));

		Map<String, Object> result = AutomationDataTransformExecutor.transform(config, scope, "transform-1");

		assertEquals(Map.of("id", 7), result.get("nested"));
		assertEquals(List.of("one", "two"), result.get("items"));
	}

	@Test
	void channelVariablesExtractsOnlyConfiguredFieldsAndNullsMissingOnes() {
		Map<String, Object> scope = Map.of("priorStep",
				Map.of("truth", "42", "extra", "ignored"));
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_SOURCE, "${priorStep}",
				AutomationConstants.CONFIG_FIELDS, List.of("truth", "missingField"));

		Map<String, Object> result = AutomationDataTransformExecutor.channelVariables(config, scope, "channel-1");

		assertEquals("42", result.get("truth"));
		assertTrue(result.containsKey("missingField"));
		assertNull(result.get("missingField"));
		assertFalse(result.containsKey("extra"));
	}

	@Test
	void channelVariablesRejectsANonObjectSource() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_SOURCE, "not-an-object",
				AutomationConstants.CONFIG_FIELDS, List.of("truth"));

		assertThrows(IllegalArgumentException.class,
				() -> AutomationDataTransformExecutor.channelVariables(config, Map.of(), "channel-1"));
	}

	private static Map<String, Object> field(String name, Map<String, Object> node) {
		Map<String, Object> field = new LinkedHashMap<>();
		field.put("name", name);
		field.put("node", node);
		return field;
	}

	private static Map<String, Object> valueNode(Object value) {
		Map<String, Object> node = new LinkedHashMap<>();
		node.put("kind", "value");
		node.put(AutomationConstants.CONFIG_VALUE, value);
		return node;
	}

	private static Map<String, Object> booleanNode(boolean value) {
		return Map.of("kind", "boolean", "boolValue", value);
	}

	private static Map<String, Object> nullNode() {
		return Map.of("kind", "null");
	}
}
