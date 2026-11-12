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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

class AutomationRunVariablesUnitTests {

	@Test
	void flattensScalarsAndNestedObjectsToDottedNames() {
		Map<String, Object> variables = new LinkedHashMap<>();
		variables.put("trackingNumber", "4471");
		variables.put("count", 3);
		variables.put("item", Map.of("veteran", Map.of("number", "V-100")));

		Map<String, String> flattened = AutomationRunVariables.flatten(variables);

		assertEquals("4471", flattened.get("trackingNumber"));
		assertEquals("3", flattened.get("count"));
		assertEquals("V-100", flattened.get("item.veteran.number"));
	}

	@Test
	void skipsListsNullsAndBlankNamesInsteadOfIndexingPayloads() {
		Map<String, Object> variables = new LinkedHashMap<>();
		variables.put("rows", List.of(Map.of("a", "b")));
		variables.put("missing", null);
		variables.put(" ", "ignored");

		assertTrue(AutomationRunVariables.flatten(variables).isEmpty());
		assertTrue(AutomationRunVariables.flatten(null).isEmpty());
	}

	@Test
	void truncatesLongValuesAndCapsVariablesPerNode() {
		Map<String, Object> variables = new LinkedHashMap<>();
		variables.put("long", "x".repeat(AutomationConstants.VARIABLE_VALUE_MAX_LENGTH + 50));
		for (int i = 0; i < AutomationConstants.VARIABLES_PER_NODE_MAX + 10; i++) {
			variables.put("name" + i, "value");
		}

		Map<String, String> flattened = AutomationRunVariables.flatten(variables);

		assertEquals(AutomationConstants.VARIABLE_VALUE_MAX_LENGTH, flattened.get("long").length());
		assertEquals(AutomationConstants.VARIABLES_PER_NODE_MAX, flattened.size());
		assertFalse(flattened.containsKey("name" + (AutomationConstants.VARIABLES_PER_NODE_MAX + 5)));
	}

	@Test
	void stopsFlatteningBeyondTheMaximumDepth() {
		Map<String, Object> deep = Map.of("a", Map.of("b", Map.of("c", Map.of("d", Map.of("e", "too deep")))));

		assertTrue(AutomationRunVariables.flatten(deep).isEmpty());
	}
}
