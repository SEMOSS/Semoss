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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class AutomationScopeResolverUnitTests {

	@Test
	void resolvesNestedMapsListsArraysAndNullWithoutCoercion() {
		Map<String, Object> scope = new LinkedHashMap<>();
		scope.put("result", Map.of("rows", List.of(Map.of("id", 17))));
		scope.put("array", new String[] { "first", "second" });
		scope.put("nullable", null);

		assertEquals(17, AutomationScopeResolver.resolveRequired("result.rows.0.id", scope, "Test value"));
		assertEquals("second", AutomationScopeResolver.resolveRequired("array.1", scope, "Test value"));
		assertNull(AutomationScopeResolver.resolveRequired("nullable", scope, "Test value"));
	}

	@Test
	void distinguishesExactReferencesFromEmbeddedAndLiteralValues() {
		Map<String, Object> scope = Map.of("name", "Ada");

		assertEquals("Ada", AutomationScopeResolver.resolveExactOrLiteral(" ${name} ", scope, "Test value"));
		assertEquals("Hello ${name}",
				AutomationScopeResolver.resolveExactOrLiteral("Hello ${name}", scope, "Test value"));
		assertEquals(42, AutomationScopeResolver.resolveExactOrLiteral(42, scope, "Test value"));
	}

	@Test
	void rejectsMissingMalformedAndOutOfRangePaths() {
		Map<String, Object> scope = Map.of("rows", List.of("one"));

		assertThrows(IllegalArgumentException.class,
				() -> AutomationScopeResolver.resolveRequired("missing", scope, "Test value"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationScopeResolver.resolveRequired("rows.2", scope, "Test value"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationScopeResolver.resolveRequired("rows..0", scope, "Test value"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationScopeResolver.resolveRequired("rows.999999999999999999999", scope, "Test value"));
	}

	@Test
	void recursivelyResolvesTypedConfiguration() {
		Map<String, Object> configured = Map.of(
				"text", "${payload.text}",
				"items", List.of("${payload.count}", "literal", Map.of("nullValue", "${payload.nullValue}")));
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("text", "quotes \" and ; unicode \u03bb");
		payload.put("count", 3);
		payload.put("nullValue", null);

		@SuppressWarnings("unchecked")
		Map<String, Object> resolved = (Map<String, Object>) AutomationScopeResolver.resolveConfig(configured,
				Map.of("payload", payload), "arguments");

		assertEquals("quotes \" and ; unicode \u03bb", resolved.get("text"));
		assertEquals(3, ((List<?>) resolved.get("items")).get(0));
		assertNull(((Map<?, ?>) ((List<?>) resolved.get("items")).get(2)).get("nullValue"));
	}
}
