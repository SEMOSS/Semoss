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
package prerna.reactor.automation.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

/**
 * Covers the payload helpers every automation reactor shares. Reactor arguments
 * arrive either Base64 encoded or raw depending on the caller, and run values are
 * serialized straight into the run tables, so both bounds are enforced here.
 */
public class AutomationRuntimeUtilsUnitTests {

	private static String encode(String value) {
		return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
	}

	@Test
	void decodesBase64Payloads() {
		assertEquals("{\"a\":1}", AutomationRuntimeUtils.decodeBase64OrRaw(encode("{\"a\":1}")));
	}

	/** Callers that post raw JSON must survive the same decode path untouched. */
	@Test
	void passesThroughPayloadsThatAreNotBase64() {
		assertEquals("{\"a\": 1}", AutomationRuntimeUtils.decodeBase64OrRaw("{\"a\": 1}"));
		assertEquals("not base64 at all!", AutomationRuntimeUtils.decodeBase64OrRaw("not base64 at all!"));
	}

	@Test
	void parsesJsonObjectsFromEitherEncoding() {
		Map<String, Object> raw = AutomationRuntimeUtils.parseJsonObject("{\"name\":\"claims\"}", "inputs");
		Map<String, Object> encoded = AutomationRuntimeUtils.parseJsonObject(encode("{\"name\":\"claims\"}"), "inputs");
		assertEquals("claims", raw.get("name"));
		assertEquals("claims", encoded.get("name"));
	}

	@Test
	void namesTheOffendingFieldWhenThePayloadIsNotAnObject() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> AutomationRuntimeUtils.parseJsonObject("[1,2,3]", "inputs"));
		assertTrue(error.getMessage().contains("inputs"));
	}

	@Test
	void parsedObjectsAreMutable() {
		Map<String, Object> parsed = AutomationRuntimeUtils.parseJsonObject("{\"a\":1}", "inputs");
		parsed.put("b", 2);
		assertEquals(2, parsed.size());
	}

	@Test
	void boundedJsonRejectsOversizedValues() {
		Map<String, Object> value = Map.of("padding", "x".repeat(200));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRuntimeUtils.toBoundedRuntimeJson(value, 64, "Automation run scope"));
	}

	@Test
	void boundedJsonAcceptsValuesInsideTheLimit() {
		String json = AutomationRuntimeUtils.toBoundedRuntimeJson(Map.of("a", 1), 1024, "Automation run scope");
		assertEquals("{\"a\":1}", json);
	}

	/**
	 * Run values originate as JSON, where a self-reference has no meaning. Catching
	 * it here stops the serializer recursing until it runs out of stack.
	 */
	@Test
	void boundedJsonRejectsSelfReferencingValues() {
		Map<String, Object> cyclic = new LinkedHashMap<>();
		cyclic.put("self", cyclic);
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRuntimeUtils.toBoundedRuntimeJson(cyclic, 4096, "Automation run scope"));
	}

	@Test
	void boundedJsonRejectsValuesNestedTooDeeply() {
		List<Object> deepest = new ArrayList<>();
		Object current = deepest;
		for (int depth = 0; depth < AutomationConstants.RUNTIME_JSON_MAX_DEPTH + 2; depth++) {
			List<Object> parent = new ArrayList<>();
			parent.add(current);
			current = parent;
		}
		Object value = current;
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRuntimeUtils.toBoundedRuntimeJson(value, 1_000_000, "Automation run scope"));
	}

	@Test
	void previewsAreBoundedAndNullSafe() {
		assertNull(AutomationRuntimeUtils.generatePreview(null));
		assertEquals("short", AutomationRuntimeUtils.generatePreview("short"));
		String oversized = "y".repeat(AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH + 50);
		assertEquals(AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH,
				AutomationRuntimeUtils.generatePreview(oversized).length());
	}
}
