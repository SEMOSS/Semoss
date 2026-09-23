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
package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class GeneratePlaywrightFieldActionsReactorTest {

	@Test
	void targetPromptRequestsOnlyClickedFieldAndIncludesCrossFieldContext() throws Exception {
		String prompt = GeneratePlaywrightFieldActionsReactor.buildPrompt("User: search for java",
				"https://example.com", "Example", List.of(field("Search", "fill", "input"), selectField()), 0);

		assertTrue(prompt.contains("Return exactly one entry for index 0"));
		assertTrue(prompt.contains("search for java"));
		assertTrue(
				prompt.contains("Other fields are context only") || prompt.contains("other fields are context only"));
		assertTrue(prompt.contains("Java"));
		assertTrue(prompt.contains("java"));
	}

	@Test
	void parserFiltersNonTargetAndPreservesTypedActionMetadata() throws Exception {
		List<Map<String, Object>> fields = List.of(field("Search", "fill", "input"), selectField());
		String response = "```json\n[{\"index\":0,\"value\":\"ignored\"},"
				+ "{\"index\":1,\"value\":\"java\"},{\"index\":1,\"value\":\"duplicate\"}]\n```";

		List<Map<String, Object>> parsed = GeneratePlaywrightFieldActionsReactor.parseFilledFields(response, fields, 1);

		assertEquals(1, parsed.size());
		assertEquals("java", parsed.get(0).get("value"));
		assertEquals("select", parsed.get(0).get("action"));
		assertEquals("select", parsed.get(0).get("tag"));
		assertEquals("css", parsed.get(0).get("selectorStrategy"));
		assertEquals("select[name=topic]", parsed.get(0).get("selectorValue"));
		assertEquals(false, parsed.get(0).get("isPassword"));
		assertEquals(true, parsed.get(0).get("storeValue"));
	}

	@Test
	void parserPreservesPasswordSensitivityAndDisablesValueStorage() throws Exception {
		Map<String, Object> password = field("Password", "fill", "input");
		password.put("isPassword", true);

		List<Map<String, Object>> parsed = GeneratePlaywrightFieldActionsReactor
				.parseFilledFields("[{\"index\":0,\"value\":\"secret\"}]", List.of(password), -1);

		assertEquals(true, parsed.get(0).get("isPassword"));
		assertEquals(false, parsed.get(0).get("storeValue"));
	}

	@Test
	void parserRejectsNonJsonAndOversizedValues() {
		List<Map<String, Object>> fields = List.of(field("Search", "fill", "input"));
		assertThrows(IllegalArgumentException.class,
				() -> GeneratePlaywrightFieldActionsReactor.parseFilledFields("not json", fields, -1));
		assertThrows(IllegalArgumentException.class, () -> GeneratePlaywrightFieldActionsReactor
				.parseFilledFields("[{\"index\":0,\"value\":\"" + "a".repeat(2_001) + "\"}]", fields, -1));
	}

	@Test
	void promptDoesNotExposeCurrentPasswordValue() throws Exception {
		Map<String, Object> password = field("Password", "fill", "input");
		password.put("isPassword", true);
		password.put("currentValue", "never-send-this");

		String prompt = GeneratePlaywrightFieldActionsReactor.buildPrompt("User: sign in", "https://example.com",
				"Example", List.of(password), 0);

		assertTrue(!prompt.contains("never-send-this"));
	}

	private static Map<String, Object> field(String label, String action, String tag) {
		Map<String, Object> field = new LinkedHashMap<>();
		field.put("label", label);
		field.put("context", "Page context");
		field.put("type", "text");
		field.put("currentValue", "");
		field.put("action", action);
		field.put("tag", tag);
		field.put("selector", Map.of("strategy", "css", "value", "input[name=search]"));
		return field;
	}

	private static Map<String, Object> selectField() {
		Map<String, Object> field = new LinkedHashMap<>();
		field.put("label", "Topic");
		field.put("context", "Choose a topic");
		field.put("type", "select");
		field.put("currentValue", "");
		field.put("action", "select");
		field.put("tag", "select");
		field.put("options", List.of(Map.of("label", "Java", "value", "java")));
		field.put("selector", Map.of("strategy", "css", "value", "select[name=topic]"));
		return field;
	}
}
