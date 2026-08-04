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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PlanPlaywrightAutomationReactorTest {

	@Test
	void promptIncludesGoalCurrentStateHistoryAndOnlyActionIndexes() throws Exception {
		String prompt = PlanPlaywrightAutomationReactor.buildPrompt("Search for Java", "User: use W3Schools",
				"https://example.com", "Example", Map.of("visibleText", "Search"),
				List.of(clickAction(), fieldAction("input", List.of())),
				List.of(Map.of("type", "click", "label", "Search")), 2, 10);

		assertTrue(prompt.contains("Search for Java"));
		assertTrue(prompt.contains("use W3Schools"));
		assertTrue(prompt.contains("PREVIOUS AUTOMATED ACTIONS"));
		assertTrue(prompt.contains("ITERATION: 2 of 10"));
		assertTrue(prompt.contains("AVAILABLE ACTIONS"));
		assertFalse(prompt.contains("button.search"));
	}

	@Test
	void parserMapsClickIndexToServerValidatedSelector() throws Exception {
		Map<String, Object> decision = PlanPlaywrightAutomationReactor.parseDecision(
				"```json\n{\"type\":\"click\",\"index\":0,\"reason\":\"open results\"}\n```",
				List.of(clickAction()));

		assertFalse((Boolean) decision.get("goalReached"));
		Map<?, ?> action = (Map<?, ?>) decision.get("action");
		assertEquals("click", action.get("type"));
		assertEquals(Map.of("strategy", "css", "value", "button.search"), action.get("selector"));
	}

	@Test
	void parserValidatesFieldActionAndSelectOption() throws Exception {
		Map<String, Object> select = fieldAction("select",
				List.of(Map.of("label", "Java", "value", "java")));
		Map<String, Object> decision = PlanPlaywrightAutomationReactor.parseDecision(
				"{\"type\":\"select\",\"index\":0,\"value\":\"java\"}", List.of(select));
		assertEquals("java", ((Map<?, ?>) decision.get("action")).get("value"));

		assertThrows(IllegalArgumentException.class,
				() -> PlanPlaywrightAutomationReactor.parseDecision(
						"{\"type\":\"select\",\"index\":0,\"value\":\"python\"}", List.of(select)));
		assertThrows(IllegalArgumentException.class,
				() -> PlanPlaywrightAutomationReactor.parseDecision(
						"{\"type\":\"click\",\"index\":0}", List.of(select)));
	}

	@Test
	void doneRequiresExplicitGoalReachedFlag() throws Exception {
		Map<String, Object> complete = PlanPlaywrightAutomationReactor.parseDecision(
				"{\"type\":\"done\",\"goalReached\":true,\"reason\":\"results are visible\"}", List.of());
		assertTrue((Boolean) complete.get("goalReached"));
		assertNull(complete.get("action"));

		Map<String, Object> blocked = PlanPlaywrightAutomationReactor.parseDecision(
				"{\"type\":\"done\",\"reason\":\"No useful action\"}", List.of());
		assertFalse((Boolean) blocked.get("goalReached"));
	}

	private static Map<String, Object> clickAction() {
		Map<String, Object> action = new LinkedHashMap<>();
		action.put("index", 0);
		action.put("kind", "click");
		action.put("label", "Search");
		action.put("tag", "button");
		action.put("selector", Map.of("strategy", "css", "value", "button.search"));
		action.put("coords", Map.of("x", 100, "y", 50));
		return action;
	}

	private static Map<String, Object> fieldAction(String tag, List<Map<String, String>> options) {
		Map<String, Object> action = new LinkedHashMap<>();
		action.put("index", 1);
		action.put("kind", "field");
		action.put("label", "Query");
		action.put("tag", tag);
		action.put("selector", Map.of("strategy", "css", "value", tag + ".query"));
		action.put("options", options);
		return action;
	}
}
