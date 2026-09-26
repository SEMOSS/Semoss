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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.om.Insight;
import prerna.om.InsightStore;

/** Covers deterministic execution-service mappings and run Insight lookup. */
public class AutomationRunExecutionServiceUnitTests {

	private static final List<Map<String, Object>> NOUL_ROUTES = List.of(
			Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "approve",
					AutomationConstants.CONFIG_DESCRIPTION, "Continue automatically",
					AutomationConstants.CONFIG_ANSWER, true),
			Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "review",
					AutomationConstants.CONFIG_DESCRIPTION, "Send for review",
					AutomationConstants.CONFIG_ANSWER, false));

	private static TypeSafeModelEngineResponse response(Map<String, Object> routeAnswer) {
		return TypeSafeModelEngineResponse.fromObject(Map.of("model", "jev-latest",
				"usage", Map.of("input_tokens", 1, "output_tokens", 1), "answers",
				Map.of("route", routeAnswer)));
	}

	@Test
	void mapsNoulYesAndNoAnswersToExplicitStableRouteIds() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_QUESTION_TYPE,
				AutomationConstants.JEV_QUESTION_TYPE_NOUL,
				AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD, 0.7);

		Map<String, Object> yes = AutomationRunExecutionService.jevDecision(
				response(Map.of("type", "noul", "noul", 0.92)), NOUL_ROUTES, config);
		assertEquals("case:approve", yes.get("branch"));
		assertEquals(true, yes.get("answer"));

		Map<String, Object> no = AutomationRunExecutionService.jevDecision(
				response(Map.of("type", "noul", "noul", 0.08)), NOUL_ROUTES, config);
		assertEquals("case:review", no.get("branch"));
		assertEquals(false, no.get("answer"));
	}

	@Test
	void sendsAnUncertainNoulAnswerToFallback() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_QUESTION_TYPE,
				AutomationConstants.JEV_QUESTION_TYPE_NOUL,
				AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD, 0.8);
		Map<String, Object> decision = AutomationRunExecutionService.jevDecision(
				response(Map.of("type", "noul", "noul", 0.55)), NOUL_ROUTES, config);
		assertEquals(AutomationConstants.CONTROL_PORT_ELSE, decision.get("branch"));
		assertEquals(0.55, decision.get("confidence"));
	}

	@Test
	void preservesChoiceRoutingForExistingDefinitions() {
		List<Map<String, Object>> routes = List.of(
				Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "research",
						AutomationConstants.CONFIG_DESCRIPTION, "Research"));
		Map<String, Object> decision = AutomationRunExecutionService.jevDecision(
				response(Map.of("type", "choice", "choice", "research", "confidence", 0.9)), routes,
				Map.of(AutomationConstants.CONFIG_CONFIDENCE_THRESHOLD, 0.6));
		assertEquals("case:research", decision.get("branch"));
	}

	@Test
	void exposesRunInsightOnlyWhileItRemainsInTheInsightStore() {
		String runId = "run-insight-lifecycle-test";
		String insightId = "automation-run-" + runId;
		Insight insight = new Insight();
		insight.setInsightId(insightId);

		try {
			InsightStore.getInstance().put(insight);
			assertEquals(insightId, AutomationRunExecutionService.getAvailableExecutionInsightId(runId));
		} finally {
			InsightStore.getInstance().remove(insightId);
		}

		assertNull(AutomationRunExecutionService.getAvailableExecutionInsightId(runId));
	}

	@Test
	void resolvesLoopItemsWithoutCoercingNativeValues() {
		List<Object> items = List.of(Map.of("id", 1), Map.of("id", 2));
		assertEquals(items, AutomationRunExecutionService.loopItems("${records}", Map.of("records", items), "loop"));
		assertEquals(List.of("a", "b"),
				AutomationRunExecutionService.loopItems(new String[] { "a", "b" }, Map.of(), "loop"));
	}

	@Test
	void rejectsMissingOrNonCollectionLoopItems() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.loopItems("${missing}", Map.of(), "loop"));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.loopItems(42, Map.of(), "loop"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void groupsMaterializedLoopHistoryUnderItsParentNode() {
		Map<String, Object> parent = new LinkedHashMap<>();
		parent.put(AutomationConstants.RUN_ID, "run");
		parent.put(AutomationConstants.NODE_ID, "loop");
		parent.put(AutomationConstants.NODE_LABEL, "Loop");
		parent.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		Map<String, Object> child = new LinkedHashMap<>();
		child.put(AutomationConstants.RUN_ID, "run");
		child.put(AutomationConstants.NODE_ID, "execution-id");
		child.put(AutomationConstants.SOURCE_NODE_ID, "body-python");
		child.put(AutomationConstants.PARENT_NODE_ID, "loop");
		child.put(AutomationConstants.ITERATION_INDEX, 0);
		child.put(AutomationConstants.NODE_LABEL, "Python");
		child.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);

		List<Map<String, Object>> results = AutomationDatabaseUtility.buildNodeResults(List.of(parent, child));

		assertEquals(1, results.size());
		List<Map<String, Object>> iterations = (List<Map<String, Object>>) results.get(0).get("iterations");
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) iterations.get(0).get("nodeResults");
		assertEquals("body-python", nodes.get(0).get(AutomationConstants.NODE_ID));
	}
}
