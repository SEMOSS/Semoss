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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.om.HeadersDataRow;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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
	void readOnlyDirectOperationsDoNotRequireEffectRecords() {
		assertFalse(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_DATABASE_QUERY));
		assertFalse(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_STORAGE_LIST));
		assertFalse(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_STORAGE_READ));
		assertFalse(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_STORAGE_DOWNLOAD));
		assertFalse(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_VECTOR_SEARCH));
		assertTrue(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_DATABASE_INSERT));
		assertTrue(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_STORAGE_UPLOAD));
		assertTrue(AutomationRunExecutionService.requiresEffectRecord(AutomationConstants.NODE_MODEL_CHAT));
	}

	@Test
	void everyDurableItemAttemptUsesANewInsightIdentity() {
		AutomationRunStore.ItemClaim first = new AutomationRunStore.ItemClaim("item", "run", "loop", 0, "[]",
				"{}", "attempt-1", 1, 3, "lease-1");
		AutomationRunStore.ItemClaim retry = new AutomationRunStore.ItemClaim("item", "run", "loop", 0, "[]",
				"{}", "attempt-2", 2, 3, "lease-2");

		assertEquals("automation-item-attempt-1", AutomationRunExecutionService.itemExecutionInsightId(first));
		assertEquals("automation-item-attempt-2", AutomationRunExecutionService.itemExecutionInsightId(retry));
		assertFalse(AutomationRunExecutionService.itemExecutionInsightId(first)
				.equals(AutomationRunExecutionService.itemExecutionInsightId(retry)));
	}

	@Test
	void durableItemSourceValidationAcceptsTypedBodyWithoutSource() {
		Map<String, Object> typed = Map.of(
				AutomationConstants.NODE_FIELD_ID, "typed",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_CONTROL_WAIT,
				AutomationConstants.NODE_FIELD_CONFIG, Map.of("durationSeconds", 0));
		Map<String, Object> python = Map.of(
				AutomationConstants.NODE_FIELD_ID, "python",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DEVELOPER_PYTHON,
				AutomationConstants.NODE_FIELD_CONFIG, Map.of());
		var definition = new AutomationDefinitionValidator.ValidatedDefinition(
				Map.of(), List.of(typed, python), List.of(), "{}", "hash");
		AutomationExecutionContract contract = AutomationExecutionContract.forSubmission(
				definition, Map.of("python", "def run(scope):\n    return None\n"));

		AutomationRunExecutionService.requireDurableItemSources(List.of(typed), contract, Map.of());
		assertThrows(IllegalStateException.class, () ->
				AutomationRunExecutionService.requireDurableItemSources(List.of(python), contract, Map.of()));
	}

	private static TypeSafeModelEngineResponse mixedResponse(double urgentProbability) {
		return TypeSafeModelEngineResponse.fromObject(Map.of("model", "jev-1.13.0",
				"usage", Map.of("input_tokens", 10, "output_tokens", 4), "answers",
				Map.of("department",
						Map.of("type", "choice", "choice", "billing", "confidence", 0.95, "probabilities",
								Map.of("billing", 0.95, "technical", 0.05)),
						"frustration", Map.of("type", "score", "score", 0.7, "confidence", 0.9),
						"urgent", Map.of("type", "noul", "noul", urgentProbability))));
	}

	private static Map<String, Object> mixedConfig() {
		return Map.of(AutomationConstants.CONFIG_QUESTIONS,
				List.of(Map.of(AutomationConstants.CONFIG_KEY, "department"),
						Map.of(AutomationConstants.CONFIG_KEY, "frustration"),
						Map.of(AutomationConstants.CONFIG_KEY, "urgent")));
	}

	private static List<Map<String, Object>> mixedRoutes() {
		return List.of(Map.of(AutomationConstants.CONFIG_CLAUSE_ID, "urgent_billing",
				AutomationConstants.CONFIG_DESCRIPTION, "Urgent billing",
				AutomationConstants.CONFIG_MATCH, AutomationConstants.JEV_ROUTE_MATCH_ALL,
				AutomationConstants.CONFIG_CONDITIONS,
				List.of(
						Map.of(AutomationConstants.CONFIG_QUESTION_KEY, "department",
								AutomationConstants.CONFIG_FIELD, AutomationConstants.JEV_FIELD_CHOICE,
								AutomationConstants.CONFIG_OPERATOR, AutomationConstants.JEV_OPERATOR_EQUALS,
								AutomationConstants.CONFIG_VALUE, "billing"),
						Map.of(AutomationConstants.CONFIG_QUESTION_KEY, "urgent",
								AutomationConstants.CONFIG_FIELD, AutomationConstants.JEV_FIELD_NOUL,
								AutomationConstants.CONFIG_OPERATOR,
								AutomationConstants.JEV_OPERATOR_GREATER_THAN_OR_EQUAL,
								AutomationConstants.CONFIG_VALUE, 0.8))));
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
	void combinesMultipleTypedAnswersIntoOneJevRoute() {
		Map<String, Object> decision = AutomationRunExecutionService.jevDecision(mixedResponse(0.89), mixedRoutes(),
				mixedConfig());
		assertEquals("case:urgent_billing", decision.get("branch"));
		assertEquals("Urgent billing", decision.get("route"));
		assertEquals(AutomationConstants.JEV_ROUTE_MATCH_ALL, decision.get("routeMatch"));
		assertEquals(2, ((List<?>) decision.get("matchedConditions")).size());
		assertEquals("jev-1.13.0", decision.get("model"));
		assertEquals(3, ((Map<?, ?>) decision.get("answers")).size());
	}

	@Test
	void buildsTheNativeMultiQuestionJevRequestWithoutFlatteningCriteria() {
		Map<String, Object> config = Map.of(AutomationConstants.CONFIG_QUESTIONS,
				List.of(
						Map.of(AutomationConstants.CONFIG_KEY, "department",
								AutomationConstants.CONFIG_TYPE, AutomationConstants.JEV_QUESTION_TYPE_CHOICE,
								AutomationConstants.CONFIG_INSTRUCTIONS, "Choose a team",
								AutomationConstants.CONFIG_CRITERIA,
								Map.of("billing", "Payments", "technical", "Bugs")),
						Map.of(AutomationConstants.CONFIG_KEY, "urgent",
								AutomationConstants.CONFIG_TYPE, AutomationConstants.JEV_QUESTION_TYPE_NOUL,
								AutomationConstants.CONFIG_INSTRUCTIONS, "${instruction}",
								AutomationConstants.CONFIG_CRITERIA,
								Map.of("true", "Today", "false", "Later"))));
		Map<String, Object> questions = AutomationRunExecutionService.jevQuestions(config,
				Map.of("instruction", "Does this require action today?"));
		assertEquals(2, questions.size());
		assertEquals("Does this require action today?",
				((Map<?, ?>) questions.get("urgent")).get(AutomationConstants.CONFIG_INSTRUCTIONS));
		assertEquals(Map.of("billing", "Payments", "technical", "Bugs"),
				((Map<?, ?>) questions.get("department")).get(AutomationConstants.CONFIG_CRITERIA));
	}

	@Test
	void preservesIntegerRetryOptionsAfterJsonDeserialization() {
		Map<String, Object> parameters = AutomationRunExecutionService.jevParameters(
				Map.of(AutomationConstants.CONFIG_PARAM_VALUES, Map.of("timeout", 30.0, "max_retries", 2.0)));
		assertEquals(30.0, parameters.get("timeout"));
		assertEquals(2, parameters.get("max_retries"));
		assertEquals(Integer.class, parameters.get("max_retries").getClass());
	}

	@Test
	void usesFallbackWhenNoMultiQuestionJevRouteMatches() {
		Map<String, Object> decision = AutomationRunExecutionService.jevDecision(mixedResponse(0.6), mixedRoutes(),
				mixedConfig());
		assertEquals(AutomationConstants.CONTROL_PORT_ELSE, decision.get("branch"));
		assertNull(decision.get("routeId"));
	}

	@Test
	void exposesRunInsightOnlyWhileItRemainsInTheInsightStore() {
		String runId = UUID.randomUUID().toString();
		String insightId = "automation-" + runId;
		assertEquals(47, insightId.length());
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
		assertEquals(items, AutomationRunExecutionService.loopItems("${query.data.rows}",
				Map.of("query", Map.of("data", Map.of("rows", items))), "loop"));
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
	void reconstructsJavaTableScopeFromRetainedChunksWithoutALiveFrame() {
		String runId = "run-table";
		Map<String, Object> output = new LinkedHashMap<>();
		output.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		output.put(AutomationConstants.NODE_ID, "query");
		output.put(AutomationConstants.OUTPUT_VAR_NAME, "query_result");
		output.put(AutomationConstants.OUTPUT_KIND, AutomationConstants.OUTPUT_KIND_TABLE);
		output.put(AutomationConstants.OUTPUT_VALUE,
				"{\"dataType\":\"table\",\"rowCount\":1,\"columnCount\":2}");
		Map<String, Object> reference = new LinkedHashMap<>();
		reference.put(AutomationConstants.RESULT_ID, "result-1");
		reference.put(AutomationConstants.NODE_ID, "query");
		reference.put(AutomationConstants.RESULT_KIND, AutomationConstants.RESULT_KIND_TABLE);
		reference.put(AutomationConstants.PARENT_NODE_ID, null);

		Insight insight = new Insight();
		insight.setInsightId("automation-" + runId);
		try (var store = mockStatic(AutomationRunStore.class, CALLS_REAL_METHODS)) {
			store.when(() -> AutomationRunStore.getRunInputs(runId)).thenReturn(Map.of());
			store.when(() -> AutomationRunStore.getNodeOutputsForRun(runId)).thenReturn(List.of(output));
			store.when(() -> AutomationRunStore.getSavedResultReferencesForRun(runId))
					.thenReturn(List.of(reference));
			store.when(() -> AutomationRunStore.getSavedResult("result-1")).thenReturn(Map.of(
					AutomationConstants.PAYLOAD_VALUE,
					"[{\"name\":\"ID\",\"type\":\"INT\"},{\"name\":\"STATUS\",\"type\":\"STRING\"}]"));
			store.when(() -> AutomationRunStore.getSavedResultChunkMetadata("result-1"))
					.thenReturn(List.of(Map.of(AutomationConstants.CHUNK_INDEX, 0)));
			store.when(() -> AutomationRunStore.getSavedResultChunk("result-1", 0))
					.thenReturn(Map.of(AutomationConstants.CHUNK_VALUE, "[[7,\"ready\"]]"));

			AutomationRunExecutionService.ReconstructedScope reconstructed =
					AutomationRunExecutionService.reconstructScope(runId, insight, "trigger");

			AutomationTableValue table =
					(AutomationTableValue) reconstructed.scope().get("query_result");
			assertEquals(List.of(7.0, "ready"), table.rows().get(0));
		}
	}

	@Test
	void serviceOwnedExecutionInsightCleanupClosesFramesAndClearsTheWorkspace() {
		String insightId = "automation-run-cleanup";
		ITableDataFrame frame = mock(ITableDataFrame.class);
		Insight insight = new Insight();
		insight.setInsightId(insightId);
		insight.setDeletePythonGlobalsOnDropInsight(false);
		insight.getVarStore().put("query_result", new NounMetadata(frame, PixelDataType.FRAME));
		InsightStore.getInstance().put(insight);

		AutomationRunExecutionService.releaseExecutionInsight(insight, true);

		assertFalse(InsightStore.getInstance().containsKey(insightId));
		assertNull(insight.getVarStore().get("query_result"));
		verify(frame, atLeastOnce()).close();
	}

	@Test
	void sessionOwnedExecutionInsightRemainsUnderSessionLifecycle() {
		String insightId = "automation-run-session";
		Insight insight = new Insight();
		insight.setInsightId(insightId);
		insight.getVarStore().put("value", new NounMetadata("retained", PixelDataType.CONST_STRING));
		InsightStore.getInstance().put(insight);

		try {
			AutomationRunExecutionService.releaseExecutionInsight(insight, false);
			assertTrue(InsightStore.getInstance().containsKey(insightId));
			assertEquals("retained", insight.getVarStore().get("value").getValue());
		} finally {
			InsightStore.getInstance().remove(insightId);
		}
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

		List<Map<String, Object>> results = AutomationRunStore.buildNodeResults(List.of(parent, child));

		assertEquals(1, results.size());
		List<Map<String, Object>> iterations = (List<Map<String, Object>>) results.get(0).get("iterations");
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) iterations.get(0).get("nodeResults");
		assertEquals("body-python", nodes.get(0).get(AutomationConstants.NODE_ID));
	}
}
