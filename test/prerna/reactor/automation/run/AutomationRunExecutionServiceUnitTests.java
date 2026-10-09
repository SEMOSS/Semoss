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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;
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
	void readsLoopRowsThroughTheSemossFrameContract() throws Exception {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow first = mock(IHeadersDataRow.class);
		IHeadersDataRow second = mock(IHeadersDataRow.class);
		when(frame.getQsHeaders()).thenReturn(new String[] { "HERO__NAME", "HERO__POWER" });
		when(frame.query(any(SelectQueryStruct.class))).thenReturn(wrapper);
		when(wrapper.hasNext()).thenReturn(true, true, false);
		when(wrapper.next()).thenReturn(first, second);
		when(first.getHeaders()).thenReturn(new String[] { "NAME", "POWER" });
		when(first.getValues()).thenReturn(new Object[] { "Storm", "Weather control" });
		when(second.getHeaders()).thenReturn(new String[] { "NAME", "POWER" });
		when(second.getValues()).thenReturn(new Object[] { "Flash", "Super speed" });

		Insight insight = new Insight();
		insight.getVarStore().put("heroes", new NounMetadata(frame, PixelDataType.FRAME));

		assertEquals(List.of(Map.of("NAME", "Storm", "POWER", "Weather control"),
				Map.of("NAME", "Flash", "POWER", "Super speed")),
				AutomationRunExecutionService.frameRows(insight, "heroes", "loop", 2));
	}

	@Test
	void rejectsFrameLoopInputBeyondTheConfiguredBound() throws Exception {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow row = mock(IHeadersDataRow.class);
		when(frame.getQsHeaders()).thenReturn(new String[] { "HERO__NAME" });
		when(frame.query(any(SelectQueryStruct.class))).thenReturn(wrapper);
		when(wrapper.hasNext()).thenReturn(true, true);
		when(wrapper.next()).thenReturn(row);
		when(row.getHeaders()).thenReturn(new String[] { "NAME" });
		when(row.getValues()).thenReturn(new Object[] { "Storm" });

		Insight insight = new Insight();
		insight.getVarStore().put("heroes", new NounMetadata(frame, PixelDataType.FRAME));

		assertThrows(IllegalArgumentException.class,
				() -> AutomationRunExecutionService.frameRows(insight, "heroes", "loop", 1));
	}

	@Test
	void resumeRequiresTheOriginalLiveFrameBinding() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		insight.getVarStore().put("heroes", new NounMetadata(frame, PixelDataType.FRAME));

		assertEquals(DataFrameTypeEnum.PYTHON.getTypeAsString(),
				AutomationRunExecutionService.requireResumableFrameBackend(insight, "run-1", "heroes"));
		IllegalStateException unavailable = assertThrows(IllegalStateException.class,
				() -> AutomationRunExecutionService.requireResumableFrameBackend(insight, "run-1", "missing"));
		assertTrue(unavailable.getMessage().contains("cannot resume"));
	}

	@Test
	void lostExecutionInsightDoesNotReplayTheSuccessfulDatabaseNode() {
		String runId = "run-1";
		String insightId = "automation-" + runId;
		Map<String, Object> frameOutput = new LinkedHashMap<>();
		frameOutput.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		frameOutput.put(AutomationConstants.NODE_ID, "query");
		frameOutput.put(AutomationConstants.OUTPUT_VAR_NAME, "query_result");
		frameOutput.put(AutomationConstants.OUTPUT_KIND, AutomationConstants.OUTPUT_KIND_FRAME);
		frameOutput.put(AutomationConstants.OUTPUT_VALUE,
				"{\"dataType\":\"table\",\"rowCount\":10,\"columnCount\":2}");
		Insight originalInsight = new Insight();
		originalInsight.setInsightId(insightId);
		InsightStore.getInstance().put(originalInsight);
		assertEquals(insightId, AutomationRunExecutionService.getAvailableExecutionInsightId(runId));
		InsightStore.getInstance().remove(insightId);
		assertNull(AutomationRunExecutionService.getAvailableExecutionInsightId(runId));

		Insight replacementInsight = new Insight();
		replacementInsight.setInsightId(insightId);

		try (var store = mockStatic(AutomationRunStore.class, CALLS_REAL_METHODS);
				var database = mockStatic(AutomationDatabaseQueryExecutor.class)) {
			store.when(() -> AutomationRunStore.getRunInputs(runId)).thenReturn(Map.of());
			store.when(() -> AutomationRunStore.getNodeOutputsForRun(runId)).thenReturn(List.of(frameOutput));

			IllegalStateException unavailable = assertThrows(IllegalStateException.class,
					() -> AutomationRunExecutionService.reconstructScope(runId, replacementInsight, "trigger"));

			assertTrue(unavailable.getMessage().contains("cannot resume"));
			database.verifyNoInteractions();
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
