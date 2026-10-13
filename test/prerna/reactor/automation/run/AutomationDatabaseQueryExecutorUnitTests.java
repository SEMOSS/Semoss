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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.reactor.qs.SqlQueryReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class AutomationDatabaseQueryExecutorUnitTests {

	@Test
	void supportsOnlyGeneratedDatabaseReads() {
		Map<String, Object> generated = new LinkedHashMap<>();
		generated.put(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_QUERY);
		assertTrue(AutomationDatabaseQueryExecutor.supports(generated));

		Map<String, Object> custom = new LinkedHashMap<>(generated);
		custom.put(AutomationConstants.NODE_FIELD_CODE_MODE, AutomationConstants.NODE_CODE_MODE_CUSTOM);
		assertFalse(AutomationDatabaseQueryExecutor.supports(custom));
		assertFalse(AutomationDatabaseQueryExecutor.supports(
				Map.of(AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_INSERT)));
	}

	@Test
	void parsesTheResolvedInternalRequest() {
		Map<String, Object> request = Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
						AutomationConstants.CONFIG_LIMIT, AutomationConstants.DB_QUERY_MAX_LIMIT));

		assertTrue(AutomationDatabaseQueryExecutor.isRequest(request));
		AutomationDatabaseQueryExecutor.QueryRequest parsed = AutomationDatabaseQueryExecutor.parseRequest(request);
		assertEquals("engine-1", parsed.databaseId());
		assertEquals("SELECT 1", parsed.query());
		assertEquals(50_000, parsed.limit());
		assertEquals(List.of(), parsed.parameters());
	}

	@Test
	void buildsTypedRequestWithoutPython() {
		Map<String, Object> scope = new LinkedHashMap<>();
		scope.put("database", "engine-1");
		scope.put("sql", "SELECT * FROM T WHERE V=? AND N=? AND M IS ?");
		scope.put("unsafe", "'; DROP TABLE T; --\u03bb");
		scope.put("number", 42);
		scope.put("missing", null);
		scope.put("limit", 25);
		Map<String, Object> request = AutomationDatabaseQueryExecutor.request(
				Map.of("engineId", "${database}", "query", "${sql}", "parameters",
						List.of("${unsafe}", "${number}", "${missing}"), "limit", "${limit}"),
				scope, "query");

		AutomationDatabaseQueryExecutor.QueryRequest parsed = AutomationDatabaseQueryExecutor.parseRequest(request);
		assertEquals("engine-1", parsed.databaseId());
		assertEquals("SELECT * FROM T WHERE V=? AND N=? AND M IS ?", parsed.query());
		assertEquals(java.util.Arrays.asList("'; DROP TABLE T; --\u03bb", 42, null), parsed.parameters());
		assertEquals(25, parsed.limit());
	}

	@Test
	void profilesTypedQuerySubmissionOverhead() {
		Map<String, Object> queryNode = Map.of(
				AutomationConstants.NODE_FIELD_ID, "query",
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_DATABASE_QUERY,
				AutomationConstants.NODE_FIELD_CONFIG,
				Map.of("engineId", "engine-1", "query", "SELECT 1", "limit", 50));
		AutomationDefinitionValidator.ValidatedDefinition definition =
				new AutomationDefinitionValidator.ValidatedDefinition(
						Map.of(), List.of(queryNode), List.of(), "{}", "hash");
		int iterations = 1_000;

		long contractStarted = System.nanoTime();
		for (int i = 0; i < iterations; i++) {
			AutomationExecutionContract.forSubmission(definition, Map.of());
		}
		long contractNanos = System.nanoTime() - contractStarted;

		long requestStarted = System.nanoTime();
		for (int i = 0; i < iterations; i++) {
			AutomationDatabaseQueryExecutor.request(
					Map.of("engineId", "engine-1", "query", "SELECT 1", "limit", 50),
					Map.of(), "query");
		}
		long requestNanos = System.nanoTime() - requestStarted;

		System.out.printf("AUTOMATION_SIMPLE_QUERY_TIMING iterations=%d "
						+ "typedContractMs=%.3f typedRequestMs=%.3f%n",
				iterations, contractNanos / 1_000_000.0,
				requestNanos / 1_000_000.0);
		assertTrue(contractNanos > 0 && requestNanos > 0);
	}

	@Test
	void rejectsMalformedOrUnboundedRequests() {
		Map<String, Object> missingQuery = Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", AutomationConstants.CONFIG_LIMIT, 50));
		assertThrows(IllegalStateException.class,
				() -> AutomationDatabaseQueryExecutor.parseRequest(missingQuery));

		Map<String, Object> excessiveLimit = Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
						AutomationConstants.CONFIG_LIMIT, AutomationConstants.DB_QUERY_MAX_LIMIT + 1));
		assertThrows(IllegalStateException.class,
				() -> AutomationDatabaseQueryExecutor.parseRequest(excessiveLimit));
	}

	@Test
	void consumesTheAuthorizedSqlTaskIntoAJavaTable() throws Exception {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		BasicIteratorTask task = mock(BasicIteratorTask.class);
		when(task.getHeaderInfo()).thenReturn(List.of(
				Map.of("alias", "ID", "dataType", "INT"),
				Map.of("alias", "STATUS", "dataType", "STRING")));
		when(task.hasNext()).thenReturn(true, true, false);
		when(task.next()).thenReturn(
				new prerna.om.HeadersDataRow(new String[] { "ID", "STATUS" }, new Object[] { 1, "ready" }),
				new prerna.om.HeadersDataRow(new String[] { "ID", "STATUS" }, new Object[] { 2, null }));

		try (MockedConstruction<SqlQueryReactor> reactors = sqlQueryReactors(task)) {
			AutomationTableValue output =
					AutomationDatabaseQueryExecutor.execute(insight, request(), "run-1", "node-1");

			assertEquals(Map.of("dataType", "table", "rowCount", 2L, "columnCount", 2), output.summary());
			assertEquals(List.of(1, "ready"), output.rows().get(0));
			assertEquals(java.util.Arrays.asList(2, null), output.rows().get(1));
			assertNull(insight.getVarStore().get("query_result"));
			verify(reactors.constructed().get(0)).setInsight(insight);
			verify(task).close();
		}
	}

	@Test
	void closesTheTaskWhenMaterializationFails() throws Exception {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		BasicIteratorTask task = mock(BasicIteratorTask.class);
		when(task.getHeaderInfo()).thenThrow(new IllegalStateException("materialization failed"));

		try (MockedConstruction<SqlQueryReactor> ignored = sqlQueryReactors(task)) {
			assertThrows(IllegalStateException.class,
					() -> AutomationDatabaseQueryExecutor.execute(insight, request(), "run-1", "node-1"));
			verify(task).close();
		}
	}

	private static MockedConstruction<SqlQueryReactor> sqlQueryReactors(BasicIteratorTask task) {
		return mockConstruction(SqlQueryReactor.class,
				(reactor, context) -> when(reactor.execute())
						.thenReturn(new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET)));
	}

	private static Map<String, Object> request() {
		return Map.of(AutomationConstants.INTERNAL_DATABASE_QUERY,
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
						AutomationConstants.CONFIG_LIMIT, 50));
	}
}
