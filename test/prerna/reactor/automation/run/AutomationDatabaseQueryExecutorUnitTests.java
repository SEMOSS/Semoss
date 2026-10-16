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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class AutomationDatabaseQueryExecutorUnitTests {

	@Test
	void resolvesTheTypedQueryRequest() {
		AutomationDatabaseQueryExecutor.QueryRequest parsed = AutomationDatabaseQueryExecutor.request(
				Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
						AutomationConstants.CONFIG_LIMIT, AutomationConstants.DB_QUERY_MAX_LIMIT),
				Map.of(), "query");
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
		AutomationDatabaseQueryExecutor.QueryRequest request = AutomationDatabaseQueryExecutor.request(
				Map.of("engineId", "${database}", "query", "${sql}", "parameters",
						List.of("${unsafe}", "${number}", "${missing}"), "limit", "${limit}"),
				scope, "query");

		assertEquals("engine-1", request.databaseId());
		assertEquals("SELECT * FROM T WHERE V=? AND N=? AND M IS ?", request.query());
		assertEquals(java.util.Arrays.asList("'; DROP TABLE T; --\u03bb", 42, null), request.parameters());
		assertEquals(25, request.limit());
	}

	@Test
	void profilesTypedQuerySubmissionOverhead() {
		int iterations = 1_000;

		long requestStarted = System.nanoTime();
		for (int i = 0; i < iterations; i++) {
			AutomationDatabaseQueryExecutor.request(
					Map.of("engineId", "engine-1", "query", "SELECT 1", "limit", 50),
					Map.of(), "query");
		}
		long requestNanos = System.nanoTime() - requestStarted;

		System.out.printf("AUTOMATION_SIMPLE_QUERY_TIMING iterations=%d typedRequestMs=%.3f%n",
				iterations, requestNanos / 1_000_000.0);
		assertTrue(requestNanos > 0);
	}

	@Test
	void rejectsMalformedOrUnboundedRequests() {
		assertThrows(IllegalArgumentException.class,
				() -> AutomationDatabaseQueryExecutor.request(
						Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", AutomationConstants.CONFIG_LIMIT, 50),
						Map.of(), "query"));

		assertThrows(IllegalArgumentException.class,
				() -> AutomationDatabaseQueryExecutor.request(
						Map.of(AutomationConstants.CONFIG_ENGINE_ID, "engine-1", "query", "SELECT 1",
								AutomationConstants.CONFIG_LIMIT, AutomationConstants.DB_QUERY_MAX_LIMIT + 1),
						Map.of(), "query"));
	}

	@Test
	void normalizesOnlyMetadataTypedDateColumnsInImportedPythonFrames() {
		PandasFrame frame = mock(PandasFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		when(frame.getName()).thenReturn("query_result");
		when(frame.getWrapperName()).thenReturn("query_resultw");
		when(frame.getColumnHeaders()).thenReturn(new String[] { "MISSING_DATE", "CREATED_AT", "NAME", "DATE \"TEXT\"" });
		when(frame.getMetaData()).thenReturn(metadata);
		when(metadata.getHeaderTypeAsString("query_result__MISSING_DATE")).thenReturn("DATE");
		when(metadata.getHeaderTypeAsString("query_result__CREATED_AT")).thenReturn("TIMESTAMP");
		when(metadata.getHeaderTypeAsString("query_result__NAME")).thenReturn("STRING");
		when(metadata.getHeaderTypeAsString("query_result__DATE \"TEXT\"")).thenReturn("DATE");

		assertEquals(List.of(
				"query_resultw.cache['data'][\"MISSING_DATE\"] = pd.to_datetime(query_resultw.cache['data'][\"MISSING_DATE\"])",
				"query_resultw.cache['data'][\"CREATED_AT\"] = pd.to_datetime(query_resultw.cache['data'][\"CREATED_AT\"])",
				"query_resultw.cache['data'][\"DATE \\\"TEXT\\\"\"] = pd.to_datetime(query_resultw.cache['data'][\"DATE \\\"TEXT\\\"\"])") ,
				AutomationDatabaseQueryExecutor.dateNormalizationScripts(frame));
	}

	@Test
	void importsTheAuthorizedSqlTaskIntoTheRunFrame() throws Exception {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		AutomationPixelExecutor pixelExecutor = mock(AutomationPixelExecutor.class);
		BasicIteratorTask task = mock(BasicIteratorTask.class);
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		PandasFrame frame = mock(PandasFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		IImporter importer = mock(IImporter.class);
		when(task.getQueryStruct()).thenReturn(queryStruct);
		when(pixelExecutor.executeTask("SqlQuery(database=\"engine-1\",query=\"SELECT 1\",limit=50);"))
				.thenReturn(task);
		when(frame.size("query_result")).thenReturn(2L);
		when(frame.getQsHeaders()).thenReturn(new String[] { "QUERY_RESULT__ID", "QUERY_RESULT__STATUS" });
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(frame.getName()).thenReturn("query_result");
		when(frame.getWrapperName()).thenReturn("query_resultw");
		when(frame.getColumnHeaders()).thenReturn(new String[] { "CREATED_AT" });
		when(frame.getMetaData()).thenReturn(metadata);
		when(metadata.getHeaderTypeAsString("query_result__CREATED_AT")).thenReturn("DATE");

		try (MockedStatic<FrameFactory> frames = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> importers = mockStatic(ImportFactory.class)) {
			frames.when(() -> FrameFactory.getFrame(insight, DataFrameTypeEnum.PYTHON.getTypeAsString(),
					"query_result")).thenReturn(frame);
			importers.when(() -> ImportFactory.getImporter(frame, queryStruct, task)).thenReturn(importer);

			AutomationFrameOutput.RegisteredFrame output = AutomationDatabaseQueryExecutor.execute(insight, pixelExecutor,
					request(), "query_result", "run-1", "node-1");

			assertEquals(Map.of("dataType", "table", "rowCount", 2L, "columnCount", 2), output.summary());
			verify(frame).runScript(
					"query_resultw.cache['data'][\"CREATED_AT\"] = pd.to_datetime(query_resultw.cache['data'][\"CREATED_AT\"])");
			assertSame(frame, insight.getVarStore().get("query_result").getValue());
			verify(pixelExecutor).executeTask("SqlQuery(database=\"engine-1\",query=\"SELECT 1\",limit=50);");
			verify(importer).setInsight(insight);
			verify(importer).insertData();
			verify(task).close();
		}
	}

	@Test
	void closesTheTaskAndFrameWhenImportFails() throws Exception {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		AutomationPixelExecutor pixelExecutor = mock(AutomationPixelExecutor.class);
		BasicIteratorTask task = mock(BasicIteratorTask.class);
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IImporter importer = mock(IImporter.class);
		when(task.getQueryStruct()).thenReturn(queryStruct);
		when(pixelExecutor.executeTask("SqlQuery(database=\"engine-1\",query=\"SELECT 1\",limit=50);"))
				.thenReturn(task);
		doThrow(new IllegalStateException("import failed")).when(importer).insertData();

		try (MockedStatic<FrameFactory> frames = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> importers = mockStatic(ImportFactory.class)) {
			frames.when(() -> FrameFactory.getFrame(insight, DataFrameTypeEnum.PYTHON.getTypeAsString(),
					"query_result")).thenReturn(frame);
			importers.when(() -> ImportFactory.getImporter(frame, queryStruct, task)).thenReturn(importer);
			assertThrows(IllegalStateException.class,
					() -> AutomationDatabaseQueryExecutor.execute(insight, pixelExecutor, request(), "query_result", "run-1",
							"node-1"));
			assertNull(insight.getVarStore().get("query_result"));
			verify(frame).close();
			verify(task).close();
		}
	}

	private static AutomationDatabaseQueryExecutor.QueryRequest request() {
		return new AutomationDatabaseQueryExecutor.QueryRequest("engine-1", "SELECT 1", 50, List.of());
	}
}
