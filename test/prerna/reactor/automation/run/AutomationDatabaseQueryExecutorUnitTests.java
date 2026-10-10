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
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.automation.AutomationConstants;
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
	void executesTheSqlTaskImporterAndFrameRegistrationPipeline() throws Exception {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		BasicIteratorTask task = mock(BasicIteratorTask.class);
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IRawSelectWrapper wrapper = mock(IRawSelectWrapper.class);
		IImporter importer = mock(IImporter.class);
		when(task.getQueryStruct()).thenReturn(queryStruct);
		when(task.getIterator()).thenReturn(wrapper);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID", "STATUS" });
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.NATIVE);

		try (MockedConstruction<SqlQueryReactor> reactors = sqlQueryReactors(task);
				MockedStatic<FrameFactory> frames = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> importers = mockStatic(ImportFactory.class);
				MockedStatic<AutomationFrameHistory> history = mockStatic(AutomationFrameHistory.class)) {
			frames.when(() -> FrameFactory.getFrame(insight, DataFrameTypeEnum.NATIVE.getTypeAsString(),
					"query_result")).thenReturn(frame);
			importers.when(() -> ImportFactory.getImporter(frame, queryStruct, task)).thenReturn(importer);
			history.when(() -> AutomationFrameHistory.captureWrapper("run-1", "node-1", "query_result", wrapper))
					.thenReturn(new AutomationFrameHistory.Snapshot("reference", "run-1", "node-1", "query_result",
							AutomationConstants.DATA_STATE_AVAILABLE, java.util.List.of("ID", "STATUS"),
							java.util.List.of("INT", "STRING"), 2L, 2, 32L));

			AutomationFrameOutput.RegisteredFrame output = AutomationDatabaseQueryExecutor.execute(insight,
					request(), "query_result", "run-1", "node-1");

			assertEquals(Map.of("dataType", "table", "rowCount", 2L, "columnCount", 2), output.summary());
			assertSame(frame, insight.getVarStore().get("query_result").getValue());
			verify(reactors.constructed().get(0)).setInsight(insight);
			verify(importer).setInsight(insight);
			verify(importer).insertData();
			verify(task).close();
		}
	}

	@Test
	void closesTheTaskAndUnregisteredFrameWhenImportFails() throws Exception {
		Insight insight = new Insight();
		insight.setInsightId("automation-run-1");
		BasicIteratorTask task = mock(BasicIteratorTask.class);
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		ITableDataFrame frame = mock(ITableDataFrame.class);
		IImporter importer = mock(IImporter.class);
		when(task.getQueryStruct()).thenReturn(queryStruct);
		doThrow(new IllegalStateException("import failed")).when(importer).insertData();

		try (MockedConstruction<SqlQueryReactor> ignored = sqlQueryReactors(task);
				MockedStatic<FrameFactory> frames = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> importers = mockStatic(ImportFactory.class)) {
			frames.when(() -> FrameFactory.getFrame(insight, DataFrameTypeEnum.NATIVE.getTypeAsString(),
					"query_result")).thenReturn(frame);
			importers.when(() -> ImportFactory.getImporter(frame, queryStruct, task)).thenReturn(importer);

			assertThrows(IllegalStateException.class, () -> AutomationDatabaseQueryExecutor.execute(insight, request(),
					"query_result", "run-1", "node-1"));

			assertNull(insight.getVarStore().get("query_result"));
			verify(frame).close();
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
