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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.automation.definition.AutomationFrameOperation;
import prerna.reactor.automation.definition.AutomationNodeType;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.frame.PurgeReactor;
import prerna.reactor.frame.py.TrimColumnsReactor;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AutomationCleanExecutorUnitTests {
	private static final List<AutomationFrameOperation> FRAME_OPERATIONS = List.of(AutomationFrameOperation.values());

	@Test
	void databaseFramesSupportPurgeOutsideTheMergePath() {
		for (DataFrameTypeEnum frame : List.of(DataFrameTypeEnum.GRID, DataFrameTypeEnum.NATIVE)) {
			for (AutomationFrameOperation operation : FRAME_OPERATIONS) {
				if (operation == AutomationFrameOperation.JOIN)
					continue;
				if (operation == AutomationFrameOperation.PURGE) {
					assertInstanceOf(PurgeReactor.class, AutomationCleanExecutor.reactor(operation, frame));
				} else {
					assertThrows(IllegalArgumentException.class,
							() -> AutomationCleanExecutor.reactor(operation, frame));
				}
			}
		}
		assertInstanceOf(PurgeReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.PURGE, DataFrameTypeEnum.PYTHON));
	}

	@Test
	void rFramesAreRejected() {
		for (AutomationFrameOperation operation : FRAME_OPERATIONS) {
			assertThrows(IllegalArgumentException.class,
					() -> AutomationCleanExecutor.reactor(operation, DataFrameTypeEnum.R));
		}
	}

	@Test
	void preservesExistingRuntimeRestrictions() {
		assertInstanceOf(TrimColumnsReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.TRIM, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.FilterEmptyValuesReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.FILTER_EMPTY, DataFrameTypeEnum.PYTHON));
		assertNotNull(AutomationCleanExecutor.reactor(AutomationFrameOperation.REPLACE, DataFrameTypeEnum.PYTHON));
		assertNotNull(
				AutomationCleanExecutor.reactor(AutomationFrameOperation.REMOVE_DUPLICATES, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ImputeNullValuesReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.IMPUTE_NULLS, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ChangeColumnTypeReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.CHANGE_TYPE, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.DropColumnReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.DROP_COLUMN, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.RenameColumnReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.RENAME_COLUMN, DataFrameTypeEnum.PYTHON));
	}

	@Test
	void resolvesGenericCleanOperationsToExistingBiNodeTypes() {
		assertEquals(AutomationFrameOperation.TRIM, AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "trim"), Map.of(), "clean"));
		assertEquals(AutomationFrameOperation.PURGE, AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "purge"), Map.of(), "clean"));
		assertEquals(AutomationFrameOperation.CHANGE_TYPE, AutomationCleanExecutor.operationType(
				AutomationNodeType.DATA_TRANSFORM, Map.of("operation", "changeType"), Map.of(), "transform"));
		assertEquals(AutomationFrameOperation.JOIN, AutomationCleanExecutor.operationType(
				AutomationNodeType.DATA_TRANSFORM, Map.of("operation", "join"), Map.of(), "join"));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "unknown"), Map.of(), "clean"));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "changeType"), Map.of(), "clean"));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.operationType(
				AutomationNodeType.DATA_TRANSFORM, Map.of("operation", "trim"), Map.of(), "transform"));
	}

	@Test
	void resolvesNewSingleFrameTransformsToExistingBiReactors() {
		assertInstanceOf(prerna.reactor.frame.py.AddColumnReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.ADD_COLUMN, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.DuplicateColumnReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.DUPLICATE_COLUMN, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.JoinColumnsReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.CONCATENATE, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.PivotReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.PIVOT, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ToLowerCaseReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.LOWER_CASE, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ToProperCaseReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.PROPER_CASE, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.SplitColumnsReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.SPLIT, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.SplitUnpivotReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.SPLIT_UNPIVOT, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.TimestampDataReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.TIMESTAMP_DATA, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ToPercentReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.TO_PERCENT, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.TransposeReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.TRANSPOSE, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.UnpivotReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.UNPIVOT, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ToUpperCaseReactor.class,
				AutomationCleanExecutor.reactor(AutomationFrameOperation.UPPER_CASE, DataFrameTypeEnum.PYTHON));
	}

	@Test
	void rejectsOrdinaryJsonMasqueradingAsAFrame() {
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.execute(new Insight(), "cleaned",
				AutomationNodeType.CLEAN_PROCESS,
				Map.of("source", "${input}", "operation", "trim", "columns", List.of("NAME")),
				Map.of("input", Map.of("dataType", "table", "rows", List.of())), "clean"));
	}

	@Test
	void validatesColumnsAndStructuredFilters() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "NAME", "AGE" });
		assertEquals(List.of("NAME"), AutomationCleanExecutor.columns(List.of("NAME", "NAME"), frame));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.columns(List.of(), frame));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.columns(List.of("MISSING"), frame));
		assertThrows(IllegalArgumentException.class,
				() -> AutomationCleanExecutor.columns(List.of("NAME');exec('x"), frame));
		SelectQueryStruct filters = AutomationCleanExecutor.purgeFilters(
				List.of(Map.of("column", "AGE", "operator", ">=", "value", "${minimum}")), frame,
				Map.of("minimum", 18));
		assertEquals(1, filters.getCombinedFilters().getFilters().size());
		OrQueryFilter retained = (OrQueryFilter) filters.getCombinedFilters().getFilters().get(0);
		assertEquals("<", ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		assertEquals(18, ((SimpleQueryFilter) retained.getFilterList().get(0)).getRComparison().getValue());
		assertThrows(IllegalArgumentException.class,
				() -> AutomationCleanExecutor.purgeFilters(List.of(), frame, Map.of()));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(
				List.of(Map.of("column", "AGE", "operator", "execute", "value", 1)), frame, Map.of()));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(
				List.of(Map.of("column", "AGE", "operator", "==", "value", Map.of())), frame, Map.of()));
	}

	@Test
	void purgeRemovesOnlyRowsMatchingAllRules() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "NAME", "AGE" });
		SelectQueryStruct query = AutomationCleanExecutor.purgeFilters(List.of(
				Map.of("column", "NAME", "operator", "==", "value", "inactive"),
				Map.of("column", "AGE", "operator", ">", "value", 18)), frame, Map.of());
		OrQueryFilter retained = (OrQueryFilter) query.getCombinedFilters().getFilters().get(0);
		assertEquals(2, retained.getFilterList().size());
		assertEquals("!=", ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		assertEquals("<=", ((SimpleQueryFilter) retained.getFilterList().get(1)).getComparator());
	}

	@Test
	void purgeRejectsListsAndNullsAtTheAutomationBoundary() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID" });
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(
				List.of(Map.of("column", "ID", "operator", "==", "value", List.of(1, 2))), frame, Map.of()));
		Map<String, Object> nullRule = new java.util.LinkedHashMap<>();
		nullRule.put("column", "ID");
		nullRule.put("operator", "==");
		nullRule.put("value", null);
		assertThrows(IllegalArgumentException.class,
				() -> AutomationCleanExecutor.purgeFilters(List.of(nullRule), frame, Map.of()));
	}

	@Test
	void purgeInvertsAllComparisonBoundaries() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "AGE" });
		for (Map.Entry<String, String> comparison : Map
				.of("==", "!=", "!=", "==", ">", "<=", ">=", "<", "<", ">=", "<=", ">").entrySet()) {
			SelectQueryStruct query = AutomationCleanExecutor.purgeFilters(
					List.of(Map.of("column", "AGE", "operator", comparison.getKey(), "value", 18)), frame, Map.of());
			OrQueryFilter retained = (OrQueryFilter) query.getCombinedFilters().getFilters().get(0);
			assertEquals(comparison.getValue(), ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		}
	}

	@Test
	void copiesWithoutDistinctAndPublishesTheCleanedFrame() throws Exception {
		exerciseCopy(false, false);
	}

	@Test
	void preservesPythonBindingsWhenTheBiRoutineReturnsANewFrameWithTheSameName() throws Exception {
		exerciseCopy(false, true);
	}

	@Test
	void synchronizesPythonBindingsAfterBiFiltering() {
		PandasFrame frame = mock(PandasFrame.class);
		when(frame.getWrapperName()).thenReturn("cleanedw");
		AutomationCleanExecutor.synchronizePythonFrame(frame, "cleaned");
		verify(frame).runScript("cleaned = cleanedw.cache['data']");
		verify(frame).syncHeaders();
	}

	@ParameterizedTest
	@ValueSource(strings = { "NAME_COPY", "NAME_REGION", "TAGS_0" })
	void publishesNewColumnsWithMatchingSelectorsTypesAndValues(String newColumn) throws Exception {
		PandasFrame frame = mock(PandasFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		when(metadata.getFrameSelectors()).thenReturn(List.of("cleaned__NAME", "cleaned__VALUE"));
		doCallRealMethod().when(frame).setMetaData(metadata);
		doCallRealMethod().when(frame).syncHeaders();
		doCallRealMethod().when(frame).getSelectors();
		doCallRealMethod().when(frame).getQsHeaders();
		frame.setMetaData(metadata);
		assertArrayEquals(new String[] { "cleaned__NAME", "cleaned__VALUE" }, frame.getQsHeaders());

		List<String> selectors = List.of("cleaned__NAME", "cleaned__" + newColumn, "cleaned__VALUE");
		when(metadata.getFrameSelectors()).thenReturn(selectors);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "NAME", newColumn, "VALUE" });
		when(frame.getMetaData()).thenReturn(metadata);
		when(metadata.getHeaderTypeAsString("cleaned__NAME")).thenReturn("STRING");
		when(metadata.getHeaderTypeAsString("cleaned__" + newColumn)).thenReturn("STRING");
		when(metadata.getHeaderTypeAsString("cleaned__VALUE")).thenReturn("DOUBLE");
		when(frame.getWrapperName()).thenReturn("cleanedw");
		AutomationCleanExecutor.synchronizePythonFrame(frame, "cleaned");
		assertArrayEquals(selectors.toArray(String[]::new), frame.getQsHeaders());

		IRawSelectWrapper rows = mock(IRawSelectWrapper.class);
		when(rows.hasNext()).thenReturn(true, false);
		when(rows.next()).thenReturn(new HeadersDataRow(
				new String[] { "NAME", newColumn, "VALUE" }, new Object[] { "Alice", "new value", 0.25 }));
		when(frame.query(any(SelectQueryStruct.class))).thenAnswer(invocation -> {
			SelectQueryStruct query = invocation.getArgument(0);
			assertEquals(3, query.getSelectors().size());
			assertFalse(query.isDistinct());
			return rows;
		});
		AutomationFrameOutput.RegisteredFrame output = new AutomationFrameOutput.RegisteredFrame(
				"cleaned", frame, Map.of("rowCount", 1L), "PY");
		assertEquals(List.of(
				Map.of("name", "NAME", "type", "STRING"),
				Map.of("name", newColumn, "type", "STRING"),
				Map.of("name", "VALUE", "type", "DOUBLE")), output.columns());
		assertEquals(List.of(Map.of("NAME", "Alice", newColumn, "new value", "VALUE", 0.25)), output.page(0, 1));
		verify(rows).close();
	}

	@Test
	void closesTheCopiedFrameWhenTheBiRoutineFails() throws Exception {
		exerciseCopy(true, false);
	}

	private void exerciseCopy(boolean fail, boolean replaceFrame) throws Exception {
		Insight insight = new Insight();
		ITableDataFrame source = mock(ITableDataFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		PandasFrame copied = mock(PandasFrame.class);
		PandasFrame returnedFrame = replaceFrame ? mock(PandasFrame.class) : copied;
		when(copied.getName()).thenReturn("cleaned");
		when(copied.getWrapperName()).thenReturn("cleanedw");
		when(copied.getColumnHeaders()).thenReturn(new String[] { "NAME", "MISSING_DATE" });
		when(copied.getMetaData()).thenReturn(metadata);
		when(metadata.getHeaderTypeAsString("cleaned__MISSING_DATE")).thenReturn("DATE");
		when(returnedFrame.getName()).thenReturn("cleaned");
		when(returnedFrame.getWrapperName()).thenReturn("cleanedw");
		java.util.concurrent.atomic.AtomicBoolean bindingPresent = new java.util.concurrent.atomic.AtomicBoolean(true);
		doAnswer(invocation -> {
			bindingPresent.set(false);
			return null;
		}).when(copied).close();
		IRawSelectWrapper rows = mock(IRawSelectWrapper.class);
		IImporter importer = mock(IImporter.class);
		SelectQueryStruct query = new SelectQueryStruct();
		when(source.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(source.getColumnHeaders()).thenReturn(new String[] { "NAME" });
		when(source.getMetaData()).thenReturn(metadata);
		when(metadata.getFlatTableQs(false)).thenReturn(query);
		when(source.getFrameFilters()).thenReturn(new GenRowFilters());
		when(source.query(query)).thenReturn(rows);
		when(returnedFrame.size("cleaned")).thenAnswer(invocation -> {
			if (!bindingPresent.get())
				throw new IllegalStateException("NameError: cleanedw is not defined");
			return 2L;
		});
		when(returnedFrame.getQsHeaders()).thenReturn(new String[] { "cleaned__NAME" });
		when(returnedFrame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		AutomationFrameOutput.RegisteredFrame input = new AutomationFrameOutput.RegisteredFrame("input", source,
				Map.of("rowCount", 2L, "columnCount", 1, "dataType", "table"), "PY");
		try (MockedStatic<FrameFactory> factory = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> imports = mockStatic(ImportFactory.class);
				MockedConstruction<TrimColumnsReactor> reactors = mockConstruction(TrimColumnsReactor.class,
						(reactor, context) -> {
							if (fail) {
								when(reactor.execute()).thenThrow(new IllegalStateException("BI failure"));
							} else {
								when(reactor.execute())
										.thenReturn(new NounMetadata(returnedFrame, PixelDataType.FRAME));
							}
						})) {
			factory.when(() -> FrameFactory.getFrame(insight, "PY", "cleaned")).thenReturn(copied);
			imports.when(() -> ImportFactory.getImporter(copied, query, rows)).thenReturn(importer);
			Map<String, Object> config = Map.of(
					"source", "${input}", "operation", "trim", "columns", List.of("NAME"));
			if (fail) {
				assertThrows(IllegalStateException.class, () -> AutomationCleanExecutor.execute(insight, "cleaned",
						AutomationNodeType.CLEAN_PROCESS, config, Map.of("input", input), "clean"));
				verify(copied).close();
				assertNull(insight.getVarStore().get("cleaned"));
			} else {
				AutomationFrameOutput.RegisteredFrame output = AutomationCleanExecutor.execute(insight, "cleaned",
						AutomationNodeType.CLEAN_PROCESS, config, Map.of("input", input), "clean");
				assertSame(returnedFrame, output.frame());
				assertSame(returnedFrame, insight.getVarStore().get("cleaned").getValue());
				verify(copied, never()).close();
				assertTrue(bindingPresent.get());
				assertEquals(2L, output.frame().size("cleaned"));
			}
			assertFalse(query.isDistinct());
			verify(importer).setInsight(insight);
			verify(importer).insertData();
			verify(copied).runScript(
					"cleanedw.cache['data'][\"MISSING_DATE\"] = pd.to_datetime(cleanedw.cache['data'][\"MISSING_DATE\"])");
			verify(rows).close();
			verify(source, never()).close();
			verify(reactors.constructed().get(0)).setNounStore(any());
		}
	}
}
