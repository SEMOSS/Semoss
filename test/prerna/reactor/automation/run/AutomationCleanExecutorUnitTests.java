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
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.automation.definition.AutomationCleanOperation;
import prerna.reactor.automation.definition.AutomationNodeType;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.frame.PurgeReactor;
import prerna.reactor.frame.py.TrimColumnsReactor;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class AutomationCleanExecutorUnitTests {
	private static final List<AutomationCleanOperation> CLEAN_OPERATIONS = List.of(AutomationCleanOperation.values());
	private static final List<AutomationCleanOperation> PYTHON_ONLY_OPERATIONS = List.of(
			AutomationCleanOperation.FILTER_EMPTY,
			AutomationCleanOperation.IMPUTE_NULLS,
			AutomationCleanOperation.CHANGE_TYPE,
			AutomationCleanOperation.DROP_COLUMN,
			AutomationCleanOperation.RENAME_COLUMN);

	@Test
	void databaseFramesOnlyAllowPurge() {
		for (DataFrameTypeEnum frame : List.of(DataFrameTypeEnum.GRID, DataFrameTypeEnum.NATIVE)) {
			for (AutomationCleanOperation operation : CLEAN_OPERATIONS) {
				if (operation == AutomationCleanOperation.PURGE) {
					assertInstanceOf(PurgeReactor.class, AutomationCleanExecutor.reactor(operation, frame));
					continue;
				}
				assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.reactor(operation, frame));
			}
		}
	}

	@Test
	void preservesExistingRuntimeRestrictions() {
		assertInstanceOf(TrimColumnsReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.TRIM, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.r.TrimColumnsReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.TRIM, DataFrameTypeEnum.R));
		assertInstanceOf(prerna.reactor.frame.py.FilterEmptyValuesReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.FILTER_EMPTY, DataFrameTypeEnum.PYTHON));
		for (DataFrameTypeEnum frame : List.of(DataFrameTypeEnum.R, DataFrameTypeEnum.PYTHON)) {
			assertNotNull(AutomationCleanExecutor.reactor(AutomationCleanOperation.REPLACE, frame));
			assertNotNull(AutomationCleanExecutor.reactor(AutomationCleanOperation.REMOVE_DUPLICATES, frame));
		}
		assertInstanceOf(prerna.reactor.frame.py.ImputeNullValuesReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.IMPUTE_NULLS, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.ChangeColumnTypeReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.CHANGE_TYPE, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.DropColumnReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.DROP_COLUMN, DataFrameTypeEnum.PYTHON));
		assertInstanceOf(prerna.reactor.frame.py.RenameColumnReactor.class,
				AutomationCleanExecutor.reactor(AutomationCleanOperation.RENAME_COLUMN, DataFrameTypeEnum.PYTHON));
		for (AutomationCleanOperation operation : PYTHON_ONLY_OPERATIONS) {
			assertThrows(IllegalArgumentException.class,
					() -> AutomationCleanExecutor.reactor(operation, DataFrameTypeEnum.R));
		}
	}

	@Test
	void resolvesGenericCleanOperationsToExistingBiNodeTypes() {
		assertEquals(AutomationCleanOperation.TRIM, AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "trim"), Map.of(), "clean"));
		assertEquals(AutomationCleanOperation.PURGE, AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "purge"), Map.of(), "clean"));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.operationType(
				AutomationNodeType.CLEAN_PROCESS, Map.of("operation", "unknown"), Map.of(), "clean"));
	}

	@Test
	void rejectsOrdinaryJsonMasqueradingAsAFrame() {
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.execute(new Insight(), "cleaned",
				AutomationNodeType.CLEAN_PROCESS, Map.of("source", "${input}", "operation", "trim", "columns", List.of("NAME")),
				Map.of("input", Map.of("dataType", "table", "rows", List.of())), "clean"));
	}

	@Test
	void validatesColumnsAndStructuredFilters() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "NAME", "AGE" });
		assertEquals(List.of("NAME"), AutomationCleanExecutor.columns(List.of("NAME", "NAME"), frame));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.columns(List.of(), frame));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.columns(List.of("MISSING"), frame));
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.columns(List.of("NAME');exec('x"), frame));
		SelectQueryStruct filters = AutomationCleanExecutor.purgeFilters(
				List.of(Map.of("column", "AGE", "operator", ">=", "value", "${minimum}")), frame, Map.of("minimum", 18));
		assertEquals(1, filters.getCombinedFilters().getFilters().size());
		OrQueryFilter retained = (OrQueryFilter) filters.getCombinedFilters().getFilters().get(0);
		assertEquals("<", ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		assertEquals(18, ((SimpleQueryFilter) retained.getFilterList().get(0)).getRComparison().getValue());
		assertNull(((SimpleQueryFilter) retained.getFilterList().get(1)).getRComparison().getValue());
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(List.of(), frame, Map.of()));
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
		assertEquals(4, retained.getFilterList().size());
		assertEquals("!=", ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		assertEquals("<=", ((SimpleQueryFilter) retained.getFilterList().get(2)).getComparator());
	}

	@Test
	void purgeSupportsBoundedMembershipAndResolvesReferencesInsideValueLists() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID" });
		for (String comparator : List.of("==", "!=")) {
			SelectQueryStruct query = AutomationCleanExecutor.purgeFilters(
					List.of(Map.of("column", "ID", "operator", comparator, "value", List.of(1, "${second}"))), frame, Map.of("second", 2));
			OrQueryFilter retained = (OrQueryFilter) query.getCombinedFilters().getFilters().get(0);
			SimpleQueryFilter membership = (SimpleQueryFilter) retained.getFilterList().get(0);
			assertEquals(comparator.equals("==") ? "!=" : "==", membership.getComparator());
			assertEquals(List.of(1, 2), membership.getRComparison().getValue());
			assertEquals(2, retained.getFilterList().size());
		}
		SelectQueryStruct explicitNull = AutomationCleanExecutor.purgeFilters(List.of(Map.of("column", "ID", "operator", "==", "value", java.util.Arrays.asList(1, null))), frame, Map.of());
		assertEquals(1, ((OrQueryFilter) explicitNull.getCombinedFilters().getFilters().get(0)).getFilterList().size());
		for (Object invalid : List.of(List.of(), List.of(List.of(1)), java.util.Collections.nCopies(101, 1))) {
			assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(List.of(Map.of("column", "ID", "operator", "==", "value", invalid)), frame, Map.of()));
		}
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(List.of(Map.of("column", "ID", "operator", ">", "value", List.of(1, 2))), frame, Map.of()));
	}

	@Test
	void purgeInvertsAllComparisonBoundariesAndSupportsExplicitNullRemoval() {
		ITableDataFrame frame = mock(ITableDataFrame.class);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "AGE" });
		for (Map.Entry<String, String> comparison : Map.of("==", "!=", "!=", "==", ">", "<=", ">=", "<", "<", ">=", "<=", ">").entrySet()) {
			SelectQueryStruct query = AutomationCleanExecutor.purgeFilters(
					List.of(Map.of("column", "AGE", "operator", comparison.getKey(), "value", 18)), frame, Map.of());
			OrQueryFilter retained = (OrQueryFilter) query.getCombinedFilters().getFilters().get(0);
			assertEquals(comparison.getValue(), ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		}
		Map<String, Object> nullRule = new java.util.LinkedHashMap<>();
		nullRule.put("column", "AGE");
		nullRule.put("operator", "==");
		nullRule.put("value", null);
		SelectQueryStruct query = AutomationCleanExecutor.purgeFilters(List.of(nullRule), frame, Map.of());
		OrQueryFilter retained = (OrQueryFilter) query.getCombinedFilters().getFilters().get(0);
		assertEquals(1, retained.getFilterList().size());
		assertEquals("!=", ((SimpleQueryFilter) retained.getFilterList().get(0)).getComparator());
		nullRule.put("operator", ">");
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.purgeFilters(List.of(nullRule), frame, Map.of()));
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
		doAnswer(invocation -> { bindingPresent.set(false); return null; }).when(copied).close();
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
			if (!bindingPresent.get()) throw new IllegalStateException("NameError: cleanedw is not defined");
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
								when(reactor.execute()).thenReturn(new NounMetadata(returnedFrame, PixelDataType.FRAME));
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