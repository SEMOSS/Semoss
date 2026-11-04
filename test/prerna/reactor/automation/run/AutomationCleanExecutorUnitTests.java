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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationFrameOperation;
import prerna.reactor.automation.definition.AutomationNodeType;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.frame.PurgeReactor;
import prerna.reactor.frame.py.TrimColumnsReactor;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.reactor.imports.ImportUtility;
import prerna.reactor.imports.MergeReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
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
		AutomationCleanExecutor.synchronizePythonFrame(frame, "cleaned", AutomationFrameOperation.FILTER_EMPTY);
		verify(frame).runScript("cleaned = cleanedw.cache['data']");
		verify(frame, never()).recreateMeta();
		verify(frame).syncHeaders();
	}

	@ParameterizedTest
	@ValueSource(ints = { 0, 4 })
	void splitUnpivotRemovesStaleSplitColumnsBeforePagingAndCapture(int rowCount) throws Exception {
		PyTranslator translator = mock(PyTranslator.class);
		PandasFrame frame = spy(new PandasFrame("cleaned", translator));
		String[] columns = { "LABEL", "NAME", "STATUS" };
		String[] selectors = { "cleaned__LABEL", "cleaned__NAME", "cleaned__STATUS" };
		try {
			// Split Unpivot has removed LABEL_0/LABEL_1 from Python, but the BI metadata
			// still contains the columns created by the preceding Split operation.
			ImportUtility.parseTableColumnsAndTypesToFlatTable(frame.getMetaData(),
					new String[] { "LABEL", "LABEL_0", "LABEL_1", "NAME", "STATUS" },
					new String[] { "STRING", "STRING", "STRING", "STRING", "STRING" }, "cleaned");
			frame.syncHeaders();
			assertEquals(5, frame.getQsHeaders().length);
			Map<String, List<String>> sources = frame.getMetaData().getHeaderToSources();
			when(translator.getStringArray(PandasSyntaxHelper.getColumns("cleaned"))).thenReturn(columns);
			when(translator.getStringArray(PandasSyntaxHelper.getTypes("cleaned")))
					.thenReturn(new String[] { "object", "object", "object" });
			doReturn((long) rowCount).when(frame).size("cleaned");

			AutomationCleanExecutor.synchronizePythonFrame(frame, "cleaned", AutomationFrameOperation.SPLIT_UNPIVOT);
			assertArrayEquals(selectors, frame.getQsHeaders());
			assertArrayEquals(columns, frame.getColumnHeaders());
			assertEquals(sources.get("cleaned__NAME"), frame.getMetaData().getHeaderToSources().get("cleaned__NAME"));
			InOrder synchronization = inOrder(frame);
			synchronization.verify(frame).runScript("cleaned = " + frame.getWrapperName() + ".cache['data']");
			synchronization.verify(frame).recreateMeta();
			synchronization.verify(frame).syncHeaders();

			IRawSelectWrapper rows = mock(IRawSelectWrapper.class);
			when(rows.hasNext()).thenReturn(true, true, true, true, false);
			HeadersDataRow first = new HeadersDataRow(columns, new Object[] { "Alice", "Alice", "replace_me" });
			HeadersDataRow second = new HeadersDataRow(columns, new Object[] { "replace_me", "Alice", "replace_me" });
			when(rows.next()).thenReturn(first, first, second, second);
			doAnswer(invocation -> {
				SelectQueryStruct query = invocation.getArgument(0);
				assertEquals(List.of(selectors), query.getSelectors().stream()
						.map(selector -> selector.getQueryStructName()).toList());
				assertFalse(query.isDistinct());
				assertEquals(0L, query.getOffset());
				assertEquals(rowCount, query.getLimit());
				return rows;
			}).when(frame).query(any(SelectQueryStruct.class));
			Insight insight = new Insight();
			AutomationFrameOutput.RegisteredFrame output = AutomationFrameOutput.register(insight, "cleaned", frame);
			assertSame(frame, insight.getVarStore().get("cleaned").getValue());
			assertEquals(3, output.summary().get("columnCount"));
			assertEquals(List.of(Map.of("name", "LABEL", "type", "STRING"),
					Map.of("name", "NAME", "type", "STRING"), Map.of("name", "STATUS", "type", "STRING")),
					output.columns());
			AutomationTableCaptureService.TableCapture capture = AutomationTableCaptureService.capture(output, "unpivot");
			assertEquals(rowCount, capture.rowCount());
			List<Object> expected = rowCount == 0 ? List.of() : List.of(
					Map.of("LABEL", "Alice", "NAME", "Alice", "STATUS", "replace_me"),
					Map.of("LABEL", "Alice", "NAME", "Alice", "STATUS", "replace_me"),
					Map.of("LABEL", "replace_me", "NAME", "Alice", "STATUS", "replace_me"),
					Map.of("LABEL", "replace_me", "NAME", "Alice", "STATUS", "replace_me"));
			assertEquals(expected, AutomationTableCaptureService.page(capture, 0, rowCount));
			if (rowCount == 0) {
				verify(frame, never()).query(any(SelectQueryStruct.class));
			} else {
				verify(rows).close();
			}
		} finally {
			frame.close();
		}
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
		AutomationCleanExecutor.synchronizePythonFrame(frame, "cleaned", AutomationFrameOperation.DUPLICATE_COLUMN);
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

	@Test
	void rejectsAndClosesAnOversizedSingleFrameResultBeforeRegistration() throws Exception {
		exerciseCopy(false, false, AutomationConstants.SAVED_RESULT_MAX_ROWS + 1);
	}

	private static Stream<Arguments> supportedDelimiters() {
		return Stream.concat(Stream.of(Arguments.of("concatenate", null, " ")),
				Stream.of("concatenate", "split", "splitUnpivot").flatMap(operation -> Stream.of(
						Arguments.of(operation, " ", " "),
						Arguments.of(operation, "  ", "  "),
						Arguments.of(operation, "${separator}", " "),
						Arguments.of(operation, ",", ","))));
	}

	@ParameterizedTest
	@MethodSource("supportedDelimiters")
	void passesDelimitersUnchangedToTheBiReactor(String operation, String configured, String expected) throws Exception {
		Insight insight = new Insight();
		PandasFrame source = pythonFrame("input");
		PandasFrame copied = pythonFrame("cleaned");
		SelectQueryStruct query = source.getMetaData().getFlatTableQs(false);
		IRawSelectWrapper rows = mock(IRawSelectWrapper.class);
		IImporter importer = mock(IImporter.class);
		when(source.query(query)).thenReturn(rows);
		Class<? extends AbstractReactor> reactorClass = switch (operation) {
			case "concatenate" -> prerna.reactor.frame.py.JoinColumnsReactor.class;
			case "split" -> prerna.reactor.frame.py.SplitColumnsReactor.class;
			default -> prerna.reactor.frame.py.SplitUnpivotReactor.class;
		};
		Map<String, Object> config = new LinkedHashMap<>(Map.of("source", "${input}", "operation", operation,
				"columns", List.of("ID", "NAME"), "newColumn", "COMBINED"));
		if (configured != null) {
			config.put("delimiter", configured);
		}
		try (MockedStatic<FrameFactory> factory = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> imports = mockStatic(ImportFactory.class);
				MockedConstruction<? extends AbstractReactor> reactors = mockConstruction(reactorClass,
						(reactor, context) -> when(reactor.execute())
								.thenReturn(new NounMetadata(copied, PixelDataType.FRAME)))) {
			factory.when(() -> FrameFactory.getFrame(insight, "PY", "cleaned")).thenReturn(copied);
			imports.when(() -> ImportFactory.getImporter(copied, query, rows)).thenReturn(importer);
			AutomationFrameOutput.RegisteredFrame output = AutomationCleanExecutor.execute(insight, "cleaned",
					AutomationNodeType.DATA_TRANSFORM, config,
					Map.of("input", registeredFrame("input", source), "separator", " "), "transform");
			ArgumentCaptor<NounStore> nouns = ArgumentCaptor.forClass(NounStore.class);
			verify(reactors.constructed().get(0)).setNounStore(nouns.capture());
			assertEquals(expected, nouns.getValue().getNoun(ReactorKeysEnum.DELIMITER.getKey()).get(0));
			assertSame(copied, output.frame());
			assertSame(copied, insight.getVarStore().get("cleaned").getValue());
			assertFalse(query.isDistinct());
			verify(importer).insertData();
			verify(rows).close();
			verify(source, never()).close();
			verify(copied, never()).close();
			if ("splitUnpivot".equals(operation)) {
				verify(copied).recreateMeta();
			} else {
				verify(copied, never()).recreateMeta();
			}
		}
	}

	@Test
	void closesSplitUnpivotCopyWhenMetadataRefreshFails() throws Exception {
		Insight insight = new Insight();
		PandasFrame source = pythonFrame("input");
		PandasFrame copied = pythonFrame("cleaned");
		insight.getVarStore().put("input", new NounMetadata(source, PixelDataType.FRAME));
		SelectQueryStruct query = source.getMetaData().getFlatTableQs(false);
		IRawSelectWrapper rows = mock(IRawSelectWrapper.class);
		IImporter importer = mock(IImporter.class);
		when(source.query(query)).thenReturn(rows);
		IllegalStateException failure = new IllegalStateException("Metadata refresh failed");
		doThrow(failure).when(copied).recreateMeta();
		try (MockedStatic<FrameFactory> factory = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> imports = mockStatic(ImportFactory.class);
				MockedConstruction<prerna.reactor.frame.py.SplitUnpivotReactor> reactors = mockConstruction(
						prerna.reactor.frame.py.SplitUnpivotReactor.class,
						(reactor, context) -> when(reactor.execute())
								.thenReturn(new NounMetadata(copied, PixelDataType.FRAME)))) {
			factory.when(() -> FrameFactory.getFrame(insight, "PY", "cleaned")).thenReturn(copied);
			imports.when(() -> ImportFactory.getImporter(copied, query, rows)).thenReturn(importer);
			IllegalStateException error = assertThrows(IllegalStateException.class,
					() -> AutomationCleanExecutor.execute(insight, "cleaned", AutomationNodeType.DATA_TRANSFORM,
							Map.of("source", "${input}", "operation", "splitUnpivot", "columns", List.of("NAME"),
									"delimiter", " "), Map.of("input", registeredFrame("input", source)), "unpivot"));
			assertSame(failure, error.getCause());
			verify(copied).close();
			verify(rows).close();
			verify(source, never()).close();
			assertNull(insight.getVarStore().get("cleaned"));
			assertFalse(insight.getVarStore().getAllCreatedFrames().contains(copied));
			assertSame(source, insight.getVarStore().get("input").getValue());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "", "'", "\"", "\\", "\n", "\t" })
	void rejectsUnsupportedDelimitersBeforeCopyingTheFrame(String delimiter) throws Exception {
		PandasFrame source = pythonFrame("input");
		assertThrows(IllegalArgumentException.class, () -> AutomationCleanExecutor.execute(new Insight(), "cleaned",
				AutomationNodeType.DATA_TRANSFORM,
				Map.of("source", "${input}", "operation", "split", "columns", List.of("NAME"), "delimiter", delimiter),
				Map.of("input", registeredFrame("input", source)), "split"));
		verify(source, never()).query(any(SelectQueryStruct.class));
		verify(source, never()).close();
	}

	private enum JoinOutcome {
		SUCCESS, OVERSIZED, HEADER_FAILURE, REGISTRATION_FAILURE
	}

	private static Stream<Arguments> joinOutcomes() {
		return Stream.of(JoinOutcome.values()).flatMap(outcome -> Stream.of(
				Arguments.of(outcome, false), Arguments.of(outcome, true)));
	}

	@ParameterizedTest
	@MethodSource("joinOutcomes")
	void joinOwnsOnlyTheWorkingResult(JoinOutcome outcome, boolean replacementWrapper) throws Exception {
		Insight insight = new Insight();
		PandasFrame left = pythonFrame("left");
		PandasFrame right = pythonFrame("right");
		PandasFrame copied = pythonFrame("joined");
		PandasFrame result = replacementWrapper ? pythonFrame("joined") : copied;
		insight.getVarStore().put("left", new NounMetadata(left, PixelDataType.FRAME));
		insight.getVarStore().put("right", new NounMetadata(right, PixelDataType.FRAME));
		SelectQueryStruct query = left.getMetaData().getFlatTableQs(false);
		SelectQueryStruct rightQuery = right.getMetaData().getFlatTableQs(false);
		IRawSelectWrapper rows = mock(IRawSelectWrapper.class);
		IImporter importer = mock(IImporter.class);
		when(left.query(query)).thenReturn(rows);
		switch (outcome) {
			case OVERSIZED -> when(result.size("joined")).thenReturn(AutomationConstants.SAVED_RESULT_MAX_ROWS + 1);
			case HEADER_FAILURE -> doThrow(new IllegalStateException("Header synchronization failed"))
					.when(result).syncHeaders();
			case REGISTRATION_FAILURE -> when(result.getQsHeaders())
					.thenThrow(new IllegalStateException("Result description failed"));
			case SUCCESS -> { }
		}
		try (MockedStatic<FrameFactory> factory = mockStatic(FrameFactory.class);
				MockedStatic<ImportFactory> imports = mockStatic(ImportFactory.class);
				MockedConstruction<MergeReactor> reactors = mockConstruction(MergeReactor.class,
						(reactor, context) -> when(reactor.execute()).thenAnswer(invocation -> {
							NounMetadata noun = new NounMetadata(result, PixelDataType.FRAME);
							// Merge may publish its result before Automation validates/registers it.
							insight.getVarStore().put("joined", noun);
							return noun;
						}))) {
			factory.when(() -> FrameFactory.getFrame(insight, "PY", "joined")).thenReturn(copied);
			imports.when(() -> ImportFactory.getImporter(copied, query, rows)).thenReturn(importer);
			Map<String, Object> config = Map.of("source", "${left}", "rightSource", "${right}",
					"operation", "join", "column", "ID", "rightColumn", "ID", "joinType", "inner.join");
			Map<String, Object> scope = Map.of("left", registeredFrame("left", left),
					"right", registeredFrame("right", right));
			if (outcome == JoinOutcome.SUCCESS) {
				AutomationFrameOutput.RegisteredFrame output = AutomationCleanExecutor.execute(insight, "joined",
						AutomationNodeType.DATA_TRANSFORM, config, scope, "join");
				assertSame(result, output.frame());
				assertSame(result, insight.getVarStore().get("joined").getValue());
				verify(result, never()).close();
				verify(copied, never()).close();
			} else {
				IllegalStateException error = assertThrows(IllegalStateException.class,
						() -> AutomationCleanExecutor.execute(insight, "joined", AutomationNodeType.DATA_TRANSFORM,
								config, scope, "join"));
				assertTrue(error.getMessage().startsWith("Unable to join tables for node 'join':"));
				assertNotNull(error.getCause());
				verify(result).close();
				if (replacementWrapper) {
					verify(copied, never()).close();
				}
				assertNull(insight.getVarStore().get("joined"));
				assertFalse(insight.getVarStore().getAllCreatedFrames().contains(result));
			}
			ArgumentCaptor<NounStore> nouns = ArgumentCaptor.forClass(NounStore.class);
			verify(reactors.constructed().get(0)).setNounStore(nouns.capture());
			assertSame(rightQuery, nouns.getValue().getNoun(ReactorKeysEnum.QUERY_STRUCT.getKey()).get(0));
			assertFalse(query.isDistinct());
			assertFalse(rightQuery.isDistinct());
			verify(importer).insertData();
			verify(rows).close();
			verify(left, never()).close();
			verify(right, never()).close();
			assertSame(left, insight.getVarStore().get("left").getValue());
			assertSame(right, insight.getVarStore().get("right").getValue());
		}
	}

	/** Creates frame metadata without invoking Python or a platform engine. */
	private static PandasFrame pythonFrame(String name) {
		PandasFrame frame = mock(PandasFrame.class);
		OwlTemporalEngineMeta metadata = mock(OwlTemporalEngineMeta.class);
		when(frame.getName()).thenReturn(name);
		when(frame.getWrapperName()).thenReturn(name + "w");
		when(frame.getFrameType()).thenReturn(DataFrameTypeEnum.PYTHON);
		when(frame.getColumnHeaders()).thenReturn(new String[] { "ID", "NAME" });
		when(frame.getQsHeaders()).thenReturn(new String[] { name + "__ID", name + "__NAME" });
		when(frame.getMetaData()).thenReturn(metadata);
		when(metadata.getHeaderTypeAsString(any(String.class))).thenReturn("STRING");
		when(metadata.getFlatTableQs(false)).thenReturn(new SelectQueryStruct());
		when(frame.getFrameFilters()).thenReturn(new GenRowFilters());
		when(frame.size(name)).thenReturn(2L);
		return frame;
	}

	private static AutomationFrameOutput.RegisteredFrame registeredFrame(String name, ITableDataFrame frame) {
		return new AutomationFrameOutput.RegisteredFrame(name, frame,
				Map.of("rowCount", 2L, "columnCount", 2, "dataType", "table"), "PY");
	}

	private void exerciseCopy(boolean fail, boolean replaceFrame) throws Exception {
		exerciseCopy(fail, replaceFrame, 2L);
	}

	private void exerciseCopy(boolean fail, boolean replaceFrame, long resultRows) throws Exception {
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
			return resultRows;
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
			boolean oversized = resultRows > AutomationConstants.SAVED_RESULT_MAX_ROWS;
			if (fail || oversized) {
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
				assertEquals(resultRows, output.frame().size("cleaned"));
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
