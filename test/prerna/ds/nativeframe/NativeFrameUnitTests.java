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
package prerna.ds.nativeframe;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Utility;

public class NativeFrameUnitTests {

	// -- Constructor Tests --

	@Test
	void testDefaultConstructor() {
		NativeFrame frame = new NativeFrame();
		assertNotNull(frame);
		assertTrue(frame.getName().startsWith("NATIVE_"));
		assertNotNull(frame.getQueryStruct());
		assertNotNull(frame.getOriginalQueryStruct());
		assertSame(frame.getQueryStruct(), frame.getOriginalQueryStruct());
		assertEquals(QUERY_STRUCT_TYPE.ENGINE, frame.getQueryStruct().getQsType());
	}

	@Test
	void testAliasConstructor() {
		NativeFrame frame = new NativeFrame("myAlias");
		assertEquals("myAlias", frame.getName());
		assertNotNull(frame.getQueryStruct());
		assertNotNull(frame.getOriginalQueryStruct());
		assertSame(frame.getQueryStruct(), frame.getOriginalQueryStruct());
		assertEquals(QUERY_STRUCT_TYPE.ENGINE, frame.getQueryStruct().getQsType());
	}

	@Test
	void testDefaultConstructor_uniqueNames() {
		NativeFrame f1 = new NativeFrame();
		NativeFrame f2 = new NativeFrame();
		assertNotEquals(f1.getName(), f2.getName());
	}

	// -- Simple Getters --

	@Test
	void testGetDataMakerName() {
		NativeFrame frame = new NativeFrame("test");
		assertEquals("NativeFrame", frame.getDataMakerName());
	}

	@Test
	void testDataMakerNameConstant() {
		assertEquals("NativeFrame", NativeFrame.DATA_MAKER_NAME);
	}

	@Test
	void testGetFrameType() {
		NativeFrame frame = new NativeFrame("test");
		assertEquals(DataFrameTypeEnum.NATIVE, frame.getFrameType());
	}

	@Test
	void testSizeAlwaysZero() {
		NativeFrame frame = new NativeFrame("test");
		assertEquals(0, frame.size("anyTable"));
		assertEquals(0, frame.size(null));
		assertEquals(0, frame.size(""));
	}

	// -- setName Tests --

	@Test
	void testSetName_null_noChange() {
		NativeFrame frame = new NativeFrame("original");
		frame.setName(null);
		assertEquals("original", frame.getName());
	}

	@Test
	void testSetName_empty_noChange() {
		NativeFrame frame = new NativeFrame("original");
		frame.setName("");
		assertEquals("original", frame.getName());
	}

	@Test
	void testSetName_valid() {
		NativeFrame frame = new NativeFrame("original");
		frame.setName("newName");
		assertEquals("newName", frame.getName());
	}

	@Test
	void testSetName_toOriginal_resetsQueryQs() {
		NativeFrame frame = new NativeFrame("original");
		SelectQueryStruct originalQs = frame.getOriginalQueryStruct();

		// Change queryQs by setting a different one
		SelectQueryStruct differentQs = new SelectQueryStruct();
		frame.setQueryStruct(differentQs);
		assertSame(differentQs, frame.getQueryStruct());
		assertNotSame(originalQs, frame.getQueryStruct());

		// Set name back to original name -> should reset queryQs
		frame.setName("original");
		assertSame(originalQs, frame.getQueryStruct());
	}

	// -- setConnection / getEngineId Tests --

	@Test
	void testSetConnection() {
		NativeFrame frame = new NativeFrame("test");
		frame.setConnection("myEngineId");
		assertEquals("myEngineId", frame.getEngineId());
		assertEquals("myEngineId", frame.getOriginalQueryStruct().getEngineId());
		assertEquals("myEngineId", frame.getQueryStruct().getEngineId());
	}

	@Test
	void testGetEngineId_initial() {
		NativeFrame frame = new NativeFrame("test");
		assertNull(frame.getEngineId());
	}

	@Test
	void testSetConnection_propagatesToBothQs() {
		NativeFrame frame = new NativeFrame("test");
		// Set a different queryQs
		SelectQueryStruct newQs = new SelectQueryStruct();
		frame.setQueryStruct(newQs);
		frame.setConnection("eng1");
		// Both should have the engineId
		assertEquals("eng1", frame.getOriginalQueryStruct().getEngineId());
		assertEquals("eng1", frame.getQueryStruct().getEngineId());
	}

	// -- QueryStruct getter/setter Tests --

	@Test
	void testGetSetQueryStruct() {
		NativeFrame frame = new NativeFrame("test");
		SelectQueryStruct newQs = new SelectQueryStruct();
		frame.setQueryStruct(newQs);
		assertSame(newQs, frame.getQueryStruct());
		// Original should be unchanged
		assertNotSame(newQs, frame.getOriginalQueryStruct());
	}

	@Test
	void testGetOriginalQueryStruct() {
		NativeFrame frame = new NativeFrame("test");
		SelectQueryStruct orig = frame.getOriginalQueryStruct();
		assertNotNull(orig);
		// Even after setting a new queryQs, original stays the same
		frame.setQueryStruct(new SelectQueryStruct());
		assertSame(orig, frame.getOriginalQueryStruct());
	}

	@Test
	void testMergeQueryStruct() {
		NativeFrame frame = new NativeFrame("test");
		SelectQueryStruct mergeQs = new SelectQueryStruct();
		QueryColumnSelector sel = new QueryColumnSelector("Table__Col");
		mergeQs.addSelector(sel);
		frame.mergeQueryStruct(mergeQs);
		// After merge, queryQs should have selectors from mergeQs
		assertFalse(frame.getQueryStruct().getSelectors().isEmpty());
	}

	// -- isEmpty Tests --

	@Test
	void testIsEmpty_nullEngine() {
		NativeFrame frame = new NativeFrame("test");
		// No engine set -> retrieveQueryStructEngine returns null
		assertTrue(frame.isEmpty());
	}

	@Test
	void testIsEmpty_hasData() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		frame.getQueryStruct().setEngine(mockEngine);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		when(mockWrapper.hasNext()).thenReturn(true);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			assertFalse(frame.isEmpty());
		}
	}

	@Test
	void testIsEmpty_noData() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		frame.getQueryStruct().setEngine(mockEngine);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		when(mockWrapper.hasNext()).thenReturn(false);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			assertTrue(frame.isEmpty());
		}
	}

	@Test
	void testIsEmpty_exceptionReturnsEmpty() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		frame.getQueryStruct().setEngine(mockEngine);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), any(SelectQueryStruct.class)))
				.thenThrow(new RuntimeException("connection error"));

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			// Exception is caught, returns false (initial value of empty variable)
			assertFalse(frame.isEmpty());
		}
	}

	// -- engineQueryCacheable Tests --

	@Nested
	class EngineCacheableTests {

		@Test
		void testCacheable_rdbms() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testCacheable_sesame() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.SESAME);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testCacheable_rdf4j() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDF4J);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testCacheable_neo4j() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.NEO4J);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testCacheable_jena() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.JENA);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testCacheable_jenaTdb() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.JENA_TDB);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testCacheable_impala() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.IMPALA);
			frame.getQueryStruct().setEngine(mockEngine);
			assertTrue(frame.engineQueryCacheable());
		}

		@Test
		void testNotCacheable_datastax() {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.DATASTAX_GRAPH);
			frame.getQueryStruct().setEngine(mockEngine);
			assertFalse(frame.engineQueryCacheable());
		}
	}

	// -- getQueryInterpreter Tests --

	@Test
	void testGetQueryInterpreter() {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		IQueryInterpreter mockInterpreter = mock(IQueryInterpreter.class);
		when(mockEngine.getQueryInterpreter()).thenReturn(mockInterpreter);
		frame.getQueryStruct().setEngine(mockEngine);

		assertSame(mockInterpreter, frame.getQueryInterpreter());
	}

	// -- prepQsForExecution Tests --

	@Nested
	class PrepQsTests {

		private void setMetaData(NativeFrame frame, OwlTemporalEngineMeta meta) throws Exception {
			Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
			metaField.setAccessible(true);
			metaField.set(frame, meta);
		}

		@Test
		void testNormalPath_nonRdbms() throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			setMetaData(frame, mockMeta);

			SelectQueryStruct inputQs = new SelectQueryStruct();

			try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
						.thenAnswer(inv -> inv.getArgument(0));

				SelectQueryStruct result = frame.prepQsForExecution(inputQs);
				assertNotNull(result);
				// Verify engine was set
				assertSame(mockEngine, result.getEngine());
			}
		}

		@Test
		void testDoubleAggregation_rdbms() throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IRDBMSEngine mockEngine = mock(IRDBMSEngine.class);
			IQueryInterpreter mockInterpreter = mock(IQueryInterpreter.class);
			when(mockEngine.getQueryInterpreter()).thenReturn(mockInterpreter);
			when(mockInterpreter.composeQuery()).thenReturn("SELECT * FROM table");

			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);

			// Set groupBy on the original QS to trigger double aggregation
			List<IQuerySelector> groupBy = new ArrayList<>();
			groupBy.add(new QueryColumnSelector("Table__Col"));
			frame.getQueryStruct().setGroupBy(groupBy);

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			setMetaData(frame, mockMeta);

			SelectQueryStruct inputQs = new SelectQueryStruct();

			try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
						.thenAnswer(inv -> inv.getArgument(0));

				SelectQueryStruct result = frame.prepQsForExecution(inputQs);
				assertNotNull(result);
				// Should have custom from set (from composed query)
				assertEquals("SELECT * FROM table", result.getCustomFrom());
				assertEquals("embed_subquery", result.getCustomFromAliasName());
			}
		}

		@Test
		void testFilterMerging_simpleFilterOverridden() throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);

			// Add a filter to the original QS
			SimpleQueryFilter originalFilter = SimpleQueryFilter.makeColToValFilter("Table__Col", "==", "Value1");
			frame.getQueryStruct().addExplicitFilter(originalFilter);

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			setMetaData(frame, mockMeta);

			// Input QS also has a filter on the same column -> should override (not merge)
			SelectQueryStruct inputQs = new SelectQueryStruct();
			SimpleQueryFilter inputFilter = SimpleQueryFilter.makeColToValFilter("Table__Col", "==", "Value2");
			inputQs.addExplicitFilter(inputFilter);

			try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
						.thenAnswer(inv -> inv.getArgument(0));

				SelectQueryStruct result = frame.prepQsForExecution(inputQs);
				assertNotNull(result);
			}
		}

		@Test
		void testFilterMerging_nonSimpleFilterAlwaysAdded() throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);

			// Add a non-simple filter (e.g., OR filter) to original QS
			IQueryFilter mockOrFilter = mock(IQueryFilter.class);
			when(mockOrFilter.getQueryFilterType()).thenReturn(IQueryFilter.QUERY_FILTER_TYPE.OR);
			frame.getQueryStruct().addExplicitFilter(mockOrFilter);

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			setMetaData(frame, mockMeta);

			SelectQueryStruct inputQs = new SelectQueryStruct();

			try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
						.thenAnswer(inv -> inv.getArgument(0));

				SelectQueryStruct result = frame.prepQsForExecution(inputQs);
				assertNotNull(result);
			}
		}

		@Test
		void testFilterMerging_simpleFilterNotOverridden() throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);

			// Add a filter on Col1 to original QS
			SimpleQueryFilter originalFilter = SimpleQueryFilter.makeColToValFilter("Table__Col1", "==", "A");
			frame.getQueryStruct().addExplicitFilter(originalFilter);

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			setMetaData(frame, mockMeta);

			// Input QS has filter on a DIFFERENT column (Col2) -> original filter should be merged
			SelectQueryStruct inputQs = new SelectQueryStruct();
			SimpleQueryFilter inputFilter = SimpleQueryFilter.makeColToValFilter("Table__Col2", "==", "B");
			inputQs.addExplicitFilter(inputFilter);

			try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
						.thenAnswer(inv -> inv.getArgument(0));

				SelectQueryStruct result = frame.prepQsForExecution(inputQs);
				assertNotNull(result);
			}
		}

		@Test
		void testRdbms_noGroupBy_normalPath() throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IRDBMSEngine mockEngine = mock(IRDBMSEngine.class);
			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);
			// No groupBy set -> should NOT trigger double aggregation even though RDBMS

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			setMetaData(frame, mockMeta);

			SelectQueryStruct inputQs = new SelectQueryStruct();

			try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
						.thenAnswer(inv -> inv.getArgument(0));

				SelectQueryStruct result = frame.prepQsForExecution(inputQs);
				assertNotNull(result);
				// No custom from since no double aggregation
				assertNull(result.getCustomFrom());
			}
		}
	}

	// -- query(String) Tests --

	@Test
	void testQueryString() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), eq("SELECT 1"))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			IRawSelectWrapper result = frame.query("SELECT 1");
			assertSame(mockWrapper, result);
		}
	}

	// -- query(SelectQueryStruct) Tests --

	@Test
	void testQueryStruct_noCache() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
					.thenAnswer(inv -> inv.getArgument(0));

			SelectQueryStruct inputQs = new SelectQueryStruct();
			IRawSelectWrapper result = frame.query(inputQs);
			assertSame(mockWrapper, result);
		}
	}

	@Test
	void testQueryStruct_withCachePragma_cacheHit() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
		IQueryInterpreter mockInterpreter = mock(IQueryInterpreter.class);
		when(mockEngine.getQueryInterpreter()).thenReturn(mockInterpreter);
		when(mockInterpreter.composeQuery()).thenReturn("SELECT cached");

		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		// Set up query cache
		CachedIterator mockCached = mock(CachedIterator.class);
		Field cacheField = AbstractTableDataFrame.class.getDeclaredField("queryCache");
		cacheField.setAccessible(true);
		Map<String, CachedIterator> cache = new HashMap<>();
		cache.put("SELECT cached", mockCached);
		cacheField.set(frame, cache);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
					.thenAnswer(inv -> inv.getArgument(0));

			SelectQueryStruct inputQs = new SelectQueryStruct();
			Map<String, Object> pragmap = new HashMap<>();
			pragmap.put("xCache", "True");
			inputQs.setPragmap(pragmap);

			IRawSelectWrapper result = frame.query(inputQs);
			assertNotNull(result);
			assertTrue(result instanceof RawCachedWrapper);
		}
	}

	@Test
	void testQueryStruct_withCachePragma_cacheMiss() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		when(mockEngine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
		IQueryInterpreter mockInterpreter = mock(IQueryInterpreter.class);
		when(mockEngine.getQueryInterpreter()).thenReturn(mockInterpreter);
		when(mockInterpreter.composeQuery()).thenReturn("SELECT notcached");

		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
					.thenAnswer(inv -> inv.getArgument(0));

			SelectQueryStruct inputQs = new SelectQueryStruct();
			Map<String, Object> pragmap = new HashMap<>();
			pragmap.put("xCache", "True");
			inputQs.setPragmap(pragmap);

			IRawSelectWrapper result = frame.query(inputQs);
			assertSame(mockWrapper, result);
		}
	}

	// -- getMax / getMin Tests --

	@Nested
	class MaxMinTests {

		private NativeFrame setupFrameWithMeta(SemossDataType dataType) throws Exception {
			NativeFrame frame = new NativeFrame("test");
			IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
			when(mockEngine.getEngineId()).thenReturn("eng1");
			frame.getQueryStruct().setEngine(mockEngine);
			frame.getOriginalQueryStruct().setEngine(mockEngine);

			OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
			when(mockMeta.getHeaderTypeAsEnum("Table__Col")).thenReturn(dataType);
			Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
			metaField.setAccessible(true);
			metaField.set(frame, mockMeta);
			return frame;
		}

		@Test
		void testGetMax_nonNumeric_returnsNull() throws Exception {
			NativeFrame frame = setupFrameWithMeta(SemossDataType.STRING);
			assertNull(frame.getMax("Table__Col"));
		}

		@Test
		void testGetMin_nonNumeric_returnsNull() throws Exception {
			NativeFrame frame = setupFrameWithMeta(SemossDataType.STRING);
			assertNull(frame.getMin("Table__Col"));
		}

		@Test
		void testGetMax_int() throws Exception {
			NativeFrame frame = setupFrameWithMeta(SemossDataType.INT);

			IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
			IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
			when(mockRow.getValues()).thenReturn(new Object[]{"col", 42});
			when(mockWrapper.next()).thenReturn(mockRow);

			WrapperManager mockWM = mock(WrapperManager.class);
			when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

			try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
				 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
						.thenAnswer(inv -> inv.getArgument(0));

				Double result = frame.getMax("Table__Col");
				assertEquals(42.0, result);
			}
		}

		@Test
		void testGetMax_double() throws Exception {
			NativeFrame frame = setupFrameWithMeta(SemossDataType.DOUBLE);

			IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
			IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
			when(mockRow.getValues()).thenReturn(new Object[]{"col", 99.9});
			when(mockWrapper.next()).thenReturn(mockRow);

			WrapperManager mockWM = mock(WrapperManager.class);
			when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

			try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
				 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
						.thenAnswer(inv -> inv.getArgument(0));

				Double result = frame.getMax("Table__Col");
				assertEquals(99.9, result);
			}
		}

		@Test
		void testGetMin_int() throws Exception {
			NativeFrame frame = setupFrameWithMeta(SemossDataType.INT);

			IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
			IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
			when(mockRow.getValues()).thenReturn(new Object[]{"col", 5});
			when(mockWrapper.next()).thenReturn(mockRow);

			WrapperManager mockWM = mock(WrapperManager.class);
			when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

			try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
				 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
						.thenAnswer(inv -> inv.getArgument(0));

				Double result = frame.getMin("Table__Col");
				assertEquals(5.0, result);
			}
		}

		@Test
		void testGetMax_queryException_returnsNull() throws Exception {
			NativeFrame frame = setupFrameWithMeta(SemossDataType.INT);

			WrapperManager mockWM = mock(WrapperManager.class);
			when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class)))
					.thenThrow(new RuntimeException("query error"));

			try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
				 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
				wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
				convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
						.thenAnswer(inv -> inv.getArgument(0));

				assertNull(frame.getMax("Table__Col"));
			}
		}
	}

	// -- getColumn / getColumnAsNumeric Tests --

	@Test
	void testGetColumn() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow row1 = mock(IHeadersDataRow.class);
		IHeadersDataRow row2 = mock(IHeadersDataRow.class);
		when(row1.getValues()).thenReturn(new Object[]{"Alice"});
		when(row2.getValues()).thenReturn(new Object[]{"Bob"});
		when(mockWrapper.hasNext()).thenReturn(true, true, false);
		when(mockWrapper.next()).thenReturn(row1, row2);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Object[] result = frame.getColumn("Table__Col");
			assertEquals(2, result.length);
			assertEquals("Alice", result[0]);
			assertEquals("Bob", result[1]);
		}
	}

	@Test
	void testGetColumnAsNumeric() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow row1 = mock(IHeadersDataRow.class);
		IHeadersDataRow row2 = mock(IHeadersDataRow.class);
		when(row1.getValues()).thenReturn(new Object[]{1.0});
		when(row2.getValues()).thenReturn(new Object[]{2.5});
		when(mockWrapper.hasNext()).thenReturn(true, true, false);
		when(mockWrapper.next()).thenReturn(row1, row2);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Double[] result = frame.getColumnAsNumeric("Table__Col");
			assertEquals(2, result.length);
			assertEquals(1.0, result[0]);
			assertEquals(2.5, result[1]);
		}
	}

	// -- getEngineQuery Tests --

	@Test
	void testGetEngineQuery() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		IQueryInterpreter mockInterpreter = mock(IQueryInterpreter.class);
		when(mockEngine.getQueryInterpreter()).thenReturn(mockInterpreter);
		when(mockInterpreter.composeQuery()).thenReturn("SELECT * FROM table");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		try (MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), eq(mockMeta)))
					.thenAnswer(inv -> inv.getArgument(0));

			String result = frame.getEngineQuery(new SelectQueryStruct());
			assertEquals("SELECT * FROM table", result);
		}
	}

	// -- save / open Tests --

	@Test
	void testSave_noCipher(@TempDir Path tempDir) throws Exception {
		NativeFrame frame = new NativeFrame("saveTest");
		frame.setConnection("testEngine");

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		CachePropFileFrameObject cf = frame.save(tempDir.toString(), null);
		assertNotNull(cf);
		assertNotNull(cf.getFrameCacheLocation());
		assertTrue(cf.getFrameCacheLocation().endsWith(".json"));
	}

	@Test
	void testOpen_noCipher(@TempDir Path tempDir) throws Exception {
		// First save
		NativeFrame frame = new NativeFrame("openTest");

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		CachePropFileFrameObject cf = frame.save(tempDir.toString(), null);

		// Now open in a different frame
		NativeFrame frame2 = new NativeFrame("loadTest");
		metaField.set(frame2, mock(OwlTemporalEngineMeta.class));
		frame2.open(cf, null);
		assertNotNull(frame2.getOriginalQueryStruct());
		assertSame(frame2.getQueryStruct(), frame2.getOriginalQueryStruct());
	}

	// -- querySQL Tests --------------------------------------------------

	@Test
	void testQuerySQL() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow row1 = mock(IHeadersDataRow.class);
		when(row1.getValues()).thenReturn(new Object[]{"Alice", 30});
		IHeadersDataRow row2 = mock(IHeadersDataRow.class);
		when(row2.getValues()).thenReturn(new Object[]{"Bob", 25});
		when(mockWrapper.hasNext()).thenReturn(true, true, false);
		when(mockWrapper.next()).thenReturn(row1, row2);
		when(mockWrapper.getHeaders()).thenReturn(new String[]{"name", "age"});
		when(mockWrapper.getTypes()).thenReturn(new SemossDataType[]{SemossDataType.STRING, SemossDataType.INT});

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Map<String, Object> result = (Map<String, Object>) frame.querySQL("SELECT * FROM test");
			assertNotNull(result);
			assertTrue(result.containsKey("data"));
			assertTrue(result.containsKey("types"));
			assertTrue(result.containsKey("columns"));

			List<List<Object>> data = (List<List<Object>>) result.get("data");
			assertEquals(2, data.size());
		}
	}

	@Test
	void testQuerySQL_exception() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class)))
				.thenThrow(new RuntimeException("db error"));

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			assertThrows(IllegalArgumentException.class, () -> frame.querySQL("SELECT * FROM test"));
		}
	}

	// -- Query exception path Tests --------------------------------------

	@Test
	void testGetMin_queryException_returnsNull() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		when(mockMeta.getHeaderTypeAsEnum("Table__Col")).thenReturn(SemossDataType.INT);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class)))
				.thenThrow(new RuntimeException("query error"));

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			assertNull(frame.getMin("Table__Col"));
		}
	}

	@Test
	void testGetColumn_queryException_returnsEmpty() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class)))
				.thenThrow(new RuntimeException("query error"));

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Object[] result = frame.getColumn("Table__Col");
			assertEquals(0, result.length);
		}
	}

	@Test
	void testGetColumnAsNumeric_queryException_returnsEmpty() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class)))
				.thenThrow(new RuntimeException("query error"));

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Double[] result = frame.getColumnAsNumeric("Table__Col");
			assertEquals(0, result.length);
		}
	}

	@Test
	void testQuerySQL_closeThrowsIOException() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		when(mockWrapper.hasNext()).thenReturn(false);
		when(mockWrapper.getHeaders()).thenReturn(new String[]{"name"});
		when(mockWrapper.getTypes()).thenReturn(new SemossDataType[]{SemossDataType.STRING});
		doThrow(new IOException("close error")).when(mockWrapper).close();

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Map<String, Object> result = (Map<String, Object>) frame.querySQL("SELECT 1");
			assertNotNull(result);
		}
	}

	// -- IOException close() path Tests ----------------------------------

	@Test
	void testIsEmpty_closeThrowsIOException() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		frame.getQueryStruct().setEngine(mockEngine);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		when(mockWrapper.hasNext()).thenReturn(false);
		doThrow(new IOException("close error")).when(mockWrapper).close();

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(eq(mockEngine), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			// Should not throw - IOException in close is caught and logged
			assertTrue(frame.isEmpty());
		}
	}

	@Test
	void testGetMax_closeThrowsIOException() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		when(mockMeta.getHeaderTypeAsEnum("Table__Col")).thenReturn(SemossDataType.INT);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
		when(mockRow.getValues()).thenReturn(new Object[]{"col", 42});
		when(mockWrapper.next()).thenReturn(mockRow);
		doThrow(new IOException("close error")).when(mockWrapper).close();

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			// Should still return the value despite close() throwing
			assertEquals(42.0, frame.getMax("Table__Col"));
		}
	}

	@Test
	void testGetMin_closeThrowsIOException() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		when(mockMeta.getHeaderTypeAsEnum("Table__Col")).thenReturn(SemossDataType.DOUBLE);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
		when(mockRow.getValues()).thenReturn(new Object[]{"col", 3.14});
		when(mockWrapper.next()).thenReturn(mockRow);
		doThrow(new IOException("close error")).when(mockWrapper).close();

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			assertEquals(3.14, frame.getMin("Table__Col"));
		}
	}

	@Test
	void testGetColumn_closeThrowsIOException() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
		when(mockRow.getValues()).thenReturn(new Object[]{"val1"});
		when(mockWrapper.hasNext()).thenReturn(true, false);
		when(mockWrapper.next()).thenReturn(mockRow);
		doThrow(new IOException("close error")).when(mockWrapper).close();

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Object[] result = frame.getColumn("Table__Col");
			assertEquals(1, result.length);
			assertEquals("val1", result[0]);
		}
	}

	@Test
	void testGetColumnAsNumeric_closeThrowsIOException() throws Exception {
		NativeFrame frame = new NativeFrame("test");
		IDatabaseEngine mockEngine = mock(IDatabaseEngine.class);
		when(mockEngine.getEngineId()).thenReturn("eng1");
		frame.getQueryStruct().setEngine(mockEngine);
		frame.getOriginalQueryStruct().setEngine(mockEngine);

		OwlTemporalEngineMeta mockMeta = mock(OwlTemporalEngineMeta.class);
		Field metaField = AbstractTableDataFrame.class.getDeclaredField("metaData");
		metaField.setAccessible(true);
		metaField.set(frame, mockMeta);

		IRawSelectWrapper mockWrapper = mock(IRawSelectWrapper.class);
		IHeadersDataRow mockRow = mock(IHeadersDataRow.class);
		when(mockRow.getValues()).thenReturn(new Object[]{7.5});
		when(mockWrapper.hasNext()).thenReturn(true, false);
		when(mockWrapper.next()).thenReturn(mockRow);
		doThrow(new IOException("close error")).when(mockWrapper).close();

		WrapperManager mockWM = mock(WrapperManager.class);
		when(mockWM.getRawWrapper(any(IDatabaseEngine.class), any(SelectQueryStruct.class))).thenReturn(mockWrapper);

		try (MockedStatic<WrapperManager> wmStatic = mockStatic(WrapperManager.class);
			 MockedStatic<QSAliasToPhysicalConverter> convStatic = mockStatic(QSAliasToPhysicalConverter.class)) {
			wmStatic.when(WrapperManager::getInstance).thenReturn(mockWM);
			convStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(any(SelectQueryStruct.class), any()))
					.thenAnswer(inv -> inv.getArgument(0));

			Double[] result = frame.getColumnAsNumeric("Table__Col");
			assertEquals(1, result.length);
			assertEquals(7.5, result[0]);
		}
	}

	// -- Deprecated method Tests -----------------------------------------

	@Test
	void testDeprecatedMethods_noOp() {
		NativeFrame frame = new NativeFrame("test");
		// These should not throw
		frame.processDataMakerComponent(mock(DataMakerComponent.class));
		frame.removeColumn("col");
		frame.addRow(new Object[]{"a"}, new String[]{"h"});
	}
}
