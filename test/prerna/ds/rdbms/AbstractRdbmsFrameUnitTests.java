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
package prerna.ds.rdbms;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.query.querystruct.transform.QSRenameTableConverter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.reactor.imports.ImportUtility;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;

import org.apache.logging.log4j.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.crypto.Cipher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

// import prerna.frame.imports.ImportUtility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AbstractRdbmsFrameUnitTests {
	private AbstractRdbmsFrame frame;
	private RdbmsFrameBuilder builder;
	private IQueryInterpreter interpreter;
	private AbstractSqlQueryUtil util;	
	private Connection conn;
	private OwlTemporalEngineMeta owl;

	private static final UUID FIXED_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

	@BeforeEach
	void setup() {
		conn =  mock(Connection.class);
		util = mock(AbstractSqlQueryUtil.class);
		builder = mock(RdbmsFrameBuilder.class);
		owl = mock(OwlTemporalEngineMeta.class);
		interpreter  = mock(IQueryInterpreter.class);

		frame = new AbstractRdbmsFrame() {
			@Override
			public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
				return null;
			}

			@Override
			public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
			}

			@Override
			protected void initConnAndBuilder() throws Exception {
			}

			@Override
			public IQueryInterpreter getQueryInterpreter() {
				return interpreter;
			}
		};

		frame.conn = conn;
		frame.util = util;
		frame.builder = builder;
	}

	// Constructors
	@Nested
	class InnerAbstractRdbmsFrameUnitTests {
		@Test
		void constuctor() throws Exception {
			try (MockedStatic<UUID> uuidStatic = Mockito.mockStatic(UUID.class)) {
				uuidStatic.when(() -> UUID.randomUUID()).thenReturn(FIXED_UUID);

				frame = new AbstractRdbmsFrame() {
					@Override
					public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
						return null;
					}

					@Override
					public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
					}

					@Override
					protected void initConnAndBuilder() throws Exception {
					}

					@Override
					public IQueryInterpreter getQueryInterpreter() {
						return interpreter;
					}
				};
				assertEquals("RDBMSFRAME_00000000_0000_0000_0000_000000000000", frame.getOriginalName());
			}
		}

		@Test
		void constuctorTableName() throws Exception {
			try (MockedStatic<UUID> uuidStatic = Mockito.mockStatic(UUID.class)) {
				uuidStatic.when(() -> UUID.randomUUID()).thenReturn(FIXED_UUID);

				frame = new AbstractRdbmsFrame("tableName") {
					@Override
					public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
						return null;
					}

					@Override
					public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
					}

					@Override
					protected void initConnAndBuilder() throws Exception {
					}

					@Override
					public IQueryInterpreter getQueryInterpreter() {
						return interpreter;
					}
				};
				assertEquals("tableName", frame.getOriginalName());

				frame = new AbstractRdbmsFrame("") {
					@Override
					public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
						return null;
					}

					@Override
					public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
					}

					@Override
					protected void initConnAndBuilder() throws Exception {
					}

					@Override
					public IQueryInterpreter getQueryInterpreter() {
						return interpreter;
					}
				};
				assertEquals("RDBMSFRAME_00000000_0000_0000_0000_000000000000", frame.getOriginalName());
			}
		}

		@Test
		void constuctorHeaders() throws Exception {
			String[] headers = {"col1"};

			try (MockedStatic<UUID> uuidStatic = Mockito.mockStatic(UUID.class)) {
				uuidStatic.when(() -> UUID.randomUUID()).thenReturn(FIXED_UUID);
				doNothing().when(builder).alterTableNewColumns(eq("RDBMSFRAME_UUID"), eq(headers), any(String[].class));

				frame = new AbstractRdbmsFrame(headers) {
					@Override
					public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
						return null;
					}

					@Override
					public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
					}

					@Override
					protected void initConnAndBuilder() throws Exception {
						builder = AbstractRdbmsFrameUnitTests.this.builder;
					}

					@Override
					public IQueryInterpreter getQueryInterpreter() {
						return interpreter;
					}
				};
				assertEquals("RDBMSFRAME_00000000_0000_0000_0000_000000000000", frame.getOriginalName());
			}
		}

		@Test
		void constuctorHeadersTypes() throws Exception {
			String[] headers = {"col1"};
			String[] types = {"STRING"};

			try (MockedStatic<UUID> uuidStatic = Mockito.mockStatic(UUID.class);
				MockedStatic<ImportUtility> util = Mockito.mockStatic(ImportUtility.class)) {
				uuidStatic.when(() -> UUID.randomUUID()).thenReturn(FIXED_UUID);
				doNothing().when(builder).alterTableNewColumns(eq("RDBMSFRAME_UUID"), eq(headers), eq(types));

				frame = new AbstractRdbmsFrame(headers, types) {
					@Override
					public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
						return null;
					}

					@Override
					public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
					}

					@Override
					protected void initConnAndBuilder() throws Exception {
						builder = AbstractRdbmsFrameUnitTests.this.builder;
					}

					@Override
					public IQueryInterpreter getQueryInterpreter() {
						return interpreter;
					}
				};
				assertEquals("RDBMSFRAME_00000000_0000_0000_0000_000000000000", frame.getOriginalName());
			}
		}
	}

	@Test
	void getters() {
		assertNotNull(frame.getQueryUtil());
		assertNotNull(frame.getBuilder());
		assertNotNull(frame.getConn());
		assertNull(frame.getDataMakerName());

		assertEquals(DataFrameTypeEnum.GRID, frame.getFrameType());
	}

	@Test
	void addRowsViaIterator() throws Exception {
		Map<String, SemossDataType> typesMap = new HashMap(){{put("col1", SemossDataType.INT);}};
		RawRDBMSSelectWrapper wrapper = mock(RawRDBMSSelectWrapper.class);

		doNothing().when(builder).addRowsViaIterator(eq(wrapper), anyString(), eq(typesMap));
		frame.addRowsViaIterator(wrapper, typesMap);

		verify(builder, times(1)).addRowsViaIterator(eq(wrapper), anyString(), eq(typesMap));
	}

	@Test
	void addRow() {
		String[] cols = {"colName"};
		Object[] vals = {"foo"};
		String[] types = {"STRING"};
		OwlTemporalEngineMeta owl = mock(OwlTemporalEngineMeta.class);

		when(owl.getHeaderTypeAsString(eq(cols[0]), anyString())).thenReturn(null);
		doNothing().when(builder).addRow(anyString(), eq(cols), eq(vals), eq(types));

		frame.setMetaData(owl);
		frame.addRow(vals, cols);

		verify(owl, times(1)).getHeaderTypeAsString(eq(cols[0]), anyString());
		verify(builder, times(1)).addRow(anyString(), eq(cols), eq(vals), eq(types));
	}

	@Test
	void addNewColumn() {
		String tableName = "table";
		String[] headers = {"col1"};
		String[] types = {"STRING"};
		// OwlTemporalEngineMeta owl = mock(OwlTemporalEngineMeta.class);

		doNothing().when(builder).alterTableNewColumns(tableName, headers, types);
		doNothing().when(owl).addProperty(tableName, tableName + "__" + headers[0]);
		doNothing().when(owl).setAliasToProperty(tableName + "__" + headers[0], headers[0]);
		doNothing().when(owl).setDataTypeToProperty(tableName + "__" + headers[0], types[0]);

		frame.setMetaData(owl);
		frame.addNewColumn(headers, types, tableName);

		verify(builder, times(1)).alterTableNewColumns(tableName, headers, types);
		verify(owl, times(1)).addProperty(tableName, tableName + "__" + headers[0]);
		verify(owl, times(1)).setAliasToProperty(tableName + "__" + headers[0], headers[0]);
		verify(owl, times(1)).setDataTypeToProperty(tableName + "__" + headers[0], types[0]);
	}

	@Test
	void removeColumn() throws Exception {
		String headers = "col1";
		
		when(util.allowDropColumn()).thenReturn(true);
		when(util.alterTableDropColumn(anyString(), eq(headers))).thenReturn("DROP COL1 FROM TABLENAME");

		doNothing().doThrow(SQLException.class).when(builder).runQuery("DROP COL1 FROM TABLENAME");

		doNothing().when(owl).dropProperty(anyString(), anyString());

		frame.setMetaData(owl);
		frame.removeColumn(headers);
		frame.removeColumn(headers);

		verify(builder, times(2)).runQuery("DROP COL1 FROM TABLENAME");
		verify(owl, times(2)).dropProperty(anyString(), anyString());
	}

	@Test
	void queryString() throws Exception {
		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();

		try (MockedStatic<RawRDBMSSelectWrapper> wrapperStatic = Mockito.mockStatic(RawRDBMSSelectWrapper.class)) {
			wrapperStatic.when(() -> RawRDBMSSelectWrapper.directExecutionViaConnection(frame.conn, "query", false)).thenReturn(wrapper);

			IRawSelectWrapper ans = frame.query("query");
			
			assertNotNull(ans);
			assertEquals(wrapper, ans);
		}
	}

	@Test
	void querySelectQueryStruct() throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();

		RawRDBMSSelectWrapper wrapper = new RawRDBMSSelectWrapper();

		try (MockedStatic<QSAliasToPhysicalConverter> qsAliasStatic = Mockito.mockStatic(QSAliasToPhysicalConverter.class);
			MockedStatic<QSRenameTableConverter> qsRemaeStatic = Mockito.mockStatic(QSRenameTableConverter.class);
			MockedStatic<RawRDBMSSelectWrapper> wrapperStatic = Mockito.mockStatic(RawRDBMSSelectWrapper.class)) {
			
			qsAliasStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(eq(qs), any(OwlTemporalEngineMeta.class))).thenReturn(qs);
			qsRemaeStatic.when(() -> QSRenameTableConverter.convertQs(eq(qs), any(Map.class), eq(true))).thenReturn(qs);

			when(frame.util.getInterpreter(any(IDatabaseEngine.class))).thenReturn(interpreter);

			doNothing().when(interpreter).setQueryStruct(qs);
			doNothing().when(interpreter).setLogger(any(Logger.class));
			when(interpreter.composeQuery()).thenReturn("query");

			wrapperStatic.when(() -> RawRDBMSSelectWrapper.directExecutionViaConnection(frame.conn, "query", false)).thenReturn(wrapper);

			IRawSelectWrapper ans = frame.query(qs);
			
			assertNotNull(ans);
			assertEquals(wrapper, ans);
		}
	}

	@Test
	void querySql() {
		String query = "query";
		IHeadersDataRow dataRow = mock(IHeadersDataRow.class);
		RawRDBMSSelectWrapper wrapper = mock(RawRDBMSSelectWrapper.class);
		HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);

		doNothing().when(qs).setQuery(query);
		
		try (MockedStatic<QSAliasToPhysicalConverter> qsAliasStatic = Mockito.mockStatic(QSAliasToPhysicalConverter.class);
			MockedStatic<QSRenameTableConverter> qsRemaeStatic = Mockito.mockStatic(QSRenameTableConverter.class);
			MockedStatic<RawRDBMSSelectWrapper> wrapperStatic = Mockito.mockStatic(RawRDBMSSelectWrapper.class)) {
			
			qsAliasStatic.when(() -> QSAliasToPhysicalConverter.getPhysicalQs(eq(qs), any(OwlTemporalEngineMeta.class))).thenReturn(qs);
			qsRemaeStatic.when(() -> QSRenameTableConverter.convertQs(eq(qs), any(Map.class), eq(true))).thenReturn(qs);

			when(frame.util.getInterpreter(any(IDatabaseEngine.class))).thenReturn(interpreter);

			doNothing().when(interpreter).setQueryStruct(qs);
			doNothing().when(interpreter).setLogger(any(Logger.class));
			when(interpreter.composeQuery()).thenReturn("query");

			wrapperStatic.when(() -> RawRDBMSSelectWrapper.directExecutionViaConnection(frame.conn, "query", false)).thenReturn(wrapper);

			when(wrapper.hasNext()).thenReturn(true).thenReturn(false).thenThrow(NullPointerException.class);
			when(wrapper.next()).thenReturn(dataRow);
			when(dataRow.getValues()).thenReturn(new Object[0]);

			Object ans = frame.querySQL(query);

			assertNotNull(ans);
			assertEquals(HashMap.class, ans.getClass());
			assertEquals("{types=null, data=[[]], columns=null}", ans.toString());

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> frame.querySQL(query));
			assertEquals("Error executing sql: query", e.getMessage());
		}
	}

	@Test
	void createUpdatePreparedStatementTestOne() {
		String[] cols = {"col1"};
		PreparedStatement ps = mock(PreparedStatement.class);
		
		when(builder.createInsertPreparedStatement(anyString(), eq(cols))).thenReturn(ps);

		assertEquals(ps, frame.createInsertPreparedStatement(cols));
	}

	@Test
	void createUpdatePreparedStatementTestTwo() {
		String[] cols = {"col1"};
		String[] where = {"table"};
		PreparedStatement ps = mock(PreparedStatement.class);
		
		when(builder.createUpdatePreparedStatement(anyString(), eq(cols), eq(where))).thenReturn(ps);

		assertEquals(ps, frame.createUpdatePreparedStatement(cols, where));
	}

	@Test
	void isEmptyTest() {
		when(builder.isEmpty(anyString())).thenReturn(true);

		assertTrue(frame.isEmpty());
	}

	@Test
	void sizeTest() {
		when(builder.isEmpty(anyString())).thenReturn(false).thenReturn(true);
		when(builder.getNumRecords(anyString())).thenReturn(1);

		assertEquals(1, frame.size(anyString()));
		assertEquals(0, frame.size(anyString()));
	}

	@Test
	void closeTest() throws Exception{
		doNothing().doThrow(SQLException.class).when(conn).close();

		frame.close();
		frame.close();

		verify(conn, times(2)).close();
	}

	@Test
	void processDataMakerComponent() {
		frame.processDataMakerComponent(new DataMakerComponent(null));
	}
}
