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
package prerna.rdf.engine.wrappers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.JdbcTestDatabase;

class RawRDBMSSelectWrapperUnitTests {

	@Test
	void legacyConnectionExecutionStillMapsRowsAndCounts() throws Exception {
		try (var db = new JdbcTestDatabase();
				var wrapper = RawRDBMSSelectWrapper.directExecutionViaConnection(db.connection, "SELECT 42 AS ANSWER",
						false)) {
			assertEquals(RawRDBMSSelectWrapper.class, wrapper.getClass());
			assertArrayEquals(new String[] { "ANSWER" }, wrapper.getHeaders());
			assertArrayEquals(new SemossDataType[] { SemossDataType.INT }, wrapper.getTypes());
			assertEquals(1, wrapper.getMetaData().getColumnCount());
			assertEquals(1, wrapper.getNumRows());
			assertEquals(1, wrapper.getNumRecords());
			assertFalse(wrapper.flushable());
			assertNull(wrapper.flush());
			assertTrue(wrapper.hasNext());
			assertTrue(wrapper.hasNext()); // Reading ahead must not skip a row.
			assertEquals(42, wrapper.next().getValues()[0]);
			assertFalse(wrapper.hasNext());
			assertThrows(NoSuchElementException.class, wrapper::next);
			assertFalse(db.connection.isClosed());
		}
	}

	@Test
	void sharedConverterPreservesJdbcValueTypesAndNulls() throws Exception {
		String sql = "SELECT 42, CAST(5000000000 AS BIGINT), CAST(1.25 AS DECIMAL(8,2)), "
				+ "DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05', CAST('text' AS CLOB), "
				+ "CAST(X'68656c6c6f' AS BLOB), CAST(X'616263' AS BINARY(3)), "
				+ "CAST(X'646566' AS VARBINARY), TRUE, ARRAY[1,2], 'label', CAST(NULL AS INTEGER)";
		try (var db = new JdbcTestDatabase();
				var wrapper = RawRDBMSSelectWrapper.directExecutionViaConnection(db.connection, sql, false)) {
			wrapper.databaseZoneId = ZoneOffset.UTC;
			Object[] row = wrapper.next().getValues();
			assertEquals(42, row[0]);
			assertEquals(5000000000L, row[1]);
			assertEquals(1.25d, row[2]);
			assertEquals("2026-01-02", assertInstanceOf(SemossDate.class, row[3]).toString());
			assertEquals("2026-01-02 03:04:05", assertInstanceOf(SemossDate.class, row[4]).toString());
			assertEquals("text", row[5]);
			assertEquals("hello", row[6]);
			assertEquals("abc", row[7]);
			assertEquals("def", row[8]);
			assertEquals(true, row[9]);
			assertArrayEquals(new Object[] { 1, 2 }, (Object[]) row[10]);
			assertEquals("label", row[11]);
			assertNull(row[12]);
			assertFalse(wrapper.hasNext());
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void engineExecutionKeepsTheLegacyConnectionPolicy(boolean closeConnection) throws Exception {
		try (var db = new JdbcTestDatabase(); var wrapper = new RawRDBMSSelectWrapper()) {
			when(db.engine.execQuery("SELECT 7 AS VALUE_COLUMN")).thenAnswer(call -> {
				Statement statement = db.connection.createStatement();
				return Map.of(IRDBMSEngine.STATEMENT_OBJECT, statement, IRDBMSEngine.ENGINE_CONNECTION_OBJECT,
						db.connection, IRDBMSEngine.RESULTSET_OBJECT,
						statement.executeQuery("SELECT 7 AS VALUE_COLUMN"));
			});
			wrapper.setEngine(db.engine);
			wrapper.setQuery("SELECT 7 AS VALUE_COLUMN");
			wrapper.setCloseConenctionAfterExecution(closeConnection);
			wrapper.execute();
			assertEquals(7, wrapper.next().getValues()[0]);
			verify(db.engine).execQuery("SELECT 7 AS VALUE_COLUMN");
			wrapper.close();
			assertEquals(closeConnection, db.connection.isClosed());
		}
	}

	@Test
	void ordinaryWrappersStillAllowChangingTheQueryAndEngine() throws Exception {
		try (var wrapper = new RawRDBMSSelectWrapper()) {
			var first = mock(IRDBMSEngine.class);
			var second = mock(IRDBMSEngine.class);
			wrapper.setQuery("SELECT 1");
			wrapper.setEngine(first);
			wrapper.setQuery("SELECT 2");
			wrapper.setEngine(second);
			assertEquals("SELECT 2", wrapper.getQuery());
			assertSame(second, wrapper.getEngine());
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void directExecutionFailureRespectsCloseIfFail(boolean closeIfFail) throws Exception {
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		SQLException failure = new SQLException("query failed");
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery("SELECT 1")).thenThrow(failure);
		assertSame(failure, assertThrows(SQLException.class,
				() -> RawRDBMSSelectWrapper.directExecutionViaConnection(connection, "SELECT 1", closeIfFail)));
		verify(statement).close();
		verify(connection, times(closeIfFail ? 1 : 0)).close();
	}

	@Test
	void metadataFailuresKeepLegacyLogAndContinueBehavior() throws Exception {
		ResultSet result = mock(ResultSet.class);
		when(result.getMetaData()).thenThrow(new SQLException("metadata unavailable"));
		try (var wrapper = assertDoesNotThrow(() -> RawRDBMSSelectWrapper.flushRsToWrapper(result))) {
			assertNull(wrapper.getHeaders());
			assertFalse(wrapper.hasNext());
		}
		verify(result).close();
	}

	@Test
	void exhaustionKeepsLegacyLogAndContinueBehaviorForCloseFailures() throws Exception {
		try (var db = new JdbcTestDatabase();
				var wrapper = RawRDBMSSelectWrapper.directExecutionViaConnection(db.connection, "SELECT 1 WHERE 1=0",
						false)) {
			wrapper.rs = spy(wrapper.rs);
			doThrow(new SQLException("close failed")).when(wrapper.rs).close();
			assertFalse(assertDoesNotThrow(wrapper::hasNext));
		}
	}

	@Test
	void cursorFailuresKeepLegacyErrorMessage() throws Exception {
		try (var db = new JdbcTestDatabase();
				var wrapper = RawRDBMSSelectWrapper.directExecutionViaConnection(db.connection, "SELECT 1", false)) {
			wrapper.rs = spy(wrapper.rs);
			when(wrapper.rs.next()).thenThrow(new SQLException("cursor failed"));
			assertTrue(assertThrows(IllegalArgumentException.class, wrapper::hasNext).getMessage()
					.contains("cursor failed"));
			verify(db.connection, never()).close();
		}
	}
}
