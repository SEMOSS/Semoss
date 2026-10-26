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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.zaxxer.hikari.HikariDataSource;

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

	@Test
	void resetRunsTheQueryAgain() throws Exception {
		try (var db = new JdbcTestDatabase(); var wrapper = new RawRDBMSSelectWrapper()) {
			when(db.engine.execQuery("SELECT 7 AS VALUE_COLUMN")).thenAnswer(call -> {
				Statement statement = db.connection.createStatement();
				return Map.of(IRDBMSEngine.STATEMENT_OBJECT, statement, IRDBMSEngine.ENGINE_CONNECTION_OBJECT,
						db.connection, IRDBMSEngine.RESULTSET_OBJECT,
						statement.executeQuery("SELECT 7 AS VALUE_COLUMN"));
			});
			wrapper.setEngine(db.engine);
			wrapper.setQuery("SELECT 7 AS VALUE_COLUMN");
			wrapper.execute();
			assertEquals(7, wrapper.next().getValues()[0]);
			wrapper.reset();
			assertEquals(7, wrapper.next().getValues()[0]);
		}
	}

	@Test
	void resetDropsTheRowReadAheadAndRunsAgainAfterTheLastRow() throws Exception {
		String sql = "SELECT ID FROM ITEMS ORDER BY ID";
		try (var db = new JdbcTestDatabase(); var wrapper = new RawRDBMSSelectWrapper()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1),(2)");
			List<Statement> statements = runOnEngineConnection(db, sql);
			wrapper.setEngine(db.engine);
			wrapper.setQuery(sql);
			wrapper.execute();
			// hasNext reads the first row ahead
			assertTrue(wrapper.hasNext());
			wrapper.reset();
			assertEquals(List.of(1, 2), values(wrapper));
			wrapper.reset();
			assertEquals(List.of(1, 2), values(wrapper));
			wrapper.reset();
			assertEquals(1, wrapper.next().getValues()[0]);
			wrapper.close();
			wrapper.close();

			assertFalse(wrapper.hasNext());
			verify(db.engine, times(4)).execQuery(sql);
			assertEquals(4, statements.size());
			for (Statement statement : statements) {
				assertTrue(statement.isClosed());
			}
			// the engine's shared connection stays open for the engine
			verify(db.connection, never()).close();
		}
	}

	@Test
	void resetKeepsAConnectionToCloseAfterExecutionOpenUntilClose() throws Exception {
		String sql = "SELECT 7 AS VALUE_COLUMN";
		try (var db = new JdbcTestDatabase(); var wrapper = new RawRDBMSSelectWrapper()) {
			runOnEngineConnection(db, sql);
			wrapper.setEngine(db.engine);
			wrapper.setQuery(sql);
			wrapper.setCloseConenctionAfterExecution(true);
			wrapper.execute();
			assertTrue(wrapper.hasNext());
			wrapper.reset();
			verify(db.connection, never()).close();
			assertEquals(7, wrapper.next().getValues()[0]);
			wrapper.close();
			wrapper.close();
			verify(db.connection, times(1)).close();
		}
	}

	@Test
	void pooledResetGivesEachConnectionBackOnce() throws Exception {
		String sql = "SELECT ID FROM ITEMS";
		try (var pool = new HikariDataSource()) {
			pool.setJdbcUrl("jdbc:h2:mem:raw_pool_" + UUID.randomUUID());
			pool.setMaximumPoolSize(2);
			try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
				statement.execute("CREATE TABLE ITEMS(ID INT)");
				statement.execute("INSERT INTO ITEMS VALUES (1)");
			}
			IRDBMSEngine engine = mock(IRDBMSEngine.class);
			List<Connection> leases = new ArrayList<>();
			// a pooled engine hands back its lease as the engine connection, with its pool
			when(engine.execQuery(sql)).thenAnswer(call -> {
				Connection lease = spy(pool.getConnection());
				leases.add(lease);
				return engineResult(lease, sql, new ArrayList<>(), pool);
			});
			try (var wrapper = new RawRDBMSSelectWrapper()) {
				wrapper.setEngine(engine);
				wrapper.setQuery(sql);
				wrapper.execute();
				assertTrue(wrapper.hasNext());
				wrapper.reset();
				assertEquals(1, pool.getHikariPoolMXBean().getActiveConnections());
				// reading past the last row gives the connection back
				assertEquals(List.of(1), values(wrapper));
				assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
				wrapper.reset();
				assertEquals(1, wrapper.next().getValues()[0]);
			}
			assertEquals(3, leases.size());
			for (Connection lease : leases) {
				verify(lease).close();
			}
			assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
		}
	}

	@Test
	void resetRunsTheQueryAgainOnTheSuppliedConnection() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1),(2)");
			try (var wrapper = RawRDBMSSelectWrapper.directExecutionViaConnection(db.connection,
					"SELECT ID FROM ITEMS ORDER BY ID", false)) {
				Statement first = wrapper.stmt;
				assertEquals(1, wrapper.next().getValues()[0]);
				assertTrue(wrapper.hasNext());
				wrapper.reset();
				assertTrue(first.isClosed());
				assertEquals(List.of(1, 2), values(wrapper));
				wrapper.reset();
				assertEquals(List.of(1, 2), values(wrapper));
				wrapper.reset();
				wrapper.close();
				wrapper.close();
				assertFalse(wrapper.hasNext());
			}
			verify(db.connection, never()).close();
			assertFalse(db.connection.isClosed());
		}
	}

	@Test
	void aFailedResetLeavesTheWrapperClosedUntilALaterResetRunsAgain() throws Exception {
		String sql = "SELECT 7 AS VALUE_COLUMN";
		SQLException failure = new SQLException("database went away");
		try (var db = new JdbcTestDatabase(); var wrapper = new RawRDBMSSelectWrapper()) {
			List<Statement> statements = new ArrayList<>();
			when(db.engine.execQuery(sql)).thenAnswer(call -> engineResult(db.connection, sql, statements, null))
					.thenThrow(failure).thenAnswer(call -> engineResult(db.connection, sql, statements, null));
			wrapper.setEngine(db.engine);
			wrapper.setQuery(sql);
			wrapper.execute();
			assertTrue(wrapper.hasNext());

			assertSame(failure, assertThrows(SQLException.class, wrapper::reset));
			assertTrue(statements.get(0).isClosed());
			assertFalse(wrapper.hasNext());
			assertThrows(NoSuchElementException.class, wrapper::next);
			wrapper.close();

			wrapper.reset();
			assertEquals(7, wrapper.next().getValues()[0]);
			verify(db.connection, never()).close();
		}
	}

	@Test
	void aFailedResetOnTheSuppliedConnectionLeavesItOpen() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			try (var wrapper = RawRDBMSSelectWrapper.directExecutionViaConnection(db.connection, "SELECT 1", true)) {
				Statement first = wrapper.stmt;
				SQLException failure = new SQLException("connection lost");
				doThrow(failure).when(db.connection).createStatement();

				assertSame(failure, assertThrows(SQLException.class, wrapper::reset));
				assertTrue(first.isClosed());
				assertFalse(wrapper.hasNext());
			}
			verify(db.connection, never()).close();
		}
	}

	@Test
	void aWrapperOverAResultSetOrACallersPreparedStatementHasNoQueryToRunAgain() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			PreparedStatement prepared = db.connection.prepareStatement("SELECT CAST(? AS INT)");
			prepared.setInt(1, 5);
			try (var wrapper = RawRDBMSSelectWrapper.directExecutionPreparedStatement(db.engine, db.connection,
					prepared, "SELECT CAST(? AS INT)", false)) {
				assertThrows(UnsupportedOperationException.class, wrapper::reset);
				// the wrapper is left as it was
				assertEquals(5, wrapper.next().getValues()[0]);
			}
			try (Statement statement = db.connection.createStatement();
					var wrapper = RawRDBMSSelectWrapper.wrapResultSet(statement.executeQuery("SELECT 6"),
							ZoneOffset.UTC)) {
				assertThrows(UnsupportedOperationException.class, wrapper::reset);
				assertEquals(6, wrapper.next().getValues()[0]);
			}
		}
	}

	@Test
	void anImpalaWrapperResetsLikeAnyEngineQuery() throws Exception {
		String sql = "SELECT 7 AS VALUE_COLUMN";
		try (var db = new JdbcTestDatabase(); var wrapper = new RawImpalaSelectWrapper()) {
			runOnEngineConnection(db, sql);
			wrapper.setEngine(db.engine);
			wrapper.setQuery(sql);
			wrapper.execute();
			assertEquals(7, wrapper.next().getValues()[0]);
			assertFalse(wrapper.hasNext());
			wrapper.reset();
			assertEquals(7, wrapper.next().getValues()[0]);
		}
	}

	/**
	 * Answers the engine's execQuery as an engine without a pool does: a new
	 * statement on its shared connection each time.
	 *
	 * @return the statements made, in order, so a test can check they were closed
	 */
	private static List<Statement> runOnEngineConnection(JdbcTestDatabase db, String sql) throws Exception {
		List<Statement> statements = new ArrayList<>();
		when(db.engine.execQuery(sql)).thenAnswer(call -> engineResult(db.connection, sql, statements, null));
		return statements;
	}

	/**
	 * Runs the SQL on a new statement and returns it the way an RDBMS engine's
	 * execQuery does, with the pool when the connection came from one.
	 */
	private static Map<String, Object> engineResult(Connection connection, String sql, List<Statement> statements,
			HikariDataSource pool) throws SQLException {
		Statement statement = connection.createStatement();
		statements.add(statement);
		Map<String, Object> result = new HashMap<>();
		result.put(IRDBMSEngine.STATEMENT_OBJECT, statement);
		result.put(IRDBMSEngine.ENGINE_CONNECTION_OBJECT, connection);
		result.put(IRDBMSEngine.RESULTSET_OBJECT, statement.executeQuery(sql));
		if (pool != null) {
			result.put(IRDBMSEngine.DATASOURCE_POOLING_OBJECT, pool);
		}
		return result;
	}

	/** The first column of every row left in the wrapper */
	private static List<Object> values(RawRDBMSSelectWrapper wrapper) {
		List<Object> values = new ArrayList<>();
		while (wrapper.hasNext()) {
			values.add(wrapper.next().getValues()[0]);
		}
		return values;
	}
}
