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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.zaxxer.hikari.HikariDataSource;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreter;
import prerna.query.interpreters.sql.ParameterizedSqlInterpreter.CompiledQuery;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.JdbcTestDatabase;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class RawPreparedRDBMSSelectWrapperUnitTests {

	private CompiledQuery compiled(IRDBMSEngine engine) {
		when(engine.isBasic()).thenReturn(true);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		when(engine.getDatabaseZoneId()).thenReturn(ZoneOffset.UTC);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ITEMS__ID", "id"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ITEMS__ID", ">=", 1));
		qs.addOrderBy("ITEMS__ID");
		return new ParameterizedSqlInterpreter(engine).compile(qs);
	}

	@Test
	void deferredExecutionCountResetAndEarlyCloseRetainBindingsAndMetadata() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1),(2)");
			var query = compiled(db.engine);
			db.manual();
			try (RawRDBMSSelectWrapper wrapper = RawPreparedRDBMSSelectWrapper.prepareParameterized(db.engine, query,
					2)) {
				assertInstanceOf(RawPreparedRDBMSSelectWrapper.class, wrapper);
				assertFalse(wrapper.hasNext());
				assertThrows(NoSuchElementException.class, wrapper::next);
				verify(db.engine, never()).getConnection();
				assertEquals(query.sql(), wrapper.getQuery());
				wrapper.execute();
				assertArrayEquals(new String[] { "id" }, wrapper.getHeaders());
				assertEquals(2, wrapper.getNumRows());
				assertEquals(2, wrapper.getNumRecords());
				assertEquals(1, wrapper.next().getValues()[0]);
				wrapper.reset();
				assertEquals(1, wrapper.next().getValues()[0]);
				assertEquals(2, wrapper.next().getValues()[0]);
				assertFalse(wrapper.hasNext());
				wrapper.reset();
				assertTrue(wrapper.hasNext());
			}
			verify(db.connection, times(3)).rollback();
			verify(db.connection, never()).commit();
			verify(db.connection, never()).close();
			verify(db.connection, never()).setAutoCommit(anyBoolean());
		}
	}

	@Test
	void borrowedConnectionKeepsUncommittedWorkAcrossExhaustionCountAndReset() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)");
			var query = compiled(db.engine);
			db.manual();
			db.execute("INSERT INTO ITEMS VALUES (1)");
			try (IRawSelectWrapper wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine,
					db.connection, query, 0)) {
				assertEquals(1, wrapper.getNumRows());
				assertEquals(1, wrapper.next().getValues()[0]);
				assertFalse(wrapper.hasNext());
				wrapper.reset();
				assertEquals(1, wrapper.getNumRows());
			}
			verify(db.connection, never()).commit();
			verify(db.connection, never()).rollback();
			verify(db.connection, never()).close();
			verify(db.connection, never()).setAutoCommit(anyBoolean());
			db.connection.rollback();
			assertEquals(0, db.count("ITEMS"));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "prepare", "bind", "execute", "metadata", "cursor", "value", "resultClose",
			"statementClose", "rollback", "connectionClose" })
	void failuresReleaseOwnedResourcesAndPropagate(String stage) throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet result = mock(ResultSet.class);
		ResultSetMetaData metadata = mock(ResultSetMetaData.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.isConnectionPooling()).thenReturn(true);
		when(connection.prepareStatement(anyString())).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(result);
		when(result.getMetaData()).thenReturn(metadata);
		when(metadata.getColumnCount()).thenReturn(1);
		when(metadata.getColumnType(1)).thenReturn(Types.INTEGER);
		when(metadata.getColumnTypeName(1)).thenReturn("INTEGER");
		when(result.next()).thenReturn(true, false);
		SQLException failure = new SQLException(stage);
		switch (stage) {
		case "prepare" -> when(connection.prepareStatement(anyString())).thenThrow(failure);
		case "bind" -> doThrow(failure).when(statement).setObject(eq(1), any());
		case "execute" -> when(statement.executeQuery()).thenThrow(failure);
		case "metadata" -> when(metadata.getColumnCount()).thenThrow(failure);
		case "cursor" -> when(result.next()).thenThrow(failure);
		case "value" -> when(result.getInt(1)).thenThrow(failure);
		case "resultClose" -> doThrow(failure).when(result).close();
		case "statementClose" -> doThrow(failure).when(statement).close();
		case "rollback" -> doThrow(failure).when(connection).rollback();
		case "connectionClose" -> doThrow(failure).when(connection).close();
		}
		var query = compiled(engine);
		Exception propagated = assertThrows(Exception.class, () -> {
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(engine, query, 0)) {
				while (wrapper.hasNext()) {
					wrapper.next();
				}
			}
		});
		Throwable cause = propagated;
		while (cause.getCause() != null) {
			cause = cause.getCause();
		}
		assertSame(failure, cause);
		verify(connection).rollback();
		verify(connection).close();
		verify(connection, never()).commit();
		if (!stage.equals("prepare")) {
			verify(statement).close();
		}
		if (!List.of("prepare", "bind", "execute").contains(stage)) {
			verify(result).close();
		}
	}

	@Test
	void cleanupKeepsOriginalFailureAndAttemptsAllResources() throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.isConnectionPooling()).thenReturn(true);
		when(connection.prepareStatement(anyString())).thenReturn(statement);
		SQLException original = new SQLException("execution");
		when(statement.executeQuery()).thenThrow(original);
		doThrow(new SQLException("statement close")).when(statement).close();
		doThrow(new SQLException("rollback")).when(connection).rollback();
		doThrow(new SQLException("release")).when(connection).close();
		assertSame(original, assertThrows(SQLException.class,
				() -> RawPreparedRDBMSSelectWrapper.executeParameterized(engine, compiled(engine), 0)));
		assertEquals(1, original.getSuppressed().length);
		assertInstanceOf(IllegalStateException.class, original.getSuppressed()[0]);
		assertEquals(2, original.getSuppressed()[0].getCause().getSuppressed().length);
		verify(connection).rollback();
		verify(connection).close();
	}

	@Test
	void zeroCountIsCachedAndCanBeComputedAfterExhaustion() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)");
			var query = compiled(db.engine);
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, query, 0)) {
				assertFalse(wrapper.hasNext());
				clearInvocations(db.connection);
				assertEquals(0, wrapper.getNumRows());
				assertEquals(0, wrapper.getNumRows());
				verify(db.connection).prepareStatement(query.countQuery().sql());
			}
		}
	}

	@Test
	void compiledQueriesSnapshotListsAndApplyJdbcMaximumRows() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1),(2)");
			var initial = compiled(db.engine);
			List<Object> parameters = new ArrayList<>(initial.parameters());
			var limited = new CompiledQuery(new ParameterizedQuery(initial.sql(), parameters, 1), initial.countQuery());
			parameters.set(0, 100);
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, limited, 0)) {
				assertEquals(1, wrapper.getNumRows());
				assertEquals(1, wrapper.next().getValues()[0]);
				assertFalse(wrapper.hasNext());
			}
		}
	}

	@Test
	void pooledExecutionCountAfterExhaustionAndResetReleaseEveryLease() throws Exception {
		try (var pool = new HikariDataSource()) {
			pool.setJdbcUrl("jdbc:h2:mem:prepared_pool_" + UUID.randomUUID());
			pool.setAutoCommit(false);
			pool.setMaximumPoolSize(2);
			try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
				statement.execute("CREATE TABLE ITEMS(ID INT)");
				statement.execute("INSERT INTO ITEMS VALUES (1)");
				connection.commit();
			}
			IRDBMSEngine engine = mock(IRDBMSEngine.class);
			when(engine.isConnectionPooling()).thenReturn(true);
			List<Connection> leases = new ArrayList<>();
			when(engine.getConnection()).thenAnswer(call -> {
				var connection = spy(pool.getConnection());
				leases.add(connection);
				return connection;
			});
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(engine, compiled(engine), 0)) {
				assertEquals(1, wrapper.next().getValues()[0]);
				assertFalse(wrapper.hasNext());
				assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
				assertEquals(1, wrapper.getNumRows());
				assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
				wrapper.reset();
				assertEquals(1, wrapper.next().getValues()[0]);
			}
			assertEquals(3, leases.size());
			for (Connection connection : leases) {
				verify(connection).rollback();
				verify(connection).close();
				verify(connection, never()).commit();
				verify(connection, never()).setAutoCommit(anyBoolean());
			}
			assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
		}
	}

	@Test
	void countFailureClosesStreamingResourcesAndOwnedReadTransaction() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1)");
			var query = compiled(db.engine);
			var brokenCount = new CompiledQuery(query.query(),
					new ParameterizedQuery("SELECT * FROM MISSING_TABLE", List.of(), 0));
			db.manual();
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, brokenCount, 0)) {
				assertTrue(wrapper.hasNext());
				assertThrows(IllegalArgumentException.class, wrapper::getNumRows);
				assertFalse(wrapper.hasNext());
			}
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void borrowedExecutionFailureLeavesCallerTransactionIntact() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var query = compiled(db.engine); // ITEMS deliberately does not exist.
			db.manual();
			assertThrows(SQLException.class,
					() -> RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, db.connection, query, 0));
			verify(db.connection, never()).rollback();
			verify(db.connection, never()).close();
			verify(db.connection, never()).commit();
			verify(db.engine, never()).getConnection();
		}
	}

	@Test
	void compiledWrappersRetainTheirTemplateAndResourceOwner() throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		var query = compiled(engine);
		try (var wrapper = RawPreparedRDBMSSelectWrapper.prepareParameterized(engine, query, 0)) {
			wrapper.setQuery(query.sql());
			wrapper.setEngine(engine);
			assertThrows(IllegalStateException.class, () -> wrapper.setQuery("SELECT 2"));
			assertThrows(IllegalStateException.class, () -> wrapper.setEngine(mock(IRDBMSEngine.class)));
			assertEquals(query.sql(), wrapper.getQuery());
			assertSame(engine, wrapper.getEngine());
		}
		assertThrows(IllegalArgumentException.class,
				() -> RawPreparedRDBMSSelectWrapper.prepareParameterized(engine, query, -1));
		assertThrows(IllegalArgumentException.class,
				() -> new CompiledQuery(new ParameterizedQuery("", List.of(), 0), query.countQuery()));
		verify(engine, never()).getConnection();
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void uncheckedReadFailuresPreserveTheCauseAndReleaseResources(boolean error) throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1)");
			var query = compiled(db.engine);
			db.manual();
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, query, 0)) {
				wrapper.rs = spy(wrapper.rs);
				Throwable failure = error ? new AssertionError("driver error")
						: new IllegalStateException("driver failure");
				when(wrapper.rs.next()).thenThrow(failure);
				assertSame(failure, assertThrows(failure.getClass(), wrapper::hasNext));
				assertFalse(wrapper.hasNext());
				verify(wrapper.rs).close();
			}
			verify(db.connection).rollback();
		}
	}

	@Test
	void earlyCloseDiscardsBufferedRowsWithoutTakingOwnershipOfBorrowedConnection() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1)");
			var query = compiled(db.engine);
			db.manual();
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, db.connection, query, 0)) {
				wrapper.setCloseConenctionAfterExecution(true);
				assertTrue(wrapper.hasNext());
				wrapper.close();
				wrapper.close();
				assertFalse(wrapper.hasNext());
				assertThrows(NoSuchElementException.class, wrapper::next);
				assertEquals(1, wrapper.getNumRows());
				wrapper.reset();
				assertEquals(1, wrapper.next().getValues()[0]);
			}
			verify(db.engine, never()).getConnection();
			verify(db.connection, never()).close();
			verify(db.connection, never()).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void countAndResetUseIndependentBindingsAndStatementTimeouts() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ITEMS (ID INT)", "INSERT INTO ITEMS VALUES (1),(2)");
			var initial = compiled(db.engine);
			// Distinct data/count values make accidentally reusing the data binder
			// observable.
			var query = new CompiledQuery(initial.query(),
					new ParameterizedQuery("SELECT COUNT(*) FROM ITEMS WHERE ID >= ?", List.of(2), 0));
			List<PreparedStatement> statements = new ArrayList<>();
			doAnswer(call -> {
				var statement = spy((PreparedStatement) call.callRealMethod());
				statements.add(statement);
				return statement;
			}).when(db.connection).prepareStatement(anyString());
			try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(db.engine, query, 7)) {
				assertEquals(1, wrapper.getNumRows());
				assertEquals(1, wrapper.next().getValues()[0]);
				assertEquals(2, wrapper.next().getValues()[0]);
				assertFalse(wrapper.hasNext());
				db.execute("INSERT INTO ITEMS VALUES (3)");
				wrapper.reset();
				assertEquals(2, wrapper.getNumRows());
			}
			assertEquals(4, statements.size());
			for (var statement : statements) {
				verify(statement).setQueryTimeout(7);
				verify(statement).close();
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "result", "statement", "rollback", "connection" })
	void cleanupErrorsAtExhaustionPropagateAfterAllResourcesAreAttempted(String stage) throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet result = mock(ResultSet.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.isConnectionPooling()).thenReturn(true);
		when(connection.prepareStatement(anyString())).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(result);
		when(result.getMetaData()).thenReturn(mock(ResultSetMetaData.class));
		AssertionError failure = new AssertionError(stage);
		switch (stage) {
		case "result" -> doThrow(failure).when(result).close();
		case "statement" -> doThrow(failure).when(statement).close();
		case "rollback" -> doThrow(failure).when(connection).rollback();
		case "connection" -> doThrow(failure).when(connection).close();
		}
		try (var wrapper = RawPreparedRDBMSSelectWrapper.executeParameterized(engine, compiled(engine), 0)) {
			assertSame(failure, assertThrows(AssertionError.class, wrapper::hasNext));
			assertFalse(wrapper.hasNext());
		}
		verify(result).close();
		verify(statement).close();
		verify(connection).rollback();
		verify(connection).close();
	}
}
