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
package prerna.engine.impl.rdbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import com.google.gson.Gson;
import com.zaxxer.hikari.HikariDataSource;

import prerna.engine.api.SqlTransactionException;
import prerna.engine.api.SqlTransactionResult;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class RDBMSNativeEngineTransactionUnitTests {

	private String url;
	private Connection keepAlive;

	@BeforeEach
	void createDatabase() throws Exception {
		this.url = "jdbc:h2:mem:transaction_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
		this.keepAlive = DriverManager.getConnection(this.url);
		try (Statement statement = this.keepAlive.createStatement()) {
			statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR(50))");
		}
	}

	@AfterEach
	void dropDatabase() throws Exception {
		try (Statement statement = this.keepAlive.createStatement()) {
			statement.execute("SHUTDOWN");
		}
		this.keepAlive.close();
	}

	@Test
	void everyStatementCommitsOnceTheLastOneRuns() throws Exception {
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SqlTransactionResult result = engine.executeInTransaction(List.of("INSERT INTO orders VALUES (1, 'what?')",
					"UPDATE orders SET note = 'b' WHERE id = 1", "SELECT note FROM orders"), 0, 0);

			assertEquals(SqlTransactionResult.Outcome.COMMITTED, result.getOutcome());
			assertEquals(3, result.getRanCount());
			assertEquals(1, result.getStatements().get(0).getUpdateCount());
			assertTrue(result.getStatements().get(2).isReturnedRows());
			assertNull(result.getStatements().get(2).getUpdateCount());
			// no rows were asked for
			assertNull(result.getStatements().get(2).getRows());
			assertEquals("b", note(1));
		}
	}

	@Test
	void aFailingStatementRollsBackTheOnesBeforeItAndSkipsTheRest() throws Exception {
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SqlTransactionException failure = assertThrows(SqlTransactionException.class,
					() -> engine.executeInTransaction(List.of("INSERT INTO orders VALUES (1, 'a')",
							"INSERT INTO orders VALUES (1, 'duplicate')", "INSERT INTO orders VALUES (3, 'c')"), 0, 0));

			SqlTransactionResult result = failure.getResult();
			assertEquals(SqlTransactionResult.Outcome.ROLLED_BACK, result.getOutcome());
			assertEquals(2, result.getFailedStatement());
			assertEquals(SqlTransactionResult.Status.ROLLED_BACK, result.getStatements().get(0).getStatus());
			assertEquals(SqlTransactionResult.Status.ERROR, result.getStatements().get(1).getStatus());
			assertEquals(SqlTransactionResult.Status.SKIPPED, result.getStatements().get(2).getStatus());
			assertEquals(0, count());
		}
	}

	@Test
	void theSharedEngineConnectionIsNeverPartOfTheTransaction() throws Exception {
		Connection shared = mock(Connection.class);
		RDBMSNativeEngine engine = engine();
		engine.setConnection(shared);
		clearInvocations(shared);

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			engine.executeInTransaction(List.of("INSERT INTO orders VALUES (1, 'a')"), 0, 0);
		}

		verifyNoInteractions(shared);
		assertEquals(1, count());
	}

	@Test
	void statementsRunAsPlainStatementsWithoutTheirFinalSemicolon() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Statement statement = mock(Statement.class);
		Connection connection = mockConnection(statement);
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			engine.executeInTransaction(List.of("SELECT data ? 'key' FROM documents ;"), 0, 0);
		}

		// a plain statement never reads the ? as a parameter
		verify(connection, never()).prepareStatement(anyString());
		verify(connection).createStatement();
		verify(statement).execute("SELECT data ? 'key' FROM documents ");
	}

	@Test
	void dataDefinitionIsRefusedBeforeAnythingRunsWhereTheDatabaseCommitsIt() throws Exception {
		// H2 commits data definition as soon as it runs
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SQLFeatureNotSupportedException refusal = assertThrows(SQLFeatureNotSupportedException.class,
					() -> engine.executeInTransaction(List.of("INSERT INTO orders VALUES (1, 'a');",
							"-- a new table\nCREATE TABLE extra (id INT);"), 0, 0));

			assertTrue(refusal.getMessage().startsWith("Statement 2 "), refusal.getMessage());
		}
		assertEquals(0, count());
	}

	@Test
	void aSingleDataDefinitionStatementRunsWhereTheDatabaseCommitsIt() throws Exception {
		// one statement commits or fails as a whole, even on a database that commits
		// data definition as soon as it runs, as H2 does
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SqlTransactionResult result = engine.executeInTransaction(List.of("CREATE TABLE extra (id INT);"), 0, 0);

			assertEquals(SqlTransactionResult.Outcome.COMMITTED, result.getOutcome());
		}
		try (Statement statement = this.keepAlive.createStatement();
				ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM extra")) {
			assertTrue(rs.next());
		}
	}

	@Test
	void rowsAreKeptUpToTheMostAskedFor() throws Exception {
		insertOrder(1, "a");
		insertOrder(2, "b");
		insertOrder(3, "c");
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SqlTransactionResult result = engine
					.executeInTransaction(List.of("SELECT id, note FROM orders ORDER BY id"), 0, 2);

			SqlTransactionResult.StatementResult select = result.getStatements().get(0);
			assertEquals(List.of("ID", "NOTE"), select.getHeaders());
			assertEquals(2, select.getRows().size());
			assertEquals(1, select.getRows().get(0)[0]);
			assertEquals("b", select.getRows().get(1)[1]);
			assertTrue(select.hasMoreRows());
		}
	}

	@Test
	void rowsReadBeforeAFailureAreKeptWithTheirStatementRolledBack() throws Exception {
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SqlTransactionException failure = assertThrows(SqlTransactionException.class,
					() -> engine.executeInTransaction(List.of("INSERT INTO orders VALUES (1, 'a')",
							"SELECT note FROM orders", "INSERT INTO orders VALUES (1, 'duplicate')"), 0, 50));

			SqlTransactionResult.StatementResult select = failure.getResult().getStatements().get(1);
			assertEquals(SqlTransactionResult.Status.ROLLED_BACK, select.getStatus());
			assertEquals(1, select.getRows().size());
			assertEquals("a", select.getRows().get(0)[0]);
			assertFalse(select.hasMoreRows());
		}
		assertEquals(0, count());
	}

	@Test
	void keptRowsAreLeftOutOfTheEngineCallLog() throws Exception {
		insertOrder(1, "private-note");
		RDBMSNativeEngine engine = engine();
		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnectionsTo(this.url)) {
			SqlTransactionResult result = engine.executeInTransaction(List.of("SELECT note FROM orders"), 0, 50);

			assertEquals("private-note", result.getStatements().get(0).getRows().get(0)[0]);
			// the engine call's audit log entry serializes the result with GSON, which
			// leaves transient fields out
			String logged = new Gson().toJson(result);
			assertFalse(logged.contains("private-note"), logged);
		}
	}

	@Test
	void aRefusedScriptHandsItsConnectionBackUntouched() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Connection connection = mockConnection(mock(Statement.class));
		when(connection.getMetaData().dataDefinitionCausesTransactionCommit()).thenReturn(true);

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			assertThrows(SQLFeatureNotSupportedException.class,
					() -> engine.executeInTransaction(List.of("DROP TABLE orders;", "SELECT 1;"), 0, 0));
		}
		verify(connection, never()).createStatement();
		verify(connection, never()).setAutoCommit(false);
		verify(connection).close();
	}

	@Test
	void aScriptWithAStatementThatCanCommitOnItsOwnIsRefusedWhereTheDatabaseCommitsDataDefinition() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Connection connection = mockConnection(mock(Statement.class));
		when(connection.getMetaData().dataDefinitionCausesTransactionCommit()).thenReturn(true);

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			SQLFeatureNotSupportedException failure = assertThrows(SQLFeatureNotSupportedException.class, () -> engine
					.executeInTransaction(List.of("UPDATE orders SET note = 'a';", "LOCK TABLES orders WRITE;"), 0, 0));
			assertTrue(failure.getMessage().startsWith("Statement 2 "), failure.getMessage());
		}
		verify(connection, never()).createStatement();
	}

	@Test
	void dataDefinitionRunsInTheTransactionWhereTheDatabaseDoesNotCommitIt() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Statement statement = mock(Statement.class);
		Connection connection = mockConnection(statement);

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			SqlTransactionResult result = engine.executeInTransaction(List.of("CREATE TABLE extra (id INT);"), 0, 0);

			assertEquals(SqlTransactionResult.Outcome.COMMITTED, result.getOutcome());
		}
		verify(statement).execute("CREATE TABLE extra (id INT)");
		verify(connection).commit();
	}

	@Test
	void aCommitThatLosesTheConnectionHasAnUnknownOutcome() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Connection connection = mockConnection(mock(Statement.class));
		doThrow(new SQLException("Connection reset", "08006")).when(connection).commit();

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			SqlTransactionException failure = assertThrows(SqlTransactionException.class,
					() -> engine.executeInTransaction(List.of("UPDATE orders SET note = 'a';"), 0, 0));

			assertEquals(SqlTransactionResult.Outcome.UNKNOWN, failure.getResult().getOutcome());
			assertEquals(SqlTransactionResult.Status.SUCCESS, failure.getResult().getStatements().get(0).getStatus());
		}
		// a connection in an unknown state is closed, never handed back with
		// auto-commit on
		verify(connection, never()).setAutoCommit(true);
		verify(connection).close();
	}

	@Test
	void aFailedStatementWhoseRollbackFailsLeavesTheOutcomeUnknown() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Statement statement = mock(Statement.class);
		Connection connection = mockConnection(statement);
		when(statement.execute(anyString())).thenReturn(false).thenThrow(new SQLException("duplicate key", "23505"));
		doThrow(new SQLException("Connection reset", "08006")).when(connection).rollback();

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			List<String> statements = List.of("UPDATE orders SET note = 'a';", "INSERT INTO orders VALUES (1);");
			SqlTransactionException failure = assertThrows(SqlTransactionException.class,
					() -> engine.executeInTransaction(statements, 0, 0));

			SqlTransactionResult result = failure.getResult();
			// the connection may commit the first statement's change when it closes
			assertEquals(SqlTransactionResult.Outcome.UNKNOWN, result.getOutcome());
			assertEquals(2, result.getFailedStatement());
			assertTrue(result.getMessage().contains("rollback did not report back"), result.getMessage());
		}
	}

	@Test
	void aCommitTheDatabaseRejectsIsRolledBack() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Connection connection = mockConnection(mock(Statement.class));
		doThrow(new SQLException("deferred constraint", "23503")).when(connection).commit();

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			SqlTransactionException failure = assertThrows(SqlTransactionException.class,
					() -> engine.executeInTransaction(List.of("UPDATE orders SET note = 'a';"), 0, 0));

			assertEquals(SqlTransactionResult.Outcome.ROLLED_BACK, failure.getResult().getOutcome());
			assertEquals(SqlTransactionResult.Status.ROLLED_BACK,
					failure.getResult().getStatements().get(0).getStatus());
		}
		verify(connection).rollback();
		verify(connection).setAutoCommit(true);
	}

	@Test
	void aScriptThatRunsOutOfTimeStopsAndRollsBack() throws Exception {
		Statement statement = mock(Statement.class);
		when(statement.execute(anyString())).thenAnswer(invocation -> {
			Thread.sleep(1_100);
			return false;
		});
		Connection connection = mockConnection(statement);
		RDBMSNativeEngine engine = mockedEngine();

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			List<String> statements = List.of("UPDATE orders SET note = 'a';", "UPDATE orders SET note = 'b';");
			SqlTransactionException failure = assertThrows(SqlTransactionException.class,
					() -> engine.executeInTransaction(statements, 1, 0));

			SqlTransactionResult result = failure.getResult();
			assertEquals(SqlTransactionResult.Outcome.ROLLED_BACK, result.getOutcome());
			assertEquals(2, result.getFailedStatement());
			assertTrue(result.getMessage().contains("1 second timeout"), result.getMessage());
		}
		verify(statement).setQueryTimeout(1);
		verify(statement, times(1)).execute(anyString());
		verify(connection).rollback();
	}

	@Test
	void anUnexpectedErrorRollsTheScriptBackBeforeItsConnectionCloses() throws Exception {
		RDBMSNativeEngine engine = mockedEngine();
		Statement statement = mock(Statement.class);
		Connection connection = mockConnection(statement);
		AssertionError error = new AssertionError("driver bug");
		when(statement.execute(anyString())).thenReturn(false).thenThrow(error);

		try (MockedStatic<AbstractSqlQueryUtil> connections = openConnection(connection)) {
			List<String> statements = List.of("UPDATE orders SET note = 'a';", "UPDATE orders SET note = 'b';");
			assertSame(error, assertThrows(AssertionError.class, () -> engine.executeInTransaction(statements, 0, 0)));
		}
		// a driver such as Oracle's commits what is pending when its connection closes
		InOrder order = inOrder(connection);
		order.verify(connection).rollback();
		order.verify(connection).close();
		verify(connection, never()).commit();
		verify(connection, never()).setAutoCommit(true);
	}

	@Test
	void aFailedWriteRollsBackTheSharedConnectionBeforeItIsUsedAgain() throws Exception {
		insertOrder(1, "a");
		Connection shared = spy(DriverManager.getConnection(this.url));
		shared.setAutoCommit(false);
		RDBMSNativeEngine engine = engine();
		engine.setConnection(shared);
		clearInvocations(shared);

		SQLException failure = assertThrows(SQLException.class,
				() -> engine.insertData("INSERT INTO orders VALUES (1, 'duplicate')"));
		assertEquals("23505", failure.getSQLState());
		engine.insertData("INSERT INTO orders VALUES (2, 'b')");

		InOrder order = inOrder(shared);
		order.verify(shared).rollback();
		order.verify(shared).prepareStatement("INSERT INTO orders VALUES (2, 'b')");
		order.verify(shared).commit();
		// the engine's shared connection stays open for its other callers
		verify(shared, never()).close();
		assertEquals(2, count());
		shared.close();
	}

	@Test
	void aFailedWriteOnAConnectionThatCommitsOnItsOwnHasNothingToRollBack() throws Exception {
		Connection shared = sharedConnection(true);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(shared.prepareStatement(anyString())).thenReturn(statement);
		SQLException failure = new SQLException("relation does not exist", "42P01");
		when(statement.execute()).thenThrow(failure);
		RDBMSNativeEngine engine = mockedEngine();
		engine.setConnection(shared);

		assertSame(failure, assertThrows(SQLException.class, () -> engine.removeData("DELETE FROM missing")));
		verify(statement).close();
		verify(shared, never()).rollback();
		verify(shared, never()).commit();
		verify(shared, never()).close();
	}

	@Test
	void aRollbackThatFailsIsKeptOnTheWritesOwnFailure() throws Exception {
		Connection shared = sharedConnection(false);
		Statement statement = mock(Statement.class);
		when(shared.createStatement()).thenReturn(statement);
		SQLException failure = new SQLException("syntax error", "42601");
		when(statement.executeUpdate(anyString())).thenThrow(failure);
		SQLException rollbackFailure = new SQLException("Connection reset", "08006");
		doThrow(rollbackFailure).when(shared).rollback();
		RDBMSNativeEngine engine = mockedEngine();
		engine.setConnection(shared);

		// a CREATE runs as a plain statement
		assertSame(failure, assertThrows(SQLException.class, () -> engine.insertData("CREATE TABLE extra (id INT")));
		assertEquals(1, failure.getSuppressed().length);
		assertSame(rollbackFailure, failure.getSuppressed()[0]);
		verify(shared, never()).prepareStatement(anyString());
		verify(shared, never()).commit();
	}

	@Test
	void aCreateInAnyCaseAfterLeadingWhitespaceRunsAsAPlainStatement() throws Exception {
		Connection shared = sharedConnection(true);
		Statement statement = mock(Statement.class);
		when(shared.createStatement()).thenReturn(statement);
		RDBMSNativeEngine engine = mockedEngine();
		engine.setConnection(shared);

		// the trigger's :NEW is a bind marker to a prepared statement on Oracle
		String trigger = "  create or replace trigger stamp before insert on orders for each row "
				+ "begin :NEW.note := 'a'; end;";
		String table = "\nCreate Table extra (id INT)";
		engine.insertData(trigger);
		engine.insertData(table);

		verify(statement).executeUpdate(trigger);
		verify(statement).executeUpdate(table);
		verify(shared, never()).prepareStatement(anyString());
	}

	@Test
	void aCreateDatabaseRunsAsAPreparedStatement() throws Exception {
		Connection shared = sharedConnection(true);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(shared.prepareStatement(anyString())).thenReturn(statement);
		RDBMSNativeEngine engine = mockedEngine();
		engine.setConnection(shared);

		engine.insertData(" create  database extra");

		verify(statement).execute();
		verify(shared, never()).createStatement();
	}

	@Test
	void aCommitThatFailsIsRolledBack() throws Exception {
		Connection shared = sharedConnection(false);
		when(shared.prepareStatement(anyString())).thenReturn(mock(PreparedStatement.class));
		SQLException failure = new SQLException("deferred constraint", "23503");
		doThrow(failure).when(shared).commit();
		RDBMSNativeEngine engine = mockedEngine();
		engine.setConnection(shared);

		assertSame(failure,
				assertThrows(SQLException.class, () -> engine.insertData("INSERT INTO orders VALUES (1, 'a')")));
		verify(shared).rollback();
		verify(shared, never()).close();
	}

	@Test
	void aPooledConnectionIsRolledBackBeforeItGoesBackToItsPool() throws Exception {
		Connection lease = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(lease.getAutoCommit()).thenReturn(false);
		when(lease.prepareStatement(anyString())).thenReturn(statement);
		when(statement.execute()).thenReturn(false).thenThrow(new SQLException("duplicate key", "23505"));
		HikariDataSource pool = mock(HikariDataSource.class);
		when(pool.getConnection()).thenReturn(lease);
		RDBMSNativeEngine engine = mockedEngine();
		engine.dataSource = pool;

		engine.insertData("INSERT INTO orders VALUES (1, 'a')");
		assertThrows(SQLException.class, () -> engine.insertData("INSERT INTO orders VALUES (1, 'duplicate')"));

		InOrder order = inOrder(lease);
		order.verify(lease).commit();
		order.verify(lease).close();
		order.verify(lease).rollback();
		order.verify(lease).close();
		verify(lease, times(1)).commit();
		verify(lease, times(1)).rollback();
	}

	/** An engine on the H2 test database */
	private static RDBMSNativeEngine engine() {
		return engine(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
	}

	/**
	 * An engine whose query util adds nothing to a connection, for a mocked
	 * connection that has to see only the script's own statements.
	 */
	private static RDBMSNativeEngine mockedEngine() {
		return engine(mock(AbstractSqlQueryUtil.class));
	}

	private static RDBMSNativeEngine engine(AbstractSqlQueryUtil queryUtil) {
		RDBMSNativeEngine engine = new RDBMSNativeEngine();
		engine.setEngineId("transaction-test");
		engine.setQueryUtil(queryUtil);
		return engine;
	}

	/**
	 * A connection whose statements each change one row and return one result, to a
	 * database that keeps data definition in the transaction
	 */
	private static Connection mockConnection(Statement statement) throws SQLException {
		when(statement.getUpdateCount()).thenReturn(1, -1);
		Connection connection = mock(Connection.class);
		when(connection.getMetaData()).thenReturn(mock(DatabaseMetaData.class));
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.createStatement()).thenReturn(statement);
		return connection;
	}

	/**
	 * A shared engine connection in the given commit mode, which passes the check
	 * the engine makes before each use
	 */
	private static Connection sharedConnection(boolean autoCommit) throws SQLException {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(autoCommit);
		when(connection.isValid(anyInt())).thenReturn(true);
		return connection;
	}

	/**
	 * Every connection the engine opens for a transaction is a new one to the test
	 * database
	 */
	private static MockedStatic<AbstractSqlQueryUtil> openConnectionsTo(String url) {
		MockedStatic<AbstractSqlQueryUtil> connections = mockStatic(AbstractSqlQueryUtil.class);
		connections
				.when(() -> AbstractSqlQueryUtil.makeConnection(nullable(AbstractSqlQueryUtil.class),
						nullable(String.class), nullable(CaseInsensitiveProperties.class)))
				.thenAnswer(invocation -> DriverManager.getConnection(url));
		return connections;
	}

	private static MockedStatic<AbstractSqlQueryUtil> openConnection(Connection connection) {
		MockedStatic<AbstractSqlQueryUtil> connections = mockStatic(AbstractSqlQueryUtil.class);
		connections.when(() -> AbstractSqlQueryUtil.makeConnection(nullable(AbstractSqlQueryUtil.class),
				nullable(String.class), nullable(CaseInsensitiveProperties.class))).thenReturn(connection);
		return connections;
	}

	private void insertOrder(int id, String note) throws SQLException {
		try (Statement statement = this.keepAlive.createStatement()) {
			statement.execute("INSERT INTO orders VALUES (" + id + ", '" + note + "')");
		}
	}

	private String note(int id) throws SQLException {
		try (Statement statement = this.keepAlive.createStatement();
				ResultSet rs = statement.executeQuery("SELECT note FROM orders WHERE id = " + id)) {
			return rs.next() ? rs.getString(1) : null;
		}
	}

	private int count() throws SQLException {
		try (Statement statement = this.keepAlive.createStatement();
				ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders")) {
			rs.next();
			return rs.getInt(1);
		}
	}
}
