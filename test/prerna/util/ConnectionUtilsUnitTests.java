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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.engine.api.IRDBMSEngine;

class ConnectionUtilsUnitTests {

	private IRDBMSEngine engine(boolean pooling) {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.isConnectionPooling()).thenReturn(pooling);
		return engine;
	}

	@Test
	void combinesFailuresWithoutLosingTheOriginalOrSuppressingSuccessOrItself() {
		SQLException original = new SQLException("work failed");
		SQLException cleanup = new SQLException("cleanup failed");
		assertNull(ConnectionUtils.jdbcCleanupFailure(null, null));
		assertNull(ConnectionUtils.jdbcCleanupFailure(null, null, "no failure"));
		assertSame(original, ConnectionUtils.jdbcCleanupFailure(null, original, "work failed"));
		assertSame(original, ConnectionUtils.jdbcCleanupFailure(original, null));
		assertSame(original, ConnectionUtils.jdbcCleanupFailure(original, original));
		assertArrayEquals(new Throwable[0], original.getSuppressed());
		assertSame(original, ConnectionUtils.jdbcCleanupFailure(original, cleanup));
		assertArrayEquals(new Throwable[] { cleanup }, original.getSuppressed());
	}

	@Test
	void attemptsEveryCloseAndReturnsFailuresInResourceOrder() throws Exception {
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		SQLException resultFailure = new SQLException("result close");
		AssertionError statementFailure = new AssertionError("statement close");
		IllegalStateException connectionFailure = new IllegalStateException("connection close");
		doThrow(resultFailure).when(result).close();
		doThrow(statementFailure).when(statement).close();
		doThrow(connectionFailure).when(connection).close();

		assertSame(resultFailure, ConnectionUtils.closeAllConnections(connection, statement, result));
		assertArrayEquals(new Throwable[] { statementFailure, connectionFailure }, resultFailure.getSuppressed());
		var order = inOrder(result, statement, connection);
		order.verify(result).close();
		order.verify(statement).close();
		order.verify(connection).close();
	}

	@ParameterizedTest
	@ValueSource(strings = { "connection", "connectionAlias", "statement", "statementResult", "connectionStatement",
			"connectionStatementResult", "varargs", "multipleConnections" })
	void everyPooledOverloadReturnsTheReleaseFailure(String variant) throws Exception {
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		IRDBMSEngine engine = engine(true);
		SQLException failure = new SQLException("release failed");
		doThrow(failure).when(connection).close();
		AtomicBoolean statementClosed = new AtomicBoolean();
		when(statement.getConnection()).thenAnswer(call -> {
			assertFalse(statementClosed.get(), "connection must be obtained before statement close");
			return connection;
		});
		doAnswer(call -> {
			statementClosed.set(true);
			return null;
		}).when(statement).close();

		Throwable actual = switch (variant) {
		case "connection" -> ConnectionUtils.closeConnectionIfPooling(engine, connection);
		case "connectionAlias" -> ConnectionUtils.closeAllConnectionsIfPooling(engine, connection);
		case "statement" -> ConnectionUtils.closeAllConnectionsIfPooling(engine, statement);
		case "statementResult" -> ConnectionUtils.closeAllConnectionsIfPooling(engine, statement, result);
		case "connectionStatement" -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, statement);
		case "connectionStatementResult" ->
			ConnectionUtils.closeAllConnectionsIfPooling(engine, connection, statement, result);
		case "varargs" ->
			ConnectionUtils.closeAllConnectionsIfPooling(engine, null, new Statement[] { null, statement });
		case "multipleConnections" -> ConnectionUtils.closeAllDbConnectionsIfPooling(engine, null, statement);
		default -> throw new AssertionError(variant);
		};
		assertSame(failure, actual);
		verify(connection).close();
		if (!variant.equals("connection") && !variant.equals("connectionAlias")) {
			verify(statement).close();
		}
	}

	@Test
	void pooledResultFailureRetainsSubsequentStatementAndReleaseFailures() throws Exception {
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		SQLException resultFailure = new SQLException("result close");
		SQLException statementFailure = new SQLException("statement close");
		SQLException connectionFailure = new SQLException("connection close");
		doThrow(resultFailure).when(result).close();
		doThrow(statementFailure).when(statement).close();
		doThrow(connectionFailure).when(connection).close();
		assertSame(resultFailure,
				ConnectionUtils.closeAllConnectionsIfPooling(engine(true), connection, statement, result));
		assertArrayEquals(new Throwable[] { statementFailure, connectionFailure }, resultFailure.getSuppressed());
		verify(result).close();
		verify(statement).close();
		verify(connection).close();
	}

	@Test
	void lookupFailureStillClosesTheStatementAndTriesTheNextConnection() throws Exception {
		Connection connection = mock(Connection.class);
		Statement first = mock(Statement.class);
		Statement second = mock(Statement.class);
		SQLException lookup = new SQLException("connection lookup");
		SQLException closeFirst = new SQLException("first close");
		SQLException release = new SQLException("release");
		when(first.getConnection()).thenThrow(lookup);
		doThrow(closeFirst).when(first).close();
		when(second.getConnection()).thenReturn(connection);
		doThrow(release).when(connection).close();
		assertSame(lookup, ConnectionUtils.closeAllConnectionsIfPooling(engine(true), null,
				new Statement[] { null, first, second, null }));
		assertArrayEquals(new Throwable[] { closeFirst, release }, lookup.getSuppressed());
		var order = inOrder(first, second, connection);
		order.verify(first).getConnection();
		order.verify(first).close();
		order.verify(second).getConnection();
		order.verify(second).close();
		order.verify(connection).close();
	}

	@Test
	void multipleDatabaseConnectionsAreAllReleasedAfterAnError() throws Exception {
		Connection firstConnection = mock(Connection.class);
		Connection secondConnection = mock(Connection.class);
		Statement first = mock(Statement.class);
		Statement second = mock(Statement.class);
		when(first.getConnection()).thenReturn(firstConnection);
		when(second.getConnection()).thenReturn(secondConnection);
		AssertionError failure = new AssertionError("first statement");
		SQLException release = new SQLException("first connection");
		SQLException later = new SQLException("second statement");
		doThrow(failure).when(first).close();
		doThrow(release).when(firstConnection).close();
		doThrow(later).when(second).close();
		assertSame(failure, ConnectionUtils.closeAllDbConnectionsIfPooling(engine(true), first, null, second));
		assertArrayEquals(new Throwable[] { release, later }, failure.getSuppressed());
		verify(firstConnection).close();
		verify(secondConnection).close();
	}

	@Test
	void poolingPolicyFailureDoesNotPreventResultAndStatementCleanup() throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		AssertionError failure = new AssertionError("pooling policy");
		when(engine.isConnectionPooling()).thenThrow(failure);
		assertSame(failure, ConnectionUtils.closeAllConnectionsIfPooling(engine, connection, statement, result));
		verify(result).close();
		verify(statement).close();
		verifyNoInteractions(connection);
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void sharedOrUnknownEngineLeavesConnectionsOpen(boolean nullEngine) throws Exception {
		IRDBMSEngine engine = nullEngine ? null : engine(false);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		assertNull(ConnectionUtils.closeConnectionIfPooling(engine, connection));
		assertNull(ConnectionUtils.closeAllConnectionsIfPooling(engine, connection, statement, result));
		verify(result).close();
		verify(statement).close();
		verify(statement, never()).getConnection();
		verifyNoInteractions(connection);
	}

	@Test
	void convenienceOverloadsReturnFailuresInsteadOfDiscardingThem() throws Exception {
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		SQLException failure = new SQLException("statement close");
		doThrow(failure).when(statement).close();
		assertSame(failure, ConnectionUtils.closeAllConnections((Connection) null, statement));
		assertSame(failure, ConnectionUtils.closeAllConnections(statement, result));
		verify(result).close();
	}

	@Test
	void nullResourcesAndSuccessfulClosesReturnNull() throws Exception {
		assertNull(ConnectionUtils.closeAllConnections(null, null, null));
		assertNull(ConnectionUtils.closeAllConnectionsIfPooling(null, null, null, null));
		assertNull(ConnectionUtils.closeAllConnectionsIfPooling(engine(true), null, (Statement[]) null));
		assertNull(ConnectionUtils.closeAllDbConnectionsIfPooling(null, (Statement[]) null));
		assertNull(ConnectionUtils.closeAllDbConnectionsIfPooling(null, new Statement[] { null }));
		assertNull(ConnectionUtils.commitConnection(null));
		assertNull(ConnectionUtils.rollbackConnection(null));
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet result = mock(ResultSet.class);
		assertNull(ConnectionUtils.closeAllConnections(connection, statement, result));
		verify(connection).close();
		verify(statement).close();
		verify(result).close();
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void commitRespectsAutoCommitWithoutClosingOrChangingTheConnection(boolean autoCommit) throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(autoCommit);
		assertNull(ConnectionUtils.commitConnection(connection));
		verify(connection, org.mockito.Mockito.times(autoCommit ? 0 : 1)).commit();
		verify(connection, never()).close();
		verify(connection, never()).setAutoCommit(org.mockito.ArgumentMatchers.anyBoolean());
	}

	@ParameterizedTest
	@CsvSource({ "commit,false", "commit,true", "rollback,false", "rollback,true", "readState,false",
			"readState,true" })
	void transactionHelpersReturnTheOriginalExceptionOrError(String operation, boolean error) throws Exception {
		Connection connection = mock(Connection.class);
		Throwable failure = error ? new AssertionError(operation) : new SQLException(operation);
		if (operation.equals("commit")) {
			doThrow(failure).when(connection).commit();
		} else if (operation.equals("rollback")) {
			doThrow(failure).when(connection).rollback();
		} else {
			when(connection.getAutoCommit()).thenThrow(failure);
		}
		assertSame(failure, operation.equals("rollback") ? ConnectionUtils.rollbackConnection(connection)
				: ConnectionUtils.commitConnection(connection));
		verify(connection, never()).close();
		verify(connection, never()).setAutoCommit(org.mockito.ArgumentMatchers.anyBoolean());
	}

	@Test
	void rollbackLeavesConnectionOwnershipAndModeToTheCaller() throws Exception {
		Connection connection = mock(Connection.class);
		assertNull(ConnectionUtils.rollbackConnection(connection));
		verify(connection).rollback();
		verify(connection, never()).getAutoCommit();
		verify(connection, never()).commit();
		verify(connection, never()).close();
	}
}
