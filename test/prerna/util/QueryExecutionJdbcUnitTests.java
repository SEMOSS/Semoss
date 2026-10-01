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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import prerna.engine.api.IRDBMSEngine;

class QueryExecutionJdbcUnitTests {

	private IRDBMSEngine engine(Connection connection, boolean pooling) throws Exception {
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.isConnectionPooling()).thenReturn(pooling);
		return engine;
	}

	@ParameterizedTest
	@CsvSource({ "true,true,true", "true,false,true", "false,true,true", "false,false,true", "true,true,false",
			"true,false,false", "false,true,false", "false,false,false" })
	void ownsSuccessfulTransaction(boolean pooling, boolean autoCommit, boolean write) throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(autoCommit);
		IRDBMSEngine engine = engine(connection, pooling);
		QueryExecutionUtility.JdbcWork<String> work = c -> {
			assertSame(connection, c);
			return "result";
		};
		assertEquals("result",
				write ? QueryExecutionUtility.write(engine, work) : QueryExecutionUtility.read(engine, work));
		verify(engine).getConnection();
		verify(connection, times(write ? 1 : 0)).commit();
		verify(connection, times(!write && !autoCommit ? 1 : 0)).rollback();
		verify(connection, times(write && autoCommit ? 1 : 0)).setAutoCommit(false);
		verify(connection, times(write && autoCommit ? 1 : 0)).setAutoCommit(true);
		verify(connection, times(pooling ? 1 : 0)).close();
	}

	@ParameterizedTest
	@CsvSource({ "true,true", "true,false", "false,true", "false,false" })
	void rollsBackExceptionsAndErrors(boolean pooling, boolean autoCommit) throws Exception {
		for (Throwable original : List.of(new SQLException("work"), new AssertionError("work"))) {
			Connection connection = mock(Connection.class);
			when(connection.getAutoCommit()).thenReturn(autoCommit);
			IRDBMSEngine engine = engine(connection, pooling);
			Throwable actual = assertThrows(original.getClass(), () -> QueryExecutionUtility.write(engine, c -> {
				if (original instanceof Error) {
					throw (Error) original;
				}
				throw (Exception) original;
			}));
			assertSame(original, actual);
			verify(connection).rollback();
			verify(connection, never()).commit();
			verify(connection, times(autoCommit ? 1 : 0)).setAutoCommit(true);
			verify(connection, times(pooling ? 1 : 0)).close();
		}
	}

	@Test
	void neverRestoresAutoCommitAfterRollbackFailureAndKeepsOriginalError() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException rollback = new SQLException("rollback");
		SQLException close = new SQLException("close");
		doThrow(rollback).when(connection).rollback();
		doThrow(close).when(connection).close();
		AssertionError original = new AssertionError("work");
		IRDBMSEngine engine = engine(connection, true);
		assertSame(original, assertThrows(AssertionError.class, () -> QueryExecutionUtility.write(engine, c -> {
			throw original;
		})));
		assertArrayEquals(new Throwable[] { rollback, close }, original.getSuppressed());
		verify(connection, never()).setAutoCommit(true);
		verify(connection).close();
	}

	@Test
	void commitFailureRollsBackBeforeRestoration() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException commit = new SQLException("commit");
		doThrow(commit).when(connection).commit();
		assertSame(commit,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, true), c -> 1)));
		var order = inOrder(connection);
		order.verify(connection).setAutoCommit(false);
		order.verify(connection).commit();
		order.verify(connection).rollback();
		order.verify(connection).setAutoCommit(true);
		order.verify(connection).close();
	}

	@Test
	void unknownInitialStateIsNeverGuessed() throws Exception {
		Connection connection = mock(Connection.class);
		SQLException original = new SQLException("getAutoCommit");
		when(connection.getAutoCommit()).thenThrow(original);
		assertSame(original,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, true), c -> {
					fail("must not run");
					return null;
				})));
		verify(connection, never()).setAutoCommit(anyBoolean());
		verify(connection, never()).rollback();
		verify(connection).close();
	}

	@Test
	void failedDisableAttemptsRollbackAndPreservesFailure() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException original = new SQLException("disable");
		doThrow(original).when(connection).setAutoCommit(false);
		assertSame(original,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, true), c -> {
					fail("must not run");
					return null;
				})));
		var order = inOrder(connection);
		order.verify(connection).rollback();
		order.verify(connection).setAutoCommit(true);
		order.verify(connection).close();
	}

	@Test
	void restorationFailureDoesNotUndoSuccessfulCommit() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException restore = new SQLException("restore");
		doThrow(restore).when(connection).setAutoCommit(true);
		assertSame(restore,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, true), c -> 1)));
		verify(connection).commit();
		verify(connection, never()).rollback();
		verify(connection).close();
	}

	@Test
	void restorationFailureIsSuppressedOnWorkFailure() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException restore = new SQLException("restore");
		doThrow(restore).when(connection).setAutoCommit(true);
		SQLException original = new SQLException("work");
		assertSame(original,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, false), c -> {
					throw original;
				})));
		assertArrayEquals(new Throwable[] { restore }, original.getSuppressed());
		verify(connection, never()).close();
	}

	@Test
	void releaseFailureAfterSuccessIsReported() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException close = new SQLException("close");
		doThrow(close).when(connection).close();
		assertSame(close,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, true), c -> 1)));
		verify(connection).commit();
		verify(connection, never()).rollback();
	}

	@ParameterizedTest
	@CsvSource({ "true", "false" })
	void readFailureOnlyRollsBackManualTransactions(boolean autoCommit) throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(autoCommit);
		assertThrows(SQLException.class, () -> QueryExecutionUtility.read(engine(connection, true), c -> {
			throw new SQLException("read");
		}));
		verify(connection, times(autoCommit ? 0 : 1)).rollback();
		verify(connection, never()).setAutoCommit(anyBoolean());
		verify(connection).close();
	}

	@ParameterizedTest
	@CsvSource({ "true", "false" })
	void h2WritesAreAtomicAndReusable(boolean autoCommit) throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:atomic_" + UUID.randomUUID())) {
			try (var statement = connection.createStatement()) {
				statement.execute("CREATE TABLE ITEMS (ID INT PRIMARY KEY)");
			}
			connection.setAutoCommit(autoCommit);
			IRDBMSEngine engine = engine(connection, false);
			QueryExecutionUtility.write(engine, c -> {
				insert(c, 1);
				return null;
			});
			assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine, c -> {
				insert(c, 2);
				insert(c, 1);
				return null;
			}));
			QueryExecutionUtility.write(engine, c -> {
				insert(c, 3);
				insert(c, 4);
				return null;
			});
			List<Integer> ids = QueryExecutionUtility.read(engine, c -> {
				List<Integer> values = new ArrayList<>();
				try (var statement = c.prepareStatement("SELECT ID FROM ITEMS ORDER BY ID");
						var rs = statement.executeQuery()) {
					while (rs.next()) {
						values.add(rs.getInt(1));
					}
				}
				return values;
			});
			assertEquals(List.of(1, 3, 4), ids);
			assertEquals(autoCommit, connection.getAutoCommit());
			assertFalse(connection.isClosed());
		}
	}

	private void insert(Connection connection, int id) throws SQLException {
		try (var statement = connection.prepareStatement("INSERT INTO ITEMS VALUES (?)")) {
			statement.setInt(1, id);
			statement.executeUpdate();
		}
	}

	@Test
	void serializesDifferentEngineProxiesSharingOneConnection() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		IRDBMSEngine first = engine(connection, false);
		IRDBMSEngine second = engine(connection, false);
		AtomicInteger active = new AtomicInteger();
		CountDownLatch started = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		CountDownLatch secondAcquired = new CountDownLatch(1);
		when(second.getConnection()).thenAnswer(i -> {
			secondAcquired.countDown();
			return connection;
		});
		try (var executor = Executors.newFixedThreadPool(2)) {
			var a = executor.submit(() -> QueryExecutionUtility.write(first, c -> {
				assertEquals(1, active.incrementAndGet());
				started.countDown();
				assertTrue(release.await(5, TimeUnit.SECONDS));
				active.decrementAndGet();
				return 1;
			}));
			assertTrue(started.await(5, TimeUnit.SECONDS));
			var b = executor.submit(() -> QueryExecutionUtility.read(second, c -> {
				assertEquals(1, active.incrementAndGet());
				active.decrementAndGet();
				return 2;
			}));
			try {
				assertTrue(secondAcquired.await(5, TimeUnit.SECONDS));
				assertFalse(b.isDone());
			} finally {
				release.countDown();
			}
			assertEquals(1, a.get(5, TimeUnit.SECONDS));
			assertEquals(2, b.get(5, TimeUnit.SECONDS));
		}
	}

	@Test
	void readRollbackFailureIsReportedWithoutRetryOrRestoration() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(false);
		SQLException rollback = new SQLException("rollback");
		doThrow(rollback).when(connection).rollback();
		assertSame(rollback,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.read(engine(connection, true), c -> 1)));
		verify(connection).rollback();
		verify(connection, never()).setAutoCommit(anyBoolean());
		verify(connection).close();
	}

	@Test
	void commitAndRollbackFailureNeverEnablesAutoCommit() throws Exception {
		Connection connection = mock(Connection.class);
		when(connection.getAutoCommit()).thenReturn(true);
		SQLException commit = new SQLException("commit");
		SQLException rollback = new SQLException("rollback");
		doThrow(commit).when(connection).commit();
		doThrow(rollback).when(connection).rollback();
		assertSame(commit,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine(connection, true), c -> 1)));
		assertArrayEquals(new Throwable[] { rollback }, commit.getSuppressed());
		verify(connection, never()).setAutoCommit(true);
		verify(connection).close();
	}

	@Test
	void connectionBasedQueryParticipatesInCallersTransaction() throws Exception {
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:borrowed_" + UUID.randomUUID())) {
			try (var statement = connection.createStatement()) {
				statement.execute("CREATE TABLE ITEMS (ID INT PRIMARY KEY)");
			}
			connection.setAutoCommit(false);
			insert(connection, 1);
			var rows = QueryExecutionUtility.flushRsToMap(connection,
					new QueryExecutionUtility.ParameterizedQuery("SELECT ID FROM ITEMS WHERE ID=?", List.of(1), 1), 3);
			assertEquals(1, rows.size());
			assertEquals(1, ((Number) rows.get(0).get("ID")).intValue());
			assertFalse(connection.isClosed());
			assertFalse(connection.getAutoCommit());
			connection.rollback();
			assertTrue(QueryExecutionUtility
					.flushRsToMap(connection,
							new QueryExecutionUtility.ParameterizedQuery("SELECT ID FROM ITEMS", List.of(), 0), 0)
					.isEmpty());
		}
	}

	@Test
	void returnsRealHikariConnectionsAfterSuccessAndFailure() throws Exception {
		try (var pool = new com.zaxxer.hikari.HikariDataSource()) {
			pool.setJdbcUrl("jdbc:h2:mem:pool_" + UUID.randomUUID());
			pool.setMaximumPoolSize(1);
			try (Connection connection = pool.getConnection(); var statement = connection.createStatement()) {
				statement.execute("CREATE TABLE ITEMS (ID INT PRIMARY KEY)");
			}
			IRDBMSEngine engine = mock(IRDBMSEngine.class);
			when(engine.isConnectionPooling()).thenReturn(true);
			when(engine.getConnection()).thenAnswer(i -> pool.getConnection());
			QueryExecutionUtility.write(engine, c -> {
				insert(c, 1);
				return null;
			});
			assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
			assertThrows(SQLException.class, () -> QueryExecutionUtility.write(engine, c -> {
				insert(c, 2);
				insert(c, 1);
				return null;
			}));
			assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
			int count = QueryExecutionUtility.read(engine, c -> {
				try (var statement = c.prepareStatement("SELECT COUNT(*) FROM ITEMS");
						var rs = statement.executeQuery()) {
					rs.next();
					return rs.getInt(1);
				}
			});
			assertEquals(1, count);
			assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
			assertFalse(pool.isClosed());
		}
	}

}
