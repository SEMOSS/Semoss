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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.engine.api.IRDBMSEngine;

class QueryExecutionStatementUnitTests {

	private static final String INSERT = "INSERT INTO ITEMS (ID, V) VALUES (?, ?)";
	private static final String SELECT = "SELECT V FROM ITEMS ORDER BY ID";
	private JdbcTestDatabase db;

	@BeforeEach
	void setup() throws Exception {
		db = new JdbcTestDatabase();
		db.execute("CREATE TABLE ITEMS (ID INT PRIMARY KEY, V VARCHAR)");
	}

	@AfterEach
	void cleanup() throws Exception {
		db.close();
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void engineUpdateCommitsAndReturnsActualAffectedRows(boolean autoCommit) throws Exception {
		db.connection.setAutoCommit(autoCommit);
		assertEquals(1, QueryExecutionUtility.executeUpdate(db.engine, INSERT, ps -> {
			ps.setInt(1, 1);
			ps.setString(2, "O'Brien ? \u03bb");
		}));
		assertEquals(0,
				QueryExecutionUtility.executeUpdate(db.engine, "DELETE FROM ITEMS WHERE ID=?", ps -> ps.setInt(1, 99)));
		try (Connection observer = DriverManager.getConnection(db.connection.getMetaData().getURL())) {
			assertEquals("O'Brien ? \u03bb", QueryExecutionUtility.queryOne(observer, SELECT, ps -> {
			}, rs -> rs.getString(1)));
		}
		assertEquals(autoCommit, db.connection.getAutoCommit());
		assertFalse(db.connection.isClosed());
	}

	@Test
	void connectionOverloadsShareUncommittedWorkAndLeaveOwnershipToCaller() throws Exception {
		db.manual();
		QueryExecutionUtility.executeUpdate(db.connection, INSERT, ps -> {
			ps.setInt(1, 1);
			ps.setString(2, "one");
		});
		QueryExecutionUtility.executeBatch(db.connection, INSERT, List.of(2, 3), (ps, id) -> {
			ps.setInt(1, id);
			ps.setString(2, "row" + id);
		});
		assertEquals(List.of("row2", "row3"), QueryExecutionUtility.queryList(db.connection,
				"SELECT V FROM ITEMS WHERE ID >= ? ORDER BY ID", ps -> ps.setInt(1, 2), rs -> rs.getString(1)));
		assertEquals("one", QueryExecutionUtility.queryOne(db.connection, SELECT, ps -> {
		}, rs -> rs.getString(1)));
		verify(db.connection, never()).commit();
		verify(db.connection, never()).rollback();
		verify(db.connection, never()).close();
		verify(db.connection, never()).setAutoCommit(anyBoolean());
		db.connection.rollback();
		assertEquals(0, db.count("ITEMS"));
	}

	@Test
	void engineBatchRollsBackEarlierRowsWhenALaterRowFails() throws Exception {
		db.execute("INSERT INTO ITEMS VALUES (9, 'existing')");
		assertThrows(SQLException.class,
				() -> QueryExecutionUtility.executeBatch(db.engine, INSERT, List.of(1, 9), (ps, id) -> {
					ps.setInt(1, id);
					ps.setString(2, "new");
				}));
		assertEquals(1, db.count("ITEMS"));
		assertEquals("existing", db.value("SELECT V FROM ITEMS WHERE ID=9"));
		assertTrue(db.connection.getAutoCommit());
	}

	@Test
	void batchClearsBindingsInsteadOfReusingMissingValuesFromPreviousRow() throws Exception {
		assertThrows(SQLException.class,
				() -> QueryExecutionUtility.executeBatch(db.engine, INSERT, List.of(1, 2), (ps, id) -> {
					ps.setInt(1, id);
					if (id == 1) {
						ps.setString(2, "first only");
					}
				}));
		assertEquals(0, db.count("ITEMS"));
	}

	@Test
	void emptyBatchAndCustomBatchAssemblyRetainJdbcCounts() throws Exception {
		assertArrayEquals(new int[0], QueryExecutionUtility.executeBatch(db.engine, INSERT, List.of(),
				(ps, row) -> fail("An empty batch must not invoke its binder")));
		assertArrayEquals(new int[] { 1, 1 }, QueryExecutionUtility.executeBatch(db.engine, INSERT, ps -> {
			for (int id = 1; id <= 2; id++) {
				ps.setInt(1, id);
				db.engine.getQueryUtil().setNullableString(ps, 2, id == 1 ? null : "");
				ps.addBatch();
			}
		}));
		assertEquals(Arrays.asList(null, ""), QueryExecutionUtility.queryList(db.engine, SELECT, ps -> {
		}, rs -> rs.getString(1)));
	}

	@Test
	void driverBatchCountsAreReturnedUnchanged() throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(connection.prepareStatement(INSERT)).thenReturn(statement);
		int[] counts = { 3, Statement.SUCCESS_NO_INFO, Statement.EXECUTE_FAILED };
		when(statement.executeBatch()).thenReturn(counts);
		assertSame(counts, QueryExecutionUtility.executeBatch(connection, INSERT, ps -> ps.addBatch()));
		verify(statement).close();
		verify(connection, never()).commit();
	}

	@Test
	void firstRowDoesNotSkipNullOrVisitLaterRowsAndEmptyQueryReturnsNull() throws Exception {
		db.execute("INSERT INTO ITEMS VALUES (1, NULL), (2, 'second')");
		AtomicInteger mapped = new AtomicInteger();
		assertNull(QueryExecutionUtility.queryOne(db.engine, SELECT, ps -> {
		}, rs -> {
			mapped.incrementAndGet();
			return rs.getString(1);
		}));
		assertEquals(1, mapped.get());
		assertNull(QueryExecutionUtility.queryOne(db.engine, "SELECT V FROM ITEMS WHERE ID=?", ps -> ps.setInt(1, 99),
				rs -> {
					fail("No row to map");
					return null;
				}));
	}

	@Test
	void listPreservesOrderDuplicatesNullsAndRemainsMutable() throws Exception {
		db.execute("INSERT INTO ITEMS VALUES (1, 'same'), (2, NULL), (3, 'same')");
		List<String> rows = QueryExecutionUtility.queryList(db.engine, SELECT, ps -> {
		}, rs -> rs.getString(1));
		assertEquals(Arrays.asList("same", null, "same"), rows);
		rows.add("caller-owned");
		assertEquals(4, rows.size());
		assertTrue(QueryExecutionUtility
				.queryList(db.engine, "SELECT V FROM ITEMS WHERE ID=?", ps -> ps.setInt(1, 99), rs -> rs.getString(1))
				.isEmpty());
	}

	@Test
	void binderCanConfigureQueryLimitsAndTimeouts() throws Exception {
		db.execute("INSERT INTO ITEMS VALUES (1, 'first'), (2, 'second')");
		assertEquals(List.of("first"), QueryExecutionUtility.queryList(db.engine, SELECT, ps -> {
			ps.setMaxRows(1);
			ps.setQueryTimeout(2);
		}, rs -> rs.getString(1)));
	}

	@Test
	void explicitDestinationPreservesPartialRowsWhileFailurePropagates() throws Exception {
		db.execute("INSERT INTO ITEMS VALUES (1, 'first'), (2, 'second')");
		db.manual();
		List<String> rows = new ArrayList<>(List.of("existing"));
		SQLException failure = new SQLException("mapping failed");
		assertSame(failure,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.queryList(db.engine, SELECT, ps -> {
				}, rs -> {
					String value = rs.getString(1);
					if (value.equals("second")) {
						throw failure;
					}
					return value;
				}, rows)));
		assertEquals(List.of("existing", "first"), rows);
		verify(db.connection).rollback();
		assertFalse(db.connection.isClosed());
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void listReturnsProvidedDestinationAndRespectsTransactionOwnership(boolean engineOwnsTransaction) throws Exception {
		db.execute("INSERT INTO ITEMS VALUES (1, 'new')");
		db.manual();
		List<String> rows = new ArrayList<>(List.of("existing"));
		List<String> result = engineOwnsTransaction ? QueryExecutionUtility.queryList(db.engine, SELECT, ps -> {
		}, rs -> rs.getString(1), rows) : QueryExecutionUtility.queryList(db.connection, SELECT, ps -> {
		}, rs -> rs.getString(1), rows);
		assertSame(rows, result);
		assertEquals(List.of("existing", "new"), rows);
		verify(db.connection, times(engineOwnsTransaction ? 1 : 0)).rollback();
		verify(db.connection, never()).commit();
		verify(db.connection, never()).close();
		verify(db.connection, never()).setAutoCommit(anyBoolean());
	}

	@Test
	void multiStatementWriteUsesOneConnectionAndRollsBackAllHelpersTogether() throws Exception {
		clearInvocations(db.engine);
		assertThrows(SQLException.class, () -> QueryExecutionUtility.write(db.engine, connection -> {
			QueryExecutionUtility.executeUpdate(connection, INSERT, ps -> {
				ps.setInt(1, 1);
				ps.setString(2, "first statement");
			});
			return QueryExecutionUtility.executeBatch(connection, INSERT, List.of(2, 1), (ps, id) -> {
				ps.setInt(1, id);
				ps.setString(2, "batch");
			});
		}));
		verify(db.engine).getConnection();
		assertEquals(0, db.count("ITEMS"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "bind", "execute", "cursor", "map", "resultClose", "statementClose" })
	void queryFailureClosesResourcesRollsBackAndReleasesPooledConnection(String stage) throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet result = mock(ResultSet.class);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.isConnectionPooling()).thenReturn(true);
		when(engine.getConnection()).thenReturn(connection);
		when(connection.prepareStatement(SELECT)).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(result);
		when(result.next()).thenReturn(true, false);
		SQLException failure = new SQLException(stage);
		switch (stage) {
		case "execute" -> when(statement.executeQuery()).thenThrow(failure);
		case "cursor" -> when(result.next()).thenThrow(failure);
		case "resultClose" -> doThrow(failure).when(result).close();
		case "statementClose" -> doThrow(failure).when(statement).close();
		}
		assertSame(failure,
				assertThrows(SQLException.class, () -> QueryExecutionUtility.queryList(engine, SELECT, ps -> {
					if (stage.equals("bind")) {
						throw failure;
					}
				}, rs -> {
					if (stage.equals("map")) {
						throw failure;
					}
					return "row";
				})));
		verify(statement).close();
		if (!stage.equals("bind") && !stage.equals("execute")) {
			verify(result).close();
		}
		verify(connection).rollback();
		verify(connection).close();
		verify(connection, never()).commit();
	}

	@Test
	void checkedBindingFailureRetainsSuppressedCloseFailure() throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(connection.prepareStatement(INSERT)).thenReturn(statement);
		IOException failure = new IOException("encoding");
		SQLException cleanup = new SQLException("close");
		doThrow(cleanup).when(statement).close();
		assertSame(failure,
				assertThrows(IOException.class, () -> QueryExecutionUtility.executeUpdate(connection, INSERT, ps -> {
					throw failure;
				})));
		assertArrayEquals(new Throwable[] { cleanup }, failure.getSuppressed());
		verify(statement, never()).executeUpdate();
	}

	@Test
	void firstRowMappingFailureClosesBothResources() throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet result = mock(ResultSet.class);
		when(connection.prepareStatement(SELECT)).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(result);
		when(result.next()).thenReturn(true);
		IOException failure = new IOException("LOB decode");
		assertSame(failure,
				assertThrows(IOException.class, () -> QueryExecutionUtility.queryOne(connection, SELECT, ps -> {
				}, rs -> {
					throw failure;
				})));
		verify(result).close();
		verify(statement).close();
		verify(connection, never()).close();
	}
}
