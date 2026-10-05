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
package prerna.util.sql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;

class PostgresJdbcUnitTests {

	@Test
	void disposablePostgresRoundTripsAndEndsManualReadTransactions() throws Exception {
		assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Disposable PostgreSQL requires Docker");
		try (var postgres = new GenericContainer<>("postgres:16-alpine").withEnv("POSTGRES_PASSWORD", "test-only")
				.withEnv("POSTGRES_DB", "binding_test").withExposedPorts(5432).waitingFor(Wait.forListeningPort())) {
			postgres.start();
			try (Connection connection = DriverManager.getConnection(
					"jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/binding_test",
					"postgres", "test-only")) {
				NullableParameterBindingUnitTests.roundTrip(connection, new PostgresQueryUtil());
				try (var statement = connection.createStatement()) {
					statement.execute("CREATE TABLE STATEMENT_VALUES (ID INT PRIMARY KEY, V TEXT)");
				}
				connection.setAutoCommit(false);
				IRDBMSEngine engine = mock(IRDBMSEngine.class);
				when(engine.getConnection()).thenReturn(connection);
				String insert = "INSERT INTO STATEMENT_VALUES VALUES (?, ?)";
				assertEquals(1, QueryExecutionUtility.executeUpdate(engine, insert, ps -> {
					ps.setInt(1, 1);
					ps.setString(2, "O'Brien ? \u03bb");
				}));
				assertArrayEquals(new int[] { 1, 1 },
						QueryExecutionUtility.executeBatch(engine, insert, List.of(2, 3), (ps, id) -> {
							ps.setInt(1, id);
							new PostgresQueryUtil().setNullableString(ps, 2, id == 2 ? null : "");
						}));
				assertThrows(SQLException.class,
						() -> QueryExecutionUtility.executeBatch(engine, insert, List.of(4, 1), (ps, id) -> {
							ps.setInt(1, id);
							ps.setString(2, "must roll back");
						}));
				assertEquals(Arrays.asList("O'Brien ? \u03bb", null, ""),
						QueryExecutionUtility.queryList(engine, "SELECT V FROM STATEMENT_VALUES ORDER BY ID", ps -> {
						}, rs -> rs.getString(1)));
				int count = QueryExecutionUtility.queryOne(engine, "SELECT COUNT(*) FROM BIND_VALUES", ps -> {
				}, rs -> rs.getInt(1));
				assertEquals(5, count);
				assertEquals(TransactionState.IDLE, connection.unwrap(BaseConnection.class).getTransactionState());
				assertFalse(connection.getAutoCommit());
				try (Connection observer = DriverManager.getConnection(connection.getMetaData().getURL(), "postgres",
						"test-only");
						var ps = observer
								.prepareStatement("SELECT state, xact_start FROM pg_stat_activity WHERE pid=?")) {
					ps.setInt(1, connection.unwrap(BaseConnection.class).getBackendPID());
					try (var rs = ps.executeQuery()) {
						assertTrue(rs.next());
						assertEquals("idle", rs.getString(1));
						assertNull(rs.getTimestamp(2));
					}
				}
			}
		}
	}
}
