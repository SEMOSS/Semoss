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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;

class QueryExecutionUtilityUnitTests {

	private Connection connection;
	private IRDBMSEngine engine;
	private PreparedStatement statement;
	private ResultSet result;

	@BeforeEach
	void setup() throws Exception {
		connection = spy(DriverManager.getConnection("jdbc:h2:mem:parameterized_" + UUID.randomUUID()));
		engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		doAnswer(invocation -> {
			statement = spy((PreparedStatement) invocation.callRealMethod());
			doAnswer(execution -> {
				result = (ResultSet) execution.callRealMethod();
				return result;
			}).when(statement).executeQuery();
			return statement;
		}).when(connection).prepareStatement(anyString());
	}

	@AfterEach
	void cleanup() throws Exception {
		connection.close();
	}

	@Test
	void bindsOrderedValuesAndNullsWhilePreservingTypesAndHeaders() throws Exception {
		var query = new ParameterizedQuery("""
				SELECT X AS ID, CAST(? AS VARCHAR) AS "label", CAST(? AS INTEGER) AS NULL_VALUE,
				CAST(? AS TIMESTAMP) AS CREATED FROM SYSTEM_RANGE(1, 4) WHERE X > ? ORDER BY X
				""",
				Arrays.asList("O'Brien \u2014 r\u00e9sum\u00e9", null, Timestamp.valueOf("2024-03-01 12:00:00"), 1), 2);

		var rows = QueryExecutionUtility.flushRsToMap(engine, query, 7);

		assertEquals(2, rows.size());
		assertEquals(2L, ((Number) rows.get(0).get("ID")).longValue());
		assertEquals(3L, ((Number) rows.get(1).get("ID")).longValue());
		assertEquals("O'Brien \u2014 r\u00e9sum\u00e9", rows.get(0).get("label"));
		assertTrue(rows.get(0).containsKey("NULL_VALUE"));
		assertNull(rows.get(0).get("NULL_VALUE"));
		assertInstanceOf(SemossDate.class, rows.get(0).get("CREATED"));
		verify(statement).setMaxRows(2);
		verify(statement).setQueryTimeout(7);
		assertTrue(result.isClosed());
		assertTrue(statement.isClosed());
		assertFalse(connection.isClosed());
	}

	@Test
	void supportsJdbcZeroLimitsAndReleasesPooledConnection() throws Exception {
		when(engine.isConnectionPooling()).thenReturn(true);
		var query = new ParameterizedQuery("SELECT X FROM SYSTEM_RANGE(1, 4)", List.of(), 0);

		assertEquals(4, QueryExecutionUtility.flushRsToMap(engine, query, 0).size());
		verify(statement).setMaxRows(0);
		verify(statement).setQueryTimeout(0);
		assertTrue(result.isClosed());
		assertTrue(statement.isClosed());
		assertTrue(connection.isClosed());
	}

	@Test
	void closesResourcesWhenParameterBindingFails() throws Exception {
		when(engine.isConnectionPooling()).thenReturn(true);
		var query = new ParameterizedQuery("SELECT CAST(? AS INTEGER) AS N", List.of(1, 2), 1);

		var error = assertThrows(IllegalArgumentException.class,
				() -> QueryExecutionUtility.flushRsToMap(engine, query, 5));

		assertEquals("Error executing parameterized query", error.getMessage());
		assertInstanceOf(SQLException.class, error.getCause());
		verify(statement, never()).executeQuery();
		assertTrue(statement.isClosed());
		assertTrue(connection.isClosed());
	}

	@Test
	void closesResourcesWhenExecutionFails() throws Exception {
		when(engine.isConnectionPooling()).thenReturn(true);
		var query = new ParameterizedQuery("SELECT CAST(? AS INTEGER) AS N", List.of("not a number"), 1);

		var error = assertThrows(IllegalArgumentException.class,
				() -> QueryExecutionUtility.flushRsToMap(engine, query, 5));

		assertEquals("Error executing parameterized query", error.getMessage());
		assertInstanceOf(SQLException.class, error.getCause());
		verify(statement).executeQuery();
		assertTrue(statement.isClosed());
		assertTrue(connection.isClosed());
	}

	@Test
	void closesResultSetAndConnectionWhenReadingFails() throws Exception {
		var failingStatement = connection.prepareStatement("SELECT 1 AS N");
		var failingResult = spy(failingStatement.executeQuery());
		doThrow(new SQLException("Read failed")).when(failingResult).next();
		doReturn(failingResult).when(failingStatement).executeQuery();
		doReturn(failingStatement).when(connection).prepareStatement(anyString());
		when(engine.isConnectionPooling()).thenReturn(true);

		var error = assertThrows(IllegalArgumentException.class, () -> QueryExecutionUtility.flushRsToMap(engine,
				new ParameterizedQuery("SELECT 1 AS N", List.of(), 1), 5));

		assertEquals("Error executing parameterized query", error.getMessage());
		assertTrue(failingResult.isClosed());
		assertTrue(failingStatement.isClosed());
		assertTrue(connection.isClosed());
	}

	@Test
	void rejectsInvalidArgumentsBeforeOpeningAStatement() {
		var query = new ParameterizedQuery("SELECT 1", List.of(), 1);
		assertThrows(IllegalArgumentException.class,
				() -> QueryExecutionUtility.flushRsToMap((IRDBMSEngine) null, query, 5));
		assertThrows(IllegalArgumentException.class, () -> QueryExecutionUtility.flushRsToMap(engine, null, 5));
		assertThrows(IllegalArgumentException.class, () -> QueryExecutionUtility.flushRsToMap(engine, query, -1));
		for (var invalid : List.of(new ParameterizedQuery(null, List.of(), 1),
				new ParameterizedQuery(" ", List.of(), 1), new ParameterizedQuery("SELECT 1", null, 1),
				new ParameterizedQuery("SELECT 1", List.of(), -1))) {
			assertThrows(IllegalArgumentException.class, () -> QueryExecutionUtility.flushRsToMap(engine, invalid, 5));
		}
		verifyNoInteractions(engine);
	}
}
