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
package prerna.auth.utils;

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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.utils.EnterpriseUsageUtils.FilterDimension;
import prerna.auth.utils.EnterpriseUsageUtils.Source;
import prerna.auth.utils.EnterpriseUsageUtils.View;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;
import prerna.util.SystemEngineRegistry;

/** Real H2 integration tests of the server-owned, bound query plans. */
class EnterpriseUsageUtilsUnitTests {

	private Connection connection;

	@BeforeEach
	void fixture() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:usage_" + java.util.UUID.randomUUID());
		org.h2.tools.RunScript.execute(connection, new java.io.StringReader(
				"""
						CREATE TABLE MESSAGE (MESSAGE_ID VARCHAR, TRANSACTION_ID VARCHAR, MESSAGE_TYPE VARCHAR, MESSAGE_DATA BLOB, MESSAGE_TOKENS INT, CACHE_READ_TOKENS INT, CACHE_CREATION_TOKENS INT, THINKING_TOKENS INT, MESSAGE_METHOD VARCHAR, RESPONSE_TIME DOUBLE, DATE_CREATED TIMESTAMP, AGENT_ID VARCHAR, ROOM_ID VARCHAR, USER_ID VARCHAR, USER_NAME VARCHAR);
						CREATE TABLE SMSS_USER (ID VARCHAR, NAME VARCHAR, USERNAME VARCHAR, TYPE VARCHAR);
						CREATE TABLE PROJECT (PROJECTID VARCHAR, PROJECTNAME VARCHAR, PROJECTDISPLAYNAME VARCHAR);
						CREATE TABLE ENGINE (ENGINEID VARCHAR, ENGINENAME VARCHAR, ENGINEDISPLAYNAME VARCHAR, ENGINETYPE VARCHAR, ENGINESUBTYPE VARCHAR);
						INSERT INTO SMSS_USER VALUES ('u-1', 'O''Brien', 'obrien', 'NATIVE'), ('u-1', 'O''Brien', 'obrien', 'SAML'), ('u-2', '', 'Other', 'NATIVE'), ('u-3', 'Literal_%!', NULL, 'NATIVE');
						INSERT INTO PROJECT VALUES ('app-1', 'finance_internal', 'Finance'), ('app-2', 'Legacy', NULL);
						INSERT INTO ENGINE VALUES ('model-1', 'model_internal', 'Model One', 'MODEL', 'OPEN_AI'), ('function-1', 'Function One', NULL, 'FUNCTION', 'REST'), ('legacy-1', 'Legacy Model', NULL, 'MODEL', NULL);
						CREATE TABLE ROOM (ROOM_ID VARCHAR, PROJECT_ID VARCHAR, PROJECT_NAME VARCHAR);
						CREATE TABLE AGENT (AGENT_ID VARCHAR, AGENT_NAME VARCHAR);
						CREATE TABLE FEEDBACK (MESSAGE_ID VARCHAR, MESSAGE_TYPE VARCHAR, RATING BOOLEAN);
						CREATE TABLE AUDIT_LOGS (LOG_ID VARCHAR, REQUEST_ID VARCHAR, USER_ID VARCHAR, USER_NAME VARCHAR, PROJECT_ID VARCHAR, PROJECT_NAME VARCHAR, ENGINE_ID VARCHAR, ENGINE_NAME VARCHAR, ENGINE_TYPE VARCHAR, ROOM_ID VARCHAR, METHOD_NAME VARCHAR, IS_SUCCESS BOOLEAN, LOG_LEVEL VARCHAR, LOG_TIMESTAMP TIMESTAMP, MESSAGE CLOB, REQUEST CLOB, RESPONSE CLOB);
						INSERT INTO ROOM VALUES ('room-1','app-1','Finance'), ('room-1','app-1','Finance');
						INSERT INTO AGENT VALUES ('model-1','Model One'), ('model-1','Model One');
						INSERT INTO MESSAGE (MESSAGE_ID, TRANSACTION_ID, MESSAGE_TYPE, MESSAGE_TOKENS, MESSAGE_METHOD, RESPONSE_TIME, DATE_CREATED, AGENT_ID, ROOM_ID, USER_ID, USER_NAME) VALUES
						 ('i-1','tx-1','INPUT',100,'ask',1000,'2024-03-01 00:00:00','model-1','room-1','u-1','O''Brien'),
						 ('r-1','tx-1','RESPONSE',40,'ask',1000,'2024-03-01 00:00:01','model-1','room-1','u-1','O''Brien'),
						 ('i-2','tx-2','INPUT',200,'ask',3000,'2024-03-31 23:59:58','model-1','room-1','u-1','O''Brien'),
						 ('r-2','tx-2','RESPONSE',60,'ask',3000,'2024-03-31 23:59:59','model-1','room-1','u-1','O''Brien'),
						 ('i-3','tx-3','INPUT',NULL,'ask',NULL,'2024-03-02 12:00:00','model-2',NULL,'u-2','Other'),
						 ('outside','tx-out','INPUT',999,'ask',500,'2024-04-01 00:00:00','model-1','room-1','u-1','O''Brien'),
						 ('vector','tx-v','INPUT',999,'nearestNeighbor',500,'2024-03-02 00:00:00','vector-1',NULL,'u-1','O''Brien');
						INSERT INTO FEEDBACK VALUES ('r-1','RESPONSE',TRUE), ('r-1','RESPONSE',TRUE), ('r-2','RESPONSE',FALSE), ('i-1','INPUT',TRUE);
						INSERT INTO AUDIT_LOGS (LOG_ID, USER_ID, PROJECT_ID, ENGINE_ID, ENGINE_TYPE, IS_SUCCESS, LOG_TIMESTAMP) VALUES
						 ('log-1','u-1','app-1','model-1','MODEL',TRUE,'2024-03-01 00:00:00'),
						 ('log-2','u-1','app-1','model-1','MODEL',FALSE,'2024-03-31 23:59:59'),
						 ('log-3','u-2',NULL,'function-1','FUNCTION',NULL,'2024-03-02 00:00:00'),
						 ('log-out','u-1','app-1','model-1','MODEL',TRUE,'2024-04-01 00:00:00');
						"""));
	}

	@AfterEach
	void close() throws Exception {
		connection.close();
	}

	private ParameterizedQuery query(Source source, View view, String user, String model, String dimension, int limit,
			int offset) {
		return EnterpriseUsageUtils.reportQuery(source, view, "2024-03-01", "2024-03-31", user, "", model, dimension,
				limit, offset);
	}

	private List<Map<String, Object>> rows(ParameterizedQuery query) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
			statement.setMaxRows(query.limit());
			for (int i = 0; i < query.parameters().size(); i++) {
				statement.setObject(i + 1, query.parameters().get(i));
			}
			try (ResultSet result = statement.executeQuery()) {
				List<Map<String, Object>> rows = new ArrayList<>();
				while (result.next()) {
					Map<String, Object> row = new LinkedHashMap<>();
					for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
						row.put(result.getMetaData().getColumnLabel(i), result.getObject(i));
					}
					rows.add(row);
				}
				return rows;
			}
		}
	}

	@Test
	void aggregatesPairsWithoutMultiplyingLookupRows() throws Exception {
		var row = rows(query(Source.MODEL, View.SUMMARY, "", "", null, 1, 0)).get(0);
		assertEquals(3L, ((Number) row.get("REQUESTS")).longValue());
		assertEquals(400L, ((Number) row.get("TOKENS")).longValue());
		assertEquals(2L, ((Number) row.get("USERS")).longValue());
		assertEquals(1L, ((Number) row.get("APPS")).longValue());
		assertEquals(2000d, ((Number) row.get("LATENCY_MS")).doubleValue());
		assertEquals(5L, ((Number) row.get("MESSAGE_ROWS")).longValue());
		assertNull(row.get("CACHE_CREATION_TOKENS"));
	}

	@Test
	void preservesMissingTokensAndEmptyPeriods() throws Exception {
		var unknown = rows(query(Source.MODEL, View.SUMMARY, "=u-2", "", null, 1, 0)).get(0);
		assertNull(unknown.get("TOKENS"));
		assertNull(unknown.get("INPUT_TOKENS"));
		var empty = rows(query(Source.MODEL, View.SUMMARY, "absent", "", null, 1, 0)).get(0);
		assertEquals(0L, ((Number) empty.get("TOKENS")).longValue());
	}

	@Test
	void bindsNameSearchAndExactIdentifiers() throws Exception {
		var query = query(Source.MODEL, View.SUMMARY, "O'Brien", "=model-1", null, 1, 0);
		assertFalse(query.sql().contains("O'Brien"));
		assertTrue(query.parameters().contains("%o'brien%"));
		assertTrue(query.parameters().contains("model-1"));
		assertEquals(400L, ((Number) rows(query).get(0).get("TOKENS")).longValue());
		var literal = query(Source.MODEL, View.SUMMARY, "name_%!", "", null, 1, 0);
		assertTrue(literal.parameters().contains("%name!_!%!!%"));
		assertEquals(0L, ((Number) rows(literal).get(0).get("REQUESTS")).longValue());
	}

	@Test
	void executesEveryViewAndPaginatesOnlyMetadata() throws Exception {
		assertEquals(3, rows(query(Source.MODEL, View.TREND, "", "", null, 366, 0)).size());
		assertEquals(3000d, ((Number) rows(query(Source.MODEL, View.LATENCY, "", "", null, 1, 0)).get(0).get("P95_MS"))
				.doubleValue());
		var feedback = rows(query(Source.MODEL, View.FEEDBACK, "", "", null, 1, 0)).get(0);
		assertEquals(2L, ((Number) feedback.get("RATINGS")).longValue());
		assertEquals(1L, ((Number) feedback.get("POSITIVE")).longValue());
		for (String group : List.of("model", "user", "app")) {
			assertEquals(2, rows(query(Source.MODEL, View.RANKING, "", "", group, 20, 0)).size());
		}
		var page = query(Source.MODEL, View.LOGS, "", "", null, 2, 2);
		assertFalse(page.sql().contains("MESSAGE_DATA"));
		var records = rows(page);
		assertEquals(2, records.size());
		assertEquals(3L, ((Number) records.get(0).get("ROW_NUM")).longValue());
		var activity = rows(query(Source.ACTIVITY, View.SUMMARY, "", "", null, 1, 0)).get(0);
		assertEquals(3L, ((Number) activity.get("EVENTS")).longValue());
		assertEquals(2L, ((Number) activity.get("KNOWN_OUTCOMES")).longValue());
		assertEquals(3, rows(query(Source.ACTIVITY, View.TREND, "", "", null, 366, 0)).size());
		assertEquals(1, rows(query(Source.ACTIVITY, View.LOGS, "", "", null, 2, 2)).size());
		assertEquals(2L,
				((Number) rows(query(Source.ACTIVITY, View.SUMMARY, "", "=model-1", null, 1, 0)).get(0).get("EVENTS"))
						.longValue());
	}

	@Test
	void engineFilterUsesTheSameIdentityAcrossLogSourcesAndIncludesNonModelEngines() throws Exception {
		var model = query(Source.MODEL, View.SUMMARY, "", "=model-1", null, 1, 0);
		var activity = query(Source.ACTIVITY, View.SUMMARY, "", "=model-1", null, 1, 0);
		assertTrue(model.sql().contains("m.AGENT_ID = ?"));
		assertTrue(activity.sql().contains("l.ENGINE_ID = ?"));
		assertEquals(2L, ((Number) rows(model).get(0).get("REQUESTS")).longValue());
		assertEquals(2L, ((Number) rows(activity).get(0).get("EVENTS")).longValue());
		assertEquals(1L, ((Number) rows(query(Source.ACTIVITY, View.SUMMARY, "", "=function-1", null, 1, 0)).get(0)
				.get("EVENTS")).longValue());
	}

	@Test
	void catalogChoicesBindSearchResolveTypesAndProjectOnlyIdentityFields() throws Exception {
		var query = EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.USER, "O'Brien", null, 50, 0);
		assertFalse(query.sql().contains("O'Brien"));
		assertTrue(query.parameters().contains("%o'brien%"));
		var choices = rows(query);
		assertEquals(2, choices.size());
		assertEquals(java.util.Set.of("ENTITY_ID", "ENTITY_NAME", "ENTITY_TYPE", "ENTITY_SUBTYPE"),
				choices.get(0).keySet());
		assertEquals("NATIVE", choices.get(0).get("ENTITY_TYPE"));
		assertEquals("SAML", choices.get(1).get("ENTITY_TYPE"));
		assertEquals("", choices.get(0).get("ENTITY_SUBTYPE"));
		assertEquals("u-3", rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.USER, "_%!", null, 50, 0))
				.get(0).get("ENTITY_ID"));
		assertEquals("Other", rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.USER, "", "u-2", 50, 0))
				.get(0).get("ENTITY_NAME"));
		assertEquals(0, rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.USER, "", "u", 50, 0)).size());
		assertEquals("Finance", rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.APP, "app-1", null, 50, 0))
				.get(0).get("ENTITY_NAME"));
		assertEquals("FUNCTION",
				rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.ENGINE, "", "function-1", 50, 0)).get(0)
						.get("ENTITY_TYPE"));
		assertEquals("REST",
				rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.ENGINE, "", "function-1", 50, 0)).get(0)
						.get("ENTITY_SUBTYPE"));
		assertEquals("OPEN_AI",
				rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.ENGINE, "Model One", null, 50, 0)).get(0)
						.get("ENTITY_SUBTYPE"));
		assertNull(rows(EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.ENGINE, "", "legacy-1", 50, 0)).get(0)
				.get("ENTITY_SUBTYPE"));
		assertThrows(IllegalArgumentException.class,
				() -> EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.USER, "", null, 101, 0));
		assertThrows(IllegalArgumentException.class,
				() -> EnterpriseUsageUtils.filterOptionsQuery(FilterDimension.USER, "", null, 50, Integer.MAX_VALUE));
	}

	@Test
	void catalogPagesHaveBoundedRowsAndAccurateContinuationAndCloseStatements() throws Exception {
		var engine = mock(IRDBMSEngine.class);
		var statements = new ArrayList<PreparedStatement>();
		connection = spy(connection);
		when(engine.getConnection()).thenReturn(connection);
		doAnswer(invocation -> {
			var statement = spy((PreparedStatement) invocation.callRealMethod());
			statements.add(statement);
			return statement;
		}).when(connection).prepareStatement(anyString());
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class)) {
			registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(engine);
			var first = EnterpriseUsageUtils.filterOptions(FilterDimension.USER, "", null, 2, 0);
			var second = EnterpriseUsageUtils.filterOptions(FilterDimension.USER, "", null, 2, 2);
			assertEquals(true, first.get("hasMore"));
			assertEquals(false, second.get("hasMore"));
			assertEquals(2, ((List<?>) first.get("rows")).size());
			assertEquals(2, ((List<?>) second.get("rows")).size());
			var ids = new java.util.HashSet<>();
			for (var page : List.of(first, second)) {
				for (Object row : (List<?>) page.get("rows")) {
					var entity = (Map<?, ?>) row;
					assertTrue(ids.add(entity.get("ENTITY_ID") + ":" + entity.get("ENTITY_TYPE")));
				}
			}
			for (var statement : statements) {
				verify(statement).setMaxRows(3);
				verify(statement).setQueryTimeout(30);
				assertTrue(statement.isClosed());
			}
			registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(null);
			assertThrows(IllegalArgumentException.class,
					() -> EnterpriseUsageUtils.filterOptions(FilterDimension.USER, "", null, 50, 0));
		}
	}

	@Test
	void pairsBodiesFromServerIdentityAndHandlesLegacyNullTransactions() throws Exception {
		assertEquals(2, rows(EnterpriseUsageUtils.detailQuery(Source.MODEL, "i-1")).size());
		assertEquals(1, rows(EnterpriseUsageUtils.detailQuery(Source.ACTIVITY, "log-1")).size());
		assertEquals(0, rows(EnterpriseUsageUtils.detailQuery(Source.MODEL, "absent")).size());
		try (var statement = connection.createStatement()) {
			statement.execute(
					"UPDATE MESSAGE SET TRANSACTION_ID = NULL, MESSAGE_ID = 'legacy' WHERE TRANSACTION_ID = 'tx-1'");
		}
		assertEquals(2, rows(EnterpriseUsageUtils.detailQuery(Source.MODEL, "legacy")).size());
	}

	@Test
	void productionReaderReturnsSerializableRowsAndClosesStatements() throws Exception {
		var engine = mock(IRDBMSEngine.class);
		var statementRef = new java.util.concurrent.atomic.AtomicReference<PreparedStatement>();
		connection = spy(connection);
		when(engine.getConnection()).thenReturn(connection);
		doAnswer(invocation -> {
			var statement = spy((PreparedStatement) invocation.callRealMethod());
			statementRef.set(statement);
			return statement;
		}).when(connection).prepareStatement(anyString());
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class)) {
			registry.when(SystemEngineRegistry::getModelInferenceLogsDb).thenReturn(engine);
			var result = EnterpriseUsageUtils.report(Source.MODEL, View.LOGS, "2024-03-01", "2024-03-31", "", "", "",
					null, 2, 0);
			assertEquals(2, result.get("limit"));
			assertTrue(result.get("rows") instanceof List<?>);
			var records = (List<?>) result.get("rows");
			assertEquals(2, records.size());
			assertInstanceOf(String.class, ((Map<?, ?>) records.get(0)).get("TIME"));
			assertEquals("2024-03-31 23:59:59", ((Map<?, ?>) records.get(0)).get("TIME"));
			verify(statementRef.get()).setMaxRows(2);
			verify(statementRef.get()).setQueryTimeout(30);
			assertTrue(statementRef.get().isClosed());
			var summary = (List<?>) EnterpriseUsageUtils
					.report(Source.MODEL, View.SUMMARY, "2024-03-01", "2024-03-31", "=u-2", "", "", null, 1, 0)
					.get("rows");
			assertTrue(((Map<?, ?>) summary.get(0)).containsKey("TOKENS"));
			assertNull(((Map<?, ?>) summary.get(0)).get("TOKENS"));
			var trend = (List<?>) EnterpriseUsageUtils
					.report(Source.MODEL, View.TREND, "2024-03-01", "2024-03-31", "", "", "", null, 366, 0).get("rows");
			assertEquals("2024-03-01", ((Map<?, ?>) trend.get(0)).get("DAY"));
			assertEquals("2024-03-31", ((Map<?, ?>) trend.get(2)).get("DAY"));
			registry.when(SystemEngineRegistry::getModelInferenceLogsDb).thenReturn(null);
			assertThrows(IllegalArgumentException.class, () -> EnterpriseUsageUtils.report(Source.MODEL, View.SUMMARY,
					"2024-03-01", "2024-03-31", "", "", "", null, 1, 0));
		}
	}

	@Test
	void productionReaderDecodesBodiesAndClosesPooledConnections() throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute(
					"UPDATE MESSAGE SET MESSAGE_DATA = STRINGTOUTF8('Revenue \u2014 r\u00e9sum\u00e9') WHERE MESSAGE_ID = 'i-1'");
			statement.execute(
					"UPDATE AUDIT_LOGS SET MESSAGE = 'Request completed', REQUEST = '{\"text\":\"r\u00e9sum\u00e9\"}' WHERE LOG_ID = 'log-1'");
		}
		var engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class)) {
			registry.when(SystemEngineRegistry::getModelInferenceLogsDb).thenReturn(engine);
			registry.when(SystemEngineRegistry::getAuditLogsDb).thenReturn(engine);
			var messages = (List<?>) EnterpriseUsageUtils.detail(Source.MODEL, "i-1").get("rows");
			assertEquals("Revenue \u2014 r\u00e9sum\u00e9", ((Map<?, ?>) messages.get(0)).get("MESSAGE_DATA"));
			assertNull(((Map<?, ?>) messages.get(1)).get("MESSAGE_DATA"));
			when(engine.isConnectionPooling()).thenReturn(true);
			var activities = (List<?>) EnterpriseUsageUtils.detail(Source.ACTIVITY, "log-1").get("rows");
			assertEquals("Request completed", ((Map<?, ?>) activities.get(0)).get("MESSAGE"));
			assertEquals("{\"text\":\"r\u00e9sum\u00e9\"}", ((Map<?, ?>) activities.get(0)).get("REQUEST"));
			assertNull(((Map<?, ?>) activities.get(0)).get("RESPONSE"));
			assertTrue(connection.isClosed());
		}
	}

	@Test
	void productionReaderClosesResourcesWhenExecutionFails() throws Exception {
		var engine = mock(IRDBMSEngine.class);
		var statement = spy(connection.prepareStatement("SELECT ? AS RECORD_ID"));
		connection = spy(connection);
		when(engine.getConnection()).thenReturn(connection);
		doReturn(statement).when(connection).prepareStatement(anyString());
		when(engine.isConnectionPooling()).thenReturn(true);
		doThrow(new java.sql.SQLException("Internal query detail")).when(statement).executeQuery();
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class)) {
			registry.when(SystemEngineRegistry::getAuditLogsDb).thenReturn(engine);
			var error = assertThrows(IllegalArgumentException.class,
					() -> EnterpriseUsageUtils.detail(Source.ACTIVITY, "log-1"));
			assertFalse(error.getMessage().contains("Internal query detail"));
			verify(statement).executeQuery();
			assertTrue(statement.isClosed());
			assertTrue(connection.isClosed());
		}
	}

	@Test
	void rejectsUnsupportedViewsAndUnboundedRequests() {
		assertThrows(IllegalArgumentException.class, () -> query(Source.ACTIVITY, View.LATENCY, "", "", null, 1, 0));
		assertThrows(IllegalArgumentException.class,
				() -> query(Source.MODEL, View.RANKING, "", "", "arbitraryColumn", 20, 0));
		assertThrows(IllegalArgumentException.class, () -> query(Source.MODEL, View.LOGS, "", "", null, 5001, 0));
		assertThrows(IllegalArgumentException.class, () -> query(Source.MODEL, View.LOGS, "", "", null, 25, -1));
		assertThrows(IllegalArgumentException.class, () -> EnterpriseUsageUtils.reportQuery(Source.MODEL, View.SUMMARY,
				"2024-03-01", "2025-04-01", "", "", "", null, 1, 0));
		assertThrows(IllegalArgumentException.class,
				() -> EnterpriseUsageUtils.option(Source.class, "externalDatabase"));
		assertThrows(IllegalArgumentException.class, () -> EnterpriseUsageUtils.detailQuery(Source.MODEL, ""));
	}
}
