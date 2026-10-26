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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
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
import java.time.ZoneId;
import java.time.ZoneOffset;
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
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.util.QueryExecutionUtility;
import prerna.util.QueryExecutionUtility.ParameterizedQuery;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/** Real H2 integration tests of the server-owned, bound query plans. */
class EnterpriseUsageUtilsUnitTests {

	private Connection connection;

	@BeforeEach
	void fixture() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:usage_" + java.util.UUID.randomUUID());
		org.h2.tools.RunScript.execute(connection, new java.io.StringReader(
				"""
						CREATE TABLE MESSAGE (MESSAGE_ID VARCHAR, TRANSACTION_ID VARCHAR, MESSAGE_TYPE VARCHAR, MESSAGE_DATA BLOB, MESSAGE_TOKENS INT, CACHE_READ_TOKENS INT, CACHE_CREATION_TOKENS INT, THINKING_TOKENS INT, MESSAGE_METHOD VARCHAR, RESPONSE_TIME DOUBLE, DATE_CREATED TIMESTAMP, AGENT_ID VARCHAR, ROOM_ID VARCHAR, USER_ID VARCHAR, USER_NAME VARCHAR, USER_TYPE VARCHAR, BILLED_SOURCE_TYPE VARCHAR, BILLED_SUBJECT_ID VARCHAR, BILLED_SUBJECT_TYPE VARCHAR, COST_MICROS BIGINT);
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

	/**
	 * Adds an audit trail with usage limit and access changes and denied requests
	 * in the report's dates, a login the audit source leaves out, and a change
	 * after the dates. The first and last days' events are far enough from UTC
	 * midnight to fall on the same days in any server's time zone.
	 *
	 * @return the user tracking database, read with no OWL
	 */
	private IRDBMSEngine auditTrail() throws Exception {
		org.h2.tools.RunScript.execute(connection, new java.io.StringReader(
				"""
						CREATE TABLE USER_AUDIT_EVENTS (EVENT_ID VARCHAR, EVENT_TIME TIMESTAMP, EVENT_TYPE VARCHAR,
								ACTION VARCHAR, STATUS VARCHAR, SEVERITY VARCHAR, ACTOR_USER_ID VARCHAR,
								ACTOR_USER_TYPE VARCHAR, ACTOR_USER_NAME VARCHAR, SUBJECT_USER_ID VARCHAR,
								SUBJECT_USER_TYPE VARCHAR, SUBJECT_USER_NAME VARCHAR, TARGET_TYPE VARCHAR, TARGET_ID VARCHAR,
								TARGET_NAME VARCHAR, PROJECT_ID VARCHAR, ENGINE_ID VARCHAR, OLD_VALUE CLOB, NEW_VALUE CLOB,
								DETAILS CLOB, ERROR_CODE VARCHAR, ERROR_MESSAGE CLOB);
						INSERT INTO USER_AUDIT_EVENTS (EVENT_ID, EVENT_TIME, EVENT_TYPE, STATUS, ACTOR_USER_ID, ACTOR_USER_NAME,
								SUBJECT_USER_ID, SUBJECT_USER_NAME, TARGET_TYPE, TARGET_ID, PROJECT_ID, ENGINE_ID, OLD_VALUE,
								NEW_VALUE, DETAILS) VALUES
						 ('limit-1','2024-03-01 12:00:00','USAGE_LIMIT_UPDATE','SUCCESS','owner-1','Owner','u-1','O''Brien','ENGINE','model-1',NULL,'model-1','{"usageLimit":null}','{"usageLimit":{"unlimited":true,"rules":[]}}','{"limitType":"DIRECT"}'),
						 ('grant-1','2024-03-31 09:00:00.123456','PERMISSION_ADD','SUCCESS','owner-1','Owner','u-2','Other','PROJECT','app-1','app-1',NULL,NULL,'{"permission":"READ_ONLY"}',NULL),
						 ('platform-1','2024-03-15 12:00:00','USAGE_LIMIT_UPDATE','SUCCESS','admin-1','Admin',NULL,NULL,'PLATFORM',NULL,NULL,NULL,NULL,NULL,'{"limitType":"PLATFORM"}'),
						 ('login-1','2024-03-02 00:00:00','LOGIN','SUCCESS','u-1','O''Brien',NULL,NULL,'USER','u-1',NULL,NULL,NULL,NULL,NULL),
						 ('outside','2024-04-01 12:00:00','PERMISSION_ADD','SUCCESS','owner-1','Owner','u-1','O''Brien','ENGINE','model-1',NULL,'model-1',NULL,NULL,NULL);
						INSERT INTO USER_AUDIT_EVENTS (EVENT_ID, EVENT_TIME, EVENT_TYPE, ACTION, STATUS, ACTOR_USER_ID,
								ACTOR_USER_NAME, TARGET_TYPE, TARGET_ID, ENGINE_ID, ERROR_CODE, ERROR_MESSAGE) VALUES
						 ('denied-1','2024-03-10 09:00:00','AUTHORIZATION_DENIED','SetEngineGlobal','DENIED','u-3','Third','ENGINE','model-1','model-1','PERMISSION_DENIED','User does not have permission'),
						 ('read-denied-1','2024-03-05 09:00:00','AUDIT_QUERY','AUDIT_QUERY','DENIED','u-1','O''Brien','ENGINE','model-1','model-1','AUDIT_ACCESS_DENIED','Resource audit access denied.');
						"""));
		var engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.isBasic()).thenReturn(true);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		return engine;
	}

	private static List<?> auditIds(String user, String app, String engine, int limit, int offset) {
		var rows = (List<?>) EnterpriseUsageUtils
				.report(Source.AUDIT, View.LOGS, "2024-03-01", "2024-03-31", user, app, engine, null, limit, offset)
				.get("rows");
		return rows.stream().map(row -> ((Map<?, ?>) row).get("EVENT_ID")).toList();
	}

	@Test
	void auditSourceCountsAndPagesOnlyChangesAndDeniedRequests() throws Exception {
		var engine = auditTrail();
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
			registry.when(SystemEngineRegistry::getUserTrackingDb).thenReturn(engine);
			var summary = EnterpriseUsageUtils.report(Source.AUDIT, View.SUMMARY, "2024-03-01", "2024-03-31", "", "",
					"", null, 1, 0);
			assertEquals("audit", summary.get("source"));
			// people making changes count only the changes' actors, and people denied
			// only the denied requests'
			assertEquals(
					Map.of("EVENTS", 5L, "USAGE_LIMIT_CHANGES", 2L, "ACCESS_CHANGES", 1L, "SETTING_CHANGES", 0L,
							"DENIALS", 2L, "ACTORS", 2L, "PEOPLE_DENIED", 2L, "PEOPLE_AFFECTED", 2L),
					((List<?>) summary.get("rows")).get(0));
			assertEquals(List.of("grant-1", "platform-1"), auditIds("", "", "", 2, 0));
			assertEquals(List.of("denied-1", "read-denied-1"), auditIds("", "", "", 2, 2));
			assertEquals(List.of("limit-1"), auditIds("", "", "", 2, 4));
			var newest = (Map<?, ?>) ((List<?>) EnterpriseUsageUtils
					.report(Source.AUDIT, View.LOGS, "2024-03-01", "2024-03-31", "", "", "", null, 1, 0).get("rows"))
					.get(0);
			// the audit trail keeps UTC times, which are read to the microsecond
			assertEquals("2024-03-31T09:00:00.123456Z", newest.get("EVENT_TIME"));
			assertFalse(newest.containsKey("NEW_VALUE"));
			var change = (List<?>) EnterpriseUsageUtils.detail(Source.AUDIT, "limit-1").get("rows");
			assertEquals("{\"usageLimit\":null}", ((Map<?, ?>) change.get(0)).get("OLD_VALUE"));
			assertEquals("{\"limitType\":\"DIRECT\"}", ((Map<?, ?>) change.get(0)).get("DETAILS"));
			var denial = (Map<?, ?>) ((List<?>) EnterpriseUsageUtils.detail(Source.AUDIT, "denied-1").get("rows"))
					.get(0);
			assertEquals("PERMISSION_DENIED", denial.get("ERROR_CODE"));
			assertEquals("User does not have permission", denial.get("ERROR_MESSAGE"));
			assertTrue(((List<?>) EnterpriseUsageUtils.detail(Source.AUDIT, "login-1").get("rows")).isEmpty());
		}
	}

	@Test
	void auditFiltersMatchEitherPersonAndTheAppOrEngineOfTheEvent() throws Exception {
		var engine = auditTrail();
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
			registry.when(SystemEngineRegistry::getUserTrackingDb).thenReturn(engine);
			// u-1 was denied a read, and is the person a limit was set for
			assertEquals(List.of("read-denied-1", "limit-1"), auditIds("=u-1", "", "", 25, 0));
			assertEquals(List.of("grant-1", "limit-1"), auditIds("=owner-1", "", "", 25, 0));
			assertEquals(List.of("read-denied-1", "limit-1"), auditIds("BRIEN", "", "", 25, 0));
			assertEquals(List.of("grant-1"), auditIds("", "=app-1", "", 25, 0));
			assertEquals(List.of("denied-1", "read-denied-1", "limit-1"), auditIds("", "", "=model-1", 25, 0));
			assertTrue(auditIds("=' OR 1=1 --", "", "", 25, 0).isEmpty());
			utility.when(Utility::isUserTrackingDisabled).thenReturn(true);
			assertThrows(IllegalArgumentException.class, () -> auditIds("", "", "", 25, 0));
		}
	}

	@Test
	void auditSourceCountsAppAndEngineSettingChangesButNotAppSaves() throws Exception {
		var engine = auditTrail();
		org.h2.tools.RunScript.execute(connection, new java.io.StringReader(
				"""
						INSERT INTO USER_AUDIT_EVENTS (EVENT_ID, EVENT_TIME, EVENT_TYPE, STATUS, ACTOR_USER_ID, ACTOR_USER_NAME,
								TARGET_TYPE, TARGET_ID, PROJECT_ID, DETAILS) VALUES
						 ('public-1','2024-03-20 12:00:00','PROJECT_GLOBAL_UPDATE','SUCCESS','owner-2','Second','PROJECT','app-1','app-1','{"field":"global","global":true}'),
						 ('saved-1','2024-03-21 12:00:00','PROJECT_BLOCKS_UPDATE','SUCCESS','owner-2','Second','PROJECT','app-1','app-1',NULL);
						"""));
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
			registry.when(SystemEngineRegistry::getUserTrackingDb).thenReturn(engine);
			var summary = (Map<?, ?>) ((List<?>) EnterpriseUsageUtils
					.report(Source.AUDIT, View.SUMMARY, "2024-03-01", "2024-03-31", "", "", "", null, 1, 0).get("rows"))
					.get(0);
			assertEquals(1L, summary.get("SETTING_CHANGES"));
			assertEquals(6L, summary.get("EVENTS"));
			// the person who made the app public counts as someone making changes
			assertEquals(3L, summary.get("ACTORS"));
			// saving an app's blocks is not a setting change, so the logs leave it out
			assertEquals(List.of("grant-1", "public-1"), auditIds("", "=app-1", "", 25, 0));
		}
	}

	/**
	 * Adds a call made after calls recorded their login type and cost: billed to a
	 * team, at 2,500 millionths of a dollar on its RESPONSE row. Every other model
	 * message was logged before costs were recorded.
	 */
	private void recordCostedCall() throws Exception {
		org.h2.tools.RunScript.execute(connection, new java.io.StringReader(
				"""
						INSERT INTO MESSAGE (MESSAGE_ID, TRANSACTION_ID, MESSAGE_TYPE, MESSAGE_TOKENS, CACHE_READ_TOKENS, CACHE_CREATION_TOKENS, THINKING_TOKENS, MESSAGE_METHOD, RESPONSE_TIME, DATE_CREATED, AGENT_ID, ROOM_ID, USER_ID, USER_NAME, USER_TYPE, BILLED_SOURCE_TYPE, BILLED_SUBJECT_ID, BILLED_SUBJECT_TYPE, COST_MICROS) VALUES
						 ('tx-c','tx-c','INPUT',1000,600,100,NULL,'ask',NULL,'2024-03-15 12:00:00','model-1','room-1','u-1','O''Brien','NATIVE','TEAM','Ops','CUSTOM',NULL),
						 ('tx-c','tx-c','RESPONSE',300,NULL,NULL,120,'ask',800,'2024-03-15 12:00:01','model-1','room-1','u-1','O''Brien','NATIVE','TEAM','Ops','CUSTOM',2500);
						"""));
	}

	/**
	 * Runs a report on the inference logs with model-1 priced at $1 a million input
	 * tokens and $2 a million output tokens, and every other model unpriced.
	 */
	private Map<String, Object> pricedReport(View view, String dimension, int limit) throws Exception {
		var engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		try (MockedStatic<SystemEngineRegistry> registry = mockStatic(SystemEngineRegistry.class);
				MockedStatic<ModelUsageCost> cost = mockStatic(ModelUsageCost.class, CALLS_REAL_METHODS)) {
			registry.when(SystemEngineRegistry::getModelInferenceLogsDb).thenReturn(engine);
			cost.when(() -> ModelUsageCost.priceEntry(anyString())).thenReturn(null);
			cost.when(() -> ModelUsageCost.priceEntry("model-1"))
					.thenReturn(Map.<String, Object>of(ModelUsageCost.INPUT, 1, ModelUsageCost.OUTPUT, 2));
			return EnterpriseUsageUtils.report(Source.MODEL, view, "2024-03-01", "2024-03-31", "", "", "", dimension,
					limit, 0);
		}
	}

	private static Object cost(Map<?, ?> row, String column) {
		return row.get(column) instanceof Number number ? number.longValue() : row.get(column);
	}

	@Test
	void costIsWhatCallsRecordedAndTheOlderCallsAtCurrentPrices() throws Exception {
		recordCostedCall();
		var summary = (Map<?, ?>) ((List<?>) pricedReport(View.SUMMARY, null, 1).get("rows")).get(0);
		// the costed call recorded 2,500; model-1's older calls are 300 input and
		// 100 output tokens, 300 + 200 at its current prices; model-2 has none
		assertEquals(2500L, cost(summary, "RECORDED_COST_MICROS"));
		assertEquals(500L, cost(summary, "ESTIMATED_COST_MICROS"));
		assertEquals(3000L, cost(summary, "COST_MICROS"));
		assertEquals(1L, cost(summary, "COSTED_REQUESTS"));
		assertEquals(3L, cost(summary, "PRICED_REQUESTS"));
		assertEquals(4L, cost(summary, "REQUESTS"));

		var days = new LinkedHashMap<String, Map<?, ?>>();
		for (Object row : (List<?>) pricedReport(View.TREND, null, 366).get("rows")) {
			days.put(String.valueOf(((Map<?, ?>) row).get("DAY")), (Map<?, ?>) row);
		}
		assertEquals(180L, cost(days.get("2024-03-01"), "COST_MICROS"));
		assertEquals(2500L, cost(days.get("2024-03-15"), "COST_MICROS"));
		assertNull(days.get("2024-03-15").get("ESTIMATED_COST_MICROS"));
		assertEquals(320L, cost(days.get("2024-03-31"), "COST_MICROS"));
		// a day with only a model with no known prices has no cost
		assertNull(days.get("2024-03-02").get("COST_MICROS"));

		var apps = new LinkedHashMap<Object, Map<?, ?>>();
		for (Object row : (List<?>) pricedReport(View.RANKING, "app", 20).get("rows")) {
			apps.put(((Map<?, ?>) row).get("ENTITY_ID"), (Map<?, ?>) row);
		}
		assertEquals(3000L, cost(apps.get("app-1"), "COST_MICROS"));
		// the unattributed calls are model-2's, which has no known prices
		assertTrue(apps.containsKey(null));
		assertNull(apps.get(null).get("COST_MICROS"));
		assertEquals(0L, cost(apps.get(null), "PRICED_REQUESTS"));
	}

	@Test
	void messagesListTheirTokenPartsCostAndWhomTheyWereBilledTo() throws Exception {
		recordCostedCall();
		var messages = new LinkedHashMap<String, Map<String, Object>>();
		for (var row : rows(EnterpriseUsageUtils.reportQuery(Source.MODEL, View.LOGS, "2024-03-15", "2024-03-15", "",
				"", "", null, 25, 0))) {
			messages.put((String) row.get("MESSAGE_TYPE"), row);
		}
		var input = messages.get("INPUT");
		assertEquals(600, input.get("CACHE_READ_TOKENS"));
		assertEquals(100, input.get("CACHE_CREATION_TOKENS"));
		assertNull(input.get("COST_MICROS"));
		var response = messages.get("RESPONSE");
		assertEquals(120, response.get("THINKING_TOKENS"));
		assertEquals(2500L, ((Number) response.get("COST_MICROS")).longValue());
		for (var row : messages.values()) {
			assertEquals("NATIVE", row.get("USER_TYPE"));
			assertEquals("TEAM", row.get("BILLED_SOURCE_TYPE"));
			assertEquals("Ops", row.get("BILLED_SUBJECT_ID"));
			assertEquals("CUSTOM", row.get("BILLED_SUBJECT_TYPE"));
		}
	}

	@Test
	void auditDatesAreTheServersDays() throws Exception {
		var engine = auditTrail();
		// New York is five hours behind UTC on 1 March 2024 and four on 31 March
		org.h2.tools.RunScript.execute(connection, new java.io.StringReader(
				"""
						INSERT INTO USER_AUDIT_EVENTS (EVENT_ID, EVENT_TIME, EVENT_TYPE, STATUS, ACTOR_USER_ID, TARGET_TYPE,
								TARGET_ID, PROJECT_ID) VALUES
						 ('evening-before','2024-03-01 04:59:59','PERMISSION_ADD','SUCCESS','owner-1','PROJECT','app-2','app-2'),
						 ('first-midnight','2024-03-01 05:00:00','PERMISSION_ADD','SUCCESS','owner-1','PROJECT','app-2','app-2'),
						 ('last-evening','2024-04-01 03:59:59.999999','PERMISSION_ADD','SUCCESS','owner-1','PROJECT','app-2','app-2'),
						 ('next-midnight','2024-04-01 04:00:00','PERMISSION_ADD','SUCCESS','owner-1','PROJECT','app-2','app-2');
						"""));
		var newYork = EnterpriseUsageUtils.auditQuery(View.LOGS, "2024-03-01", "2024-03-31", "", "=app-2", "", 25, 0,
				ZoneId.of("America/New_York"));
		// a change made late on the last day is on the next UTC day, and still counts
		assertEquals(List.of("last-evening", "first-midnight"),
				QueryExecutionUtility.queryList(engine, newYork, rs -> rs.getString("EVENT_ID")));
		var utc = EnterpriseUsageUtils.auditQuery(View.LOGS, "2024-03-01", "2024-03-31", "", "=app-2", "", 25, 0,
				ZoneOffset.UTC);
		assertEquals(List.of("first-midnight", "evening-before"),
				QueryExecutionUtility.queryList(engine, utc, rs -> rs.getString("EVENT_ID")));
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
		assertThrows(IllegalArgumentException.class, () -> query(Source.AUDIT, View.SUMMARY, "", "", null, 1, 0));
		assertThrows(IllegalArgumentException.class, () -> EnterpriseUsageUtils.auditQuery(View.TREND, "2024-03-01",
				"2024-03-31", "", "", "", 366, 0, ZoneOffset.UTC));
		assertThrows(IllegalArgumentException.class, () -> EnterpriseUsageUtils.auditQuery(View.LOGS, "2024-03-01",
				"2024-03-31", "", "", "", 5001, 0, ZoneOffset.UTC));
		assertThrows(IllegalArgumentException.class, () -> EnterpriseUsageUtils.auditDetailQuery(""));
	}
}
