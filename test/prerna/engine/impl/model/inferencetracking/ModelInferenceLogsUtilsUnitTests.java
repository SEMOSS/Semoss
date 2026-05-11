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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.gson.Gson;

import prerna.SemossUnitTest;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.model.MessageFeedback;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsUtilsUnitTests extends SemossUnitTest {
	User user;
	ResultSet rs;
	Statement stmt;
	Connection conn;
	AuthProvider auth;
	AccessToken access;
	IRDBMSEngine engine;
	PreparedStatement ps;
	IHeadersDataRow dataRow;
	WriteOWLEngine owlEngine;
	OWLEngineFactory owlFactory;
	IRawSelectWrapper rawWrapper;
	WrapperManager wrapperManager;
	IQueryInterpreter interpreter;
	IDatabaseEngine databaseEngine;
	ModelInferenceLogsUtils reactor;
	AbstractSqlQueryUtil absQueryUtil;

	private static final UUID FIXED_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

	@BeforeEach
	void setup() throws Exception {
		FileUtils.cleanDirectory(tempDir.toFile());

		user = mock(User.class);
		rs = mock(ResultSet.class);
		stmt = mock(Statement.class);
		conn = mock(Connection.class);
		auth = mock(AuthProvider.class);
		access = mock(AccessToken.class);
		engine = mock(IRDBMSEngine.class);
		ps = mock(PreparedStatement.class);
		dataRow = mock(IHeadersDataRow.class);
		owlEngine = mock(WriteOWLEngine.class);
		owlFactory = mock(OWLEngineFactory.class);
		rawWrapper = mock(IRawSelectWrapper.class);
		wrapperManager = mock(WrapperManager.class);
		interpreter = mock(IQueryInterpreter.class);
		databaseEngine = mock(IDatabaseEngine.class);
		absQueryUtil = mock(AbstractSqlQueryUtil.class);

		reactor = new ModelInferenceLogsUtils();
		Field registryField = SystemEngineRegistry.class.getDeclaredField("modelInferenceLogsDbHolder");
		registryField.setAccessible(true);
		registryField.set(null, (Supplier<IRDBMSEngine>) () -> engine);
	}

	@AfterEach
	void tearDown() throws Exception {
		Field registryField = SystemEngineRegistry.class.getDeclaredField("modelInferenceLogsDbHolder");
		registryField.setAccessible(true);
		registryField.set(null, null);
	}

	@Test
	void initModelInferenceLogsDatabase() throws Exception {
		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
				MockedStatic<ConnectionUtils> connUtil = Mockito.mockStatic(ConnectionUtils.class)) {
			when(engine.getQueryUtil()).thenReturn(absQueryUtil);
			when(engine.getOWLEngineFactory()).thenReturn(owlFactory);
			when(owlFactory.getWriteOWL()).thenReturn(owlEngine);

			when(engine.getConnection()).thenReturn(conn);

			when(absQueryUtil.allowsIfExistsTableSyntax()).thenReturn(false).thenReturn(true);
			when(absQueryUtil.allowIfExistsIndexSyntax()).thenReturn(false).thenReturn(true);
			when(absQueryUtil.createTableIfNotExists(anyString(), any(String[].class), any(String[].class)))
					.thenReturn("create query");
			when(absQueryUtil.createIndexIfNotExists(anyString(), anyString(), anyString())).thenReturn("");

			when(absQueryUtil.allowIfExistsAddConstraint()).thenReturn(false).thenReturn(true);

			when(conn.createStatement()).thenReturn(stmt);
			when(stmt.execute(anyString())).thenReturn(true);

			when(conn.getAutoCommit()).thenReturn(false);

			ModelInferenceLogsUtils.initModelInferenceLogsDatabase();
			ModelInferenceLogsUtils.initModelInferenceLogsDatabase();

			util.verify(() -> Utility.synchronizeEngineMetadata(Constants.MODEL_INFERENCE_LOGS_DB), times(2));
			connUtil.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, null, null), times(2));
		}
	}

	@Test
	void userIsMessageAuthor() throws Exception {
		try (MockedStatic<WrapperManager> staticWrapperManager = Mockito.mockStatic(WrapperManager.class)) {
			staticWrapperManager.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);

			when(rawWrapper.hasNext()).thenReturn(true);
			when(rawWrapper.next()).thenThrow(NoSuchElementException.class).thenReturn(dataRow).thenReturn(dataRow);
			when(dataRow.getValues()).thenReturn(new Object[] { 1 }).thenReturn(new Object[] { null });
			doThrow(IOException.class).doNothing().when(rawWrapper).close();

			assertFalse(ModelInferenceLogsUtils.userIsMessageAuthor("userId", "messageId"));
			assertTrue(ModelInferenceLogsUtils.userIsMessageAuthor("userId", "messageId"));
			assertFalse(ModelInferenceLogsUtils.userIsMessageAuthor("userId", "messageId"));
		}
	}

	@Test
	void recordFeedback() throws Exception {
		try (MockedStatic<WrapperManager> staticWrapperManager = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<ConnectionUtils> staticConnUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			staticWrapperManager.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);

			when(rawWrapper.hasNext()).thenReturn(true);
			when(rawWrapper.next()).thenThrow(NoSuchElementException.class).thenReturn(dataRow);
			when(dataRow.getValues()).thenReturn(new Object[] { 1 }).thenReturn(new Object[] { null });
			doThrow(IOException.class).doNothing().when(rawWrapper).close();

			when(engine.getPreparedStatement(
					"INSERT INTO FEEDBACK (MESSAGE_ID, FEEDBACK_TEXT, FEEDBACK_DATE, RATING) VALUES (?, ?, ?, ?)"))
					.thenReturn(ps);
			when(ps.execute()).thenReturn(true).thenThrow(SQLException.class);
			when(ps.getConnection()).thenReturn(conn);
			when(conn.getAutoCommit()).thenReturn(false);

			MessageFeedback testFeedback = new MessageFeedback("messageId", "feedback", true);
			SemossPixelException spe = assertThrows(SemossPixelException.class,
					() -> ModelInferenceLogsUtils.recordFeedback(testFeedback));
			assertEquals("Error while checking feedbackExists or not .null", spe.getMessage());

			// Goes into updateFeedback
			ModelInferenceLogsUtils.recordFeedback(testFeedback);

			// Goes into insertFeedback
			ModelInferenceLogsUtils.recordFeedback(testFeedback);
			verify(engine, times(1)).getPreparedStatement(
					"INSERT INTO FEEDBACK (MESSAGE_ID, FEEDBACK_TEXT, FEEDBACK_DATE, RATING) VALUES (?, ?, ?, ?)");
			verify(ps, times(1)).execute();
			verify(ps, times(2)).getConnection();
			verify(conn).getAutoCommit();
			verify(conn).commit();
			staticConnUtils.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null),
					times(1));

			staticWrapperManager.verify(() -> WrapperManager.getInstance(), times(3));
			verify(wrapperManager, times(3)).getRawWrapper(eq(engine), any(SelectQueryStruct.class));
			verify(rawWrapper, times(3)).hasNext();
			verify(rawWrapper, times(3)).next();
			verify(rawWrapper, times(3)).close();
			verify(dataRow, times(2)).getValues();
		}
	}

	@Test
	void getOverAllEngineUsageFromModelInferenceLogs() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> executeUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			executeUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.getOverAllEngineUsageFromModelInferenceLogs("engineId", "0",
					"5", "01-01-2025", "31-25-2025"));
		}
	}

	@Test
	void getTokenUsagePerProjectForEngine() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> executeUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			executeUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.getTokenUsagePerProjectForEngine("engineId", "0", "5",
					"01-01-2025", "31-12-2025"));
		}
	}

	@Test
	void getUserUsagePerEngine() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> executeUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			executeUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected,
					ModelInferenceLogsUtils.getUserUsagePerEngine("engineId", "0", "5", "01-01-2025", "31-12-2025"));
		}
	}

	@Test
	void getProjectUsageFromModelInferenceLogs() {
		Map<String, Object> expected = new HashMap<>();
		expected.put("key", "value");
		List<Map<String, Object>> list = new ArrayList<>();
		list.add(expected);

		try (MockedStatic<QueryExecutionUtility> executeUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			executeUtil.when(() -> QueryExecutionUtility.flushToListString(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn((new ArrayList<>()));
			executeUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(list);

			assertEquals(expected, ModelInferenceLogsUtils.getProjectUsageFromModelInferenceLogs("projectId"));
		}
	}

	@Test
	void doCreateNewConversation() throws Exception {
		try (MockedStatic<UUID> statticUUID = Mockito.mockStatic(UUID.class);
				MockedStatic<ConnectionUtils> connUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			statticUUID.when(() -> UUID.randomUUID()).thenReturn(FIXED_UUID);

			when(engine.getPreparedStatement(
					"INSERT INTO ROOM (INSIGHT_ID, ROOM_ID, ROOM_NAME, ROOM_CONTEXT, USER_ID, USER_NAME, USER_EMAIL_ID, AGENT_TYPE, AGENT_ID, IS_ACTIVE, DATE_CREATED, PROJECT_ID, PROJECT_NAME, WORKSPACE_ID, OPTIONS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
					.thenReturn(ps);
			when(engine.getQueryUtil()).thenReturn(absQueryUtil);
			when(ps.getConnection()).thenReturn(conn);

			when(conn.getAutoCommit()).thenReturn(false);
			doNothing().doNothing().doThrow(SQLException.class).when(conn).commit();

			ModelInferenceLogsUtils.doCreateNewConversation(null, null, "userId", null, null, null, null, true,
					"projectId", "projectName");
			ModelInferenceLogsUtils.doCreateNewConversation(FIXED_UUID.toString(), "roomId", "roomName", "roomContext",
					"userId", "userName", "userEmail", "agentType", "agentId", true, "projectId", "projectName",
					new HashMap<>());
			ModelInferenceLogsUtils.doCreateNewConversation(FIXED_UUID.toString(), "roomId", "roomName", "roomContext",
					"userId", "userName", "userEmail", "agentType", "agentId", true, "projectId", "projectName",
					"workspaceId", new HashMap<>(), "parentRoomId");

			verify(engine, times(3)).getPreparedStatement(
					"INSERT INTO ROOM (INSIGHT_ID, ROOM_ID, ROOM_NAME, ROOM_CONTEXT, USER_ID, USER_NAME, USER_EMAIL_ID, AGENT_TYPE, AGENT_ID, IS_ACTIVE, DATE_CREATED, PROJECT_ID, PROJECT_NAME, WORKSPACE_ID, OPTIONS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			verify(engine, times(3)).getQueryUtil();
			verify(absQueryUtil, times(2)).handleInsertionOfClob(eq(ps), eq("roomContext"), eq(4), any(Gson.class));
			verify(absQueryUtil).handleInsertionOfClob(eq(ps), any(HashMap.class), eq(15), any(Gson.class));

			verify(ps, times(26)).setString(anyInt(), anyString());
			verify(ps, times(10)).setNull(anyInt(), anyInt());

			connUtils.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null), times(3));
		}
	}

	@Test
	void doCheckRoomExists() throws Exception {
		when(engine.getPreparedStatement("SELECT COUNT(*) FROM ROOM WHERE ROOM_ID = ?")).thenReturn(ps)
				.thenThrow(SQLException.class);

		when(ps.execute()).thenReturn(true);
		when(ps.getResultSet()).thenReturn(rs);
		when(rs.next()).thenReturn(true).thenReturn(false);
		when(rs.getInt(1)).thenReturn(1);

		try (MockedStatic<ConnectionUtils> connUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			assertTrue(ModelInferenceLogsUtils.doCheckRoomExists("1"));
			assertFalse(ModelInferenceLogsUtils.doCheckRoomExists("1"));
		}
	}

	@Test
	void doModelIsRegistered() throws Exception {
		when(engine.getPreparedStatement("SELECT COUNT(*) FROM AGENT WHERE AGENT_ID = ?")).thenReturn(ps)
				.thenThrow(SQLException.class);

		when(ps.execute()).thenReturn(true);
		when(ps.getResultSet()).thenReturn(rs);
		when(rs.next()).thenReturn(true).thenReturn(false);
		when(rs.getInt(1)).thenReturn(1);

		try (MockedStatic<ConnectionUtils> connUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			assertTrue(ModelInferenceLogsUtils.doModelIsRegistered("1"));
			assertFalse(ModelInferenceLogsUtils.doModelIsRegistered("1"));
		}
	}

	@Test
	void doCreateNewAgent() throws Exception {
		try (MockedStatic<ConnectionUtils> connUtils = Mockito.mockStatic(ConnectionUtils.class)) {

			when(engine.getPreparedStatement(
					"INSERT INTO AGENT (AGENT_ID, AGENT_NAME, DESCRIPTION, AGENT_TYPE, AUTHOR, DATE_CREATED) VALUES (?, ?, ?, ?, ?, ?)"))
					.thenReturn(ps).thenThrow(SQLException.class);

			when(ps.execute()).thenReturn(true);
			when(ps.getConnection()).thenReturn(conn);
			when(conn.getAutoCommit()).thenReturn(false);

			assertNotNull(
					ModelInferenceLogsUtils.doCreateNewAgent("agentName", "agentDescription", "agentType", "author"));

			verify(ps, times(5)).setString(anyInt(), anyString());
			verify(ps, times(1)).setTimestamp(anyInt(), any(Timestamp.class));
			connUtils.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null), times(1));
		}
	}

	@Test
	void doRecordMessage() throws Exception {
		when(engine.getPreparedStatement(
				"INSERT INTO MESSAGE (MESSAGE_ID, TRANSACTION_ID, MESSAGE_TYPE, MESSAGE_DATA, MESSAGE_METHOD, MESSAGE_TOKENS, RESPONSE_TIME, DATE_CREATED, AGENT_ID, INSIGHT_ID, ROOM_ID, SESSIONID, USER_ID, USER_NAME, USER_EMAIL_ID) 	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
				.thenReturn(ps);
		when(engine.getQueryUtil()).thenReturn(absQueryUtil);
		when(ps.getConnection()).thenReturn(conn);

		when(ps.execute()).thenReturn(true).thenThrow(SQLException.class);
		when(conn.getAutoCommit()).thenReturn(false);

		ModelInferenceLogsUtils.doRecordMessage("messageId", "messageType", "messageData", "messageMethod", 1, 2.0,
				"agentId", "insightId", "sessionId", "userId", "userName", "userEmail");
		ModelInferenceLogsUtils.doRecordMessage("messageId", "messageType", null, "messageMethod", null, 2.0, "agentId",
				"insightId", "sessionId", "userId", null, null);

		verify(engine).getQueryUtil();
		verify(absQueryUtil).handleInsertionOfBlob(conn, ps, "messageData", 4);
		verify(ps, times(18)).setString(anyInt(), anyString());
		verify(ps, times(6)).setNull(anyInt(), anyInt());
		verify(ps, times(2)).setTimestamp(anyInt(), any(Timestamp.class));
		verify(ps, times(2)).setDouble(anyInt(), any(Double.class));
		verify(ps, times(1)).setInt(anyInt(), anyInt());
		verify(ps, times(2)).execute();
		verify(ps, times(3)).getConnection();
		verify(conn).getAutoCommit();
		verify(conn).commit();
	}

	@Test
	void doSetRoomToInactive() {
		assertFalse(ModelInferenceLogsUtils.doSetRoomToInactive("userId", "roomId"));
	}

	@Test
	void doSetRoomToPinned() {
		assertFalse(ModelInferenceLogsUtils.doSetRoomToPinned("userId", "roomId", true));
	}

	@Test
	void doSetNameForRoom() {
		assertFalse(ModelInferenceLogsUtils.doSetNameForRoom("userId", "roomId", "roomName"));
	}

	@Test
	void doRetrieveConversation() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecuteUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			queryExecuteUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.doRetrieveConversation("userId", "roomId", "dateSort"));
		}
	}

	@Test
	void doRetrieveConversation2() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecuteUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			queryExecuteUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.doRetrieveConversation("userId", "roomId", "DESC", 1, 5));
		}
	}

	@Test
	void doRetrieveNearestNeighbor() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecutionUtility = Mockito
				.mockStatic(QueryExecutionUtility.class)) {
			queryExecutionUtility
					.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.doRetrieveNearestNeighbor("userId", "roomId", "dateSort"));
		}
	}

	@Test
	void doVerifyConversation() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecutionUtility = Mockito
				.mockStatic(QueryExecutionUtility.class)) {
			queryExecutionUtility
					.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.doVerifyConversation("userId", "roomId"));
		}
	}

	@Test
	void removeFeedback() throws Exception {
		try (MockedStatic<WrapperManager> staticWrapperManager = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<ConnectionUtils> staticConnUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			staticWrapperManager.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);

			when(rawWrapper.hasNext()).thenReturn(true);
			when(rawWrapper.next()).thenReturn(dataRow);
			when(dataRow.getValues()).thenReturn(new Object[] { 1 }).thenReturn(new Object[] { 1 })
					.thenReturn(new Object[] { null });

			when(engine.getPreparedStatement("DELETE FROM FEEDBACK WHERE MESSAGE_ID = ?")).thenReturn(ps);
			when(ps.executeUpdate()).thenReturn(0);
			when(ps.getConnection()).thenReturn(conn);
			when(conn.getAutoCommit()).thenReturn(false);
			doNothing().doThrow(SQLException.class).when(conn).commit();

			ModelInferenceLogsUtils.removeFeedback("messageId");

			verify(engine, times(1)).getPreparedStatement("DELETE FROM FEEDBACK WHERE MESSAGE_ID = ?");
			verify(ps, times(1)).setString(1, "messageId");
			verify(ps, times(1)).executeUpdate();
			verify(ps, times(2)).getConnection();
			verify(conn, times(1)).getAutoCommit();
			verify(conn, times(1)).commit();
			staticConnUtils.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null),
					times(1));

			SemossPixelException e = assertThrows(SemossPixelException.class,
					() -> ModelInferenceLogsUtils.removeFeedback("messageId"));
			assertEquals("Error while deleting feedback: null", e.getMessage());

			staticWrapperManager.verify(() -> WrapperManager.getInstance(), times(2));
			verify(wrapperManager, times(2)).getRawWrapper(eq(engine), any(SelectQueryStruct.class));
			verify(rawWrapper, times(2)).hasNext();
			verify(rawWrapper, times(2)).next();
			verify(dataRow, times(2)).getValues();
		}
	}

	@Test
	void getRoomContext() {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecutionUtility = Mockito
				.mockStatic(QueryExecutionUtility.class)) {
			queryExecutionUtility
					.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.getRoomContext("userId", "roomId"));
		}
	}

	@Test
	void getRoomOptions() throws Exception {
		List<Map<String, Object>> expected = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecutionUtility = Mockito
				.mockStatic(QueryExecutionUtility.class)) {
			queryExecutionUtility
					.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(expected);

			assertEquals(expected, ModelInferenceLogsUtils.getRoomOptions("roomId", "userId"));
		}
	}

	@Test
	void setRoomOptions() throws Exception {
		try (MockedStatic<ConnectionUtils> connUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			when(engine.getPreparedStatement("UPDATE ROOM SET OPTIONS = ? WHERE USER_ID = ? AND ROOM_ID = ?"))
					.thenReturn(ps);
			when(engine.getQueryUtil()).thenReturn(absQueryUtil);

			when(ps.getConnection()).thenReturn(conn);
			when(conn.getAutoCommit()).thenReturn(false);
			when(ps.executeUpdate()).thenReturn(1).thenThrow(SQLException.class);

			ModelInferenceLogsUtils.setRoomOptions("roomId", "userId", new HashMap<>());
			ModelInferenceLogsUtils.setRoomOptions("roomId", "userId", null);

			verify(engine).getQueryUtil();
			verify(absQueryUtil).handleInsertionOfClob(eq(ps), anyMap(), eq(1), any(Gson.class));
			verify(ps, times(4)).setString(anyInt(), anyString());
			verify(ps).setNull(anyInt(), anyInt());
			verify(ps, times(2)).getConnection();
			verify(conn).getAutoCommit();
			verify(conn).commit();

			connUtils.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null), times(2));
		}
	}

	@Test
	void setRoomWorkspaceId() throws Exception {
		try (MockedStatic<ConnectionUtils> connUtils = Mockito.mockStatic(ConnectionUtils.class)) {
			when(engine.getPreparedStatement("UPDATE ROOM SET WORKSPACE_ID = ? WHERE USER_ID = ? AND ROOM_ID = ?"))
					.thenReturn(ps);

			when(ps.getConnection()).thenReturn(conn);
			when(conn.getAutoCommit()).thenReturn(false);
			when(ps.executeUpdate()).thenReturn(1).thenThrow(SQLException.class);

			ModelInferenceLogsUtils.setRoomWorkspaceId("roomId", "userId", "workspaceId");
			ModelInferenceLogsUtils.setRoomWorkspaceId("roomId", "userId", null);

			verify(ps, times(5)).setString(anyInt(), anyString());
			verify(ps).setNull(anyInt(), anyInt());
			verify(ps, times(2)).getConnection();
			verify(conn).getAutoCommit();
			verify(conn).commit();

			connUtils.verify(() -> ConnectionUtils.closeAllConnectionsIfPooling(engine, null, ps, null), times(2));
		}
	}

	@Test
	void setRoomContext() {
		ModelInferenceLogsUtils.setRoomContext("roomId", "userId", "context");
	}

	@Test
	void getTotalTokensOrTotalResponseTime() throws Exception {
		RawRDBMSSelectWrapper rawRDBMSSelectWrapper = mock(RawRDBMSSelectWrapper.class);
		List<AuthProvider> list = new ArrayList<>();
		list.add(auth);

		try (MockedStatic<RawRDBMSSelectWrapper> staticRDBMSWrapper = Mockito.mockStatic(RawRDBMSSelectWrapper.class)) {
			when(engine.getPreparedStatement(anyString())).thenReturn(ps);

			when(user.getLogins()).thenReturn(list);
			when(user.getAccessToken(auth)).thenReturn(access);
			when(access.getId()).thenReturn("accessId");

			when(ps.getConnection()).thenReturn(conn);

			staticRDBMSWrapper
					.when(() -> RawRDBMSSelectWrapper.directExecutionPreparedStatement(eq(engine), eq(conn), eq(ps),
							anyString(), eq(false)))
					.thenReturn(rawRDBMSSelectWrapper).thenReturn(rawRDBMSSelectWrapper).thenThrow(Exception.class);
			when(rawRDBMSSelectWrapper.hasNext()).thenReturn(true);
			when(rawRDBMSSelectWrapper.next()).thenReturn(dataRow);
			when(dataRow.getValues()).thenReturn(new Object[] { 1 }).thenReturn(new Object[] { null });

			assertEquals(1, ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime("token", user, "engineId",
					ZonedDateTime.now(), "WEEK"));
			assertEquals(0, ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime("compute", user, "engineId",
					ZonedDateTime.now(), "MONTH"));
			assertNull(ModelInferenceLogsUtils.getTotalTokensOrTotalResponseTime("compute", user, "engineId",
					ZonedDateTime.now(), "DAILY"));

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ModelInferenceLogsUtils
					.getTotalTokensOrTotalResponseTime(null, user, "engineId", ZonedDateTime.now(), "freq"));
			assertEquals("Must pass in a valid restriction mode", e.getMessage());
		}
	}

	@Test
	void getTotalUsageForUser() throws Exception {
		Exception e = assertThrows(IllegalArgumentException.class, () -> ModelInferenceLogsUtils
				.getTotalUsageForUser(null, user, "engineId", ZonedDateTime.now(), "freq"));
		assertEquals("Must pass in a valid restriction mode", e.getMessage());

		RawRDBMSSelectWrapper rawRDBMSSelectWrapper = mock(RawRDBMSSelectWrapper.class);

		List<AuthProvider> logins = new ArrayList<>();
		logins.add(auth);

		List<String> engineList = new ArrayList<>();
		engineList.add("engine1");
		engineList.add("engine2");

		try (MockedStatic<SecurityEngineUtils> staticSecEngineUtils = Mockito.mockStatic(SecurityEngineUtils.class);
				MockedStatic<RawRDBMSSelectWrapper> staticRDBMSWrapper = Mockito
						.mockStatic(RawRDBMSSelectWrapper.class);
				MockedStatic<ConnectionUtils> connUtilStatic = Mockito.mockStatic(ConnectionUtils.class)) {
			staticSecEngineUtils.when(() -> SecurityEngineUtils.getModelEngineIdsWithRestrictions(user, "engineId"))
					.thenReturn(engineList);

			when(engine.getPreparedStatement(anyString())).thenReturn(ps).thenReturn(ps).thenThrow(SQLException.class);

			when(user.getLogins()).thenReturn(logins);
			when(user.getAccessToken(auth)).thenReturn(access);

			when(ps.getConnection()).thenReturn(conn);
			staticRDBMSWrapper.when(() -> RawRDBMSSelectWrapper.directExecutionPreparedStatement(eq(engine), eq(conn),
					eq(ps), anyString(), eq(false))).thenReturn(rawRDBMSSelectWrapper);

			when(rawRDBMSSelectWrapper.hasNext()).thenReturn(true);
			when(rawRDBMSSelectWrapper.next()).thenReturn(dataRow);
			when(dataRow.getValues()).thenReturn(new Object[] { null }).thenReturn(new Object[] { 1 });

			assertEquals(0, ModelInferenceLogsUtils.getTotalUsageForUser("token", user, "engineId", ZonedDateTime.now(),
					"WEEK"));
			assertEquals(1, ModelInferenceLogsUtils.getTotalUsageForUser("compute", user, "engineId",
					ZonedDateTime.now(), "MONTH"));
			assertNull(ModelInferenceLogsUtils.getTotalUsageForUser("compute", user, "engineId", ZonedDateTime.now(),
					"DAILY"));
		}
	}

	@Test
	void llm2_updateRoomMessages() throws Exception {
		when(engine
				.getPreparedStatement("UPDATE ROOM SET MESSAGES = ?, UPDATED_AT = ? WHERE ROOM_ID = ? AND USER_ID = ?"))
				.thenReturn(ps);

		when(ps.executeUpdate()).thenReturn(1).thenReturn(0).thenThrow(SQLException.class);
		when(ps.getConnection()).thenReturn(conn);
		when(conn.getAutoCommit()).thenReturn(false);

		assertTrue(ModelInferenceLogsUtils.llm2_updateRoomMessages("roomId", "userId", "messageHistory"));
		assertFalse(ModelInferenceLogsUtils.llm2_updateRoomMessages("roomId", "userId", "messageHistory"));
		Exception e = assertThrows(IllegalArgumentException.class,
				() -> ModelInferenceLogsUtils.llm2_updateRoomMessages("roomId", "userId", "messageHistory"));
		assertEquals("Error updating room messages: null", e.getMessage());
	}

	@Test
	void llm2_updateRoomMessages2() throws Exception {
		when(engine.getPreparedStatement(
				"UPDATE ROOM SET MESSAGES = ?, UPDATED_AT = ? , ROOM_NAME = ?, MODEL_ID = ?  WHERE ROOM_ID = ? AND USER_ID = ?"))
				.thenThrow(SQLException.class).thenReturn(ps);
		when(ps.executeUpdate()).thenReturn(1);
		when(ps.getConnection()).thenReturn(conn);
		when(conn.getAutoCommit()).thenReturn(false);

		Exception e = assertThrows(IllegalArgumentException.class, () -> ModelInferenceLogsUtils
				.llm2_updateRoomMessages("roomId", "userId", "messageHistory", "roomName", "engineId"));
		assertEquals("Error updating room messages: null", e.getMessage());

		assertTrue(ModelInferenceLogsUtils.llm2_updateRoomMessages("roomId", "userId", "messageHistory", "roomName",
				"engineId"));
	}

	@Test
	void getRoomById() throws Exception {
		Room expected = new Room("", "", "", "", "", "", true, new Timestamp(0), new Timestamp(0), "", true, "", "",
				"");

		when(engine.getPreparedStatement("SELECT *  FROM ROOM WHERE ROOM_ID = ? and USER_ID = ? "))
				.thenThrow(SQLException.class).thenReturn(ps);
		when(ps.executeQuery()).thenReturn(rs);
		when(rs.next()).thenReturn(false).thenReturn(true);
		when(rs.getString(anyString())).thenReturn("");
		when(rs.getBoolean(anyString())).thenReturn(true);
		when(rs.getTimestamp(anyString())).thenReturn(new Timestamp(0));

		assertNull(ModelInferenceLogsUtils.getRoomById("roomId", "userId"));
		assertNull(ModelInferenceLogsUtils.getRoomById("roomId", "userId"));

		Room retVal = ModelInferenceLogsUtils.getRoomById("roomId", "userId");
		assertEquals(expected.getUserId(), retVal.getUserId());
		assertEquals(expected.getRoomName(), retVal.getRoomName());
		assertEquals(expected.getShareId(), retVal.getShareId());
		assertEquals(expected.isActive(), retVal.isActive());
		assertEquals(expected.getCreatedAt(), retVal.getCreatedAt());
		assertEquals(expected.getUpdatedAt(), retVal.getUpdatedAt());
		assertEquals(expected.getMessages(), retVal.getMessages());
		assertEquals(expected.getOptions(), retVal.getOptions());
		assertEquals(expected.getModelId(), retVal.getModelId());
	}

	@Test
	void validUserRoom() {
		List<Map<String, Object>> rooms = new ArrayList<>();

		try (MockedStatic<QueryExecutionUtility> queryExecutionUtil = Mockito.mockStatic(QueryExecutionUtility.class)) {
			queryExecutionUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(rooms);

			Exception e = assertThrows(IllegalArgumentException.class,
					() -> ModelInferenceLogsUtils.validUserRoom("roomId", "userId"));
			assertEquals("Unable to find room", e.getMessage());

			Map<String, Object> map = new HashMap<>();
			map.put("IS_ACTIVE", false);
			rooms.add(map);

			queryExecutionUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(rooms);

			e = assertThrows(IllegalArgumentException.class,
					() -> ModelInferenceLogsUtils.validUserRoom("roomId", "userId"));
			assertEquals("Room is closed", e.getMessage());

			map = new HashMap<>();
			map.put("IS_ACTIVE", true);
			rooms = new ArrayList<>();
			rooms.add(map);

			queryExecutionUtil.when(() -> QueryExecutionUtility.flushRsToMap(eq(engine), any(SelectQueryStruct.class)))
					.thenReturn(rooms);

			assertTrue(ModelInferenceLogsUtils.validUserRoom("roomId", "userId"));
		}
	}

	@Test
	void createNewWorkspaceEntry() throws Exception {
		Map<String, String> map = new HashMap<>();
		map.put("workspace_resource_id", "workspace_resource_id_value");
		map.put("workspace_id", "workspace_id_value");
		map.put("resource_id", "resource_id_value");
		map.put("resource_type", "resource_type_value");
		map.put("resource_subtype", "resource_subtype_value");
		List<Map<String, String>> resources = new ArrayList<>();
		resources.add(map);

		when(engine.getConnection()).thenReturn(conn);
		when(conn.prepareStatement(
				"INSERT INTO WORKSPACE (WORKSPACE_ID, NAME, DESCRIPTION, SYSTEM_PROMPT, OWNER, IS_ACTIVE, DATE_CREATED, DATE_UPDATED) VALUES (?,?,?,?,?,?,?,?)"))
				.thenReturn(ps);

		when(engine.getQueryUtil()).thenReturn(absQueryUtil);
		when(ps.execute()).thenThrow(SQLException.class).thenReturn(true);
		when(conn.getAutoCommit()).thenReturn(false);

		when(conn.prepareStatement(
				"INSERT INTO WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE) VALUES (?,?,?,?,?)"))
				.thenReturn(ps);

		Exception e = assertThrows(IllegalArgumentException.class,
				() -> ModelInferenceLogsUtils.createNewWorkspaceEntry("workspaceId", "ownerId", "workspaceName",
						"workspaceDescription", "systemPrompt", resources));
		assertEquals("Error creating workspace: null", e.getMessage());

		ModelInferenceLogsUtils.createNewWorkspaceEntry("workspaceId", "ownerId", "workspaceName",
				"workspaceDescription", "systemPrompt", null);
		ModelInferenceLogsUtils.createNewWorkspaceEntry("workspaceId", "ownerId", "workspaceName",
				"workspaceDescription", "systemPrompt", resources);

		verify(engine, times(3)).getConnection();
		verify(conn, times(4)).prepareStatement(anyString());
		verify(ps, times(14 - 3)).setString(anyInt(), anyString());
		verify(ps, times(3)).setBoolean(anyInt(), anyBoolean());
		verify(ps, times(6)).setTimestamp(anyInt(), any(Timestamp.class));
		verify(ps, times(3)).execute();
		verify(conn, times(3)).getAutoCommit();
		verify(conn, times(3)).commit();
		verify(ps, times(1)).executeBatch();
	}

	@Test
	void updateWorkspaceEntry() throws Exception {
		Map<String, String> map = new HashMap<>();
		map.put("workspace_resource_id", "workspace_resource_id_value");
		map.put("workspace_id", "workspace_id_value");
		map.put("resource_id", "resource_id_value");
		map.put("resource_type", "resource_type_value");
		map.put("resource_subtype", "resource_subtype_value");
		List<Map<String, String>> resources = new ArrayList<>();
		resources.add(map);

		when(engine.getConnection()).thenReturn(conn);
		when(conn.prepareStatement(
				"UPDATE WORKSPACE SET NAME = ?, DESCRIPTION = ?, SYSTEM_PROMPT = ?, IS_ACTIVE = ?, DATE_UPDATED = ? WHERE WORKSPACE_ID = ?"))
				.thenReturn(ps);

		when(engine.getQueryUtil()).thenReturn(absQueryUtil);
		when(ps.execute()).thenThrow(SQLException.class).thenReturn(true);
		when(conn.getAutoCommit()).thenReturn(false);

		when(conn.prepareStatement("DELETE FROM WORKSPACE_RESOURCE WHERE WORKSPACE_ID = ?")).thenReturn(ps);
		when(conn.prepareStatement(
				"INSERT INTO WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE) VALUES (?,?,?,?,?)"))
				.thenReturn(ps);

		Exception e = assertThrows(IllegalArgumentException.class,
				() -> ModelInferenceLogsUtils.updateWorkspaceEntry("workspaceId", "workspaceName",
						"workspaceDescription", "systemPrompt", true, resources));
		assertEquals("Error updating workspace: null", e.getMessage());

		ModelInferenceLogsUtils.updateWorkspaceEntry("workspaceId", "workspaceName", "workspaceDescription",
				"systemPrompt", true, null);
		ModelInferenceLogsUtils.updateWorkspaceEntry("workspaceId", "workspaceName", "workspaceDescription",
				"systemPrompt", true, resources);

		verify(engine, times(3)).getConnection();
		verify(engine, times(6)).getQueryUtil();

		verify(absQueryUtil, times(6)).handleInsertionOfClob(eq(conn), eq(ps), anyString(), anyInt(), any(Gson.class));

		verify(conn, times(6)).prepareStatement(anyString());
		verify(conn, times(5)).getAutoCommit();
		verify(conn, times(5)).commit();

		verify(ps, times(13 - 3)).setString(anyInt(), anyString());
		verify(ps, times(3)).setBoolean(anyInt(), anyBoolean());
		verify(ps, times(3)).setTimestamp(anyInt(), any(Timestamp.class));
		verify(ps, times(5)).execute();
		verify(ps).executeBatch();
	}

	@Test
	void deleteWorkspaceEntry() throws Exception {
		when(engine.getConnection()).thenReturn(conn);
		when(conn.prepareStatement(anyString())).thenReturn(ps);
		when(ps.execute()).thenThrow(SQLException.class).thenReturn(true);
		when(conn.getAutoCommit()).thenReturn(false);

		Exception e = assertThrows(IllegalArgumentException.class,
				() -> ModelInferenceLogsUtils.deleteWorkspaceEntry("workspaceId"));
		assertEquals("Error deleting workspace: null", e.getMessage());

		ModelInferenceLogsUtils.deleteWorkspaceEntry("workspaceId");

		verify(engine, times(2)).getConnection();
		verify(conn, times(6)).prepareStatement(anyString());
		verify(ps, times(6)).setString(anyInt(), anyString());
		verify(ps, times(4)).execute();
		verify(conn).getAutoCommit();
		verify(conn).commit();
	}

	@Test
	void getWorkspaceEntry() throws Exception {
		Clob clob = mock(Clob.class);
		Blob blob = mock(Blob.class);

		Map<String, Object> expected = new HashMap<>();
		expected.put("header1", "clob");
		expected.put("header2", "blob");
		expected.put("header3", "1");

		String[] headers = new String[] { "header1", "header2", "header3" };
		Object[] values = new Object[] { clob, blob, 1 };

		try (MockedStatic<WrapperManager> staticWrapper = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractSqlQueryUtil> staticQueryUtil = Mockito.mockStatic(AbstractSqlQueryUtil.class)) {
			staticWrapper.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);
			when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
			when(rawWrapper.next()).thenThrow(NoSuchElementException.class).thenReturn(dataRow);
			when(dataRow.getHeaders()).thenReturn(headers);
			when(dataRow.getValues()).thenReturn(values);

			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushClobToString(any(Clob.class))).thenReturn("clob");
			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushBlobToString(any(Blob.class))).thenReturn("blob");

			assertNull(ModelInferenceLogsUtils.getWorkspaceEntry("workspaceId"));
			assertEquals(expected.toString(), ModelInferenceLogsUtils.getWorkspaceEntry("workspaceId").toString());
		}
	}

	@Test
	void getWorkspaceRoomsForUser() throws Exception {
		Clob clob = mock(Clob.class);
		Blob blob = mock(Blob.class);
		GenRowFilters filters = mock(GenRowFilters.class);

		Map<String, Object> subMap = new HashMap<>();
		subMap.put("header1", "clob");
		subMap.put("header2", "blob");
		subMap.put("header3", "1");
		List<Map<String, Object>> list = new ArrayList<>();
		list.add(subMap);
		Map<String, Object> expected = new HashMap<>();
		expected.put("rooms", list);
		expected.put("total_count", 0);

		List<IQuerySort> sorts = new ArrayList<>();
		sorts.add(null);
		List<AuthProvider> logins = new ArrayList<>();
		logins.add(auth);

		String[] headers = new String[] { "header1", "header2", "header3" };
		Object[] values = new Object[] { clob, blob, 1 };

		when(user.getLogins()).thenReturn(logins);
		when(user.getAccessToken(auth)).thenReturn(access);
		when(access.getId()).thenReturn("id");

		when(engine.getQueryInterpreter()).thenReturn(interpreter);
		when(interpreter.composeQuery()).thenReturn("subQuery");

		try (MockedStatic<WrapperManager> staticWrapper = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractSqlQueryUtil> staticQueryUtil = Mockito.mockStatic(AbstractSqlQueryUtil.class)) {
			staticWrapper.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);
			when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
			when(rawWrapper.next()).thenThrow(NoSuchElementException.class).thenReturn(dataRow);
			when(dataRow.getHeaders()).thenReturn(headers);
			when(dataRow.getValues()).thenReturn(values);

			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushClobToString(any(Clob.class))).thenReturn("clob");
			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushBlobToString(any(Blob.class))).thenReturn("blob");

			assertNull(ModelInferenceLogsUtils.getWorkspaceRoomsForUser("workspaceId", user, 10, 0, filters, null));
			Map<String, Object> entries = ModelInferenceLogsUtils.getWorkspaceRoomsForUser("workspaceId", user, 10, 0,
					filters, sorts);
			assertTrue(expected.toString().equals(entries.toString()));
		}
	}

	@Test
	void getWorkspaceEntriesForUser() throws Exception {
		Clob clob = mock(Clob.class);
		Blob blob = mock(Blob.class);
		GenRowFilters filters = mock(GenRowFilters.class);

		Map<String, Object> subMap = new HashMap<>();
		subMap.put("header1", "clob");
		subMap.put("header2", "blob");
		subMap.put("header3", "1");
		List<Map<String, Object>> list = new ArrayList<>();
		list.add(subMap);
		Map<String, Object> expected = new HashMap<>();
		expected.put("workspaces", list);
		expected.put("total_count", 0);

		List<IQuerySort> sorts = new ArrayList<>();
		sorts.add(null);
		Set<String> sharedWorkspaceIds = new HashSet<>();
		sharedWorkspaceIds.add(null);
		List<AuthProvider> logins = new ArrayList<>();
		logins.add(auth);

		String[] headers = new String[] { "header1", "header2", "header3" };
		Object[] values = new Object[] { clob, blob, 1 };

		when(user.getLogins()).thenReturn(logins);
		when(user.getAccessToken(auth)).thenReturn(access);
		when(access.getId()).thenReturn("id");

		when(engine.getQueryInterpreter()).thenReturn(interpreter);
		when(interpreter.composeQuery()).thenReturn("subQuery");

		try (MockedStatic<WrapperManager> staticWrapper = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractSqlQueryUtil> staticQueryUtil = Mockito.mockStatic(AbstractSqlQueryUtil.class)) {
			staticWrapper.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);
			when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
			when(rawWrapper.next()).thenThrow(NoSuchElementException.class).thenReturn(dataRow);
			when(dataRow.getHeaders()).thenReturn(headers);
			when(dataRow.getValues()).thenReturn(values);

			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushClobToString(any(Clob.class))).thenReturn("clob");
			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushBlobToString(any(Blob.class))).thenReturn("blob");

			assertNull(
					ModelInferenceLogsUtils.getWorkspaceEntriesForUser(user, 10, 0, filters, null, sharedWorkspaceIds));
			Map<String, Object> entries = ModelInferenceLogsUtils.getWorkspaceEntriesForUser(user, 10, 0, filters,
					sorts, sharedWorkspaceIds);
			assertTrue(expected.toString().equals(entries.toString()));
		}
	}

	@Test
	void getWorkspaceResourcesByType() throws Exception {
		Clob clob = mock(Clob.class);
		Blob blob = mock(Blob.class);
		String[] headers = new String[] { "header1", "header2", "header3" };
		Object[] values = new Object[] { clob, blob, 1 };

		Map<String, Object> map = new HashMap<>();
		map.put("header1", "clob");
		map.put("header2", "blob");
		map.put("header3", "1");
		List<Map<String, Object>> expected = new ArrayList<>();
		expected.add(map);

		try (MockedStatic<WrapperManager> staticWrapper = Mockito.mockStatic(WrapperManager.class);
				MockedStatic<AbstractSqlQueryUtil> staticQueryUtil = Mockito.mockStatic(AbstractSqlQueryUtil.class)) {
			staticWrapper.when(() -> WrapperManager.getInstance()).thenReturn(wrapperManager);
			when(wrapperManager.getRawWrapper(eq(engine), any(SelectQueryStruct.class))).thenReturn(rawWrapper);
			when(rawWrapper.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
			when(rawWrapper.next()).thenThrow(NoSuchElementException.class).thenReturn(dataRow);
			when(dataRow.getHeaders()).thenReturn(headers);
			when(dataRow.getValues()).thenReturn(values);

			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushClobToString(any(Clob.class))).thenReturn("clob");
			staticQueryUtil.when(() -> AbstractSqlQueryUtil.flushBlobToString(any(Blob.class))).thenReturn("blob");

			assertNull(ModelInferenceLogsUtils.getWorkspaceResourcesByType("workspaceId", List.of("resourceType")));
			assertTrue(expected.toString().equals(ModelInferenceLogsUtils
					.getWorkspaceResourcesByType("workspaceId", List.of("resourceType")).toString()));
		}
	}

	@Test
	void createNewWorkspaceResource() throws Exception {
		when(engine.getConnection()).thenReturn(conn);
		when(conn.prepareStatement(
				"INSERT INTO WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID, WORKSPACE_ID, RESOURCE_ID, RESOURCE_TYPE, RESOURCE_SUBTYPE) VALUES (?,?,?,?,?)"))
				.thenReturn(ps);
		when(ps.execute()).thenThrow(SQLException.class).thenReturn(true);
		when(conn.getAutoCommit()).thenReturn(false);

		Exception e = assertThrows(IllegalArgumentException.class,
				() -> ModelInferenceLogsUtils.createNewWorkspaceResource("workspaceResourceId", "workspaceId",
						"resourceId", "resourceType", "resourceSubType"));
		assertEquals("Error creating workspace resource: null", e.getMessage());

		ModelInferenceLogsUtils.createNewWorkspaceResource("workspaceResourceId", "workspaceId", "resourceId",
				"resourceType", "resourceSubType");

		verify(engine, times(2)).getConnection();
		verify(conn, times(2)).prepareStatement(anyString());
		verify(ps, times(10)).setString(anyInt(), anyString());
		verify(ps, times(2)).execute();
		verify(conn).getAutoCommit();
		verify(conn).commit();
	}

	@Test
	void doSetWorkspaceToInactive() throws Exception {
		when(engine.getConnection()).thenReturn(conn);
		when(conn.prepareStatement("UPDATE WORKSPACE SET IS_ACTIVE = ? WHERE WORKSPACE_ID = ?")).thenReturn(ps);
		when(ps.execute()).thenThrow(SQLException.class).thenReturn(true);
		when(conn.getAutoCommit()).thenReturn(false);

		Exception e = assertThrows(IllegalArgumentException.class,
				() -> ModelInferenceLogsUtils.doSetWorkspaceToInactive("workspaceId"));
		assertEquals("Error deactivating workspace: null", e.getMessage());

		ModelInferenceLogsUtils.doSetWorkspaceToInactive("workspaceId");

		verify(engine, times(2)).getConnection();
		verify(conn, times(2)).prepareStatement(anyString());
		verify(ps, times(2)).setBoolean(anyInt(), anyBoolean());
		verify(ps, times(2)).setString(anyInt(), anyString());
		verify(ps, times(2)).execute();
		verify(conn).getAutoCommit();
		verify(conn).commit();
	}
}
