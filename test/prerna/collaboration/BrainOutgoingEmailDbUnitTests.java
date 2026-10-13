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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class BrainOutgoingEmailDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	private MockedStatic<RoomUtils> rooms;
	private MockedStatic<MicrosoftLoginUtils> login;
	private MockedStatic<HttpHelperUtility> http;
	private User user;
	private Insight insight;
	private Room room;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_outgoing_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString())).thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil).getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					columns.add(column.getValue0() + " " + column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		AccessToken token = new AccessToken();
		token.setId("owner-" + UUID.randomUUID());
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		var owner = CollaborationDbUtils.ownerOf(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		when(insight.getRoomId()).thenReturn("room-1");
		room = mock(Room.class);
		when(room.getId()).thenReturn("room-1");
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of());
		rooms = mockStatic(RoomUtils.class);
		rooms.when(() -> RoomUtils.getOrLoadRoom("room-1", insight)).thenReturn(room);
		logs = mockStatic(ModelInferenceLogsUtils.class);
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(token.getId(), List.of("room-1")))
				.thenReturn(List.of(Map.of("ROOM_ID", "room-1", "PROJECT_ID", CollaborationUtils.COLLABORATION_PROJECT_ID)));
		login = mockStatic(MicrosoftLoginUtils.class);
		login.when(() -> MicrosoftLoginUtils.getValidAccessToken(user)).thenReturn("token");
		http = mockStatic(HttpHelperUtility.class);
		draft("conv-1");
		CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) "
				+ "VALUES (?, ?, 'topic-1', 'Release updates', 'active')", ownerId, ownerType);
		CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE) "
				+ "VALUES (?, ?, 'room-1', 'topic-1', 'linked')", ownerId, ownerType);
	}

	@AfterEach
	void tearDown() throws Exception {
		http.close();
		login.close();
		logs.close();
		rooms.close();
		registry.close();
		connection.close();
	}

	@Test
	void newMailIsDurablyLinkedBeforeSendWithoutImportingTheDraft() throws Exception {
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		String expected = "th-" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "thread", "email:conv-1");
		assertEquals(expected, value("SELECT THREAD_ID FROM BRAIN_THREAD"));
		assertEquals(1, count("BRAIN_THREAD"));
		assertEquals(0, count("BRAIN_MESSAGE"));
		assertEquals(1, count("BRAIN_THREAD_TOPIC"));
		assertEquals("you", value("SELECT SOURCE FROM BRAIN_THREAD_TOPIC"));
		assertEquals(Boolean.TRUE, value("SELECT IS_PRIMARY FROM BRAIN_THREAD_TOPIC"));
		assertEquals("linked", value("SELECT STATE FROM BRAIN_THREAD_ROOM"));
		assertEquals("message:email:" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "email", "<Original@Example.com>"),
				value("SELECT THREAD_KEY FROM BRAIN_THREAD_ALIAS"));
		assertEquals(expected, value("SELECT THREAD_ID FROM BRAIN_THREAD_ALIAS"));
	}

	@Test
	void sentCopyImportsWithItsActualGraphIdIntoTheReservedTopicThread() throws Exception {
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		CollaborationDbUtils.update("INSERT INTO SOURCE_CONNECTION (OWNER_ID, OWNER_TYPE, SOURCE, ENABLED) "
				+ "VALUES (?, ?, 'email', TRUE)", ownerId, ownerType);
		BrainMailImport.Run run = new BrainMailImport.Run(ownerId, ownerType);
		run.ensureSelf("owner@test.com", "Owner", "owner@test.com", List.of());
		Map<String, Object> sent = new HashMap<>();
		sent.put("id", "sent-1");
		sent.put("internetMessageId", "<Original@Example.com>");
		sent.put("conversationId", "conv-1");
		sent.put("subject", "Release updates");
		sent.put("from", Map.of("emailAddress", Map.of("address", "owner@test.com")));
		sent.put("toRecipients", List.of(Map.of("emailAddress", Map.of("address", "recipient@test.com"))));
		sent.put("receivedDateTime", "2026-10-11T23:00:00Z");
		run.importOne(sent);
		run.refreshThreads();
		assertEquals(1, count("BRAIN_THREAD"));
		assertEquals(1, count("BRAIN_MESSAGE"));
		assertEquals("sent-1", value("SELECT GRAPH_ID FROM BRAIN_MESSAGE"));
		assertEquals(value("SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC"), value("SELECT THREAD_ID FROM BRAIN_MESSAGE"));
	}

	@Test
	void sendingInAnAliasedConversationReusesTheExistingThread() throws Exception {
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		String threadId = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD");
		CollaborationDbUtils.update("INSERT INTO BRAIN_THREAD_ALIAS (OWNER_ID, OWNER_TYPE, THREAD_KEY, THREAD_ID) "
				+ "VALUES (?, ?, 'email:outside-conversation', ?)", ownerId, ownerType, threadId);
		draft("outside-conversation");
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		assertEquals(1, count("BRAIN_THREAD"));
		assertEquals(threadId, value("SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC"));
	}

	@Test
	void missingTopicRollsBackEveryLinkBeforeSending() {
		CollaborationDbUtils.update("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE) "
				+ "VALUES (?, ?, 'room-1', 'topic-deleted', 'linked')", ownerId, ownerType);
		assertThrows(IllegalArgumentException.class, () -> BrainOutgoingEmail.trackDraft(insight, "draft-1"));
		assertEquals(0, count("BRAIN_THREAD"));
		assertEquals(0, count("BRAIN_THREAD_TOPIC"));
		assertEquals(0, count("BRAIN_THREAD_ALIAS"));
	}

	@Test
	void anotherUsersCachedRoomCannotLinkOrReadADraft() {
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(anyString(), anyList())).thenReturn(List.of());
		assertThrows(IllegalArgumentException.class, () -> BrainOutgoingEmail.trackDraft(insight, "draft-1"));
		assertEquals(0, count("BRAIN_THREAD"));
		http.verifyNoInteractions();
	}

	@Test
	void ordinaryAndDelegationMailHaveNoCollaborationSideEffects() throws Exception {
		when(insight.getRoomId()).thenReturn(null);
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		when(insight.getRoomId()).thenReturn("room-1");
		when(room.getProjectId()).thenReturn("generic-project");
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "delegated"));
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		assertEquals(0, count("BRAIN_THREAD"));
		logs.verifyNoInteractions();
		http.verifyNoInteractions();
	}

	@Test
	void incompleteProviderIdentityCannotSilentlySendUntrackedMail() {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenReturn("{\"id\":\"draft-1\"}");
		assertThrows(IllegalStateException.class, () -> BrainOutgoingEmail.trackDraft(insight, "draft-1"));
		assertEquals(0, count("BRAIN_THREAD"));
	}

	@Test
	void longInternetMessageIdsFitTheExistingAliasKey() throws Exception {
		String messageId = "<" + "A".repeat(260) + "@Example.com>";
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenReturn("{\"id\":\"draft-1\",\"conversationId\":\"conv-1\",\"internetMessageId\":\"" + messageId + "\"}");
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		assertEquals("message:email:" + CollaborationDbUtils.deterministicId(ownerId, ownerType, "email", messageId),
				value("SELECT THREAD_KEY FROM BRAIN_THREAD_ALIAS"));
	}

	@Test
	void missingInternetMessageIdStillLinksTheConversationToItsRoomAndTopic() throws Exception {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenReturn("{\"id\":\"draft-1\",\"conversationId\":\"conv-1\",\"subject\":\"Release updates\"}");
		BrainOutgoingEmail.trackDraft(insight, "draft-1");
		String threadId = (String) value("SELECT THREAD_ID FROM BRAIN_THREAD");
		assertNotNull(threadId);
		assertEquals("email:conv-1", value("SELECT THREAD_KEY FROM BRAIN_THREAD"));
		assertEquals(threadId, value("SELECT THREAD_ID FROM BRAIN_THREAD_TOPIC"));
		assertEquals("topic-1", value("SELECT TOPIC_ID FROM BRAIN_THREAD_TOPIC"));
		assertEquals(threadId, value("SELECT THREAD_ID FROM BRAIN_THREAD_ROOM"));
		assertEquals("room-1", value("SELECT ROOM_ID FROM BRAIN_THREAD_ROOM"));
		assertEquals("linked", value("SELECT STATE FROM BRAIN_THREAD_ROOM"));
		assertEquals(0, count("BRAIN_THREAD_ALIAS"));
		assertEquals(0, count("BRAIN_MESSAGE"));
	}

	private void draft(String conversationId) {
		http.when(() -> HttpHelperUtility.getRequest(anyString(), anyMap(), isNull(), isNull(), isNull()))
				.thenReturn("{\"id\":\"draft-1\",\"conversationId\":\"" + conversationId
						+ "\",\"internetMessageId\":\"<Original@Example.com>\",\"subject\":\"Release updates\"}");
	}

	private int count(String table) {
		return CollaborationDbUtils.count("SELECT COUNT(*) FROM " + table);
	}

	private Object value(String sql) {
		return CollaborationDbUtils.queryOne(sql, rs -> rs.getObject(1));
	}
}
