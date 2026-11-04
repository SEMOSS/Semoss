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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.redis.RedisConnectionConfig;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageIO;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.reactor.agent.runtime.AgentCodeExecutionContext;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.run.AutomationRunStore;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class WorkRoutineRoomUnitTests {
	private static final String STARTED = "2026-10-15T12:00:00Z";
	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<Utility> utility;
	private MockedStatic<RedisConnectionConfig> redis;
	private MockedStatic<BrainTopicModel> models;
	private MockedStatic<RoomUtils> rooms;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	private User user;
	private Insight insight;
	private final Map<String, String> owners = new HashMap<>();
	private final Map<String, String> messages = new HashMap<>();
	private final Map<String, String> projects = new HashMap<>();
	private final Map<String, Map<String, Object>> options = new HashMap<>();
	private boolean failPersist;
	private int writes;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:routine_room_" + UUID.randomUUID());
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		when(engine.getPreparedStatement(anyString())).thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (String table : List.of("WORK_INBOX_ROUTINE", "WORK_AGENDA_ROUTINE", "WORK_EXPENSE_ROUTINE",
					"WORK_CUSTOM_ROUTINE")) {
				statement.execute("CREATE TABLE " + table + " (OWNER_ID VARCHAR, OWNER_TYPE VARCHAR, ROUTINE_ID VARCHAR, "
						+ "SETTINGS_JSON CLOB, ENABLED BOOLEAN, RUN_STATUS VARCHAR, STARTED_AT TIMESTAMP, "
						+ "FINISHED_AT TIMESTAMP, LAST_SUCCESS_AT TIMESTAMP, ROOM_ID VARCHAR, RUN_ID VARCHAR, "
						+ "SUMMARY CLOB, ERROR CLOB, ERROR_CAUSE VARCHAR)");
			}
		}
		user = owner("owner-one");
		insight = insight(user);
		utility = mockStatic(Utility.class);
		utility.when(Utility::getApplicationZoneId).thenReturn("UTC");
		redis = mockStatic(RedisConnectionConfig.class);
		models = mockStatic(BrainTopicModel.class);
		models.when(() -> BrainTopicModel.engine(any())).thenReturn("collaboration-model");
		rooms = mockStatic(RoomUtils.class);
		rooms.when(() -> RoomUtils.createRoomIfNotExists(anyString(), any(), isNull(), anyString(),
				isNull(), anyMap(), isNull(), anyString(), isNull())).thenAnswer(call -> {
			String id = call.getArgument(0);
			Insight caller = call.getArgument(1);
			String ownerId = caller.getUser().getPrimaryLoginToken().getId();
			if (owners.containsKey(id) && !ownerId.equals(owners.get(id))) {
				throw new IllegalArgumentException("Room is not valid for this user");
			}
			owners.putIfAbsent(id, ownerId);
			messages.putIfAbsent(id, "[]");
			projects.putIfAbsent(id, call.getArgument(7));
			options.putIfAbsent(id, call.getArgument(5));
			return storedRoom(id);
		});
		logs = mockStatic(ModelInferenceLogsUtils.class);
		logs.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(anyString()))
				.thenAnswer(call -> owners.containsKey(call.getArgument(0)));
		logs.when(() -> ModelInferenceLogsUtils.getRoomById(anyString(), anyString())).thenAnswer(call -> {
			String id = call.getArgument(0);
			return call.getArgument(1).equals(owners.get(id)) ? storedRoom(id) : null;
		});
		logs.when(() -> ModelInferenceLogsUtils.llm2_updateRoomMessages(anyString(), anyString(), anyString())).thenAnswer(call -> {
			writes++;
			if (failPersist) {
				return false;
			}
			assertEquals(owners.get(call.getArgument(0)), call.getArgument(1));
			messages.put(call.getArgument(0), call.getArgument(2));
			return true;
		});
	}

	@AfterEach
	void tearDown() throws Exception {
		logs.close();
		rooms.close();
		models.close();
		redis.close();
		utility.close();
		registry.close();
		user.removeUserMemory();
		connection.close();
	}

	@ParameterizedTest
	@ValueSource(strings = { WorkInboxRoutine.ID, WorkAgendaRoutine.ID })
	void opensExactOutputAsFirstAssistantMessageWithCollaborationTools(String routineId) {
		String summary = "# Latest output\n\nEmail follow-ups and upcoming meetings.";
		completed(user, routineId, STARTED, summary);

		String id = open(routineId);
		Room room = storedRoom(id);
		assertEquals(1, room.getMessages().size());
		AbstractMessage first = room.getMessages().getFirst();
		assertInstanceOf(ResponseMessage.class, first);
		assertEquals(MessageIO.OUTPUT, first.getIo());
		assertEquals(summary, ((ResponseMessage) first).getContent());
		assertTrue(first.isVisible());
		assertNull(first.getParentMessageId());
		assertTrue(CollaborationAgentTools.appliesTo(room));
		assertTrue(CollaborationUtils.isAssistantRoom(room));
		assertEquals("collaboration-model", room.getOptionsMap().get("modelId"));
		assertNotEquals("automation-room", id);
	}

	@Test
	void reopeningKeepsFollowupMessagesAndDoesNotDuplicateOutput() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Saved output");
		String id = open(WorkInboxRoutine.ID);
		Room room = storedRoom(id);
		InputMessage followup = InputMessage.builder(room).withText("Which meetings need preparation?").build();
		followup.setParentMessageId(room.getMessages().getFirst().getMessageId());
		room.getMessages().add(followup);
		RoomMessageStore.persist(room, "owner-one");

		assertEquals(id, open(WorkInboxRoutine.ID));
		assertEquals(2, storedRoom(id).getMessages().size());
		assertEquals(followup.getMessageId(), storedRoom(id).getMessages().getLast().getMessageId());
		assertEquals(2, writes);
	}

	@Test
	void failedInitialPersistenceCanBeRetriedWithoutReturningAnEmptyRoom() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Saved output");
		failPersist = true;
		assertThrows(IllegalStateException.class, () -> open(WorkInboxRoutine.ID));
		assertEquals(1, owners.size());
		assertEquals("[]", messages.values().iterator().next());

		failPersist = false;
		String id = open(WorkInboxRoutine.ID);
		assertEquals(1, owners.size());
		assertEquals(1, storedRoom(id).getMessages().size());
		assertEquals(2, writes);
	}

	@Test
	void anotherOwnersOutputCannotBeOpened() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Private output");
		User other = owner("owner-two");
		try {
			assertThrows(IllegalArgumentException.class,
					() -> WorkRoutineRoom.open(insight(other), WorkInboxRoutine.ID, STARTED));
			rooms.verifyNoInteractions();
		} finally {
			other.removeUserMemory();
		}
	}

	@Test
	void sameRunForDifferentOwnersHasDifferentRoomsAndOnlyTheirOutput() {
		completed(user, WorkInboxRoutine.ID, STARTED, "First owner's output");
		String first = open(WorkInboxRoutine.ID);
		User other = owner("owner-two");
		try {
			completed(other, WorkInboxRoutine.ID, STARTED, "Second owner's output");
			String second = (String) WorkRoutineRoom.open(insight(other), WorkInboxRoutine.ID, STARTED).get("roomId");
			assertNotEquals(first, second);
			assertEquals("Second owner's output", ((ResponseMessage) storedRoom(second).getMessages().getFirst()).getContent());
		} finally {
			other.removeUserMemory();
		}
	}

	@Test
	void staleDisplayedOutputIsRejectedAndANewerRunGetsItsOwnRoom() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Yesterday");
		String previous = open(WorkInboxRoutine.ID);
		String newer = "2026-10-16T12:00:00Z";
		completed(user, WorkInboxRoutine.ID, newer, "Today");
		assertThrows(IllegalArgumentException.class, () -> open(WorkInboxRoutine.ID));
		String next = (String) WorkRoutineRoom.open(insight, WorkInboxRoutine.ID, newer).get("roomId");
		assertNotEquals(previous, next);
		assertEquals("Yesterday", ((ResponseMessage) storedRoom(previous).getMessages().getFirst()).getContent());
		assertEquals("Today", ((ResponseMessage) storedRoom(next).getMessages().getFirst()).getContent());
	}

	@ParameterizedTest
	@ValueSource(strings = { "running", "failed", "completed" })
	void missingOrIncompleteOutputIsRejected(String status) {
		completed(user, WorkInboxRoutine.ID, STARTED, status.equals("completed") ? " " : "Incomplete output");
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_STATUS = ?", status);
		assertThrows(IllegalArgumentException.class, () -> open(WorkInboxRoutine.ID));
		rooms.verifyNoInteractions();
	}

	@Test
	void aRoutineWhoseAgentAnsweredOpensTheRoomItWorkedInWithoutAChat() {
		completed(user, WorkExpenseRoutine.ID, STARTED, "Suggested decision");
		User other = owner("owner-two");
		try (MockedStatic<AutomationRunStore> history = runHistory(WorkExpenseRoutine.KIND, "run-1",
				AutomationConstants.STATUS_SUCCESS)) {
			assertEquals("automation-room", open(WorkExpenseRoutine.ID));
			assertEquals("agent-room", WorkRoutineRoom.open(insight, WorkExpenseRoutine.ID, null, "run-1").get("roomId"));
			assertThrows(IllegalArgumentException.class,
					() -> WorkRoutineRoom.open(insight(other), WorkExpenseRoutine.ID, null, "run-1"));
			rooms.verifyNoInteractions();
		} finally {
			other.removeUserMemory();
		}
	}

	@Test
	void unknownRoutineCannotSelectAnArbitraryTable() {
		assertThrows(IllegalArgumentException.class, () -> open("WORK_INBOX_ROUTINE"));
		rooms.verifyNoInteractions();
	}

	@Test
	void latestOutputOpensTheChatHistoryOpensForItsRun() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Saved output");
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_ID = ?", "run-1");
		try (MockedStatic<AutomationRunStore> history = runHistory("run-1", AutomationConstants.STATUS_SUCCESS)) {
			String id = open(WorkInboxRoutine.ID);
			assertEquals(id, WorkRoutineRoom.open(insight, WorkInboxRoutine.ID, null, "run-1").get("roomId"));
			assertEquals(1, storedRoom(id).getMessages().size());
		}
	}

	@Test
	void aChatLatestOpenedBeforeItsRunWasKnownKeepsOpening() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Saved output");
		String earlier = open(WorkInboxRoutine.ID);
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_ID = ?", "run-1");
		assertEquals(earlier, open(WorkInboxRoutine.ID));
	}

	@Test
	void deletingARunDeletesItsChatsAndItsAgentsRoomsBeforeTheRun() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Saved output");
		String latest = open(WorkInboxRoutine.ID);
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_ID = ?", "run-1");
		List<String> closed = new ArrayList<>();
		logs.when(() -> ModelInferenceLogsUtils.doSetRoomToInactive(anyString(), anyString())).thenAnswer(call -> {
			assertEquals("owner-one", call.getArgument(0));
			closed.add(call.getArgument(1));
			return true;
		});
		try (MockedStatic<AutomationRunStore> history = runHistory("run-1", AutomationConstants.STATUS_SUCCESS);
				MockedStatic<AgentCodeExecutionContext> code = mockStatic(AgentCodeExecutionContext.class)) {
			String chat = (String) WorkRoutineRoom.open(insight, WorkInboxRoutine.ID, null, "run-1").get("roomId");
			history.when(() -> AutomationRunStore.deleteRun(anyString(), anyString(), anyString())).thenAnswer(call -> {
				assertEquals(4, closed.size());
				return 1;
			});

			List<String> deleted = WorkRoutineRoom.deleteRun(insight, WorkInboxRoutine.ID, "run-1");

			assertEquals(List.of("agent-room", "loop-room", chat, latest), deleted);
			assertEquals(deleted, closed);
			history.verify(() -> AutomationRunStore.deleteRun(AutomationProjectService.inlineRunScope(user),
					WorkInboxRoutine.KIND.automationId(), "run-1"));
			code.verify(() -> AgentCodeExecutionContext.removeForRoom(user, chat));
		}
	}

	@Test
	void aChatThatCannotBeDeletedKeepsTheRunToTryAgain() {
		completed(user, WorkInboxRoutine.ID, STARTED, "Saved output");
		logs.when(() -> ModelInferenceLogsUtils.doSetRoomToInactive(anyString(), anyString())).thenReturn(false);
		try (MockedStatic<AutomationRunStore> history = runHistory("run-1", AutomationConstants.STATUS_SUCCESS)) {
			assertThrows(IllegalStateException.class,
					() -> WorkRoutineRoom.deleteRun(insight, WorkInboxRoutine.ID, "run-1"));
			history.verify(() -> AutomationRunStore.deleteRun(anyString(), anyString(), anyString()), never());
		}
	}

	/** The scheduler's record of one of the owner's inbox runs: its output and the rooms its agent worked in. */
	private MockedStatic<AutomationRunStore> runHistory(String runId, String status) {
		return runHistory(WorkInboxRoutine.KIND, runId, status);
	}

	private MockedStatic<AutomationRunStore> runHistory(WorkRoutine.Kind kind, String runId, String status) {
		MockedStatic<AutomationRunStore> history = mockStatic(AutomationRunStore.class);
		Map<String, Object> run = new HashMap<>();
		run.put(AutomationConstants.RUN_ID, runId);
		run.put(AutomationConstants.PROJECT_ID, AutomationProjectService.inlineRunScope(user));
		run.put(AutomationConstants.AUTOMATION_ID, kind.automationId());
		run.put(AutomationConstants.STATUS, status);
		run.put(AutomationConstants.STARTED_AT, Timestamp.valueOf("2026-10-15 12:00:05"));
		history.when(() -> AutomationRunStore.getRunDetail(runId)).thenReturn(run);
		Map<String, Object> agent = new HashMap<>();
		agent.put(AutomationConstants.NODE_ID, kind.outputNode());
		agent.put(AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS);
		agent.put(AutomationConstants.OUTPUT_VALUE, "\"Saved output\"");
		agent.put(AutomationConstants.ROOM_ID, "agent-room");
		Map<String, Object> loop = new HashMap<>();
		loop.put(AutomationConstants.NODE_ID, "step");
		loop.put(AutomationConstants.PARENT_NODE_ID, "loop");
		loop.put(AutomationConstants.ROOM_ID, "loop-room");
		history.when(() -> AutomationRunStore.getNodeOutputsForRun(runId)).thenReturn(List.of(agent, loop));
		return history;
	}

	private String open(String routineId) {
		return (String) WorkRoutineRoom.open(insight, routineId, STARTED).get("roomId");
	}

	private void completed(User owner, String routineId, String started, String summary) {
		String table = WorkInboxRoutine.ID.equals(routineId) ? "WORK_INBOX_ROUTINE"
				: WorkExpenseRoutine.ID.equals(routineId) ? "WORK_EXPENSE_ROUTINE" : "WORK_AGENDA_ROUTINE";
		var identity = CollaborationDbUtils.ownerOf(owner);
		CollaborationDbUtils.update("DELETE FROM " + table + " WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				identity.getValue0(), identity.getValue1());
		CollaborationDbUtils.update("INSERT INTO " + table
				+ " (OWNER_ID, OWNER_TYPE, SETTINGS_JSON, ENABLED, RUN_STATUS, STARTED_AT, ROOM_ID, SUMMARY) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
				identity.getValue0(), identity.getValue1(), "{}", true, "completed",
				CollaborationDbUtils.toTimestamp(started, "startedAt"), "automation-room", summary);
	}

	private Room storedRoom(String id) {
		Room room = new Room();
		room.setId(id);
		room.setUserId(owners.get(id));
		room.setProjectId(projects.get(id));
		room.setOptionsMap(options.get(id));
		room.setMessages(RoomMessageStore.loadFromPersistedJson(room, messages.get(id)));
		return room;
	}

	private static Insight insight(User user) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		return insight;
	}

	private static User owner(String id) {
		User user = new User();
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.NATIVE);
		token.setId(id);
		user.setAccessToken(token);
		return user;
	}
}
