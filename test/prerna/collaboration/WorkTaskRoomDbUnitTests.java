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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
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
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

/** A work item's chats against an in-memory H2 Collaboration schema: links, the list, item merge, the old column. */
class WorkTaskRoomDbUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	// the user's open chats in the inference database, by id
	private final Map<String, Map<String, Object>> openChats = new HashMap<>();
	private User user;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_task_room_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil)
					.getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					// quoted as the server quotes them: H2 2.x reserves words such as VALUE
					String name = column.getValue0();
					columns.add((queryUtil.isSelectorKeyword(name) ? queryUtil.getEscapeKeyword(name) : name) + " "
							+ column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		String userId = "owner-" + UUID.randomUUID();
		AccessToken token = new AccessToken();
		token.setId(userId);
		token.setName("Owner");
		token.setEmail("owner@test.com");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		logs = mockStatic(ModelInferenceLogsUtils.class);
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(eq(userId), anyList())).thenAnswer(call -> {
			List<Map<String, Object>> found = new ArrayList<>();
			for (Object roomId : (List<?>) call.getArgument(1)) {
				if (openChats.containsKey(roomId)) {
					found.add(openChats.get(roomId));
				}
			}
			return found;
		});
	}

	@AfterEach
	void tearDown() throws Exception {
		logs.close();
		registry.close();
		connection.close();
	}

	@Test
	void anItemListsItsOpenChatsNewestFirstAndEachOnce() {
		String taskId = item("Budget");
		open("room-1");
		open("room-2");
		WorkTaskUtils.setRoom(user, taskId, "room-1");
		WorkTaskUtils.setRoom(user, taskId, "room-2");
		WorkTaskUtils.setRoom(user, taskId, "room-2");
		// both links are made within a millisecond; room-1 is the older one
		CollaborationDbUtils.update("UPDATE WORK_TASK_ROOM SET CREATED_AT = ? WHERE ROOM_ID = ?",
				Timestamp.valueOf("2026-01-01 00:00:00"), "room-1");

		Map<String, Object> listed = listed(taskId);
		assertEquals(List.of("room-2", "room-1"), roomIds(listed));
		assertEquals("room-2", listed.get("roomId"));
		assertEquals("room-2 chat", rooms(listed).get(0).get("title"));

		// the owner closes the newest chat: the one before it is the one to open
		openChats.remove("room-2");
		assertEquals(List.of("room-1"), roomIds(listed(taskId)));
		assertEquals("room-1", WorkTaskUtils.getWork(user, taskId).get("roomId"));
	}

	@Test
	void mergingAnItemMovesItsChatsToTheKeptItem() {
		String from = item("Old budget");
		String into = item("Budget");
		open("room-1");
		WorkTaskUtils.setRoom(user, from, "room-1");

		assertEquals(List.of("room-1"), roomIds(WorkTaskUtils.mergeWork(user, from, into)));
		assertEquals(into, CollaborationDbUtils.queryOne("SELECT TASK_ID FROM WORK_TASK_ROOM WHERE ROOM_ID = ?",
				rs -> rs.getString(1), "room-1"));
	}

	@Test
	void anOldRoomIdMovesOnceAndIsCleared() {
		String taskId = item("Budget");
		open("room-1");
		CollaborationDbUtils.update("UPDATE WORK_TASK SET ROOM_ID = ? WHERE TASK_ID = ?", "room-1", taskId);

		WorkTaskRoomMigration.run();
		WorkTaskRoomMigration.run();
		assertEquals(List.of("room-1"), roomIds(WorkTaskUtils.getWork(user, taskId)));
		assertNull(CollaborationDbUtils.queryOne("SELECT ROOM_ID FROM WORK_TASK WHERE TASK_ID = ?",
				rs -> rs.getString(1), taskId));

		// a chat taken off the item does not come back on the next boot
		CollaborationDbUtils.update("DELETE FROM WORK_TASK_ROOM");
		WorkTaskRoomMigration.run();
		assertEquals(List.of(), roomIds(WorkTaskUtils.getWork(user, taskId)));
	}

	private String item(String title) {
		return (String) WorkTaskUtils.saveWork(user, Map.of("title", title), null).get("id");
	}

	private void open(String roomId) {
		Map<String, Object> room = new HashMap<>();
		room.put("ROOM_ID", roomId);
		room.put("ROOM_NAME", roomId + " chat");
		room.put("PROJECT_ID", CollaborationUtils.COLLABORATION_PROJECT_ID);
		room.put("UPDATED_AT", Timestamp.valueOf("2026-11-01 09:00:00"));
		openChats.put(roomId, room);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> listed(String taskId) {
		List<Map<String, Object>> items = (List<Map<String, Object>>) WorkTaskUtils.listMyWork(user, Map.of())
				.get("workItems");
		return items.stream().filter(item -> taskId.equals(item.get("id"))).findFirst().orElseThrow();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> rooms(Map<String, Object> task) {
		return (List<Map<String, Object>>) task.get("rooms");
	}

	private static List<Object> roomIds(Map<String, Object> task) {
		return rooms(task).stream().map(room -> room.get("id")).toList();
	}
}
