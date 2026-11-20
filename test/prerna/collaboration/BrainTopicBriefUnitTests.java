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
 *
 * 	You should have received a copy of the GNU General Public License
 * 	along with this program; if not, write to the Free Software
 * 	Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *******************************************************************************/
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class BrainTopicBriefUnitTests {

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<ModelInferenceLogsUtils> logs;
	private final Map<String, Map<String, Object>> rooms = new LinkedHashMap<>();
	private User user;
	private String ownerId;
	private String ownerType;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:collab_topic_brief_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString()))
				.thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
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
		token.setId("brief-owner");
		token.setProvider(AuthProvider.NATIVE);
		user = new User();
		user.setAccessToken(token);
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		ownerId = owner.getValue0();
		ownerType = owner.getValue1();
		logs = mockStatic(ModelInferenceLogsUtils.class);
		logs.when(() -> ModelInferenceLogsUtils.getActiveRoomSummaries(eq(ownerId), anyList())).thenAnswer(call -> {
			List<Map<String, Object>> found = new ArrayList<>();
			for (Object roomId : (List<?>) call.getArgument(1)) {
				if (rooms.containsKey(roomId)) {
					found.add(rooms.get(roomId));
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
	void includesTopicsOfAllCurrentThreadsWithoutChangingRoomTopicTags() throws Exception {
		Room room = room("chat", Map.of("source", Map.of("threadId", "original")));
		thread("original");
		thread("added");
		topic("direct");
		topic("from-original");
		topic("from-added");
		roomTopic("direct", "linked", "you");
		threadTopic("original", "from-original", "you");
		threadTopic("added", "from-added", "you");
		threadTopic("added", "from-original", "you");
		roomThread("added", "linked");

		assertEquals(List.of("direct", "from-added", "from-original"), BrainTopicBrief.chatTopics(user, room));
		assertEquals(BrainTopicBrief.chatTopics(user, room), BrainTopicBrief.chatTopics(user, "chat"));
		assertEquals(1, CollaborationDbUtils.count("SELECT COUNT(*) FROM BRAIN_TOPIC_ROOM"));

		topic("added-later");
		threadTopic("added", "added-later", "you");
		assertTrue(BrainTopicBrief.chatTopics(user, room).contains("added-later"));
	}

	@Test
	void filtersUnconfirmedExcludedAndOtherOwnerTopics() throws Exception {
		Room room = room("chat", Map.of());
		thread("included");
		roomThread("included", "linked");
		for (String id : List.of("active", "dormant", "archived", "suggested", "unconfirmed", "dismissed",
				"room-suggestion", "ignored", "person-ignore")) {
			topic(id);
			threadTopic("included", id, "unconfirmed".equals(id) ? "suggested" : "you");
		}
		for (String status : List.of("dormant", "archived", "suggested")) {
			sql("UPDATE BRAIN_TOPIC SET STATUS = ? WHERE TOPIC_ID = ?", status, status);
		}
		roomTopic("dismissed", "dismissed", "you");
		roomTopic("room-suggestion", "suggested", "brain");
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, TOPIC_ID) "
				+ "VALUES (?, ?, 'exclude', 'exclude_topic', 'ignored')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_RULE (OWNER_ID, OWNER_TYPE, RULE_ID, KIND, TOPIC_ID, PERSON_ID) "
				+ "VALUES (?, ?, 'person-exclude', 'exclude_topic', 'person-ignore', 'someone')", ownerId, ownerType);
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) "
				+ "VALUES ('other-owner', ?, 'foreign', 'Private', 'active')", ownerType);
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES ('other-owner', ?, 'chat', 'foreign', 'linked', 'you')", ownerType);

		assertEquals(List.of("active", "dormant", "person-ignore"), BrainTopicBrief.chatTopics(user, room));
	}

	@Test
	void removingThreadsRemovesInheritedContextButKeepsExplicitRoomTopics() throws Exception {
		Room room = room("chat", Map.of("source", Map.of("threadId", "original")));
		thread("original");
		thread("added");
		topic("from-original");
		topic("from-added");
		threadTopic("original", "from-original", "you");
		threadTopic("added", "from-added", "you");
		roomTopic("from-original", "linked", "thread");
		roomThread("original", "removed");
		roomThread("added", "linked");
		assertEquals(List.of("from-added"), BrainTopicBrief.chatTopics(user, room));

		sql("UPDATE BRAIN_THREAD_ROOM SET STATE = 'removed' WHERE THREAD_ID = 'added'");
		assertTrue(BrainTopicBrief.chatTopics(user, room).isEmpty());
		sql("UPDATE BRAIN_TOPIC_ROOM SET ORIGIN = 'you' WHERE TOPIC_ID = 'from-original'");
		assertEquals(List.of("from-original"), BrainTopicBrief.chatTopics(user, room));
	}

	@Test
	void onlyOwnedAssistantRoomsCanSupplyContextById() {
		Room unrelated = room("other-project", Map.of());
		when(unrelated.getProjectId()).thenReturn("another-project");
		rooms.get("other-project").put("PROJECT_ID", "another-project");
		Room delegation = room("delegation", Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "action"));
		for (String id : List.of("not-owned", "other-project", "delegation")) {
			assertThrows(IllegalArgumentException.class, () -> BrainTopicBrief.chatTopics(user, id));
		}
		assertTrue(BrainTopicBrief.chatTopics(user, unrelated).isEmpty());
		assertTrue(BrainTopicBrief.chatTopics(user, delegation).isEmpty());
		assertTrue(BrainTopicBrief.chatTopics(null, unrelated).isEmpty());
		assertNull(BrainTopicBrief.runtimeNote(user, delegation));
	}

	@Test
	void briefCarriesGoalIdsAndReflectsCompletionOnTheNextCall() throws Exception {
		Room room = room("chat", Map.of());
		topic("nevada");
		roomTopic("nevada", "linked", "you");
		String goalId = (String) BrainTopicUtils.saveTopicNote(user, "nevada", null, BrainTopicUtils.GOAL,
				"Get booking details for nevada", "open").get("noteId");
		String before = BrainTopicBrief.runtimeNote(user, room);
		assertTrue(before.contains("Get booking details for nevada (noteId " + goalId + ")"));
		assertFalse(before.contains("more open goals"));

		BrainTopicUtils.completeGoal(user, "nevada", goalId, "American flight 4884, arrival 11/01/2026");
		String after = BrainTopicBrief.runtimeNote(user, room);
		assertTrue(after.contains("Open goals: none"));
		assertFalse(after.contains(goalId));
		assertEquals("done", BrainTopicUtils.getGoals(ownerId, ownerType, "nevada").get(0).get("status"));
	}

	@Test
	void reportsGoalsBeyondTheBriefLimit() throws Exception {
		Room room = room("chat", Map.of());
		topic("many-goals");
		roomTopic("many-goals", "linked", "you");
		for (int i = 0; i < BrainTopicBrief.MAX_GOALS + 2; i++) {
			BrainTopicUtils.saveTopicNote(user, "many-goals", null, BrainTopicUtils.GOAL, "Goal " + i, "open");
		}
		BrainTopicUtils.saveTopicNote(user, "many-goals", null, BrainTopicUtils.GOAL, "Already finished", "done");
		String brief = BrainTopicBrief.runtimeNote(user, room);
		assertEquals(BrainTopicBrief.MAX_GOALS, brief.lines().filter(line -> line.contains("(noteId ")).count());
		assertTrue(brief.contains("2 more open goals not shown; use ListTopics with this topic to review them."));
		assertFalse(brief.contains("Already finished"));
	}

	private Room room(String id, Map<String, Object> options) {
		Room room = mock(Room.class);
		when(room.getId()).thenReturn(id);
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		when(room.getOptionsMap()).thenReturn(options);
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("ROOM_ID", id);
		row.put("PROJECT_ID", CollaborationUtils.COLLABORATION_PROJECT_ID);
		row.put("OPTIONS", CollaborationDbUtils.toJson(options));
		rooms.put(id, row);
		return room;
	}

	private void topic(String id) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC (OWNER_ID, OWNER_TYPE, TOPIC_ID, NAME, STATUS) VALUES (?, ?, ?, ?, 'active')",
				ownerId, ownerType, id, id);
	}

	private void thread(String id) throws Exception {
		sql("INSERT INTO BRAIN_THREAD (OWNER_ID, OWNER_TYPE, THREAD_ID, SOURCE) VALUES (?, ?, ?, 'email')", ownerId,
				ownerType, id);
	}

	private void threadTopic(String threadId, String topicId, String source) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_TOPIC (OWNER_ID, OWNER_TYPE, THREAD_ID, TOPIC_ID, SOURCE) VALUES (?, ?, ?, ?, ?)",
				ownerId, ownerType, threadId, topicId, source);
	}

	private void roomTopic(String topicId, String state, String origin) throws Exception {
		sql("INSERT INTO BRAIN_TOPIC_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, TOPIC_ID, STATE, ORIGIN) "
				+ "VALUES (?, ?, 'chat', ?, ?, ?)", ownerId, ownerType, topicId, state, origin);
	}

	private void roomThread(String threadId, String state) throws Exception {
		sql("INSERT INTO BRAIN_THREAD_ROOM (OWNER_ID, OWNER_TYPE, ROOM_ID, THREAD_ID, STATE) "
				+ "VALUES (?, ?, 'chat', ?, ?)", ownerId, ownerType, threadId, state);
	}

	private void sql(String sql, Object... params) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}
			statement.executeUpdate();
		}
	}
}
