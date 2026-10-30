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
package prerna.reactor.agent.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.RoomMessageStore;
import prerna.om.Insight;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.JdbcTestDatabase;

/**
 * Closing a room covers the room's runs and every run below them, wherever they
 * run, and the rooms they run in. Only an admin closes another user's room, and
 * each such close is recorded.
 */
class AgentRunServiceRoomTreeUnitTests {

	JdbcTestDatabase db;

	@BeforeEach
	void setup() throws Exception {
		db = new JdbcTestDatabase();
		when(db.engine.getPreparedStatement(anyString()))
				.thenAnswer(invocation -> db.connection.prepareStatement(invocation.getArgument(0, String.class)));
		db.execute("CREATE TABLE AGENT_RUN (RUN_ID VARCHAR, PARENT_RUN_ID VARCHAR, ROOM_ID VARCHAR, "
				+ "WORKSPACE_ID VARCHAR, MODEL_ID VARCHAR, HARNESS_TYPE VARCHAR, JOB_ID VARCHAR, STATUS VARCHAR, "
				+ "INPUT CLOB, REQUEST_JSON CLOB, INPUT_MESSAGE_ID VARCHAR, FINAL_OUTPUT CLOB, "
				+ "FINAL_OUTPUT_MESSAGE_ID VARCHAR, ERROR_MESSAGE CLOB, PROGRESS_JSON CLOB, DATE_CREATED TIMESTAMP, "
				+ "STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, USER_ID VARCHAR, USER_TYPE VARCHAR)",
				"CREATE TABLE ROOM (ROOM_ID VARCHAR, USER_ID VARCHAR, ROOM_NAME VARCHAR, IS_ACTIVE BOOLEAN)",
				"INSERT INTO ROOM VALUES ('r1', 'alice', 'Quarterly report', TRUE)");
		run("run1", null, "r1", "COMPLETED", "10:00", "alice");
		run("run2", null, "r1", "RUNNING", "11:00", "alice");
		// a subagent runs in a room of its own, and may spawn its own subagent
		run("child", "run2", "r1-sub", "RUNNING", "11:01", "alice");
		run("grandchild", "child", "r1-sub-sub", "SUBMITTED", "11:02", "alice");
		// a task for a person has no room
		run("person", "run2", null, "INPUT_REQUIRED", "11:03", "alice");
		// rooms are kept per user, so another user's room may have the same id
		run("bob-run", null, "r1", "RUNNING", "12:00", "bob");
		// another user's run under alice's is never alice's to close
		run("bob-under-alice", "run2", "r1-bob", "RUNNING", "11:04", "bob");
		// a subagent's result can start a continuation in the subagent's room, with no
		// parent run
		run("continuation", null, "r1-sub", "RUNNING", "11:05", "alice");
	}

	@AfterEach
	void close() throws Exception {
		db.close();
	}

	private void run(String runId, String parentRunId, String roomId, String status, String created, String userId)
			throws Exception {
		try (var statement = db.connection.prepareStatement("INSERT INTO AGENT_RUN (RUN_ID, PARENT_RUN_ID, ROOM_ID, "
				+ "WORKSPACE_ID, STATUS, DATE_CREATED, USER_ID) VALUES (?, ?, ?, 'agent-a', ?, ?, ?)")) {
			statement.setString(1, runId);
			statement.setString(2, parentRunId);
			statement.setString(3, roomId);
			statement.setString(4, status);
			statement.setObject(5, java.sql.Timestamp.valueOf("2026-10-01 " + created + ":00"));
			statement.setString(6, userId);
			statement.executeUpdate();
		}
	}

	private String status(String runId) throws Exception {
		try (var statement = db.connection.prepareStatement("SELECT STATUS FROM AGENT_RUN WHERE RUN_ID = ?")) {
			statement.setString(1, runId);
			try (var result = statement.executeQuery()) {
				return result.next() ? result.getString(1) : null;
			}
		}
	}

	private static List<Object> runIds(AgentRunService.RoomTree tree) {
		return tree.runs().stream().map(run -> run.get("runId")).toList();
	}

	@Test
	void coversTheRoomsRunsAndEveryRunBelowThemTheDeepestFirst() {
		AgentRunService.RoomTree tree = AgentRunService.roomTree("r1", "alice", null);

		assertEquals(List.of("grandchild", "continuation", "child", "person", "run1", "run2"), runIds(tree));
		assertEquals(List.of("r1", "r1-sub", "r1-sub-sub"), List.copyOf(tree.rooms().keySet()));
		assertEquals("alice", tree.rooms().get("r1-sub-sub"));
	}

	@Test
	void anotherUsersRoomOfTheSameIdIsTheirsAlone() {
		AgentRunService.RoomTree tree = AgentRunService.roomTree("r1", "bob", null);

		assertEquals(List.of("bob-run"), runIds(tree));
		assertEquals(Map.of("r1", "bob"), tree.rooms());
	}

	@Test
	void aRunUnderTheOwnersUnderAnotherLoginIsNotCovered() throws Exception {
		db.execute("INSERT INTO AGENT_RUN (RUN_ID, PARENT_RUN_ID, ROOM_ID, STATUS, USER_ID, USER_TYPE) "
				+ "VALUES ('other-login', 'run2', 'r1-other', 'RUNNING', 'alice', 'MICROSOFT')");

		assertEquals(false, runIds(AgentRunService.roomTree("r1", "alice", "NATIVE")).contains("other-login"));
		// with no login type known, the id alone matches
		assertEquals(true, runIds(AgentRunService.roomTree("r1", "alice", null)).contains("other-login"));
	}

	@Test
	void aRoomTheUserHasNoRunsInCoversNoRuns() {
		assertEquals(List.of(), AgentRunService.roomTree("r9", "alice", null).runs());
	}

	@Test
	void onlyASignedInUserClosesTheirOwnRoom() {
		Insight insight = mock(Insight.class);
		assertThrows(SecurityException.class, () -> AgentRunService.get().closeRoom("r1", insight));
	}

	@Test
	void onlyAnAdminClosesAnotherUsersRoom() throws Exception {
		User user = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class); var audits = mockStatic(UserAuditTrailUtils.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);

			SecurityException refused = assertThrows(SecurityException.class,
					() -> AgentRunService.get().closeRoomForAdmin(user, "r1", "alice", null));

			assertEquals("This functionality is limited to only admins", refused.getMessage());
			assertEquals("RUNNING", status("run2"));
			audits.verifyNoInteractions();
		}
	}

	@Test
	void anAdminsCloseIsRecordedWithWhatItClosed() throws Exception {
		// the query struct names the table and columns as created above, with no OWL
		when(db.engine.isBasic()).thenReturn(true);
		db.execute("INSERT INTO ROOM VALUES ('r5', 'carol', 'Finished', TRUE)");
		run("carol-run", null, "r5", "COMPLETED", "09:00", "carol");
		User admin = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class);
				var audits = mockStatic(UserAuditTrailUtils.class);
				var messages = mockStatic(RoomMessageStore.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(admin)).thenReturn(true);

			AgentRunService.RoomClosure closure = AgentRunService.get().closeRoomForAdmin(admin, "r5", "carol", null);

			assertEquals(new AgentRunService.RoomClosure("agent-a", List.of("r5"), List.of()), closure);
			audits.verify(() -> UserAuditTrailUtils.recordAgentRoomClose(admin, "agent-a", "r5", "carol", null,
					List.of("r5"), List.of()));
		}
	}

	@Test
	void aCloseThatFindsNoRunsIsNotRecorded() {
		User admin = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class); var audits = mockStatic(UserAuditTrailUtils.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(admin)).thenReturn(true);

			assertThrows(IllegalArgumentException.class,
					() -> AgentRunService.get().closeRoomForAdmin(admin, "r9", "alice", null));
			audits.verifyNoInteractions();
		}
	}
}
