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
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.util.JdbcTestDatabase;

/**
 * A run records its owner as an id and a login type, and the worker and the
 * owner's own reads match them together. Every read of another user's runs
 * takes the admin pass.
 */
class AgentRunStoreUnitTests {

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
				"CREATE TABLE ROOM (ROOM_ID VARCHAR, USER_ID VARCHAR, ROOM_NAME VARCHAR, IS_ACTIVE BOOLEAN)");
	}

	@AfterEach
	void close() throws Exception {
		db.close();
	}

	private static final Set<AgentRunStatus> ANY_STATUS = EnumSet.noneOf(AgentRunStatus.class);

	private static Insight signedInAs(String userId, AuthProvider provider) {
		AccessToken login = new AccessToken();
		login.setId(userId);
		login.setProvider(provider);
		User user = mock(User.class);
		when(user.getPrimaryLoginToken()).thenReturn(login);
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		return insight;
	}

	private static AgentRunRequest request(String roomId) {
		return new AgentRunRequest(roomId, "Summarize the report", "model-1", "semoss", "agent-a", 5, 1, null, null,
				null);
	}

	@Test
	void aSubmittedRunStoresItsOwnersLoginTypeForTheWorker() throws Exception {
		AgentRunStore.insertSubmitted("run1", request("r1"), "alice", "MICROSOFT");

		assertEquals("alice", db.value("SELECT USER_ID FROM AGENT_RUN WHERE RUN_ID = 'run1'"));
		assertEquals("MICROSOFT", db.value("SELECT USER_TYPE FROM AGENT_RUN WHERE RUN_ID = 'run1'"));
		List<AgentRunRecord> submitted = AgentRunStore.getSubmittedRuns(0, null);
		assertEquals(List.of("run1"), submitted.stream().map(run -> run.runId()).toList());
		assertEquals("alice", submitted.get(0).userId());
		assertEquals("MICROSOFT", submitted.get(0).userType());
	}

	@Test
	void aTaskForAPersonStoresItsOwnersLoginType() throws Exception {
		AgentRunStore.insertInputRequired("run2", request(null), "bob", "NATIVE");

		assertEquals("INPUT_REQUIRED", db.value("SELECT STATUS FROM AGENT_RUN WHERE RUN_ID = 'run2'"));
		assertEquals("bob", db.value("SELECT USER_ID FROM AGENT_RUN WHERE RUN_ID = 'run2'"));
		assertEquals("NATIVE", db.value("SELECT USER_TYPE FROM AGENT_RUN WHERE RUN_ID = 'run2'"));
	}

	@Test
	void theCallersOwnRoomsAndRunsAreReadByTheirLogin() throws Exception {
		AgentRunStore.insertSubmitted("alice-ms", request("r1"), "alice", "MICROSOFT");
		AgentRunStore.insertSubmitted("alice-native", request("r1"), "alice", "NATIVE");
		AgentRunStore.insertSubmitted("bob-run", request("r1"), "bob", "MICROSOFT");
		Insight alice = signedInAs("alice", AuthProvider.MICROSOFT);

		assertEquals(List.of("alice-ms"),
				AgentRunStore.getRunsForRoomByLogin(alice, "r1").stream().map(run -> run.get("runId")).toList());
		List<Map<String, Object>> rooms = AgentRunStore.getRoomsForAgent(alice, "agent-a", null, ANY_STATUS, 25, 0);
		assertEquals(List.of("r1"), rooms.stream().map(room -> room.get("roomId")).toList());
		assertEquals(1L, rooms.get(0).get("runCount"));
		assertEquals(1L, AgentRunStore.countRoomsForAgent(alice, "agent-a", null, ANY_STATUS));
	}

	@Test
	void aSignedOutCallerReadsNoRoomsOrRuns() {
		Insight signedOut = mock(Insight.class);

		assertThrows(SecurityException.class, () -> AgentRunStore.getRunsForRoomByLogin(signedOut, "r1"));
		assertThrows(SecurityException.class,
				() -> AgentRunStore.getRoomsForAgent(signedOut, "agent-a", null, ANY_STATUS, 25, 0));
		assertThrows(SecurityException.class,
				() -> AgentRunStore.countRoomsForAgent(signedOut, "agent-a", null, ANY_STATUS));
	}

	@Test
	void everyReadOfAnotherUsersRunsTakesTheAdminPass() throws Exception {
		AgentRunStore.insertSubmitted("alice-ms", request("r1"), "alice", "MICROSOFT");
		AgentRunStore.RoomFilter everyRoom = new AgentRunStore.RoomFilter(null, null, null, null, ANY_STATUS);

		assertThrows(SecurityException.class, () -> AgentRunStore.getRunMapForAdmin(null, "alice-ms"));
		assertThrows(SecurityException.class, () -> AgentRunStore.getSubagentRunsForAdmin(null, "alice-ms"));
		assertThrows(SecurityException.class, () -> AgentRunStore.getRunsForRoomForAdmin(null, "r1", "alice", null));
		assertThrows(SecurityException.class, () -> AgentRunStore.getRoomsForAdmin(null, everyRoom, 25, 0));
		assertThrows(SecurityException.class, () -> AgentRunStore.countRoomsForAdmin(null, everyRoom));
		assertThrows(SecurityException.class, () -> AgentRunStore.getRoomAgentsForAdmin(null, null, List.of(), 25));
		assertThrows(SecurityException.class, () -> AgentRunStore.getRoomUsersForAdmin(null, null, List.of(), 25));
		assertThrows(SecurityException.class, () -> AgentRunService.get().getRunForAdmin(null, "alice-ms", false));
	}

	@Test
	void aCancelledRunStaysCancelledWhateverItsThreadReports() throws Exception {
		AgentRunStore.insertSubmitted("run1", request("r1"), "alice", "MICROSOFT");
		AgentRunStore.markCancelledIfNotTerminal("run1", "run1", "Agent run cancelled: an admin closed the room");

		assertEquals(false, AgentRunStore.markCompleted("run1", "run1", "Here is the summary."));
		assertEquals(false, AgentRunStore.markIncomplete("run1", "run1", "Partial", "Out of turns"));
		assertEquals(false, AgentRunStore.markFailed("run1", "run1", "Model call failed"));

		assertEquals("CANCELLED", db.value("SELECT STATUS FROM AGENT_RUN WHERE RUN_ID = 'run1'"));
		assertEquals("Agent run cancelled: an admin closed the room", AgentRunStore.getErrorMessage("run1"));
	}

	@Test
	void aRunningRunTakesItsOutcome() throws Exception {
		AgentRunStore.insertSubmitted("run1", request("r1"), "alice", "MICROSOFT");

		assertEquals(true, AgentRunStore.markCompleted("run1", "run1", "Here is the summary."));
		assertEquals("COMPLETED", db.value("SELECT STATUS FROM AGENT_RUN WHERE RUN_ID = 'run1'"));
		assertEquals(null, AgentRunStore.getErrorMessage("missing"));
	}

	@Test
	void aCancelledRunKeepsTheFinalTextItsThreadStillReached() throws Exception {
		AgentRunStore.insertSubmitted("run1", request("r1"), "alice", "MICROSOFT");
		AgentRunStore.insertSubmitted("run2", request("r1"), "alice", "MICROSOFT");
		AgentRunStore.markCancelledIfNotTerminal("run1", "run1", "Agent run cancelled: an admin closed the room");

		AgentRunStore.recordFinalOutputAfterCancel("run1", "Here is the summary.");
		AgentRunStore.recordFinalOutputAfterCancel("run2", "Not cancelled");

		assertEquals("CANCELLED", db.value("SELECT STATUS FROM AGENT_RUN WHERE RUN_ID = 'run1'"));
		assertEquals("Agent run cancelled: an admin closed the room", AgentRunStore.getErrorMessage("run1"));
		assertEquals("Here is the summary.", db.value("SELECT FINAL_OUTPUT FROM AGENT_RUN WHERE RUN_ID = 'run1'"));
		assertEquals(null, db.value("SELECT FINAL_OUTPUT FROM AGENT_RUN WHERE RUN_ID = 'run2'"));
	}
}
