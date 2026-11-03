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
package prerna.reactor.agent.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.agent.run.AdminAccess;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.util.JdbcTestDatabase;

/**
 * The admin Agent Runs reads: every user's rooms with agent runs, the runs in a
 * room, and a run's subagents, as their owners see them, for admins only.
 */
class AdminAgentRunReadServiceUnitTests {

	private static final Set<AgentRunStatus> ANY_STATUS = EnumSet.noneOf(AgentRunStatus.class);

	static AdminAccess access;
	static AdminAgentRunReadService reads;

	JdbcTestDatabase db;

	@BeforeEach
	void setup() throws Exception {
		User admin = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(admin)).thenReturn(true);
			access = AdminAccess.of(admin);
			reads = AdminAgentRunReadService.getInstance(admin);
		}
		db = new JdbcTestDatabase();
		// the catalog reads name the tables and columns as created below, with no OWL
		when(db.engine.isBasic()).thenReturn(true);
		when(db.engine.getPreparedStatement(anyString()))
				.thenAnswer(invocation -> db.connection.prepareStatement(invocation.getArgument(0, String.class)));
		db.execute("CREATE TABLE AGENT_RUN (RUN_ID VARCHAR, PARENT_RUN_ID VARCHAR, ROOM_ID VARCHAR, "
				+ "WORKSPACE_ID VARCHAR, MODEL_ID VARCHAR, HARNESS_TYPE VARCHAR, JOB_ID VARCHAR, STATUS VARCHAR, "
				+ "INPUT CLOB, REQUEST_JSON CLOB, INPUT_MESSAGE_ID VARCHAR, FINAL_OUTPUT CLOB, "
				+ "FINAL_OUTPUT_MESSAGE_ID VARCHAR, ERROR_MESSAGE CLOB, PROGRESS_JSON CLOB, DATE_CREATED TIMESTAMP, "
				+ "STARTED_AT TIMESTAMP, COMPLETED_AT TIMESTAMP, USER_ID VARCHAR, USER_TYPE VARCHAR)",
				"CREATE TABLE ROOM (ROOM_ID VARCHAR, USER_ID VARCHAR, ROOM_NAME VARCHAR, IS_ACTIVE BOOLEAN)",
				"CREATE TABLE SMSS_USER (ID VARCHAR, NAME VARCHAR, USERNAME VARCHAR, EMAIL VARCHAR, TYPE VARCHAR)",
				"CREATE TABLE PROJECT (PROJECTID VARCHAR, PROJECTNAME VARCHAR, PROJECTDISPLAYNAME VARCHAR)",
				"CREATE TABLE ENGINE (ENGINEID VARCHAR, ENGINENAME VARCHAR, ENGINEDISPLAYNAME VARCHAR, "
						+ "ENGINETYPE VARCHAR, ENGINESUBTYPE VARCHAR)",
				// alice's id is held by two logins, so her rooms tell them apart by login type
				"INSERT INTO SMSS_USER VALUES ('alice', 'Alice Doe', 'alice', 'alice@example.com', 'NATIVE'), "
						+ "('alice', 'Alice Contoso', 'alice.c', 'alice@contoso.com', 'MICROSOFT'), "
						+ "('bob', NULL, 'bob.smith', 'bob@example.com', 'MICROSOFT')",
				"INSERT INTO PROJECT VALUES ('agent-a', 'agent_a', 'Agent A'), ('agent-b', 'agent_b', NULL)",
				"INSERT INTO ENGINE VALUES ('model-1', 'model_1', 'GPT Model', 'MODEL', 'OPEN_AI')",
				// bob's room is closed; a room whose IS_ACTIVE is not set is open
				"INSERT INTO ROOM VALUES ('r1', 'alice', 'Quarterly report', TRUE), "
						+ "('r2', 'bob', 'Ticket triage', FALSE), ('r3', 'alice', 'Vendor review', NULL)");
		run("run1", null, "r1", "agent-a", "alice", "NATIVE", "COMPLETED", "10:00", "10:00", "10:05");
		// a run that stores no login type belongs to its id under any type
		run("run2", null, "r1", "agent-a", "alice", null, "RUNNING", "11:00", "11:00", null);
		// a subagent's own room holds only its run, so it shows nested in r1
		run("run2-child", "run2", "r1-sub", "agent-b", "alice", "NATIVE", "FAILED", "11:01", "11:01", "11:02");
		run("run3", null, "r2", "agent-b", "bob", null, "FAILED", "09:00", "09:00", "09:01");
		// a run with no room is not a conversation to open
		run("run4", null, null, "agent-b", "bob", "MICROSOFT", "COMPLETED", "12:00", "12:00", "12:01");
		run("run5", null, "r3", "agent-a", "alice", "MICROSOFT", "COMPLETED", "08:00", "08:00", "08:05");
	}

	@AfterEach
	void close() throws Exception {
		db.close();
	}

	private void run(String runId, String parentRunId, String roomId, String agentId, String userId, String userType,
			String status, String created, String started, String completed) throws Exception {
		try (var statement = db.connection.prepareStatement("INSERT INTO AGENT_RUN (RUN_ID, PARENT_RUN_ID, ROOM_ID, "
				+ "WORKSPACE_ID, MODEL_ID, STATUS, DATE_CREATED, STARTED_AT, COMPLETED_AT, USER_ID, USER_TYPE) "
				+ "VALUES (?, ?, ?, ?, 'model-1', ?, ?, ?, ?, ?, ?)")) {
			statement.setString(1, runId);
			statement.setString(2, parentRunId);
			statement.setString(3, roomId);
			statement.setString(4, agentId);
			statement.setString(5, status);
			statement.setObject(6, timestamp(created));
			statement.setObject(7, timestamp(started));
			statement.setObject(8, timestamp(completed));
			statement.setString(9, userId);
			statement.setString(10, userType);
			statement.executeUpdate();
		}
	}

	private static List<Object> roomIds(Map<String, Object> page) {
		return rooms(page).stream().map(room -> room.get("roomId")).toList();
	}

	private static java.sql.Timestamp timestamp(String time) {
		return time == null ? null : java.sql.Timestamp.valueOf("2026-10-01 " + time + ":00");
	}

	private static List<Map<String, Object>> rooms(Map<String, Object> page) {
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> rooms = (List<Map<String, Object>>) page.get("rooms");
		return rooms;
	}

	@Test
	void listsEveryUsersConversationsMostRecentlyActiveFirst() {
		Map<String, Object> page = reads.getRooms(new AgentRunStore.RoomFilter(null, null, null, null, ANY_STATUS), 25,
				0);

		List<Map<String, Object>> rooms = rooms(page);
		assertEquals(List.of("r1", "r2", "r3"), roomIds(page));
		assertEquals(3L, page.get("total"));

		Map<String, Object> alice = rooms.get(0);
		assertEquals("alice", alice.get("userId"));
		// the room takes the login type its runs store, and that login's name
		assertEquals("NATIVE", alice.get("userType"));
		assertEquals("Alice Doe", alice.get("userName"));
		assertEquals("alice@example.com", alice.get("userEmail"));
		assertEquals("agent-a", alice.get("agentId"));
		assertEquals("Agent A", alice.get("agentName"));
		assertEquals("Quarterly report", alice.get("roomName"));
		assertEquals(2L, alice.get("runCount"));
		assertEquals(1L, alice.get("activeRunCount"));
		assertEquals(0L, alice.get("failedRunCount"));
		assertEquals(false, alice.get("isClosed"));

		Map<String, Object> bob = rooms.get(1);
		// a room whose runs store no login type takes its id's login when the id has
		// just one; a user with no name shows their username, and an agent with no
		// display name its name
		assertEquals("MICROSOFT", bob.get("userType"));
		assertEquals("bob.smith", bob.get("userName"));
		assertEquals("agent_b", bob.get("agentName"));
		assertEquals(1L, bob.get("failedRunCount"));
		assertEquals(true, bob.get("isClosed"));

		// the same id under its other login is that login's user
		Map<String, Object> aliceAtContoso = rooms.get(2);
		assertEquals("alice", aliceAtContoso.get("userId"));
		assertEquals("MICROSOFT", aliceAtContoso.get("userType"));
		assertEquals("Alice Contoso", aliceAtContoso.get("userName"));
		assertEquals("alice@contoso.com", aliceAtContoso.get("userEmail"));
		assertEquals(false, aliceAtContoso.get("isClosed"));
	}

	@Test
	void aUserWhoIsNoLongerThereKeepsTheirStoredLoginType() throws Exception {
		db.execute("DELETE FROM SMSS_USER WHERE ID = 'alice' AND TYPE = 'MICROSOFT'");

		Map<String, Object> aliceAtContoso = rooms(
				reads.getRooms(new AgentRunStore.RoomFilter(null, null, null, null, ANY_STATUS), 25, 0)).get(2);
		// never the id's other login
		assertNull(aliceAtContoso.get("userName"));
		assertNull(aliceAtContoso.get("userEmail"));
		assertEquals("MICROSOFT", aliceAtContoso.get("userType"));
	}

	@Test
	void aRoomWithNoStoredLoginTypeIsShownByIdWhenItsIdHasTwoLogins() throws Exception {
		db.execute(
				"INSERT INTO SMSS_USER VALUES ('dana', 'Dana Native', 'dana', 'dana@example.com', 'NATIVE'), "
						+ "('dana', 'Dana Contoso', 'dana.c', 'dana@contoso.com', 'MICROSOFT')",
				"INSERT INTO ROOM VALUES ('r9', 'dana', 'Planning', TRUE)");
		run("run9", null, "r9", "agent-a", "dana", null, "COMPLETED", "07:00", "07:00", "07:05");

		Map<String, Object> dana = rooms(
				reads.getRooms(new AgentRunStore.RoomFilter(null, "dana", null, null, ANY_STATUS), 25, 0)).get(0);
		assertNull(dana.get("userName"));
		assertNull(dana.get("userType"));
	}

	@Test
	void listsAnAgentsSubagentRoomsOnlyWhenAsked() {
		// agent-b ran in alice's rooms only as a subagent, in its own room
		assertEquals(List.of(), AgentRunStore.getRoomsForAdmin(access,
				new AgentRunStore.RoomFilter("agent-b", "alice", null, null, ANY_STATUS), 25, 0));
		assertEquals(List.of("r1-sub"),
				AgentRunStore
						.getRoomsForAdmin(access,
								new AgentRunStore.RoomFilter("agent-b", "alice", null, null, ANY_STATUS, true), 25, 0)
						.stream().map(room -> room.get("roomId")).toList());
	}

	@Test
	void filtersByUserAgentAndStatusAndPages() {
		assertEquals(List.of("r2"),
				roomIds(reads.getRooms(new AgentRunStore.RoomFilter(null, "bob", null, null, ANY_STATUS), 25, 0)));
		assertEquals(List.of("r2"),
				roomIds(reads.getRooms(new AgentRunStore.RoomFilter("agent-b", null, null, null, ANY_STATUS), 25, 0)));
		Map<String, Object> running = reads.getRooms(
				new AgentRunStore.RoomFilter(null, null, null, null,
						EnumSet.of(AgentRunStatus.SUBMITTED, AgentRunStatus.RUNNING, AgentRunStatus.INPUT_REQUIRED)),
				25, 0);
		assertEquals(List.of("r1"), roomIds(running));
		assertEquals(1L, running.get("total"));

		Map<String, Object> second = reads.getRooms(new AgentRunStore.RoomFilter(null, null, null, null, ANY_STATUS), 1,
				1);
		assertEquals(List.of("r2"), roomIds(second));
		assertEquals(3L, second.get("total"));
	}

	@Test
	void filtersByUserIdAloneOrWithItsLoginType() {
		assertEquals(List.of("r1", "r3"),
				roomIds(reads.getRooms(new AgentRunStore.RoomFilter(null, "alice", null, null, ANY_STATUS), 25, 0)));
		assertEquals(List.of("r1"), roomIds(
				reads.getRooms(new AgentRunStore.RoomFilter(null, "alice", "NATIVE", null, ANY_STATUS), 25, 0)));
		Map<String, Object> contoso = reads
				.getRooms(new AgentRunStore.RoomFilter(null, "alice", "MICROSOFT", null, ANY_STATUS), 25, 0);
		// r1's run with no login type matches alice under either login
		assertEquals(List.of("r1", "r3"), roomIds(contoso));
		assertEquals(2L, contoso.get("total"));
		assertEquals(1L, rooms(contoso).get(0).get("runCount"));
	}

	private static List<Object> searchRooms(String search) {
		return roomIds(reads.getRooms(new AgentRunStore.RoomFilter(null, null, null, search, ANY_STATUS), 25, 0));
	}

	@Test
	void searchesRoomsByNameOrIdInAnyCase() {
		assertEquals(List.of("r1"), searchRooms("REPORT"));
		assertEquals(List.of("r2"), searchRooms("r2"));
		// a wildcard is searched for as itself
		assertEquals(List.of(), searchRooms("%"));
		assertEquals(List.of(), searchRooms("_1"));
	}

	private static List<List<Object>> users(List<Map<String, Object>> users) {
		return users.stream().map(user -> List.of(user.get("userId"), user.get("userType"), user.get("userName")))
				.toList();
	}

	@Test
	void offersTheAgentsOfTheListedRoomsMostRecentlyActiveFirst() {
		List<Map<String, Object>> agents = reads.getAgentOptions(null, 25);
		// agent-b's only other runs are a subagent's and one with no room
		assertEquals(List.of("agent-a", "agent-b"), agents.stream().map(agent -> agent.get("agentId")).toList());
		assertEquals("Agent A", agents.get(0).get("agentName"));

		assertEquals(List.of("agent-a"),
				reads.getAgentOptions("agent a", 25).stream().map(agent -> agent.get("agentId")).toList());
		assertEquals(List.of("agent-b"),
				reads.getAgentOptions("-B", 25).stream().map(agent -> agent.get("agentId")).toList());
	}

	@Test
	void offersTheUsersOfTheListedRoomsByIdAndLoginType() {
		// alice's run with no login type is offered under her logins, not on its own;
		// bob's runs store none, so he shows as the one login his id has
		assertEquals(List.of(List.of("alice", "NATIVE", "Alice Doe"), List.of("bob", "MICROSOFT", "bob.smith"),
				List.of("alice", "MICROSOFT", "Alice Contoso")), users(reads.getUserOptions(null, 25)));

		// by name or email, matched to that login only
		assertEquals(List.of(List.of("alice", "MICROSOFT", "Alice Contoso")),
				users(reads.getUserOptions("contoso", 25)));
		assertEquals(List.of(List.of("bob", "MICROSOFT", "bob.smith")), users(reads.getUserOptions("BOB", 25)));
	}

	@Test
	void readsAnyUsersRoomRunsAndSubagentsWithTheirModelNames() {
		List<Map<String, Object>> runs = reads.getRunsForRoom("r1", "alice", null);
		assertEquals(List.of("run2", "run1"), runs.stream().map(run -> run.get("runId")).toList());
		assertEquals("GPT Model", runs.get(0).get("modelName"));
		assertNull(runs.get(0).get("userType"));
		assertEquals("NATIVE", runs.get(1).get("userType"));
		// rooms are kept per user, so another user's room of the same id is not theirs
		assertEquals(List.of(), reads.getRunsForRoom("r1", "bob", null));
		// nor are the same id's runs under another login
		assertEquals(List.of(), reads.getRunsForRoom("r3", "alice", "NATIVE"));
		assertEquals(List.of("run5"),
				reads.getRunsForRoom("r3", "alice", "MICROSOFT").stream().map(run -> run.get("runId")).toList());

		List<Map<String, Object>> children = reads.getSubagentRuns("run2");
		assertEquals(List.of("run2-child"), children.stream().map(run -> run.get("runId")).toList());
		assertEquals("AGENT", children.get(0).get("executorType"));
		assertEquals("GPT Model", children.get(0).get("modelName"));
	}

	@Test
	void readsAnyUsersRunDetails() {
		Map<String, Object> run = AgentRunStore.getRunMapForAdmin(access, "run3");
		assertEquals("bob", run.get("userId"));
		assertEquals("FAILED", run.get("status"));
		assertEquals("Ticket triage", run.get("roomName"));
		assertEquals("MICROSOFT", AgentRunStore.getRunMapForAdmin(access, "run5").get("userType"));
		// each run says whether its room is closed: bob's is, and one with no value set
		// is not
		assertEquals(true, run.get("isRoomClosed"));
		assertEquals(false, AgentRunStore.getRunMapForAdmin(access, "run5").get("isRoomClosed"));
		assertEquals(false, AgentRunStore.getRunMapForAdmin(access, "run1").get("isRoomClosed"));
		assertNull(AgentRunStore.getRunMapForAdmin(access, "missing"));
	}

	@Test
	void onlyAnAdminGetsTheReads() {
		User user = mock(User.class);
		User admin = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);
			admins.when(() -> SecurityAdminUtils.userIsAdmin(admin)).thenReturn(true);

			SecurityException refused = assertThrows(SecurityException.class,
					() -> AdminAgentRunReadService.getInstance(user));
			assertEquals("This functionality is limited to only admins", refused.getMessage());
			assertThrows(SecurityException.class, () -> AdminAgentRunReadService.getInstance(null));
			assertEquals(List.of("r1", "r2", "r3"), roomIds(AdminAgentRunReadService.getInstance(admin)
					.getRooms(new AgentRunStore.RoomFilter(null, null, null, null, ANY_STATUS), 25, 0)));
		}
	}
}
