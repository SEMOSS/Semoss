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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.access.PermissionSubject;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.pipeline.AgentRunOperation;
import prerna.reactor.agent.AgentUseAccess;
import prerna.util.SystemEngineRegistry;

/**
 * Who may use an agent: a person who can view it, or, in a sub-agent's room, a
 * person who can view an agent above it when each agent on the way was given
 * the one below it, read only from the agents the room records; and the agent
 * run that begins only for such a person.
 */
class AgentUseAccessUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String TOP_AGENT = "topagent";
	private static final String MIDDLE_AGENT = "middleagent";
	private static final String SUB_AGENT = "subagent";
	private static final String APP_ID = "notanagent";
	private static final String APP = PermissionSubject.PROJECT_TYPE;

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User person;
	private User builder;

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		person = UnitTestSecurityAuthUtils.createUser("person", false);
		builder = UnitTestSecurityAuthUtils.createUser("builder", false);

		UnitTestSecurityAuthUtils.createProjectOfType(TOP_AGENT, "top agent", "WORKSPACE", person);
		UnitTestSecurityAuthUtils.createProjectOfType(MIDDLE_AGENT, "middle agent", "WORKSPACE", builder);
		UnitTestSecurityAuthUtils.createProjectOfType(SUB_AGENT, "sub agent", "WORKSPACE", builder);
		UnitTestSecurityAuthUtils.createProject(APP_ID, "an app", person);
		SecurityProjectUtils.setProjectGranteePermission(builder, TOP_AGENT, APP, MIDDLE_AGENT, "READ_ONLY", null);
		SecurityProjectUtils.setProjectGranteePermission(builder, MIDDLE_AGENT, APP, SUB_AGENT, "READ_ONLY", null);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void aPersonUsesAnAgentTheyCanView() {
		assertTrue(AgentUseAccess.mayUseAgent(person, null, TOP_AGENT));
		assertFalse(AgentUseAccess.mayUseAgent(person, null, SUB_AGENT));
		assertFalse(AgentUseAccess.mayUseAgent(null, null, TOP_AGENT));
	}

	@Test
	void aSubAgentIsUsedThroughTheAgentsItsRoomRecords() {
		assertTrue(AgentUseAccess.mayUseAgent(person, roomRecording(TOP_AGENT), MIDDLE_AGENT));
		assertTrue(AgentUseAccess.mayUseAgent(person, roomRecording(TOP_AGENT, MIDDLE_AGENT), SUB_AGENT));
	}

	@Test
	void eachAgentOnTheWayMustHaveBeenGivenTheOneBelowIt() {
		assertFalse(AgentUseAccess.mayUseAgent(person, roomRecording(TOP_AGENT), SUB_AGENT));
		assertFalse(AgentUseAccess.mayUseAgent(person, roomRecording(MIDDLE_AGENT), SUB_AGENT));
		assertFalse(AgentUseAccess.mayUseAgent(person, roomRecording(APP_ID, MIDDLE_AGENT), SUB_AGENT));
		assertFalse(AgentUseAccess.mayUseAgent(person, roomRecording(TOP_AGENT, " "), SUB_AGENT));
	}

	@Test
	void aRoomThatRecordsTooManyAgentsGivesNothing() {
		Room tooMany = roomRecording("a1", "a2", "a3", "a4", "a5", "a6", "a7", TOP_AGENT, MIDDLE_AGENT);

		assertEquals(List.of(), AgentUseAccess.agentChainOf(tooMany));
		assertFalse(AgentUseAccess.mayUseAgent(person, tooMany, SUB_AGENT));
	}

	@Test
	void aSubAgentRecordsItsParentRoomsAgentsAndTheAgentThatSpawnedIt() {
		assertEquals(List.of(TOP_AGENT), AgentUseAccess.subAgentChain(roomRecording(), TOP_AGENT, MIDDLE_AGENT));
		assertEquals(List.of(TOP_AGENT, MIDDLE_AGENT),
				AgentUseAccess.subAgentChain(roomRecording(TOP_AGENT), MIDDLE_AGENT, SUB_AGENT));
		assertEquals(List.of(TOP_AGENT), AgentUseAccess.subAgentChain(roomRecording(TOP_AGENT), MIDDLE_AGENT, null));
		assertEquals(List.of(TOP_AGENT),
				AgentUseAccess.subAgentChain(roomRecording(TOP_AGENT), MIDDLE_AGENT, MIDDLE_AGENT));
		assertEquals(List.of(), AgentUseAccess.subAgentChain(null, null, SUB_AGENT));
	}

	@Test
	void anAgentRunBeginsOnlyForAPersonWhoMayUseTheAgent() {
		assertNull(AgentUseAccess.beginAgentRun(person, null, SUB_AGENT));
		assertNull(AgentUseAccess.beginAgentRun(person, null, APP_ID));
		assertNull(AgentUseAccess.beginAgentRun(null, null, TOP_AGENT));
		assertNull(AgentUseAccess.beginAgentRun(person, null, null));

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, roomRecording(TOP_AGENT, MIDDLE_AGENT),
				SUB_AGENT)) {
			assertNotNull(run);
			assertEquals(SUB_AGENT, AgentRunOperation.runningAgent());
		}
		assertNull(AgentRunOperation.runningAgent());
	}

	/**
	 * A room whose options record the agents it was started under.
	 *
	 * @param agents the agents, the first agent first
	 * @return the room
	 */
	private static Room roomRecording(String... agents) {
		Room room = mock(Room.class);
		when(room.getOptionsMap()).thenReturn(Map.of(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(agents)));
		return room;
	}

}
