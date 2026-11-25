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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.auth.utils.AbstractSecurityUtilsUnitTestsSetup;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.responses.AskStringModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.engine.impl.pipeline.AgentRunOperation;
import prerna.engine.impl.pipeline.EngineCallPermission;
import prerna.engine.impl.pipeline.StubEngines;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.agent.AgentUseAccess;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What an agent's run may use, end to end: the agent's run asks models and
 * reads skills with the permissions engines and projects gave the agent, on top
 * of the person's own access, and only for a person who may use the agent. A
 * sub-agent's run uses the sub-agent's permissions, never its parent's. Each
 * test is one use case of docs/platform_services/engine_access.md for agent
 * runs.
 *
 * <p>
 * The pieces are the real ones wherever the access check reads them: the run
 * begins through {@link AgentUseAccess#beginAgentRun}, as the agent runner and
 * the room namer begin it; the models and the skill are behind the real engine
 * proxy ({@link StubEngines}); and every permission is a row in the junit
 * security database, given and taken away the way the Apps and Agents tables
 * do. Only the models' answers are made up.
 * </p>
 */
class AgentEngineChainUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String AGENT = "chain-agent";
	private static final String SUB_AGENT = "chain-sub-agent";
	private static final String SKILL = "chain-skill";
	private static final String AGENT_MODEL = "chain-agent-model";
	private static final String SUB_AGENT_MODEL = "chain-sub-agent-model";
	private static final String PERSONS_MODEL = "chain-persons-model";
	private static final String PROJECT_TYPE = "PROJECT";
	private static final String PERSON = "agentchainperson";

	private static User owner;
	private static User person;

	private final AtomicReference<EngineGrantee> seenByTheAgentModel = new AtomicReference<>();
	private final AtomicReference<EngineGrantee> seenByTheSubAgentModel = new AtomicReference<>();
	private final AtomicReference<EngineGrantee> seenByThePersonsModel = new AtomicReference<>();
	private final List<String[]> engineGrants = new ArrayList<>();
	private final List<String[]> projectGrants = new ArrayList<>();
	private MockedStatic<ModelUsageCost> prices;
	private IModelEngine agentModel;
	private IModelEngine subAgentModel;
	private IModelEngine personsModel;
	private IProject skill;

	@BeforeAll
	static void twoAgentsASkillAndThreeModelsOwnedBySomeoneElse() throws Exception {
		owner = signedUpUser("agentchainowner");
		person = signedUpUser(PERSON);
		for (String[] project : new String[][] { { AGENT, "WORKSPACE" }, { SUB_AGENT, "WORKSPACE" },
				{ SKILL, "SKILL" } }) {
			SecurityProjectUtils.addProject(project[0], project[0], project[1], null, false, owner);
			SecurityProjectUtils.addProjectOwner(owner, project[0]);
		}
		SecurityProjectUtils.addProjectUserPermissions(owner, AGENT,
				List.of(Map.of("userid", PERSON, "permission", "READ_ONLY")),
				ZonedDateTime.now().plusDays(2).toString());
		for (String model : List.of(AGENT_MODEL, SUB_AGENT_MODEL, PERSONS_MODEL)) {
			SecurityEngineUtils.addEngine(model, model, IEngine.CATALOG_TYPE.MODEL, null, null, false, owner);
			SecurityEngineUtils.addEngineOwner(owner, model);
		}
		SecurityEngineUtils.addEngineUserPermissions(owner, PERSONS_MODEL,
				List.of(Map.<String, Object>of("userid", PERSON, "permission", "READ_ONLY")));
	}

	@BeforeEach
	void signInThePerson() {
		prices = mockStatic(ModelUsageCost.class);
		prices.when(() -> ModelUsageCost.hasKnownPrices(anyString())).thenReturn(false);
		prices.when(() -> ModelUsageCost.priceEntry(anyString())).thenReturn(null);
		agentModel = model(AGENT_MODEL, seenByTheAgentModel);
		subAgentModel = model(SUB_AGENT_MODEL, seenByTheSubAgentModel);
		personsModel = model(PERSONS_MODEL, seenByThePersonsModel);
		skill = StubEngines.project(SKILL);
		ThreadStore.setUser(person);
	}

	@AfterEach
	void takeTheGrantsAway() throws Exception {
		ThreadStore.remove();
		for (String[] grant : engineGrants) {
			SecurityEngineUtils.removeEngineGranteePermission(owner, grant[0], grant[1], grant[2]);
		}
		engineGrants.clear();
		for (String[] grant : projectGrants) {
			SecurityProjectUtils.removeProjectGranteePermission(owner, grant[0], grant[1], grant[2]);
		}
		projectGrants.clear();
		prices.close();
	}

	@Test
	void thePersonsOwnAccessNeedsNoAgentAndNoGrant() {
		assertEquals("answer", ask(personsModel));
		assertNull(seenByThePersonsModel.get());
		assertThrows(SemossPixelException.class, () -> ask(agentModel));
	}

	@Test
	void anAgentsRunAsksAModelTheAgentWasGivenAndNothingElseCanAskIt() {
		give(AGENT_MODEL, AGENT);

		assertThrows(SemossPixelException.class, () -> ask(agentModel));
		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, AGENT)) {
			assertNotNull(run);
			assertEquals("answer", ask(agentModel));
			assertEquals(EngineGrantee.ofApp(AGENT), seenByTheAgentModel.get());
		}
		assertThrows(SemossPixelException.class, () -> ask(agentModel));
	}

	@Test
	void aPersonWhoCannotViewTheAgentGetsNoneOfItsAccess() {
		give(SUB_AGENT_MODEL, SUB_AGENT);

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, SUB_AGENT)) {
			assertNull(run);
			assertThrows(SemossPixelException.class, () -> ask(subAgentModel));
		}
	}

	@Test
	void aReadOnlyGrantLetsTheAgentAskTheModelButNotChangeIt() {
		give(AGENT_MODEL, AGENT);

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, AGENT)) {
			assertEquals("answer", ask(agentModel));
			SemossPixelException refusal = assertThrows(SemossPixelException.class, agentModel::delete);
			assertTrue(refusal.getMessage().contains("agent " + AGENT), refusal.getMessage());
		}
	}

	@Test
	void anAgentsRunReadsASkillTheAgentWasGiven() {
		giveOnProject(SKILL, AGENT);

		assertThrows(SemossPixelException.class, skill::getProjectName);
		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, AGENT)) {
			assertDoesNotThrow(skill::getProjectName);
		}
	}

	@Test
	void aSubAgentsRunUsesTheSubAgentsGrantsAndNeverItsParents() {
		give(AGENT_MODEL, AGENT);
		giveOnProject(SUB_AGENT, AGENT);
		give(SUB_AGENT_MODEL, SUB_AGENT);

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, roomStartedUnder(AGENT), SUB_AGENT)) {
			assertNotNull(run);
			assertEquals("answer", ask(subAgentModel));
			assertEquals(EngineGrantee.ofApp(SUB_AGENT), seenByTheSubAgentModel.get());
			assertThrows(SemossPixelException.class, () -> ask(agentModel));
		}
		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, AGENT)) {
			assertEquals("answer", ask(agentModel));
			assertThrows(SemossPixelException.class, () -> ask(subAgentModel));
		}
	}

	@Test
	void aSubAgentItsParentWasNotGivenGetsNoneOfItsAccess() {
		give(SUB_AGENT_MODEL, SUB_AGENT);

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, roomStartedUnder(AGENT), SUB_AGENT)) {
			assertNull(run);
			assertThrows(SemossPixelException.class, () -> ask(subAgentModel));
		}
	}

	@Test
	void aThreadTheRunStartsAsksAsThePersonOnly() throws Exception {
		give(AGENT_MODEL, AGENT);
		AtomicReference<Throwable> onTheThread = new AtomicReference<>();

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, AGENT)) {
			Thread thread = Thread.ofVirtual().start(() -> {
				ThreadStore.setUser(person);
				try {
					ask(agentModel);
				} catch (Throwable e) {
					onTheThread.set(e);
				} finally {
					ThreadStore.remove();
				}
			});
			thread.join();
			assertEquals("answer", ask(agentModel));
		}
		assertInstanceOf(SemossPixelException.class, onTheThread.get());
	}

	@Test
	void takingTheAgentsPermissionAwayStopsItsNextCall() throws Exception {
		give(AGENT_MODEL, AGENT);

		try (AgentRunOperation run = AgentUseAccess.beginAgentRun(person, null, AGENT)) {
			assertEquals("answer", ask(agentModel));
			takeAway(AGENT_MODEL, AGENT);
			assertThrows(SemossPixelException.class, () -> ask(agentModel));
		}
	}

	/**
	 * Asks a model the way a run does.
	 *
	 * @param model the model, behind the engine proxy
	 * @return its answer
	 */
	private static Object ask(IModelEngine model) {
		return model.ask("When does order 42 ship?", null, null, new HashMap<>()).getResponse();
	}

	/**
	 * A model behind the engine proxy that answers every ask, noting the app, agent
	 * or engine whose permission let the ask through.
	 *
	 * @param id   the model
	 * @param seen where the model notes who made the call
	 * @return the model
	 */
	private static IModelEngine model(String id, AtomicReference<EngineGrantee> seen) {
		return StubEngines.model(id, Map.of("ask", () -> {
			seen.set(EngineCallPermission.granteeMakingTheCall());
			return new AskStringModelEngineResponse("answer", 0, 0);
		}));
	}

	/**
	 * A sub-agent's room that records the agents it was started under, as a spawn
	 * records them.
	 *
	 * @param agents the agents, the first agent first
	 * @return the room
	 */
	private static Room roomStartedUnder(String... agents) {
		Room room = mock(Room.class);
		when(room.getOptionsMap()).thenReturn(Map.of(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(agents)));
		return room;
	}

	/**
	 * Gives an agent Read-Only on a model, as the model's owner does from its Apps
	 * and Agents table, and takes it away after the test.
	 *
	 * @param modelId the model
	 * @param agentId the agent
	 */
	private void give(String modelId, String agentId) {
		try {
			SecurityEngineUtils.setEngineGranteePermission(owner, agentId, PROJECT_TYPE, modelId, "READ_ONLY", null);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		engineGrants.add(new String[] { agentId, PROJECT_TYPE, modelId });
	}

	/**
	 * Takes an agent's permission on a model away during the test.
	 *
	 * @param modelId the model
	 * @param agentId the agent
	 * @throws IllegalAccessException when the owner may not take it away
	 */
	private void takeAway(String modelId, String agentId) throws IllegalAccessException {
		SecurityEngineUtils.removeEngineGranteePermission(owner, agentId, PROJECT_TYPE, modelId);
		engineGrants.removeIf(grant -> grant[0].equals(agentId) && grant[2].equals(modelId));
	}

	/**
	 * Gives an agent Read-Only on a skill or another agent, as the project's owner
	 * does from its Apps and Agents table, and takes it away after the test.
	 *
	 * @param projectId the skill or agent giving the permission
	 * @param agentId   the agent given it
	 */
	private void giveOnProject(String projectId, String agentId) {
		try {
			SecurityProjectUtils.setProjectGranteePermission(owner, agentId, PROJECT_TYPE, projectId, "READ_ONLY",
					null);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		projectGrants.add(new String[] { agentId, PROJECT_TYPE, projectId });
	}

	private static User signedUpUser(String id) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setName(id);
		token.setUsername(id);
		token.setEmail(id + "@test.com");
		token.setProvider(AuthProvider.NATIVE);
		assertTrue(SecurityUpdateUtils.registerUser(id, id, id + "@test.com", "Test123!",
				AuthProvider.NATIVE.getLabel(), "5555555555", "001", "US", false, false, false, null));
		User user = new User();
		user.setPrimaryLogin(AuthProvider.NATIVE);
		user.setAccessToken(token);
		return user;
	}

}
