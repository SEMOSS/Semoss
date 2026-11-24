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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.collaboration.CollaborationUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.pipeline.AgentRunOperation;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.AgentUseAccess;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/** Workspace authorization precedes model access, using the real grants. */
class AgentRunnerAccessUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String ROOM_ID = "access-room";
	private static final String AGENT = "access-agent";
	private static final String PARENT_AGENT = "access-parent";
	private static final String MIDDLE_AGENT = "access-middle";
	private static final String CODE_PROJECT = "access-code";
	private static final String MODEL = "access-model";
	private static final String OWN_MODEL = "access-own-model";
	private static final String DATABASE = "access-database";
	private static final String TEAM = "access-team";

	private User owner;
	private User person;
	private Insight insight;
	private Room room;
	private Map<String, Object> options;
	private MockedStatic<RoomUtils> rooms;
	private MockedStatic<ModelInferenceLogsUtils> inferenceLogs;
	private MockedStatic<Utility> utility;

	@BeforeEach
	void setup() throws Exception {
		owner = UnitTestSecurityAuthUtils.createUser("accessowner", false);
		person = UnitTestSecurityAuthUtils.createUser("accessperson", false);
		UnitTestSecurityAuthUtils.createProjectOfType(AGENT, "agent", "WORKSPACE", owner);
		UnitTestSecurityAuthUtils.createProjectOfType(PARENT_AGENT, "parent", "WORKSPACE", person);
		SecurityEngineUtils.addEngine(MODEL, "model", IEngine.CATALOG_TYPE.MODEL, null, null, false, owner);
		SecurityEngineUtils.addEngineOwner(owner, MODEL);
		SecurityEngineUtils.addEngine(OWN_MODEL, "own model", IEngine.CATALOG_TYPE.MODEL, null, null, false, person);
		SecurityEngineUtils.addEngineOwner(person, OWN_MODEL);

		insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(person);
		options = new HashMap<>();
		room = mock(Room.class);
		when(room.getId()).thenReturn(ROOM_ID);
		when(room.getOptionsMap()).thenReturn(options);
		rooms = Mockito.mockStatic(RoomUtils.class);
		rooms.when(() -> RoomUtils.getOrLoadRoom(ROOM_ID, insight)).thenReturn(room);
		inferenceLogs = Mockito.mockStatic(ModelInferenceLogsUtils.class);
		inferenceLogs.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(ROOM_ID)).thenReturn(true);
		utility = Mockito.mockStatic(Utility.class, invocation -> {
			if ("getModel".equals(invocation.getMethod().getName())) {
				return null;
			}
			return invocation.callRealMethod();
		});
		ThreadStore.setUser(person);
	}

	@AfterEach
	void cleanup() throws Exception {
		ThreadStore.remove();
		utility.close();
		inferenceLogs.close();
		rooms.close();
		UnitTestSecurityAuthUtils.clearSecurityDB(SystemEngineRegistry.getSecurityDb(), new ArrayList<>());
	}

	@Test
	void inaccessibleWorkspaceIsReportedBeforeAnInaccessibleModel() {
		assertWorkspaceDenied(AGENT, MODEL);
	}

	@Test
	void inaccessibleWorkspaceIsReportedEvenWhenThePersonCanUseTheModel() {
		assertWorkspaceDenied(AGENT, OWN_MODEL);
	}

	@Test
	void inaccessibleWorkspaceInheritedFromTheRoomIsReportedFirst() {
		options.put("workspace", Map.of("workspace_id", AGENT));
		assertWorkspaceDenied(null, MODEL);
	}

	@Test
	void explicitWorkspaceOverridesAnAccessibleRoomWorkspace() {
		options.put("workspace", PARENT_AGENT);
		assertWorkspaceDenied(AGENT, MODEL);
	}

	@Test
	void missingWorkspaceProducesTheSameAccessError() {
		assertWorkspaceDenied("missing-agent", MODEL);
	}

	@Test
	void inaccessibleWorkspaceDoesNotCreateANewRoom() {
		inferenceLogs.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(ROOM_ID)).thenReturn(false);
		assertWorkspaceDenied(AGENT, MODEL);
		rooms.verifyNoInteractions();
	}

	@Test
	void aRunWithoutAWorkspaceStillChecksThePersonsModelAccess() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(null, MODEL));
		assertEquals("You do not have access to model " + MODEL, error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
	}

	@Test
	void teamAccessToTheWorkspaceAllowsItsDelegatedModel() throws Exception {
		User admin = UnitTestSecurityAuthUtils.createUser("accessadmin", true);
		AdminSecurityGroupUtils groups = AdminSecurityGroupUtils.getInstance(admin);
		groups.addGroup(admin, TEAM, "CUSTOM", "agent users");
		var login = person.getPrimaryLoginToken();
		groups.addUserToGroup(admin, TEAM, login.getId(), login.getProvider().getLabel(), null);
		SecurityGroupProjectUtils.addProjectGroupPermission(owner, TEAM, "CUSTOM", AGENT, "READ_ONLY", null);
		assertDelegatedModelAuthorized();
	}

	@Test
	void aSubAgentCanUseItsModelThroughAnAuthorizedParent() throws Exception {
		SecurityProjectUtils.setProjectGranteePermission(owner, PARENT_AGENT, "PROJECT", AGENT, "READ_ONLY", null);
		options.put(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(PARENT_AGENT));
		assertDelegatedModelAuthorized();
	}

	@Test
	void aSubAgentWithoutItsParentsGrantIsRefusedBeforeTheModel() {
		options.put(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(PARENT_AGENT));
		assertWorkspaceDenied(AGENT, MODEL);
	}

	@Test
	void aNewRoomIsCreatedWithTheAuthorizedModel() {
		inferenceLogs.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(ROOM_ID)).thenReturn(false);
		IModelEngine model = mock(IModelEngine.class);
		utility.when(() -> Utility.getModel(OWN_MODEL)).thenReturn(model);
		rooms.when(() -> RoomUtils.createRoomIfNotExists(ROOM_ID, insight, model, "hello"))
				.thenThrow(new IllegalArgumentException("room creation reached"));
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> run(PARENT_AGENT, OWN_MODEL));
		assertEquals("room creation reached", error.getMessage());
		rooms.verify(() -> RoomUtils.createRoomIfNotExists(ROOM_ID, insight, model, "hello"));
		assertNull(AgentRunOperation.runningAgent());
	}

	@Test
	void aDirectGrantOnTheWorkspaceAllowsItsDelegatedModel() throws Exception {
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(owner, AGENT, personId(), "READ_ONLY");
		assertDelegatedModelAuthorized();
	}

	@Test
	void aPublicWorkspaceAllowsItsDelegatedModel() throws Exception {
		SecurityProjectUtils.setProjectGlobal(owner, AGENT, true);
		assertDelegatedModelAuthorized();
	}

	@Test
	void aProjectThatIsNotAnAgentIsRefusedAsTheWorkspace() {
		UnitTestSecurityAuthUtils.createProjectOfType(CODE_PROJECT, "code", "CODE", person);
		assertWorkspaceDenied(CODE_PROJECT, OWN_MODEL);
	}

	@Test
	void aWorkspaceThePersonOwnsRunsOnTheirOwnModel() {
		assertModelReached(PARENT_AGENT, OWN_MODEL, OWN_MODEL);
	}

	@Test
	void aWorkspaceThePersonCanUseDoesNotLendAModelItWasNotGiven() {
		assertModelDenied(PARENT_AGENT, MODEL, MODEL);
	}

	@Test
	void aBlankWorkspaceParameterRunsWithoutAnAgent() {
		assertModelDenied("  ", MODEL, MODEL);
	}

	@Test
	void theCamelCaseWorkspaceParameterIsCheckedBeforeTheModel() {
		Map<String, Object> params = new HashMap<>();
		params.put("workspaceId", AGENT);
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> runWithParams(params, MODEL));
		assertEquals("You do not have access to workspace " + AGENT, error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	@Test
	void aRoomWorkspaceStoredAsAnIdAllowsItsDelegatedModel() throws Exception {
		SecurityProjectUtils.setProjectGlobal(owner, AGENT, true);
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT, "PROJECT", MODEL, "READ_ONLY", null);
		options.put("workspace", AGENT);
		assertModelReached(null, MODEL, MODEL);
	}

	@Test
	void aRunWithoutAUserIsRefusedTheWorkspace() {
		when(insight.getUser()).thenReturn(null);
		assertWorkspaceDenied(PARENT_AGENT, OWN_MODEL);
	}

	@Test
	void aRunWithoutAUserOrWorkspaceIsRefusedTheModel() {
		when(insight.getUser()).thenReturn(null);
		assertModelDenied(null, OWN_MODEL, OWN_MODEL);
	}

	@Test
	void aCollaborationChatWithoutAnAgentRunsTheCollaborationAgent() throws Exception {
		createCollaborationAgent(true);
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		SecurityEngineUtils.setEngineGranteePermission(owner, Constants.AGENT_COLLABORATION, "PROJECT", MODEL,
				"READ_ONLY", null);
		assertModelReached(null, MODEL, MODEL);
	}

	@Test
	void aCollaborationChatIsRefusedWhenThePersonCannotUseTheCollaborationAgent() {
		createCollaborationAgent(false);
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		assertWorkspaceDenied(null, OWN_MODEL, Constants.AGENT_COLLABORATION);
	}

	@Test
	void aRoutineRoomWithoutAnAgentRunsTheCollaborationAgent() throws Exception {
		createCollaborationAgent(true);
		options.put(CollaborationUtils.ROOM_OPTION_ROUTINE, true);
		SecurityEngineUtils.setEngineGranteePermission(owner, Constants.AGENT_COLLABORATION, "PROJECT", MODEL,
				"READ_ONLY", null);
		assertModelReached(null, MODEL, MODEL);
	}

	@Test
	void anAgentNamedForACollaborationChatReplacesTheCollaborationAgent() {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		assertModelReached(PARENT_AGENT, OWN_MODEL, OWN_MODEL);
	}

	@Test
	void theRoomsAgentInACollaborationChatReplacesTheCollaborationAgent() {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		options.put("workspace", PARENT_AGENT);
		assertModelReached(null, OWN_MODEL, OWN_MODEL);
	}

	@Test
	void aCollaborationDelegationRoomRunsOnThePersonsOwnAccess() {
		when(room.getProjectId()).thenReturn(CollaborationUtils.COLLABORATION_PROJECT_ID);
		options.put(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "access-action");
		assertModelDenied(null, MODEL, MODEL);
	}

	@Test
	void aSubAgentTwoLevelsDownIsAllowedWhenEveryLinkWasGiven() throws Exception {
		UnitTestSecurityAuthUtils.createProjectOfType(MIDDLE_AGENT, "middle", "WORKSPACE", owner);
		SecurityProjectUtils.setProjectGranteePermission(owner, PARENT_AGENT, "PROJECT", MIDDLE_AGENT, "READ_ONLY",
				null);
		SecurityProjectUtils.setProjectGranteePermission(owner, MIDDLE_AGENT, "PROJECT", AGENT, "READ_ONLY", null);
		options.put(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(PARENT_AGENT, MIDDLE_AGENT));
		assertDelegatedModelAuthorized();
	}

	@Test
	void aSubAgentTwoLevelsDownIsRefusedWhenTheMiddleLinkWasNotGiven() throws Exception {
		UnitTestSecurityAuthUtils.createProjectOfType(MIDDLE_AGENT, "middle", "WORKSPACE", owner);
		SecurityProjectUtils.setProjectGranteePermission(owner, PARENT_AGENT, "PROJECT", MIDDLE_AGENT, "READ_ONLY",
				null);
		options.put(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(PARENT_AGENT, MIDDLE_AGENT));
		assertWorkspaceDenied(AGENT, MODEL);
	}

	@Test
	void aSubAgentDoesNotUseAModelOnlyItsParentWasGiven() throws Exception {
		SecurityProjectUtils.setProjectGranteePermission(owner, PARENT_AGENT, "PROJECT", AGENT, "READ_ONLY", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, PARENT_AGENT, "PROJECT", MODEL, "READ_ONLY", null);
		options.put(AgentUseAccess.ROOM_OPTION_AGENT_CHAIN, List.of(PARENT_AGENT));
		assertModelDenied(AGENT, MODEL, MODEL);
	}

	@Test
	void theRoomsBoundModelIsChecked() {
		when(room.getModelId()).thenReturn(MODEL);
		assertModelDenied(null, null, MODEL);
	}

	@Test
	void aModelStoredInTheRoomOptionsIsChecked() {
		options.put("engine", MODEL);
		assertModelDenied(null, null, MODEL);
	}

	@Test
	void theRuntimeModelReplacesTheRoomsBoundModel() {
		when(room.getModelId()).thenReturn(OWN_MODEL);
		assertModelDenied(null, MODEL, MODEL);
	}

	@Test
	void theRoomsBoundModelIsAllowedThroughTheWorkspacesGrant() throws Exception {
		SecurityProjectUtils.setProjectGlobal(owner, AGENT, true);
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT, "PROJECT", MODEL, "READ_ONLY", null);
		when(room.getModelId()).thenReturn(MODEL);
		assertModelReached(AGENT, null, MODEL);
	}

	@Test
	void theWorkspacesDefaultModelIsAllowedThroughItsGrant() throws Exception {
		SecurityProjectUtils.setProjectGlobal(owner, AGENT, true);
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT, "PROJECT", MODEL, "READ_ONLY", null);
		inferenceLogs.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(AGENT))
				.thenReturn(new JSONObject().put("model_id", MODEL));
		assertModelReached(AGENT, null, MODEL);
	}

	@Test
	void theWorkspacesDefaultModelIsRefusedWithoutAGrant() {
		inferenceLogs.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(PARENT_AGENT))
				.thenReturn(new JSONObject().put("model_id", MODEL));
		assertModelDenied(PARENT_AGENT, null, MODEL);
	}

	@Test
	void aRunWithNoModelAnywhereIsRefusedAfterTheWorkspaceCheck() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(PARENT_AGENT, null));
		assertTrue(error.getMessage().startsWith("No model engine found for room '" + ROOM_ID + "'"),
				error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	@Test
	void anEngineThatIsNotAModelIsRefused() {
		UnitTestSecurityAuthUtils.createEngine(DATABASE, "database", person);
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> run(PARENT_AGENT, DATABASE));
		assertEquals("Engine " + DATABASE + " is not a model", error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	@Test
	void aModelThatDoesNotGenerateTextIsRefused() {
		SecurityModelMetadataUtils.upsertModelMetadata(OWN_MODEL, Map.of(Constants.MODEL_CAPABILITY, "EMBEDDING"));
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> run(PARENT_AGENT, OWN_MODEL));
		assertEquals("Model " + OWN_MODEL + " does not generate text, so an agent cannot run on it",
				error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	@Test
	void aTextGenerationModelPassesTheCapabilityCheck() {
		SecurityModelMetadataUtils.upsertModelMetadata(OWN_MODEL,
				Map.of(Constants.MODEL_CAPABILITY, "TEXT_GENERATION"));
		assertModelReached(PARENT_AGENT, OWN_MODEL, OWN_MODEL);
	}

	@Test
	void anExistingRoomIsReusedWithTheAuthorizedModel() {
		IModelEngine model = mock(IModelEngine.class);
		utility.when(() -> Utility.getModel(OWN_MODEL)).thenReturn(model);
		doThrow(new IllegalArgumentException("existing room reached")).when(room).setModelId(OWN_MODEL);
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> run(PARENT_AGENT, OWN_MODEL));
		assertEquals("existing room reached", error.getMessage());
		rooms.verify(() -> RoomUtils.createRoomIfNotExists(ROOM_ID, insight, model, "hello"), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	@Test
	void aNewRoomIsCreatedWithTheWorkspacesDelegatedModel() throws Exception {
		SecurityProjectUtils.setProjectGlobal(owner, AGENT, true);
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT, "PROJECT", MODEL, "READ_ONLY", null);
		inferenceLogs.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(ROOM_ID)).thenReturn(false);
		IModelEngine model = mock(IModelEngine.class);
		utility.when(() -> Utility.getModel(MODEL)).thenReturn(model);
		rooms.when(() -> RoomUtils.createRoomIfNotExists(ROOM_ID, insight, model, "hello"))
				.thenThrow(new IllegalArgumentException("room creation reached"));
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(AGENT, MODEL));
		assertEquals("room creation reached", error.getMessage());
		rooms.verify(() -> RoomUtils.getOrLoadRoom(anyString(), Mockito.any()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	private void assertWorkspaceDenied(String workspaceId, String modelId) {
		assertWorkspaceDenied(workspaceId, modelId, workspaceId == null ? AGENT : workspaceId);
	}

	private void assertWorkspaceDenied(String workspaceId, String modelId, String deniedWorkspaceId) {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(workspaceId, modelId));
		assertEquals("You do not have access to workspace " + deniedWorkspaceId, error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	private void assertModelDenied(String workspaceId, String modelId, String deniedModelId) {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(workspaceId, modelId));
		assertEquals("You do not have access to model " + deniedModelId, error.getMessage());
		utility.verify(() -> Utility.getModel(anyString()), never());
		assertNull(AgentRunOperation.runningAgent());
	}

	private void assertModelReached(String workspaceId, String modelId, String reachedModelId) {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> run(workspaceId, modelId));
		assertEquals("Could not load model engine '" + reachedModelId + "' for room '" + ROOM_ID + "'",
				error.getMessage());
		utility.verify(() -> Utility.getModel(reachedModelId));
		assertNull(AgentRunOperation.runningAgent());
	}

	private void assertDelegatedModelAuthorized() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT, "PROJECT", MODEL, "READ_ONLY", null);
		// A missing model instance stops the run after both access checks passed.
		assertModelReached(AGENT, MODEL, MODEL);
	}

	private void createCollaborationAgent(boolean global) {
		SecurityProjectUtils.addProject(Constants.AGENT_COLLABORATION, "Collaboration Agent", "WORKSPACE", null, global,
				owner);
		SecurityProjectUtils.addProjectOwner(owner, Constants.AGENT_COLLABORATION);
	}

	private String personId() {
		return person.getPrimaryLoginToken().getId();
	}

	private void run(String workspaceId, String modelId) throws Exception {
		Map<String, Object> params = new HashMap<>();
		if (workspaceId != null) {
			params.put(AgentRunner.PARAM_WORKSPACE_ID, workspaceId);
		}
		runWithParams(params, modelId);
	}

	private void runWithParams(Map<String, Object> params, String modelId) throws Exception {
		AgentRunner.run(ROOM_ID, "hello", modelId, "semoss", 1, 0, params, Map.of(), "access-run", insight);
	}
}
