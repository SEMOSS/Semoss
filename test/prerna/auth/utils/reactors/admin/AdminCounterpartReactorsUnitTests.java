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
package prerna.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.DeleteWorkspaceReactor;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.skill.DeleteSkillReactor;
import prerna.reactor.database.RdbmsReconnectReactor;
import prerna.reactor.database.ReloadDatabaseReactor;
import prerna.reactor.engine.CloneEngineReactor;
import prerna.reactor.engine.GetEngineUserAccessRequestReactor;
import prerna.reactor.insights.GetInsightUserAccessRequestReactor;
import prerna.reactor.project.GetProjectUserAccessRequestReactor;
import prerna.reactor.utils.BackupDatabaseReactor;
import prerna.reactor.utils.CloseEngineReactor;
import prerna.reactor.utils.DeleteDatabaseReactor;
import prerna.reactor.utils.DeleteEngineReactor;
import prerna.reactor.utils.DeleteProjectReactor;
import prerna.reactor.utils.ExportEngineReactor;
import prerna.reactor.utils.ExportProjectAppReactor;
import prerna.reactor.utils.ExportProjectReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SystemDefaultEngines;

/**
 * The admin reactors that do for an admin, on any engine, project or insight,
 * what the reactor each one is the counterpart of does for an owner or editor.
 * The rule is the section "Admin operations" of
 * docs/platform_services/engine_access.md.
 */
class AdminCounterpartReactorsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	private static final String MISSING = "no-such-id";

	/**
	 * Every admin reactor with the reactor it is the counterpart of and inputs that
	 * name the database, project or insight of the test.
	 *
	 * @return one case for each admin reactor
	 */
	static Stream<Arguments> counterparts() {
		return Stream.of(pair(new AdminDeleteEngineReactor(), new DeleteEngineReactor(), Map.of("engine", ENGINE)),
				pair(new AdminDeleteDatabaseReactor(), new DeleteDatabaseReactor(), Map.of("database", ENGINE)),
				pair(new AdminDeleteProjectReactor(), new DeleteProjectReactor(), Map.of("project", PROJECT)),
				pair(new AdminDeleteWorkspaceReactor(), new DeleteWorkspaceReactor(), Map.of("workspaceId", PROJECT)),
				pair(new AdminDeleteSkillReactor(), new DeleteSkillReactor(), Map.of("skillId", PROJECT)),
				pair(new AdminCloseEngineReactor(), new CloseEngineReactor(), Map.of("engine", ENGINE)),
				pair(new AdminExportEngineReactor(), new ExportEngineReactor(), Map.of("engine", ENGINE)),
				pair(new AdminExportProjectReactor(), new ExportProjectReactor(), Map.of("project", PROJECT)),
				pair(new AdminExportProjectAppReactor(), new ExportProjectAppReactor(), Map.of("project", PROJECT)),
				pair(new AdminBackupDatabaseReactor(), new BackupDatabaseReactor(), Map.of("database", ENGINE)),
				pair(new AdminCloneEngineReactor(), new CloneEngineReactor(), Map.of("engineId", ENGINE)),
				pair(new AdminReloadDatabaseReactor(), new ReloadDatabaseReactor(), Map.of("database", ENGINE)),
				pair(new AdminRdbmsReconnectReactor(), new RdbmsReconnectReactor(), Map.of("database", ENGINE)),
				pair(new AdminGetEngineUserAccessRequestReactor(), new GetEngineUserAccessRequestReactor(),
						Map.of("engine", ENGINE)),
				pair(new AdminGetProjectUserAccessRequestReactor(), new GetProjectUserAccessRequestReactor(),
						Map.of("project", PROJECT)),
				pair(new AdminGetInsightUserAccessRequestReactor(), new GetInsightUserAccessRequestReactor(),
						Map.of("project", PROJECT, "id", INSIGHT)));
	}

	/**
	 * An owner holds the highest permission there is on the resource, and it gives
	 * them nothing in an admin reactor.
	 */
	@ParameterizedTest
	@MethodSource("counterparts")
	void anAdminReactorRefusesAnOwnerWhoIsNotAnAdmin(AbstractReactor adminReactor, AbstractReactor counterpart,
			Map<String, Object> arguments) {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(adminReactor, anOwnerWhoIsNotAnAdmin(), arguments));

		assertEquals(NOT_AN_ADMIN, refusal.getMessage());
	}

	/**
	 * A pixel written for the ordinary reactor runs in admin mode with the same
	 * inputs: the same keys, the same other names for them, and the same ones
	 * required.
	 */
	@ParameterizedTest
	@MethodSource("counterparts")
	void anAdminReactorTakesTheKeysOfItsCounterpart(AbstractReactor adminReactor, AbstractReactor counterpart,
			Map<String, Object> arguments) {
		assertEquals(keysOf(counterpart), keysOf(adminReactor));
	}

	/**
	 * The description is what an MCP tool and the help text show, so it opens with
	 * the words every admin reactor opens with.
	 */
	@ParameterizedTest
	@MethodSource("counterparts")
	void anAdminReactorSaysItIsForAdminsOnly(AbstractReactor adminReactor, AbstractReactor counterpart,
			Map<String, Object> arguments) {
		assertTrue(adminReactor.getReactorDefinition().getDescription().startsWith("Admin Only: "));
	}

	/**
	 * The description names the reactor a user who is not an admin runs instead,
	 * by the name a pixel calls it.
	 */
	@ParameterizedTest
	@MethodSource("counterparts")
	void anAdminReactorNamesItsCounterpart(AbstractReactor adminReactor, AbstractReactor counterpart,
			Map<String, Object> arguments) {
		String counterpartName = counterpart.getClass().getSimpleName().replace("Reactor", "");

		assertTrue(adminReactor.getReactorDefinition().getDescription()
				.contains("the admin counterpart to " + counterpartName + ","));
	}

	/**
	 * A definition that describes a key the reactor does not take fails the OpenAPI
	 * export, so none does.
	 */
	@ParameterizedTest
	@MethodSource("counterparts")
	void anAdminReactorDefinesNoProblems(AbstractReactor adminReactor, AbstractReactor counterpart,
			Map<String, Object> arguments) {
		assertEquals(List.of(), adminReactor.getReactorDefinition().getProblems());
	}

	/**
	 * Reading an engine's access requests touches the security database only, so
	 * the whole reactor runs here for an admin with no permission on the engine.
	 */
	@Test
	void adminGetEngineUserAccessRequestReturnsTheRequestsForAnAdminWhoHoldsNothing() {
		theRequesterAsksForTheEngine();

		NounMetadata result = run(new AdminGetEngineUserAccessRequestReactor(), anAdminWhoHoldsNothing(),
				Map.of("engine", ENGINE));

		assertEquals(List.of(REQUESTER_ID), requestersOf(result));
	}

	/**
	 * Reading a project's access requests touches the security database only, so
	 * the whole reactor runs here for an admin with no permission on the project.
	 */
	@Test
	void adminGetProjectUserAccessRequestReturnsTheRequestsForAnAdminWhoHoldsNothing() {
		theRequesterAsksForTheProject();

		NounMetadata result = run(new AdminGetProjectUserAccessRequestReactor(), anAdminWhoHoldsNothing(),
				Map.of("project", PROJECT));

		assertEquals(List.of(REQUESTER_ID), requestersOf(result));
	}

	/**
	 * Reading an insight's access requests touches the security database only, so
	 * the whole reactor runs here for an admin with no permission on the insight
	 * or its project.
	 */
	@Test
	void adminGetInsightUserAccessRequestReturnsTheRequestsForAnAdminWhoHoldsNothing() {
		theRequesterAsksForTheInsight();

		NounMetadata result = run(new AdminGetInsightUserAccessRequestReactor(), anAdminWhoHoldsNothing(),
				Map.of("project", PROJECT, "id", INSIGHT));

		assertEquals(List.of(REQUESTER_ID), requestersOf(result));
	}

	/**
	 * The engine of the test is not open on this node, so closing it runs the whole
	 * reactor without engine files.
	 */
	@Test
	void adminCloseEngineClosesAnEngineForAnAdminWhoHoldsNothing() {
		NounMetadata result = run(new AdminCloseEngineReactor(), anAdminWhoHoldsNothing(), Map.of("engine", ENGINE));

		assertEquals(true, result.getValue());
	}

	/**
	 * Every engine is checked to exist before any is closed.
	 */
	@Test
	void adminCloseEngineRefusesAnEngineThatDoesNotExist() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminCloseEngineReactor(), anAdminWhoHoldsNothing(), Map.of("engine", MISSING)));

		assertEquals("Engine " + MISSING + " does not exist", refusal.getMessage());
	}

	/**
	 * Every engine is checked to exist before any is deleted.
	 */
	@Test
	void adminDeleteEngineRefusesAnEngineThatDoesNotExist() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteEngineReactor(), anAdminWhoHoldsNothing(), Map.of("engine", MISSING)));

		assertEquals("Engine " + MISSING + " does not exist", refusal.getMessage());
	}

	/**
	 * A project is checked to exist before it is loaded to be deleted.
	 */
	@Test
	void adminDeleteProjectRefusesAProjectThatDoesNotExist() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteProjectReactor(), anAdminWhoHoldsNothing(), Map.of("project", MISSING)));

		assertEquals("Project " + MISSING + " does not exist", refusal.getMessage());
	}

	/**
	 * Every project is checked before any is deleted, so a list with one that does
	 * not exist deletes none of them.
	 */
	@Test
	void adminDeleteProjectDeletesNothingWhenOneOfTheProjectsDoesNotExist() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteProjectReactor(), anAdminWhoHoldsNothing(),
						Map.of("project", List.of(PROJECT, MISSING))));

		assertEquals("Project " + MISSING + " does not exist", refusal.getMessage());
		assertTrue(SecurityProjectUtils.projectExists(PROJECT));
	}

	/**
	 * An engine or project is checked to exist before it is loaded to be deleted,
	 * reloaded or exported.
	 */
	@Test
	void adminDeleteDatabaseReloadAndExportRefuseAnIdThatDoesNotExist() {
		IllegalArgumentException deleteDatabase = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteDatabaseReactor(), anAdminWhoHoldsNothing(), Map.of("database", MISSING)));
		IllegalArgumentException reload = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminReloadDatabaseReactor(), anAdminWhoHoldsNothing(), Map.of("database", MISSING)));
		IllegalArgumentException exportEngine = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminExportEngineReactor(), anAdminWhoHoldsNothing(), Map.of("engine", MISSING)));
		IllegalArgumentException exportProject = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminExportProjectReactor(), anAdminWhoHoldsNothing(), Map.of("project", MISSING)));
		IllegalArgumentException exportApp = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminExportProjectAppReactor(), anAdminWhoHoldsNothing(), Map.of("project", MISSING)));

		assertEquals("Database " + MISSING + " does not exist", deleteDatabase.getMessage());
		assertEquals("Database " + MISSING + " does not exist", reload.getMessage());
		assertEquals("Engine " + MISSING + " does not exist", exportEngine.getMessage());
		assertEquals("Project " + MISSING + " does not exist", exportProject.getMessage());
		assertEquals("Project " + MISSING + " does not exist", exportApp.getMessage());
	}

	/**
	 * The apps the platform seeds are kept for every user, admins included.
	 */
	@Test
	void adminDeleteProjectRefusesAnAppBuiltIntoThePlatform() {
		String builtIn = SystemDefaultEngines.getSystemApps().getFirst();
		SecurityProjectUtils.addProject(builtIn, builtIn, "APP", null, false, this.owner);

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteProjectReactor(), anAdminWhoHoldsNothing(), Map.of("project", builtIn)));

		assertEquals("Project " + builtIn + " is a built-in platform app/MCP/skill/agent and cannot be deleted",
				refusal.getMessage());
	}

	/**
	 * An id the security database does not list is no workspace and no skill.
	 */
	@Test
	void adminDeleteWorkspaceAndSkillRefuseAnIdThatDoesNotExist() {
		IllegalArgumentException noWorkspace = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteWorkspaceReactor(), anAdminWhoHoldsNothing(), Map.of("workspaceId", MISSING)));
		IllegalArgumentException noSkill = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteSkillReactor(), anAdminWhoHoldsNothing(), Map.of("skillId", MISSING)));

		assertEquals("Workspace " + MISSING + " does not exist", noWorkspace.getMessage());
		assertEquals("Skill " + MISSING + " does not exist", noSkill.getMessage());
	}

	/**
	 * The agents and skills the platform seeds are kept for every user, admins
	 * included.
	 */
	@Test
	void adminDeleteWorkspaceAndSkillRefuseTheOnesBuiltIntoThePlatform() {
		String agent = SystemDefaultEngines.getSystemAgents().getFirst();
		String skill = SystemDefaultEngines.getSystemSkills().getFirst();

		IllegalArgumentException builtInAgent = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteWorkspaceReactor(), anAdminWhoHoldsNothing(), Map.of("workspaceId", agent)));
		IllegalArgumentException builtInSkill = assertThrows(IllegalArgumentException.class,
				() -> run(new AdminDeleteSkillReactor(), anAdminWhoHoldsNothing(), Map.of("skillId", skill)));

		assertEquals("Workspace " + agent + " is a built-in system agent and cannot be deleted",
				builtInAgent.getMessage());
		assertEquals("Skill " + skill + " is a built-in platform skill and cannot be deleted",
				builtInSkill.getMessage());
	}

	/**
	 * One case of {@link #counterparts()}, shown in the test report by the name of
	 * the admin reactor.
	 *
	 * @param adminReactor the admin reactor
	 * @param counterpart  the reactor it is the counterpart of
	 * @param arguments    inputs that name the resource of the test
	 * @return the case
	 */
	private static Arguments pair(AbstractReactor adminReactor, AbstractReactor counterpart,
			Map<String, Object> arguments) {
		return Arguments.of(Named.of(adminReactor.getClass().getSimpleName(), adminReactor), counterpart, arguments);
	}

	/**
	 * The keys of a reactor as its definition gives them: each key's name, the
	 * other names it can be passed under, and whether it must be passed.
	 *
	 * @param reactor the reactor
	 * @return one entry for each key, in order
	 */
	private static List<String> keysOf(AbstractReactor reactor) {
		return reactor.getReactorDefinition().getKeys().stream()
				.map(key -> key.getName() + " " + key.getAliases() + " required=" + key.getRequired()).toList();
	}

	/**
	 * The people whose access requests a reactor returned.
	 *
	 * @param result what the reactor returned
	 * @return the user id of each request
	 */
	private static List<Object> requestersOf(NounMetadata result) {
		return rowsOf(result).stream().map(row -> row.get("REQUEST_USERID")).toList();
	}

}
