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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedStatic;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.GetWorkspaceReactor;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.om.Insight;
import prerna.reactor.agent.runtime.PlatformAgentTools;
import prerna.sablecc2.om.NounStore;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Checks that a Digital Twin can only be shared with named people: its mark
 * cannot be set or cleared as metadata, and every way of making a project
 * public, discoverable, a template, a team's or everyone's is refused on it,
 * for its owner and in admin mode, while a plain project is left as it was.
 */
class DigitalTwinUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String PEOPLE_ONLY = SecurityProjectUtils.DIGITAL_TWIN_PEOPLE_ONLY;
	private static final String TWIN_ID = "twinproject";
	private static final String PLAIN_ID = "plainproject";
	private static final String GROUP_ID = "team";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private MockedStatic<MicrosoftGraphUserLookup> directory;

	private User admin;
	private User owner;
	private User editor;
	private User reader;
	private User stranger;

	@BeforeEach
	void setup() throws Exception {
		directory = mockStatic(MicrosoftGraphUserLookup.class);
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		stranger = UnitTestSecurityAuthUtils.createUser("stranger", false);

		for (String projectId : List.of(TWIN_ID, PLAIN_ID)) {
			UnitTestSecurityAuthUtils.createProject(projectId, projectId, owner);
			UnitTestSecurityAuthUtils.addPermissionsToUserForProject(owner, projectId, "editorid", "EDIT");
			UnitTestSecurityAuthUtils.addPermissionsToUserForProject(owner, projectId, "readerid", "READ_ONLY");
		}
		SecurityProjectUtils.markDigitalTwin(owner, TWIN_ID);
		UnitTestSecurityAuthUtils.createGroup(admin, GROUP_ID, CUSTOM);
	}

	@AfterEach
	void cleanup() throws Exception {
		directory.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void theMarkIsNotMetadataAnyoneSetsOrClears() {
		assertTrue(SecurityProjectUtils.isDigitalTwin(TWIN_ID));
		assertFalse(SecurityProjectUtils.isDigitalTwin(PLAIN_ID));

		// what an uploaded project's metadata file, or a metadata reactor, would send
		SecurityProjectUtils.updateProjectMetadata(owner, TWIN_ID,
				Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, List.of(), "tag", List.of("kept")));
		SecurityProjectUtils.updateProjectMetadata(owner, TWIN_ID,
				Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, "false"));
		SecurityProjectUtils.updateProjectMetadata(owner, PLAIN_ID,
				Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, "true"));

		assertTrue(SecurityProjectUtils.isDigitalTwin(TWIN_ID));
		assertFalse(SecurityProjectUtils.isDigitalTwin(PLAIN_ID));
		// the rest of the metadata is still written
		assertEquals(List.of(TWIN_ID), SecurityProjectUtils.getProjectIdsWithMetaValue("tag", "kept"));
		assertEquals(List.of(TWIN_ID),
				SecurityProjectUtils.getProjectIdsWithMetaValue(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, "true"));
	}

	@Test
	void aTwinNamesThePersonItAnswersAbout() {
		// its creator, by id and login type, as the fixture registered them
		assertEquals("ownername", SecurityProjectUtils.getProjectCreatorName(TWIN_ID));
		assertNull(SecurityProjectUtils.getProjectCreatorName("no-such-project"));
	}

	@Test
	void onlyThePersonWhoMadeATwinIsItsCreator() throws Exception {
		assertTrue(SecurityProjectUtils.isProjectCreator(owner, TWIN_ID));
		// someone who can change the twin did not make it, and it is not theirs to add
		// to from a chat
		assertTrue(SecurityProjectUtils.userCanEditProject(editor, TWIN_ID));
		assertFalse(SecurityProjectUtils.isProjectCreator(editor, TWIN_ID));
		assertFalse(SecurityProjectUtils.isProjectCreator(stranger, TWIN_ID));
		assertFalse(SecurityProjectUtils.isProjectCreator(null, TWIN_ID));
		assertFalse(SecurityProjectUtils.isProjectCreator(owner, "no-such-project"));
		// the id and login type Collaboration keeps that person's data under, their
		// persona included
		assertEquals(User.getPrimaryUserIdAndTypePair(owner), SecurityProjectUtils.getProjectCreator(TWIN_ID));
		assertNull(SecurityProjectUtils.getProjectCreator("no-such-project"));
		assertNull(SecurityProjectUtils.getProjectCreator(null));

		// a project stored without its creator's login type matches the id alone
		QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET CREATEDBYTYPE = NULL WHERE PROJECTID = ?",
				ps -> ps.setString(1, TWIN_ID));
		assertTrue(SecurityProjectUtils.isProjectCreator(owner, TWIN_ID));
		assertFalse(SecurityProjectUtils.isProjectCreator(editor, TWIN_ID));
		// and says so, for a caller that must not take an id alone for a person
		assertNull(SecurityProjectUtils.getProjectCreator(TWIN_ID).getValue1());
	}

	@Test
	void itsOwnerCannotMakeItPublicDiscoverableOrATemplate() throws Exception {
		assertPeopleOnly(() -> SecurityProjectUtils.setProjectGlobal(owner, TWIN_ID, true));
		assertPeopleOnly(() -> SecurityProjectUtils.setProjectDiscoverable(owner, TWIN_ID, true));
		assertPeopleOnly(() -> SecurityProjectUtils.setProjectTemplate(owner, TWIN_ID, true));

		// turning them off is always allowed, and a plain project is as before
		assertTrue(SecurityProjectUtils.setProjectGlobal(owner, TWIN_ID, false));
		assertTrue(SecurityProjectUtils.setProjectDiscoverable(owner, TWIN_ID, false));
		assertTrue(SecurityProjectUtils.setProjectTemplate(owner, TWIN_ID, false));
		assertTrue(SecurityProjectUtils.setProjectGlobal(owner, PLAIN_ID, true));
		assertTrue(SecurityProjectUtils.setProjectDiscoverable(owner, PLAIN_ID, true));
		assertTrue(SecurityProjectUtils.setProjectTemplate(owner, PLAIN_ID, true));
		assertFalse(SecurityProjectUtils.userCanViewProject(stranger, TWIN_ID));
		assertTrue(SecurityProjectUtils.userCanViewProject(stranger, PLAIN_ID));
	}

	@Test
	void itsPortalIsNeverServedToSignedOutVisitors() throws Exception {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		assertPeopleOnly(() -> SecurityProjectUtils.setProjectCustomLogin(owner, TWIN_ID, true));
		assertPeopleOnly(() -> adminUtils.setProjectCustomLogin(TWIN_ID, true));
		assertFalse(SecurityProjectUtils.projectHasCustomLogin(TWIN_ID));

		// turning it off is always allowed, and a plain project can have one
		SecurityProjectUtils.setProjectCustomLogin(owner, TWIN_ID, false);
		adminUtils.setProjectCustomLogin(TWIN_ID, false);
		SecurityProjectUtils.setProjectCustomLogin(owner, PLAIN_ID, true);
		assertTrue(SecurityProjectUtils.projectHasCustomLogin(PLAIN_ID));
	}

	@Test
	void someoneWhoIsNotItsOwnerIsNotToldItIsATwin() {
		// the ordinary refusal, as on any project
		assertThrows(IllegalAccessException.class, () -> SecurityProjectUtils.setProjectGlobal(reader, TWIN_ID, true));
		assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectDiscoverable(reader, TWIN_ID, true));
		assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectTemplate(reader, TWIN_ID, true));

		Map<String, Object> change = changes(
				SecurityAccessPermissionBatchUtils.validateBatches(editor, List.of(teamAdd(TWIN_ID))).get(0)).get(0);
		assertEquals(false, change.get("allowed"));
		assertEquals("Only this project's owners can change which teams can use it", change.get("reason"));
	}

	@Test
	void anAdminCannotShareItBeyondNamedPeopleEither() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		assertPeopleOnly(() -> adminUtils.setProjectGlobal(TWIN_ID, true));
		assertPeopleOnly(() -> adminUtils.setProjectDiscoverable(TWIN_ID, true));
		assertPeopleOnly(() -> adminUtils.setProjectTemplate(TWIN_ID, true));
		assertPeopleOnly(() -> adminUtils.grantNewUsersProjectAccess(TWIN_ID, "READ_ONLY", admin, null));
		assertPeopleOnly(() -> adminUtils.addAllProjectUsers(TWIN_ID, "READ_ONLY", admin, null));
		assertFalse(SecurityProjectUtils.userCanViewProject(stranger, TWIN_ID));

		assertTrue(adminUtils.setProjectGlobal(TWIN_ID, false));
		assertTrue(adminUtils.setProjectGlobal(PLAIN_ID, true));
	}

	@Test
	void givingSomeoneEveryProjectLeavesItOut() {
		SecurityAdminUtils.getInstance(admin).grantAllProjects("strangerid", "READ_ONLY", true, admin);

		assertTrue(SecurityProjectUtils.userCanViewProject(stranger, PLAIN_ID));
		assertFalse(SecurityProjectUtils.userCanViewProject(stranger, TWIN_ID));
	}

	@Test
	void aTeamCannotBeGivenAccessToIt() throws Exception {
		PermissionTargetResource twin = PermissionTargetResource.project(TWIN_ID);

		// Bulk Access Management, for its owner and in admin mode
		assertBlocked(
				changes(SecurityAccessPermissionBatchUtils.validateBatches(owner, List.of(teamAdd(TWIN_ID))).get(0))
						.get(0));
		assertBlocked(changes(SecurityAccessPermissionBatchUtils
				.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(teamAdd(TWIN_ID))).get(0))
				.get(0));
		// the single method the group endpoints call
		assertThrows(IllegalAccessException.class, () -> SecurityGroupProjectUtils.addProjectGroupPermission(owner,
				GROUP_ID, CUSTOM, TWIN_ID, "READ_ONLY", null));
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission(GROUP_ID, CUSTOM, TWIN_ID));

		// the access screens read this to leave teams out
		Map<String, Object> allowed = map(
				SecurityAccessPermissionBatchUtils.getPermissions(owner, twin).get("allowedPermissionChanges"));
		assertEquals(false, allowed.get("canChangeTeams"));
		assertEquals(true, allowed.get("canChangePeople"));

		// a plain project still takes a team
		SecurityGroupProjectUtils.addProjectGroupPermission(owner, GROUP_ID, CUSTOM, PLAIN_ID, "READ_ONLY", null);
		assertEquals(AccessPermissionEnum.READ_ONLY.getId(),
				SecurityGroupProjectUtils.getGroupProjectPermission(GROUP_ID, CUSTOM, PLAIN_ID));
	}

	@Test
	void namedPeopleCanStillBeGivenAccessToIt() throws Exception {
		SecurityProjectUtils.addProjectUser(owner, "strangerid", TWIN_ID, "READ_ONLY", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(stranger, TWIN_ID));
	}

	@Test
	void itIsListedForItsOwnerAndThePeopleItIsSharedWith() {
		Map<String, Object> twins = Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, List.of("true"));

		List<Map<String, Object>> shared = SecurityProjectUtils.getUserProjectList(reader, null, null, false, twins,
				null, null, null, null);
		assertEquals(List.of(TWIN_ID), ids(shared));
		assertEquals("ownername", shared.getFirst().get("digital_twin_owner_name"));
		assertEquals(List.of(), ids(
				SecurityProjectUtils.getUserProjectList(stranger, null, null, false, twins, null, null, null, null)));
		// what the create step asks before it makes a second one: a twin the user made
		// and owns
		assertEquals(List.of(TWIN_ID),
				ids(SecurityProjectUtils.getUserProjectList(owner, null, null, false, twins, null, null, null, null,
						null, false, List.of(AccessPermissionEnum.OWNER.getId()), User.getUserIdAndType(owner))));
		assertEquals(List.of(),
				ids(SecurityProjectUtils.getUserProjectList(editor, null, null, false, twins, null, null, null, null,
						null, false, List.of(AccessPermissionEnum.OWNER.getId()), User.getUserIdAndType(editor))));
	}

	@Test
	void twinBylinesResolveLegacyIdsOnlyWhenUnambiguous() throws Exception {
		Map<String, Object> twins = Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, List.of("true"));
		Supplier<Object> byline = () -> SecurityProjectUtils
				.getUserProjectList(reader, null, null, false, twins, null, null, null, null).getFirst()
				.get("digital_twin_owner_name");
		QueryExecutionUtility.executeUpdate(securityDb, "UPDATE PROJECT SET CREATEDBYTYPE = NULL WHERE PROJECTID = ?",
				ps -> ps.setString(1, TWIN_ID));
		assertEquals("ownername", byline.get());
		QueryExecutionUtility.executeUpdate(securityDb,
				"INSERT INTO SMSS_USER (ID, TYPE, NAME) VALUES (?, 'MICROSOFT', 'Other person')",
				ps -> ps.setString(1, "ownerid"));
		assertNull(byline.get());
		QueryExecutionUtility.executeUpdate(securityDb,
				"UPDATE PROJECT SET CREATEDBYTYPE = 'NATIVE' WHERE PROJECTID = ?", ps -> ps.setString(1, TWIN_ID));
		assertEquals("ownername", byline.get());
	}

	@Test
	void onlySomeoneWhoCanChangeItSeesWhoItIsSharedWith() throws Exception {
		// its owner and an editor see its people, and who could still be added
		for (User manager : List.of(owner, editor)) {
			assertEquals(3, SecurityProjectUtils.getProjectUsers(manager, TWIN_ID, null, null, -1, -1).size());
			assertEquals(3L, SecurityProjectUtils.getProjectUsersCount(manager, TWIN_ID, null, null));
			SecurityProjectUtils.getProjectUsersNoCredentials(manager, TWIN_ID, null, -1, -1);
		}
		// someone it is only shared with is not told who else has it, in the words a
		// project they cannot view at all would get
		String refusal = "The user does not have access to view this project";
		for (User other : List.of(reader, stranger)) {
			assertEquals(refusal,
					assertThrows(IllegalAccessException.class,
							() -> SecurityProjectUtils.getProjectUsers(other, TWIN_ID, null, null, -1, -1))
							.getMessage());
			assertEquals(refusal, assertThrows(IllegalAccessException.class,
					() -> SecurityProjectUtils.getProjectUsersCount(other, TWIN_ID, null, null)).getMessage());
			assertEquals(refusal,
					assertThrows(IllegalAccessException.class,
							() -> SecurityProjectUtils.getProjectUsersNoCredentials(other, TWIN_ID, null, -1, -1))
							.getMessage());
		}
		// any other project is as it was: a reader sees who else has it
		assertEquals(3, SecurityProjectUtils.getProjectUsers(reader, PLAIN_ID, null, null, -1, -1).size());
		assertEquals(3L, SecurityProjectUtils.getProjectUsersCount(reader, PLAIN_ID, null, null));
		SecurityProjectUtils.getProjectUsersNoCredentials(reader, PLAIN_ID, null, -1, -1);
	}

	@Test
	void accessGivenUntilADayEndsThen() throws Exception {
		// as the People tab adds someone: the last second of the day its owner picked
		SecurityProjectUtils.addProjectUserPermissions(owner, TWIN_ID,
				List.of(Map.of("userid", "strangerid", "permission", "READ_ONLY")), "2999-12-31T23:59:59.000Z");
		assertTrue(SecurityProjectUtils.userCanViewProject(stranger, TWIN_ID));

		// its owner's list says when, as a UTC time without a zone
		String ends = SecurityProjectUtils.getProjectUsers(owner, TWIN_ID, null, null, -1, -1).stream()
				.filter(person -> "strangerid".equals(person.get("id")))
				.map(person -> String.valueOf(person.get("end_date"))).findFirst().orElse(null);
		assertEquals("2999-12-31 23:59:59", ends);

		// the day is over: the server's once-a-minute removal of ended access takes
		// theirs
		QueryExecutionUtility.write(securityDb, connection -> {
			QueryExecutionUtility.executeUpdate(connection,
					"UPDATE PROJECTPERMISSION SET ENDDATE = ? WHERE USERID = ? AND PROJECTID = ?", ps -> {
						ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC).minusDays(1)));
						ps.setString(2, "strangerid");
						ps.setString(3, TWIN_ID);
					});
			return null;
		});
		ExpiredAccessRemoval.removeAllEndedAccess();
		assertFalse(SecurityProjectUtils.userCanViewProject(stranger, TWIN_ID));
		assertTrue(SecurityProjectUtils.userCanViewProject(reader, TWIN_ID));
	}

	@Test
	void chatFileAccessIsOptInAndFollowsTheCallerAndTwin() {
		try (MockedStatic<ModelInferenceLogsUtils> workspaces = mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<PlatformAgentTools> tools = mockStatic(PlatformAgentTools.class)) {
			tools.when(PlatformAgentTools::getDefaultToolDefinitions).thenReturn(List.of());
			for (String id : List.of(TWIN_ID, PLAIN_ID)) {
				workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(id))
						.thenAnswer(call -> new HashMap<>(Map.of("workspace_id", id, "is_active", true)));
			}
			assertFalse(workspace(owner, TWIN_ID, false).containsKey("digital_twin_file_access"));
			assertEquals("manage", workspace(owner, TWIN_ID, true).get("digital_twin_file_access"));
			assertEquals("read", workspace(editor, TWIN_ID, true).get("digital_twin_file_access"));
			assertEquals("read", workspace(reader, TWIN_ID, true).get("digital_twin_file_access"));
			assertEquals("read", workspace(owner, PLAIN_ID, true).get("digital_twin_file_access"));
			assertThrows(IllegalArgumentException.class, () -> workspace(stranger, TWIN_ID, true));
		}
	}

	///

	private static void assertPeopleOnly(Executable sharing) {
		assertEquals(PEOPLE_ONLY, assertThrows(IllegalArgumentException.class, sharing).getMessage());
	}

	private static Map<String, Object> workspace(User user, String projectId, boolean includeAccess) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		NounStore store = new NounStore("get-workspace");
		store.makeNoun("workspaceId").addLiteral(projectId);
		if (includeAccess) {
			store.makeNoun("includeDigitalTwinFileAccess").addBoolean(true);
		}
		GetWorkspaceReactor reactor = new GetWorkspaceReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return map(reactor.execute().getValue());
	}

	private static void assertBlocked(Map<String, Object> change) {
		assertEquals(false, change.get("allowed"));
		assertEquals(PEOPLE_ONLY, change.get("reason"));
	}

	private static PermissionBatch teamAdd(String projectId) {
		return new PermissionBatch(PermissionTargetResource.project(projectId), List.of(PermissionChange
				.add(PermissionSubject.ofTeam(GROUP_ID, CUSTOM), new PermissionGrant("READ_ONLY", null))));
	}

	private static List<String> ids(List<Map<String, Object>> projects) {
		return projects.stream().map(project -> String.valueOf(project.get("project_id"))).toList();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return (Map<String, Object>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> changes(Map<String, Object> result) {
		return (List<Map<String, Object>>) result.get("changes");
	}
}
