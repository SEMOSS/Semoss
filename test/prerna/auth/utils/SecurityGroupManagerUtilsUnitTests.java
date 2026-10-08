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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.util.SystemEngineRegistry;

/**
 * Runs the group manager rules against a disposable security database. The
 * Microsoft directory is replaced with a static mock so each test decides
 * whether it is available and what adding a directory user does.
 */
class SecurityGroupManagerUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String NATIVE = AuthProvider.NATIVE.getLabel();
	private static final String MICROSOFT = AuthProvider.MICROSOFT.getLabel();

	private static final String GROUP_ID = "team";
	private static final String OTHER_GROUP_ID = "otherteam";
	private static final String NON_CUSTOM_GROUP_ID = "msgroup";

	private static final String ADMIN_ID = "adminid";
	private static final String MANAGER_ID = "managerid";
	private static final String OUTSIDER_ID = "outsiderid";
	private static final String MANAGER_MS_ID = "manager-ms";
	private static final String DIRECTORY_USER_ID = "directory-user";
	private static final String PROJECT_ID = "pid1";
	private static final String ENGINE_ID = "eid1";

	private static final Set<String> MANAGER_KEYS = Set.of("userid", "type", "dateadded", "name", "username", "email");

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private MockedStatic<MicrosoftGraphUserLookup> directory;

	private User admin;
	private User manager;
	private User outsider;
	private AdminSecurityGroupUtils groups;

	@BeforeEach
	void setup() throws Exception {
		directory = mockStatic(MicrosoftGraphUserLookup.class);
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		manager = UnitTestSecurityAuthUtils.createUser("manager", false);
		outsider = UnitTestSecurityAuthUtils.createUser("outsider", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);

		groups.addGroup(admin, GROUP_ID, CUSTOM, "the team");
		groups.addGroup(admin, OTHER_GROUP_ID, CUSTOM, "another team");
		groups.addGroup(admin, NON_CUSTOM_GROUP_ID, MICROSOFT, "a login group");
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, MANAGER_ID, NATIVE);
	}

	@AfterEach
	void cleanup() throws Exception {
		directory.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// userIsGroupManager
	///

	@Test
	void userIsGroupManager_trueForAManagersLogin() {
		assertTrue(SecurityGroupManagerUtils.userIsGroupManager(manager, GROUP_ID));
		assertTrue(SecurityGroupManagerUtils.userIsGroupManager(admin, GROUP_ID));
	}

	@Test
	void userIsGroupManager_falseForAnotherGroupOrANonManager() {
		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(manager, OTHER_GROUP_ID));
		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(outsider, GROUP_ID));
	}

	@Test
	void userIsGroupManager_requiresTheLoginTypeToMatchAsWellAsTheId() throws Exception {
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, OUTSIDER_ID, MICROSOFT);

		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(outsider, GROUP_ID));
	}

	@Test
	void userIsGroupManager_matchesAnyOfTheUsersLogins() throws Exception {
		UnitTestGroupTables.insertManager(securityDb, OTHER_GROUP_ID, MANAGER_MS_ID, MICROSOFT);
		addMicrosoftLogin(manager, MANAGER_MS_ID);

		assertTrue(SecurityGroupManagerUtils.userIsGroupManager(manager, OTHER_GROUP_ID));
	}

	@Test
	void userIsGroupManager_falseWithoutAUserGroupOrLogin() {
		User noLogins = new User();
		User loginWithoutId = new User();
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.NATIVE);
		loginWithoutId.setAccessToken(token);

		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(null, GROUP_ID));
		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(manager, null));
		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(noLogins, GROUP_ID));
		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(loginWithoutId, GROUP_ID));
	}

	///
	/// userCanManageGroup
	///

	@Test
	void userCanManageGroup_adminCanManageACustomGroupTheyDoNotManage() {
		User otherAdmin = UnitTestSecurityAuthUtils.createUser("otheradmin", true);

		assertFalse(SecurityGroupManagerUtils.userIsGroupManager(otherAdmin, GROUP_ID));
		assertTrue(SecurityGroupManagerUtils.userCanManageGroup(otherAdmin, GROUP_ID));
	}

	@Test
	void userCanManageGroup_managerCanManageOnlyTheirGroup() {
		assertTrue(SecurityGroupManagerUtils.userCanManageGroup(manager, GROUP_ID));
		assertFalse(SecurityGroupManagerUtils.userCanManageGroup(manager, OTHER_GROUP_ID));
		assertFalse(SecurityGroupManagerUtils.userCanManageGroup(outsider, GROUP_ID));
	}

	@Test
	void userCanManageGroup_falseForAGroupThatIsNotCustomEvenWithAManagerRow() throws Exception {
		UnitTestGroupTables.insertManager(securityDb, NON_CUSTOM_GROUP_ID, MANAGER_ID, NATIVE);

		assertFalse(SecurityGroupManagerUtils.userCanManageGroup(manager, NON_CUSTOM_GROUP_ID));
		assertFalse(SecurityGroupManagerUtils.userCanManageGroup(admin, NON_CUSTOM_GROUP_ID));
	}

	@Test
	void userCanManageGroup_falseForAMissingGroup() {
		assertFalse(SecurityGroupManagerUtils.userCanManageGroup(admin, "missing"));
		assertFalse(SecurityGroupManagerUtils.userCanManageGroup(admin, null));
	}

	///
	/// userCanViewGroupManagers
	///

	@Test
	void userCanViewGroupManagers_managerCanView() {
		assertTrue(SecurityGroupManagerUtils.userCanViewGroupManagers(manager, GROUP_ID, null, null));
	}

	@Test
	void userCanViewGroupManagers_projectOwnerCanView() throws Exception {
		UnitTestSecurityAuthUtils.createProject("pid", "pname", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, "pid", OUTSIDER_ID, "OWNER");

		assertTrue(SecurityGroupManagerUtils.userCanViewGroupManagers(outsider, GROUP_ID, "pid", null));
	}

	@Test
	void userCanViewGroupManagers_engineOwnerCanView() {
		UnitTestSecurityAuthUtils.createEngine("eid", "ename", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, OUTSIDER_ID, "eid", "OWNER");

		assertTrue(SecurityGroupManagerUtils.userCanViewGroupManagers(outsider, GROUP_ID, null, "eid"));
	}

	@Test
	void userCanViewGroupManagers_editorsCannotView() throws Exception {
		UnitTestSecurityAuthUtils.createProject("pid", "pname", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, "pid", OUTSIDER_ID, "EDIT");
		UnitTestSecurityAuthUtils.createEngine("eid", "ename", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, OUTSIDER_ID, "eid", "EDIT");

		assertFalse(SecurityGroupManagerUtils.userCanViewGroupManagers(outsider, GROUP_ID, "pid", "eid"));
	}

	@Test
	void userCanViewGroupManagers_ownersCannotViewAGroupThatIsNotCustom() throws Exception {
		UnitTestSecurityAuthUtils.createProject("pid", "pname", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, "pid", OUTSIDER_ID, "OWNER");

		assertFalse(SecurityGroupManagerUtils.userCanViewGroupManagers(outsider, NON_CUSTOM_GROUP_ID, "pid", null));
	}

	@Test
	void userCanViewGroupManagers_noResourceMeansOnlyManagersAndAdmins() {
		assertFalse(SecurityGroupManagerUtils.userCanViewGroupManagers(outsider, GROUP_ID, null, null));
		assertTrue(SecurityGroupManagerUtils.userCanViewGroupManagers(admin, OTHER_GROUP_ID, null, null));
	}

	///
	/// clampPageSize
	///

	@ParameterizedTest
	@CsvSource({ "0, 200", "-1, 200", "-500, 200", "201, 200", "5000, 200", "1, 1", "50, 50", "200, 200" })
	void clampPageSize_keepsThePageBetweenOneAndTheMaximum(long requested, long expected) {
		assertEquals(expected, SecurityGroupManagerUtils.clampPageSize(requested));
	}

	///
	/// getManagedGroups and getNumManagedGroups
	///

	@Test
	void getManagedGroups_listsOnlyTheCustomGroupsTheUserManages() throws Exception {
		UnitTestGroupTables.insertManager(securityDb, NON_CUSTOM_GROUP_ID, MANAGER_ID, NATIVE);

		List<Map<String, Object>> managed = SecurityGroupManagerUtils.getManagedGroups(manager, null, 0, 0);

		assertEquals(List.of(GROUP_ID), ids(managed));
		assertEquals(CUSTOM, managed.getFirst().get("type"));
		assertEquals(1L, SecurityGroupManagerUtils.getNumManagedGroups(manager, null));
		assertTrue(SecurityGroupManagerUtils.getManagedGroups(outsider, null, 0, 0).isEmpty());
		assertEquals(0L, SecurityGroupManagerUtils.getNumManagedGroups(outsider, null));
	}

	@Test
	void getManagedGroups_returnsAGroupOnceWithTheEarliestDateOfTheUsersLogins() throws Exception {
		groups.addGroup(admin, "since", CUSTOM, "two logins manage this");
		UnitTestGroupTables.insertManager(securityDb, "since", MANAGER_ID, NATIVE,
				Timestamp.valueOf(LocalDateTime.of(2024, 6, 15, 12, 0)));
		UnitTestGroupTables.insertManager(securityDb, "since", MANAGER_MS_ID, MICROSOFT,
				Timestamp.valueOf(LocalDateTime.of(2020, 6, 15, 12, 0)));
		addMicrosoftLogin(manager, MANAGER_MS_ID);

		List<Map<String, Object>> managed = SecurityGroupManagerUtils.getManagedGroups(manager, "since", 0, 0);

		assertEquals(List.of("since"), ids(managed));
		SemossDate managerSince = assertInstanceOf(SemossDate.class, managed.getFirst().get("manager_since"));
		assertEquals(2020, managerSince.getLocalDateTime().getYear());
		assertEquals(1L, SecurityGroupManagerUtils.getNumManagedGroups(manager, "since"));
	}

	@Test
	void getManagedGroups_addsTheMemberCount() throws Exception {
		groups.addUserToGroup(admin, GROUP_ID, OUTSIDER_ID, NATIVE, null);
		groups.addUserToGroup(admin, GROUP_ID, ADMIN_ID, NATIVE, null);

		List<Map<String, Object>> managed = SecurityGroupManagerUtils.getManagedGroups(manager, null, 0, 0);

		assertEquals(2L, managed.getFirst().get("member_count"));
		assertNotNull(managed.getFirst().get("manager_since"));
	}

	@Test
	void getManagedGroups_searchesLimitsAndOffsets() throws Exception {
		for (String id : List.of("alpha", "beta", "gamma")) {
			groups.addGroup(admin, id, CUSTOM, id);
			UnitTestGroupTables.insertManager(securityDb, id, MANAGER_ID, NATIVE);
		}

		assertEquals(List.of("alpha", "beta", "gamma", GROUP_ID),
				ids(SecurityGroupManagerUtils.getManagedGroups(manager, null, 0, 0)));
		assertEquals(List.of("beta", "gamma"), ids(SecurityGroupManagerUtils.getManagedGroups(manager, null, 2, 1)));
		assertEquals(List.of("beta"), ids(SecurityGroupManagerUtils.getManagedGroups(manager, "bet", 0, 0)));
		assertEquals(4L, SecurityGroupManagerUtils.getNumManagedGroups(manager, "  "));
		assertEquals(1L, SecurityGroupManagerUtils.getNumManagedGroups(manager, "gam"));
	}

	@Test
	void getManagedGroups_emptyWithoutAUserOrLogin() {
		assertTrue(SecurityGroupManagerUtils.getManagedGroups(null, null, 0, 0).isEmpty());
		assertTrue(SecurityGroupManagerUtils.getManagedGroups(new User(), null, 0, 0).isEmpty());
		assertEquals(0L, SecurityGroupManagerUtils.getNumManagedGroups(null, null));
		assertEquals(0L, SecurityGroupManagerUtils.getNumManagedGroups(new User(), null));
	}

	///
	/// getGroupManagers
	///

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "  " })
	void getGroupManagers_requiresTheGroupId(String groupId) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.getGroupManagers(admin, groupId, null, null));
		assertEquals("Must define the group id", e.getMessage());
	}

	@Test
	void getGroupManagers_deniesUsersWhoCannotView() throws Exception {
		UnitTestSecurityAuthUtils.createProject("pid", "pname", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, "pid", OUTSIDER_ID, "EDIT");

		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getGroupManagers(outsider, GROUP_ID, null, null));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getGroupManagers(outsider, GROUP_ID, "pid", null));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getGroupManagers(admin, NON_CUSTOM_GROUP_ID, null, null));
	}

	@Test
	void getGroupManagers_returnsOnlyTheManagerKeysSortedByName() throws Exception {
		List<Map<String, Object>> managers = SecurityGroupManagerUtils.getGroupManagers(manager, GROUP_ID, null, null);

		assertEquals(2, managers.size());
		assertEquals("adminname", managers.get(0).get("name"));
		assertEquals("managername", managers.get(1).get("name"));
		for (Map<String, Object> row : managers) {
			assertEquals(MANAGER_KEYS, row.keySet());
			assertEquals(NATIVE, row.get("type"));
			assertNotNull(row.get("dateadded"));
		}
		assertEquals(MANAGER_ID, managers.get(1).get("userid"));
		assertEquals("manager@test.com", managers.get(1).get("email"));
	}

	@Test
	void getGroupManagers_leavesOutRowsWithoutAMatchingUser() throws Exception {
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, "ghost", NATIVE);
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, OUTSIDER_ID, MICROSOFT);

		List<Map<String, Object>> managers = SecurityGroupManagerUtils.getGroupManagers(admin, GROUP_ID, null, null);

		assertEquals(List.of(ADMIN_ID, MANAGER_ID), managers.stream().map(row -> row.get("userid")).toList());
	}

	@Test
	void getGroupManagers_resourceOwnerCanReadThem() throws Exception {
		UnitTestSecurityAuthUtils.createEngine("eid", "ename", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, OUTSIDER_ID, "eid", "OWNER");

		assertEquals(2, SecurityGroupManagerUtils.getGroupManagers(outsider, GROUP_ID, null, "eid").size());
	}

	///
	/// managerExists
	///

	@Test
	void managerExists_matchesGroupIdAndType() {
		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, MANAGER_ID, NATIVE));
		assertFalse(SecurityGroupManagerUtils.managerExists(GROUP_ID, MANAGER_ID, MICROSOFT));
		assertFalse(SecurityGroupManagerUtils.managerExists(OTHER_GROUP_ID, MANAGER_ID, NATIVE));
	}

	///
	/// addGroupManager
	///

	@Test
	void addGroupManager_managerAddsAManagerWithTheNormalizedType() throws Exception {
		SecurityGroupManagerUtils.addGroupManager(manager, GROUP_ID, OUTSIDER_ID, "native");

		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, OUTSIDER_ID, NATIVE));
		assertTrue(SecurityGroupManagerUtils.userIsGroupManager(outsider, GROUP_ID));
		String grantedBy = "SELECT PERMISSIONGRANTEDBY || ':' || PERMISSIONGRANTEDBYTYPE FROM GROUPMANAGERS "
				+ "WHERE GROUPID=? AND USERID=?";
		assertEquals(MANAGER_ID + ":" + NATIVE,
				UnitTestGroupTables.queryValue(securityDb, grantedBy, GROUP_ID, OUTSIDER_ID));
		assertNotNull(UnitTestGroupTables.queryValue(securityDb,
				"SELECT DATEADDED FROM GROUPMANAGERS WHERE GROUPID=? AND USERID=?", GROUP_ID, OUTSIDER_ID));
	}

	@Test
	void addGroupManager_rejectsAUserWhoAlreadyManagesTheGroup() throws Exception {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, MANAGER_ID, NATIVE));

		assertEquals("User managerid already manages group team", e.getMessage());
		assertEquals(List.of(ADMIN_ID + ":" + NATIVE, MANAGER_ID + ":" + NATIVE),
				UnitTestGroupTables.managers(securityDb, GROUP_ID));
	}

	@Test
	void addGroupManager_rejectsAUserWhoDoesNotExist() throws Exception {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, "nobody", NATIVE));

		assertEquals("User nobody does not exist", e.getMessage());
		assertFalse(SecurityGroupManagerUtils.managerExists(GROUP_ID, "nobody", NATIVE));
	}

	@Test
	void addGroupManager_deniesUsersWhoCannotManageTheGroup() throws Exception {
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(outsider, GROUP_ID, OUTSIDER_ID, NATIVE));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(manager, OTHER_GROUP_ID, OUTSIDER_ID, NATIVE));

		assertFalse(SecurityGroupManagerUtils.managerExists(GROUP_ID, OUTSIDER_ID, NATIVE));
		assertFalse(SecurityGroupManagerUtils.managerExists(OTHER_GROUP_ID, OUTSIDER_ID, NATIVE));
	}

	@Test
	void addGroupManager_validatesTheGroupTypeAndUserId() {
		assertEquals("Must define the group id", assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, " ", OUTSIDER_ID, NATIVE)).getMessage());
		assertEquals("Group msgroup is not a custom group", assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, NON_CUSTOM_GROUP_ID, OUTSIDER_ID, NATIVE))
				.getMessage());
		assertEquals("Group missing is not a custom group",
				assertThrows(IllegalArgumentException.class,
						() -> SecurityGroupManagerUtils.addGroupManager(admin, "missing", OUTSIDER_ID, NATIVE))
						.getMessage());
		assertEquals("Must define the user's login type",
				assertThrows(IllegalArgumentException.class,
						() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, OUTSIDER_ID, null))
						.getMessage());
		assertEquals("Must define the user id", assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, " ", NATIVE)).getMessage());
	}

	@Test
	void addGroupManager_addsAMissingDirectoryUserAfterValidation() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		directory.when(() -> MicrosoftGraphUserLookup.addMissingUser(any(), eq(DIRECTORY_USER_ID))).thenAnswer(call -> {
			UnitTestGroupTables.insertUser(securityDb, DIRECTORY_USER_ID, MICROSOFT);
			return true;
		});

		SecurityGroupManagerUtils.addGroupManager(manager, GROUP_ID, DIRECTORY_USER_ID, "ms");

		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, DIRECTORY_USER_ID, MICROSOFT));
		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(manager, DIRECTORY_USER_ID), times(1));
	}

	@Test
	void addGroupManager_checksRightsAndDuplicatesBeforeTheDirectory() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT);

		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(outsider, GROUP_ID, "someone-new", MICROSOFT));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, NON_CUSTOM_GROUP_ID, "someone-new", MICROSOFT));

		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(any(), any()), never());
	}

	@Test
	void addGroupManager_skipsTheDirectoryForOtherLoginTypes() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);

		SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, OUTSIDER_ID, NATIVE);

		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, OUTSIDER_ID, NATIVE));
		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(any(), any()), never());
	}

	@Test
	void addGroupManager_skipsTheDirectoryWhenItIsNotAvailable() throws Exception {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT));

		assertEquals("User directory-user does not exist", e.getMessage());
		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(any(), any()), never());
	}

	@Test
	void addGroupManager_rechecksForTheManagerBeforeInserting() throws Exception {
		// the directory step stands in for a second request that adds the same
		// manager after the first duplicate check
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		directory.when(() -> MicrosoftGraphUserLookup.addMissingUser(any(), eq(DIRECTORY_USER_ID))).thenAnswer(call -> {
			UnitTestGroupTables.insertUser(securityDb, DIRECTORY_USER_ID, MICROSOFT);
			UnitTestGroupTables.insertManager(securityDb, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT);
			return true;
		});

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(admin, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT));

		assertEquals("User directory-user already manages group team", e.getMessage());
		assertEquals(1L,
				((Number) UnitTestGroupTables.queryValue(securityDb,
						"SELECT COUNT(*) FROM GROUPMANAGERS WHERE GROUPID=? AND USERID=?", GROUP_ID, DIRECTORY_USER_ID))
						.longValue());
	}

	///
	/// removeGroupManager
	///

	@Test
	void removeGroupManager_managerRemovesAnotherManager() throws Exception {
		SecurityGroupManagerUtils.removeGroupManager(manager, GROUP_ID, ADMIN_ID, "Native");

		assertEquals(List.of(MANAGER_ID + ":" + NATIVE), UnitTestGroupTables.managers(securityDb, GROUP_ID));
	}

	@Test
	void removeGroupManager_onlyRemovesTheMatchingLoginType() throws Exception {
		UnitTestGroupTables.insertManager(securityDb, GROUP_ID, MANAGER_ID, MICROSOFT);

		SecurityGroupManagerUtils.removeGroupManager(admin, GROUP_ID, MANAGER_ID, "ms");

		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, MANAGER_ID, NATIVE));
		assertFalse(SecurityGroupManagerUtils.managerExists(GROUP_ID, MANAGER_ID, MICROSOFT));
	}

	@Test
	void removeGroupManager_rejectsAUserWhoIsNotAManager() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.removeGroupManager(admin, GROUP_ID, OUTSIDER_ID, NATIVE));

		assertEquals("User outsiderid does not manage group team", e.getMessage());
	}

	@Test
	void removeGroupManager_deniesUsersWhoCannotManageTheGroup() throws Exception {
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.removeGroupManager(outsider, GROUP_ID, MANAGER_ID, NATIVE));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.removeGroupManager(manager, OTHER_GROUP_ID, ADMIN_ID, NATIVE));

		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, MANAGER_ID, NATIVE));
		assertTrue(SecurityGroupManagerUtils.managerExists(OTHER_GROUP_ID, ADMIN_ID, NATIVE));
	}

	///
	/// getGroupDetails and the member reads
	///

	@Test
	void getGroupDetails_managerReadsTheirGroup() throws Exception {
		Map<String, Object> details = SecurityGroupManagerUtils.getGroupDetails(manager, GROUP_ID);

		assertEquals(GROUP_ID, details.get("id"));
		assertEquals(CUSTOM, details.get("type"));
		assertEquals("the team", details.get("description"));
	}

	@Test
	void memberReads_managerReadsMembersAndNonMembers() throws Exception {
		groups.addUserToGroup(admin, GROUP_ID, OUTSIDER_ID, NATIVE, null);

		List<Map<String, Object>> members = SecurityGroupManagerUtils.getGroupMembers(manager, GROUP_ID, null, 0, 0);
		List<Map<String, Object>> nonMembers = SecurityGroupManagerUtils.getNonGroupMembers(manager, GROUP_ID, null, 0,
				0);

		assertEquals(List.of(OUTSIDER_ID), members.stream().map(row -> row.get("userid")).toList());
		assertEquals(List.of(OUTSIDER_ID), SecurityGroupManagerUtils.getAllGroupMembers(manager, GROUP_ID).stream()
				.map(row -> row.get("userid")).toList());
		assertEquals(Set.of(ADMIN_ID, MANAGER_ID), Set.copyOf(nonMembers.stream().map(row -> row.get("id")).toList()));
		assertEquals(1L, SecurityGroupManagerUtils.getNumMembersInGroup(manager, GROUP_ID, null));
	}

	@Test
	void memberReads_pagesAreClampedButTheFullListIsNot() throws Exception {
		int extra = SecurityGroupManagerUtils.MAX_PAGE_SIZE + 5;
		UnitTestGroupTables.insertUsers(securityDb, "member", extra, NATIVE, GROUP_ID);
		UnitTestGroupTables.insertUsers(securityDb, "user", extra, NATIVE, null);

		assertEquals(200, SecurityGroupManagerUtils.getGroupMembers(manager, GROUP_ID, null, 0, 0).size());
		assertEquals(200, SecurityGroupManagerUtils.getGroupMembers(manager, GROUP_ID, null, 1000, 0).size());
		assertEquals(10, SecurityGroupManagerUtils.getGroupMembers(manager, GROUP_ID, null, 10, 0).size());
		assertEquals(extra, SecurityGroupManagerUtils.getAllGroupMembers(manager, GROUP_ID).size());
		assertEquals(extra, SecurityGroupManagerUtils.getNumMembersInGroup(manager, GROUP_ID, null).intValue());

		assertEquals(200, SecurityGroupManagerUtils.getNonGroupMembers(manager, GROUP_ID, null, -1, 0).size());
		assertEquals(5, SecurityGroupManagerUtils.getNonGroupMembers(manager, GROUP_ID, null, 5, 0).size());
	}

	@Test
	void memberReads_denyUsersWhoCannotManageTheGroup() {
		assertThrows(IllegalAccessException.class, () -> SecurityGroupManagerUtils.getGroupDetails(outsider, GROUP_ID));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getGroupMembers(outsider, GROUP_ID, null, 0, 0));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getAllGroupMembers(outsider, GROUP_ID));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getNumMembersInGroup(outsider, GROUP_ID, null));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getNonGroupMembers(outsider, GROUP_ID, null, 0, 0));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getGroupMembers(manager, OTHER_GROUP_ID, null, 0, 0));
	}

	@Test
	void memberReads_rejectGroupsThatAreNotCustom() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.getGroupMembers(admin, NON_CUSTOM_GROUP_ID, null, 0, 0));
		assertEquals("Group msgroup is not a custom group", e.getMessage());
	}

	///
	/// for admins, who prove it with the admin group utilities
	///

	@Test
	void adminVersions_doNotAskTheDatabaseAgainWhetherTheUserIsAnAdmin() throws Exception {
		try (MockedStatic<SecurityAdminUtils> adminCheck = mockStatic(SecurityAdminUtils.class, CALLS_REAL_METHODS)) {
			SecurityGroupManagerUtils.addUserToGroup(groups, admin, GROUP_ID, OUTSIDER_ID, NATIVE, null);
			SecurityGroupManagerUtils.removeUserFromGroup(groups, GROUP_ID, OUTSIDER_ID, NATIVE);
			SecurityGroupManagerUtils.addGroupManager(groups, admin, GROUP_ID, OUTSIDER_ID, NATIVE);
			assertEquals(Set.of(MANAGER_ID, OUTSIDER_ID), Set.copyOf(SecurityGroupManagerUtils
					.getGroupManagers(groups, GROUP_ID).stream().map(row -> row.get("userid")).toList()));
			SecurityGroupManagerUtils.removeGroupManager(groups, GROUP_ID, OUTSIDER_ID, NATIVE);

			adminCheck.verify(() -> SecurityAdminUtils.userIsAdmin(any()), never());
		}
		assertFalse(SecurityGroupManagerUtils.managerExists(GROUP_ID, OUTSIDER_ID, NATIVE));
		assertTrue(UnitTestGroupTables.members(securityDb, GROUP_ID).isEmpty());
	}

	@Test
	void adminVersions_requireTheAdminGroupUtilities() throws Exception {
		AdminSecurityGroupUtils none = null;
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.addUserToGroup(none, admin, GROUP_ID, OUTSIDER_ID, NATIVE, null));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.removeUserFromGroup(none, GROUP_ID, OUTSIDER_ID, NATIVE));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.addGroupManager(none, admin, GROUP_ID, OUTSIDER_ID, NATIVE));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.removeGroupManager(none, GROUP_ID, MANAGER_ID, NATIVE));
		assertThrows(IllegalAccessException.class, () -> SecurityGroupManagerUtils.getGroupManagers(none, GROUP_ID));
		// nothing changed
		assertTrue(UnitTestGroupTables.members(securityDb, GROUP_ID).isEmpty());
		assertTrue(SecurityGroupManagerUtils.managerExists(GROUP_ID, MANAGER_ID, NATIVE));
	}

	@Test
	void adminVersions_stillOnlyWorkOnCustomGroups() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.getGroupManagers(groups, NON_CUSTOM_GROUP_ID));
		assertEquals("Group msgroup is not a custom group", e.getMessage());
		assertThrows(IllegalArgumentException.class, () -> SecurityGroupManagerUtils.addUserToGroup(groups, admin,
				NON_CUSTOM_GROUP_ID, OUTSIDER_ID, NATIVE, null));
	}

	///
	/// the group's projects and engines
	///

	@Test
	void resourceReads_showAManagerTheGroupsProjectsAndEngines() throws Exception {
		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "Project One", admin);
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "Engine One", admin);
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 3, null);
		groups.addGroupEnginePermission(admin, GROUP_ID, CUSTOM, ENGINE_ID, 3, null);

		assertEquals(List.of(PROJECT_ID),
				SecurityGroupManagerUtils.getProjectsForGroup(manager, GROUP_ID, null, 0, 0, false).stream()
						.map(row -> row.get("project_id")).toList());
		assertEquals(1L, SecurityGroupManagerUtils.getNumProjectsForGroup(manager, GROUP_ID, null, false));
		assertEquals(List.of(ENGINE_ID), SecurityGroupManagerUtils.getEnginesForGroup(manager, GROUP_ID, null, 0, 0)
				.stream().map(row -> row.get("engine_id")).toList());
		assertEquals(1L, SecurityGroupManagerUtils.getNumEnginesForGroup(manager, GROUP_ID, null));

		// another group's resources are not mixed in
		assertEquals(0L, SecurityGroupManagerUtils.getNumProjectsForGroup(admin, OTHER_GROUP_ID, null, false));
		assertEquals(0L, SecurityGroupManagerUtils.getNumEnginesForGroup(admin, OTHER_GROUP_ID, null));
	}

	@Test
	void resourceReads_denyUsersWhoCannotManageTheGroup() {
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getProjectsForGroup(outsider, GROUP_ID, null, 0, 0, false));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getNumProjectsForGroup(outsider, GROUP_ID, null, false));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getEnginesForGroup(outsider, GROUP_ID, null, 0, 0));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getNumEnginesForGroup(outsider, GROUP_ID, null));
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.getProjectsForGroup(manager, OTHER_GROUP_ID, null, 0, 0, false));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.getEnginesForGroup(admin, NON_CUSTOM_GROUP_ID, null, 0, 0));
		assertEquals("Group msgroup is not a custom group", e.getMessage());
	}

	///
	/// addUserToGroup
	///

	@Test
	void addUserToGroup_managerAddsAMemberWithTheNormalizedType() throws Exception {
		SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, OUTSIDER_ID, "native", null);

		assertEquals(List.of(OUTSIDER_ID + ":" + NATIVE), UnitTestGroupTables.members(securityDb, GROUP_ID));
	}

	@Test
	void addUserToGroup_rejectsAnExistingMember() throws Exception {
		SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, OUTSIDER_ID, NATIVE, null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, OUTSIDER_ID, NATIVE, null));
		assertEquals("User outsiderid already has access to group team", e.getMessage());
		assertEquals(1, UnitTestGroupTables.members(securityDb, GROUP_ID).size());
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "   " })
	void addUserToGroup_treatsAMissingEndDateAsNone(String endDate) throws Exception {
		SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, OUTSIDER_ID, NATIVE, endDate);

		assertEquals(1, UnitTestGroupTables.members(securityDb, GROUP_ID).size());
		assertNull(memberEndDate(OUTSIDER_ID));
	}

	@Test
	void addUserToGroup_storesAValidEndDate() throws Exception {
		String endDate = " " + ZonedDateTime.now().plusDays(2) + " ";

		SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, OUTSIDER_ID, NATIVE, endDate);

		assertNotNull(memberEndDate(OUTSIDER_ID));
	}

	@Test
	void addUserToGroup_rejectsAnUnreadableEndDateBeforeAddingADirectoryUser() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupManagerUtils
				.addUserToGroup(manager, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT, "not-a-date"));

		assertEquals("The end date not-a-date is not a valid date", e.getMessage());
		assertTrue(UnitTestGroupTables.members(securityDb, GROUP_ID).isEmpty());
		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(any(), any()), never());
	}

	@Test
	void addUserToGroup_addsAMissingDirectoryUserThenTheMembership() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		directory.when(() -> MicrosoftGraphUserLookup.addMissingUser(any(), eq(DIRECTORY_USER_ID))).thenAnswer(call -> {
			UnitTestGroupTables.insertUser(securityDb, DIRECTORY_USER_ID, MICROSOFT);
			return true;
		});

		SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, DIRECTORY_USER_ID, "microsoft", null);

		assertEquals(List.of(DIRECTORY_USER_ID + ":" + MICROSOFT), UnitTestGroupTables.members(securityDb, GROUP_ID));
		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(manager, DIRECTORY_USER_ID), times(1));
	}

	@Test
	void addUserToGroup_checksRightsAndMembershipBeforeTheDirectory() throws Exception {
		directory.when(MicrosoftGraphUserLookup::isEnabled).thenReturn(true);
		UnitTestGroupTables.insertUser(securityDb, DIRECTORY_USER_ID, MICROSOFT);
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT);

		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.addUserToGroup(outsider, GROUP_ID, "someone-new", MICROSOFT, null));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, DIRECTORY_USER_ID, MICROSOFT, null));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, "  ", MICROSOFT, null));

		directory.verify(() -> MicrosoftGraphUserLookup.addMissingUser(any(), any()), never());
	}

	@Test
	void addUserToGroup_rejectsAUserWhoDoesNotExist() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.addUserToGroup(manager, GROUP_ID, "nobody", NATIVE, null));

		assertEquals("User nobody does not exist", e.getMessage());
	}

	///
	/// removeUserFromGroup
	///

	@Test
	void removeUserFromGroup_managerRemovesAMember() throws Exception {
		groups.addUserToGroup(admin, GROUP_ID, OUTSIDER_ID, NATIVE, null);

		SecurityGroupManagerUtils.removeUserFromGroup(manager, GROUP_ID, OUTSIDER_ID, "native");

		assertTrue(UnitTestGroupTables.members(securityDb, GROUP_ID).isEmpty());
	}

	@Test
	void removeUserFromGroup_rejectsAUserWhoIsNotAMember() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.removeUserFromGroup(manager, GROUP_ID, OUTSIDER_ID, NATIVE));

		assertEquals("User outsiderid does not have access to group team", e.getMessage());
	}

	@Test
	void removeUserFromGroup_validatesAndChecksRights() throws Exception {
		groups.addUserToGroup(admin, GROUP_ID, ADMIN_ID, NATIVE, null);

		assertEquals("Must define the user id",
				assertThrows(IllegalArgumentException.class,
						() -> SecurityGroupManagerUtils.removeUserFromGroup(manager, GROUP_ID, null, NATIVE))
						.getMessage());
		assertThrows(IllegalAccessException.class,
				() -> SecurityGroupManagerUtils.removeUserFromGroup(outsider, GROUP_ID, ADMIN_ID, NATIVE));

		assertEquals(List.of(ADMIN_ID + ":" + NATIVE), UnitTestGroupTables.members(securityDb, GROUP_ID));
	}

	///
	/// normalizeUserType
	///

	@ParameterizedTest
	@CsvSource({ "native, NATIVE", "' Native ', NATIVE", "NATIVE, NATIVE", "ms, MICROSOFT", "microsoft, MICROSOFT",
			"MICROSOFT, MICROSOFT", "SomeOtherType, SomeOtherType" })
	void normalizeUserType_returnsTheStoredLabel(String requested, String expected) {
		assertEquals(expected, SecurityGroupManagerUtils.normalizeUserType(requested));
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { "  " })
	void normalizeUserType_requiresAType(String requested) {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupManagerUtils.normalizeUserType(requested));
		assertEquals("Must define the user's login type", e.getMessage());
	}

	private static void addMicrosoftLogin(User user, String id) {
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setName(id);
		token.setProvider(AuthProvider.MICROSOFT);
		user.setAccessToken(token);
	}

	private static List<Object> ids(List<Map<String, Object>> rows) {
		return rows.stream().map(row -> row.get("id")).toList();
	}

	private Object memberEndDate(String userId) throws Exception {
		return UnitTestGroupTables.queryValue(securityDb,
				"SELECT ENDDATE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=? AND USERID=?", GROUP_ID, userId);
	}
}
