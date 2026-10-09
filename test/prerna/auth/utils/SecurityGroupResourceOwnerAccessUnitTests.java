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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

/**
 * Checks that only a project's or engine's owners, and admins, change which
 * groups have access to it, through {@link SecurityGroupProjectUtils} and
 * {@link SecurityGroupEngineUtils}. Each test runs once for a project and once
 * for an engine.
 */
class SecurityGroupResourceOwnerAccessUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String PROJECT = "project";
	private static final String ENGINE = "engine";

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String MICROSOFT = AuthProvider.MICROSOFT.getLabel();
	private static final String NATIVE = AuthProvider.NATIVE.getLabel();

	private static final String RESOURCE_ID = "resource";
	private static final String GROUP_ID = "team";

	private static final String PROJECT_DENIED = "Only this project's owners can change which teams have access to it.";
	private static final String ENGINE_DENIED = "Only this engine's owners can change which teams have access to it.";

	private static final int OWNER = 1;
	private static final int EDITOR = 2;
	private static final int READER = 3;

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User admin;
	private User owner;
	private User editor;
	private User reader;
	private User stranger;
	private AdminSecurityGroupUtils groups;

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		stranger = UnitTestSecurityAuthUtils.createUser("stranger", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);
		groups.addGroup(admin, GROUP_ID, CUSTOM, "the team");
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// userCanManageGroupAccess
	///

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void userCanManageGroupAccess_adminWithoutOwnershipCan(String kind) {
		createResource(kind);
		User otherAdmin = UnitTestSecurityAuthUtils.createUser("otheradmin", true);

		assertTrue(canManage(kind, otherAdmin));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void userCanManageGroupAccess_onlyOwnersAmongTheUsersWithAccess(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "ownerid", "OWNER");
		grantUser(kind, "editorid", "EDIT");
		grantUser(kind, "readerid", "READ_ONLY");

		assertTrue(canManage(kind, owner));
		assertFalse(canManage(kind, editor));
		assertFalse(canManage(kind, reader));
		assertFalse(canManage(kind, stranger));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void userCanManageGroupAccess_ownersThroughAGroupCan(String kind) throws Exception {
		createResource(kind);
		groups.addGroup(admin, "owners", CUSTOM, "owns the resource");
		groups.addUserToGroup(admin, "owners", "strangerid", NATIVE, null);
		attachAsAdmin(kind, "owners", CUSTOM, OWNER);

		assertTrue(canManage(kind, stranger));
	}

	///
	/// add, edit and remove group access
	///

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void addGroupAccess_deniesAnEditorWhoIsNotAnOwner(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "editorid", "EDIT");

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> addGroupAccess(kind, editor, GROUP_ID, "READ_ONLY"));

		assertEquals(deniedMessage(kind), e.getMessage());
		assertNull(groupPermission(kind, GROUP_ID));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void editGroupAccess_deniesAnEditorWhoIsNotAnOwner(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "editorid", "EDIT");
		attachAsAdmin(kind, GROUP_ID, CUSTOM, READER);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> editGroupAccess(kind, editor, GROUP_ID, "EDIT"));

		assertEquals(deniedMessage(kind), e.getMessage());
		assertEquals(READER, groupPermission(kind, GROUP_ID));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void removeGroupAccess_deniesAnEditorWhoIsNotAnOwner(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "editorid", "EDIT");
		attachAsAdmin(kind, GROUP_ID, CUSTOM, READER);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> removeGroupAccess(kind, editor, GROUP_ID));

		assertEquals(deniedMessage(kind), e.getMessage());
		assertEquals(READER, groupPermission(kind, GROUP_ID));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void groupAccess_deniesUsersWithoutOwnership(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "readerid", "READ_ONLY");
		attachAsAdmin(kind, GROUP_ID, CUSTOM, READER);

		assertThrows(IllegalAccessException.class, () -> addGroupAccess(kind, reader, "other", "READ_ONLY"));
		assertThrows(IllegalAccessException.class, () -> editGroupAccess(kind, stranger, GROUP_ID, "OWNER"));
		assertThrows(IllegalAccessException.class, () -> removeGroupAccess(kind, stranger, GROUP_ID));
		assertEquals(READER, groupPermission(kind, GROUP_ID));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void groupAccess_ownerAddsEditsAndRemoves(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "ownerid", "OWNER");

		addGroupAccess(kind, owner, GROUP_ID, "READ_ONLY");
		assertEquals(READER, groupPermission(kind, GROUP_ID));

		editGroupAccess(kind, owner, GROUP_ID, "OWNER");
		assertEquals(OWNER, groupPermission(kind, GROUP_ID));

		removeGroupAccess(kind, owner, GROUP_ID);
		assertNull(groupPermission(kind, GROUP_ID));
	}

	///
	/// getAvailableGroupsFor...
	///

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void availableGroups_leaveOutOnlyTheGroupsAlreadyAttached(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "ownerid", "OWNER");
		groups.addGroup(admin, "alpha", CUSTOM, "alpha custom");
		groups.addGroup(admin, "alpha", MICROSOFT, "alpha login group");
		groups.addGroup(admin, "beta", CUSTOM, "beta custom");
		attachAsAdmin(kind, "alpha", CUSTOM, READER);
		attachAsAdmin(kind, GROUP_ID, CUSTOM, READER);

		List<Map<String, Object>> available = availableGroups(kind, owner, null, 0, 0);

		assertEquals(List.of("alpha:" + MICROSOFT, "beta:" + CUSTOM), idAndTypes(available));
		assertEquals(Set.of("id", "type", "description"), available.getFirst().keySet());
		assertEquals("alpha login group", available.getFirst().get("description"));
		assertEquals(List.of("beta:" + CUSTOM), idAndTypes(availableGroups(kind, owner, "bet", 0, 0)));
		assertEquals(List.of("beta:" + CUSTOM), idAndTypes(availableGroups(kind, owner, null, 1, 1)));
	}

	@ParameterizedTest
	@ValueSource(strings = { PROJECT, ENGINE })
	void availableGroups_denyUsersWhoAreNotOwners(String kind) throws Exception {
		createResource(kind);
		grantUser(kind, "editorid", "EDIT");

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> availableGroups(kind, editor, null, 0, 0));

		assertEquals(deniedMessage(kind), e.getMessage());
		assertEquals(List.of(GROUP_ID + ":" + CUSTOM), idAndTypes(availableGroups(kind, admin, null, 0, 0)));
	}

	private void createResource(String kind) {
		if (ENGINE.equals(kind)) {
			UnitTestSecurityAuthUtils.createEngine(RESOURCE_ID, "resource name", admin);
		} else {
			UnitTestSecurityAuthUtils.createProject(RESOURCE_ID, "resource name", admin);
		}
	}

	private void grantUser(String kind, String userId, String permission) throws Exception {
		if (ENGINE.equals(kind)) {
			UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, userId, RESOURCE_ID, permission);
		} else {
			UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, RESOURCE_ID, userId, permission);
		}
	}

	private void attachAsAdmin(String kind, String groupId, String groupType, int permission) {
		if (ENGINE.equals(kind)) {
			groups.addGroupEnginePermission(admin, groupId, groupType, RESOURCE_ID, permission, null);
		} else {
			groups.addGroupProjectPermission(admin, groupId, groupType, RESOURCE_ID, permission, null);
		}
	}

	private static boolean canManage(String kind, User user) {
		return ENGINE.equals(kind) ? SecurityGroupEngineUtils.userCanManageGroupAccess(user, RESOURCE_ID)
				: SecurityGroupProjectUtils.userCanManageGroupAccess(user, RESOURCE_ID);
	}

	private static void addGroupAccess(String kind, User user, String groupId, String permission)
			throws IllegalAccessException {
		if (ENGINE.equals(kind)) {
			SecurityGroupEngineUtils.addEngineGroupPermission(user, groupId, CUSTOM, RESOURCE_ID, permission, null);
		} else {
			SecurityGroupProjectUtils.addProjectGroupPermission(user, groupId, CUSTOM, RESOURCE_ID, permission, null);
		}
	}

	private static void editGroupAccess(String kind, User user, String groupId, String permission)
			throws IllegalAccessException {
		if (ENGINE.equals(kind)) {
			SecurityGroupEngineUtils.editDatabaseGroupPermission(user, groupId, CUSTOM, RESOURCE_ID, permission, null);
		} else {
			SecurityGroupProjectUtils.editProjectGroupPermission(user, groupId, CUSTOM, RESOURCE_ID, permission, null);
		}
	}

	private static void removeGroupAccess(String kind, User user, String groupId) throws IllegalAccessException {
		if (ENGINE.equals(kind)) {
			SecurityGroupEngineUtils.removeDatabaseGroupPermission(user, groupId, CUSTOM, RESOURCE_ID);
		} else {
			SecurityGroupProjectUtils.removeProjectGroupPermission(user, groupId, CUSTOM, RESOURCE_ID);
		}
	}

	private static Integer groupPermission(String kind, String groupId) {
		return ENGINE.equals(kind) ? SecurityGroupEngineUtils.getGroupDatabasePermission(groupId, CUSTOM, RESOURCE_ID)
				: SecurityGroupProjectUtils.getGroupProjectPermission(groupId, CUSTOM, RESOURCE_ID);
	}

	private static List<Map<String, Object>> availableGroups(String kind, User user, String searchTerm, long limit,
			long offset) throws IllegalAccessException {
		return ENGINE.equals(kind)
				? SecurityGroupEngineUtils.getAvailableGroupsForEngine(user, RESOURCE_ID, searchTerm, limit, offset)
				: SecurityGroupProjectUtils.getAvailableGroupsForProject(user, RESOURCE_ID, searchTerm, limit, offset);
	}

	private static String deniedMessage(String kind) {
		return ENGINE.equals(kind) ? ENGINE_DENIED : PROJECT_DENIED;
	}

	private static List<String> idAndTypes(List<Map<String, Object>> rows) {
		return rows.stream().map(row -> row.get("id") + ":" + row.get("type")).toList();
	}
}
