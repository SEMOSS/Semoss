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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

public class SecurityUserProjectUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;

	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// getFullUserProjectIds
	///

	@Test
	void testGetFullUserProjectIds_WithProjects() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", user);

		List<String> projectIds = SecurityUserProjectUtils.getFullUserProjectIds(user);
		assertNotNull(projectIds);
		assertTrue(projectIds.contains("pid1"));
		assertTrue(projectIds.contains("pid2"));
	}

	@Test
	void testGetFullUserProjectIds_NoProjects() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		List<String> projectIds = SecurityUserProjectUtils.getFullUserProjectIds(user);
		assertNotNull(projectIds);
		assertTrue(projectIds.isEmpty());
	}

	///
	/// getActualUserProjectPermission
	///

	@Test
	void testGetActualUserProjectPermission_Owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String permission = SecurityUserProjectUtils.getActualUserProjectPermission(user, "pid1");
		assertEquals("OWNER", permission);
	}

	@Test
	void testGetActualUserProjectPermission_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		String permission = SecurityUserProjectUtils.getActualUserProjectPermission(user2, "pid1");
		assertNull(permission);
	}

	@Test
	void testGetActualUserProjectPermission_GlobalProject() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		SecurityProjectUtils.setProjectGlobal(user, "pid1", true);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		String permission = SecurityUserProjectUtils.getActualUserProjectPermission(user2, "pid1");
		assertEquals("READ_ONLY", permission);
	}

	@Test
	void testGetActualUserProjectPermission_Edit() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		String permission = SecurityUserProjectUtils.getActualUserProjectPermission(user2, "pid1");
		assertEquals("EDIT", permission);
	}

	///
	/// getActualGroupUserProjectPermission
	///

	@Test
	void testGetActualGroupUserProjectPermission_NoGroups() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		List<String> permissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user, "pid1");
		assertTrue(permissions.isEmpty());
	}

	@Test
	void testGetActualGroupUserProjectPermission_WithGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

		List<String> permissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user, "pid1");
		assertEquals(1, permissions.size());
		assertEquals("EDIT", permissions.get(0));
	}

	@Test
	void testGetActualGroupUserProjectPermission_GlobalProject() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		SecurityProjectUtils.setProjectGlobal(user, "pid1", true);

		List<String> permissions = SecurityUserProjectUtils.getActualGroupUserProjectPermission(user, "pid1");
		assertEquals(1, permissions.size());
		assertEquals("READ_ONLY", permissions.get(0));
	}

	///
	/// getHighestProjectPermission
	///

	@Test
	void testGetHighestProjectPermission_UserOnly() {
		String highest = SecurityUserProjectUtils.getHighestProjectPermission("EDIT", null);
		assertEquals("EDIT", highest);
	}

	@Test
	void testGetHighestProjectPermission_GroupOnly() {
		String highest = SecurityUserProjectUtils.getHighestProjectPermission(null, Arrays.asList("READ_ONLY", "EDIT"));
		assertEquals("EDIT", highest);
	}

	@Test
	void testGetHighestProjectPermission_UserHigher() {
		String highest = SecurityUserProjectUtils.getHighestProjectPermission("OWNER",
				Arrays.asList("READ_ONLY", "EDIT"));
		assertEquals("OWNER", highest);
	}

	@Test
	void testGetHighestProjectPermission_GroupHigher() {
		String highest = SecurityUserProjectUtils.getHighestProjectPermission("READ_ONLY", Arrays.asList("OWNER"));
		assertEquals("OWNER", highest);
	}

	@Test
	void testGetHighestProjectPermission_BothNull() {
		String highest = SecurityUserProjectUtils.getHighestProjectPermission(null, null);
		assertNull(highest);
	}

	@Test
	void testGetHighestProjectPermission_EmptyGroup() {
		String highest = SecurityUserProjectUtils.getHighestProjectPermission("EDIT", new ArrayList<>());
		assertEquals("EDIT", highest);
	}

	///
	/// getUserProjectPermission
	///

	@Test
	void testGetUserProjectPermission_ByUserId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		Integer permission = SecurityUserProjectUtils.getUserProjectPermission(userId, "pid1");
		assertNotNull(permission);
		assertEquals(1, permission); // OWNER = 1
	}

	@Test
	void testGetUserProjectPermission_ByUserId_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		Integer permission = SecurityUserProjectUtils.getUserProjectPermission("nonexistent", "pid1");
		assertNull(permission);
	}

	@Test
	void testGetUserProjectPermission_ByUser() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		Integer permission = SecurityUserProjectUtils.getUserProjectPermission(user, "pid1");
		assertNotNull(permission);
		assertEquals(1, permission); // OWNER = 1
	}

	@Test
	void testGetUserProjectPermission_ByUser_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		Integer permission = SecurityUserProjectUtils.getUserProjectPermission(user2, "pid1");
		assertNull(permission);
	}

	///
	/// userIsOwner
	///

	@Test
	void testUserIsOwner_True() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		assertTrue(SecurityUserProjectUtils.userIsOwner(user, "pid1"));
	}

	@Test
	void testUserIsOwner_False_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		assertFalse(SecurityUserProjectUtils.userIsOwner(user2, "pid1"));
	}

	@Test
	void testUserIsOwner_False_EditPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		assertFalse(SecurityUserProjectUtils.userIsOwner(user2, "pid1"));
	}

	///
	/// userCanViewProject
	///

	@Test
	void testUserCanViewProject_Owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		assertTrue(SecurityUserProjectUtils.userCanViewProject(user, "pid1"));
	}

	@Test
	void testUserCanViewProject_Edit() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		assertTrue(SecurityUserProjectUtils.userCanViewProject(user2, "pid1"));
	}

	@Test
	void testUserCanViewProject_ReadOnly() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "READ_ONLY");

		assertTrue(SecurityUserProjectUtils.userCanViewProject(user2, "pid1"));
	}

	@Test
	void testUserCanViewProject_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserProjectUtils.userCanViewProject(user2, "pid1"));
	}

	@Test
	void testUserCanViewProject_GlobalProject() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		SecurityProjectUtils.setProjectGlobal(user, "pid1", true);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertTrue(SecurityUserProjectUtils.userCanViewProject(user2, "pid1"));
	}

	///
	/// userCanEditProject
	///

	@Test
	void testUserCanEditProject_Owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		assertTrue(SecurityUserProjectUtils.userCanEditProject(user, "pid1"));
	}

	@Test
	void testUserCanEditProject_Edit() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		assertTrue(SecurityUserProjectUtils.userCanEditProject(user2, "pid1"));
	}

	@Test
	void testUserCanEditProject_ReadOnly() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "READ_ONLY");

		assertFalse(SecurityUserProjectUtils.userCanEditProject(user2, "pid1"));
	}

	@Test
	void testUserCanEditProject_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserProjectUtils.userCanEditProject(user2, "pid1"));
	}

	///
	/// checkUserHasAccessToProject
	///

	@Test
	void testCheckUserHasAccessToProject_True() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		assertTrue(SecurityUserProjectUtils.checkUserHasAccessToProject("pid1", userId));
	}

	@Test
	void testCheckUserHasAccessToProject_False() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		assertFalse(SecurityUserProjectUtils.checkUserHasAccessToProject("pid1", user2Id));
	}

	///
	/// getProjectUsers
	///

	@Test
	void testGetProjectUsers_NoFilters() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		List<Map<String, Object>> users = SecurityUserProjectUtils.getProjectUsers("pid1", null, null, -1, -1);
		assertNotNull(users);
		assertEquals(2, users.size()); // Owner + editor
	}

	@Test
	void testGetProjectUsers_WithSearchParam() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		List<Map<String, Object>> users = SecurityUserProjectUtils.getProjectUsers("pid1", "admin", null, -1, -1);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetProjectUsers_WithPermissionFilter() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		List<Map<String, Object>> users = SecurityUserProjectUtils.getProjectUsers("pid1", null, "OWNER", -1, -1);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetProjectUsers_WithLimit() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user3Id, "READ_ONLY");

		List<Map<String, Object>> users = SecurityUserProjectUtils.getProjectUsers("pid1", null, null, 2, 0);
		assertNotNull(users);
		assertEquals(2, users.size());
	}

	@Test
	void testGetProjectUsers_WithOffset() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user3Id, "READ_ONLY");

		List<Map<String, Object>> users = SecurityUserProjectUtils.getProjectUsers("pid1", null, null, 10, 1);
		assertNotNull(users);
		assertEquals(2, users.size()); // 3 total, offset 1, so 2 remaining
	}

	///
	/// projectPermissionIsExpired
	///

	@Test
	void testProjectPermissionIsExpired_NotExpired() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		// Owner permission typically has no end date
		assertFalse(SecurityUserProjectUtils.projectPermissionIsExpired(userId, "pid1"));
	}

	@Test
	void testProjectPermissionIsExpired_NoPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		assertFalse(SecurityUserProjectUtils.projectPermissionIsExpired("nonexistent", "pid1"));
	}
}
