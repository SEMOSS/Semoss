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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

public class SecurityGroupProjectUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

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
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void testUserGroupCanViewProjectWhileUserNotInGroup() {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// check to see if user can view project
		assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));
	}

	@Test
	public void testUserGroupCanViewProjectUserGroupNoPermission() {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// check to see if user can view project
		assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "OWNER", "EDIT", "READ_ONLY" })
	public void testUserGroupCanViewProjectUserGroupHasPermission(String permissionType) throws IllegalAccessException {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// end date within reason
		String endDate = ZonedDateTime.now().plusDays(2).toString();

		// assign permsission
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", permissionType,
				endDate);

		// check to see if user can view project
		assertTrue(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));
	}

	@Test
	public void testUserGroupCanViewProjectUserGroupHasPermissionButExpired() throws IllegalAccessException {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// end date within reason
		String endDate = ZonedDateTime.now().minusDays(2).toString();

		// assign permsission
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "1", endDate);

		// check to see if user can view project
		assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(user, "pid1"));

		// make sure that expired group permission was removed
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("pid1", "groupId1", "CUSTOM"));
	}

	@Test
	void testUserGroupCanEditProjectWhileUserNotInGroup() {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// check to see if user can view project
		assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
	}

	@Test
	public void testUserGroupCanEditProjectUserGroupNoPermission() {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// check to see if user can edit project
		assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "OWNER", "EDIT", "READ_ONLY" })
	public void testUserGroupCanEditProjectUserGroupByPermissionType(String permissionType)
			throws IllegalAccessException {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// end date within reason
		String endDate = ZonedDateTime.now().plusDays(2).toString();

		// assign permsission
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", permissionType,
				endDate);

		// check to see if user can edit project
		if (permissionType.equalsIgnoreCase("READ_ONLY")) {
			assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
		} else {
			assertTrue(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));
		}
	}

	@Test
	public void testUserGroupCanEditProjectUserGroupHasPermissionButExpired() throws IllegalAccessException {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// end date within reason
		String endDate = ZonedDateTime.now().minusDays(2).toString();

		// assign permsission
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "1", endDate);

		// check to see if user can edit project
		assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(user, "pid1"));

		// make sure that expired group permission was removed
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("pid1", "groupId1", "CUSTOM"));
	}

	@Test
	void testUserGroupIsOwnerGroupNotOwner() {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// check to see if user can view project
		assertFalse(SecurityGroupProjectUtils.userGroupIsOwner(user, "pid1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "OWNER", "EDIT", "READ_ONLY" })
	public void testUserGroupIsOnwerByPermissionType(String permissionType) throws IllegalAccessException {
		// create test user
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// create Group
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");

		// Set user token to group
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");

		// create project as user
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// end date within reason
		String endDate = ZonedDateTime.now().plusDays(2).toString();

		// assign permsission
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", permissionType,
				endDate);

		// check to see if user can edit project
		if (permissionType.equalsIgnoreCase("OWNER")) {
			assertTrue(SecurityGroupProjectUtils.userGroupIsOwner(user, "pid1"));
		} else {
			assertFalse(SecurityGroupProjectUtils.userGroupIsOwner(user, "pid1"));
		}
	}

	@Test
	void testGetBestProjectPermissionGroupBetter() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "READ_ONLY");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

		Integer permission = SecurityGroupProjectUtils.getBestProjectPermission(user2, "pid1");
		assertEquals(2, permission);
	}

	@Test
	void testGetBestProjectPermissionPersonalBetter() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate);

		Integer permission = SecurityGroupProjectUtils.getBestProjectPermission(user2, "pid1");
		assertEquals(2, permission);
	}

	@Test
	void testAddProjectGroupPermission_UserCannotEditProject() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add user to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		// try to giver user2 permissions as user2. Not allowed
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupProjectUtils
				.addProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate));
		assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
	}

	@Test
	void testAddProjectGroupPermission_GroupHasNoPermission() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate);

		// try to giver user2 permissions as user2. Not allowed
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupProjectUtils
				.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate));
		assertEquals("This group already has access to this project. Please edit the existing permission level.",
				e.getMessage());
	}

	@Test
	void testGetGroupProjectPermission() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate);

		assertEquals(1, SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1").intValue());
	}

	@Test
	void testGetGroupProjectPermission_DoesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1"));
	}

	@Test
	void testEditProjectGroupPermission_UserCannotEditProject() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add user to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		// try to giver user2 permissions as user2. Not allowed
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupProjectUtils
				.editProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate));
		assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
	}

	@Test
	void testEditProjectGroupPermission_GroupHasNoPermission() {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		// try to giver user2 permissions as user2. Not allowed
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupProjectUtils
				.editProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "OWNER", endDate));
		assertEquals(
				"Attempting to modify group project permission for a group who does not currently have access to the project",
				e.getMessage());
	}

	@Test
	void testEditProjectGroupPermission_NotHighEnoughPermissions() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add user to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

		// try to giver user2 permissions as user2. Not allowed
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupProjectUtils
				.editProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "OWNER", endDate));
		assertEquals("Cannot give owner level access to this project since you are not currently an owner.",
				e.getMessage());
	}

	@Test
	void testEditProjectGroupPermission() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add user to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

		// set to read only
		SecurityGroupProjectUtils.editProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "READ_ONLY", endDate);

		// verify read only
		Integer perm = SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1");
		assertEquals(3, perm);
	}

	@Test
	void testRemoveProjectGroupPermission_UserCannotEditProject() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add user to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		// try to giver user2 permissions as user2. Not allowed
		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityGroupProjectUtils.removeProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1"));
		assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
	}

	@Test
	void testRemoveProjectGroupPermission_GroupHasNoPermission() {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		// try to giver user2 permissions as user2. Not allowed
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupProjectUtils.removeProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1"));
		assertEquals(
				"Attempting to modify group permission for a user who does not currently have access to the project",
				e.getMessage());
	}

	@Test
	void testRemoveProjectGroupPermission() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// create second user and add user to group
		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user, "pid1", user2Id, "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

		// set to read only
		SecurityGroupProjectUtils.removeProjectGroupPermission(user2, "groupId1", "CUSTOM", "pid1");

		// verify permissions removed
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1"));
	}

	/**
	 * remove Expired Project Group Permissions Tests
	 */
	@Test
	void testExpiredRemoveProjectGroupPermission_NoPermission() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// set to read only
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupProjectUtils.removeExpiredProjectGroupPermission("groupId1", "CUSTOM", "pid1"));

		assertEquals(e.getMessage(),
				"Attempting to modify group permission for a user who does not currently have access to the project");
	}

	@Test
	void testExpiredRemoveProjectGroupPermission() throws Exception {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// minus days to go back 2 days
		String endDate = ZonedDateTime.now().minusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);

		// set to read only
		SecurityGroupProjectUtils.removeExpiredProjectGroupPermission("groupId1", "CUSTOM", "pid1");

		// verify permissions removed
		assertNull(SecurityGroupProjectUtils.getGroupProjectPermission("groupId1", "CUSTOM", "pid1"));
	}

	@Test
	void testGetAllUserGroupProjects() throws IllegalAccessException {
		// create user, group, and project
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createGroup(user, "groupId2", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId2", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createProject("pid2", "pname2", user);

		// minus days to go back 2 days
		String endDate = ZonedDateTime.now().minusDays(2).toString();
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId1", "CUSTOM", "pid1", "EDIT", endDate);
		SecurityGroupProjectUtils.addProjectGroupPermission(user, "groupId2", "CUSTOM", "pid2", "EDIT", endDate);

		List<String> projectIds = SecurityGroupProjectUtils.getAllUserGroupProjects(user);
		assertTrue(projectIds.contains("pid1"));
		assertTrue(projectIds.contains("pid2"));
	}

}
