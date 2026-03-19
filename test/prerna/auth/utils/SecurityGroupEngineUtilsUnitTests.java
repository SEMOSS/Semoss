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
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

public class SecurityGroupEngineUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

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

	@Test
	void testUserGroupCanViewEngineWhileUserNotInGroup() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertFalse(SecurityGroupEngineUtils.userGroupCanViewEngine(user, "eid1"));
	}

	@Test
	void testUserGroupCanViewEngineUserGroupNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertFalse(SecurityGroupEngineUtils.userGroupCanViewEngine(user, "eid1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "OWNER", "EDIT", "READ_ONLY" })
	void testUserGroupCanViewEngineUserGroupHasPermission(String permissionType) throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", permissionType, endDate);

		assertTrue(SecurityGroupEngineUtils.userGroupCanViewEngine(user, "eid1"));
	}

	@Test
	void testUserGroupCanViewEngineUserGroupHasPermissionButExpired() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().minusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "READ_ONLY", endDate);

		assertFalse(SecurityGroupEngineUtils.userGroupCanViewEngine(user, "eid1"));
		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1"));
	}

	@Test
	void testUserGroupCanEditEngineWhileUserNotInGroup() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertFalse(SecurityGroupEngineUtils.userGroupCanEditEngine(user, "eid1"));
	}

	@Test
	void testUserGroupCanEditEngineUserGroupNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertFalse(SecurityGroupEngineUtils.userGroupCanEditEngine(user, "eid1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "OWNER", "EDIT", "READ_ONLY" })
	void testUserGroupCanEditEngineByPermissionType(String permissionType) throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", permissionType, endDate);

		if (permissionType.equalsIgnoreCase("READ_ONLY")) {
			assertFalse(SecurityGroupEngineUtils.userGroupCanEditEngine(user, "eid1"));
		} else {
			assertTrue(SecurityGroupEngineUtils.userGroupCanEditEngine(user, "eid1"));
		}
	}

	@Test
	void testUserGroupCanEditEngineUserGroupHasPermissionButExpired() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().minusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		assertFalse(SecurityGroupEngineUtils.userGroupCanEditEngine(user, "eid1"));
		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1"));
	}

	@Test
	void testUserGroupIsOwnerGroupNotOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertFalse(SecurityGroupEngineUtils.userGroupIsOwner(user, "eid1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "OWNER", "EDIT", "READ_ONLY" })
	void testUserGroupIsOwnerByPermissionType(String permissionType) throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", permissionType, endDate);

		if (permissionType.equalsIgnoreCase("OWNER")) {
			assertTrue(SecurityGroupEngineUtils.userGroupIsOwner(user, "eid1"));
		} else {
			assertFalse(SecurityGroupEngineUtils.userGroupIsOwner(user, "eid1"));
		}
	}

	@Test
	void testGetBestDatabasePermissionGroupBetter() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "READ_ONLY");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		Integer permission = SecurityGroupEngineUtils.getBestDatabasePermission(user2, "eid1");
		assertEquals(2, permission);
	}

	@Test
	void testGetBestDatabasePermissionPersonalBetter() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "READ_ONLY", endDate);

		Integer permission = SecurityGroupEngineUtils.getBestDatabasePermission(user2, "eid1");
		assertEquals(2, permission);
	}

	@Test
	void testAddEngineGroupPermission_UserCannotEditEngine() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupEngineUtils
				.addEngineGroupPermission(user2, "groupId1", "CUSTOM", "eid1", "READ_ONLY", endDate));
		assertEquals("Insufficient privileges to modify this engine's permissions.", e.getMessage());
	}

	@Test
	void testAddEngineGroupPermission_GroupAlreadyHasPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "OWNER", endDate);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupEngineUtils
				.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "OWNER", endDate));
		assertEquals("This group already has access to this engine. Please edit the existing permission level.",
				e.getMessage());
	}

	@Test
	void testGetGroupDatabasePermission() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "OWNER", endDate);

		assertEquals(1, SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1").intValue());
	}

	@Test
	void testGetGroupDatabasePermission_DoesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1"));
	}

	@Test
	void testEditDatabaseGroupPermission_UserCannotEditEngine() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupEngineUtils
				.editDatabaseGroupPermission(user2, "groupId1", "CUSTOM", "eid1", "READ_ONLY", endDate));
		assertEquals("Insufficient privileges to modify this database's permissions.", e.getMessage());
	}

	@Test
	void testEditDatabaseGroupPermission_GroupHasNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupEngineUtils
				.editDatabaseGroupPermission(user, "groupId1", "CUSTOM", "eid1", "OWNER", endDate));
		assertEquals(
				"Attempting to modify database permission for a group who does not currently have access to the database",
				e.getMessage());
	}

	@Test
	void testEditDatabaseGroupPermission_NotHighEnoughPermissions() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupEngineUtils
				.editDatabaseGroupPermission(user2, "groupId1", "CUSTOM", "eid1", "OWNER", endDate));
		assertEquals("Cannot give owner level access to this database since you are not currently an owner.",
				e.getMessage());
	}

	@Test
	void testEditDatabaseGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		SecurityGroupEngineUtils.editDatabaseGroupPermission(user2, "groupId1", "CUSTOM", "eid1", "READ_ONLY", endDate);

		Integer perm = SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1");
		assertEquals(3, perm);
	}

	@Test
	void testRemoveDatabaseGroupPermission_UserCannotEditEngine() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityGroupEngineUtils.removeDatabaseGroupPermission(user2, "groupId1", "CUSTOM", "eid1"));
		assertEquals("Insufficient privileges to modify this database's permissions.", e.getMessage());
	}

	@Test
	void testRemoveDatabaseGroupPermission_GroupHasNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupEngineUtils.removeDatabaseGroupPermission(user, "groupId1", "CUSTOM", "eid1"));
		assertEquals(
				"Attempting to modify group permission for a user who does not currently have access to the database",
				e.getMessage());
	}

	@Test
	void testRemoveDatabaseGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		SecurityGroupEngineUtils.removeDatabaseGroupPermission(user2, "groupId1", "CUSTOM", "eid1");

		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1"));
	}

	@Test
	void testRemoveExpiredEngineGroupPermission_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupEngineUtils.removeExpiredEngineGroupPermission("groupId1", "CUSTOM", "eid1"));
		assertEquals(
				"Attempting to modify group permission for a user who does not currently have access to the database",
				e.getMessage());
	}

	@Test
	void testRemoveExpiredEngineGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().minusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		SecurityGroupEngineUtils.removeExpiredEngineGroupPermission("groupId1", "CUSTOM", "eid1");

		assertNull(SecurityGroupEngineUtils.getGroupDatabasePermission("groupId1", "CUSTOM", "eid1"));
	}

	@Test
	void testGetAllUserGroupDatabases() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createGroup(user, "groupId2", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId2", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);
		UnitTestSecurityAuthUtils.createEngine("eid2", "ename2", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId2", "CUSTOM", "eid2", "EDIT", endDate);

		List<String> engineIds = SecurityGroupEngineUtils.getAllUserGroupDatabases(user);
		assertTrue(engineIds.contains("eid1"));
		assertTrue(engineIds.contains("eid2"));
	}

	@Test
	void testGetGroupsWithAccessToEngine_NullEngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user, null, 0, 0));
		assertEquals("Input engineId must not be null or blank", e.getMessage());
	}

	@Test
	void testGetGroupsWithAccessToEngine_BlankEngineId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user, "   ", 0, 0));
		assertEquals("Input engineId must not be null or blank", e.getMessage());
	}

	@Test
	void testGetGroupsWithAccessToEngine_UserCannotView() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user2, "eid1", 0, 0));
		assertEquals("The user does not have access to view this engine", e.getMessage());
	}

	@Test
	void testGetGroupsWithAccessToEngine() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		List<Map<String, Object>> groups = SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user, "eid1", 0, 0);
		assertEquals(1, groups.size());
		assertEquals("groupId1", groups.get(0).get("ID"));
		assertEquals("CUSTOM", groups.get(0).get("TYPE"));
		assertEquals(2, groups.get(0).get("PERMISSION"));
	}

	@Test
	void testGetGroupsWithAccessToEngine_WithLimitAndOffset() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupA", "CUSTOM");
		UnitTestSecurityAuthUtils.createGroup(user, "groupB", "CUSTOM");
		UnitTestSecurityAuthUtils.createGroup(user, "groupC", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupA", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupA", "CUSTOM", "eid1", "EDIT", endDate);
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupB", "CUSTOM", "eid1", "READ_ONLY", endDate);
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupC", "CUSTOM", "eid1", "OWNER", endDate);

		List<Map<String, Object>> groups = SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user, "eid1", 2, 0);
		assertEquals(2, groups.size());

		groups = SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user, "eid1", 2, 1);
		assertEquals(2, groups.size());
	}
}
