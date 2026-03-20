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

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

public class SecurityGroupInsightsUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

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
	/// userGroupCanViewInsight
	///

	@Test
	void testUserGroupCanViewInsightWhileUserNotInGroup() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertFalse(SecurityGroupInsightsUtils.userGroupCanViewInsight(user, "pid1", "iid1"));
	}

	@Test
	void testUserGroupCanViewInsightUserGroupNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertFalse(SecurityGroupInsightsUtils.userGroupCanViewInsight(user, "pid1", "iid1"));
	}

	///
	/// userGroupCanEditInsight
	///

	@Test
	void testUserGroupCanEditInsightWhileUserNotInGroup() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertFalse(SecurityGroupInsightsUtils.userGroupCanEditInsight(user, "pid1", "iid1"));
	}

	@Test
	void testUserGroupCanEditInsightUserGroupNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertFalse(SecurityGroupInsightsUtils.userGroupCanEditInsight(user, "pid1", "iid1"));
	}

	///
	/// userGroupIsOwner
	///

	@Test
	void testUserGroupIsOwnerGroupNotOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertFalse(SecurityGroupInsightsUtils.userGroupIsOwner(user, "pid1", "iid1"));
	}

	///
	/// getBestInsightPermission
	///

	@Test
	void testGetBestInsightPermissionGroupBetter() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		// Give user2 READ_ONLY permission directly
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		// Give group EDIT permission
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "EDIT",
				endDate);

		Integer permission = SecurityGroupInsightsUtils.getBestInsightPermission(user2, "pid1", "iid1");
		assertEquals(2, permission); // EDIT is better than READ_ONLY
	}

	@Test
	void testGetBestInsightPermissionPersonalBetter() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		// Give user2 EDIT permission directly
		List<Map<String, String>> user2Permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", user2Permissions, null);

		// Give group READ_ONLY permission
		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "READ_ONLY",
				endDate);

		Integer permission = SecurityGroupInsightsUtils.getBestInsightPermission(user2, "pid1", "iid1");
		assertEquals(2, permission); // EDIT is better than READ_ONLY
	}

	@Test
	void testGetBestInsightPermissionNoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		Integer permission = SecurityGroupInsightsUtils.getBestInsightPermission(user2, "pid1", "iid1");
		assertNull(permission);
	}

	///
	/// addInsightGroupPermission
	///

	@Test
	void testAddInsightGroupPermission_UserCannotEditInsight() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupInsightsUtils
				.addInsightGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "iid1", "READ_ONLY", endDate));
		assertEquals("Insufficient privileges to modify this insight's permissions.", e.getMessage());
	}

	@Test
	void testAddInsightGroupPermission_GroupAlreadyHasPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "OWNER",
				endDate);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupInsightsUtils
				.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "OWNER", endDate));
		assertEquals("This group already has access to this insight. Please edit the existing permission level.",
				e.getMessage());
	}

	///
	/// getGroupInsightPermission
	///

	@Test
	void testGetGroupInsightPermission() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "OWNER",
				endDate);

		assertEquals(1,
				SecurityGroupInsightsUtils.getGroupInsightPermission("groupId1", "CUSTOM", "pid1", "iid1").intValue());
	}

	@Test
	void testGetGroupInsightPermission_DoesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertNull(SecurityGroupInsightsUtils.getGroupInsightPermission("groupId1", "CUSTOM", "pid1", "iid1"));
	}

	///
	/// editInsightGroupPermission
	///

	@Test
	void testEditInsightGroupPermission_UserCannotEditInsight() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupInsightsUtils
				.editInsightGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "iid1", "READ_ONLY", endDate));
		assertEquals("Insufficient privileges to modify this insight's permissions.", e.getMessage());
	}

	@Test
	void testEditInsightGroupPermission_GroupHasNoPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		// Give user direct insight permission so they can attempt the edit
		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> userPermissions = List.of(Map.of("userid", userId, "permission", "OWNER"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", userPermissions, null);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupInsightsUtils
				.editInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "OWNER", endDate));
		assertEquals(
				"Attempting to modify insight permission for a group who does not currently have access to the insight",
				e.getMessage());
	}

	@Test
	void testEditInsightGroupPermission_NotHighEnoughPermissions() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		// Give user2 EDIT permission
		List<Map<String, String>> user2Permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", user2Permissions, null);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "EDIT",
				endDate);

		// User2 tries to grant OWNER permission
		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupInsightsUtils
				.editInsightGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "iid1", "OWNER", endDate));
		assertEquals("Cannot give owner level access to this insight since you are not currently an owner.",
				e.getMessage());
	}

	@Test
	void testEditInsightGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		// Give user2 EDIT permission
		List<Map<String, String>> user2Permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", user2Permissions, null);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "EDIT",
				endDate);

		// User2 changes group permission to READ_ONLY
		SecurityGroupInsightsUtils.editInsightGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "iid1", "READ_ONLY",
				endDate);

		Integer perm = SecurityGroupInsightsUtils.getGroupInsightPermission("groupId1", "CUSTOM", "pid1", "iid1");
		assertEquals(3, perm); // READ_ONLY = 3
	}

	///
	/// removeInsightGroupPermission
	///

	@Test
	void testRemoveInsightGroupPermission_UserCannotEditInsight() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		IllegalAccessException e = assertThrows(IllegalAccessException.class, () -> SecurityGroupInsightsUtils
				.removeInsightGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "iid1"));
		assertEquals("Insufficient privileges to modify this insight's permissions.", e.getMessage());
	}

	@Test
	void testRemoveInsightGroupPermission_GroupHasNoPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		// Give user direct insight permission so they can attempt the removal
		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> userPermissions = List.of(Map.of("userid", userId, "permission", "OWNER"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", userPermissions, null);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityGroupInsightsUtils.removeInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1"));
		assertEquals(
				"Attempting to modify group permission for a user who does not currently have access to the insight",
				e.getMessage());
	}

	@Test
	void testRemoveInsightGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user2, "groupId1", "CUSTOM");
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		String user2Type = user2.getPrimaryLogin().getLabel();
		UnitTestSecurityAuthUtils.addUserToGroup(user, "groupId1", user2Id, user2Type);

		// Give user2 EDIT permission
		List<Map<String, String>> user2Permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", user2Permissions, null);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "EDIT",
				endDate);

		SecurityGroupInsightsUtils.removeInsightGroupPermission(user2, "groupId1", "CUSTOM", "pid1", "iid1");

		assertNull(SecurityGroupInsightsUtils.getGroupInsightPermission("groupId1", "CUSTOM", "pid1", "iid1"));
	}

	///
	/// removeExpiredInsightGroupPermission
	///

	@Test
	void testRemoveExpiredInsightGroupPermission_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> SecurityGroupInsightsUtils
				.removeExpiredInsightGroupPermission("groupId1", "CUSTOM", "pid1", "iid1"));
		assertEquals(
				"Attempting to modify group permission for a user who does not currently have access to the insight",
				e.getMessage());
	}

	@Test
	void testRemoveExpiredInsightGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		// Use a past date for expiration
		String endDate = ZonedDateTime.now().minusDays(2).toString();
		SecurityGroupInsightsUtils.addInsightGroupPermission(user, "groupId1", "CUSTOM", "pid1", "iid1", "EDIT",
				endDate);

		SecurityGroupInsightsUtils.removeExpiredInsightGroupPermission("groupId1", "CUSTOM", "pid1", "iid1");

		assertNull(SecurityGroupInsightsUtils.getGroupInsightPermission("groupId1", "CUSTOM", "pid1", "iid1"));
	}
}
