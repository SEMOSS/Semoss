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

public class SecurityUserInsightUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

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
	/// getActualUserInsightPermission
	///

	@Test
	void testGetActualUserInsightPermission_ProjectOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		// Project owner should have OWNER permission on all insights
		String permission = SecurityUserInsightUtils.getActualUserInsightPermission(user, "pid1", "iid1");
		assertEquals("OWNER", permission);
	}

	@Test
	void testGetActualUserInsightPermission_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		String permission = SecurityUserInsightUtils.getActualUserInsightPermission(user2, "pid1", "iid1");
		assertNull(permission);
	}

	@Test
	void testGetActualUserInsightPermission_DirectPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 EDIT permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		String permission = SecurityUserInsightUtils.getActualUserInsightPermission(user2, "pid1", "iid1");
		assertEquals("EDIT", permission);
	}

	///
	/// userCanViewInsight
	///

	@Test
	void testUserCanViewInsight_WithPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 READ_ONLY permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));
	}

	@Test
	void testUserCanViewInsight_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));
	}

	@Test
	void testUserCanViewInsight_OwnerPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 OWNER permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "OWNER"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));
	}

	///
	/// userCanEditInsight
	///

	@Test
	void testUserCanEditInsight_OwnerPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 OWNER permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "OWNER"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertTrue(SecurityUserInsightUtils.userCanEditInsight(user2, "pid1", "iid1"));
	}

	@Test
	void testUserCanEditInsight_EditPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 EDIT permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertTrue(SecurityUserInsightUtils.userCanEditInsight(user2, "pid1", "iid1"));
	}

	@Test
	void testUserCanEditInsight_ReadOnlyPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 READ_ONLY permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertFalse(SecurityUserInsightUtils.userCanEditInsight(user2, "pid1", "iid1"));
	}

	@Test
	void testUserCanEditInsight_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserInsightUtils.userCanEditInsight(user2, "pid1", "iid1"));
	}

	///
	/// userIsInsightOwner
	///

	@Test
	void testUserIsInsightOwner_True() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 OWNER permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "OWNER"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertTrue(SecurityUserInsightUtils.userIsInsightOwner(user2, "pid1", "iid1"));
	}

	@Test
	void testUserIsInsightOwner_False_EditPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 EDIT permission
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertFalse(SecurityUserInsightUtils.userIsInsightOwner(user2, "pid1", "iid1"));
	}

	@Test
	void testUserIsInsightOwner_False_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserInsightUtils.userIsInsightOwner(user2, "pid1", "iid1"));
	}

	///
	/// insightPermissionIsExpired
	///

	@Test
	void testInsightPermissionIsExpired_NotExpired() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		// Give user2 permission without end date
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		assertFalse(SecurityUserInsightUtils.insightPermissionIsExpired(user2Id, "pid1", "iid1"));
	}

	@Test
	void testInsightPermissionIsExpired_NoPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		assertFalse(SecurityUserInsightUtils.insightPermissionIsExpired("nonexistent", "pid1", "iid1"));
	}

	///
	/// getInsightUsers
	///

	@Test
	void testGetInsightUsers_UserCannotView() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityUserInsightUtils.getInsightUsers(user2, "pid1", "iid1", null, null, -1, -1));
		assertEquals("The user does not have access to view this insight", e.getMessage());
	}

	@Test
	void testGetInsightUsers_NoFilters() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		// user (project owner) can view the insight
		List<Map<String, Object>> users = SecurityUserInsightUtils.getInsightUsers(user, "pid1", "iid1", null, null, -1,
				-1);
		assertNotNull(users);
		assertEquals(1, users.size()); // Only user2 has direct insight permission
	}

	@Test
	void testGetInsightUsers_WithSearchTerm() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions2 = List.of(Map.of("userid", user3Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions2, null);

		List<Map<String, Object>> users = SecurityUserInsightUtils.getInsightUsers(user, "pid1", "iid1", "editor", null,
				-1, -1);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetInsightUsers_WithPermissionFilter() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions2 = List.of(Map.of("userid", user3Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions2, null);

		List<Map<String, Object>> users = SecurityUserInsightUtils.getInsightUsers(user, "pid1", "iid1", null, "EDIT",
				-1, -1);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetInsightUsers_WithLimit() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions2 = List.of(Map.of("userid", user3Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions2, null);

		List<Map<String, Object>> users = SecurityUserInsightUtils.getInsightUsers(user, "pid1", "iid1", null, null, 1,
				0);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	///
	/// getInsightUsersCount
	///

	@Test
	void testGetInsightUsersCount_UserCannotView() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityUserInsightUtils.getInsightUsersCount(user2, "pid1", "iid1", null, null));
		assertEquals("The user does not have access to view this insight", e.getMessage());
	}

	@Test
	void testGetInsightUsersCount_NoFilters() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions2 = List.of(Map.of("userid", user3Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions2, null);

		long count = SecurityUserInsightUtils.getInsightUsersCount(user, "pid1", "iid1", null, null);
		assertEquals(2, count);
	}

	@Test
	void testGetInsightUsersCount_WithPermissionFilter() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "EDIT"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions2 = List.of(Map.of("userid", user3Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions2, null);

		long count = SecurityUserInsightUtils.getInsightUsersCount(user, "pid1", "iid1", null, "EDIT");
		assertEquals(1, count);
	}

	///
	/// setInsightFavorite
	///

	@Test
	void testSetInsightFavorite_UserCannotModify() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityUserInsightUtils.setInsightFavorite(user2, "pid1", "iid1", true));
		assertEquals("The user doesn't have the permission to modify this insight", e.getMessage());
	}

	@Test
	void testSetInsightFavorite_WithViewPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		User user2 = UnitTestSecurityAuthUtils.createUser("viewer", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		// Should not throw exception - user has view permission
		SecurityUserInsightUtils.setInsightFavorite(user2, "pid1", "iid1", true);
	}

	///
	/// deleteInsight
	///

	@Test
	void testDeleteInsight_Single() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");

		// Give user permission so we can verify via user permission
		User user2 = UnitTestSecurityAuthUtils.createUser("viewer", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> permissions = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", permissions, null);

		// Verify user has permission before deletion
		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));

		SecurityUserInsightUtils.deleteInsight("pid1", "iid1");

		// Verify user no longer has permission after deletion
		assertFalse(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));
	}

	@Test
	void testDeleteInsight_Multiple() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid1", "insight1", "grid");
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid2", "insight2", "grid");
		UnitTestSecurityAuthUtils.createInsight("pid1", "iid3", "insight3", "grid");

		// Give user permission so we can verify via user permission
		User user2 = UnitTestSecurityAuthUtils.createUser("viewer", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		List<Map<String, String>> perm1 = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid1", perm1, null);
		List<Map<String, String>> perm2 = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid2", perm2, null);
		List<Map<String, String>> perm3 = List.of(Map.of("userid", user2Id, "permission", "READ_ONLY"));
		SecurityInsightUtils.addInsightUserPermissions(user, "pid1", "iid3", perm3, null);

		// Verify user has permission before deletion
		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));
		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid2"));
		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid3"));

		SecurityUserInsightUtils.deleteInsight("pid1", "iid1", "iid2");

		// Verify deleted insights permissions are gone
		assertFalse(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid1"));
		assertFalse(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid2"));
		// Verify iid3 still has permissions
		assertTrue(SecurityUserInsightUtils.userCanViewInsight(user2, "pid1", "iid3"));
	}
}
