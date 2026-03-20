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

public class SecurityUserEngineUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

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
	/// getActualUserEnginePermission
	///

	@Test
	void testGetActualUserEnginePermission_Owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String permission = SecurityUserEngineUtils.getActualUserEnginePermission(user, "eid1");
		assertEquals("OWNER", permission);
	}

	@Test
	void testGetActualUserEnginePermission_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		String permission = SecurityUserEngineUtils.getActualUserEnginePermission(user2, "eid1");
		assertNull(permission);
	}

	@Test
	void testGetActualUserEnginePermission_GlobalEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngineGlobal("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		String permission = SecurityUserEngineUtils.getActualUserEnginePermission(user2, "eid1");
		assertEquals("READ_ONLY", permission);
	}

	@Test
	void testGetActualUserEnginePermission_Edit() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		String permission = SecurityUserEngineUtils.getActualUserEnginePermission(user2, "eid1");
		assertEquals("EDIT", permission);
	}

	@Test
	void testGetActualUserEnginePermission_ReadOnly() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "READ_ONLY");

		String permission = SecurityUserEngineUtils.getActualUserEnginePermission(user2, "eid1");
		assertEquals("READ_ONLY", permission);
	}

	///
	/// getActualGroupUserEnginePermission
	///

	@Test
	void testGetActualGroupUserEnginePermission_NoGroups() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		List<String> permissions = SecurityUserEngineUtils.getActualGroupUserEnginePermission(user, "eid1");
		assertTrue(permissions.isEmpty());
	}

	@Test
	void testGetActualGroupUserEnginePermission_WithGroupPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String endDate = ZonedDateTime.now().plusDays(2).toString();
		SecurityGroupEngineUtils.addEngineGroupPermission(user, "groupId1", "CUSTOM", "eid1", "EDIT", endDate);

		List<String> permissions = SecurityUserEngineUtils.getActualGroupUserEnginePermission(user, "eid1");
		assertEquals(1, permissions.size());
		assertEquals("EDIT", permissions.get(0));
	}

	@Test
	void testGetActualGroupUserEnginePermission_GlobalEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.addUserTokenToGroup(user, "groupId1", "CUSTOM");
		UnitTestSecurityAuthUtils.createEngineGlobal("eid1", "ename1", user);

		List<String> permissions = SecurityUserEngineUtils.getActualGroupUserEnginePermission(user, "eid1");
		assertEquals(1, permissions.size());
		assertEquals("READ_ONLY", permissions.get(0));
	}

	///
	/// getHighestEnginePermission
	///

	@Test
	void testGetHighestEnginePermission_UserOnly() {
		String highest = SecurityUserEngineUtils.getHighestEnginePermission("EDIT", null);
		assertEquals("EDIT", highest);
	}

	@Test
	void testGetHighestEnginePermission_GroupOnly() {
		String highest = SecurityUserEngineUtils.getHighestEnginePermission(null, Arrays.asList("READ_ONLY", "EDIT"));
		assertEquals("EDIT", highest);
	}

	@Test
	void testGetHighestEnginePermission_UserHigher() {
		String highest = SecurityUserEngineUtils.getHighestEnginePermission("OWNER", Arrays.asList("READ_ONLY", "EDIT"));
		assertEquals("OWNER", highest);
	}

	@Test
	void testGetHighestEnginePermission_GroupHigher() {
		String highest = SecurityUserEngineUtils.getHighestEnginePermission("READ_ONLY", Arrays.asList("OWNER"));
		assertEquals("OWNER", highest);
	}

	@Test
	void testGetHighestEnginePermission_BothNull() {
		String highest = SecurityUserEngineUtils.getHighestEnginePermission(null, null);
		assertNull(highest);
	}

	@Test
	void testGetHighestEnginePermission_EmptyGroup() {
		String highest = SecurityUserEngineUtils.getHighestEnginePermission("EDIT", new ArrayList<>());
		assertEquals("EDIT", highest);
	}

	///
	/// getUserEnginePermission
	///

	@Test
	void testGetUserEnginePermission_ByUserId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		Integer permission = SecurityUserEngineUtils.getUserEnginePermission(userId, "eid1");
		assertNotNull(permission);
		assertEquals(1, permission); // OWNER = 1
	}

	@Test
	void testGetUserEnginePermission_ByUserId_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		Integer permission = SecurityUserEngineUtils.getUserEnginePermission("nonexistent", "eid1");
		assertNull(permission);
	}

	@Test
	void testGetUserEnginePermission_ByUser() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		Integer permission = SecurityUserEngineUtils.getUserEnginePermission(user, "eid1");
		assertNotNull(permission);
		assertEquals(1, permission); // OWNER = 1
	}

	@Test
	void testGetUserEnginePermission_ByUser_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		Integer permission = SecurityUserEngineUtils.getUserEnginePermission(user2, "eid1");
		assertNull(permission);
	}

	///
	/// userIsOwner
	///

	@Test
	void testUserIsOwner_True() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertTrue(SecurityUserEngineUtils.userIsOwner(user, "eid1"));
	}

	@Test
	void testUserIsOwner_False_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		assertFalse(SecurityUserEngineUtils.userIsOwner(user2, "eid1"));
	}

	@Test
	void testUserIsOwner_False_EditPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		assertFalse(SecurityUserEngineUtils.userIsOwner(user2, "eid1"));
	}

	///
	/// userCanViewEngine
	///

	@Test
	void testUserCanViewEngine_Owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertTrue(SecurityUserEngineUtils.userCanViewEngine(user, "eid1"));
	}

	@Test
	void testUserCanViewEngine_Edit() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		assertTrue(SecurityUserEngineUtils.userCanViewEngine(user2, "eid1"));
	}

	@Test
	void testUserCanViewEngine_ReadOnly() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "READ_ONLY");

		assertTrue(SecurityUserEngineUtils.userCanViewEngine(user2, "eid1"));
	}

	@Test
	void testUserCanViewEngine_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserEngineUtils.userCanViewEngine(user2, "eid1"));
	}

	@Test
	void testUserCanViewEngine_GlobalEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngineGlobal("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertTrue(SecurityUserEngineUtils.userCanViewEngine(user2, "eid1"));
	}

	///
	/// userCanEditEngine
	///

	@Test
	void testUserCanEditEngine_Owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertTrue(SecurityUserEngineUtils.userCanEditEngine(user, "eid1"));
	}

	@Test
	void testUserCanEditEngine_Edit() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		assertTrue(SecurityUserEngineUtils.userCanEditEngine(user2, "eid1"));
	}

	@Test
	void testUserCanEditEngine_ReadOnly() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "READ_ONLY");

		assertFalse(SecurityUserEngineUtils.userCanEditEngine(user2, "eid1"));
	}

	@Test
	void testUserCanEditEngine_NoPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);

		assertFalse(SecurityUserEngineUtils.userCanEditEngine(user2, "eid1"));
	}

	///
	/// enginePermissionIsExpired
	///

	@Test
	void testEnginePermissionIsExpired_NotExpired() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		// Owner permission typically has no end date
		assertFalse(SecurityUserEngineUtils.enginePermissionIsExpired(userId, "eid1"));
	}

	@Test
	void testEnginePermissionIsExpired_NoPermission() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		assertFalse(SecurityUserEngineUtils.enginePermissionIsExpired("nonexistent", "eid1"));
	}

	///
	/// checkUserHasAccessToEngine
	///

	@Test
	void testCheckUserHasAccessToEngine_True() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		assertTrue(SecurityUserEngineUtils.checkUserHasAccessToEngine("eid1", userId));
	}

	@Test
	void testCheckUserHasAccessToEngine_False() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("notadmin", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();

		assertFalse(SecurityUserEngineUtils.checkUserHasAccessToEngine("eid1", user2Id));
	}

	///
	/// getUserEnginePermissions
	///

	@Test
	void testGetUserEnginePermissions_MultipleUsers() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user3Id, "eid1", "READ_ONLY");

		String userId = user.getAccessToken(AuthProvider.NATIVE).getId();
		List<String> userIds = Arrays.asList(userId, user2Id, user3Id);

		Map<String, Integer> permissions = SecurityUserEngineUtils.getUserEnginePermissions(userIds, "eid1");
		assertEquals(3, permissions.size());
		assertEquals(1, permissions.get(userId)); // OWNER
		assertEquals(2, permissions.get(user2Id)); // EDIT
		assertEquals(3, permissions.get(user3Id)); // READ_ONLY
	}

	@Test
	void testGetUserEnginePermissions_EmptyList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		Map<String, Integer> permissions = SecurityUserEngineUtils.getUserEnginePermissions(new ArrayList<>(), "eid1");
		assertTrue(permissions.isEmpty());
	}

	///
	/// getDisplayEngineOwnersAndEditors
	///

	@Test
	void testGetDisplayEngineOwnersAndEditors_PrivateEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		List<Map<String, Object>> users = SecurityUserEngineUtils.getDisplayEngineOwnersAndEditors("eid1");
		assertNotNull(users);
		// Should include owner and editor
		assertTrue(users.size() >= 2);
	}

	@Test
	void testGetDisplayEngineOwnersAndEditors_GlobalEngine() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngineGlobal("eid1", "ename1", user);

		List<Map<String, Object>> users = SecurityUserEngineUtils.getDisplayEngineOwnersAndEditors("eid1");
		assertNotNull(users);
		// Should include owner and PUBLIC DATABASE entry
		boolean hasPublicEntry = users.stream()
				.anyMatch(m -> "PUBLIC DATABASE".equals(m.get("name")));
		assertTrue(hasPublicEntry);
	}

	///
	/// getEngineUsers
	///

	@Test
	void testGetEngineUsers_NoFilters() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		List<Map<String, Object>> users = SecurityUserEngineUtils.getEngineUsers("eid1", null, null, -1, -1);
		assertNotNull(users);
		assertEquals(2, users.size());
	}

	@Test
	void testGetEngineUsers_WithSearchParam() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		List<Map<String, Object>> users = SecurityUserEngineUtils.getEngineUsers("eid1", "admin", null, -1, -1);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetEngineUsers_WithPermissionFilter() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		List<Map<String, Object>> users = SecurityUserEngineUtils.getEngineUsers("eid1", null, "OWNER", -1, -1);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetEngineUsers_WithLimit() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user3Id, "eid1", "READ_ONLY");

		List<Map<String, Object>> users = SecurityUserEngineUtils.getEngineUsers("eid1", null, null, 2, 0);
		assertNotNull(users);
		assertEquals(2, users.size());
	}

	@Test
	void testGetEngineUsers_WithOffset() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createEngine("eid1", "ename1", user);

		User user2 = UnitTestSecurityAuthUtils.createUser("editor", false);
		String user2Id = user2.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user2Id, "eid1", "EDIT");

		User user3 = UnitTestSecurityAuthUtils.createUser("reader", false);
		String user3Id = user3.getAccessToken(AuthProvider.NATIVE).getId();
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(user, user3Id, "eid1", "READ_ONLY");

		List<Map<String, Object>> users = SecurityUserEngineUtils.getEngineUsers("eid1", null, null, 10, 1);
		assertNotNull(users);
		assertEquals(2, users.size()); // 3 total, offset 1, so 2 remaining
	}
}
