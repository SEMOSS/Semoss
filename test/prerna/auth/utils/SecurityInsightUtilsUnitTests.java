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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.util.SystemEngineRegistry;

public class SecurityInsightUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;

	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// getInsight
	///

	@Test
	void testGetInsight_doesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertNull(SecurityInsightUtils.getInsight("testProjectId", "nonExistentId"));
	}

	///
	/// getUserInsightIdList
	///

	@Test
	void testGetUserInsightIdList_noInsights() {
		User user = UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<String> insightIds = SecurityInsightUtils.getUserInsightIdList(user, true, true);
		assertNotNull(insightIds);
		assertTrue(insightIds.isEmpty());
	}

	@Test
	void testGetUserInsightIdList_withInsights() throws IllegalAccessException {
		User user1 = UnitTestSecurityAuthUtils.createUser("user1", false);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user1);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight2", "Insight Two", "grid");

		// Give user2 access to insight1
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(user1, "testProjectId", "user2id", "OWNER");

		List<String> insightIds = SecurityInsightUtils.getUserInsightIdList(user2, true, true);
		assertNotNull(insightIds);
		assertFalse(insightIds.isEmpty());
		assertTrue(insightIds.contains("insight1") && insightIds.contains("insight2"));
	}

	@Test
	void testGetUserInsightIdList_excludeGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");

		List<String> insightIds = SecurityInsightUtils.getUserInsightIdList(user, false, true);
		assertNotNull(insightIds);
		assertEquals("insight1", insightIds.getFirst());
	}

	///
	/// insightNameExists
	///

	@Test
	void testInsightNameExists_doesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String insightId = SecurityInsightUtils.insightNameExists("testProjectId", "NonExistentInsight");
		assertNull(insightId);
	}

	@Test
	void testInsightNameExists() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		String insightId = SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName");
		assertEquals("testInsightId", insightId);
	}

	@Test
	void testInsightNameExists_caseInsensitive() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		String insightId = SecurityInsightUtils.insightNameExists("testProjectId", "testinsightname");
		assertEquals("testInsightId", insightId);
	}

	///
	/// insightNameExistsMinusId
	///

	@Test
	void testInsightNameExistsMinusId_doesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean exists = SecurityInsightUtils.insightNameExistsMinusId("testProjectId", "OtherInsightName",
				"testInsightId");
		assertFalse(exists);
	}

	@Test
	void testInsightNameExistsMinusId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId2", "OtherInsightName", "grid");

		boolean exists = SecurityInsightUtils.insightNameExistsMinusId("testProjectId", "OtherInsightName",
				"testInsightId");
		assertTrue(exists);
	}

	@Test
	void testInsightNameExistsMinusId_sameInsightReturnsTrue() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean exists = SecurityInsightUtils.insightNameExistsMinusId("testProjectId", "TestInsightName",
				"testInsightId");
		assertFalse(exists);
	}

	///
	/// getUserEditableInsights
	///

	@Test
	void testGetUserEditableInsights_noPermission() {
		User owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", owner);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");

		List<Map<String, Object>> insights = SecurityInsightUtils.getUserEditableInsights(user2, "testProjectId");
		assertNotNull(insights);
		assertTrue(insights.isEmpty());
	}

	@Test
	void testGetUserEditableInsights_ownerPermission() {
		User owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", owner);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight2", "Insight Two", "grid");

		List<Map<String, Object>> insights = SecurityInsightUtils.getUserEditableInsights(owner, "testProjectId");
		assertNotNull(insights);
		assertEquals(2, insights.size());
	}

	@Test
	void testGetUserEditableInsights_editorPermission() throws IllegalAccessException {
		User owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		User editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", owner);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");

		// Give editor permission to the project
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(owner, "testProjectId", "editorid", "EDIT");

		List<Map<String, Object>> insights = SecurityInsightUtils.getUserEditableInsights(editor, "testProjectId");
		assertNotNull(insights);
	}

	///
	/// getAllInsightIds
	///

	@Test
	void testGetAllInsightIds_empty() {
		List<String> insightIds = SecurityInsightUtils.getAllInsightIds();
		assertEquals(0, insightIds.size());
	}

	@Test
	void testGetAllInsightIds() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insightId1", "Insight1", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insightId2", "Insight2", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insightId3", "Insight3", "grid");

		List<String> insightIds = SecurityInsightUtils.getAllInsightIds();
		assertEquals(3, insightIds.size());
		assertTrue(insightIds.contains("insightId1"));
		assertTrue(insightIds.contains("insightId2"));
		assertTrue(insightIds.contains("insightId3"));
	}

	///
	/// insightIsGlobal
	///

	@Test
	void testInsightIsGlobal_notGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean isGlobal = SecurityInsightUtils.insightIsGlobal("testProjectId", "testInsightId");
		assertFalse(isGlobal);
	}

	@Test
	void testInsightIsGlobal() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		SecurityInsightUtils.setInsightGlobalWithinProject(user, "testProjectId", "testInsightId", true);

		boolean isGlobal = SecurityInsightUtils.insightIsGlobal("testProjectId", "testInsightId");
		assertTrue(isGlobal);
	}

	///
	/// userCanViewInsight
	///

	@Test
	void testUserCanViewInsight_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean canView = SecurityInsightUtils.userCanViewInsight(user, "testProjectId", "testInsightId");
		assertTrue(canView);
	}

	@Test
	void testUserCanViewInsight_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean canView = SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId");
		assertFalse(canView);
	}

	@Test
	void testUserCanViewInsight_globalInsight() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		SecurityInsightUtils.setInsightGlobalWithinProject(user, "testProjectId", "testInsightId", true);

		boolean canView = SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId");
		assertTrue(canView);
	}

	///
	/// userCanEditInsight
	///

	@Test
	void testUserCanEditInsight_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean canEdit = SecurityInsightUtils.userCanEditInsight(user, "testProjectId", "testInsightId");
		assertTrue(canEdit);
	}

	@Test
	void testUserCanEditInsight_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean canEdit = SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "testInsightId");
		assertFalse(canEdit);
	}

	///
	/// userIsInsightOwner
	///

	@Test
	void testUserIsInsightOwner_projectOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean isOwner = SecurityInsightUtils.userIsInsightOwner(user, "testProjectId", "testInsightId");
		assertTrue(isOwner);
	}

	@Test
	void testUserIsInsightOwner_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		boolean isOwner = SecurityInsightUtils.userIsInsightOwner(user2, "testProjectId", "testInsightId");
		assertFalse(isOwner);
	}

	///
	/// setInsightGlobalWithinProject
	///

	@Test
	void testSetInsightGlobalWithinProject_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityInsightUtils
				.setInsightGlobalWithinProject(user2, "testProjectId", "testInsightId", true));
		assertEquals(
				"The user doesn't have the permission to set this insight as global. Only the owner or an admin can perform this action.",
				ex.getMessage());
	}

	@Test
	void testSetInsightGlobalWithinProject() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		assertFalse(SecurityInsightUtils.insightIsGlobal("testProjectId", "testInsightId"));

		SecurityInsightUtils.setInsightGlobalWithinProject(user, "testProjectId", "testInsightId", true);

		assertTrue(SecurityInsightUtils.insightIsGlobal("testProjectId", "testInsightId"));
	}

	///
	/// setInsightFavorite
	///

	@Test
	void testSetInsightFavorite_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityInsightUtils.setInsightFavorite(user2, "testProjectId", "testInsightId", true));
		assertEquals("The user doesn't have the permission to modify this insight", ex.getMessage());
	}

	@Test
	void testSetInsightFavorite() throws IllegalAccessException, SQLException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		SecurityInsightUtils.setInsightFavorite(user, "testProjectId", "testInsightId", true);

		// We can't easily verify the favorite status without querying the database
		// but at least we verify no exception is thrown
		assertTrue(SecurityInsightUtils.userIsInsightOwner(user, "testProjectId", "testInsightId"));
	}

	///
	/// addInsight
	///

	@Test
	void testAddInsight() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertNull(SecurityInsightUtils.insightNameExists("testProjectId", "NewInsight"));

		SecurityInsightUtils.addInsight("testProjectId", "newInsightId", "NewInsight", false, "grid", false, 0, null,
				null, false, null, null);

		String insightId = SecurityInsightUtils.insightNameExists("testProjectId", "NewInsight");
		assertEquals("newInsightId", insightId);
	}

	///
	/// addUserInsightCreator
	///

	@Test
	void testAddUserInsightCreator() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// Verify no permission initially
		Integer permission = SecurityInsightUtils.getUserInsightPermission("adminid", "testProjectId", "testInsightId");
		assertNull(permission);

		// Add user as creator (owner)
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		// Verify owner permission was added
		permission = SecurityInsightUtils.getUserInsightPermission("adminid", "testProjectId", "testInsightId");
		assertNotNull(permission);
		assertEquals(1, permission); // OWNER permission
	}

	///
	/// updateInsight
	///

	@Test
	void testUpdateInsight() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "OriginalName", "grid");

		assertEquals("testInsightId", SecurityInsightUtils.insightNameExists("testProjectId", "OriginalName"));
		assertFalse(SecurityInsightUtils.insightIsGlobal("testProjectId", "testInsightId"));

		// Update the insight name and make it global
		SecurityInsightUtils.updateInsight("testProjectId", "testInsightId", "UpdatedName", true, "grid", false, 0,
				null, null, false, null, null);

		assertNull(SecurityInsightUtils.insightNameExists("testProjectId", "OriginalName"));
		assertEquals("testInsightId", SecurityInsightUtils.insightNameExists("testProjectId", "UpdatedName"));
		assertTrue(SecurityInsightUtils.insightIsGlobal("testProjectId", "testInsightId"));
	}

	///
	/// updateInsightName
	///

	@Test
	void testUpdateInsightName() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "OriginalName", "grid");

		assertEquals("testInsightId", SecurityInsightUtils.insightNameExists("testProjectId", "OriginalName"));

		SecurityInsightUtils.updateInsightName("testProjectId", "testInsightId", "UpdatedName");

		assertNull(SecurityInsightUtils.insightNameExists("testProjectId", "OriginalName"));
		assertEquals("testInsightId", SecurityInsightUtils.insightNameExists("testProjectId", "UpdatedName"));
	}

	///
	/// updateInsightCache
	///

	@Test
	void testUpdateInsightCache() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// Update cache settings
		SecurityInsightUtils.updateInsightCache("testProjectId", "testInsightId", true, 60, null, null, false);

		// Verify insight still exists
		assertNotNull(SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName"));
	}

	///
	/// updateInsightCachedOn
	///

	@Test
	void testUpdateInsightCachedOn() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// Update cached on timestamp
		SecurityInsightUtils.updateInsightCachedOn("testProjectId", "testInsightId", null);

		// Verify insight still exists
		assertNotNull(SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName"));
	}

	///
	/// updateInsightMetadata
	///

	@Test
	void testUpdateInsightMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// Update metadata
		Map<String, Object> metadata = new HashMap<>();
		metadata.put("testKey", "testValue");
		SecurityInsightUtils.updateInsightMetadata("testProjectId", "testInsightId", metadata);

		// Verify insight still exists
		assertNotNull(SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName"));
	}

	///
	/// deleteInsight
	///

	@Test
	void testDeleteInsight() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		assertNotNull(SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName"));

		SecurityInsightUtils.deleteInsight("testProjectId", "testInsightId");

		assertNull(SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName"));
	}

	@Test
	void testDeleteInsight_multiple() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insightId1", "Insight1", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insightId2", "Insight2", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insightId3", "Insight3", "grid");

		assertEquals(3, SecurityInsightUtils.getAllInsightIds().size());

		SecurityInsightUtils.deleteInsight("testProjectId", "insightId1", "insightId2");

		assertEquals(1, SecurityInsightUtils.getAllInsightIds().size());
		assertNull(SecurityInsightUtils.insightNameExists("testProjectId", "Insight1"));
		assertNull(SecurityInsightUtils.insightNameExists("testProjectId", "Insight2"));
		assertNotNull(SecurityInsightUtils.insightNameExists("testProjectId", "Insight3"));
	}

	///
	/// addInsightUser
	///

	@Test
	void testAddInsightUser_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> SecurityInsightUtils
				.addInsightUser(user2, "user3id", "testProjectId", "testInsightId", "OWNER", null));
		assertEquals("Insufficient privileges to modify this insight's permissions.", ex.getMessage());
	}

	@Test
	void testAddInsightUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		assertFalse(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));

		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
	}

	///
	/// editInsightUserPermission
	///

	@Test
	void testEditInsightUserPermission_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityInsightUtils
				.editInsightUserPermission(user2, "user3id", "testProjectId", "testInsightId", "OWNER", null));
		assertEquals("Insufficient privileges to modify this insight's permissions.", ex.getMessage());
	}

	@Test
	void testEditInsightUserPermission() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
		assertFalse(SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "testInsightId"));

		SecurityInsightUtils.editInsightUserPermission(user, "user2id", "testProjectId", "testInsightId", "EDIT", null);

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "testInsightId"));
	}

	///
	/// removeInsightUser
	///

	@Test
	void testRemoveInsightUser_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityInsightUtils.removeInsightUser(user2, "user3id", "testProjectId", "testInsightId"));
		assertEquals("Insufficient privileges to modify this insight's permissions.", ex.getMessage());
	}

	@Test
	void testRemoveInsightUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));

		SecurityInsightUtils.removeInsightUser(user, "user2id", "testProjectId", "testInsightId");

		assertFalse(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
	}

	///
	/// searchUserInsights
	///

	@Test
	void testSearchUserInsights_noInsights() {
		User user = UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> results = SecurityInsightUtils.searchUserInsights(user, null, null, false, null, null,
				null, null);
		assertNotNull(results);
	}

	@Test
	void testSearchUserInsights_withSearchTerm() {
		User user = UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "MyInsight", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight2", "OtherInsight", "grid");

		List<Map<String, Object>> results = SecurityInsightUtils.searchUserInsights(user, null, "MyInsight", false,
				null, null, null, null);
		assertNotNull(results);
	}

	@Test
	void testSearchUserInsights_withProjectFilter() {
		User user = UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");

		List<String> projectFilter = List.of("testProjectId");
		List<Map<String, Object>> results = SecurityInsightUtils.searchUserInsights(user, projectFilter, null, false,
				null, null, null, null);
		assertNotNull(results);
	}

	///
	/// getInsightAliasForId
	///

	@Test
	void testGetInsightAliasForId_doesNotExist() {
		assertNull(SecurityInsightUtils.getInsightAliasForId("testProjectId", "nonExistentId"));
	}

	@Test
	void testGetInsightAliasForId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		String alias = SecurityInsightUtils.getInsightAliasForId("testProjectId", "testInsightId");
		assertEquals("TestInsightName", alias);
	}

	///
	/// getAllMetakeys
	///

	@Test
	void testGetAllMetakeys() {
		List<String> metakeys = SecurityInsightUtils.getAllMetakeys();
		assertNotNull(metakeys);
	}

	///
	/// getInsightSchemaName
	///

	@Test
	void testGetInsightSchemaName_doesNotExist() {
		assertNull(SecurityInsightUtils.getInsightSchemaName("nonExistentProject", "nonExistentInsight"));
	}

	@Test
	void testGetInsightSchemaName_noSchemaName() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		String schemaName = SecurityInsightUtils.getInsightSchemaName("testProjectId", "testInsightId");
		assertNull(schemaName);
	}

	///
	/// makeInsightSchemaNameUnique
	///

	@Test
	void testMakeInsightSchemaNameUnique_nullSchemaName() {
		assertNull(SecurityInsightUtils.makeInsightSchemaNameUnique("testProjectId", null));
	}

	@Test
	void testMakeInsightSchemaNameUnique_uniqueName() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique("testProjectId", "MySchema");
		assertEquals("MySchema", schemaName);
	}

	@Test
	void testMakeInsightSchemaNameUnique_cleansSpaces() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique("testProjectId", "My Schema Name");
		assertEquals("My_Schema_Name", schemaName);
	}

	@Test
	void testMakeInsightSchemaNameUnique_cleansNonAlphanumeric() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique("testProjectId", "My-Schema@Name!");
		assertEquals("MySchemaName", schemaName);
	}

	///
	/// getUserInsightPermission
	///

	@Test
	void testGetUserInsightPermission_noPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		Integer permission = SecurityInsightUtils.getUserInsightPermission("user2id", "testProjectId", "testInsightId");
		assertNull(permission);
	}

	@Test
	void testGetUserInsightPermission() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "EDIT", null);

		Integer permission = SecurityInsightUtils.getUserInsightPermission("user2id", "testProjectId", "testInsightId");
		assertNotNull(permission);
		assertEquals(2, permission); // EDIT permission
	}

	///
	/// getUserInsightPermissions
	///

	@Test
	void testGetUserInsightPermissions_noPermissions() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		List<String> userIds = List.of("user2id", "user3id");
		Map<String, Integer> permissions = SecurityInsightUtils.getUserInsightPermissions(userIds, "testProjectId",
				"testInsightId");
		assertNotNull(permissions);
		assertTrue(permissions.isEmpty());
	}

	@Test
	void testGetUserInsightPermissions_multipleUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "EDIT", null);
		SecurityInsightUtils.addInsightUser(user, "user3id", "testProjectId", "testInsightId", "READ_ONLY", null);

		List<String> userIds = List.of("user2id", "user3id");
		Map<String, Integer> permissions = SecurityInsightUtils.getUserInsightPermissions(userIds, "testProjectId",
				"testInsightId");
		assertNotNull(permissions);
		assertEquals(2, permissions.size());
		assertEquals(2, permissions.get("user2id")); // EDIT
		assertEquals(3, permissions.get("user3id")); // READ_ONLY
	}

	///
	/// getUserInsightPermissionsWrapper
	///

	@Test
	void testGetUserInsightPermissionsWrapper_noPermissions() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		List<String> userIds = List.of("user2id");
		try (IRawSelectWrapper wrapper = SecurityInsightUtils.getUserInsightPermissionsWrapper(userIds, "testProjectId",
				"testInsightId")) {
			assertNotNull(wrapper);
			assertFalse(wrapper.hasNext());
		}
	}

	@Test
	void testGetUserInsightPermissionsWrapper_withPermissions() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "EDIT", null);

		List<String> userIds = List.of("user2id");
		try (IRawSelectWrapper wrapper = SecurityInsightUtils.getUserInsightPermissionsWrapper(userIds, "testProjectId",
				"testInsightId")) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
		}
	}

	///
	/// getInsightUsers
	///

	@Test
	void testGetInsightUsers_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityInsightUtils.getInsightUsers(user2, "testProjectId", "testInsightId", null, null, 0, 0));
		assertEquals("The user does not have access to view this insight", ex.getMessage());
	}

	@Test
	void testGetInsightUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		List<Map<String, Object>> users = SecurityInsightUtils.getInsightUsers(user, "testProjectId", "testInsightId",
				null, null, 0, 0);
		assertEquals(1, users.size());
		assertEquals("adminid", users.get(0).get("id").toString());
	}

	///
	/// getInsightUsersCount
	///

	@Test
	void testGetInsightUsersCount_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityInsightUtils.getInsightUsersCount(user2, "testProjectId", "testInsightId", null, null));
		assertEquals("The user does not have access to view this insight", ex.getMessage());
	}

	@Test
	void testGetInsightUsersCount() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		long count = SecurityInsightUtils.getInsightUsersCount(user, "testProjectId", "testInsightId", null, null);
		assertEquals(1, count);
	}

	///
	/// updateInsightDescription
	///

	@Test
	void testUpdateInsightDescription() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		SecurityInsightUtils.updateInsightDescription("testProjectId", "testInsightId", "This is a test description");

		// Verify no exception was thrown
		assertTrue(SecurityInsightUtils.userIsInsightOwner(user, "testProjectId", "testInsightId"));
	}

	///
	/// updateInsightTags
	///

	@Test
	void testUpdateInsightTags_list() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> tags = List.of("tag1", "tag2", "tag3");
		SecurityInsightUtils.updateInsightTags("testProjectId", "testInsightId", tags);

		// Verify no exception was thrown
		assertTrue(SecurityInsightUtils.userIsInsightOwner(user, "testProjectId", "testInsightId"));
	}

	@Test
	void testUpdateInsightTags_array() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		String[] tags = new String[] { "tag1", "tag2", "tag3" };
		SecurityInsightUtils.updateInsightTags("testProjectId", "testInsightId", tags);

		// Verify no exception was thrown
		assertTrue(SecurityInsightUtils.userIsInsightOwner(user, "testProjectId", "testInsightId"));
	}

	///
	/// updateInsightFrames
	///

	@Test
	void testUpdateInsightFrames() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// Update with empty set of frames
		SecurityInsightUtils.updateInsightFrames("testProjectId", "testInsightId", new java.util.HashSet<>());

		// Verify insight still exists
		assertNotNull(SecurityInsightUtils.insightNameExists("testProjectId", "TestInsightName"));
	}

	///
	/// updateExecutionCount
	///

	@Test
	void testUpdateExecutionCount() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		SecurityInsightUtils.updateExecutionCountAsync("testProjectId", "testInsightId");

		// Verify no exception was thrown
		assertTrue(SecurityInsightUtils.userIsInsightOwner(user, "testProjectId", "testInsightId"));
	}

	///
	/// addInsightUserPermissions
	///

	@Test
	void testAddInsightUserPermissions_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<Map<String, String>> permissions = List.of(Map.of("userid", "user3id", "permission", "READ_ONLY"));

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> SecurityInsightUtils
				.addInsightUserPermissions(user2, "testProjectId", "testInsightId", permissions, null));
		assertEquals("Insufficient privileges to modify this insight's permissions.", ex.getMessage());
	}

	@Test
	void testAddInsightUserPermissions() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "READ_ONLY");
		perm1.put("endDate", null);

		Map<String, String> perm2 = new HashMap<>();
		perm2.put("userid", "user3id");
		perm2.put("permission", "EDIT");
		perm2.put("endDate", null);

		List<Map<String, String>> permissions = List.of(perm1, perm2);

		SecurityInsightUtils.addInsightUserPermissions(user, "testProjectId", "testInsightId", permissions, null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
		assertTrue(SecurityInsightUtils.userCanEditInsight(user3, "testProjectId", "testInsightId"));
	}

	///
	/// editInsightUserPermissions
	///

	@Test
	void testEditInsightUserPermissions_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<Map<String, String>> permissions = List.of(Map.of("userid", "user3id", "permission", "EDIT"));

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityInsightUtils
				.editInsightUserPermissions(user2, "testProjectId", "testInsightId", permissions, null));
		assertEquals("Insufficient privileges to modify insight permissions.", ex.getMessage());
	}

	@Test
	void testEditInsightUserPermissions() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);

		Map<String, String> perm = new HashMap<>();
		perm.put("userid", "user2id");
		perm.put("permission", "EDIT");
		perm.put("endDate", null);

		List<Map<String, String>> permissions = List.of(perm);

		SecurityInsightUtils.editInsightUserPermissions(user, "testProjectId", "testInsightId", permissions, null);

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "testInsightId"));
	}

	///
	/// removeInsightUsers
	///

	@Test
	void testRemoveInsightUsers_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityInsightUtils
				.removeInsightUsers(user2, List.of("user3id"), "testProjectId", "testInsightId"));
		assertEquals("Insufficient privileges to modify this insight's permissions.", ex.getMessage());
	}

	@Test
	void testRemoveInsightUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);
		SecurityInsightUtils.addInsightUser(user, "user3id", "testProjectId", "testInsightId", "EDIT", null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
		assertTrue(SecurityInsightUtils.userCanEditInsight(user3, "testProjectId", "testInsightId"));

		SecurityInsightUtils.removeInsightUsers(user, List.of("user2id", "user3id"), "testProjectId", "testInsightId");

		assertFalse(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
		assertFalse(SecurityInsightUtils.userCanViewInsight(user3, "testProjectId", "testInsightId"));
	}

	///
	/// removeExpiredInsightUser
	///

	@Test
	void testRemoveExpiredInsightUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));

		SecurityInsightUtils.removeExpiredInsightUser("user2id", "testProjectId", "testInsightId");

		assertFalse(SecurityInsightUtils.userCanViewInsight(user2, "testProjectId", "testInsightId"));
	}

	///
	/// copyInsightPermissions
	///

	@Test
	void testCopyInsightPermissions() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "sourceInsightId", "SourceInsight", "grid");
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "targetInsightId", "TargetInsight", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "sourceInsightId");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "targetInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "sourceInsightId", "EDIT", null);

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "sourceInsightId"));
		assertFalse(SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "targetInsightId"));

		SecurityInsightUtils.copyInsightPermissions("testProjectId", "sourceInsightId", "testProjectId",
				"targetInsightId");

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "testProjectId", "targetInsightId"));
	}

	///
	/// getInsightOwners
	///

	@Test
	void testGetInsightOwners_empty() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> owners = SecurityInsightUtils.getInsightOwners("testProjectId", "testInsightId");
		assertEquals(0, owners.size());
	}

	@Test
	void testGetInsightOwners() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		List<String> owners = SecurityInsightUtils.getInsightOwners("testProjectId", "testInsightId");
		assertEquals(1, owners.size());
		assertEquals("admin@test.com", owners.get(0));
	}

	///
	/// getActualUserInsightPermission
	///

	@Test
	void testGetActualUserInsightPermission_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		String permission = SecurityInsightUtils.getActualUserInsightPermission(user, "testProjectId", "testInsightId");
		assertEquals("OWNER", permission);
	}

	@Test
	void testGetActualUserInsightPermission_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		String permission = SecurityInsightUtils.getActualUserInsightPermission(user2, "testProjectId",
				"testInsightId");
		assertNull(permission);
	}

	@Test
	void testGetActualUserInsightPermission_editor() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "EDIT", null);

		String permission = SecurityInsightUtils.getActualUserInsightPermission(user2, "testProjectId",
				"testInsightId");
		assertEquals("EDIT", permission);
	}

	@Test
	void testGetActualUserInsightPermission_readOnly() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");
		SecurityInsightUtils.addInsightUser(user, "user2id", "testProjectId", "testInsightId", "READ_ONLY", null);

		String permission = SecurityInsightUtils.getActualUserInsightPermission(user2, "testProjectId",
				"testInsightId");
		assertEquals("READ_ONLY", permission);
	}

	///
	/// getMetakeyOptions
	///

	@Test
	void testGetMetakeyOptions_noMetakey() {
		List<Map<String, Object>> options = SecurityInsightUtils.getMetakeyOptions(null);
		assertNotNull(options);
	}

	@Test
	void testGetMetakeyOptions_withMetakey() {
		List<Map<String, Object>> options = SecurityInsightUtils.getMetakeyOptions("description");
		assertNotNull(options);
	}

	///
	/// getSpecificInsightMetadata
	///

	@Test
	void testGetSpecificInsightMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> metakeys = List.of("description", "tag");
		Map<String, Object> metadata = SecurityInsightUtils.getSpecificInsightMetadata("testProjectId", "testInsightId",
				metakeys);
		assertNotNull(metadata);
	}

	///
	/// getSpecificInsightCacheDetails
	///

	@Test
	void testGetSpecificInsightCacheDetails() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		Map<String, Object> cacheDetails = SecurityInsightUtils.getSpecificInsightCacheDetails("testProjectId",
				"testInsightId");
		assertNotNull(cacheDetails);
	}

	@Test
	void testGetSpecificInsightCacheDetails_doesNotExist() {
		Map<String, Object> cacheDetails = SecurityInsightUtils.getSpecificInsightCacheDetails("nonExistentProject",
				"nonExistentInsight");
		assertNotNull(cacheDetails);
		assertTrue(cacheDetails.isEmpty());
	}

	///
	/// getAvailableInsightTagsAndCounts
	///

	@Test
	void testGetAvailableInsightTagsAndCounts_noFilter() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<Map<String, Object>> tags = SecurityInsightUtils.getAvailableInsightTagsAndCounts(null);
		assertNotNull(tags);
	}

	@Test
	void testGetAvailableInsightTagsAndCounts_withFilter() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> projectFilter = List.of("testProjectId");
		List<Map<String, Object>> tags = SecurityInsightUtils.getAvailableInsightTagsAndCounts(projectFilter);
		assertNotNull(tags);
	}

	///
	/// getInsightFrames
	///

	@Test
	void testGetInsightFrames() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<Object[]> frames = SecurityInsightUtils.getInsightFrames("testProjectId", "testInsightId");
		assertNotNull(frames);
	}

	@Test
	void testGetInsightFrames_withPattern() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<Object[]> frames = SecurityInsightUtils.getInsightFrames("testProjectId", "testInsightId", "Frame%");
		assertNotNull(frames);
	}

	///
	/// getInsightUsersNoCredentials
	///

	@Test
	void testGetInsightUsersNoCredentials() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");
		SecurityInsightUtils.addUserInsightCreator(user, "testProjectId", "testInsightId");

		List<Map<String, Object>> users = SecurityInsightUtils.getInsightUsersNoCredentials(user, "testProjectId",
				"testInsightId", null, 10, 0);
		assertNotNull(users);
	}

	@Test
	void testGetInsightUsersNoCredentials_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityInsightUtils
				.getInsightUsersNoCredentials(user2, "testProjectId", "testInsightId", null, 10, 0));
		assertEquals("The user does not have access to view this insight", ex.getMessage());
	}

	///
	/// getUserAccessRequestInsightPermission
	///

	@Test
	void testGetUserAccessRequestInsightPermission_noRequest() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		Integer permission = SecurityInsightUtils.getUserAccessRequestInsightPermission("nonExistentUserId",
				"testProjectId", "testInsightId");
		assertNull(permission);
	}

	///
	/// getUserAccessRequestsByInsight
	///

	@Test
	void testGetUserAccessRequestsByInsight_noRequests() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<Map<String, Object>> requests = SecurityInsightUtils.getUserAccessRequestsByInsight("testProjectId",
				"testInsightId");
		assertNotNull(requests);
		assertTrue(requests.isEmpty());
	}

	///
	/// getAvailableMetaValues
	///

	@Test
	void testGetAvailableMetaValues() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> metaKeys = List.of("description", "tag");
		List<Map<String, Object>> values = SecurityInsightUtils.getAvailableMetaValues(null, metaKeys);
		assertNotNull(values);
	}

	@Test
	void testGetAvailableMetaValues_withInsightFilter() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> insightFilter = List.of("testInsightId");
		List<String> metaKeys = List.of("description");
		List<Map<String, Object>> values = SecurityInsightUtils.getAvailableMetaValues(insightFilter, metaKeys);
		assertNotNull(values);
	}

	///
	/// getInsightMetadataWrapper
	///

	@Test
	void testGetInsightMetadataWrapper() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> insightIds = List.of("testInsightId");
		List<String> metaKeys = List.of("description", "tag");
		try (IRawSelectWrapper wrapper = SecurityInsightUtils.getInsightMetadataWrapper("testProjectId", insightIds,
				metaKeys)) {
			assertNotNull(wrapper);
		}
	}

	@Test
	void testGetInsightMetadataWrapper_multipleProjects() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		Map<String, List<String>> projectToInsightMap = new HashMap<>();
		projectToInsightMap.put("testProjectId", List.of("testInsightId"));
		List<String> metaKeys = List.of("description");
		try (IRawSelectWrapper wrapper = SecurityInsightUtils.getInsightMetadataWrapper(projectToInsightMap,
				metaKeys)) {
			assertNotNull(wrapper);
		}
	}

	///
	/// predictInsightSearch
	///

	@Test
	void testPredictInsightSearch() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		List<String> predictions = SecurityInsightUtils.predictInsightSearch("Test", "10", "0");
		assertNotNull(predictions);
	}

	///
	/// updateMetakeyOptions
	///

	@Test
	void testUpdateMetakeyOptions() {
		Map<String, Object> option1 = new HashMap<>();
		option1.put("metakey", "description");
		option1.put("singlemulti", "single");
		option1.put("order", 1);
		option1.put("displayoptions", "text");

		Map<String, Object> option2 = new HashMap<>();
		option2.put("metakey", "tag");
		option2.put("singlemulti", "multi");
		option2.put("order", 2);
		option2.put("displayoptions", "tag");

		List<Map<String, Object>> metaoptions = List.of(option1, option2);

		boolean result = SecurityInsightUtils.updateMetakeyOptions(metaoptions);
		assertTrue(result);

		// Verify the metakeys were added
		List<String> metakeys = SecurityInsightUtils.getAllMetakeys();
		assertNotNull(metakeys);
		assertEquals(2, metakeys.size());
		assertTrue(metakeys.contains("description"));
		assertTrue(metakeys.contains("tag"));
	}

	@Test
	void testUpdateMetakeyOptions_emptyList() {
		List<Map<String, Object>> metaoptions = List.of();

		boolean result = SecurityInsightUtils.updateMetakeyOptions(metaoptions);
		assertTrue(result);

		// Verify metakeys table is now empty
		List<String> metakeys = SecurityInsightUtils.getAllMetakeys();
		assertNotNull(metakeys);
		assertTrue(metakeys.isEmpty());
	}

	@Test
	void testUpdateMetakeyOptions_replacesExisting() {
		// First insert some options
		Map<String, Object> option1 = new HashMap<>();
		option1.put("metakey", "oldkey");
		option1.put("singlemulti", "single");
		option1.put("order", 1);
		option1.put("displayoptions", "text");
		SecurityInsightUtils.updateMetakeyOptions(List.of(option1));

		// Now replace with new options
		Map<String, Object> option2 = new HashMap<>();
		option2.put("metakey", "newkey");
		option2.put("singlemulti", "multi");
		option2.put("order", 1);
		option2.put("displayoptions", "dropdown");

		boolean result = SecurityInsightUtils.updateMetakeyOptions(List.of(option2));
		assertTrue(result);

		// Verify old key is gone and new key exists
		List<String> metakeys = SecurityInsightUtils.getAllMetakeys();
		assertEquals(1, metakeys.size());
		assertTrue(metakeys.contains("newkey"));
		assertFalse(metakeys.contains("oldkey"));
	}

	///
	/// setUserAccessRequest
	///

	@Test
	void testSetUserAccessRequest() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// User2 requests access to the insight
		SecurityInsightUtils.setUserAccessRequest("user2id", "NATIVE", "testProjectId", "I need access to this insight",
				"testInsightId", 3, user2); // 3 = READ_ONLY

		// Verify the request was created by checking getUserAccessRequestsByInsight
		// Note: getUserAccessRequestInsightPermission filters for
		// APPROVER_DECISION=null,
		// but setUserAccessRequest inserts with APPROVER_DECISION='NEW_REQUEST'
		List<Map<String, Object>> requests = SecurityInsightUtils.getUserAccessRequestsByInsight("testProjectId",
				"testInsightId");
		assertNotNull(requests);
		assertEquals(1, requests.size());
	}

	@Test
	void testSetUserAccessRequest_multipleRequests() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// First request for READ_ONLY
		SecurityInsightUtils.setUserAccessRequest("user2id", "NATIVE", "testProjectId", "First request",
				"testInsightId", 3, user2);

		// Second request for EDIT - should mark first as 'OLD' and create new
		SecurityInsightUtils.setUserAccessRequest("user2id", "NATIVE", "testProjectId", "Second request - need edit",
				"testInsightId", 2, user2);

		// Verify only the latest request appears (old one is marked as 'OLD')
		List<Map<String, Object>> requests = SecurityInsightUtils.getUserAccessRequestsByInsight("testProjectId",
				"testInsightId");
		assertNotNull(requests);
		assertEquals(1, requests.size());
	}

	@Test
	void testSetUserAccessRequest_verifyInRequestsList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "testInsightId", "TestInsightName", "grid");

		// User2 requests access
		SecurityInsightUtils.setUserAccessRequest("user2id", "NATIVE", "testProjectId", "Please give me access",
				"testInsightId", 3, user2);

		// Verify request appears in the requests list
		List<Map<String, Object>> requests = SecurityInsightUtils.getUserAccessRequestsByInsight("testProjectId",
				"testInsightId");
		assertNotNull(requests);
		assertEquals(1, requests.size());
	}

}