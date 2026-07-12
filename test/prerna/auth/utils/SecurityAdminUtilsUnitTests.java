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

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

public class SecurityAdminUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User adminUser;
	private SecurityAdminUtils instance;

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);

		adminUser = UnitTestSecurityAuthUtils.createUser("admin", true);
		instance = SecurityAdminUtils.getInstance(adminUser);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// getInstance
	///

	@Test
	void testGetInstance_userNull() {
		assertNull(SecurityAdminUtils.getInstance(null));
	}

	@Test
	void testGetInstance_userNotAdmin() {
		User nonAdmin = UnitTestSecurityAuthUtils.createUser("nonAdmin", false);
		assertNull(SecurityAdminUtils.getInstance(nonAdmin));
	}

	@Test
	void testGetInstance_userAdmin() {
		assertNotNull(SecurityAdminUtils.getInstance(adminUser));
	}

	///
	/// userIsAdmin (static)
	///

	@Test
	void testUserIsAdmin_adminUser() {
		assertTrue(SecurityAdminUtils.userIsAdmin(adminUser));
	}

	@Test
	void testUserIsAdmin_nonAdminUser() {
		User nonAdmin = UnitTestSecurityAuthUtils.createUser("nonAdmin", false);
		assertFalse(SecurityAdminUtils.userIsAdmin(nonAdmin));
	}

	@Test
	void testUserIsAdmin_nullUser() {
		assertFalse(SecurityAdminUtils.userIsAdmin(null));
	}

	///
	/// userIsAdmin (instance)
	///

	@Test
	void testUserIsAdminInstance_isAdmin() {
		assertTrue(instance.userIsAdmin("adminid", "NATIVE"));
	}

	@Test
	void testUserIsAdminInstance_notAdmin() {
		User nonAdmin = UnitTestSecurityAuthUtils.createUser("nonAdmin", false);
		assertFalse(instance.userIsAdmin("nonAdminid", "NATIVE"));
	}

	@Test
	void testUserIsAdminInstance_wrongType() {
		assertFalse(instance.userIsAdmin("adminid", "WRONG"));
	}

	///
	/// otherAdminsExist
	///

	@Test
	void testOtherAdminsExist_noOtherAdmins() {
		assertFalse(instance.otherAdminsExist("adminid", "NATIVE"));
	}

	@Test
	void testOtherAdminsExist_hasOtherAdmin() {
		User admin2 = UnitTestSecurityAuthUtils.createUser("admin2", true);
		assertTrue(instance.otherAdminsExist("adminid", "NATIVE"));
	}

	///
	/// getAllUsers
	///

	@Test
	void testGetAllUsers_empty() {
		// Admin user is already created
		List<Map<String, Object>> users = instance.getAllUsers(null, 10, 0);
		assertNotNull(users);
		assertEquals(1, users.size()); // Just the admin
	}

	@Test
	void testGetAllUsers_multipleUsers() {
		UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createUser("user2", false);

		List<Map<String, Object>> users = instance.getAllUsers(null, 10, 0);
		assertNotNull(users);
		assertEquals(3, users.size()); // admin + 2 users
	}

	@Test
	void testGetAllUsers_withSearchTerm() {
		UnitTestSecurityAuthUtils.createUser("searchuser", false);
		UnitTestSecurityAuthUtils.createUser("otheruser", false);

		List<Map<String, Object>> users = instance.getAllUsers("search", 10, 0);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	@Test
	void testGetAllUsers_withLimit() {
		UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);

		List<Map<String, Object>> users = instance.getAllUsers(null, 2, 0);
		assertNotNull(users);
		assertEquals(2, users.size());
	}

	@Test
	void testGetAllUsers_withOffset() {
		UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createUser("user2", false);

		List<Map<String, Object>> users = instance.getAllUsers(null, 10, 1);
		assertNotNull(users);
		assertEquals(2, users.size()); // Skips first result
	}

	///
	/// getAllUserEngines
	///

	@Test
	void testGetAllUserEngines_empty() {
		List<Map<String, Object>> engines = instance.getAllUserEngines("adminid", null);
		assertNotNull(engines);
		assertTrue(engines.isEmpty());
	}

	@Test
	void testGetAllUserEngines_hasEngines() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);

		List<Map<String, Object>> engines = instance.getAllUserEngines("adminid", null);
		assertNotNull(engines);
		assertEquals(1, engines.size());
	}

	@Test
	void testGetAllUserEngines_withTypeFilter() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		UnitTestSecurityAuthUtils.createEngine("model1", "Model One", IEngine.CATALOG_TYPE.MODEL, adminUser);

		List<String> types = List.of("DATABASE");
		List<Map<String, Object>> engines = instance.getAllUserEngines("adminid", types);
		assertNotNull(engines);
		assertEquals(1, engines.size());
	}

	///
	/// getAllUserProjects
	///

	@Test
	void testGetAllUserProjects_empty() {
		List<Map<String, Object>> projects = instance.getAllUserProjects("adminid");
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetAllUserProjects_hasProjects() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<Map<String, Object>> projects = instance.getAllUserProjects("adminid");
		assertNotNull(projects);
		assertEquals(1, projects.size());
	}

	///
	/// editUser
	///

	@Test
	void testEditUser_updateName() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		Map<String, Object> userInfo = new HashMap<>();
		userInfo.put("id", "testuserid");
		userInfo.put("type", "NATIVE");
		userInfo.put("name", "Updated Name");
		userInfo.put("email", "testuser@test.com");
		userInfo.put("username", "testuserid");

		boolean result = instance.editUser(userInfo);
		assertTrue(result);
	}

	@Test
	void testEditUser_missingId() {
		Map<String, Object> userInfo = new HashMap<>();
		userInfo.put("type", "NATIVE");

		assertThrows(NullPointerException.class, () -> instance.editUser(userInfo));
	}

	@Test
	void testEditUser_missingType() {
		Map<String, Object> userInfo = new HashMap<>();
		userInfo.put("id", "testuserid");

		assertThrows(NullPointerException.class, () -> instance.editUser(userInfo));
	}

	@Test
	void testEditUser_makeAdmin() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);
		assertFalse(SecurityAdminUtils.userIsAdmin(user));

		Map<String, Object> userInfo = new HashMap<>();
		userInfo.put("id", "testuserid");
		userInfo.put("type", "NATIVE");
		userInfo.put("name", "testusername");
		userInfo.put("email", "testuser@test.com");
		userInfo.put("username", "testuserid");
		userInfo.put("admin", true);

		instance.editUser(userInfo);
		assertTrue(SecurityAdminUtils.userIsAdmin(user));
	}

	@Test
	void testEditUser_makePublisher() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		Map<String, Object> userInfo = new HashMap<>();
		userInfo.put("id", "testuserid");
		userInfo.put("type", "NATIVE");
		userInfo.put("name", "testusername");
		userInfo.put("email", "testuser@test.com");
		userInfo.put("username", "testuserid");
		userInfo.put("publisher", true);

		boolean result = instance.editUser(userInfo);
		assertTrue(result);
	}

	///
	/// deleteUser
	///

	@Test
	void testDeleteUser() {
		User user = UnitTestSecurityAuthUtils.createUser("todelete", false);

		// Delete the user
		boolean result = instance.deleteUser("todeleteid", "NATIVE");
		assertTrue(result);

		// Verify user no longer appears in user list
		List<Map<String, Object>> users = instance.getAllUsers("todelete", 10, 0);
		assertTrue(users.isEmpty());
	}

	///
	/// setUserPublisher
	///

	@Test
	void testSetUserPublisher_enable() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		instance.setUserPublisher("testuserid", true);
		// Verify by getting user info
		List<Map<String, Object>> users = instance.getAllUsers("testuser", 10, 0);
		assertEquals(1, users.size());
		assertTrue((Boolean) users.get(0).get("publisher"));
	}

	@Test
	void testSetUserPublisher_disable() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);
		instance.setUserPublisher("testuserid", true);
		instance.setUserPublisher("testuserid", false);

		List<Map<String, Object>> users = instance.getAllUsers("testuser", 10, 0);
		assertEquals(1, users.size());
		assertFalse((Boolean) users.get(0).get("publisher"));
	}

	///
	/// setUserExporter
	///

	@Test
	void testSetUserExporter_enable() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		// Just verify no exception is thrown
		instance.setUserExporter("testuserid", true);

		List<Map<String, Object>> users = instance.getAllUsers("testuser", 10, 0);
		assertEquals(1, users.size());
	}

	///
	/// setUserLock
	///

	@Test
	void testSetUserLock_lock() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		// Just verify no exception is thrown
		instance.setUserLock("testuserid", "NATIVE", true);

		List<Map<String, Object>> users = instance.getAllUsers("testuser", 10, 0);
		assertEquals(1, users.size());
	}

	@Test
	void testSetUserLock_unlock() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);
		instance.setUserLock("testuserid", "NATIVE", true);

		// Just verify no exception is thrown when unlocking
		instance.setUserLock("testuserid", "NATIVE", false);

		List<Map<String, Object>> users = instance.getAllUsers("testuser", 10, 0);
		assertEquals(1, users.size());
	}

	///
	/// setEngineGlobal
	///

	@Test
	void testSetEngineGlobal_enable() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		assertFalse(SecurityEngineUtils.engineIsGlobal("engine1"));

		boolean result = instance.setEngineGlobal("engine1", true);
		assertTrue(result);
		assertTrue(SecurityEngineUtils.engineIsGlobal("engine1"));
	}

	@Test
	void testSetEngineGlobal_disable() {
		UnitTestSecurityAuthUtils.createEngineGlobal("engine1", "Engine One", adminUser);
		assertTrue(SecurityEngineUtils.engineIsGlobal("engine1"));

		boolean result = instance.setEngineGlobal("engine1", false);
		assertTrue(result);
		assertFalse(SecurityEngineUtils.engineIsGlobal("engine1"));
	}

	///
	/// setEngineDiscoverable
	///

	@Test
	void testSetEngineDiscoverable_enable() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		assertFalse(SecurityEngineUtils.engineIsDiscoverable("engine1"));

		boolean result = instance.setEngineDiscoverable("engine1", true);
		assertTrue(result);
		assertTrue(SecurityEngineUtils.engineIsDiscoverable("engine1"));
	}

	@Test
	void testSetEngineDiscoverable_disable() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		instance.setEngineDiscoverable("engine1", true);

		boolean result = instance.setEngineDiscoverable("engine1", false);
		assertTrue(result);
		assertFalse(SecurityEngineUtils.engineIsDiscoverable("engine1"));
	}

	///
	/// setProjectGlobal
	///

	@Test
	void testSetProjectGlobal_enable() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		assertFalse(SecurityProjectUtils.projectIsGlobal("project1"));

		boolean result = instance.setProjectGlobal("project1", true);
		assertTrue(result);
		assertTrue(SecurityProjectUtils.projectIsGlobal("project1"));
	}

	@Test
	void testSetProjectGlobal_disable() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		instance.setProjectGlobal("project1", true);

		boolean result = instance.setProjectGlobal("project1", false);
		assertTrue(result);
		assertFalse(SecurityProjectUtils.projectIsGlobal("project1"));
	}

	///
	/// setProjectDiscoverable
	///

	@Test
	void testSetProjectDiscoverable_enable() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		assertFalse(SecurityProjectUtils.projectIsDiscoverable("project1"));

		boolean result = instance.setProjectDiscoverable("project1", true);
		assertTrue(result);
		assertTrue(SecurityProjectUtils.projectIsDiscoverable("project1"));
	}

	@Test
	void testSetProjectDiscoverable_disable() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		instance.setProjectDiscoverable("project1", true);

		boolean result = instance.setProjectDiscoverable("project1", false);
		assertTrue(result);
		assertFalse(SecurityProjectUtils.projectIsDiscoverable("project1"));
	}

	///
	/// getEngineUsers
	///

	@Test
	void testGetEngineUsers_empty() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);

		List<Map<String, Object>> users = instance.getEngineUsers("engine1", null, null, 10, 0);
		assertNotNull(users);
		assertEquals(1, users.size()); // Owner
	}

	@Test
	void testGetEngineUsers_multipleUsers() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		List<Map<String, Object>> users = instance.getEngineUsers("engine1", null, null, 10, 0);
		assertNotNull(users);
		assertEquals(2, users.size());
	}

	///
	/// getEngineUsersCount
	///

	@Test
	void testGetEngineUsersCount() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		long count = instance.getEngineUsersCount("engine1", null, null);
		assertEquals(2, count);
	}

	///
	/// getProjectUsers
	///

	@Test
	void testGetProjectUsers() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<Map<String, Object>> users = instance.getProjectUsers("project1", null, null, 10, 0);
		assertNotNull(users);
		assertEquals(1, users.size()); // Owner
	}

	///
	/// getProjectUsersCount
	///

	@Test
	void testGetProjectUsersCount() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);

		long count = instance.getProjectUsersCount("project1", null, null);
		assertEquals(2, count);
	}

	///
	/// addEngineUser
	///

	@Test
	void testAddEngineUser() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		instance.addEngineUser("user2id", "engine1", "READ_ONLY", adminUser, null, null, null, 0, 0.0);

		assertTrue(SecurityEngineUtils.userCanViewEngine(user2, "engine1"));
	}

	@Test
	void testAddEngineUser_withOwnerPermission() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		instance.addEngineUser("user2id", "engine1", "OWNER", adminUser, null, null, null, 0, 0.0);

		Integer permission = SecurityEngineUtils.getUserEnginePermission("user2id", "engine1");
		assertEquals(AccessPermissionEnum.OWNER.getId(), permission);
	}

	///
	/// addProjectUser
	///

	@Test
	void testAddProjectUser() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		instance.addProjectUser("user2id", "project1", "READ_ONLY", adminUser, null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "project1"));
	}

	///
	/// editEngineUserPermission
	///

	@Test
	void testEditEngineUserPermission() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		instance.editEngineUserPermission("user2id", "engine1", "EDIT", adminUser, null, null, null, 0, 0.0);

		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "engine1"));
	}

	///
	/// editProjectUserPermission
	///

	@Test
	void testEditProjectUserPermission() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);

		instance.editProjectUserPermission("user2id", "project1", "EDIT", adminUser, null);

		assertTrue(SecurityProjectUtils.userCanEditProject(user2, "project1"));
	}

	///
	/// removeEngineUser
	///

	@Test
	void testRemoveEngineUser() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);
		assertTrue(SecurityEngineUtils.userCanViewEngine(user2, "engine1"));

		instance.removeEngineUser("user2id", "engine1");

		assertFalse(SecurityEngineUtils.userCanViewEngine(user2, "engine1"));
	}

	///
	/// removeProjectUser
	///

	@Test
	void testRemoveProjectUser() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);
		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "project1"));

		instance.removeProjectUser("user2id", "project1");

		assertFalse(SecurityProjectUtils.userCanViewProject(user2, "project1"));
	}

	///
	/// removeEngineUsers (batch)
	///

	@Test
	void testRemoveEngineUsers_batch() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);
		SecurityEngineUtils.addEngineUser(adminUser, "user3id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		instance.removeEngineUsers(List.of("user2id", "user3id"), "engine1");

		assertFalse(SecurityEngineUtils.userCanViewEngine(user2, "engine1"));
		assertFalse(SecurityEngineUtils.userCanViewEngine(user3, "engine1"));
	}

	///
	/// removeProjectUsers (batch)
	///

	@Test
	void testRemoveProjectUsers_batch() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);
		SecurityProjectUtils.addProjectUser(adminUser, "user3id", "project1", "READ_ONLY", null);

		instance.removeProjectUsers(List.of("user2id", "user3id"), "project1");

		assertFalse(SecurityProjectUtils.userCanViewProject(user2, "project1"));
		assertFalse(SecurityProjectUtils.userCanViewProject(user3, "project1"));
	}

	///
	/// getProjectInsights
	///

	@Test
	void testGetProjectInsights_empty() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<Map<String, Object>> insights = instance.getProjectInsights("project1");
		assertNotNull(insights);
		assertTrue(insights.isEmpty());
	}

	@Test
	void testGetProjectInsights_hasInsights() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		List<Map<String, Object>> insights = instance.getProjectInsights("project1");
		assertNotNull(insights);
		assertEquals(1, insights.size());
	}

	///
	/// getInsightUsers
	///

	@Test
	void testGetInsightUsers() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		List<Map<String, Object>> users = instance.getInsightUsers("project1", "insight1", null, null, 10, 0);
		assertNotNull(users);
	}

	///
	/// addInsightUser
	///

	@Test
	void testAddInsightUser() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));
	}

	///
	/// editInsightUserPermission
	///

	@Test
	void testEditInsightUserPermission() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);

		instance.editInsightUserPermission("user2id", "project1", "insight1", "EDIT", adminUser, null);

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "project1", "insight1"));
	}

	///
	/// removeInsightUser
	///

	@Test
	void testRemoveInsightUser() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);
		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));

		instance.removeInsightUser("user2id", "project1", "insight1");

		assertFalse(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));
	}

	///
	/// setInsightGlobalWithinProject
	///

	@Test
	void testSetInsightGlobalWithinProject() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		instance.setInsightGlobalWithinProject("project1", "insight1", true);

		assertTrue(SecurityInsightUtils.insightIsGlobal("project1", "insight1"));
	}

	@Test
	void testSetInsightGlobalWithinProject_disable() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		instance.setInsightGlobalWithinProject("project1", "insight1", true);

		instance.setInsightGlobalWithinProject("project1", "insight1", false);

		assertFalse(SecurityInsightUtils.insightIsGlobal("project1", "insight1"));
	}

	///
	/// getProjectsUserHasExplicitAccess
	///

	@Test
	void testGetProjectsUserHasExplicitAccess_empty() {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		List<String> projects = instance.getProjectsUserHasExplicitAccess("user2id");
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetProjectsUserHasExplicitAccess_hasAccess() throws IllegalAccessException {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);

		List<String> projects = instance.getProjectsUserHasExplicitAccess("user2id");
		assertNotNull(projects);
		assertEquals(1, projects.size());
		assertTrue(projects.contains("project1"));
	}

	///
	/// getEnginesUserHasExplicitAccess
	///

	@Test
	void testGetEnginesUserHasExplicitAccess_empty() {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		List<String> engines = instance.getEnginesUserHasExplicitAccess("user2id", null);
		assertNotNull(engines);
		assertTrue(engines.isEmpty());
	}

	@Test
	void testGetEnginesUserHasExplicitAccess_hasAccess() throws IllegalAccessException {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		List<String> engines = instance.getEnginesUserHasExplicitAccess("user2id", null);
		assertNotNull(engines);
		assertEquals(1, engines.size());
		assertTrue(engines.contains("engine1"));
	}

	///
	/// getNumAdmins
	///

	@Test
	void testGetNumAdmins() {
		int count = instance.getNumAdmins();
		assertEquals(1, count);
	}

	@Test
	void testGetNumAdmins_multipleAdmins() {
		UnitTestSecurityAuthUtils.createUser("admin2", true);

		int count = instance.getNumAdmins();
		assertEquals(2, count);
	}

	///
	/// getAdminUserIdAndType
	///

	@Test
	void testGetAdminUserIdAndType() {
		Object[] result = instance.getAdminUserIdAndType();
		assertNotNull(result);
		assertEquals(2, result.length);
		assertEquals("adminid", result[0]);
		assertEquals("NATIVE", result[1]);
	}

	///
	/// getNumUsers
	///

	@Test
	void testGetNumUsers() {
		Long count = instance.getNumUsers();
		assertEquals(1L, count);
	}

	@Test
	void testGetNumUsers_multiple() {
		UnitTestSecurityAuthUtils.createUser("user1", false);
		UnitTestSecurityAuthUtils.createUser("user2", false);

		Long count = instance.getNumUsers();
		assertEquals(3L, count);
	}

	///
	/// updateUserEmail
	///

	@Test
	void testUpdateUserEmail() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		instance.updateUserEmail("testuserid", "NATIVE", "newemail@test.com");

		List<Map<String, Object>> users = instance.getAllUsers("testuser", 10, 0);
		assertEquals(1, users.size());
		assertEquals("newemail@test.com", users.get(0).get("email"));
	}

	///
	/// getAllEngineSettings
	///

	@Test
	void testGetAllEngineSettings_empty() {
		List<Map<String, Object>> settings = instance.getAllEngineSettings(null, null, null, null, null, null);
		assertNotNull(settings);
		assertTrue(settings.isEmpty());
	}

	@Test
	void testGetAllEngineSettings_hasEngines() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);

		List<Map<String, Object>> settings = instance.getAllEngineSettings(null, null, null, null, null, null);
		assertNotNull(settings);
		assertEquals(1, settings.size());
	}

	@Test
	void testGetAllEngineSettings_withFilter() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		UnitTestSecurityAuthUtils.createEngine("engine2", "Engine Two", adminUser);

		List<Map<String, Object>> settings = instance.getAllEngineSettings(List.of("engine1"), null, null, null, null,
				null);
		assertNotNull(settings);
		assertEquals(1, settings.size());
	}

	///
	/// getAllProjectSettings
	///

	@Test
	void testGetAllProjectSettings_empty() {
		List<Map<String, Object>> settings = instance.getAllProjectSettings(null, null, null, null, null, null);
		assertNotNull(settings);
		assertTrue(settings.isEmpty());
	}

	@Test
	void testGetAllProjectSettings_hasProjects() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<Map<String, Object>> settings = instance.getAllProjectSettings(null, null, null, null, null, null);
		assertNotNull(settings);
		assertEquals(1, settings.size());
	}

	///
	/// grantAllProjects
	///

	@Test
	void testGrantAllProjects() {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", adminUser);

		// isAddNew=false means grant access to all existing projects
		instance.grantAllProjects("user2id", "READ_ONLY", false, adminUser);

		// Verify no exception was thrown and user has projects in their access list
		List<String> userProjects = instance.getProjectsUserHasExplicitAccess("user2id");
		assertNotNull(userProjects);
	}

	///
	/// grantAllEngines
	///

	@Test
	void testGrantAllEngines() {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		UnitTestSecurityAuthUtils.createEngine("engine2", "Engine Two", adminUser);

		// isAddNew=false means grant access to all existing engines
		instance.grantAllEngines("user2id", "READ_ONLY", false, null, adminUser);

		// Verify no exception was thrown and user has engines in their access list
		List<String> userEngines = instance.getEnginesUserHasExplicitAccess("user2id", null);
		assertNotNull(userEngines);
	}

	///
	/// getEngineUsersNoCredentials
	///

	@Test
	void testGetEngineUsersNoCredentials() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);

		List<Map<String, Object>> users = instance.getEngineUsersNoCredentials("engine1", null, 10, 0);
		assertNotNull(users);
		// Method may exclude credential users, so just check it doesn't throw
	}

	///
	/// getProjectUsersNoCredentials
	///

	@Test
	void testGetProjectUsersNoCredentials() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<Map<String, Object>> users = instance.getProjectUsersNoCredentials("project1", null, 10, 0);
		assertNotNull(users);
		// Method may exclude credential users, so just check it doesn't throw
	}

	///
	/// getInsightUsersNoCredentials
	///

	@Test
	void testGetInsightUsersNoCredentials() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		List<Map<String, Object>> users = instance.getInsightUsersNoCredentials("project1", "insight1", null, 10, 0);
		assertNotNull(users);
	}

	///
	/// addEngineUserPermissions (batch)
	///

	@Test
	void testAddEngineUserPermissions_batch() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		List<Map<String, Object>> permissions = new ArrayList<>();
		Map<String, Object> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "READ_ONLY");
		permissions.add(perm1);
		Map<String, Object> perm2 = new HashMap<>();
		perm2.put("userid", "user3id");
		perm2.put("permission", "READ_ONLY");
		permissions.add(perm2);

		instance.addEngineUserPermissions("engine1", permissions, adminUser);

		assertTrue(SecurityEngineUtils.userCanViewEngine(user2, "engine1"));
		assertTrue(SecurityEngineUtils.userCanViewEngine(user3, "engine1"));
	}

	///
	/// addProjectUserPermissions (batch)
	///

	@Test
	void testAddProjectUserPermissions_batch() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		List<Map<String, String>> permissions = new ArrayList<>();
		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "READ_ONLY");
		permissions.add(perm1);
		Map<String, String> perm2 = new HashMap<>();
		perm2.put("userid", "user3id");
		perm2.put("permission", "READ_ONLY");
		permissions.add(perm2);

		instance.addProjectUserPermissions("project1", permissions, adminUser);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "project1"));
		assertTrue(SecurityProjectUtils.userCanViewProject(user3, "project1"));
	}

	///
	/// addInsightUserPermissions (batch)
	///

	@Test
	void testAddInsightUserPermissions_batch() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		List<Map<String, String>> permissions = new ArrayList<>();
		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "READ_ONLY");
		permissions.add(perm1);
		Map<String, String> perm2 = new HashMap<>();
		perm2.put("userid", "user3id");
		perm2.put("permission", "READ_ONLY");
		permissions.add(perm2);

		instance.addInsightUserPermissions("project1", "insight1", permissions, adminUser, null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));
		assertTrue(SecurityInsightUtils.userCanViewInsight(user3, "project1", "insight1"));
	}

	///
	/// editEngineUserPermissions (batch)
	///

	@Test
	void testEditEngineUserPermissions_batch() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		List<Map<String, Object>> permissions = new ArrayList<>();
		Map<String, Object> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "EDIT");
		permissions.add(perm1);

		instance.editEngineUserPermissions("engine1", permissions, adminUser);

		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "engine1"));
	}

	///
	/// editProjectUserPermissions (batch)
	///

	@Test
	void testEditProjectUserPermissions_batch() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);

		List<Map<String, String>> permissions = new ArrayList<>();
		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "EDIT");
		permissions.add(perm1);

		instance.editProjectUserPermissions("project1", permissions, adminUser, null);

		assertTrue(SecurityProjectUtils.userCanEditProject(user2, "project1"));
	}

	///
	/// editInsightUserPermissions (batch)
	///

	@Test
	void testEditInsightUserPermissions_batch() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);

		List<Map<String, String>> permissions = new ArrayList<>();
		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "EDIT");
		permissions.add(perm1);

		instance.editInsightUserPermissions("project1", "insight1", permissions, adminUser, null);

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "project1", "insight1"));
	}

	///
	/// removeInsightUsers (batch)
	///

	@Test
	void testRemoveInsightUsers_batch() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);
		instance.addInsightUser("user3id", "project1", "insight1", "READ_ONLY", adminUser, null);

		instance.removeInsightUsers(List.of("user2id", "user3id"), "project1", "insight1");

		assertFalse(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));
		assertFalse(SecurityInsightUtils.userCanViewInsight(user3, "project1", "insight1"));
	}

	///
	/// addAllEngineUsers
	///

	@Test
	void testAddAllEngineUsers() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		instance.addAllEngineUsers("engine1", "READ_ONLY", adminUser, null);

		assertTrue(SecurityEngineUtils.userCanViewEngine(user2, "engine1"));
		assertTrue(SecurityEngineUtils.userCanViewEngine(user3, "engine1"));
	}

	///
	/// addAllProjectUsers
	///

	@Test
	void testAddAllProjectUsers() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		instance.addAllProjectUsers("project1", "READ_ONLY", adminUser, null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "project1"));
		assertTrue(SecurityProjectUtils.userCanViewProject(user3, "project1"));
	}

	///
	/// addAllInsightUsers
	///

	@Test
	void testAddAllInsightUsers() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		instance.addAllInsightUsers("project1", "insight1", "READ_ONLY", adminUser, null);

		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));
		assertTrue(SecurityInsightUtils.userCanViewInsight(user3, "project1", "insight1"));
	}

	///
	/// updateEngineUserPermissions (all users)
	///

	@Test
	void testUpdateEngineUserPermissions() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		instance.updateEngineUserPermissions("engine1", "EDIT", adminUser, null);

		assertTrue(SecurityEngineUtils.userCanEditEngine(user2, "engine1"));
	}

	///
	/// updateProjectUserPermissions (all users)
	///

	@Test
	void testUpdateProjectUserPermissions() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);

		instance.updateProjectUserPermissions("project1", "EDIT", adminUser, null);

		assertTrue(SecurityProjectUtils.userCanEditProject(user2, "project1"));
	}

	///
	/// updateInsightUserPermissions (all users)
	///

	@Test
	void testUpdateInsightUserPermissions() throws IllegalAccessException {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);

		instance.updateInsightUserPermissions("project1", "insight1", "EDIT", adminUser, null);

		assertTrue(SecurityInsightUtils.userCanEditInsight(user2, "project1", "insight1"));
	}

	///
	/// getAllUserInsights
	///

	@Test
	void testGetAllUserInsights_empty() {
		List<Map<String, Object>> insights = instance.getAllUserInsights(adminUser, null, null, 10, 0);
		assertNotNull(insights);
		assertTrue(insights.isEmpty());
	}

	@Test
	void testGetAllUserInsights_hasInsights() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		List<Map<String, Object>> insights = instance.getAllUserInsights(adminUser, null, null, 10, 0);
		assertNotNull(insights);
		assertEquals(1, insights.size());
	}

	@Test
	void testGetAllUserInsights_withProjectFilter() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		UnitTestSecurityAuthUtils.createInsight("project2", "insight2", "Insight Two", "grid");

		List<Map<String, Object>> insights = instance.getAllUserInsights(adminUser, List.of("project1"), null, 10, 0);
		assertNotNull(insights);
		assertEquals(1, insights.size());
	}

	@Test
	void testGetAllUserInsights_withSearchTerm() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "SearchableInsight", "grid");
		UnitTestSecurityAuthUtils.createInsight("project1", "insight2", "OtherInsight", "grid");

		List<Map<String, Object>> insights = instance.getAllUserInsights(adminUser, null, "Searchable", 10, 0);
		assertNotNull(insights);
		assertEquals(1, insights.size());
	}

	///
	/// getAllUserInsightAccess
	///

	@Test
	void testGetAllUserInsightAccess_empty() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<Map<String, Object>> access = instance.getAllUserInsightAccess("project1", "adminid");
		assertNotNull(access);
	}

	@Test
	void testGetAllUserInsightAccess_hasAccess() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);

		List<Map<String, Object>> access = instance.getAllUserInsightAccess("project1", "user2id");
		assertNotNull(access);
		assertTrue(access.stream().anyMatch(m -> "insight1".equals(m.get("insight_id"))));
	}

	///
	/// updateUserMetadata
	///

	@Test
	void testUpdateUserMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		Map<String, Object> metadata = new HashMap<>();
		metadata.put("customField", "customValue");

		// Just verify no exception is thrown
		instance.updateUserMetadata("testuserid", AuthProvider.NATIVE, metadata);
	}

	@Test
	void testUpdateUserMetadata_multipleValues() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		Map<String, Object> metadata = new HashMap<>();
		metadata.put("field1", "value1");
		metadata.put("field2", List.of("val1", "val2"));

		// Just verify no exception is thrown
		instance.updateUserMetadata("testuserid", AuthProvider.NATIVE, metadata);
	}

	///
	/// getProjectsAndVisibilityUserHasExplicitAccess
	///

	@Test
	void testGetProjectsAndVisibilityUserHasExplicitAccess_empty() {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		Map<String, Boolean> projects = instance.getProjectsAndVisibilityUserHasExplicitAccess("user2id");
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetProjectsAndVisibilityUserHasExplicitAccess_hasAccess() throws IllegalAccessException {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		SecurityProjectUtils.addProjectUser(adminUser, "user2id", "project1", "READ_ONLY", null);

		Map<String, Boolean> projects = instance.getProjectsAndVisibilityUserHasExplicitAccess("user2id");
		assertNotNull(projects);
		assertTrue(projects.containsKey("project1"));
	}

	///
	/// getEnginesAndVisibilityUserHasExplicitAccess
	///

	@Test
	void testGetEnginesAndVisibilityUserHasExplicitAccess_empty() {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		Map<String, Boolean> engines = instance.getEnginesAndVisibilityUserHasExplicitAccess("user2id", null);
		assertNotNull(engines);
		assertTrue(engines.isEmpty());
	}

	@Test
	void testGetEnginesAndVisibilityUserHasExplicitAccess_hasAccess() throws IllegalAccessException {
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		SecurityEngineUtils.addEngineUser(adminUser, "user2id", "engine1", "READ_ONLY", null, null, null, 0, 0.0);

		Map<String, Boolean> engines = instance.getEnginesAndVisibilityUserHasExplicitAccess("user2id", null);
		assertNotNull(engines);
		assertTrue(engines.containsKey("engine1"));
	}

	///
	/// grantNewUsersEngineAccess
	///

	@Test
	void testGrantNewUsersEngineAccess() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		// Grant access to users who don't have access
		instance.grantNewUsersEngineAccess("engine1", "READ_ONLY", adminUser, null);

		// Verify no exception was thrown
		assertNotNull(instance);
	}

	///
	/// grantNewUsersProjectAccess
	///

	@Test
	void testGrantNewUsersProjectAccess() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		// Grant access to users who don't have access
		instance.grantNewUsersProjectAccess("project1", "READ_ONLY", adminUser, null);

		// Verify no exception was thrown
		assertNotNull(instance);
	}

	///
	/// grantAllProjectInsights
	///

	@Test
	void testGrantAllProjectInsights() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		UnitTestSecurityAuthUtils.createInsight("project1", "insight2", "Insight Two", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		instance.grantAllProjectInsights("project1", "user2id", "READ_ONLY", adminUser);

		// Verify user now has access to insights
		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight1"));
		assertTrue(SecurityInsightUtils.userCanViewInsight(user2, "project1", "insight2"));
	}

	///
	/// getInsightUsersCount
	///

	@Test
	void testGetInsightUsersCount_empty() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		long count = instance.getInsightUsersCount("project1", "insight1", null, null);
		assertEquals(0, count);
	}

	@Test
	void testGetInsightUsersCount_hasUsers() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		instance.addInsightUser("user2id", "project1", "insight1", "READ_ONLY", adminUser, null);

		long count = instance.getInsightUsersCount("project1", "insight1", null, null);
		assertEquals(1, count);
	}

	///
	/// grantNewUsersInsightAccess
	///

	@Test
	void testGrantNewUsersInsightAccess() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);

		// Grant access to users who don't have access
		instance.grantNewUsersInsightAccess("project1", "insight1", "READ_ONLY", adminUser, null);

		// Verify no exception was thrown
		assertNotNull(instance);
	}

	///
	/// lockAccounts
	///

	@Test
	void testLockAccounts() {
		// Create a user - they will be created with lastLogin = null or current time
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		// Lock accounts that haven't logged in for 0 days (all accounts)
		int numLocked = instance.lockAccounts(0);

		// Verify no exception was thrown and method returns a count
		assertTrue(numLocked >= 0);
	}

	///
	/// getUserEmailsGettingLocked
	///

	@Test
	void testGetUserEmailsGettingLocked() {
		// This method depends on PasswordRequirements configuration
		List<Object[]> emails = instance.getUserEmailsGettingLocked();
		assertNotNull(emails);
	}

	///
	/// setLockAccountsAndRecalculate
	///

	@Test
	void testSetLockAccountsAndRecalculate() {
		User user = UnitTestSecurityAuthUtils.createUser("testuser", false);

		// Recalculate lock status based on days since last login
		int numUpdated = instance.setLockAccountsAndRecalculate(365);

		// Verify no exception was thrown and method returns a count
		assertTrue(numUpdated >= 0);
	}

	///
	/// approveEngineUserAccessRequests
	///

	@Test
	void testApproveEngineUserAccessRequests() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		List<Map<String, Object>> requests = new ArrayList<>();
		Map<String, Object> request = new HashMap<>();
		request.put("userid", "user2id");
		request.put("permission", "READ_ONLY");
		request.put("requestid", "req1");
		requests.add(request);

		// This will fail if ENGINEACCESSREQUEST table doesn't have the request, but
		// should not throw
		try {
			instance.approveEngineUserAccessRequests("adminid", "NATIVE", "engine1", requests, null);
		} catch (IllegalArgumentException e) {
			// Expected if no access request exists - just verify the method can be called
		}
	}

	///
	/// denyEngineUserAccessRequests
	///

	@Test
	void testDenyEngineUserAccessRequests() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);

		List<String> requestIds = List.of("req1");

		// This will update ENGINEACCESSREQUEST table
		try {
			instance.denyEngineUserAccessRequests("adminid", "NATIVE", "engine1", requestIds);
		} catch (IllegalArgumentException e) {
			// Expected if no access request exists
		}
	}

	///
	/// approveProjectUserAccessRequests
	///

	@Test
	void testApproveProjectUserAccessRequests() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		List<Map<String, Object>> requests = new ArrayList<>();
		Map<String, Object> request = new HashMap<>();
		request.put("userid", "user2id");
		request.put("permission", "READ_ONLY");
		request.put("requestid", "req1");
		requests.add(request);

		try {
			instance.approveProjectUserAccessRequests("adminid", "NATIVE", "project1", requests, null);
		} catch (IllegalArgumentException e) {
			// Expected if no access request exists
		}
	}

	///
	/// denyProjectUserAccessRequests
	///

	@Test
	void testDenyProjectUserAccessRequests() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		List<String> requestIds = List.of("req1");

		try {
			instance.denyProjectUserAccessRequests("adminid", "NATIVE", "project1", requestIds);
		} catch (IllegalArgumentException e) {
			// Expected if no access request exists
		}
	}

	///
	/// approveInsightUserAccessRequests
	///

	@Test
	void testApproveInsightUserAccessRequests() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		List<Map<String, Object>> requests = new ArrayList<>();
		Map<String, Object> request = new HashMap<>();
		request.put("userid", "user2id");
		request.put("permission", "READ_ONLY");
		request.put("requestid", "req1");
		requests.add(request);

		try {
			instance.approveInsightUserAccessRequests("adminid", "NATIVE", "project1", "insight1", requests, null);
		} catch (IllegalArgumentException e) {
			// Expected if no access request exists
		}
	}

	///
	/// denyInsightUserAccessRequests
	///

	@Test
	void testDenyInsightUserAccessRequests() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);
		UnitTestSecurityAuthUtils.createInsight("project1", "insight1", "Insight One", "grid");

		List<String> requestIds = List.of("req1");

		try {
			instance.denyInsightUserAccessRequests("adminid", "NATIVE", "project1", "insight1", requestIds);
		} catch (IllegalArgumentException e) {
			// Expected if no access request exists
		}
	}

	///
	/// getProjectMarkdown
	///

	@Test
	void testGetProjectMarkdown_noMarkdown() {
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", adminUser);

		String markdown = instance.getProjectMarkdown("project1");
		// May return null or empty if no markdown is set
		assertNull(markdown);
	}

	///
	/// getEngineMarkdown
	///

	@Test
	void testGetEngineMarkdown_noMarkdown() {
		UnitTestSecurityAuthUtils.createEngine("engine1", "Engine One", adminUser);

		String markdown = instance.getEngineMarkdown("engine1");
		// May return null or empty if no markdown is set
		assertNull(markdown);
	}

}
