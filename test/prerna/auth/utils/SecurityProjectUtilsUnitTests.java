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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.notifications.NotificationDbUtils;
import prerna.project.api.IProject;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;
import prerna.util.InsightsRDBMSUtils;
import prerna.util.NotificationConstants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class SecurityProjectUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;

	private List<String> tables = new ArrayList<>();

	/** The projects a test put in DIHelper, taken out again after it */
	private List<String> stagedProjects = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);
	}

	@AfterEach
	void cleanup() throws SQLException {
		for (String projectId : stagedProjects) {
			DIHelper.getInstance().removeProjectProperty(projectId);
			DIHelper.getInstance().removeProjectProperty(projectId + "_" + Constants.STORE);
		}
		stagedProjects.clear();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// projectExists
	///

	@Test
	void testProjectExists_false() {
		boolean exists = SecurityProjectUtils.projectExists("nonExistentProjectId");
		assertFalse(exists);
	}

	@Test
	void testProjectExists_true() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean exists = SecurityProjectUtils.projectExists("testProjectId");
		assertTrue(exists);
	}

	///
	/// projectIsGlobal
	///

	@Test
	void testProjectIsGlobal_notGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean isGlobal = SecurityProjectUtils.projectIsGlobal("testProjectId");
		assertFalse(isGlobal);
	}

	@Test
	void testProjectIsGlobal_true() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		boolean isGlobal = SecurityProjectUtils.projectIsGlobal("testProjectId");
		assertTrue(isGlobal);
	}

	///
	/// projectIsDiscoverable
	///

	@Test
	void testProjectIsDiscoverable_false() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean discoverable = SecurityProjectUtils.projectIsDiscoverable("testProjectId");
		assertFalse(discoverable);
	}

	@Test
	void testProjectIsDiscoverable_true() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		boolean discoverable = SecurityProjectUtils.projectIsDiscoverable("testProjectId");
		assertTrue(discoverable);
	}

	///
	/// userIsOwner
	///

	@Test
	void testUserIsOwner_true() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean isOwner = SecurityProjectUtils.userIsOwner(user, "testProjectId");
		assertTrue(isOwner);
	}

	@Test
	void testUserIsOwner_false() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean isOwner = SecurityProjectUtils.userIsOwner(user2, "testProjectId");
		assertFalse(isOwner);
	}

	///
	/// userCanViewProject
	///

	@Test
	void testUserCanViewProject_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean canView = SecurityProjectUtils.userCanViewProject(user, "testProjectId");
		assertTrue(canView);
	}

	@Test
	void testUserCanViewProject_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean canView = SecurityProjectUtils.userCanViewProject(user2, "testProjectId");
		assertFalse(canView);
	}

	@Test
	void testUserCanViewProject_globalProject() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		boolean canView = SecurityProjectUtils.userCanViewProject(user2, "testProjectId");
		assertTrue(canView);
	}

	///
	/// userCanEditProject
	///

	@Test
	void testUserCanEditProject_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean canEdit = SecurityProjectUtils.userCanEditProject(user, "testProjectId");
		assertTrue(canEdit);
	}

	@Test
	void testUserCanEditProject_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean canEdit = SecurityProjectUtils.userCanEditProject(user2, "testProjectId");
		assertFalse(canEdit);
	}

	@Test
	void testUserCanEditProject_editor() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		boolean canEdit = SecurityProjectUtils.userCanEditProject(user2, "testProjectId");
		assertTrue(canEdit);
	}

	///
	/// getProjectAliasForId
	///

	@Test
	void testGetProjectAliasForId_notFound() {
		String alias = SecurityProjectUtils.getProjectAliasForId("nonExistentId");
		assertNull(alias);
	}

	@Test
	void testGetProjectAliasForId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String alias = SecurityProjectUtils.getProjectAliasForId("testProjectId");
		assertEquals("testProjectName", alias);
	}

	///
	/// getAllProjectIds
	///

	@Test
	void testGetAllProjectIds_empty() {
		List<String> ids = SecurityProjectUtils.getAllProjectIds();
		assertNotNull(ids);
		assertTrue(ids.isEmpty());
	}

	@Test
	void testGetAllProjectIds() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", user);

		List<String> ids = SecurityProjectUtils.getAllProjectIds();
		assertNotNull(ids);
		assertEquals(2, ids.size());
		assertTrue(ids.contains("project1"));
		assertTrue(ids.contains("project2"));
	}

	///
	/// getGlobalProjectIds
	///

	@Test
	void testGetGlobalProjectIds_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Set<String> globalIds = SecurityProjectUtils.getGlobalProjectIds();
		assertNotNull(globalIds);
		assertTrue(globalIds.isEmpty());
	}

	@Test
	void testGetGlobalProjectIds() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", user);
		SecurityProjectUtils.setProjectGlobal(user, "project1", true);

		Set<String> globalIds = SecurityProjectUtils.getGlobalProjectIds();
		assertNotNull(globalIds);
		assertEquals(1, globalIds.size());
		assertTrue(globalIds.contains("project1"));
	}

	///
	/// getUserProjectPermission
	///

	@Test
	void testGetUserProjectPermission_noPermission() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Integer permission = SecurityProjectUtils.getUserProjectPermission("nonExistentUserId", "testProjectId");
		assertNull(permission);
	}

	@Test
	void testGetUserProjectPermission_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Integer permission = SecurityProjectUtils.getUserProjectPermission("adminid", "testProjectId");
		assertNotNull(permission);
		assertEquals(AccessPermissionEnum.OWNER.getId(), permission);
	}

	///
	/// getUserProjectPermissions
	///

	@Test
	void testGetUserProjectPermissions_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<String> userIds = List.of("nonExistent1", "nonExistent2");
		Map<String, Integer> permissions = SecurityProjectUtils.getUserProjectPermissions(userIds, "testProjectId");
		assertNotNull(permissions);
		assertTrue(permissions.isEmpty());
	}

	@Test
	void testGetUserProjectPermissions_multipleUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);
		SecurityProjectUtils.addProjectUser(user, "user3id", "testProjectId", "READ_ONLY", null);

		List<String> userIds = List.of("user2id", "user3id");
		Map<String, Integer> permissions = SecurityProjectUtils.getUserProjectPermissions(userIds, "testProjectId");
		assertNotNull(permissions);
		assertEquals(2, permissions.size());
		assertEquals(AccessPermissionEnum.EDIT.getId(), permissions.get("user2id"));
		assertEquals(AccessPermissionEnum.READ_ONLY.getId(), permissions.get("user3id"));
	}

	///
	/// getUserProjectPermissionsWrapper
	///

	@Test
	void testGetUserProjectPermissionsWrapper() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		List<String> userIds = List.of("user2id");
		try (IRawSelectWrapper wrapper = SecurityProjectUtils.getUserProjectPermissionsWrapper(userIds,
				"testProjectId")) {
			assertNotNull(wrapper);
			assertTrue(wrapper.hasNext());
		}
	}

	///
	/// getActualUserProjectPermission
	///

	@Test
	void testGetActualUserProjectPermission_owner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String permission = SecurityProjectUtils.getActualUserProjectPermission(user, "testProjectId");
		assertEquals("OWNER", permission);
	}

	@Test
	void testGetActualUserProjectPermission_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String permission = SecurityProjectUtils.getActualUserProjectPermission(user2, "testProjectId");
		assertNull(permission);
	}

	@Test
	void testGetActualUserProjectPermission_editor() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		String permission = SecurityProjectUtils.getActualUserProjectPermission(user2, "testProjectId");
		assertEquals("EDIT", permission);
	}

	///
	/// addProjectUser
	///

	@Test
	void testAddProjectUser_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.addProjectUser(user2, "user3id", "testProjectId", "READ_ONLY", null));
		assertEquals("Insufficient privileges to modify this project's permissions.", ex.getMessage());
	}

	@Test
	void testAddProjectUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
	}

	///
	/// editProjectUserPermission
	///

	@Test
	void testEditProjectUserPermission_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityProjectUtils
				.editProjectUserPermission(user2, "user3id", "NATIVE", "testProjectId", "EDIT", null));
		assertEquals("Insufficient privileges to modify this project's permissions.", ex.getMessage());
	}

	@Test
	void testEditProjectUserPermission() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		assertFalse(SecurityProjectUtils.userCanEditProject(user2, "testProjectId"));

		SecurityProjectUtils.editProjectUserPermission(user, "user2id", "NATIVE", "testProjectId", "EDIT", null);

		assertTrue(SecurityProjectUtils.userCanEditProject(user2, "testProjectId"));
	}

	///
	/// editProjectUserPermissions (batch)
	///

	@Test
	void testEditProjectUserPermissions_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, String>> requests = List.of(Map.of("userid", "user3id", "permission", "EDIT"));

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.editProjectUserPermissions(user2, "testProjectId", requests, null));
		assertEquals("Insufficient privileges to modify this project's permissions.", ex.getMessage());
	}

	@Test
	void testEditProjectUserPermissions() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);
		SecurityProjectUtils.addProjectUser(user, "user3id", "testProjectId", "READ_ONLY", null);

		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "EDIT");

		Map<String, String> perm2 = new HashMap<>();
		perm2.put("userid", "user3id");
		perm2.put("permission", "EDIT");

		List<Map<String, String>> requests = List.of(perm1, perm2);
		SecurityProjectUtils.editProjectUserPermissions(user, "testProjectId", requests, null);

		assertTrue(SecurityProjectUtils.userCanEditProject(user2, "testProjectId"));
		assertTrue(SecurityProjectUtils.userCanEditProject(user3, "testProjectId"));
	}

	///
	/// removeProjectUser
	///

	@Test
	void testRemoveProjectUser_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.removeProjectUser(user2, "user3id", "testProjectId"));
		assertEquals("Insufficient privileges to modify this project's permissions.", ex.getMessage());
	}

	@Test
	void testRemoveProjectUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));

		SecurityProjectUtils.removeProjectUser(user, "user2id", "testProjectId");

		assertFalse(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
	}

	///
	/// removeProjectUsers (batch)
	///

	@Test
	void testRemoveProjectUsers_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.removeProjectUsers(user2, List.of("user3id"), "testProjectId"));
		assertEquals("Insufficient privileges to modify this project's permissions.", ex.getMessage());
	}

	@Test
	void testRemoveProjectUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);
		SecurityProjectUtils.addProjectUser(user, "user3id", "testProjectId", "EDIT", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
		assertTrue(SecurityProjectUtils.userCanEditProject(user3, "testProjectId"));

		SecurityProjectUtils.removeProjectUsers(user, List.of("user2id", "user3id"), "testProjectId");

		assertFalse(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
		assertFalse(SecurityProjectUtils.userCanViewProject(user3, "testProjectId"));
	}

	///
	/// removeExpiredProjectUser
	///

	@Test
	void testRemoveExpiredProjectUser() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));

		SecurityProjectUtils.removeExpiredProjectUser("user2id", "testProjectId");

		assertFalse(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
	}

	///
	/// addProjectUserPermissions (batch)
	///

	@Test
	void testAddProjectUserPermissions_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, String>> permissions = List.of(Map.of("userid", "user3id", "permission", "READ_ONLY"));

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.addProjectUserPermissions(user2, "testProjectId", permissions, null));
		assertEquals("Insufficient privileges to modify this project's permissions.", ex.getMessage());
	}

	@Test
	void testAddProjectUserPermissions() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Map<String, String> perm1 = new HashMap<>();
		perm1.put("userid", "user2id");
		perm1.put("permission", "READ_ONLY");

		Map<String, String> perm2 = new HashMap<>();
		perm2.put("userid", "user3id");
		perm2.put("permission", "EDIT");

		List<Map<String, String>> permissions = List.of(perm1, perm2);
		SecurityProjectUtils.addProjectUserPermissions(user, "testProjectId", permissions, null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
		assertTrue(SecurityProjectUtils.userCanEditProject(user3, "testProjectId"));
	}

	///
	/// setProjectGlobal
	///

	@Test
	void testSetProjectGlobal_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectGlobal(user2, "testProjectId", true));
		assertEquals(
				"The user doesn't have the permission to set this project as global. Only the owner or an admin can perform this action.",
				ex.getMessage());
	}

	@Test
	void testSetProjectGlobal() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertFalse(SecurityProjectUtils.projectIsGlobal("testProjectId"));

		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		assertTrue(SecurityProjectUtils.projectIsGlobal("testProjectId"));
	}

	///
	/// setProjectDiscoverable
	///

	@Test
	void testSetProjectDiscoverable_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectDiscoverable(user2, "testProjectId", true));
		assertEquals(
				"The user doesn't have the permission to set this project as discoverable. Only the owner or an admin can perform this action.",
				ex.getMessage());
	}

	@Test
	void testSetProjectDiscoverable() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertFalse(SecurityProjectUtils.projectIsDiscoverable("testProjectId"));

		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		assertTrue(SecurityProjectUtils.projectIsDiscoverable("testProjectId"));
	}

	///
	/// setProjectName
	///

	@Test
	void testSetProjectName_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectName(user2, "testProjectId", "newName"));
		assertEquals(
				"The user doesn't have the permission to change the project name. Only the owner or an admin can perform this action.",
				ex.getMessage());
	}

	@Test
	void testSetProjectName() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertEquals("testProjectName", SecurityProjectUtils.getProjectAliasForId("testProjectId"));

		SecurityProjectUtils.setProjectName(user, "testProjectId", "newProjectName");

		assertEquals("newProjectName", SecurityProjectUtils.getProjectAliasForId("testProjectId"));
	}

	///
	/// getProjectOwners
	///

	@Test
	void testGetProjectOwners_empty() {
		List<String> owners = SecurityProjectUtils.getProjectOwners("nonExistentProject");
		assertNotNull(owners);
		assertTrue(owners.isEmpty());
	}

	@Test
	void testGetProjectOwners() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<String> owners = SecurityProjectUtils.getProjectOwners("testProjectId");
		assertNotNull(owners);
		assertEquals(1, owners.size());
		assertEquals("admin@test.com", owners.getFirst());
	}

	///
	/// getProjectUsers
	///

	@Test
	void testGetProjectUsers_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.getProjectUsers(user2, "testProjectId", null, null, 10, 0));
		assertEquals("The user does not have access to view this project", ex.getMessage());
	}

	@Test
	void testGetProjectUsers() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		List<Map<String, Object>> users = SecurityProjectUtils.getProjectUsers(user, "testProjectId", null, null, 10,
				0);
		assertNotNull(users);
		assertEquals(2, users.size()); // owner + user2
	}

	///
	/// getProjectUsersCount
	///

	@Test
	void testGetProjectUsersCount_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.getProjectUsersCount(user2, "testProjectId", null, null));
		assertEquals("The user does not have access to view this project", ex.getMessage());
	}

	@Test
	void testGetProjectUsersCount() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		long count = SecurityProjectUtils.getProjectUsersCount(user, "testProjectId", null, null);
		assertEquals(2, count); // owner + user2
	}

	///
	/// getProjectUsersNoCredentials
	///

	@Test
	void testGetProjectUsersNoCredentials_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.getProjectUsersNoCredentials(user2, "testProjectId", null, 10, 0));
		assertEquals("The user does not have access to view this project", ex.getMessage());
	}

	@Test
	void testGetProjectUsersNoCredentials() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> users = SecurityProjectUtils.getProjectUsersNoCredentials(user, "testProjectId", null,
				10, 0);
		assertNotNull(users);
	}

	///
	/// getUserProjectList
	///

	@Test
	void testGetUserProjectList_noProjects() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, null);
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetUserProjectList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", user);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, null);
		assertNotNull(projects);
		assertEquals(2, projects.size());
	}

	///
	/// getUserProjectIdList
	///

	@Test
	void testGetUserProjectIdList_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// Use includeExplicitUser=true to get only projects where user has explicit
		// permission
		List<String> ids = SecurityProjectUtils.getUserProjectIdList(user, null, false, false, true);
		assertNotNull(ids);
		assertTrue(ids.isEmpty());
	}

	@Test
	void testGetUserProjectIdList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", user);

		List<String> ids = SecurityProjectUtils.getUserProjectIdList(user, null, false, false, true);
		assertNotNull(ids);
		assertEquals(2, ids.size());
	}

	///
	/// getFullUserProjectIds
	///

	@Test
	void testGetFullUserProjectIds_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		List<String> ids = SecurityProjectUtils.getFullUserProjectIds(user);
		assertNotNull(ids);
		assertTrue(ids.isEmpty());
	}

	@Test
	void testGetFullUserProjectIds() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);

		List<String> ids = SecurityProjectUtils.getFullUserProjectIds(user);
		assertNotNull(ids);
		assertEquals(1, ids.size());
		assertTrue(ids.contains("project1"));
	}

	///
	/// testUserProjectIdForAlias
	///

	@Test
	void testTestUserProjectIdForAlias_notFound() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		// Method returns the input if no project is found
		String result = SecurityProjectUtils.testUserProjectIdForAlias(user, "nonExistent");
		assertEquals("nonExistent", result);
	}

	@Test
	void testTestUserProjectIdForAlias_byId() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String result = SecurityProjectUtils.testUserProjectIdForAlias(user, "testProjectId");
		assertEquals("testProjectId", result);
	}

	///
	/// deleteProject
	///

	@Test
	void testDeleteProject() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertTrue(SecurityProjectUtils.projectExists("testProjectId"));

		SecurityProjectUtils.deleteProject("testProjectId");

		assertFalse(SecurityProjectUtils.projectExists("testProjectId"));
	}

	///
	/// setProjectFavorite
	///

	@Test
	void testSetProjectFavorite_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectFavorite(user2, "testProjectId", true));
		assertEquals("The user doesn't have the permission to modify his visibility of this project.", ex.getMessage());
	}

	@Test
	void testSetProjectFavorite() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		// Just verify no exception is thrown - we can't easily verify the favorite
		// status
		SecurityProjectUtils.setProjectFavorite(user, "testProjectId", true);
		assertTrue(SecurityProjectUtils.userIsOwner(user, "testProjectId"));
	}

	@Test
	void testSetProjectFavorite_publicProjectWithoutAccess() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		String user2Id = user2.getPrimaryLoginToken().getId();
		SecurityProjectUtils.setProjectFavorite(user2, "testProjectId", true);

		List<Map<String, Object>> favorites = SecurityProjectUtils.getUserProjectList(user2, null, null, true, null,
				null, null, null, null);
		assertEquals(1, favorites.size());
		assertEquals("1", favorites.getFirst().get("project_favorite").toString());
		assertEquals("1", SecurityProjectUtils.getUserProjectList(user2, "testProjectId").getFirst()
				.get("project_favorite").toString());
		// the favorite is kept apart from the permissions, so no access row is added
		assertEquals(0L,
				((Number) UnitTestGroupTables.queryValue(securityDb,
						"SELECT COUNT(*) FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?", user2Id,
						"testProjectId")).longValue());

		SecurityProjectUtils.setProjectFavorite(user2, "testProjectId", false);
		assertTrue(SecurityProjectUtils.getUserProjectList(user2, null, null, true, null, null, null, null, null)
				.isEmpty());
		assertEquals("0", SecurityProjectUtils.getUserProjectList(user2, "testProjectId").getFirst()
				.get("project_favorite").toString());
	}

	///
	/// setProjectVisibility
	///

	@Test
	void testSetProjectVisibility_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectVisibility(user2, "testProjectId", true));
		assertEquals("The user doesn't have the permission to modify his visibility of this project.", ex.getMessage());
	}

	@Test
	void testSetProjectVisibility() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		// Just verify no exception is thrown
		SecurityProjectUtils.setProjectVisibility(user, "testProjectId", true);
		assertTrue(SecurityProjectUtils.userIsOwner(user, "testProjectId"));
	}

	@Test
	void testSetProjectVisibility_onlyChangesThatProject() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createProject("testProjectId2", "testProjectName2", user);

		SecurityProjectUtils.setProjectVisibility(user, "testProjectId", false);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, null, null, false, null,
				null, null, null, null);
		assertEquals(1, projects.size());
		assertEquals("testProjectId2", projects.getFirst().get("project_id"));

		SecurityProjectUtils.setProjectVisibility(user, "testProjectId", true);
		assertEquals(2,
				SecurityProjectUtils.getUserProjectList(user, null, null, false, null, null, null, null, null).size());
	}

	///
	/// getAllMetakeys
	///

	@Test
	void testGetAllMetakeys() {
		List<String> metakeys = SecurityProjectUtils.getAllMetakeys();
		assertNotNull(metakeys);
		// The PROJECTMETAKEYS table may be empty in test environment
	}

	///
	/// getMetakeyOptions
	///

	@Test
	void testGetMetakeyOptions() {
		List<Map<String, Object>> options = SecurityProjectUtils.getMetakeyOptions("description");
		assertNotNull(options);
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

		List<Map<String, Object>> metaoptions = List.of(option1);

		boolean result = SecurityProjectUtils.updateMetakeyOptions(metaoptions);
		assertTrue(result);

		List<String> metakeys = SecurityProjectUtils.getAllMetakeys();
		assertTrue(metakeys.contains("description"));
	}

	///
	/// getAvailableMetaValues
	///

	@Test
	void testGetAvailableMetaValues() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<String> metaKeys = List.of("description");
		List<Map<String, Object>> values = SecurityProjectUtils.getAvailableMetaValues(null, metaKeys);
		assertNotNull(values);
	}

	///
	/// updateProjectMetadata
	///

	@Test
	void testUpdateProjectMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Map<String, Object> metadata = new HashMap<>();
		metadata.put("description", "Test description");

		SecurityProjectUtils.updateProjectMetadata("testProjectId", metadata);

		// Verify project still exists after update
		assertTrue(SecurityProjectUtils.projectExists("testProjectId"));
	}

	///
	/// getProjectMetadataWrapper
	///

	@Test
	void testGetProjectMetadataWrapper() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<String> projectIds = List.of("testProjectId");
		List<String> metaKeys = List.of("description");
		try (IRawSelectWrapper wrapper = SecurityProjectUtils.getProjectMetadataWrapper(projectIds, metaKeys, false)) {
			assertNotNull(wrapper);
		}
	}

	///
	/// getAggregateProjectMetadata
	///

	@Test
	void testGetAggregateProjectMetadata() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<String> metaKeys = List.of("description");
		Map<String, Object> metadata = SecurityProjectUtils.getAggregateProjectMetadata("testProjectId", metaKeys,
				false);
		assertNotNull(metadata);
	}

	///
	/// checkUserHasAccessToProject
	///

	@Test
	void testCheckUserHasAccessToProject_noAccess() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean hasAccess = SecurityProjectUtils.checkUserHasAccessToProject("testProjectId", "nonExistentUserId");
		assertFalse(hasAccess);
	}

	@Test
	void testCheckUserHasAccessToProject() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean hasAccess = SecurityProjectUtils.checkUserHasAccessToProject("testProjectId", "adminid");
		assertTrue(hasAccess);
	}

	///
	/// getProjectsUserHasExplicitAccess
	///

	@Test
	void testGetProjectsUserHasExplicitAccess_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		Set<String> projects = SecurityProjectUtils.getProjectsUserHasExplicitAccess(user);
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetProjectsUserHasExplicitAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Set<String> projects = SecurityProjectUtils.getProjectsUserHasExplicitAccess(user);
		assertNotNull(projects);
		assertEquals(1, projects.size());
		assertTrue(projects.contains("testProjectId"));
	}

	///
	/// userHasExplicitAccess
	///

	@Test
	void testUserHasExplicitAccess_false() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean hasAccess = SecurityProjectUtils.userHasExplicitAccess(user2, "testProjectId");
		assertFalse(hasAccess);
	}

	@Test
	void testUserHasExplicitAccess_true() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean hasAccess = SecurityProjectUtils.userHasExplicitAccess(user, "testProjectId");
		assertTrue(hasAccess);
	}

	///
	/// getProjectInfo
	///

	@Test
	void testGetProjectInfo() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> info = SecurityProjectUtils.getProjectInfo(List.of("testProjectId"));
		assertNotNull(info);
	}

	///
	/// getUserRequestableProjects
	///

	@Test
	void testGetUserRequestableProjects() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserRequestableProjects(List.of());
		assertNotNull(projects);
	}

	///
	/// canRequestProject
	///

	@Test
	void testCanRequestProject_notDiscoverable() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		boolean canRequest = SecurityProjectUtils.canRequestProject("testProjectId");
		assertFalse(canRequest);
	}

	@Test
	void testCanRequestProject_discoverable() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		boolean canRequest = SecurityProjectUtils.canRequestProject("testProjectId");
		assertTrue(canRequest);
	}

	///
	/// setUserAccessRequest
	///

	@Test
	void testSetUserAccessRequest() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		SecurityProjectUtils.setUserAccessRequest("user2id", "NATIVE", "testProjectId", "I need access", 3, user2);

		List<Map<String, Object>> requests = SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId");
		assertNotNull(requests);
		assertEquals(1, requests.size());
	}

	///
	/// getUserPendingAccessRequest
	///

	@Test
	void testGetUserPendingAccessRequest_noRequest() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		int pending = SecurityProjectUtils.getUserPendingAccessRequest(user, "testProjectId");
		assertEquals(-1, pending);
	}

	///
	/// getUserAccessRequestsByProject
	///

	@Test
	void testGetUserAccessRequestsByProject_noRequests() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> requests = SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId");
		assertNotNull(requests);
		assertTrue(requests.isEmpty());
	}

	///
	/// getUserAccessRequestProjectPermission
	///

	@Test
	void testGetUserAccessRequestProjectPermission_noRequest() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Integer permission = SecurityProjectUtils.getUserAccessRequestProjectPermission("nonExistentUser",
				"testProjectId");
		assertNull(permission);
	}

	///
	/// getDiscoverableProjectList
	///

	@Test
	void testGetDiscoverableProjectList_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> projects = SecurityProjectUtils.getDiscoverableProjectList(null, null);
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetDiscoverableProjectList() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		List<Map<String, Object>> projects = SecurityProjectUtils.getDiscoverableProjectList(null, null);
		assertNotNull(projects);
		assertEquals(1, projects.size());
	}

	///
	/// getProjectMarkdown
	///

	@Test
	void testGetProjectMarkdown_noAccess() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String markdown = SecurityProjectUtils.getProjectMarkdown(user2, "testProjectId");
		assertNull(markdown);
	}

	@Test
	void testGetProjectMarkdown() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String markdown = SecurityProjectUtils.getProjectMarkdown(user, "testProjectId");
		// Markdown may be null if not set, but method should not throw
	}

	///
	/// getAllUserProjectList
	///

	@Test
	void testGetAllUserProjectList_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		List<Map<String, Object>> projects = SecurityProjectUtils.getAllUserProjectList(user);
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetAllUserProjectList() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);

		List<Map<String, Object>> projects = SecurityProjectUtils.getAllUserProjectList(user);
		assertNotNull(projects);
		assertEquals(1, projects.size());
	}

	// =========================================================================
	// ADDITIONAL COVERAGE TESTS - Edge Cases and Code Paths
	// =========================================================================

	///
	/// addProjectUser - additional edge cases
	///

	@Test
	void testAddProjectUser_userAlreadyHasAccess() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		// Try to add the same user again
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null));
		assertEquals("This user already has access to this project. Please edit the existing permission level.",
				ex.getMessage());
	}

	@Test
	void testAddProjectUser_editorCannotGrantOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		// Editor (user2) tries to grant OWNER to user3
		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.addProjectUser(user2, "user3id", "testProjectId", "OWNER", null));
		assertEquals("Cannot give owner level access to this project since you are not currently an owner.",
				ex.getMessage());
	}

	@Test
	void testAddProjectUser_withEndDate() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		// Add user with an end date (must use ISO format with timezone)
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", "2099-12-31T00:00:00Z");

		Integer permission = SecurityProjectUtils.getUserProjectPermission("user2id", "testProjectId");
		assertNotNull(permission);
		assertEquals(AccessPermissionEnum.READ_ONLY.getId(), permission);
	}

	@Test
	void testAddProjectUser_editorCanGrantReadOnly() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		// Editor (user2) grants READ_ONLY to user3 - should succeed
		SecurityProjectUtils.addProjectUser(user2, "user3id", "testProjectId", "READ_ONLY", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user3, "testProjectId"));
	}

	///
	/// editProjectUserPermission - additional edge cases
	///

	@Test
	void testEditProjectUserPermission_userDoesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityProjectUtils
				.editProjectUserPermission(user, "nonExistentUser", "NATIVE", "testProjectId", "EDIT", null));
		assertEquals(
				"Attempting to modify project permission for a user who does not currently have access to the project",
				ex.getMessage());
	}

	@Test
	void testEditProjectUserPermission_editorCannotEditOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		// Editor (user2) tries to edit owner's (admin) permission
		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityProjectUtils
				.editProjectUserPermission(user2, "adminid", "NATIVE", "testProjectId", "EDIT", null));
		assertEquals("The user doesn't have the high enough permissions to modify this users project permission.",
				ex.getMessage());
	}

	@Test
	void testEditProjectUserPermission_editorCannotGrantOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);
		SecurityProjectUtils.addProjectUser(user, "user3id", "testProjectId", "READ_ONLY", null);

		// Editor (user2) tries to grant OWNER to user3
		IllegalAccessException ex = assertThrows(IllegalAccessException.class, () -> SecurityProjectUtils
				.editProjectUserPermission(user2, "user3id", "NATIVE", "testProjectId", "OWNER", null));
		assertEquals("Cannot give owner level access to this project since you are not currently an owner.",
				ex.getMessage());
	}

	@Test
	void testEditProjectUserPermission_withEndDate() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		SecurityProjectUtils.editProjectUserPermission(user, "user2id", "NATIVE", "testProjectId", "EDIT",
				"2099-12-31T00:00:00Z");

		assertTrue(SecurityProjectUtils.userCanEditProject(user2, "testProjectId"));
	}

	///
	/// editProjectUserPermissions (batch) - additional edge cases
	///

	@Test
	void testEditProjectUserPermissions_userNotInProject() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		Map<String, String> perm = new HashMap<>();
		perm.put("userid", "user2id");
		perm.put("permission", "EDIT");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.editProjectUserPermissions(user, "testProjectId", List.of(perm), null));
		assertTrue(ex.getMessage().contains("do not currently have access to the project"));
	}

	@Test
	void testEditProjectUserPermissions_editorCannotEditOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		Map<String, String> perm = new HashMap<>();
		perm.put("userid", "adminid");
		perm.put("permission", "EDIT");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.editProjectUserPermissions(user2, "testProjectId", List.of(perm), null));
		assertEquals("As a non-owner, you cannot edit access of an owner.", ex.getMessage());
	}

	@Test
	void testEditProjectUserPermissions_editorCannotGrantOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		User user3 = UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);
		SecurityProjectUtils.addProjectUser(user, "user3id", "testProjectId", "READ_ONLY", null);

		Map<String, String> perm = new HashMap<>();
		perm.put("userid", "user3id");
		perm.put("permission", "OWNER");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.editProjectUserPermissions(user2, "testProjectId", List.of(perm), null));
		assertEquals("As a non-owner, you cannot give a user access as an owner.", ex.getMessage());
	}

	///
	/// removeProjectUser - additional edge cases
	///

	@Test
	void testRemoveProjectUser_userDoesNotExist() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.removeProjectUser(user, "nonExistentUser", "testProjectId"));
		assertEquals(
				"Attempting to modify user permission for a user who does not currently have access to the project",
				ex.getMessage());
	}

	@Test
	void testRemoveProjectUser_editorCannotRemoveOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.removeProjectUser(user2, "adminid", "testProjectId"));
		assertEquals("The user doesn't have the high enough permissions to modify this users project permission.",
				ex.getMessage());
	}

	@Test
	void testRemoveProjectUser_userRemovesSelf() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		assertTrue(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));

		// User removes themselves
		SecurityProjectUtils.removeProjectUser(user2, "user2id", "testProjectId");

		assertFalse(SecurityProjectUtils.userCanViewProject(user2, "testProjectId"));
	}

	///
	/// addProjectUserPermissions (batch) - additional edge cases
	///

	@Test
	void testAddProjectUserPermissions_userAlreadyHasAccess() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		Map<String, String> perm = new HashMap<>();
		perm.put("userid", "user2id");
		perm.put("permission", "EDIT");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.addProjectUserPermissions(user, "testProjectId", List.of(perm), null));
		assertTrue(ex.getMessage().contains("already have access"));
	}

	@Test
	void testAddProjectUserPermissions_editorCannotGrantOwner() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createUser("user3", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "EDIT", null);

		Map<String, String> perm = new HashMap<>();
		perm.put("userid", "user3id");
		perm.put("permission", "OWNER");

		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.addProjectUserPermissions(user2, "testProjectId", List.of(perm), null));
		assertEquals("As a non-owner, you cannot add owner user access.", ex.getMessage());
	}

	///
	/// updateProject
	///

	@Test
	void testUpdateProject() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		SecurityProjectUtils.updateProject("testProjectId", "updatedName", "APP", null, false);

		assertEquals("updatedName", SecurityProjectUtils.getProjectAliasForId("testProjectId"));
	}

	@Test
	void testUpdateProject_setGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertFalse(SecurityProjectUtils.projectIsGlobal("testProjectId"));

		SecurityProjectUtils.updateProject("testProjectId", "testProjectName", "APP", null, true);

		assertTrue(SecurityProjectUtils.projectIsGlobal("testProjectId"));
	}

	///
	/// updateProjectLastEditedDate
	///

	@Test
	void testUpdateProjectLastEditedDate() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		// Just verify no exception is thrown
		SecurityProjectUtils.updateProjectLastEditedDate("testProjectId");
		assertTrue(SecurityProjectUtils.projectExists("testProjectId"));
	}

	///
	/// deleteInsightsFromProjectForRecreation
	///

	@Test
	void testDeleteInsightsFromProjectForRecreation() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createInsight("testProjectId", "insight1", "Insight One", "grid");

		// Just verify no exception is thrown
		SecurityProjectUtils.deleteInsightsFromProjectForRecreation("testProjectId");
		assertTrue(SecurityProjectUtils.projectExists("testProjectId"));
	}

	///
	/// setProjectCompletelyGlobal
	///

	@Test
	void testSetProjectCompletelyGlobal() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		assertFalse(SecurityProjectUtils.projectIsGlobal("testProjectId"));

		SecurityProjectUtils.setProjectCompletelyGlobal("testProjectId");

		assertTrue(SecurityProjectUtils.projectIsGlobal("testProjectId"));
	}

	///
	/// getPortalPublishedTimestamp & setPortalPublish
	///

	@Test
	void testSetPortalPublish() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user, true);

		// Verify no timestamp before publish
		assertNull(SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId"));

		SecurityProjectUtils.setPortalPublish(user, "testProjectId");

		// Verify timestamp is now set
		LocalDateTime timestamp = SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId");
		assertNotNull(timestamp);
	}

	@Test
	void testGetPortalPublishedTimestamp_noTimestamp() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user, true);

		LocalDateTime timestamp = SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId");
		assertNull(timestamp);
	}

	@Test
	void testInitPortalPublishedTimestamp() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user, true);

		assertNull(SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId"));

		assertTrue(SecurityProjectUtils.initPortalPublishedTimestamp("testProjectId"));

		assertNotNull(SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId"));
	}

	@Test
	void testInitPortalPublishedTimestamp_doesNotOverwriteExisting() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user, true);

		SecurityProjectUtils.setPortalPublish(user, "testProjectId");
		LocalDateTime reported = SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId");
		assertNotNull(reported);

		// the IS NULL guard leaves an already recorded timestamp alone, so a container
		// initializing one cannot move a change that another container reported
		assertTrue(SecurityProjectUtils.initPortalPublishedTimestamp("testProjectId"));

		assertEquals(reported, SecurityProjectUtils.getPortalPublishedTimestamp("testProjectId"));
	}

	///
	/// getReactorCompilationTimestamp & setReactorCompilation
	///

	@Test
	void testSetReactorCompilation() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		// Verify no timestamp before compilation
		assertNull(SecurityProjectUtils.getReactorCompilationTimestamp("testProjectId"));

		SecurityProjectUtils.setReactorCompilation(user, "testProjectId");

		// Verify timestamp is now set
		LocalDateTime timestamp = SecurityProjectUtils.getReactorCompilationTimestamp("testProjectId");
		assertNotNull(timestamp);
	}

	@Test
	void testGetReactorCompilationTimestamp_noTimestamp() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		LocalDateTime timestamp = SecurityProjectUtils.getReactorCompilationTimestamp("testProjectId");
		assertNull(timestamp);
	}

	///
	/// testUserProjectIdForAlias - additional cases
	///

	@Test
	void testTestUserProjectIdForAlias_byName() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		String result = SecurityProjectUtils.testUserProjectIdForAlias(user, "testProjectName");
		assertEquals("testProjectId", result);
	}

	@Test
	void testTestUserProjectIdForAlias_globalProject() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		// User2 doesn't have explicit access but project is global
		String result = SecurityProjectUtils.testUserProjectIdForAlias(user2, "testProjectName");
		assertEquals("testProjectId", result);
	}

	///
	/// approveProjectUserAccessRequests & denyProjectUserAccessRequests
	///

	@Test
	void testApproveProjectUserAccessRequests_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, String>> requests = List.of(Map.of("requestid", "someId"));

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.approveProjectUserAccessRequests(user2, "testProjectId", requests, null));
		assertNotNull(ex.getMessage());
	}

	@Test
	void testDenyProjectUserAccessRequests_notOwner() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		IllegalAccessException ex = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.denyProjectUserAccessRequests(user2, "testProjectId", List.of("someId")));
		assertNotNull(ex.getMessage());
	}

	///
	/// getUserProjectList with filter
	///

	@Test
	void testGetUserProjectList_withFilter() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Alpha Project", user);
		UnitTestSecurityAuthUtils.createProject("project2", "Beta Project", user);

		// The filter parameter is actually a projectId filter, not a search term
		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, "project1");
		assertNotNull(projects);
		assertEquals(1, projects.size());
	}

	///
	/// propagateProjectPermission
	///

	@Test
	void testPropagateProjectPermission_noDependencies() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		// Call propagate with no project dependencies - should return empty result maps
		Map<String, Object> result = SecurityProjectUtils.propagateProjectPermission(user, "testProjectId", "user2id",
				"user", "READ_ONLY", null);
		assertNotNull(result);
	}

	///
	/// getProjectDependencies & updateProjectDependencies
	///

	@Test
	void testGetProjectDependencies_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> deps = SecurityProjectUtils.getProjectDependencies("testProjectId", true);
		assertNotNull(deps);
		assertTrue(deps.isEmpty());
	}

	@Test
	void testGetProjectDependencyDetails_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> deps = SecurityProjectUtils.getProjectDependencyDetails("testProjectId", false);
		assertNotNull(deps);
		assertTrue(deps.isEmpty());
	}

	///
	/// getUserDiscoverableProjectList
	///

	@Test
	void testGetUserDiscoverableProjectList_empty() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserDiscoverableProjectList(user, null, null, null,
				null, null, null);
		assertNotNull(projects);
		assertTrue(projects.isEmpty());
	}

	@Test
	void testGetUserDiscoverableProjectList() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserDiscoverableProjectList(user2, null, null,
				null, null, null, null);
		assertNotNull(projects);
		assertEquals(1, projects.size());
	}

	///
	/// getUserProjectList with multiple parameters
	///

	@Test
	void testGetUserProjectList_withMultipleFilters() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);
		UnitTestSecurityAuthUtils.createProject("project2", "Project Two", user);

		List<String> projectTypes = List.of("APP");
		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, projectTypes, null, false,
				null, null, null, "10", "0");
		assertNotNull(projects);
	}

	@Test
	void testGetUserProjectList_favoritesOnly() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("project1", "Project One", user);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(user, null, null, true, null, null,
				null, "10", "0");
		assertNotNull(projects);
	}

	///
	/// getUserProjectIdList - additional cases
	///

	@Test
	void testGetUserProjectIdList_includeGlobal() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		// includeExistingAccess must be true when includeDiscoverable is false
		List<String> ids = SecurityProjectUtils.getUserProjectIdList(user2, null, true, true, true);
		assertNotNull(ids);
		assertTrue(ids.contains("testProjectId"));
	}

	@Test
	void testGetUserProjectIdList_includeDiscoverable() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		List<String> ids = SecurityProjectUtils.getUserProjectIdList(user2, null, false, true, false);
		assertNotNull(ids);
		assertTrue(ids.contains("testProjectId"));
	}

	///
	/// getProjectUsers with search/filter
	///

	@Test
	void testGetProjectUsers_withSearch() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("searchuser", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "searchuserid", "testProjectId", "READ_ONLY", null);

		List<Map<String, Object>> users = SecurityProjectUtils.getProjectUsers(user, "testProjectId", "search", null,
				10, 0);
		assertNotNull(users);
	}

	@Test
	void testGetProjectUsers_withPermissionFilter() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("user2", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "user2id", "testProjectId", "READ_ONLY", null);

		List<Map<String, Object>> users = SecurityProjectUtils.getProjectUsers(user, "testProjectId", null, "READ_ONLY",
				10, 0);
		assertNotNull(users);
		assertEquals(1, users.size());
	}

	///
	/// setProjectGlobal - toggle back to false
	///

	@Test
	void testSetProjectGlobal_toggleOff() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", true);

		assertTrue(SecurityProjectUtils.projectIsGlobal("testProjectId"));

		SecurityProjectUtils.setProjectGlobal(user, "testProjectId", false);

		assertFalse(SecurityProjectUtils.projectIsGlobal("testProjectId"));
	}

	///
	/// setProjectDiscoverable - toggle back to false
	///

	@Test
	void testSetProjectDiscoverable_toggleOff() throws IllegalAccessException {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", true);

		assertTrue(SecurityProjectUtils.projectIsDiscoverable("testProjectId"));

		SecurityProjectUtils.setProjectDiscoverable(user, "testProjectId", false);

		assertFalse(SecurityProjectUtils.projectIsDiscoverable("testProjectId"));
	}

	///
	/// addProject: the project's row, then its insights
	///

	@Test
	void testAddProject_registersTheProjectWithItsInsights() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		stageProject("newProject", false, "first", "second");

		SecurityProjectUtils.addProject("newProject", false, user);

		assertTrue(SecurityProjectUtils.projectExists("newProject"));
		assertEquals(2L, count("SELECT COUNT(*) FROM INSIGHT WHERE PROJECTID=?", "newProject"));
		assertEquals(2L, count("SELECT COUNT(*) FROM INSIGHTMETA WHERE PROJECTID=?", "newProject"));
	}

	@Test
	void testAddProject_aFailedInsightCopyKeepsTheProjectRegistered() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		stageProject("newProject", false, "first", "reject");

		// the second insight fails after the first insight was written
		addCheck("ADD_PROJECT_FAIL", "INSIGHT", "INSIGHTNAME <> 'reject'");
		try {
			assertDoesNotThrow(() -> SecurityProjectUtils.addProject("newProject", false, user));
		} finally {
			dropCheck("ADD_PROJECT_FAIL", "INSIGHT");
		}

		// the project's row was committed before the insights, which are all rolled
		// back
		assertTrue(SecurityProjectUtils.projectExists("newProject"));
		assertEquals(0L, count("SELECT COUNT(*) FROM INSIGHT WHERE PROJECTID=?", "newProject"));
		assertEquals(0L, count("SELECT COUNT(*) FROM INSIGHTMETA WHERE PROJECTID=?", "newProject"));
	}

	@Test
	void testAddProject_aFailedProjectRowCopiesNoInsights() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		stageProject("newProject", false, "first", "second");

		addCheck("ADD_PROJECT_ROW_FAIL", "PROJECT", "PROJECTID <> 'newProject'");
		try {
			assertDoesNotThrow(() -> SecurityProjectUtils.addProject("newProject", false, user));
		} finally {
			dropCheck("ADD_PROJECT_ROW_FAIL", "PROJECT");
		}

		assertFalse(SecurityProjectUtils.projectExists("newProject"));
		assertEquals(0L, count("SELECT COUNT(*) FROM INSIGHT WHERE PROJECTID=?", "newProject"));
	}

	@Test
	void testAddProject_reloadReplacesTheInsightsAndTheirPermissions() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		stageRegisteredProject(user);
		Path smss = stageProject("reloaded", true, "kept");

		SecurityProjectUtils.addProject("reloaded", true, user);

		assertTrue(SecurityProjectUtils.projectIsGlobal("reloaded"));
		assertEquals(1L, count("SELECT COUNT(*) FROM INSIGHT WHERE PROJECTID=?", "reloaded"));
		assertEquals(1L, count("SELECT COUNT(*) FROM USERINSIGHTPERMISSION WHERE PROJECTID=? AND INSIGHTID=?",
				"reloaded", "keptId"));
		assertEquals(0L, count("SELECT COUNT(*) FROM USERINSIGHTPERMISSION WHERE PROJECTID=? AND INSIGHTID=?",
				"reloaded", "goneId"));
		assertEquals("false", Utility.loadProperties(smss.toString()).getProperty(Constants.RELOAD_INSIGHTS));
	}

	@Test
	void testAddProject_aFailedReloadKeepsWhatWasThere() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		stageRegisteredProject(user);
		Path smss = stageProject("reloaded", true, "kept", "reject");

		addCheck("RELOAD_FAIL", "INSIGHT", "INSIGHTNAME <> 'reject'");
		try {
			assertDoesNotThrow(() -> SecurityProjectUtils.addProject("reloaded", true, user));
		} finally {
			dropCheck("RELOAD_FAIL", "INSIGHT");
		}

		// the project's row was updated before the insights; its insights and the
		// permissions of the insight that is gone are as they were
		assertTrue(SecurityProjectUtils.projectIsGlobal("reloaded"));
		assertEquals(2L, count("SELECT COUNT(*) FROM INSIGHT WHERE PROJECTID=?", "reloaded"));
		assertEquals(1L, count("SELECT COUNT(*) FROM USERINSIGHTPERMISSION WHERE PROJECTID=? AND INSIGHTID=?",
				"reloaded", "goneId"));
		// the reload is not asked for again
		assertEquals("false", Utility.loadProperties(smss.toString()).getProperty(Constants.RELOAD_INSIGHTS));
	}

	/**
	 * Registers the project "reloaded" with the insights "kept" and "gone", each
	 * shared with the second user.
	 */
	private void stageRegisteredProject(User user) throws Exception {
		UnitTestSecurityAuthUtils.createProject("reloaded", "reloadedName", user);
		for (String insightId : List.of("keptId", "goneId")) {
			UnitTestSecurityAuthUtils.createInsight("reloaded", insightId, insightId.replace("Id", ""), "grid");
			SecurityInsightUtils.addInsightUser(user, "secondid", "reloaded", insightId, "READ_ONLY", null);
		}
	}

	///
	/// createInsightsDatabase: the security rows read in one transaction
	///

	@Test
	void testCreateInsightsDatabase_copiesTheInsightsAndTheirMetadata() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("exported", "exportedName", user);
		SecurityInsightUtils.addInsight("exported", "insightId", "insightName", true, "grid", false, -1, null, null,
				false, List.of("AddPanel(0);"), null);
		SecurityInsightUtils.updateInsightTags("exported", "insightId", List.of("tagged"));
		stageLoadedProject("exported");
		Path folder = Files.createDirectories(dbDir.resolve("exported_" + UUID.randomUUID()));

		File databaseFile = SecurityProjectUtils.createInsightsDatabase("exported", folder.toString());

		assertTrue(databaseFile.isFile());
		IRDBMSEngine exported = InsightsRDBMSUtils.generateInsightsDatabase("exported", RdbmsTypeEnum.H2_DB,
				folder.toString());
		try {
			assertEquals(List.of("insightId:insightName"),
					QueryExecutionUtility.queryList(exported, "SELECT ID, QUESTION_NAME FROM QUESTION_ID",
							QueryExecutionUtility.NO_PARAMETERS, rs -> rs.getString(1) + ":" + rs.getString(2)));
			assertEquals(List.of("tagged"),
					QueryExecutionUtility.queryList(exported, "SELECT METAVALUE FROM INSIGHTMETA WHERE INSIGHTID=?",
							ps -> ps.setString(1, "insightId"), rs -> rs.getString(1)));
		} finally {
			exported.close();
		}
	}

	@Test
	void testCreateInsightsDatabase_aFailureLeavesNoFile() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("exported", "exportedName", user);
		SecurityInsightUtils.addInsight("exported", "insightId", "insightName", true, "grid", false, -1, null, null,
				false, List.of("AddPanel(0);"), null);
		QueryExecutionUtility.executeUpdate(securityDb, "UPDATE INSIGHT SET RECIPE=? WHERE INSIGHTID=?", ps -> {
			ps.setString(1, "{not a recipe");
			ps.setString(2, "insightId");
		});
		stageLoadedProject("exported");
		Path folder = Files.createDirectories(dbDir.resolve("exported_" + UUID.randomUUID()));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.createInsightsDatabase("exported", folder.toString()));
		assertEquals("Error occured creating the insights database", e.getMessage());
		assertFalse(new File(folder.toFile(), "insights_database.mv.db").exists());
	}

	/**
	 * Puts a loaded project in DIHelper whose insights database is H2, as
	 * createInsightsDatabase reads it.
	 */
	private void stageLoadedProject(String projectId) {
		IRDBMSEngine insightsDb = mock(IRDBMSEngine.class);
		when(insightsDb.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		IProject project = mock(IProject.class);
		when(project.getInsightDatabase()).thenReturn(insightsDb);
		DIHelper.getInstance().setProjectProperty(projectId, project);
		stagedProjects.add(projectId);
	}

	/**
	 * Writes a project's smss file, and puts the project in DIHelper as loaded,
	 * with an insights database that has the insights named, each with a tag, as
	 * addProject reads them. Each insight's id is its name and "Id".
	 *
	 * @return the smss file
	 */
	private Path stageProject(String projectId, boolean reloadInsights, String... insightNames) throws Exception {
		Path folder = Files.createDirectories(dbDir.resolve(projectId + "_" + UUID.randomUUID()));
		IRDBMSEngine insightsDb = InsightsRDBMSUtils.generateInsightsDatabase(projectId, RdbmsTypeEnum.H2_DB,
				folder.toString());
		InsightAdministrator admin = new InsightAdministrator(insightsDb);
		for (String insightName : insightNames) {
			String insightId = insightName + "Id";
			admin.addInsight(insightId, insightName, "grid", new String[] { "AddPanel(0);" }, true, false, -1, null,
					null, false, null);
			admin.updateInsightTags(insightId, List.of(insightName + "Tag"));
		}
		IProject project = mock(IProject.class);
		when(project.getInsightDatabase()).thenReturn(insightsDb);
		DIHelper.getInstance().setProjectProperty(projectId, project);
		stagedProjects.add(projectId);

		Properties props = new Properties();
		props.setProperty(Constants.PROJECT, projectId);
		props.setProperty(Constants.PROJECT_ALIAS, projectId + "Name");
		props.setProperty(Constants.RELOAD_INSIGHTS, String.valueOf(reloadInsights));
		Path smss = UnitTestSecurityAuthUtils.createSmssFileFromProps(props, folder, projectId + ".smss");
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smss.toString());
		return smss;
	}

	///
	/// access request decisions and permission changes: one transaction, then
	/// notifications
	///

	@Test
	void testApproveProjectUserAccessRequests_notifiesOnlyAfterTheApprovalIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setUserAccessRequest("secondid", "NATIVE", "testProjectId", "reason", 2, secondUser);
		List<Map<String, String>> requests = List.of(Map.of("requestid", pendingRequestId("testProjectId"), "userid",
				"secondid", "type", "NATIVE", "permission", "EDIT"));

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class);
				MockedStatic<EmailUtility> emails = mockStatic(EmailUtility.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the save has committed: no transaction holds the connection, and
						// a read sees the grant and the approval
						assertFalse(insideSecurityTransaction());
						assertEquals(AccessPermissionEnum.EDIT.getId(),
								SecurityProjectUtils.getUserProjectPermission("secondid", "testProjectId"));
						assertTrue(SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId").isEmpty());
						return null;
					});

			SecurityProjectUtils.approveProjectUserAccessRequests(user, "testProjectId", requests, null);

			notifications.verify(() -> NotificationDbUtils.createNotification(eq(user), eq("secondid"), eq("NATIVE"),
					eq("testProjectId"), eq(NotificationConstants.Type.REQUEST_APPROVAL),
					eq(NotificationConstants.APP_CATALOG), any(), isNull(), eq("EDIT"), any()), times(1));
			emails.verify(() -> EmailUtility.sendAccessRequestApprovalEmailNotification(user, "secondid",
					"testProjectId", "EDIT", EmailUtility.RESOURCE_TYPE.PROJECT), times(1));
		}
	}

	@Test
	void testApproveProjectUserAccessRequests_aFailedApprovalSavesNothingAndTellsNoOne() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setUserAccessRequest("secondid", "NATIVE", "testProjectId", "reason", 2, secondUser);
		List<Map<String, String>> requests = List.of(Map.of("requestid", pendingRequestId("testProjectId"), "userid",
				"secondid", "type", "NATIVE", "permission", "EDIT"));

		// marking the request approved fails after the grant was written
		addCheck("APPROVE_FAIL", "PROJECTACCESSREQUEST", "APPROVER_DECISION <> 'APPROVED'");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class);
				MockedStatic<EmailUtility> emails = mockStatic(EmailUtility.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityProjectUtils.approveProjectUserAccessRequests(user, "testProjectId", requests, null));
			assertEquals("An error occurred while approving the access requests", e.getMessage());
			notifications.verifyNoInteractions();
			emails.verifyNoInteractions();
		} finally {
			dropCheck("APPROVE_FAIL", "PROJECTACCESSREQUEST");
		}

		assertNull(SecurityProjectUtils.getUserProjectPermission("secondid", "testProjectId"));
		assertEquals(1, SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId").size());
	}

	@Test
	void testDenyProjectUserAccessRequests_tellsTheProjectsRequestersOnlyAfterTheDenialIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		UnitTestSecurityAuthUtils.createProject("otherProjectId", "otherProjectName", user);
		SecurityProjectUtils.setUserAccessRequest("secondid", "NATIVE", "testProjectId", "reason", 2, secondUser);
		SecurityProjectUtils.setUserAccessRequest("secondid", "NATIVE", "otherProjectId", "reason", 3, secondUser);
		String requestId = pendingRequestId("testProjectId");
		String otherRequestId = pendingRequestId("otherProjectId");

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the denial has committed: no transaction holds the connection,
						// and a read sees it
						assertFalse(insideSecurityTransaction());
						assertTrue(SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId").isEmpty());
						return null;
					});

			// a request of another project is neither denied nor told
			SecurityProjectUtils.denyProjectUserAccessRequests(user, "testProjectId",
					List.of(requestId, otherRequestId));

			notifications.verify(() -> NotificationDbUtils.createNotification(eq(user), eq("secondid"), eq("NATIVE"),
					eq("testProjectId"), eq(NotificationConstants.Type.REQUEST_DENIAL),
					eq(NotificationConstants.APP_CATALOG), any(), isNull(), eq("EDIT"), any()), times(1));
			notifications.verify(() -> NotificationDbUtils.createNotification(any(), any(), any(), eq("otherProjectId"),
					any(), any(), any(), any(), any(), any()), never());
		}

		assertEquals(1, SecurityProjectUtils.getUserAccessRequestsByProject("otherProjectId").size());
	}

	@Test
	void testDenyProjectUserAccessRequests_aFailedDenialTellsNoOne() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		User secondUser = UnitTestSecurityAuthUtils.createUser("second", true);
		User thirdUser = UnitTestSecurityAuthUtils.createUser("third", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.setUserAccessRequest("secondid", "NATIVE", "testProjectId", "reason", 2, secondUser);
		SecurityProjectUtils.setUserAccessRequest("thirdid", "NATIVE", "testProjectId", "reason", 3, thirdUser);
		List<String> requestIds = new ArrayList<>();
		for (Map<String, Object> request : SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId")) {
			requestIds.add(request.get("ID").toString());
		}

		// the third person's request cannot be denied, so neither is
		addCheck("DENY_FAIL", "PROJECTACCESSREQUEST", "REQUEST_USERID <> 'thirdid' OR APPROVER_DECISION <> 'DENIED'");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityProjectUtils.denyProjectUserAccessRequests(user, "testProjectId", requestIds));
			assertEquals("An error occurred while updating user access request", e.getMessage());
			notifications.verifyNoInteractions();
		} finally {
			dropCheck("DENY_FAIL", "PROJECTACCESSREQUEST");
		}

		assertEquals(2, SecurityProjectUtils.getUserAccessRequestsByProject("testProjectId").size());
	}

	@Test
	void testAddProjectUserPermissions_notifiesOnlyAfterEveryoneIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createUser("third", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		List<Map<String, String>> permissions = List.of(Map.of("userid", "secondid", "permission", "READ_ONLY"),
				Map.of("userid", "thirdid", "permission", "EDIT"));

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the save has committed: no transaction holds the connection, and
						// a read sees everyone it added
						assertFalse(insideSecurityTransaction());
						assertEquals(AccessPermissionEnum.READ_ONLY.getId(),
								SecurityProjectUtils.getUserProjectPermission("secondid", "testProjectId"));
						assertEquals(AccessPermissionEnum.EDIT.getId(),
								SecurityProjectUtils.getUserProjectPermission("thirdid", "testProjectId"));
						return null;
					});

			SecurityProjectUtils.addProjectUserPermissions(user, "testProjectId", permissions, null);

			notifications.verify(() -> NotificationDbUtils.createNotification(any(), any(), any(), eq("testProjectId"),
					eq(NotificationConstants.Type.USER_ADDITION), eq(NotificationConstants.APP_CATALOG), any(), any(),
					any(), any()), times(2));
		}
	}

	@Test
	void testAddProjectUserPermissions_aFailedSaveAddsNoOneAndTellsNoOne() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createUser("third", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		List<Map<String, String>> permissions = List.of(Map.of("userid", "secondid", "permission", "READ_ONLY"),
				Map.of("userid", "thirdid", "permission", "EDIT"));

		// the second grant fails after the first was written
		addCheck("ADD_FAIL", "PROJECTPERMISSION", "USERID <> 'thirdid'");
		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> SecurityProjectUtils.addProjectUserPermissions(user, "testProjectId", permissions, null));
			assertEquals("An error occurred adding user permissions for this project", e.getMessage());
			notifications.verifyNoInteractions();
		} finally {
			dropCheck("ADD_FAIL", "PROJECTPERMISSION");
		}

		assertNull(SecurityProjectUtils.getUserProjectPermission("secondid", "testProjectId"));
		assertNull(SecurityProjectUtils.getUserProjectPermission("thirdid", "testProjectId"));
	}

	@Test
	void testEditProjectUserPermissions_notifiesOnlyAfterEveryChangeIsSaved() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createUser("second", false);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		SecurityProjectUtils.addProjectUser(user, "secondid", "testProjectId", "READ_ONLY", null);
		List<Map<String, String>> requests = List
				.of(Map.of("userid", "secondid", "type", "NATIVE", "permission", "EDIT"));

		try (MockedStatic<Utility> utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				MockedStatic<NotificationDbUtils> notifications = mockStatic(NotificationDbUtils.class)) {
			utility.when(Utility::isNotificationDatabaseEnabled).thenReturn(true);
			notifications.when(() -> NotificationDbUtils.createNotification(any(), any(), any(), any(), any(), any(),
					any(), any(), any(), any())).thenAnswer(invocation -> {
						// the save has committed: no transaction holds the connection, and
						// a read sees the change
						assertFalse(insideSecurityTransaction());
						assertEquals(AccessPermissionEnum.EDIT.getId(),
								SecurityProjectUtils.getUserProjectPermission("secondid", "testProjectId"));
						return null;
					});

			SecurityProjectUtils.editProjectUserPermissions(user, "testProjectId", requests, null);

			notifications.verify(() -> NotificationDbUtils.createNotification(eq(user), eq("secondid"), any(),
					eq("testProjectId"), eq(NotificationConstants.Type.PERMISSION_CHANGE),
					eq(NotificationConstants.APP_CATALOG), any(), eq("READ_ONLY"), eq("EDIT"), any()), times(1));
		}
	}

	///
	/// setProjectVisibility: every login or none
	///

	@Test
	void testSetProjectVisibility_savesEveryLoginOrNone() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("testProjectId", "testProjectName", user);
		AccessToken github = new AccessToken();
		github.setId("admingithub");
		github.setName("adminname");
		github.setProvider(AuthProvider.GITHUB);
		user.setAccessToken(github);

		// the second login's row fails after the first login's was written
		addCheck("VISIBILITY_FAIL", "USERPROJECTPREFERENCE", "TYPE <> 'GITHUB'");
		try {
			assertThrows(Exception.class,
					() -> SecurityProjectUtils.setProjectVisibility(user, "testProjectId", false));
		} finally {
			dropCheck("VISIBILITY_FAIL", "USERPROJECTPREFERENCE");
		}
		assertEquals(0L, count("SELECT COUNT(*) FROM USERPROJECTPREFERENCE WHERE PROJECTID=? AND VISIBILITY=FALSE",
				"testProjectId"));

		SecurityProjectUtils.setProjectVisibility(user, "testProjectId", false);
		assertEquals(2L, count("SELECT COUNT(*) FROM USERPROJECTPREFERENCE WHERE PROJECTID=? AND VISIBILITY=FALSE",
				"testProjectId"));
	}

	private long count(String sql, Object... params) throws SQLException {
		return ((Number) UnitTestGroupTables.queryValue(securityDb, sql, params)).longValue();
	}

	private static String pendingRequestId(String projectId) {
		List<Map<String, Object>> pending = SecurityProjectUtils.getUserAccessRequestsByProject(projectId);
		assertEquals(1, pending.size());
		return pending.getFirst().get("ID").toString();
	}

	/**
	 * Whether this thread is inside a security database transaction: a write holds
	 * the engine's connection until it has committed or rolled back.
	 */
	private boolean insideSecurityTransaction() throws SQLException {
		Connection connection = securityDb.getConnection();
		try {
			return Thread.holdsLock(connection);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, connection);
		}
	}

	/**
	 * Adds a check to a security table, so a test can fail a later statement of a
	 * save on purpose; {@link #dropCheck} takes it off again.
	 */
	private void addCheck(String name, String table, String condition) throws Exception {
		QueryExecutionUtility.executeUpdate(securityDb,
				"ALTER TABLE " + table + " ADD CONSTRAINT " + name + " CHECK (" + condition + ")",
				QueryExecutionUtility.NO_PARAMETERS);
	}

	private void dropCheck(String name, String table) throws Exception {
		QueryExecutionUtility.executeUpdate(securityDb, "ALTER TABLE " + table + " DROP CONSTRAINT " + name,
				QueryExecutionUtility.NO_PARAMETERS);
	}

}
