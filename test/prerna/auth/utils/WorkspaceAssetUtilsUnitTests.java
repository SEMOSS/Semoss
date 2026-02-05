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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;

public class WorkspaceAssetUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;

	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = AbstractSecurityUtils.securityDb;
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// Constants
	///

	@Test
	void testConstants() {
		assertEquals("Workspace", WorkspaceAssetUtils.WORKSPACE_APP_NAME);
		assertEquals("Asset", WorkspaceAssetUtils.ASSET_APP_NAME);
		assertEquals(".semoss", WorkspaceAssetUtils.HIDDEN_FILE);
	}

	///
	/// registerUserWorkspaceProject
	///

	@Test
	void testRegisterUserWorkspaceProject_WithAccessToken() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken token = user.getAccessToken(AuthProvider.NATIVE);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(token, projectId);

		// Verify the workspace was registered
		String retrievedProjectId = WorkspaceAssetUtils.getUserWorkspaceProject(token);
		assertEquals(projectId, retrievedProjectId);
	}

	@Test
	void testRegisterUserWorkspaceProject_WithUserAndProvider() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(user, AuthProvider.NATIVE, projectId);

		// Verify the workspace was registered
		String retrievedProjectId = WorkspaceAssetUtils.getUserWorkspaceProject(user, AuthProvider.NATIVE);
		assertEquals(projectId, retrievedProjectId);
	}

	///
	/// registerUserAssetProject
	///

	@Test
	void testRegisterUserAssetProject_WithAccessToken() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken token = user.getAccessToken(AuthProvider.NATIVE);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(token, projectId);

		// Verify the asset was registered
		String retrievedProjectId = WorkspaceAssetUtils.getUserAssetProject(token);
		assertEquals(projectId, retrievedProjectId);
	}

	@Test
	void testRegisterUserAssetProject_WithUserAndProvider() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, projectId);

		// Verify the asset was registered
		String retrievedProjectId = WorkspaceAssetUtils.getUserAssetProject(user, AuthProvider.NATIVE);
		assertEquals(projectId, retrievedProjectId);
	}

	///
	/// getUserWorkspaceProject
	///

	@Test
	void testGetUserWorkspaceProject_Exists() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(user, AuthProvider.NATIVE, projectId);

		String retrievedProjectId = WorkspaceAssetUtils.getUserWorkspaceProject(user, AuthProvider.NATIVE);
		assertNotNull(retrievedProjectId);
		assertEquals(projectId, retrievedProjectId);
	}

	@Test
	void testGetUserWorkspaceProject_NotExists() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		String retrievedProjectId = WorkspaceAssetUtils.getUserWorkspaceProject(user, AuthProvider.NATIVE);
		assertNull(retrievedProjectId);
	}

	@Test
	void testGetUserWorkspaceProject_WithAccessToken() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken token = user.getAccessToken(AuthProvider.NATIVE);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(token, projectId);

		String retrievedProjectId = WorkspaceAssetUtils.getUserWorkspaceProject(token);
		assertNotNull(retrievedProjectId);
		assertEquals(projectId, retrievedProjectId);
	}

	///
	/// getUserAssetProject
	///

	@Test
	void testGetUserAssetProject_Exists() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, projectId);

		String retrievedProjectId = WorkspaceAssetUtils.getUserAssetProject(user, AuthProvider.NATIVE);
		assertNotNull(retrievedProjectId);
		assertEquals(projectId, retrievedProjectId);
	}

	@Test
	void testGetUserAssetProject_NotExists() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		String retrievedProjectId = WorkspaceAssetUtils.getUserAssetProject(user, AuthProvider.NATIVE);
		assertNull(retrievedProjectId);
	}

	@Test
	void testGetUserAssetProject_WithAccessToken() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		AccessToken token = user.getAccessToken(AuthProvider.NATIVE);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(token, projectId);

		String retrievedProjectId = WorkspaceAssetUtils.getUserAssetProject(token);
		assertNotNull(retrievedProjectId);
		assertEquals(projectId, retrievedProjectId);
	}

	///
	/// isAssetOrWorkspaceProject
	///

	@Test
	void testIsAssetOrWorkspaceProject_IsWorkspace() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(user, AuthProvider.NATIVE, projectId);

		assertTrue(WorkspaceAssetUtils.isAssetOrWorkspaceProject(projectId));
	}

	@Test
	void testIsAssetOrWorkspaceProject_IsAsset() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, projectId);

		assertTrue(WorkspaceAssetUtils.isAssetOrWorkspaceProject(projectId));
	}

	@Test
	void testIsAssetOrWorkspaceProject_IsNeither() {
		String projectId = UUID.randomUUID().toString();

		assertFalse(WorkspaceAssetUtils.isAssetOrWorkspaceProject(projectId));
	}

	@Test
	void testIsAssetOrWorkspaceProject_RegularProject() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		// Regular project should not be workspace or asset
		assertFalse(WorkspaceAssetUtils.isAssetOrWorkspaceProject("pid1"));
	}

	///
	/// isAssetProject
	///

	@Test
	void testIsAssetProject_True() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, projectId);

		assertTrue(WorkspaceAssetUtils.isAssetProject(projectId));
	}

	@Test
	void testIsAssetProject_False_Workspace() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		String projectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(user, AuthProvider.NATIVE, projectId);

		// Workspace is not an asset
		assertFalse(WorkspaceAssetUtils.isAssetProject(projectId));
	}

	@Test
	void testIsAssetProject_False_RegularProject() {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);
		UnitTestSecurityAuthUtils.createProject("pid1", "pname1", user);

		assertFalse(WorkspaceAssetUtils.isAssetProject("pid1"));
	}

	@Test
	void testIsAssetProject_False_NotExists() {
		String projectId = UUID.randomUUID().toString();

		assertFalse(WorkspaceAssetUtils.isAssetProject(projectId));
	}

	///
	/// Multiple Users
	///

	@Test
	void testMultipleUsersHaveDifferentWorkspaces() throws Exception {
		User user1 = UnitTestSecurityAuthUtils.createUser("user1", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		String projectId1 = UUID.randomUUID().toString();
		String projectId2 = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(user1, AuthProvider.NATIVE, projectId1);
		WorkspaceAssetUtils.registerUserWorkspaceProject(user2, AuthProvider.NATIVE, projectId2);

		assertEquals(projectId1, WorkspaceAssetUtils.getUserWorkspaceProject(user1, AuthProvider.NATIVE));
		assertEquals(projectId2, WorkspaceAssetUtils.getUserWorkspaceProject(user2, AuthProvider.NATIVE));
	}

	@Test
	void testMultipleUsersHaveDifferentAssets() throws Exception {
		User user1 = UnitTestSecurityAuthUtils.createUser("user1", true);
		User user2 = UnitTestSecurityAuthUtils.createUser("user2", false);

		String projectId1 = UUID.randomUUID().toString();
		String projectId2 = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserAssetProject(user1, AuthProvider.NATIVE, projectId1);
		WorkspaceAssetUtils.registerUserAssetProject(user2, AuthProvider.NATIVE, projectId2);

		assertEquals(projectId1, WorkspaceAssetUtils.getUserAssetProject(user1, AuthProvider.NATIVE));
		assertEquals(projectId2, WorkspaceAssetUtils.getUserAssetProject(user2, AuthProvider.NATIVE));
	}

	@Test
	void testUserCanHaveBothWorkspaceAndAsset() throws Exception {
		User user = UnitTestSecurityAuthUtils.createUser("admin", true);

		String workspaceProjectId = UUID.randomUUID().toString();
		String assetProjectId = UUID.randomUUID().toString();

		WorkspaceAssetUtils.registerUserWorkspaceProject(user, AuthProvider.NATIVE, workspaceProjectId);
		WorkspaceAssetUtils.registerUserAssetProject(user, AuthProvider.NATIVE, assetProjectId);

		assertEquals(workspaceProjectId, WorkspaceAssetUtils.getUserWorkspaceProject(user, AuthProvider.NATIVE));
		assertEquals(assetProjectId, WorkspaceAssetUtils.getUserAssetProject(user, AuthProvider.NATIVE));

		// Workspace and asset are different
		assertTrue(WorkspaceAssetUtils.isAssetOrWorkspaceProject(workspaceProjectId));
		assertTrue(WorkspaceAssetUtils.isAssetOrWorkspaceProject(assetProjectId));
		assertFalse(WorkspaceAssetUtils.isAssetProject(workspaceProjectId));
		assertTrue(WorkspaceAssetUtils.isAssetProject(assetProjectId));
	}
}
