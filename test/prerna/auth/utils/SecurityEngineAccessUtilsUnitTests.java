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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

/**
 * What the engine proxy's check reads from the security database: a user's
 * level on an engine or project, what the platform may read for a user with no
 * permission, who is an admin, and which engines and projects are listed. Each
 * test is one rule of docs/platform_services/engine_access.md.
 */
class SecurityEngineAccessUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String ENGINE_ID = "ordersdb";
	private static final String PUBLIC_ENGINE_ID = "publicdb";
	private static final String PROJECT_ID = "ordersapp";
	private static final String INSIGHT_ID = "insight-1";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User admin;
	private User owner;
	private User editor;
	private User reader;
	private User stranger;

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		stranger = UnitTestSecurityAuthUtils.createUser("stranger", false);

		UnitTestSecurityAuthUtils.createEngineGlobal(PUBLIC_ENGINE_ID, "public", owner);
		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "orders app", owner);
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "orders", owner);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(owner, "editorid", ENGINE_ID, "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(owner, "readerid", ENGINE_ID, "READ_ONLY");
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void theServerHoldsTheSecurityDatabase() {
		assertTrue(SecurityEngineAccessUtils.isSecurityDatabaseLoaded());
	}

	@Test
	void aUsersLevelOnAnEngineIsTheirHighestGrant() {
		assertEquals(AccessPermissionEnum.OWNER, SecurityEngineAccessUtils.getUserPermission(owner, ENGINE_ID, false));
		assertEquals(AccessPermissionEnum.EDIT, SecurityEngineAccessUtils.getUserPermission(editor, ENGINE_ID, false));
		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getUserPermission(reader, ENGINE_ID, false));
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, ENGINE_ID, false));
		assertNull(SecurityEngineAccessUtils.getUserPermission(admin, ENGINE_ID, false));
	}

	@Test
	void everyoneCanViewAPublicEngine() {
		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getUserPermission(stranger, PUBLIC_ENGINE_ID, false));
		assertEquals(AccessPermissionEnum.OWNER,
				SecurityEngineAccessUtils.getUserPermission(owner, PUBLIC_ENGINE_ID, false));
	}

	@Test
	void aUsersLevelOnAProjectIsReadFromTheProjectsOwnGrants() {
		assertEquals(AccessPermissionEnum.OWNER, SecurityEngineAccessUtils.getUserPermission(owner, PROJECT_ID, true));
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, PROJECT_ID, true));
		assertNull(SecurityEngineAccessUtils.getUserPermission(owner, PROJECT_ID, false));
	}

	@Test
	void noUserAndNoIdHoldNothing() {
		assertNull(SecurityEngineAccessUtils.getUserPermission(null, ENGINE_ID, false));
		assertNull(SecurityEngineAccessUtils.getUserPermission(owner, null, false));
		assertNull(SecurityEngineAccessUtils.getUserPermission(owner, "no-such-engine", false));
		assertFalse(SecurityEngineAccessUtils.isAdmin(null));
		assertFalse(SecurityEngineAccessUtils.isReadableThroughPlatform(null, ENGINE_ID, false));
		assertNull(SecurityEngineAccessUtils.getProjectPermission(null, ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getProjectPermission(PROJECT_ID, null));
	}

	@Test
	void aSavedPermissionIsReadAtOnce() throws Exception {
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, ENGINE_ID, false));

		SecurityEngineUtils.addEngineUser(owner, "strangerid", ENGINE_ID, "EDIT", null, null);
		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getUserPermission(stranger, ENGINE_ID, false));

		SecurityEngineUtils.removeEngineUser(owner, "strangerid", ENGINE_ID);
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, ENGINE_ID, false));
	}

	@Test
	void theOwnerOfANewEngineHoldsItAtOnce() {
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, "newdb", false));

		UnitTestSecurityAuthUtils.createEngine("newdb", "new", stranger);

		assertEquals(AccessPermissionEnum.OWNER, SecurityEngineAccessUtils.getUserPermission(stranger, "newdb", false));
	}

	@Test
	void anAdminIsKnownAsOne() {
		assertTrue(SecurityEngineAccessUtils.isAdmin(admin));
		assertFalse(SecurityEngineAccessUtils.isAdmin(owner));
	}

	@Test
	void anEngineOrProjectIsListedWhileTheSecurityDatabaseHasIt() {
		assertTrue(SecurityEngineAccessUtils.isListed(ENGINE_ID, false));
		assertTrue(SecurityEngineAccessUtils.isListed(PROJECT_ID, true));
		assertFalse(SecurityEngineAccessUtils.isListed("no-such-engine", false));
		assertFalse(SecurityEngineAccessUtils.isListed(ENGINE_ID, true));
		assertFalse(SecurityEngineAccessUtils.isListed(null, false));

		SecurityEngineUtils.deleteEngine(ENGINE_ID);
		assertFalse(SecurityEngineAccessUtils.isListed(ENGINE_ID, false));
	}

	@Test
	void thePlatformReadsADiscoverableEngineForAnyone() throws Exception {
		assertFalse(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, ENGINE_ID, false));

		SecurityEngineUtils.setEngineDiscoverable(owner, ENGINE_ID, true);
		SecurityModelUsageLimitUtils.markChanged();

		assertTrue(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, ENGINE_ID, false));
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, ENGINE_ID, false));
	}

	@Test
	void thePlatformReadsADiscoverableProjectForAnyone() throws Exception {
		assertFalse(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, PROJECT_ID, true));

		SecurityProjectUtils.setProjectDiscoverable(owner, PROJECT_ID, true);
		SecurityModelUsageLimitUtils.markChanged();

		assertTrue(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, PROJECT_ID, true));
	}

	@Test
	void thePlatformReadsAProjectForAUserGivenOneOfItsInsights() {
		UnitTestSecurityAuthUtils.createInsight(PROJECT_ID, INSIGHT_ID, "an insight", "grid");
		assertFalse(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, PROJECT_ID, true));

		SecurityInsightUtils.addInsightUser(owner, "strangerid", PROJECT_ID, INSIGHT_ID, "READ_ONLY", null);

		assertTrue(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, PROJECT_ID, true));
		assertFalse(SecurityEngineAccessUtils.isReadableThroughPlatform(reader, PROJECT_ID, true));
		assertNull(SecurityEngineAccessUtils.getUserPermission(stranger, PROJECT_ID, true));
	}

	@Test
	void thePlatformReadsAProjectWithAPublicInsightForAnyone() throws Exception {
		UnitTestSecurityAuthUtils.createInsight(PROJECT_ID, INSIGHT_ID, "an insight", "grid");
		assertFalse(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, PROJECT_ID, true));

		SecurityInsightUtils.setInsightGlobalWithinProject(owner, PROJECT_ID, INSIGHT_ID, true);
		SecurityModelUsageLimitUtils.markChanged();

		assertTrue(SecurityEngineAccessUtils.isReadableThroughPlatform(stranger, PROJECT_ID, true));
	}

}
