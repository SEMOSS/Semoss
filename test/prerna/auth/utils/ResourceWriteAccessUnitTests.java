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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.pipeline.PlatformOperation;
import prerna.util.SystemEngineRegistry;

/**
 * The check the security utilities make before they add, change or delete an
 * engine, a project or an insight.
 */
class ResourceWriteAccessUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String ENGINE_ID = "ordersdb";
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

		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "orders", owner);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(owner, "editorid", ENGINE_ID, "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(owner, "readerid", ENGINE_ID, "READ_ONLY");
		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "orders app", owner);
		UnitTestSecurityAuthUtils.createInsight(PROJECT_ID, INSIGHT_ID, "orders", "grid");
		SecurityInsightUtils.addUserInsightCreator(owner, PROJECT_ID, INSIGHT_ID);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void anEditorChangesAnEngineAndAReaderOrStrangerCannot() {
		assertDoesNotThrow(() -> ResourceWriteAccess.requireEngine(editor, ENGINE_ID, AccessPermissionEnum.EDIT));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceWriteAccess.requireEngine(reader, ENGINE_ID, AccessPermissionEnum.EDIT));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceWriteAccess.requireEngine(stranger, ENGINE_ID, AccessPermissionEnum.EDIT));
	}

	@Test
	void onlyAnOwnerDeletesAnEngine() {
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils.deleteEngine(editor, ENGINE_ID));
		assertTrue(SecurityEngineUtils.engineExists(ENGINE_ID));

		SecurityEngineUtils.deleteEngine(owner, ENGINE_ID);

		assertFalse(SecurityEngineUtils.engineExists(ENGINE_ID));
	}

	@Test
	void theCreatorChangesANewEngineUntilSomeoneIsGivenAccess() {
		SecurityEngineUtils.addEngine("newdb", "new", "new", IEngine.CATALOG_TYPE.DATABASE, null, null, false,
				stranger);
		PermissionTargetResource newEngine = PermissionTargetResource.engine("newdb");

		assertDoesNotThrow(() -> ResourceWriteAccess.requireEngine(stranger, "newdb", AccessPermissionEnum.EDIT));
		assertThrows(IllegalArgumentException.class, () -> ResourceWriteAccess.requireNewOwner(editor, newEngine));

		SecurityEngineUtils.addEngineOwner(stranger, "newdb");

		assertThrows(IllegalArgumentException.class, () -> ResourceWriteAccess.requireNewOwner(stranger, newEngine));
		assertTrue(SecurityEngineUtils.userIsOwner(stranger, "newdb"));
	}

	@Test
	void aCallWithNoUserPassesOnlyInsideAPlatformOperation() {
		assertThrows(IllegalArgumentException.class,
				() -> ResourceWriteAccess.requireEngine(null, ENGINE_ID, AccessPermissionEnum.OWNER));
		assertThrows(IllegalArgumentException.class, () -> ResourceWriteAccess.requirePlatform("test"));

		try (PlatformOperation operation = PlatformOperation.begin("test")) {
			assertDoesNotThrow(() -> ResourceWriteAccess.requireEngine(null, ENGINE_ID, AccessPermissionEnum.OWNER));
			assertDoesNotThrow(() -> ResourceWriteAccess.requirePlatform("test"));
		}
	}

	@Test
	void anAdminActsAsOneOnlyInAnAdminOperation() {
		assertThrows(IllegalArgumentException.class,
				() -> ResourceWriteAccess.requireEngine(admin, ENGINE_ID, AccessPermissionEnum.OWNER));
		assertThrows(IllegalArgumentException.class, () -> ResourceWriteAccess.requireAdmin(admin, "test"));

		AdminWriteAccessCaller.run(() -> {
			assertDoesNotThrow(() -> ResourceWriteAccess.requireEngine(admin, ENGINE_ID, AccessPermissionEnum.OWNER));
			assertDoesNotThrow(() -> ResourceWriteAccess.requireAdmin(admin, "test"));
			assertThrows(IllegalArgumentException.class,
					() -> ResourceWriteAccess.requireEngine(stranger, ENGINE_ID, AccessPermissionEnum.OWNER));
		});
	}

	@Test
	void aProjectOwnerChangesItsInsightsAndOthersCannot() {
		assertDoesNotThrow(
				() -> ResourceWriteAccess.requireInsight(owner, PROJECT_ID, INSIGHT_ID, AccessPermissionEnum.EDIT));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceWriteAccess.requireInsight(stranger, PROJECT_ID, INSIGHT_ID, AccessPermissionEnum.EDIT));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceWriteAccess.requireNewInsightOwner(owner, PROJECT_ID, INSIGHT_ID));
	}

	@Test
	void aProjectIsDeletedByItsOwnerOnly() {
		assertThrows(IllegalArgumentException.class, () -> SecurityProjectUtils.deleteProject(stranger, PROJECT_ID));
		assertTrue(SecurityProjectUtils.projectExists(PROJECT_ID));

		SecurityProjectUtils.deleteProject(owner, PROJECT_ID);

		assertFalse(SecurityProjectUtils.projectExists(PROJECT_ID));
	}
}
