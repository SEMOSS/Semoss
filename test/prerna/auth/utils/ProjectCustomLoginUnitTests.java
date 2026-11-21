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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.SystemEngineRegistry;

/**
 * Tests a project's custom login flag: a project has no login page of its own
 * until one of its owners, or an admin, says it does, each change is audited,
 * and project lists carry the flag.
 */
class ProjectCustomLoginUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String PROJECT_ID = "loginproject";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User admin;
	private User owner;
	private User editor;

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);

		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, PROJECT_ID, owner);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(owner, PROJECT_ID, "editorid", "EDIT");
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void aNewProjectHasNoCustomLoginPage() {
		assertFalse(SecurityProjectUtils.projectHasCustomLogin(PROJECT_ID));
		assertFalse(SecurityProjectUtils.projectHasCustomLogin("no-such-project"));
	}

	@Test
	void itsOwnerCanTurnItOnAndOff() throws Exception {
		SecurityProjectUtils.setProjectCustomLogin(owner, PROJECT_ID, true);
		assertTrue(SecurityProjectUtils.projectHasCustomLogin(PROJECT_ID));

		SecurityProjectUtils.setProjectCustomLogin(owner, PROJECT_ID, false);
		assertFalse(SecurityProjectUtils.projectHasCustomLogin(PROJECT_ID));
	}

	@Test
	void anEditorCannotChangeIt() {
		assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.setProjectCustomLogin(editor, PROJECT_ID, true));
		assertFalse(SecurityProjectUtils.projectHasCustomLogin(PROJECT_ID));
	}

	@Test
	void anAdminCanChangeItOnAnyProject() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		adminUtils.setProjectCustomLogin(PROJECT_ID, true);
		assertTrue(SecurityProjectUtils.projectHasCustomLogin(PROJECT_ID));

		adminUtils.setProjectCustomLogin(PROJECT_ID, false);
		assertFalse(SecurityProjectUtils.projectHasCustomLogin(PROJECT_ID));
	}

	@Test
	void anOwnersChangeIsAuditedWithItsNewValue() throws Exception {
		try (var audit = mockStatic(UserAuditTrailUtils.class)) {
			SecurityProjectUtils.setProjectCustomLogin(owner, PROJECT_ID, true);
			SecurityProjectUtils.setProjectCustomLogin(owner, PROJECT_ID, false);

			audit.verify(
					() -> UserAuditTrailUtils.recordProjectLifecycle(owner, AuditEventType.PROJECT_CUSTOM_LOGIN_UPDATE,
							PROJECT_ID, null, Map.of("field", "customLogin", "customLogin", true)));
			audit.verify(
					() -> UserAuditTrailUtils.recordProjectLifecycle(owner, AuditEventType.PROJECT_CUSTOM_LOGIN_UPDATE,
							PROJECT_ID, null, Map.of("field", "customLogin", "customLogin", false)));
		}
	}

	@Test
	void anAdminsChangeIsAuditedAsAnAdmin() {
		List<JsonObject> events = new ArrayList<>();
		try (var audit = mockStatic(UserAuditTrailUtils.class)) {
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
				events.add(
						ProjectPermissionAuditUnitTests.AUDIT_JSON.toJsonTree(call.getArgument(0)).getAsJsonObject());
				return null;
			});
			SecurityAdminUtils.getInstance(admin).setProjectCustomLogin(PROJECT_ID, true);
		}

		List<JsonObject> customLoginEvents = events.stream()
				.filter(e -> "PROJECT_CUSTOM_LOGIN_UPDATE".equals(e.get("eventType").getAsString())).toList();
		assertEquals(1, customLoginEvents.size());
		JsonObject event = customLoginEvents.get(0);
		assertEquals("PROJECT", event.get("targetType").getAsString());
		assertEquals(PROJECT_ID, event.get("targetId").getAsString());
		assertTrue(event.get("actorIsAdmin").getAsBoolean());
		assertEquals("customLogin", event.getAsJsonObject("detailsRaw").get("field").getAsString());
		assertTrue(event.getAsJsonObject("detailsRaw").get("customLogin").getAsBoolean());
	}

	@Test
	void aRefusedChangeIsNotAuditedAsMade() {
		try (var audit = mockStatic(UserAuditTrailUtils.class)) {
			assertThrows(IllegalAccessException.class,
					() -> SecurityProjectUtils.setProjectCustomLogin(editor, PROJECT_ID, true));

			audit.verify(() -> UserAuditTrailUtils.recordProjectLifecycle(any(), any(), any(), any(), any()), never());
		}
	}

	@Test
	void aProjectThatDoesNotExistIsRefused() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		assertThrows(IllegalArgumentException.class, () -> adminUtils.setProjectCustomLogin("no-such-project", true));
	}

	@Test
	void projectListsCarryTheFlag() throws Exception {
		SecurityProjectUtils.setProjectCustomLogin(owner, PROJECT_ID, true);

		List<Map<String, Object>> projects = SecurityProjectUtils.getUserProjectList(owner, PROJECT_ID);
		assertEquals(1, projects.size());
		assertEquals(Boolean.TRUE, projects.get(0).get("project_has_custom_login"));

		List<Map<String, Object>> adminProjects = SecurityAdminUtils.getInstance(admin)
				.getAllProjectSettings(List.of(PROJECT_ID), null, null, null, null, null);
		assertEquals(Boolean.TRUE, adminProjects.get(0).get("project_has_custom_login"));
	}
}
