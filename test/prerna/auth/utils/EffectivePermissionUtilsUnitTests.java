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
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SystemEngineRegistry;

/**
 * Runs the effective access read against a disposable security database: one
 * row per person, with the most access from being given it directly and from
 * each custom group they are in.
 */
class EffectivePermissionUtilsUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String NATIVE = AuthProvider.NATIVE.getLabel();
	private static final String MICROSOFT = AuthProvider.MICROSOFT.getLabel();

	private static final String PROJECT_ID = "effectiveproject";
	private static final String GROUP_ID = "team";
	private static final String PROVIDER_GROUP_ID = "msgroup";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User admin;
	private User editor;
	private AdminSecurityGroupUtils groups;
	private PermissionTargetResource project;

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		UnitTestSecurityAuthUtils.createUser("reader", false);
		UnitTestSecurityAuthUtils.createUser("member", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);
		project = new PermissionTargetResource(PermissionTargetResource.Kind.PROJECT, PROJECT_ID, null);

		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "effective project", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "editorid", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "readerid", "READ_ONLY");

		groups.addGroup(admin, GROUP_ID, CUSTOM, "the team");
		groups.addGroup(admin, PROVIDER_GROUP_ID, MICROSOFT, "a login group");
		groups.addGroupProjectPermission(admin, GROUP_ID, CUSTOM, PROJECT_ID, 2, null);
		groups.addGroupProjectPermission(admin, PROVIDER_GROUP_ID, MICROSOFT, PROJECT_ID, 3, null);
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "readerid", NATIVE);
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "memberid", NATIVE);
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void getEffectiveAccess_mergesDirectAndTeamAccessPerPerson() throws Exception {
		Map<String, Object> access = EffectivePermissionUtils.getEffectivePermissions(editor, project);

		List<Map<String, Object>> people = list(access.get("people"));
		// the reader's own Read-Only access and the team's Editor access, the most
		// first
		Map<String, Object> readerRow = rowFor(people, "readerid");
		assertEquals("EDIT", readerRow.get("level"));
		List<Map<String, Object>> sources = list(readerRow.get("sources"));
		assertEquals(2, sources.size());
		assertEquals("TEAM", sources.get(0).get("kind"));
		assertEquals(GROUP_ID, sources.get(0).get("id"));
		assertEquals("DIRECT", sources.get(1).get("kind"));
		assertEquals("READ_ONLY", sources.get(1).get("level"));

		// a member with no access of their own still has a row
		Map<String, Object> memberRow = rowFor(people, "memberid");
		assertEquals("EDIT", memberRow.get("level"));
		assertEquals(1, list(memberRow.get("sources")).size());

		assertEquals("OWNER", rowFor(people, "adminid").get("level"));
		assertEquals(4, people.size());
	}

	@Test
	void getEffectiveAccess_listsLoginProviderTeamsApart() throws Exception {
		List<Map<String, Object>> unlisted = list(
				EffectivePermissionUtils.getEffectivePermissions(admin, project).get("unlistedTeams"));

		assertEquals(1, unlisted.size());
		assertEquals(PROVIDER_GROUP_ID, unlisted.get(0).get("id"));
		assertEquals("READ_ONLY", unlisted.get(0).get("level"));
	}

	@Test
	void getEffectiveAccess_isForEditorsOwnersAndAdmins() throws Exception {
		// the reader edits through the team, so a reader outside it is used here
		User outsider = UnitTestSecurityAuthUtils.createUser("outsider", false);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "outsiderid", "READ_ONLY");

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> EffectivePermissionUtils.getEffectivePermissions(outsider, project));
		assertEquals("Only this project's editors and owners can see who can use it in the end", e.getMessage());
		assertThrows(IllegalArgumentException.class, () -> EffectivePermissionUtils.getEffectivePermissions(admin,
				new PermissionTargetResource(PermissionTargetResource.Kind.TEAM, GROUP_ID, null)));
	}

	@Test
	void getEffectiveAccess_givesAnAdminNothingExtraOutsideAdminMode() throws Exception {
		User otherAdmin = UnitTestSecurityAuthUtils.createUser("otheradmin", true);

		assertThrows(IllegalAccessException.class,
				() -> EffectivePermissionUtils.getEffectivePermissions(otherAdmin, project));

		Map<String, Object> access = EffectivePermissionUtils
				.getEffectivePermissions(SecurityAdminUtils.getInstance(otherAdmin), otherAdmin, project);
		assertEquals(4, list(access.get("people")).size());
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> list(Object value) {
		return (List<Map<String, Object>>) value;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> rowFor(List<Map<String, Object>> rows, String userId) {
		return rows.stream().filter(row -> userId.equals(((Map<String, Object>) row.get("subject")).get("id")))
				.findFirst().orElseThrow();
	}
}
