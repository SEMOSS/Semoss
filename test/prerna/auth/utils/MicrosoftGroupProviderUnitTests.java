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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.SocialPropertiesProcessor;
import prerna.util.SystemEngineRegistry;

class MicrosoftGroupProviderUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final List<String> GROUP_TABLES = List.of("SMSS_GROUP", "GROUPENGINEPERMISSION",
			"GROUPPROJECTPERMISSION", "GROUPINSIGHTPERMISSION");

	private IRDBMSEngine securityDb;
	private User admin;
	private User member;
	private AdminSecurityGroupUtils groups;
	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		groups = AdminSecurityGroupUtils.getInstance(admin);
		AccessToken token = new AccessToken();
		token.setId("member");
		token.setProvider(AuthProvider.MICROSOFT);
		token.setUserGroupType(AuthProvider.MICROSOFT.getLabel());
		token.setUserGroups(Set.of("group"));
		member = new User();
		member.setAccessToken(token);
		member.setPrimaryLogin(AuthProvider.MICROSOFT);
	}

	@AfterEach
	void cleanup() throws Exception {
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void socialPropertiesAliasProducesPermissionsThatMatchTheLoginToken() throws Exception {
		var config = tempDir.resolve("microsoft-groups-social.properties");
		Files.writeString(config, "ms_login=true\nms_groups=true\n");
		var provider = new SocialPropertiesProcessor(config.toString()).getAvailableProviders().get(0);
		assertEquals("ms", provider.get("provider"));
		String type = AuthProvider.getProviderLabel(provider.get("provider").toString());
		createPermissions(type);
		assertStoredType("group", "MICROSOFT");
		assertMemberPermissions();
	}

	@ParameterizedTest
	@ValueSource(strings = { "MS", "ms", "Ms", "mS", "microsoft", "Microsoft", "MiCrOsOfT" })
	void startupMigratesExistingMicrosoftGroupsAndPermissions(String legacyType) throws Exception {
		createPermissions(legacyType);
		assertFalse(SecurityGroupEngineUtils.userGroupCanViewEngine(member, "engine"));
		assertFalse(SecurityGroupProjectUtils.userGroupCanViewProject(member, "project"));
		groups.addGroup(admin, "custom-group", "Custom-Realm", "custom namespace");
		groups.addGroup(admin, "canonical-group", "MICROSOFT", "already canonical");

		AbstractSecurityUtils.loadSecurityDatabase();
		assertStoredType("group", "MICROSOFT");
		assertMemberPermissions();
		assertTrue(groups.groupExists("custom-group", "Custom-Realm"));
		assertTrue(groups.groupExists("canonical-group", "MICROSOFT"));

		// Re-running startup must preserve the grants and avoid duplicate rows.
		AbstractSecurityUtils.loadSecurityDatabase();
		assertStoredType("group", "MICROSOFT");
		assertMemberPermissions();
	}

	private void createPermissions(String type) throws Exception {
		groups.addGroup(admin, "group", type, "Microsoft group");
		UnitTestSecurityAuthUtils.createEngine("engine", "engine name", admin);
		UnitTestSecurityAuthUtils.createProject("project", "project name", admin);
		UnitTestSecurityAuthUtils.createInsight("project", "insight", "insight name", "grid");
		groups.addGroupEnginePermission(admin, "group", type, "engine", 3, null);
		groups.addGroupProjectPermission(admin, "group", type, "project", 3, null);
		SecurityGroupInsightsUtils.addInsightGroupPermission(admin, "group", type, "project", "insight", "READ_ONLY",
				null);
	}

	private void assertMemberPermissions() throws Exception {
		assertEquals(Set.of("group"), AdminSecurityGroupUtils.getMatchingGroupsByType(Set.of("group"),
				member.getPrimaryLoginToken().getUserGroupType()));
		assertTrue(SecurityGroupEngineUtils.userGroupCanViewEngine(member, "engine"));
		assertTrue(SecurityGroupProjectUtils.userGroupCanViewProject(member, "project"));
		assertEquals(3, SecurityGroupInsightsUtils.getGroupInsightPermission("group",
				member.getPrimaryLoginToken().getUserGroupType(), "project", "insight"));
		assertFalse(SecurityGroupEngineUtils.userGroupCanEditEngine(member, "engine"));
		assertFalse(SecurityGroupProjectUtils.userGroupCanEditProject(member, "project"));
	}

	private void assertStoredType(String groupId, String expectedType) throws Exception {
		try (Connection connection = securityDb.getConnection()) {
			for (String table : GROUP_TABLES) {
				try (PreparedStatement statement = connection
						.prepareStatement("SELECT TYPE FROM " + table + " WHERE ID=?")) {
					statement.setString(1, groupId);
					try (ResultSet rows = statement.executeQuery()) {
						assertTrue(rows.next(), table);
						assertEquals(expectedType, rows.getString(1), table);
						assertFalse(rows.next(), table);
					}
				}
			}
		}
	}
}
