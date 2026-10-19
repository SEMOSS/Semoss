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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.util.SystemEngineRegistry;

/**
 * Checks the single and bulk permission methods that save their changes as
 * permission batches: the options that keep each method's behavior, and the
 * rules every method follows.
 */
class PermissionBatchUtilsSaveChangesUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String PROJECT_ID = "savedproject";
	private static final String ENGINE_ID = "savedengine";
	private static final String INSIGHT_ID = "savedinsight";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private MockedStatic<MicrosoftGraphUserLookup> directory;

	private User admin;
	private User owner;
	private User reader;

	@BeforeEach
	void setup() throws Exception {
		directory = mockStatic(MicrosoftGraphUserLookup.class);
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		UnitTestSecurityAuthUtils.createUser("editor", false);
		reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		UnitTestSecurityAuthUtils.createUser("stranger", false);

		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "saved project", admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "ownerid", "OWNER");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "editorid", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(admin, PROJECT_ID, "readerid", "READ_ONLY");
	}

	@AfterEach
	void cleanup() throws Exception {
		directory.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// rules every method follows
	///

	@Test
	void unknownPeopleAreNotGivenAccess() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.addProjectUser(owner, "nobody", PROJECT_ID, "READ_ONLY", null));

		assertEquals("User nobody does not exist", e.getMessage());
	}

	@Test
	void aPersonListedTwiceIsRefused() {
		List<Map<String, String>> permissions = List.of(Map.of("userid", "strangerid", "permission", "READ_ONLY"),
				Map.of("userid", "strangerid", "permission", "EDIT"));

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.addProjectUserPermissions(owner, PROJECT_ID, permissions, null));

		assertEquals("The following users are listed more than once: strangerid", e.getMessage());
	}

	@Test
	void anAdminIsAnOrdinaryUserOutsideAdminMode() {
		User otherAdmin = UnitTestSecurityAuthUtils.createUser("otheradmin", true);

		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.addProjectUser(otherAdmin, "strangerid", PROJECT_ID, "READ_ONLY", null));

		assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
	}

	@Test
	void aBlockedBulkChangeSavesNothing() throws Exception {
		List<Map<String, String>> requests = List.of(Map.of("userid", "readerid", "permission", "EDIT"),
				Map.of("userid", "strangerid", "permission", "EDIT"));

		assertThrows(IllegalArgumentException.class,
				() -> SecurityProjectUtils.editProjectUserPermissions(owner, PROJECT_ID, requests, null));

		assertEquals(3, SecurityProjectUtils.getUserProjectPermission("readerid", PROJECT_ID));
	}

	///
	/// rows with no level
	///

	@Test
	void givingAccessFillsInARowWithNoLevelRatherThanAddingASecondRow() throws Exception {
		insertRowWithNoLevel("PROJECTPERMISSION", "PROJECTID", PROJECT_ID, "strangerid");

		SecurityProjectUtils.addProjectUser(owner, "strangerid", PROJECT_ID, "EDIT", null);

		assertEquals(1L,
				((Number) UnitTestGroupTables.queryValue(securityDb,
						"SELECT COUNT(*) FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?", "strangerid",
						PROJECT_ID)).longValue());
		assertEquals(2, SecurityProjectUtils.getUserProjectPermission("strangerid", PROJECT_ID));
	}

	///
	/// leaving a project
	///

	@Test
	void anyoneCanLeaveAProjectAndTheirInsightRightsGoWithIt() throws Exception {
		UnitTestSecurityAuthUtils.createInsight(PROJECT_ID, INSIGHT_ID, "saved insight", "grid");
		SecurityInsightUtils.addInsightUser(owner, "readerid", PROJECT_ID, INSIGHT_ID, "READ_ONLY", null);

		SecurityProjectUtils.removeProjectUser(reader, "readerid", PROJECT_ID);

		assertNull(SecurityProjectUtils.getUserProjectPermission("readerid", PROJECT_ID));
		assertTrue(
				SecurityInsightUtils.getUserInsightPermissions(List.of("readerid"), PROJECT_ID, INSIGHT_ID).isEmpty());
	}

	@Test
	void aReaderCannotTakeAwaySomeoneElsesAccess() {
		IllegalAccessException e = assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.removeProjectUser(reader, "editorid", PROJECT_ID));

		assertEquals("Insufficient privileges to modify this project's permissions.", e.getMessage());
	}

	@Test
	void removingManyPeopleKeepsTheirInsightPermissions() throws Exception {
		UnitTestSecurityAuthUtils.createInsight(PROJECT_ID, INSIGHT_ID, "saved insight", "grid");
		SecurityInsightUtils.addInsightUser(owner, "readerid", PROJECT_ID, INSIGHT_ID, "READ_ONLY", null);

		SecurityProjectUtils.removeProjectUsers(owner, List.of("readerid"), PROJECT_ID);

		assertNull(SecurityProjectUtils.getUserProjectPermission("readerid", PROJECT_ID));
		assertEquals(3, SecurityInsightUtils.getUserInsightPermissions(List.of("readerid"), PROJECT_ID, INSIGHT_ID)
				.get("readerid"));
	}

	///
	/// model usage limits
	///

	@Test
	void theEngineMethodsStoreLimitsOfZeroAsNoLimitDefined() throws Exception {
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "saved engine", IEngine.CATALOG_TYPE.MODEL, admin);

		SecurityEngineUtils.addEngineUser(admin, "strangerid", ENGINE_ID, "READ_ONLY", null, tokensADay(0));
		SecurityEngineUtils.addEngineUserPermissions(admin, ENGINE_ID,
				List.of(Map.of("userid", "readerid", "permission", "READ_ONLY", "usageLimit",
						"{\"rules\": [{\"metric\": \"TOKENS\", \"amount\": 0, \"window\": \"DAY\", "
								+ "\"counts\": \"EACH_PERSON\"}]}")));

		assertNull(usageLimit("strangerid"));
		assertNull(usageLimit("readerid"));
	}

	@Test
	void changingEveryoneOnAnEngineKeepsTheirLimits() throws Exception {
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "saved engine", IEngine.CATALOG_TYPE.MODEL, admin);
		SecurityEngineUtils.addEngineUser(admin, "strangerid", ENGINE_ID, "READ_ONLY", null, tokensADay(500));

		SecurityAdminUtils.getInstance(admin).updateEngineUserPermissions(ENGINE_ID, "EDIT", admin, null);

		assertEquals(2,
				SecurityUserEngineUtils.getUserEnginePermissions(List.of("strangerid"), ENGINE_ID).get("strangerid"));
		assertEquals(500L, usageLimit("strangerid").getRules().get(0).getAmount());
	}

	@Test
	void approvingARequestKeepsTheLimit() throws Exception {
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "saved engine", IEngine.CATALOG_TYPE.MODEL, admin);
		SecurityEngineUtils.addEngineUser(admin, "strangerid", ENGINE_ID, "READ_ONLY", null, tokensADay(500));

		SecurityEngineUtils.approveEngineUserAccessRequests(admin, ENGINE_ID,
				List.of(Map.of("requestid", "request", "userid", "strangerid", "type", "NATIVE", "permission", "EDIT")),
				null);

		assertEquals(2,
				SecurityUserEngineUtils.getUserEnginePermissions(List.of("strangerid"), ENGINE_ID).get("strangerid"));
		assertEquals(500L, usageLimit("strangerid").getRules().get(0).getAmount());
	}

	@Test
	void anEditorCannotChangeALimit() throws Exception {
		UnitTestSecurityAuthUtils.createEngine(ENGINE_ID, "saved engine", IEngine.CATALOG_TYPE.MODEL, admin);
		User editor = UnitTestSecurityAuthUtils.createUser("modeleditor", false);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "modeleditorid", ENGINE_ID, "EDIT");
		SecurityEngineUtils.addEngineUser(admin, "editorid", ENGINE_ID, "EDIT", null, tokensADay(500));
		SecurityEngineUtils.addEngineUser(admin, "readerid", ENGINE_ID, "READ_ONLY", null, tokensADay(500));

		IllegalAccessException refused = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils
				.editEngineUserPermission(editor, "editorid", "NATIVE", ENGINE_ID, "EDIT", null, tokensADay(900)));
		assertEquals("Only owners can set usage limits", refused.getMessage());
		// a Read-Only person's limit too
		assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils.editEngineUserPermission(editor,
				"readerid", "NATIVE", ENGINE_ID, "READ_ONLY", null, tokensADay(900)));
		assertEquals(500L, usageLimit("editorid").getRules().get(0).getAmount());
		assertEquals(500L, usageLimit("readerid").getRules().get(0).getAmount());
		// the limit sent back as it was read, or a change of level only, leaves it as
		// it is
		SecurityEngineUtils.editEngineUserPermission(editor, "editorid", "NATIVE", ENGINE_ID, "READ_ONLY", null,
				tokensADay(500));
		SecurityEngineUtils.editEngineUserPermission(editor, "readerid", "NATIVE", ENGINE_ID, "EDIT", null);

		assertEquals(500L, usageLimit("editorid").getRules().get(0).getAmount());
		assertEquals(500L, usageLimit("readerid").getRules().get(0).getAmount());
	}

	///
	/// the methods that change everyone or everything
	///

	@Test
	void grantingEveryEngineOfATypeGivesOnlyThatType() throws Exception {
		UnitTestSecurityAuthUtils.createEngine("database", "a database", IEngine.CATALOG_TYPE.DATABASE, admin);
		UnitTestSecurityAuthUtils.createEngine("model", "a model", IEngine.CATALOG_TYPE.MODEL, admin);

		SecurityAdminUtils.getInstance(admin).grantAllEngines("strangerid", "READ_ONLY", true, List.of("MODEL"), admin);

		assertEquals(3,
				SecurityUserEngineUtils.getUserEnginePermissions(List.of("strangerid"), "model").get("strangerid"));
		assertTrue(SecurityUserEngineUtils.getUserEnginePermissions(List.of("strangerid"), "database").isEmpty());
	}

	@Test
	void manyChangesAreSavedInOneSave() throws Exception {
		List<PermissionChange> changes = List.of(
				PermissionChange.add(PermissionSubject.ofUser("strangerid", null),
						new PermissionGrant("READ_ONLY", null)),
				PermissionChange.change(PermissionSubject.ofUser("readerid", null), new PermissionGrant("EDIT", null)));

		PermissionBatchUtils.saveChangesNoCheckedException(admin, PermissionTargetResource.project(PROJECT_ID), changes,
				PermissionBatchOptions.adminMode(), new PermissionErrorRules());

		assertEquals(3, SecurityProjectUtils.getUserProjectPermission("strangerid", PROJECT_ID));
		assertEquals(2, SecurityProjectUtils.getUserProjectPermission("readerid", PROJECT_ID));
	}

	///
	/// helpers
	///

	private void insertRowWithNoLevel(String table, String idColumn, String id, String userId) throws Exception {
		try (Connection conn = securityDb.getConnection();
				PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO " + table + " (USERID, " + idColumn + ", PERMISSION) VALUES (?,?,NULL)")) {
			ps.setString(1, userId);
			ps.setString(2, id);
			ps.execute();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		}
	}

	private static ModelUsageLimit usageLimit(String userId) {
		return SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct(ENGINE_ID, userId, null));
	}

	private static ModelUsageLimit tokensADay(long amount) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(ModelUsageLimitRule.Metric.TOKENS, amount,
				ModelUsageLimitRule.Window.DAY, ModelUsageLimitRule.Counts.EACH_PERSON)));
	}
}
