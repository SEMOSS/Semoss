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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionBatchException;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.util.SystemEngineRegistry;

/**
 * The permission an engine gives an app, which the app's custom reactors use in
 * place of the signed in user's own: who can give it, what it can be, how it is
 * saved and read, and what the engine proxy's check reads back. Each test is
 * one rule of docs/platform_services/engine_access.md.
 */
class EngineProjectPermissionUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String ENGINE_ID = "ordersdb";
	private static final String MODEL_ID = "ordersmodel";
	private static final String OTHER_ENGINE_ID = "otherdb";
	private static final String APP_ID = "ordersapp";
	private static final String OTHER_APP_ID = "otherapp";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private User admin;
	private User owner;
	private User editor;
	private User reader;
	private User stranger;
	private MockedStatic<ModelUsageCost> prices;

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
		UnitTestSecurityAuthUtils.createEngine(OTHER_ENGINE_ID, "other", owner);
		UnitTestSecurityAuthUtils.createEngine(MODEL_ID, "orders model", IEngine.CATALOG_TYPE.MODEL, owner);
		prices = mockStatic(ModelUsageCost.class);
		prices.when(() -> ModelUsageCost.hasKnownPrices(anyString())).thenReturn(false);
		prices.when(() -> ModelUsageCost.priceEntry(anyString())).thenReturn(null);
		UnitTestSecurityAuthUtils.createProject(APP_ID, "orders app", stranger);
		UnitTestSecurityAuthUtils.createProject(OTHER_APP_ID, "other app", stranger);
	}

	@AfterEach
	void cleanup() throws Exception {
		prices.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void anOwnerGivesAnAppAPermissionOnTheEngine() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		assertEquals(AccessPermissionEnum.EDIT.getId(),
				SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
		assertEquals(AccessPermissionEnum.EDIT, SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
	}

	@Test
	void thePermissionIsForThatAppAndThatEngineOnly() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		assertNull(SecurityEngineAccessUtils.getProjectPermission(OTHER_APP_ID, ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getProjectPermission(APP_ID, OTHER_ENGINE_ID));
	}

	@Test
	void settingAgainChangesTheLevel() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "READ_ONLY", null);

		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
		assertEquals(1, SecurityProjectEngineUtils.getEngineProjects(ENGINE_ID).size());
	}

	@Test
	void anOwnerTakesThePermissionAway() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		SecurityEngineUtils.removeEngineProjectPermission(owner, APP_ID, ENGINE_ID);

		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
	}

	@Test
	void takingAwayAPermissionThatIsNotThereIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.removeEngineProjectPermission(owner, APP_ID, ENGINE_ID));

		assertEquals(SecurityEngineUtils.APP_HAS_NO_ACCESS, refusal.getMessage());
	}

	@Test
	void onlyAnOwnerOfTheEngineGivesAnAppAPermission() {
		for (User notAnOwner : List.of(editor, reader, stranger, admin)) {
			IllegalAccessException refusal = assertThrows(IllegalAccessException.class,
					() -> SecurityEngineUtils.setEngineProjectPermission(notAnOwner, APP_ID, ENGINE_ID, "EDIT", null));
			assertEquals(SecurityEngineUtils.APPS_NEED_OWNER, refusal.getMessage());
		}
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	@Test
	void onlyAnOwnerOfTheEngineTakesAnAppsPermissionAway() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		for (User notAnOwner : List.of(editor, reader, stranger, admin)) {
			assertThrows(IllegalAccessException.class,
					() -> SecurityEngineUtils.removeEngineProjectPermission(notAnOwner, APP_ID, ENGINE_ID));
		}
		assertNotNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	@Test
	void someoneWhoIsNotAnOwnerLearnsNothingAboutTheApp() {
		IllegalAccessException missingApp = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineProjectPermission(editor, "no-such-app", ENGINE_ID, "EDIT", null));
		IllegalAccessException missingEngine = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineProjectPermission(editor, APP_ID, "no-such-engine", "EDIT", null));

		assertEquals(SecurityEngineUtils.APPS_NEED_OWNER, missingApp.getMessage());
		assertEquals(SecurityEngineUtils.APPS_NEED_OWNER, missingEngine.getMessage());
	}

	@Test
	void anAdminGivesAndTakesAwayInAdminMode() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		adminUtils.setEngineProjectPermission(APP_ID, ENGINE_ID, "READ_ONLY", admin, null);
		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
		assertEquals(1, adminUtils.getEngineProjectPermissions(ENGINE_ID).size());

		adminUtils.removeEngineProjectPermission(APP_ID, ENGINE_ID, admin);
		assertNull(SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
	}

	@Test
	void adminModeRefusesAnEngineThatDoesNotExist() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		assertThrows(IllegalArgumentException.class,
				() -> adminUtils.setEngineProjectPermission(APP_ID, "no-such-engine", "EDIT", admin, null));
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, "no-such-engine"));
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> adminUtils.getEngineProjectPermissions("no-such-engine"));
		assertEquals("Engine no-such-engine does not exist", refusal.getMessage());
	}

	@Test
	void anAppIsNeverAnOwnerOfAnEngine() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "OWNER", null));

		assertTrue(refusal.getMessage().contains("not Author access"), refusal.getMessage());
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	@Test
	void aLevelThatIsNotOneOfTheLevelsIsRefused() {
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "everything", null));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, null, null));
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	@Test
	void anAppThatDoesNotExistIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineProjectPermission(owner, "no-such-app", ENGINE_ID, "EDIT", null));

		assertEquals(SecurityEngineUtils.APP_DOES_NOT_EXIST, refusal.getMessage());
	}

	@Test
	void anAppsLimitIsOnlyOnAModel() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(PermissionSubject.ofProject(APP_ID),
						new PermissionGrant("EDIT", null).withUsageLimit(appLimit(1_000, 100)))));

		assertEquals(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, problemOf(owner, batch).getErrorCode());
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	@Test
	void anOwnerGivesAnAppALimitOnAModelWithAnAmountForTheAppAndOneForEachUser() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		ModelUsageLimit saved = SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.app(MODEL_ID, APP_ID));
		assertEquals(2, saved.getRules().size());
		assertEquals(Set.of(Counts.SHARED, Counts.EACH_PERSON),
				Set.of(saved.getRules().get(0).getCounts(), saved.getRules().get(1).getCounts()));
		Map<?, ?> project = (Map<?, ?>) ((List<?>) SecurityAccessPermissionBatchUtils
				.getPermissions(owner, PermissionTargetResource.engine(MODEL_ID)).get("projects")).get(0);
		assertEquals(saved.getRules().size(),
				((ModelUsageLimit) ((Map<?, ?>) project.get("access")).get("usageLimit")).getRules().size());
	}

	@Test
	void changingAnAppsLevelKeepsItsLimit() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "EDIT", null);

		assertEquals(AccessPermissionEnum.EDIT, SecurityEngineAccessUtils.getProjectPermission(APP_ID, MODEL_ID));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.app(MODEL_ID, APP_ID)));
	}

	@Test
	void sendingNoLimitDefinedRemovesAnAppsLimit() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "READ_ONLY", null, null);

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.app(MODEL_ID, APP_ID)));
		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getProjectPermission(APP_ID, MODEL_ID));
	}

	@Test
	void takingAnAppsPermissionAwayRemovesItsLimit() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.removeEngineProjectPermission(owner, APP_ID, MODEL_ID);

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.app(MODEL_ID, APP_ID)));
	}

	@Test
	void deletingTheAppRemovesItsLimits() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));
		SecurityEngineUtils.setEngineProjectPermission(owner, OTHER_APP_ID, MODEL_ID, "READ_ONLY", null,
				appLimit(2_000, 200));

		SecurityProjectUtils.deleteProject(APP_ID);

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.app(MODEL_ID, APP_ID)));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.app(MODEL_ID, OTHER_APP_ID)));
	}

	@Test
	void anAppsLimitThatCannotBeDefinedIsRefused() {
		ModelUsageLimit twoRulesForTheSameThing = ModelUsageLimit.of(List.of(
				new ModelUsageLimitRule(Metric.TOKENS, 1_000L, Window.DAY, Counts.SHARED),
				new ModelUsageLimitRule(Metric.TOKENS, 2_000L, Window.DAY, Counts.SHARED)));
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofProject(APP_ID),
						new PermissionGrant("READ_ONLY", null).withUsageLimit(twoRulesForTheSameThing))));

		assertEquals(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, problemOf(owner, batch).getErrorCode());
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, MODEL_ID));
	}

	@Test
	void anAppIsGivenAPermissionOnlyOnAnEngine() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.project(OTHER_APP_ID),
				List.of(PermissionChange.add(PermissionSubject.ofProject(APP_ID), new PermissionGrant("EDIT", null))));

		assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, problemOf(stranger, batch).getErrorCode());
	}

	@Test
	void anAppIsInABatchOnce() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(PermissionSubject.ofProject(APP_ID), new PermissionGrant("EDIT", null)),
						PermissionChange.add(PermissionSubject.ofProject(APP_ID),
								new PermissionGrant("READ_ONLY", null))));

		assertEquals(PermissionProblem.ErrorCode.DUPLICATE, problemOf(owner, batch).getErrorCode());
	}

	@Test
	void anEndDateThatCannotBeReadIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils.setEngineProjectPermission(owner,
				APP_ID, ENGINE_ID, "EDIT", "the end of the month"));
		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	@Test
	void aPermissionThatHasEndedIsNotInEffect() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT",
				ZonedDateTime.now().minusDays(1).toString());
		SecurityEngineUtils.setEngineProjectPermission(owner, OTHER_APP_ID, ENGINE_ID, "EDIT",
				ZonedDateTime.now().plusDays(1).toString());

		assertNotNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getProjectPermission(OTHER_APP_ID, ENGINE_ID));
	}

	@Test
	void aBatchSentAsJsonGivesAnAppAPermission() throws Exception {
		List<PermissionBatch> batches = SecurityAccessPermissionBatchUtils
				.readBatches("[{\"target\":{\"resourceType\":\"ENGINE\",\"id\":\"" + ENGINE_ID + "\"},"
						+ "\"changes\":[{\"action\":\"ADD\",\"subject\":{\"category\":\"PROJECT\",\"id\":\"" + APP_ID
						+ "\"},\"to\":{\"level\":\"READ_ONLY\",\"endDate\":null}}]}]");

		SecurityAccessPermissionBatchUtils.saveBatches(owner, batches);

		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
	}

	@Test
	void aBatchThatAddsAnAppTwiceInARowIsRefusedTheSecondTime() throws Exception {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(PermissionSubject.ofProject(APP_ID), new PermissionGrant("EDIT", null))));
		SecurityAccessPermissionBatchUtils.saveBatches(owner, List.of(batch));

		assertEquals(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS, problemOf(owner, batch).getErrorCode());
	}

	@Test
	void theEnginesPermissionsListItsApps() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(owner,
				PermissionTargetResource.engine(ENGINE_ID));

		List<?> projects = (List<?>) permissions.get("projects");
		assertEquals(1, projects.size());
		Map<?, ?> project = (Map<?, ?>) projects.get(0);
		Map<?, ?> subject = (Map<?, ?>) project.get("subject");
		assertEquals("PROJECT", subject.get("category"));
		assertEquals(APP_ID, subject.get("id"));
		assertEquals("orders app", subject.get("name"));
		assertEquals("EDIT", ((Map<?, ?>) project.get("access")).get("level"));
		assertEquals(true, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canChangeProjects"));
		assertEquals(true, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canViewProjects"));
	}

	@Test
	void onlyAnOwnerIsToldTheyCanChangeTheApps() throws Exception {
		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(editor,
				PermissionTargetResource.engine(ENGINE_ID));

		assertEquals(false, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canChangeProjects"));
	}

	@Test
	void aProjectsPermissionsHaveNoApps() throws Exception {
		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(stranger,
				PermissionTargetResource.project(APP_ID));

		assertFalse(permissions.containsKey("projects"));
	}

	@Test
	void onlyTheEnginesEditorsAndOwnersListItsApps() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		assertEquals(1, SecurityEngineUtils.getEngineProjectPermissions(owner, ENGINE_ID).size());
		assertEquals(1, SecurityEngineUtils.getEngineProjectPermissions(editor, ENGINE_ID).size());
		assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineProjectPermissions(reader, ENGINE_ID));
		assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineProjectPermissions(stranger, ENGINE_ID));
	}

	@Test
	void aReaderSeesNoneOfTheEnginesAppsInItsPermissions() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(reader,
				PermissionTargetResource.engine(ENGINE_ID));

		assertEquals(List.of(), permissions.get("projects"));
		assertEquals(false, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canViewProjects"));
	}

	@Test
	void deletingTheEngineRemovesThePermissionsItGave() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		SecurityEngineUtils.deleteEngine(ENGINE_ID);

		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getProjectPermission(APP_ID, ENGINE_ID));
	}

	@Test
	void deletingTheAppRemovesThePermissionsItWasGiven() throws Exception {
		SecurityEngineUtils.setEngineProjectPermission(owner, APP_ID, ENGINE_ID, "EDIT", null);

		SecurityProjectUtils.deleteProject(APP_ID);

		assertNull(SecurityProjectEngineUtils.getProjectEnginePermission(APP_ID, ENGINE_ID));
	}

	private static ModelUsageLimit appLimit(long forTheApp, long forEachUser) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, forTheApp, Window.DAY, Counts.SHARED),
				new ModelUsageLimitRule(Metric.TOKENS, forEachUser, Window.DAY, Counts.EACH_PERSON)));
	}

	private static PermissionProblem problemOf(User user, PermissionBatch batch) {
		PermissionBatchException refusal = assertThrows(PermissionBatchException.class,
				() -> SecurityAccessPermissionBatchUtils.saveBatches(user, List.of(batch)));
		assertEquals(1, refusal.getProblems().size());
		return refusal.getProblems().get(0);
	}

}
