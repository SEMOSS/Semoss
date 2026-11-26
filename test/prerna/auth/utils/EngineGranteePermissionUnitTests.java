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

import java.sql.Connection;
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
import prerna.auth.access.EngineGrantee;
import prerna.auth.access.GranteePermissionQuery;
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
import prerna.engine.impl.pipeline.PlatformOperation;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * The permission an engine gives an app or another engine, which the app's
 * custom reactors or the other engine's own code use in place of the signed in
 * user's own: who can give it, what it can be, how it is saved and read, and
 * what the engine proxy's check reads back. Each test is one rule of
 * docs/platform_services/engine_access.md.
 */
class EngineGranteePermissionUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String ENGINE_ID = "ordersdb";
	private static final String MODEL_ID = "ordersmodel";
	private static final String OTHER_ENGINE_ID = "otherdb";
	private static final String APP_ID = "ordersapp";
	private static final String OTHER_APP_ID = "otherapp";
	private static final String VECTOR_ID = "ordersvectors";
	private static final String APP = PermissionSubject.PROJECT_TYPE;
	private static final String VECTOR = IEngine.CATALOG_TYPE.VECTOR.name();
	private static final String SKILL_ID = "ordersskill";
	private static final String AGENT_ID = "ordersagent";

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
		UnitTestSecurityAuthUtils.createEngine(VECTOR_ID, "orders vectors", IEngine.CATALOG_TYPE.VECTOR, stranger);
		UnitTestSecurityAuthUtils.createProjectOfType(SKILL_ID, "orders skill", "SKILL", owner);
		UnitTestSecurityAuthUtils.createProjectOfType(AGENT_ID, "orders agent", "WORKSPACE", stranger);
	}

	@AfterEach
	void cleanup() throws Exception {
		prices.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void anOwnerGivesAnAppAPermissionOnTheEngine() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		assertEquals(AccessPermissionEnum.EDIT.getId(),
				SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void thePermissionIsForThatAppAndThatEngineOnly() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(OTHER_APP_ID), ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), OTHER_ENGINE_ID));
	}

	@Test
	void settingAgainChangesTheLevel() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "READ_ONLY", null);

		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertEquals(1, SecurityEngine2EngineUtils.getEngineGrantees(ENGINE_ID).size());
	}

	@Test
	void anOwnerTakesThePermissionAway() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		SecurityEngineUtils.removeEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID);

		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void takingAwayAPermissionThatIsNotThereIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.removeEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID));

		assertEquals(SecurityEngineUtils.GRANTEE_HAS_NO_ACCESS, refusal.getMessage());
	}

	@Test
	void onlyAnOwnerOfTheEngineGivesAnAppAPermission() {
		for (User notAnOwner : List.of(editor, reader, stranger, admin)) {
			IllegalAccessException refusal = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils
					.setEngineGranteePermission(notAnOwner, APP_ID, APP, ENGINE_ID, "EDIT", null));
			assertEquals(SecurityEngineUtils.GRANTEES_NEED_OWNER, refusal.getMessage());
		}
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void onlyAnOwnerOfTheEngineTakesAnAppsPermissionAway() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		for (User notAnOwner : List.of(editor, reader, stranger, admin)) {
			assertThrows(IllegalAccessException.class,
					() -> SecurityEngineUtils.removeEngineGranteePermission(notAnOwner, APP_ID, APP, ENGINE_ID));
		}
		assertNotNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void someoneWhoIsNotAnOwnerLearnsNothingAboutTheApp() {
		IllegalAccessException missingApp = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils
				.setEngineGranteePermission(editor, "no-such-app", APP, ENGINE_ID, "EDIT", null));
		IllegalAccessException missingEngine = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils
				.setEngineGranteePermission(editor, APP_ID, APP, "no-such-engine", "EDIT", null));

		assertEquals(SecurityEngineUtils.GRANTEES_NEED_OWNER, missingApp.getMessage());
		assertEquals(SecurityEngineUtils.GRANTEES_NEED_OWNER, missingEngine.getMessage());
	}

	@Test
	void anAdminGivesAndTakesAwayInAdminMode() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		adminUtils.setEngineGranteePermission(APP_ID, APP, ENGINE_ID, "READ_ONLY", admin, null);
		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertEquals(1, adminUtils.getEngineGranteePermissions(ENGINE_ID).size());

		adminUtils.removeEngineGranteePermission(APP_ID, APP, ENGINE_ID, admin);
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void adminModeRefusesAnEngineThatDoesNotExist() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		assertThrows(IllegalArgumentException.class,
				() -> adminUtils.setEngineGranteePermission(APP_ID, APP, "no-such-engine", "EDIT", admin, null));
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), "no-such-engine"));
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> adminUtils.getEngineGranteePermissions("no-such-engine"));
		assertEquals("Engine no-such-engine does not exist", refusal.getMessage());
	}

	@Test
	void anAppIsNeverAnOwnerOfAnEngine() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "OWNER", null));

		assertTrue(refusal.getMessage().contains("not Author access"), refusal.getMessage());
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void aLevelThatIsNotOneOfTheLevelsIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID,
				APP, ENGINE_ID, "everything", null));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, null, null));
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void anAppThatDoesNotExistIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils
				.setEngineGranteePermission(owner, "no-such-app", APP, ENGINE_ID, "EDIT", null));

		assertEquals(SecurityEngineUtils.GRANTEE_DOES_NOT_EXIST, refusal.getMessage());
	}

	@Test
	void anAppsLimitIsOnlyOnAModel() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP),
						new PermissionGrant("EDIT", null).withUsageLimit(appLimit(1_000, 100)))));

		assertEquals(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, problemOf(owner, batch).getErrorCode());
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void anOwnerGivesAnAppALimitOnAModelWithAnAmountForTheAppAndOneForEachUser() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		ModelUsageLimit saved = SecurityModelUsageLimitUtils
				.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP));
		assertEquals(2, saved.getRules().size());
		assertEquals(Set.of(Counts.SHARED, Counts.EACH_PERSON),
				Set.of(saved.getRules().get(0).getCounts(), saved.getRules().get(1).getCounts()));
		Map<?, ?> project = (Map<?, ?>) ((List<?>) SecurityAccessPermissionBatchUtils
				.getPermissions(owner, PermissionTargetResource.engine(MODEL_ID)).get("engines")).get(0);
		assertEquals(saved.getRules().size(),
				((ModelUsageLimit) ((Map<?, ?>) project.get("access")).get("usageLimit")).getRules().size());
	}

	@Test
	void changingAnAppsLevelKeepsItsLimit() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "EDIT", null);

		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), MODEL_ID));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));
	}

	@Test
	void sendingNoLimitDefinedRemovesAnAppsLimit() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null, null);

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));
		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), MODEL_ID));
	}

	@Test
	void takingAnAppsPermissionAwayRemovesItsLimit() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.removeEngineGranteePermission(owner, APP_ID, APP, MODEL_ID);

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));
	}

	@Test
	void deletingTheAppRemovesItsLimits() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));
		SecurityEngineUtils.setEngineGranteePermission(owner, OTHER_APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(2_000, 200));

		SecurityProjectUtils.deleteProject(stranger, APP_ID);

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, OTHER_APP_ID, APP)));
	}

	@Test
	void anAppsLimitThatCannotBeDefinedIsRefused() {
		ModelUsageLimit twoRulesForTheSameThing = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1_000L, Window.DAY, Counts.SHARED),
						new ModelUsageLimitRule(Metric.TOKENS, 2_000L, Window.DAY, Counts.SHARED)));
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP),
						new PermissionGrant("READ_ONLY", null).withUsageLimit(twoRulesForTheSameThing))));

		assertEquals(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, problemOf(owner, batch).getErrorCode());
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), MODEL_ID));
	}

	@Test
	void anAppIsGivenAPermissionOnAnotherApp() throws Exception {
		SecurityProjectUtils.setProjectGranteePermission(stranger, APP_ID, APP, OTHER_APP_ID, "EDIT", null);

		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), OTHER_APP_ID, true));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(OTHER_APP_ID), APP_ID, true));
	}

	@Test
	void anAppIsInABatchOnce() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID), List.of(
				PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP), new PermissionGrant("EDIT", null)),
				PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP), new PermissionGrant("READ_ONLY", null))));

		assertEquals(PermissionProblem.ErrorCode.DUPLICATE, problemOf(owner, batch).getErrorCode());
	}

	@Test
	void anEndDateThatCannotBeReadIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID,
				APP, ENGINE_ID, "EDIT", "the end of the month"));
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void aPermissionThatHasEndedIsNotInEffect() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT",
				ZonedDateTime.now().minusDays(1).toString());
		SecurityEngineUtils.setEngineGranteePermission(owner, OTHER_APP_ID, APP, ENGINE_ID, "EDIT",
				ZonedDateTime.now().plusDays(1).toString());

		assertNotNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(OTHER_APP_ID), ENGINE_ID));
	}

	@Test
	void aBatchSentAsJsonGivesAnAppAPermission() throws Exception {
		List<PermissionBatch> batches = SecurityAccessPermissionBatchUtils
				.readBatches("[{\"target\":{\"resourceType\":\"ENGINE\",\"id\":\"" + ENGINE_ID + "\"},"
						+ "\"changes\":[{\"action\":\"ADD\",\"subject\":{\"category\":\"ENGINE\",\"id\":\"" + APP_ID
						+ "\",\"type\":\"PROJECT\"},\"to\":{\"level\":\"READ_ONLY\",\"endDate\":null}}]}]");

		SecurityAccessPermissionBatchUtils.saveBatches(owner, batches);

		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void aBatchThatAddsAnAppTwiceInARowIsRefusedTheSecondTime() throws Exception {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID), List
				.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP), new PermissionGrant("EDIT", null))));
		SecurityAccessPermissionBatchUtils.saveBatches(owner, List.of(batch));

		assertEquals(PermissionProblem.ErrorCode.ALREADY_HAS_ACCESS, problemOf(owner, batch).getErrorCode());
	}

	@Test
	void theEnginesPermissionsListItsApps() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(owner,
				PermissionTargetResource.engine(ENGINE_ID));

		List<?> projects = (List<?>) permissions.get("engines");
		assertEquals(1, projects.size());
		Map<?, ?> project = (Map<?, ?>) projects.get(0);
		Map<?, ?> subject = (Map<?, ?>) project.get("subject");
		assertEquals("ENGINE", subject.get("category"));
		assertEquals(APP_ID, subject.get("id"));
		assertEquals("PROJECT", subject.get("type"));
		assertEquals("orders app", subject.get("name"));
		assertEquals("EDIT", ((Map<?, ?>) project.get("access")).get("level"));
		assertEquals(true, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canChangeEngines"));
		assertEquals(true, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canViewEngines"));
	}

	@Test
	void onlyAnOwnerIsToldTheyCanChangeTheApps() throws Exception {
		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(editor,
				PermissionTargetResource.engine(ENGINE_ID));

		assertEquals(false, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canChangeEngines"));
	}

	@Test
	void aProjectThatGivesNoPermissionListsNoApps() throws Exception {
		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(stranger,
				PermissionTargetResource.project(APP_ID));

		assertEquals(List.of(), permissions.get("engines"));
		assertEquals(true, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canChangeEngines"));
	}

	@Test
	void aSkillsOwnerGivesAnAgentAPermissionOnTheSkill() throws Exception {
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(AGENT_ID), SKILL_ID, true));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(AGENT_ID), SKILL_ID, false));
		assertTrue(SecurityEngineAccessUtils.givesAGranteePermission(SKILL_ID, true));
		assertFalse(SecurityEngineAccessUtils.givesAGranteePermission(SKILL_ID, false));
	}

	@Test
	void onlyAProjectsOwnerGivesOrTakesAwayItsPermission() throws Exception {
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(owner, SKILL_ID, "editorid", "EDIT");
		for (User notAnOwner : List.of(editor, reader, stranger, admin)) {
			IllegalAccessException refusal = assertThrows(IllegalAccessException.class, () -> SecurityProjectUtils
					.setProjectGranteePermission(notAnOwner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null));
			assertEquals(SecurityProjectUtils.PROJECT_GRANTEES_NEED_OWNER, refusal.getMessage());
		}
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		assertThrows(IllegalAccessException.class,
				() -> SecurityProjectUtils.removeProjectGranteePermission(editor, AGENT_ID, APP, SKILL_ID));
		SecurityProjectUtils.removeProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID);
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(AGENT_ID), SKILL_ID, true));
	}

	@Test
	void aProjectIsNeverGivenAPermissionOnItselfAndItsGrantsHaveNoLimits() {
		PermissionBatch itself = new PermissionBatch(PermissionTargetResource.project(SKILL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(SKILL_ID, APP),
						new PermissionGrant("READ_ONLY", null))));
		PermissionBatch withALimit = new PermissionBatch(PermissionTargetResource.project(SKILL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(AGENT_ID, APP),
						new PermissionGrant("READ_ONLY", null).withUsageLimit(appLimit(1_000, 100)))));

		PermissionProblem onItself = problemOf(owner, itself);
		assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, onItself.getErrorCode());
		assertEquals("A project cannot be given a permission on itself", onItself.getMessage());
		assertEquals(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, problemOf(owner, withALimit).getErrorCode());
	}

	@Test
	void aProjectGivesAccessToAppsAndAgentsOnlyAndAnEnginesRowCanStillBeRemoved() {
		PermissionBatch engineOnASkill = new PermissionBatch(PermissionTargetResource.project(SKILL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(VECTOR_ID, VECTOR),
						new PermissionGrant("READ_ONLY", null))));
		PermissionBatch removeIt = new PermissionBatch(PermissionTargetResource.project(SKILL_ID),
				List.of(PermissionChange.remove(PermissionSubject.ofEngine(VECTOR_ID, VECTOR))));

		PermissionProblem refused = problemOf(owner, engineOnASkill);
		assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, refused.getErrorCode());
		assertEquals(PermissionBatchUtils.PROJECTS_GIVE_APPS_ONLY, refused.getMessage());
		assertNull(
				SecurityEngine2EngineUtils.getGranteePermission(new EngineGrantee(VECTOR_ID, VECTOR), SKILL_ID, true));
		assertEquals(PermissionProblem.ErrorCode.NO_ACCESS, problemOf(owner, removeIt).getErrorCode());
	}

	@Test
	void anAppOrEngineIsGivenAccessOnlyToAnEngineOrAProject() throws Exception {
		UnitTestSecurityAuthUtils.createInsight(APP_ID, "ordersinsight", "orders insight", "grid");
		UnitTestSecurityAuthUtils.createGroup(admin, "ordersteam", SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE);
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		for (PermissionTargetResource target : List.of(PermissionTargetResource.insight(APP_ID, "ordersinsight"),
				PermissionTargetResource.team("ordersteam"), PermissionTargetResource.teamManagers("ordersteam"))) {
			for (PermissionSubject grantee : List.of(PermissionSubject.ofEngine(AGENT_ID, APP),
					PermissionSubject.ofEngine(VECTOR_ID, VECTOR))) {
				PermissionBatch batch = new PermissionBatch(target,
						List.of(PermissionChange.add(grantee, new PermissionGrant("READ_ONLY", null))));
				PermissionBatchException refusal = assertThrows(PermissionBatchException.class,
						() -> SecurityAccessPermissionBatchUtils.saveBatches(adminUtils, admin, List.of(batch)));

				assertEquals(1, refusal.getProblems().size());
				PermissionProblem problem = refusal.getProblems().get(0);
				assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, problem.getErrorCode());
				assertEquals("An app or engine can be given access only to an engine or a project",
						problem.getMessage());
			}
		}
	}

	@Test
	void anAdminGivesAnAgentAPermissionOnAProjectInAdminMode() {
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(admin);

		adminUtils.setProjectGranteePermission(AGENT_ID, APP, SKILL_ID, "EDIT", admin, null);
		assertEquals(AccessPermissionEnum.EDIT,
				SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(AGENT_ID), SKILL_ID, true));
		assertEquals(1, adminUtils.getProjectGranteePermissions(SKILL_ID).size());

		adminUtils.removeProjectGranteePermission(AGENT_ID, APP, SKILL_ID, admin);
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(AGENT_ID), SKILL_ID, true));
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> adminUtils.getProjectGranteePermissions("no-such-project"));
		assertEquals("Project no-such-project does not exist", refusal.getMessage());
	}

	@Test
	void aProjectsPermissionsListItsAgentsAndTheAgentSeesTheProjectItCanUse() throws Exception {
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		List<?> grantees = (List<?>) SecurityAccessPermissionBatchUtils
				.getPermissions(owner, PermissionTargetResource.project(SKILL_ID)).get("engines");
		List<?> accessToEngines = (List<?>) SecurityAccessPermissionBatchUtils
				.getPermissions(stranger, PermissionTargetResource.project(AGENT_ID)).get("accessToEngines");

		assertEquals(1, grantees.size());
		Map<?, ?> agent = (Map<?, ?>) ((Map<?, ?>) grantees.get(0)).get("subject");
		assertEquals(AGENT_ID, agent.get("id"));
		assertEquals("WORKSPACE", agent.get("subtype"));
		assertEquals(1, accessToEngines.size());
		Map<?, ?> skill = (Map<?, ?>) ((Map<?, ?>) accessToEngines.get(0)).get("target");
		assertEquals("PROJECT", skill.get("resourceType"));
		assertEquals(SKILL_ID, skill.get("id"));
		assertEquals("PROJECT", skill.get("type"));
		assertEquals("SKILL", skill.get("subtype"));
		assertEquals("orders skill", skill.get("name"));
	}

	@Test
	void deletingAProjectRemovesThePermissionsItGave() throws Exception {
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		SecurityProjectUtils.deleteProject(owner, SKILL_ID);

		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(AGENT_ID), SKILL_ID, true));
	}

	@Test
	void theGrantsPageListsThePermissionsOnWhatTheUserCanEditAndOnEverythingInAdminMode() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		Map<String, Object> forTheOwner = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				GranteePermissionQuery.all());
		Map<String, Object> forTheEditor = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(editor,
				GranteePermissionQuery.all());
		Map<String, Object> forTheReader = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(reader,
				GranteePermissionQuery.all());
		Map<String, Object> inAdminMode = SecurityAccessPermissionBatchUtils
				.getAllGranteePermissions(SecurityAdminUtils.getInstance(admin), admin, GranteePermissionQuery.all());

		assertEquals(3, forTheOwner.get("total"));
		assertEquals(1, forTheEditor.get("total"));
		assertEquals(false, ((Map<?, ?>) ((List<?>) forTheEditor.get("grants")).get(0)).get("canChange"));
		assertEquals(0, forTheReader.get("total"));
		assertEquals(3, inAdminMode.get("total"));
		Map<?, ?> modelGrant = ((List<?>) forTheOwner.get("grants")).stream().map(grant -> (Map<?, ?>) grant)
				.filter(grant -> MODEL_ID.equals(((Map<?, ?>) grant.get("target")).get("id"))).findFirst()
				.orElseThrow();
		assertEquals(true, modelGrant.get("canChange"));
		assertNotNull(((Map<?, ?>) modelGrant.get("access")).get("usageLimit"));
	}

	@Test
	void theGrantsPageFiltersByTypeAndTextAndPages() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, AGENT_ID, APP, MODEL_ID, "READ_ONLY", null);
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		Map<String, Object> skills = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery(null, null, "skill", null, null, 0, 0));
		Map<String, Object> agents = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery(null, null, null, null, "WORKSPACE", 0, 0));
		Map<String, Object> searched = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery("ORDERS APP", null, null, null, null, 0, 0));
		Map<String, Object> firstPage = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery(null, null, null, null, null, 2, 0));

		assertEquals(1, skills.get("total"));
		assertEquals(2, agents.get("total"));
		assertEquals(1, searched.get("total"));
		assertEquals(3, firstPage.get("total"));
		assertEquals(2, ((List<?>) firstPage.get("grants")).size());
	}

	@Test
	void theGrantsPageSplitsWhatProjectsAndEnginesUseByTheKindOfEachSide() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null);
		SecurityProjectUtils.setProjectGranteePermission(owner, AGENT_ID, APP, SKILL_ID, "READ_ONLY", null);

		Map<String, Object> projectsUsingProjects = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery(null, "PROJECT", null, "PROJECT", null, 0, 0));
		Map<String, Object> projectsUsingEngines = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery(null, "engine", null, "project", null, 0, 0));
		Map<String, Object> enginesUsingEngines = SecurityAccessPermissionBatchUtils.getAllGranteePermissions(owner,
				new GranteePermissionQuery(null, "ENGINE", null, "ENGINE", null, 0, 0));

		assertEquals(1, projectsUsingProjects.get("total"));
		assertEquals(1, projectsUsingEngines.get("total"));
		assertEquals(1, enginesUsingEngines.get("total"));
		assertEquals(VECTOR_ID,
				((Map<?, ?>) ((Map<?, ?>) ((List<?>) enginesUsingEngines.get("grants")).get(0)).get("subject"))
						.get("id"));
		assertThrows(IllegalArgumentException.class,
				() -> new GranteePermissionQuery(null, "FOLDER", null, null, null, 0, 0));
	}

	@Test
	void onlyTheEnginesEditorsAndOwnersListItsApps() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		assertEquals(1, SecurityEngineUtils.getEngineGranteePermissions(owner, ENGINE_ID).size());
		assertEquals(1, SecurityEngineUtils.getEngineGranteePermissions(editor, ENGINE_ID).size());
		assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineGranteePermissions(reader, ENGINE_ID));
		assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.getEngineGranteePermissions(stranger, ENGINE_ID));
	}

	@Test
	void aReaderSeesNoneOfTheEnginesAppsInItsPermissions() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(reader,
				PermissionTargetResource.engine(ENGINE_ID));

		assertEquals(List.of(), permissions.get("engines"));
		assertEquals(false, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canViewEngines"));
	}

	@Test
	void deletingTheEngineRemovesThePermissionsItGave() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		SecurityEngineUtils.deleteEngine(owner, ENGINE_ID);

		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void deletingTheAppRemovesThePermissionsItWasGiven() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);

		SecurityProjectUtils.deleteProject(stranger, APP_ID);

		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void anOwnerGivesAnotherEngineAPermission() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null);

		EngineGrantee vectors = new EngineGrantee(VECTOR_ID, VECTOR);
		assertEquals(AccessPermissionEnum.READ_ONLY.getId(),
				SecurityEngine2EngineUtils.getGranteePermission(vectors, MODEL_ID));
		assertEquals(AccessPermissionEnum.READ_ONLY, SecurityEngineAccessUtils.getGranteePermission(vectors, MODEL_ID));
		assertTrue(SecurityEngineAccessUtils.givesAGranteePermission(MODEL_ID));
		assertNull(SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(VECTOR_ID), MODEL_ID));
	}

	@Test
	void aPermissionKeepsTheCatalogTypeOfTheEngineGivingIt() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null);

		List<String> types = QueryExecutionUtility.queryList(securityDb,
				"SELECT ENGINETYPE FROM ENGINE2ENGINEPERMISSION WHERE ENGINEID = ?", ps -> ps.setString(1, MODEL_ID),
				rs -> rs.getString(1));
		assertEquals(List.of("MODEL"), types);
	}

	@Test
	void aGranteeTypeIsReadIgnoringCase() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, " vector ", MODEL_ID, "READ_ONLY", null);

		assertEquals(AccessPermissionEnum.READ_ONLY,
				SecurityEngineAccessUtils.getGranteePermission(new EngineGrantee(VECTOR_ID, VECTOR), MODEL_ID));
	}

	@Test
	void anEngineNamedWithAnotherCatalogTypeIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils
				.setEngineGranteePermission(owner, VECTOR_ID, "MODEL", MODEL_ID, "READ_ONLY", null));

		assertEquals(SecurityEngineUtils.GRANTEE_DOES_NOT_EXIST, refusal.getMessage());
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(new EngineGrantee(VECTOR_ID, "MODEL"), MODEL_ID));
	}

	@Test
	void aGranteeWithNoCatalogTypeIsRefused() {
		for (String type : new String[] { null, "", "WIDGET" }) {
			PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID),
					List.of(PermissionChange.add(PermissionSubject.ofEngine(VECTOR_ID, type),
							new PermissionGrant("EDIT", null))));

			assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, problemOf(owner, batch).getErrorCode());
		}
	}

	@Test
	void anEngineIsNeverGivenAPermissionOnItself() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID), List.of(PermissionChange
				.add(PermissionSubject.ofEngine(MODEL_ID, "MODEL"), new PermissionGrant("READ_ONLY", null))));

		PermissionProblem problem = problemOf(owner, batch);
		assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, problem.getErrorCode());
		assertEquals("An engine cannot be given a permission on itself", problem.getMessage());
	}

	@Test
	void anAppAndAnEngineWithTheSameIdAreTwoGrantees() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, ENGINE_ID, "EDIT", null);

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.removeEngineGranteePermission(owner, VECTOR_ID, APP, ENGINE_ID));

		assertEquals(SecurityEngineUtils.GRANTEE_HAS_NO_ACCESS, refusal.getMessage());
		assertNotNull(SecurityEngine2EngineUtils.getGranteePermission(new EngineGrantee(VECTOR_ID, VECTOR), ENGINE_ID));
	}

	@Test
	void anOwnerGivesAnEngineALimitOnAModel() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		ModelUsageLimit saved = SecurityModelUsageLimitUtils
				.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, VECTOR_ID, VECTOR));
		assertEquals(2, saved.getRules().size());
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, VECTOR_ID, APP)));
	}

	@Test
	void theEnginesPermissionsListItsAppsAndEnginesWithTheirTypes() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "EDIT", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null);

		List<?> engines = (List<?>) SecurityAccessPermissionBatchUtils
				.getPermissions(owner, PermissionTargetResource.engine(MODEL_ID)).get("engines");

		assertEquals(2, engines.size());
		Map<?, ?> app = (Map<?, ?>) ((Map<?, ?>) engines.get(0)).get("subject");
		Map<?, ?> vectors = (Map<?, ?>) ((Map<?, ?>) engines.get(1)).get("subject");
		assertEquals(APP, app.get("type"));
		assertEquals("orders app", app.get("name"));
		assertEquals("APP", app.get("subtype"));
		assertEquals(VECTOR, vectors.get("type"));
		assertEquals(VECTOR_ID, vectors.get("id"));
		assertEquals("orders vectors", vectors.get("name"));
		assertTrue(vectors.containsKey("subtype"));
	}

	@Test
	void anAppsPermissionsListTheEnginesThatGiveItAPermission() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(stranger,
				PermissionTargetResource.project(APP_ID));

		List<?> accessToEngines = (List<?>) permissions.get("accessToEngines");
		assertEquals(2, accessToEngines.size());
		Map<?, ?> database = (Map<?, ?>) accessToEngines.get(0);
		Map<?, ?> databaseTarget = (Map<?, ?>) database.get("target");
		assertEquals("ENGINE", databaseTarget.get("resourceType"));
		assertEquals(ENGINE_ID, databaseTarget.get("id"));
		assertEquals("DATABASE", databaseTarget.get("type"));
		assertEquals("orders", databaseTarget.get("name"));
		assertTrue(databaseTarget.containsKey("subtype"));
		assertEquals("EDIT", ((Map<?, ?>) database.get("access")).get("level"));
		assertNull(((Map<?, ?>) database.get("access")).get("usageLimit"));
		Map<?, ?> model = (Map<?, ?>) accessToEngines.get(1);
		assertEquals(MODEL_ID, ((Map<?, ?>) model.get("target")).get("id"));
		assertEquals("MODEL", ((Map<?, ?>) model.get("target")).get("type"));
		assertEquals("READ_ONLY", ((Map<?, ?>) model.get("access")).get("level"));
		assertEquals(2, ((ModelUsageLimit) ((Map<?, ?>) model.get("access")).get("usageLimit")).getRules().size());
		assertEquals(true, ((Map<?, ?>) permissions.get("allowedPermissionChanges")).get("canViewAccessToEngines"));
		assertEquals(List.of(), permissions.get("engines"));
	}

	@Test
	void anEnginesPermissionsListTheEnginesThatGiveItAPermissionApartFromTheOnesItGivesOne() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null);

		Map<String, Object> vectors = SecurityAccessPermissionBatchUtils.getPermissions(stranger,
				PermissionTargetResource.engine(VECTOR_ID));
		Map<String, Object> model = SecurityAccessPermissionBatchUtils.getPermissions(owner,
				PermissionTargetResource.engine(MODEL_ID));

		List<?> accessToEngines = (List<?>) vectors.get("accessToEngines");
		assertEquals(1, accessToEngines.size());
		Map<?, ?> target = (Map<?, ?>) ((Map<?, ?>) accessToEngines.get(0)).get("target");
		assertEquals(MODEL_ID, target.get("id"));
		assertEquals("MODEL", target.get("type"));
		assertEquals(List.of(), vectors.get("engines"));
		assertEquals(1, ((List<?>) model.get("engines")).size());
		assertEquals(List.of(), model.get("accessToEngines"));
	}

	@Test
	void aPermissionGivenToAnEngineIsNotListedForAnAppWithTheSameId() throws Exception {
		try (PlatformOperation operation = PlatformOperation.begin("an app registered from its smss")) {
			SecurityProjectUtils.addProject(VECTOR_ID, "vectors app", "APP", null, false, null);
			SecurityProjectUtils.addProjectOwner(stranger, VECTOR_ID);
		}
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null);

		Map<String, Object> app = SecurityAccessPermissionBatchUtils.getPermissions(stranger,
				PermissionTargetResource.project(VECTOR_ID));

		assertEquals(List.of(), app.get("accessToEngines"));
	}

	@Test
	void onlyTheEditorsAndOwnersOfAnAppOrEngineSeeTheEnginesThatGiveItAPermission() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "EDIT", null);
		SecurityEngineUtils.setEngineGranteePermission(owner, ENGINE_ID, "DATABASE", OTHER_ENGINE_ID, "READ_ONLY",
				null);
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(stranger, APP_ID, "editorid", "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForProject(stranger, APP_ID, "readerid", "READ_ONLY");

		Map<String, Object> appForTheEditor = SecurityAccessPermissionBatchUtils.getPermissions(editor,
				PermissionTargetResource.project(APP_ID));
		Map<String, Object> appForTheReader = SecurityAccessPermissionBatchUtils.getPermissions(reader,
				PermissionTargetResource.project(APP_ID));
		Map<String, Object> appInAdminMode = SecurityAccessPermissionBatchUtils
				.getPermissions(SecurityAdminUtils.getInstance(admin), admin, PermissionTargetResource.project(APP_ID));
		Map<String, Object> engineForTheEditor = SecurityAccessPermissionBatchUtils.getPermissions(editor,
				PermissionTargetResource.engine(ENGINE_ID));
		Map<String, Object> engineForTheReader = SecurityAccessPermissionBatchUtils.getPermissions(reader,
				PermissionTargetResource.engine(ENGINE_ID));

		assertEquals(1, ((List<?>) appForTheEditor.get("accessToEngines")).size());
		assertEquals(1, ((List<?>) appInAdminMode.get("accessToEngines")).size());
		assertEquals(1, ((List<?>) engineForTheEditor.get("accessToEngines")).size());
		assertEquals(List.of(), appForTheReader.get("accessToEngines"));
		assertEquals(List.of(), engineForTheReader.get("accessToEngines"));
		assertEquals(false,
				((Map<?, ?>) appForTheReader.get("allowedPermissionChanges")).get("canViewAccessToEngines"));
		assertEquals(false,
				((Map<?, ?>) engineForTheReader.get("allowedPermissionChanges")).get("canViewAccessToEngines"));
	}

	@Test
	void deletingAnEngineRemovesThePermissionsItWasGivenAndTheirLimits() throws Exception {
		SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, VECTOR, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));

		SecurityEngineUtils.deleteEngine(stranger, VECTOR_ID);

		assertNull(SecurityEngine2EngineUtils.getGranteePermission(new EngineGrantee(VECTOR_ID, VECTOR), MODEL_ID));
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, VECTOR_ID, VECTOR)));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));
	}

	@Test
	void startUpRemovesTheOldAppPermissionTableAndItsLimits() throws Exception {
		Connection conn = securityDb.getConnection();
		QueryExecutionUtility.executeUpdate(securityDb,
				"CREATE TABLE PROJECTENGINEPERMISSION (PROJECTID VARCHAR(255), ENGINEID VARCHAR(255))", ps -> {
				});
		SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, MODEL_ID, "READ_ONLY", null,
				appLimit(1_000, 100));
		QueryExecutionUtility.executeUpdate(securityDb,
				"INSERT INTO MODELUSAGELIMIT (ID, ENGINEID, LIMITTYPE, SUBJECTID) VALUES ('old', ?, 'PROJECT', ?)",
				ps -> {
					ps.setString(1, MODEL_ID);
					ps.setString(2, APP_ID);
				});
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));

		try {
			AbstractSecurityUtils.removeProjectEnginePermissionTable(securityDb, conn, securityDb.getQueryUtil(),
					securityDb.getDatabase(), securityDb.getSchema());

			assertFalse(securityDb.getQueryUtil().tableExists(conn, "PROJECTENGINEPERMISSION", securityDb.getDatabase(),
					securityDb.getSchema()));
		} finally {
			if (securityDb.isConnectionPooling()) {
				conn.close();
			}
		}
		assertEquals(0, QueryExecutionUtility
				.queryList(securityDb, "SELECT ID FROM MODELUSAGELIMIT WHERE LIMITTYPE = 'PROJECT'", ps -> {
				}, rs -> rs.getString(1)).size());
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP)));
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
