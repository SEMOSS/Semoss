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
		assertEquals(AccessPermissionEnum.EDIT, SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
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
			IllegalAccessException refusal = assertThrows(IllegalAccessException.class,
					() -> SecurityEngineUtils.setEngineGranteePermission(notAnOwner, APP_ID, APP, ENGINE_ID, "EDIT", null));
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
		IllegalAccessException missingApp = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(editor, "no-such-app", APP, ENGINE_ID, "EDIT", null));
		IllegalAccessException missingEngine = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(editor, APP_ID, APP, "no-such-engine", "EDIT", null));

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
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, "everything", null));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(owner, APP_ID, APP, ENGINE_ID, null, null));
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), ENGINE_ID));
	}

	@Test
	void anAppThatDoesNotExistIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(owner, "no-such-app", APP, ENGINE_ID, "EDIT", null));

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

		ModelUsageLimit saved = SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, APP));
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

		assertEquals(AccessPermissionEnum.EDIT, SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), MODEL_ID));
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
		ModelUsageLimit twoRulesForTheSameThing = ModelUsageLimit.of(List.of(
				new ModelUsageLimitRule(Metric.TOKENS, 1_000L, Window.DAY, Counts.SHARED),
				new ModelUsageLimitRule(Metric.TOKENS, 2_000L, Window.DAY, Counts.SHARED)));
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP),
						new PermissionGrant("READ_ONLY", null).withUsageLimit(twoRulesForTheSameThing))));

		assertEquals(PermissionProblem.ErrorCode.INVALID_USAGE_LIMIT, problemOf(owner, batch).getErrorCode());
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(EngineGrantee.ofApp(APP_ID), MODEL_ID));
	}

	@Test
	void anAppIsGivenAPermissionOnlyOnAnEngine() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.project(OTHER_APP_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP), new PermissionGrant("EDIT", null))));

		assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, problemOf(stranger, batch).getErrorCode());
	}

	@Test
	void anAppIsInABatchOnce() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP), new PermissionGrant("EDIT", null)),
						PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP),
								new PermissionGrant("READ_ONLY", null))));

		assertEquals(PermissionProblem.ErrorCode.DUPLICATE, problemOf(owner, batch).getErrorCode());
	}

	@Test
	void anEndDateThatCannotBeReadIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUtils.setEngineGranteePermission(owner,
				APP_ID, APP, ENGINE_ID, "EDIT", "the end of the month"));
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
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(ENGINE_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(APP_ID, APP), new PermissionGrant("EDIT", null))));
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
	void aProjectsPermissionsHaveNoApps() throws Exception {
		Map<String, Object> permissions = SecurityAccessPermissionBatchUtils.getPermissions(stranger,
				PermissionTargetResource.project(APP_ID));

		assertFalse(permissions.containsKey("engines"));
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
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUtils.setEngineGranteePermission(owner, VECTOR_ID, "MODEL", MODEL_ID, "READ_ONLY",
						null));

		assertEquals(SecurityEngineUtils.GRANTEE_DOES_NOT_EXIST, refusal.getMessage());
		assertNull(SecurityEngine2EngineUtils.getGranteePermission(new EngineGrantee(VECTOR_ID, "MODEL"), MODEL_ID));
	}

	@Test
	void aGranteeWithNoCatalogTypeIsRefused() {
		for (String type : new String[] { null, "", "WIDGET" }) {
			PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID), List.of(
					PermissionChange.add(PermissionSubject.ofEngine(VECTOR_ID, type), new PermissionGrant("EDIT", null))));

			assertEquals(PermissionProblem.ErrorCode.NOT_WELL_FORMED, problemOf(owner, batch).getErrorCode());
		}
	}

	@Test
	void anEngineIsNeverGivenAPermissionOnItself() {
		PermissionBatch batch = new PermissionBatch(PermissionTargetResource.engine(MODEL_ID),
				List.of(PermissionChange.add(PermissionSubject.ofEngine(MODEL_ID, "MODEL"),
						new PermissionGrant("READ_ONLY", null))));

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
		assertFalse(app.containsKey("subtype"));
		assertEquals(VECTOR, vectors.get("type"));
		assertEquals(VECTOR_ID, vectors.get("id"));
		assertEquals("orders vectors", vectors.get("name"));
		assertTrue(vectors.containsKey("subtype"));
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

			assertFalse(securityDb.getQueryUtil().tableExists(conn, "PROJECTENGINEPERMISSION",
					securityDb.getDatabase(), securityDb.getSchema()));
		} finally {
			if (securityDb.isConnectionPooling()) {
				conn.close();
			}
		}
		assertEquals(0, QueryExecutionUtility.queryList(securityDb,
				"SELECT ID FROM MODELUSAGELIMIT WHERE LIMITTYPE = 'PROJECT'", ps -> {
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
