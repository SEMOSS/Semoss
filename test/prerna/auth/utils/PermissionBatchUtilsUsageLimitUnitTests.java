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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionSubject.SubjectCategory;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.usage.ModelUsageCost;
import prerna.engine.impl.model.usage.ModelUsagePathView;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Runs permission batches that carry model usage limits against a disposable
 * security database: how a grant's limit is read and saved, who may set each
 * limit, what is refused, and the reads that return them. The model's prices,
 * the Microsoft directory and the path view are replaced with static mocks.
 */
class PermissionBatchUtilsUsageLimitUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String CUSTOM = SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE;
	private static final String NATIVE = AuthProvider.NATIVE.getLabel();

	private static final String MODEL_ID = "limitedmodel";
	private static final String OTHER_MODEL_ID = "othermodel";
	private static final String DATABASE_ID = "limiteddatabase";
	private static final String PROJECT_ID = "limitedproject";
	private static final String GROUP_ID = "team";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();
	private MockedStatic<MicrosoftGraphUserLookup> directory;
	private MockedStatic<ModelUsageCost> prices;
	private MockedStatic<ModelUsagePathView> pathView;

	private User admin;
	private User owner;
	private User editor;
	private User reader;
	private User stranger;
	private AdminSecurityGroupUtils groups;

	@BeforeEach
	void setup() throws Exception {
		directory = mockStatic(MicrosoftGraphUserLookup.class);
		prices = mockStatic(ModelUsageCost.class);
		prices.when(() -> ModelUsageCost.hasKnownPrices(anyString())).thenReturn(false);
		pathView = mockStatic(ModelUsagePathView.class);
		pathView.when(() -> ModelUsagePathView.toMap(any(ModelUsageLimitPath.class), eq(true))).thenAnswer(call -> Map
				.of("billedLimitType", String.valueOf(call.<ModelUsageLimitPath>getArgument(0).getBilledLimitType())));
		pathView.when(() -> ModelUsagePathView.toMap(any(ModelUsageLimitPath.class), eq(true), anyBoolean()))
				.thenAnswer(call -> Map.of("billedLimitType",
						String.valueOf(call.<ModelUsageLimitPath>getArgument(0).getBilledLimitType()),
						"hidesOtherModels", call.<Boolean>getArgument(2)));
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));

		admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		UnitTestSecurityAuthUtils.createUser("other", false);
		reader = UnitTestSecurityAuthUtils.createUser("reader", false);
		stranger = UnitTestSecurityAuthUtils.createUser("stranger", false);
		UnitTestSecurityAuthUtils.createUser("member", false);
		groups = AdminSecurityGroupUtils.getInstance(admin);

		UnitTestSecurityAuthUtils.createEngine(MODEL_ID, "limited model", IEngine.CATALOG_TYPE.MODEL, admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "ownerid", MODEL_ID, "OWNER");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "editorid", MODEL_ID, "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "otherid", MODEL_ID, "EDIT");
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "readerid", MODEL_ID, "READ_ONLY");
		UnitTestSecurityAuthUtils.createEngine(DATABASE_ID, "limited database", IEngine.CATALOG_TYPE.DATABASE, admin);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(admin, "ownerid", DATABASE_ID, "OWNER");

		groups.addGroup(admin, GROUP_ID, CUSTOM, "the team");
		UnitTestGroupTables.insertMember(securityDb, GROUP_ID, "memberid", NATIVE);
	}

	@AfterEach
	void cleanup() throws Exception {
		pathView.close();
		prices.close();
		directory.close();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// reading batches
	///

	@Test
	void readBatches_readsAUsageLimitAndTheModelsOwnSubjects() {
		List<PermissionBatch> batches = SecurityAccessPermissionBatchUtils
				.readBatches("[{\"target\":{\"resourceType\":\"ENGINE\",\"id\":\"m1\"},\"changes\":["
						+ "{\"action\":\"ADD\",\"subject\":{\"category\":\"USER\",\"id\":\"ada\"},"
						+ "\"to\":{\"level\":\"READ_ONLY\",\"usageLimit\":{\"unlimited\":false,\"rules\":[{\"metric\":"
						+ "\"TOKENS\",\"amount\":100000,\"window\":\"MINUTE\",\"counts\":\"EACH_PERSON\"}]}}},"
						+ "{\"action\":\"CHANGE\",\"subject\":{\"category\":\"PUBLIC\"},\"to\":{\"usageLimit\":null}}]}]");

		PermissionGrant grant = batches.get(0).getChanges().get(0).getTo();
		ModelUsageLimitRule rule = grant.getUsageLimit().getRules().get(0);
		assertEquals(Metric.TOKENS, rule.getMetric());
		assertEquals(100_000L, rule.getAmount());
		assertEquals(Window.MINUTE, rule.getWindow());
		assertEquals(SubjectCategory.PUBLIC, batches.get(0).getChanges().get(1).getSubject().getCategory());
	}

	@Test
	void engineGrant_readsAUsageLimitKeyAsAMapOrJson() {
		Map<String, Object> asMap = Map.of("permission", "READ_ONLY", "usageLimit", Map.of("unlimited", true));
		Map<String, Object> asJson = Map.of("permission", "READ_ONLY", "usageLimit",
				"{\"rules\":[{\"metric\":\"COMPUTE_TIME\",\"amount\":5000.0,\"window\":\"DAY\",\"counts\":\"EACH_PERSON\"}]}");

		assertTrue(PermissionBatchUtils.engineGrant(asMap).getUsageLimit().isUnlimited());
		assertTrue(PermissionBatchUtils.engineGrant(asMap).isUsageLimitGiven());
		assertEquals(5000L, PermissionBatchUtils.engineGrant(asJson).getUsageLimit().getRules().get(0).getAmount());
		assertThrows(IllegalArgumentException.class,
				() -> PermissionBatchUtils.engineGrant(Map.of("permission", "READ_ONLY", "usageLimit", "not json")));
	}

	@Test
	void engineGrant_aRowGivesItsLimitOnlyWithAUsageLimitKey() {
		Map<String, Object> nullLimit = new HashMap<>();
		nullLimit.put("permission", "READ_ONLY");
		nullLimit.put("usageLimit", null);

		PermissionGrant noKey = PermissionBatchUtils.engineGrant(Map.of("permission", "READ_ONLY"));
		assertFalse(noKey.isUsageLimitGiven());
		assertNull(noKey.getUsageLimit());
		PermissionGrant cleared = PermissionBatchUtils.engineGrant(nullLimit);
		assertTrue(cleared.isUsageLimitGiven());
		assertNull(cleared.getUsageLimit());
	}

	@Test
	void engineGrant_readsATokenAmountOverWhatAnIntHolds() {
		// a row read from JSON with Gson's defaults gives a double
		PermissionGrant grant = PermissionBatchUtils.engineGrant(Map.of("permission", "READ_ONLY", "usageLimit", Map.of(
				"rules",
				List.of(Map.of("metric", "TOKENS", "amount", 3.0E9, "window", "DAY", "counts", "EACH_PERSON")))));

		assertSameLimit(tokensADay(3_000_000_000L), grant.getUsageLimit());
	}

	///
	/// saving a grant's limit
	///

	@Test
	void saveBatches_marksTheLimitsChangedOnceTheSaveIsDone() throws Exception {
		long before = SecurityModelUsageLimitUtils.version();

		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(addWith("strangerid", "READ_ONLY", tokensADay(5000)))));

		assertTrue(SecurityModelUsageLimitUtils.version() > before);
	}

	@Test
	void setPlatformDefault_marksTheLimitsChangedOnceTheSaveIsDone() throws Exception {
		long before = SecurityModelUsageLimitUtils.version();

		SecurityModelUsageLimitUtils.setPlatformDefault(SecurityAdminUtils.getInstance(admin), admin, tokensADay(10));

		assertTrue(SecurityModelUsageLimitUtils.version() > before);
		assertSameLimit(tokensADay(10), SecurityModelUsageLimitUtils.getPlatformDefault());
	}

	@Test
	void removeExpiredEngineUser_removesTheLimitAndMarksTheLimitsChanged() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(5000)))));
		long before = SecurityModelUsageLimitUtils.version();

		SecurityEngineUtils.removeExpiredEngineUser("readerid", MODEL_ID);

		assertNull(direct("readerid"));
		assertTrue(SecurityModelUsageLimitUtils.version() > before);
	}

	@Test
	void saveBatches_savesAPersonsLimitAndRemovingTheGrantRemovesIt() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(addWith("strangerid", "READ_ONLY", tokensADay(5000)))));

		assertSameLimit(tokensADay(5000), direct("strangerid"));

		SecurityAccessPermissionBatchUtils.saveBatches(owner, List.of(modelBatch(remove("strangerid"))));

		assertNull(direct("strangerid"));
	}

	@Test
	void saveBatches_keepsAPersonsLimitWithTheirLoginTypeAndRemovingTheGrantRemovesItOnEveryLoginType()
			throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(addWith("strangerid", "READ_ONLY", tokensADay(5000)))));

		assertTrue(SecurityModelUsageLimitUtils.getEngineLimits(MODEL_ID)
				.containsKey(ModelUsageLimitKey.direct(MODEL_ID, "strangerid", NATIVE)));

		// a limit kept for the same user id on another login, which the grant covers
		// too
		QueryExecutionUtility.write(securityDb, conn -> {
			SecurityModelUsageLimitUtils.setLimit(conn, ModelUsageLimitKey.direct(MODEL_ID, "strangerid", "MICROSOFT"),
					tokensADay(100), null);
			return null;
		});
		SecurityAccessPermissionBatchUtils.saveBatches(owner, List.of(modelBatch(remove("strangerid"))));

		assertTrue(SecurityModelUsageLimitUtils.getEngineLimits(MODEL_ID).keySet().stream()
				.noneMatch(limitKey -> "strangerid".equals(limitKey.subjectId())));
	}

	@Test
	void saveBatches_aGrantThatLeavesItsLimitOutHasNoLimitDefined() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(5000)))));

		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(PermissionChange.change(user("readerid"), new PermissionGrant("EDIT", null)))));

		assertNull(direct("readerid"));
	}

	@Test
	void saveBatches_rulesOfZeroAreNoLimitDefined() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(addWith("strangerid", "READ_ONLY", tokensADay(0)))));

		assertNull(direct("strangerid"));
	}

	@Test
	void saveBatches_savesATeamLimitAndKeepsTheDateTheTeamWasFirstGivenAccess() throws Exception {
		ModelUsageLimit teamLimit = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 900_000L, Window.DAY, Counts.SHARED),
						new ModelUsageLimitRule(Metric.TOKENS, 360_000L, Window.DAY, Counts.EACH_PERSON)));
		SecurityGroupEngineUtils.addEngineGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID, "READ_ONLY", null,
				teamLimit);
		assertSameLimit(teamLimit, team(GROUP_ID));
		Timestamp firstGiven = Timestamp.valueOf(LocalDateTime.of(2026, 1, 15, 9, 0));
		execute("UPDATE GROUPENGINEPERMISSION SET DATEADDED=? WHERE ID=? AND ENGINEID=?", firstGiven, GROUP_ID,
				MODEL_ID);

		SecurityGroupEngineUtils.editDatabaseGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID, "EDIT", null);

		assertEquals(2, SecurityGroupEngineUtils.getGroupDatabasePermission(GROUP_ID, CUSTOM, MODEL_ID));
		Object dateAdded = UnitTestGroupTables.queryValue(securityDb,
				"SELECT DATEADDED FROM GROUPENGINEPERMISSION WHERE ID=? AND ENGINEID=?", GROUP_ID, MODEL_ID);
		assertEquals(firstGiven.toLocalDateTime(),
				dateAdded instanceof Timestamp timestamp ? timestamp.toLocalDateTime() : dateAdded);
		// the team methods without a limit keep it
		assertSameLimit(teamLimit, team(GROUP_ID));

		SecurityGroupEngineUtils.removeDatabaseGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID);

		assertNull(team(GROUP_ID));
	}

	@Test
	void saveBatches_setsThePublicUseLimitAndTheTotal() throws Exception {
		ModelUsageLimit publicLimit = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1_000_000L, Window.DAY, Counts.SHARED),
						new ModelUsageLimitRule(Metric.TOKENS, 20_000L, Window.DAY, Counts.EACH_PERSON)));
		ModelUsageLimit total = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 5_000_000L, Window.MONTH, Counts.SHARED)));

		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(publicUse(publicLimit), modelTotal(total))));

		assertSameLimit(publicLimit, SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.publicUse(MODEL_ID)));
		assertSameLimit(total, SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.modelTotal(MODEL_ID)));

		SecurityAccessPermissionBatchUtils.saveBatches(owner, List.of(modelBatch(publicUse(null))));

		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.publicUse(MODEL_ID)));
	}

	///
	/// who may set a limit
	///

	@Test
	void validateBatches_ownersSetAnyonesLimitTheirOwnIncluded() {
		PermissionBatch batch = modelBatch(changeWith("editorid", "EDIT", tokensADay(10)),
				changeWith("ownerid", "OWNER", ModelUsageLimit.unlimited()),
				changeWith("readerid", "EDIT", tokensADay(10)));

		for (Map<String, Object> change : changes(validate(owner, batch))) {
			assertAllowed(change);
		}
	}

	@Test
	void validateBatches_editorsSetNoLimitsNotEvenOnReadOnlyPeople() {
		PermissionBatch batch = modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(10)),
				addWith("strangerid", "READ_ONLY", tokensADay(10)), changeWith("otherid", "EDIT", tokensADay(10)),
				changeWith("ownerid", "OWNER", tokensADay(10)));

		List<Map<String, Object>> changes = changes(validate(editor, batch));

		assertBlocked(changes.get(0), "Only owners can set usage limits");
		assertBlocked(changes.get(1), "Only owners can set usage limits");
		assertBlocked(changes.get(2), "Only owners can set usage limits");
		assertBlocked(changes.get(3), "Only owners can change an Author's access");
	}

	@Test
	void validateBatches_editorsStillAddPeopleAndChangeLevelsWithNoLimit() {
		PermissionBatch batch = modelBatch(changeWith("readerid", "EDIT", null),
				addWith("strangerid", "READ_ONLY", null), changeWith("otherid", "READ_ONLY", null));

		for (Map<String, Object> change : changes(validate(editor, batch))) {
			assertAllowed(change);
		}
	}

	@Test
	void validateBatches_anEditorWhoMakesSomeoneAnEditorLeavesTheirLimit() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(10)))));

		List<Map<String, Object>> changes = changes(validate(editor, modelBatch(
				changeWith("readerid", "EDIT", tokensADay(99)), addWith("memberid", "EDIT", tokensADay(5)))));
		assertBlocked(changes.get(0), "Only owners can set usage limits");
		assertBlocked(changes.get(1), "Only owners can set usage limits");
		// leaving the limit out is a change to it too
		assertBlocked(changes(validate(editor, modelBatch(changeWith("readerid", "EDIT", null)))).get(0),
				"Only owners can set usage limits");

		// the same limit is no change to it, and is kept
		assertAllowed(changes(validate(editor, modelBatch(changeWith("readerid", "EDIT", tokensADay(10))))).get(0));
		SecurityAccessPermissionBatchUtils.saveBatches(editor,
				List.of(modelBatch(changeWith("readerid", "EDIT", tokensADay(10)))));
		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(10), direct("readerid"));
	}

	@Test
	void validateBatches_anEditorDoesNotChangeAnotherEditorsLimitByMakingThemReadOnly() {
		assertBlocked(changes(validate(editor, modelBatch(changeWith("otherid", "READ_ONLY", tokensADay(10))))).get(0),
				"Only owners can set usage limits");
		assertAllowed(changes(validate(editor,
				modelBatch(PermissionChange.change(user("otherid"), new PermissionGrant("READ_ONLY", null))))).get(0));
	}

	@Test
	void validateBatches_onlyOwnersTakeAwayTheAccessOfSomeoneWithALimit() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(10)))));

		// the limit goes with the grant, and adding them back would give none
		assertBlocked(changes(validate(editor, modelBatch(remove("readerid")))).get(0),
				"Only owners can end or shorten the access of someone with a usage limit");
		// someone with no limit defined is still the editor's to take away
		assertAllowed(changes(validate(editor, modelBatch(remove("otherid")))).get(0));
		assertAllowed(changes(validate(owner, modelBatch(remove("readerid")))).get(0));
	}

	@Test
	void validateBatches_onlyOwnersGiveSomeoneWithALimitAnEarlierEndDate() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(10)))));
		PermissionChange ending = PermissionChange.change(user("readerid"),
				new PermissionGrant("READ_ONLY", "2099-01-01T00:00:00Z").withUsageLimit(tokensADay(10)));

		assertBlocked(changes(validate(editor, modelBatch(ending))).get(0),
				"Only owners can end or shorten the access of someone with a usage limit");
		assertAllowed(changes(validate(owner, modelBatch(ending))).get(0));
		// a level change that leaves the end date as it is, none, still goes through
		assertAllowed(changes(validate(editor, modelBatch(changeWith("readerid", "EDIT", tokensADay(10))))).get(0));
	}

	@Test
	void removeEngineUser_someoneWithALimitCannotLeaveToLoseIt() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(10)))));

		IllegalAccessException refused = assertThrows(IllegalAccessException.class,
				() -> SecurityEngineUtils.removeEngineUser(reader, "readerid", MODEL_ID));
		assertEquals("Only owners can end or shorten the access of someone with a usage limit", refused.getMessage());
		assertSameLimit(tokensADay(10), direct("readerid"));
	}

	@Test
	void validateBatches_someoneWhoIsNotAnOwnerCannotChangeTheirOwnLimit() {
		assertBlocked(changes(validate(editor, modelBatch(changeWith("editorid", "EDIT", tokensADay(10))))).get(0),
				"Only this engine's owners can change their own access");
	}

	@Test
	void validateBatches_adminModeSetsAnyLimit() {
		PermissionBatch batch = modelBatch(changeWith("ownerid", "OWNER", tokensADay(10)), publicUse(tokensADay(10)));

		for (Map<String, Object> change : changes(SecurityAccessPermissionBatchUtils
				.validateBatches(SecurityAdminUtils.getInstance(admin), admin, List.of(batch)).get(0))) {
			assertAllowed(change);
		}
	}

	@Test
	void validateBatches_publicUseAndTheTotalAreForOwners() {
		String reason = "Only this engine's owners can change its public use limit and its total";
		PermissionBatch batch = modelBatch(publicUse(sharedTokens(10)), modelTotal(sharedTokens(10)));

		for (Map<String, Object> change : changes(validate(editor, batch))) {
			assertBlocked(change, reason);
		}
		for (Map<String, Object> change : changes(validate(stranger, batch))) {
			assertBlocked(change, reason);
		}
		assertAllowed(changes(validate(owner, batch)).get(0));
	}

	@Test
	void validateBatches_onlyOwnersSetTeamLimits() throws Exception {
		SecurityGroupEngineUtils.addEngineGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID, "READ_ONLY", null, null);
		PermissionChange teamChange = new PermissionChange(PermissionChange.Action.CHANGE,
				new PermissionSubject(SubjectCategory.TEAM, GROUP_ID, CUSTOM),
				new PermissionGrant("READ_ONLY", null).withUsageLimit(sharedTokens(10)));

		assertBlocked(changes(validate(editor, modelBatch(teamChange))).get(0),
				"Only this engine's owners can change which teams can use it");
		assertAllowed(changes(validate(owner, modelBatch(teamChange))).get(0));
	}

	///
	/// what is refused
	///

	@Test
	void validateBatches_refusesLimitsThatCannotBeDefinedOnTheGrant() {
		PermissionBatch batch = modelBatch(changeWith("readerid", "READ_ONLY", costADay(2_000_000)),
				changeWith("otherid", "EDIT", sharedTokens(10)), changeWith("editorid", "EDIT", ModelUsageLimit
						.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 10L, Window.DAY, 2, Counts.EACH_PERSON)))));

		List<Map<String, Object>> changes = changes(validate(owner, batch));

		assertBlocked(changes.get(0), "This model has no known prices, so its limits cannot count cost");
		assertBlocked(changes.get(1), "Only a team, public use and a model's total can have shared rules");
		assertBlocked(changes.get(2), "Only minute and hour windows can be more than one minute or hour long");
	}

	@Test
	void validateBatches_costRulesNeedAModelWithKnownPrices() {
		prices.when(() -> ModelUsageCost.hasKnownPrices(MODEL_ID)).thenReturn(true);

		assertAllowed(
				changes(validate(owner, modelBatch(changeWith("readerid", "READ_ONLY", costADay(2_000_000))))).get(0));
	}

	@Test
	void validateBatches_onlyAModelCanHaveLimits() {
		PermissionBatch database = new PermissionBatch(PermissionTargetResource.engine(DATABASE_ID),
				List.of(addWith("strangerid", "READ_ONLY", tokensADay(10)), publicUse(sharedTokens(10))));
		PermissionBatch project = new PermissionBatch(PermissionTargetResource.project(PROJECT_ID),
				List.of(publicUse(sharedTokens(10))));
		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "limited project", owner);

		List<Map<String, Object>> changes = changes(validate(owner, database));
		assertBlocked(changes.get(0), "Only a model's grants can have usage limits");
		assertBlocked(changes.get(1), "Only a model has a public use limit and a total");
		assertBlocked(changes(validate(owner, project)).get(0), "Only a model has a public use limit and a total");
		// no limit at all is fine anywhere
		assertAllowed(changes(validate(owner, new PermissionBatch(PermissionTargetResource.engine(DATABASE_ID),
				List.of(addWith("strangerid", "READ_ONLY", null))))).get(0));
	}

	@Test
	void validateBatches_publicUseAndTheTotalOnlyChangeTheirLimit() {
		PermissionChange addPublic = new PermissionChange(PermissionChange.Action.ADD,
				new PermissionSubject(SubjectCategory.PUBLIC, null, null),
				new PermissionGrant(null, null).withUsageLimit(sharedTokens(10)));
		PermissionChange withLevel = new PermissionChange(PermissionChange.Action.CHANGE,
				new PermissionSubject(SubjectCategory.MODEL, null, null),
				new PermissionGrant("READ_ONLY", null).withUsageLimit(sharedTokens(10)));
		PermissionChange eachPersonTotal = modelTotal(tokensADay(10));

		List<Map<String, Object>> changes = changes(validate(owner, modelBatch(addPublic, withLevel)));
		assertBlocked(changes.get(0), "Public use can only be changed, not added or removed");
		assertBlocked(changes.get(1),
				"The change for the model's total sets only its usage limit, with no access level or end date");
		assertBlocked(changes(validate(owner, modelBatch(eachPersonTotal))).get(0),
				"A model's total has shared rules only");
		assertBlocked(
				changes(validate(owner, modelBatch(publicUse(sharedTokens(1)), publicUse(sharedTokens(2))))).get(1),
				"public use is in the batch more than once");
	}

	///
	/// the engine user methods
	///

	@Test
	void anEditWithAUsageLimitSetsItAndANullOneRemovesIt() throws Exception {
		ModelUsageLimit twoRules = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 100_000L, Window.MINUTE, Counts.EACH_PERSON),
						new ModelUsageLimitRule(Metric.TOKENS, 10_000_000L, Window.DAY, Counts.EACH_PERSON)));
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", twoRules))));

		SecurityEngineUtils.editEngineUserPermission(owner, "readerid", NATIVE, MODEL_ID, "EDIT", null,
				tokensADay(500));
		assertSameLimit(tokensADay(500), direct("readerid"));

		SecurityEngineUtils.editEngineUserPermission(owner, "readerid", NATIVE, MODEL_ID, "EDIT", null,
				(ModelUsageLimit) null);
		assertNull(direct("readerid"));
	}

	@Test
	void aLevelOnlyEditKeepsTheLimit() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(500)))));

		// what the endpoints call when a form has no usageLimit
		SecurityEngineUtils.editEngineUserPermission(editor, "readerid", NATIVE, MODEL_ID, "EDIT", null);
		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));

		SecurityAdminUtils.getInstance(admin).editEngineUserPermission("readerid", MODEL_ID, "READ_ONLY", admin, null);
		assertEquals(3, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));
	}

	@Test
	void aBulkRowWithoutAUsageLimitKeepsTheLimitAndANullOneClearsIt() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(500)),
						changeWith("otherid", "EDIT", tokensADay(700)))));
		Map<String, Object> nullLimit = new HashMap<>();
		nullLimit.put("userid", "otherid");
		nullLimit.put("permission", "EDIT");
		nullLimit.put("usageLimit", null);

		SecurityEngineUtils.editEngineUserPermissions(owner, MODEL_ID,
				List.of(Map.of("userid", "readerid", "permission", "EDIT"), nullLimit));

		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));
		assertNull(direct("otherid"));

		// in admin mode too, where a row with a limit sets it
		SecurityAdminUtils.getInstance(admin).editEngineUserPermissions(MODEL_ID,
				List.of(Map.of("userid", "readerid", "permission", "READ_ONLY"),
						Map.of("userid", "otherid", "permission", "EDIT", "usageLimit",
								"{\"rules\":[{\"metric\":\"TOKENS\",\"amount\":900,\"window\":\"DAY\","
										+ "\"counts\":\"EACH_PERSON\"}]}")),
				admin);

		assertEquals(3, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));
		assertSameLimit(tokensADay(900), direct("otherid"));
	}

	@Test
	void theEngineUserMethodsAreForOwnersWhenTheLimitChanges() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(500)))));

		IllegalAccessException refused = assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils
				.editEngineUserPermission(editor, "readerid", NATIVE, MODEL_ID, "READ_ONLY", null, tokensADay(900)));
		assertEquals("Only owners can set usage limits", refused.getMessage());
		assertThrows(IllegalAccessException.class, () -> SecurityEngineUtils.addEngineUser(editor, "strangerid",
				MODEL_ID, "READ_ONLY", null, tokensADay(900)));
		assertSameLimit(tokensADay(500), direct("readerid"));

		// the same limit is no change to it
		SecurityEngineUtils.editEngineUserPermission(editor, "readerid", NATIVE, MODEL_ID, "EDIT", null,
				tokensADay(500));
		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));
		// and an add with none gives none
		SecurityEngineUtils.addEngineUser(editor, "strangerid", MODEL_ID, "READ_ONLY", null, null);
		assertNull(direct("strangerid"));
	}

	@Test
	void aTokenAmountOverWhatAnIntHoldsIsKept() throws Exception {
		SecurityEngineUtils.editEngineUserPermission(owner, "readerid", NATIVE, MODEL_ID, "READ_ONLY", null,
				tokensADay(3_000_000_000L));

		assertSameLimit(tokensADay(3_000_000_000L), direct("readerid"));
		Map<String, Object> access = map(rowFor(list(SecurityAccessPermissionBatchUtils
				.getPermissions(owner, PermissionTargetResource.engine(MODEL_ID)).get("people")), "readerid")
				.get("access"));
		assertSameLimit(tokensADay(3_000_000_000L), (ModelUsageLimit) access.get("usageLimit"));
	}

	///
	/// propagating a project's access to the model it depends on
	///

	@Test
	void propagateProjectPermission_aChangeKeepsTheLimitUnlessOneIsGiven() throws Exception {
		dependOnTheModel();
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(500)))));

		Map<String, Object> noLimit = SecurityProjectUtils.propagateProjectPermission(owner, PROJECT_ID, "readerid",
				NATIVE, "EDIT", null);

		assertTrue(strings(noLimit.get("accessGranted")).contains(MODEL_ID));
		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));
	}

	@Test
	void propagateProjectPermission_aGivenLimitReplacesTheCurrentOne() throws Exception {
		dependOnTheModel();
		ModelUsageLimit twoRules = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 100_000L, Window.MINUTE, Counts.EACH_PERSON),
						new ModelUsageLimitRule(Metric.TOKENS, 10_000_000L, Window.DAY, Counts.EACH_PERSON)));
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", twoRules))));

		SecurityProjectUtils.propagateProjectPermission(owner, PROJECT_ID, "readerid", NATIVE, "EDIT", null,
				tokensADay(500));

		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertSameLimit(tokensADay(500), direct("readerid"));
	}

	@Test
	void propagateProjectPermission_aRefusedEngineDoesNotStopTheOthers() throws Exception {
		dependOnTheModel();
		execute("INSERT INTO PROJECTDEPENDENCIES (PROJECTID, ENGINEID, ENGINETYPE) VALUES (?,?,?)", PROJECT_ID,
				DATABASE_ID, IEngine.CATALOG_TYPE.DATABASE.name());

		// a shared rule cannot be a person's limit, so the model refuses the change
		Map<String, Object> result = SecurityProjectUtils.propagateProjectPermission(owner, PROJECT_ID, "readerid",
				NATIVE, "EDIT", null, sharedTokens(10));

		assertTrue(strings(result.get("couldNotAddRequest")).contains(MODEL_ID));
		assertTrue(strings(result.get("accessGranted")).contains(DATABASE_ID));
		assertEquals(3, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertEquals(2, SecurityEngineUtils.getUserEnginePermission("readerid", DATABASE_ID));
	}

	@Test
	void propagateProjectPermission_anEditorWhoGivesALimitIsRefusedOnTheModel() throws Exception {
		dependOnTheModel();

		Map<String, Object> result = SecurityProjectUtils.propagateProjectPermission(editor, PROJECT_ID, "readerid",
				NATIVE, "EDIT", null, tokensADay(900));

		assertTrue(strings(result.get("couldNotAddRequest")).contains(MODEL_ID));
		assertEquals(3, SecurityEngineUtils.getUserEnginePermission("readerid", MODEL_ID));
		assertNull(direct("readerid"));
	}

	///
	/// reads
	///

	@Test
	void getPermissions_readsTheLimitsOnAModel() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(5000)), publicUse(sharedTokens(9)),
						modelTotal(sharedTokens(99)))));
		SecurityGroupEngineUtils.addEngineGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID, "READ_ONLY", null,
				sharedTokens(50));

		Map<String, Object> asEditor = SecurityAccessPermissionBatchUtils.getPermissions(editor,
				PermissionTargetResource.engine(MODEL_ID));

		Map<String, Object> readerAccess = map(rowFor(list(asEditor.get("people")), "readerid").get("access"));
		assertSameLimit(tokensADay(5000), (ModelUsageLimit) readerAccess.get("usageLimit"));
		assertSameLimit(sharedTokens(50),
				(ModelUsageLimit) map(list(asEditor.get("teams")).get(0).get("access")).get("usageLimit"));
		assertEquals(false, map(asEditor.get("publicUse")).get("isPublic"));
		assertSameLimit(sharedTokens(9), (ModelUsageLimit) map(asEditor.get("publicUse")).get("usageLimit"));
		assertSameLimit(sharedTokens(99), (ModelUsageLimit) map(asEditor.get("modelTotal")).get("usageLimit"));
		assertEquals(false, asEditor.get("costLimitsAvailable"));
		assertNull(asEditor.get("price"));
		Map<String, Object> editorAllowed = map(asEditor.get("allowedPermissionChanges"));
		assertEquals(List.of(), editorAllowed.get("limitLevels"));
		assertEquals(false, editorAllowed.get("canChangeModelLimits"));

		Map<String, Object> ownerAllowed = map(SecurityAccessPermissionBatchUtils
				.getPermissions(owner, PermissionTargetResource.engine(MODEL_ID)).get("allowedPermissionChanges"));
		assertEquals(List.of("OWNER", "EDIT", "READ_ONLY"), ownerAllowed.get("limitLevels"));
		assertEquals(true, ownerAllowed.get("canChangeModelLimits"));

		// an engine that is not a model has no limits to read
		Map<String, Object> database = SecurityAccessPermissionBatchUtils.getPermissions(owner,
				PermissionTargetResource.engine(DATABASE_ID));
		assertFalse(database.containsKey("publicUse"));
		assertFalse(map(database.get("allowedPermissionChanges")).containsKey("limitLevels"));
	}

	@Test
	void getPermissionsToCopy_carriesEachGrantsLimit() throws Exception {
		UnitTestSecurityAuthUtils.createEngine(OTHER_MODEL_ID, "other model", IEngine.CATALOG_TYPE.MODEL, owner);
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(5000)))));

		List<Map<String, Object>> rows = SecurityAccessPermissionBatchUtils.getPermissionsToCopy(owner,
				PermissionTargetResource.engine(MODEL_ID), PermissionTargetResource.engine(OTHER_MODEL_ID));

		assertSameLimit(tokensADay(5000),
				(ModelUsageLimit) map(rowFor(rows, "readerid").get("source")).get("usageLimit"));
	}

	@Test
	void getUsageLimitPath_isForTheModelsEditorsAndOwners() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(5000)))));

		Map<String, Object> asEditor = SecurityAccessPermissionBatchUtils.getUsageLimitPath(editor, MODEL_ID,
				"readerid", NATIVE);
		assertEquals("DIRECT", asEditor.get("billedLimitType"));
		// the limits on other models are hidden from anyone but the person and admins
		assertEquals(true, asEditor.get("hidesOtherModels"));
		IllegalAccessException notEditor = assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getUsageLimitPath(reader, MODEL_ID, "readerid", NATIVE));
		IllegalAccessException missing = assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getUsageLimitPath(reader, "missingmodel", "readerid", NATIVE));
		assertEquals(notEditor.getMessage(), missing.getMessage());
		// someone who cannot use the model is not looked up
		assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.getUsageLimitPath(editor, MODEL_ID, "strangerid", NATIVE));
		Map<String, Object> asAdmin = SecurityAccessPermissionBatchUtils
				.getUsageLimitPath(SecurityAdminUtils.getInstance(admin), admin, MODEL_ID, "strangerid", NATIVE);
		assertEquals("null", asAdmin.get("billedLimitType"));
		assertFalse(asAdmin.containsKey("hidesOtherModels"));
	}

	@Test
	void getUsageLimitPath_needsAGrantOfTheirOwnOrATeamsNotJustAPublicModel() throws Exception {
		SecurityEngineUtils.setEngineGlobal(owner, MODEL_ID, true);

		assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.getUsageLimitPath(editor, MODEL_ID, "strangerid", NATIVE));

		SecurityGroupEngineUtils.addEngineGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID, "READ_ONLY", null, null);
		assertEquals("null", SecurityAccessPermissionBatchUtils.getUsageLimitPath(editor, MODEL_ID, "memberid", NATIVE)
				.get("billedLimitType"));
	}

	@Test
	void getUsageLimitPath_needsTheLoginTypeInAnyCase() throws Exception {
		SecurityGroupEngineUtils.addEngineGroupPermission(owner, GROUP_ID, CUSTOM, MODEL_ID, "READ_ONLY", null, null);

		assertThrows(IllegalArgumentException.class,
				() -> SecurityAccessPermissionBatchUtils.getUsageLimitPath(editor, MODEL_ID, "readerid", " "));
		assertThrows(IllegalArgumentException.class, () -> SecurityAccessPermissionBatchUtils
				.getUsageLimitPath(SecurityAdminUtils.getInstance(admin), admin, MODEL_ID, "readerid", null));
		// the team membership is stored with the type's label, which a lower case type
		// finds
		assertEquals("null", SecurityAccessPermissionBatchUtils
				.getUsageLimitPath(editor, MODEL_ID, "memberid", NATIVE.toLowerCase()).get("billedLimitType"));
	}

	@Test
	void getMyUsageLimitPath_isForAnyoneWhoCanUseTheModel() throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				List.of(modelBatch(changeWith("readerid", "READ_ONLY", tokensADay(5000)))));

		assertEquals("DIRECT",
				SecurityAccessPermissionBatchUtils.getMyUsageLimitPath(reader, MODEL_ID).get("billedLimitType"));
		IllegalAccessException noAccess = assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getMyUsageLimitPath(stranger, MODEL_ID));
		IllegalAccessException missing = assertThrows(IllegalAccessException.class,
				() -> SecurityAccessPermissionBatchUtils.getMyUsageLimitPath(stranger, "missingmodel"));
		assertEquals(noAccess.getMessage(), missing.getMessage());
	}

	///
	/// helpers
	///

	private static ModelUsageLimit tokensADay(long amount) {
		return ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.EACH_PERSON)));
	}

	private static ModelUsageLimit sharedTokens(long amount) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.SHARED)));
	}

	private static ModelUsageLimit costADay(long micros) {
		return ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.COST, micros, Window.DAY, Counts.EACH_PERSON)));
	}

	private static PermissionSubject user(String id) {
		return new PermissionSubject(SubjectCategory.USER, id, NATIVE);
	}

	private static PermissionChange addWith(String userId, String level, ModelUsageLimit limit) {
		return PermissionChange.add(user(userId), new PermissionGrant(level, null).withUsageLimit(limit));
	}

	private static PermissionChange changeWith(String userId, String level, ModelUsageLimit limit) {
		return PermissionChange.change(user(userId), new PermissionGrant(level, null).withUsageLimit(limit));
	}

	private static PermissionChange remove(String userId) {
		return PermissionChange.remove(user(userId));
	}

	private static PermissionChange publicUse(ModelUsageLimit limit) {
		return PermissionChange.change(new PermissionSubject(SubjectCategory.PUBLIC, null, null),
				new PermissionGrant().withUsageLimit(limit));
	}

	private static PermissionChange modelTotal(ModelUsageLimit limit) {
		return PermissionChange.change(new PermissionSubject(SubjectCategory.MODEL, null, null),
				new PermissionGrant().withUsageLimit(limit));
	}

	private static PermissionBatch modelBatch(PermissionChange... changes) {
		return new PermissionBatch(PermissionTargetResource.engine(MODEL_ID), List.of(changes));
	}

	private static Map<String, Object> validate(User user, PermissionBatch batch) {
		return SecurityAccessPermissionBatchUtils.validateBatches(user, List.of(batch)).get(0);
	}

	private void dependOnTheModel() throws Exception {
		UnitTestSecurityAuthUtils.createProject(PROJECT_ID, "limited project", owner);
		execute("INSERT INTO PROJECTDEPENDENCIES (PROJECTID, ENGINEID, ENGINETYPE) VALUES (?,?,?)", PROJECT_ID,
				MODEL_ID, IEngine.CATALOG_TYPE.MODEL.name());
	}

	private static ModelUsageLimit direct(String userId) {
		return SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct(MODEL_ID, userId, null));
	}

	private static ModelUsageLimit team(String groupId) {
		return SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.team(MODEL_ID, groupId, CUSTOM));
	}

	private void execute(String sql, Object... params) throws Exception {
		try (Connection conn = securityDb.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
			for (int i = 0; i < params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
			ps.execute();
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		}
	}

	private static void assertSameLimit(ModelUsageLimit expected, ModelUsageLimit actual) {
		assertTrue(PermissionBatchUtils.sameLimit(expected, actual), "the limits differ");
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return (Map<String, Object>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> list(Object value) {
		return (List<Map<String, Object>>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<String> strings(Object value) {
		return (List<String>) value;
	}

	private static List<Map<String, Object>> changes(Map<String, Object> result) {
		return list(result.get("changes"));
	}

	private static Map<String, Object> rowFor(List<Map<String, Object>> rows, String userId) {
		return rows.stream().filter(row -> userId.equals(map(row.get("subject")).get("id"))).findFirst().orElseThrow();
	}

	private static void assertAllowed(Map<String, Object> change) {
		assertEquals(true, change.get("allowed"), String.valueOf(change.get("reason")));
	}

	private static void assertBlocked(Map<String, Object> change, String reason) {
		assertEquals(false, change.get("allowed"));
		assertEquals(reason, change.get("reason"));
	}
}
