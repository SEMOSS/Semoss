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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.Reason;
import prerna.auth.access.ModelUsageLimitPath.State;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class SecurityModelUsageLimitUtilsUnitTests {

	private static final String MODEL = "model-1";
	private static final Pair<String, String> ADMIN = Pair.with("admin", "NATIVE");
	/** The security database tables the limits are read and resolved from */
	private static final Set<String> TABLES = Set.of("MODELUSAGELIMIT", "MODELUSAGELIMITRULE", "ENGINE",
			"ENGINEPERMISSION", "GROUPENGINEPERMISSION", "CUSTOMGROUPASSIGNMENT", "SMSS_USER");

	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:usage-limits-" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		IRDBMSEngine securityDb = mock(IRDBMSEngine.class);
		when(securityDb.getConnection()).thenReturn(connection);
		when(securityDb.getQueryUtil()).thenReturn(queryUtil);
		// the tables as the security database defines them
		AbstractOwlCreator.syncSchema(securityDb, connection, new SecurityOwlCreator(queryUtil).getDBSchema().stream()
				.filter(table -> TABLES.contains(table.getValue0())).toList());
		execute("INSERT INTO ENGINE (ENGINEID, GLOBAL) VALUES ('" + MODEL + "', FALSE)");
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(securityDb);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (registry != null) {
			registry.close();
		}
		if (connection != null) {
			connection.close();
		}
	}

	private void execute(String sql) throws Exception {
		try (Statement statement = connection.createStatement()) {
			statement.execute(sql);
		}
	}

	private void save(ModelUsageLimitKey key, ModelUsageLimit limit) throws Exception {
		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), conn -> {
			SecurityModelUsageLimitUtils.setLimit(conn, key, limit, ADMIN);
			return null;
		});
	}

	private static ModelUsageLimit tokensADay(long amount) {
		return ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.EACH_PERSON)));
	}

	private static ModelUsageLimit sharedCostADay(long micros) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.COST, micros, Window.DAY, Counts.SHARED)));
	}

	/*
	 * Storage
	 */

	@Test
	void setLimit_replacesTheRulesAndNullRemovesTheLimit() throws Exception {
		ModelUsageLimitKey key = ModelUsageLimitKey.direct(MODEL, "jdoe", null);
		save(key, tokensADay(100));
		save(key,
				ModelUsageLimit
						.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1000L, Window.MINUTE, Counts.EACH_PERSON),
								new ModelUsageLimitRule(Metric.TOKENS, 50000L, Window.DAY, Counts.EACH_PERSON))));

		ModelUsageLimit read = SecurityModelUsageLimitUtils.getLimit(key);
		assertEquals(2, read.getRules().size());
		assertEquals(Window.MINUTE, read.getRules().get(0).getWindow());

		save(key, null);
		assertNull(SecurityModelUsageLimitUtils.getLimit(key));
	}

	@Test
	void setLimit_keepsAGrantsLimitWithTheLoginTypeFromSmssUserWhenNotGiven() throws Exception {
		execute("INSERT INTO SMSS_USER (ID, TYPE) VALUES ('jdoe', 'NATIVE')");

		save(ModelUsageLimitKey.direct(MODEL, "jdoe", null), tokensADay(100));

		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTTYPE = 'NATIVE'"));
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct(MODEL, "jdoe", "NATIVE")));
	}

	@Test
	void setLimit_keepsAGrantsLimitWithNoLoginTypeWhenTheUserIdHasSeveralLogins() throws Exception {
		execute("INSERT INTO SMSS_USER (ID, TYPE) VALUES ('jdoe', 'NATIVE')");
		execute("INSERT INTO SMSS_USER (ID, TYPE) VALUES ('jdoe', 'MICROSOFT')");

		save(ModelUsageLimitKey.direct(MODEL, "jdoe", null), tokensADay(100));

		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTTYPE IS NULL"));
		// a limit kept with no login type applies to the user id on any login
		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct(MODEL, "jdoe", "MICROSOFT")));
	}

	@Test
	void setLimit_withALoginTypeReplacesTheGrantsLimitKeptWithNone() throws Exception {
		insertLimitRow("untyped", ModelUsageLimitKey.direct(MODEL, "jdoe", null), "2026-01-01 00:00:00", 100);

		save(ModelUsageLimitKey.direct(MODEL, "jdoe", "NATIVE"), tokensADay(300));

		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT"));
		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTTYPE = 'NATIVE'"));
		assertEquals(300L, SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct(MODEL, "jdoe", "NATIVE"))
				.getRules().get(0).getAmount());
	}

	@Test
	void findDirectLimit_prefersTheLoginTypeThenNoneAndIsUnsureAcrossTwoLogins() {
		ModelUsageLimit typed = tokensADay(100);
		ModelUsageLimit untyped = tokensADay(200);
		Map<ModelUsageLimitKey, ModelUsageLimit> limits = new LinkedHashMap<>();
		limits.put(ModelUsageLimitKey.direct(MODEL, "jdoe", "NATIVE"), typed);
		limits.put(ModelUsageLimitKey.direct(MODEL, "jdoe", null), untyped);
		assertSame(typed, SecurityModelUsageLimitUtils.findDirectLimit(limits, MODEL, "jdoe", "NATIVE"));
		assertSame(untyped, SecurityModelUsageLimitUtils.findDirectLimit(limits, MODEL, "jdoe", "MICROSOFT"));
		assertSame(untyped, SecurityModelUsageLimitUtils.findDirectLimit(limits, MODEL, "jdoe", null));

		Map<ModelUsageLimitKey, ModelUsageLimit> twoLogins = new LinkedHashMap<>();
		twoLogins.put(ModelUsageLimitKey.direct(MODEL, "jdoe", "NATIVE"), typed);
		twoLogins.put(ModelUsageLimitKey.direct(MODEL, "jdoe", "MICROSOFT"), untyped);
		assertSame(typed, SecurityModelUsageLimitUtils.findDirectLimit(twoLogins, MODEL, "jdoe", "NATIVE"));
		assertNull(SecurityModelUsageLimitUtils.findDirectLimit(twoLogins, MODEL, "jdoe", null));
	}

	@Test
	void fillDirectLimitLoginTypes_givesTheLoginTypeWhereTheUserIdHasOneLogin() throws Exception {
		execute("INSERT INTO SMSS_USER (ID, TYPE) VALUES ('jdoe', 'NATIVE')");
		execute("INSERT INTO SMSS_USER (ID, TYPE) VALUES ('shared', 'NATIVE')");
		execute("INSERT INTO SMSS_USER (ID, TYPE) VALUES ('shared', 'MICROSOFT')");
		insertLimitRow("one", ModelUsageLimitKey.direct(MODEL, "jdoe", null), "2026-01-01 00:00:00", 100);
		insertLimitRow("two", ModelUsageLimitKey.direct(MODEL, "shared", null), "2026-01-01 00:00:00", 100);
		insertLimitRow("three", ModelUsageLimitKey.direct(MODEL, "gone", null), "2026-01-01 00:00:00", 100);
		long before = SecurityModelUsageLimitUtils.version();

		SecurityModelUsageLimitUtils.fillDirectLimitLoginTypes(SystemEngineRegistry.getSecurityDb());

		assertEquals(1L,
				count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTID = 'jdoe' AND SUBJECTTYPE = 'NATIVE'"));
		assertEquals(1L,
				count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTID = 'shared' AND SUBJECTTYPE IS NULL"));
		assertEquals(1L,
				count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTID = 'gone' AND SUBJECTTYPE IS NULL"));
		assertTrue(SecurityModelUsageLimitUtils.version() > before);
	}

	@Test
	void deleteDirectLimits_removesTheGrantsLimitOnEveryLoginType() throws Exception {
		insertLimitRow("native", ModelUsageLimitKey.direct(MODEL, "jdoe", "NATIVE"), "2026-01-01 00:00:00", 100);
		insertLimitRow("microsoft", ModelUsageLimitKey.direct(MODEL, "jdoe", "MICROSOFT"), "2026-01-01 00:00:00", 100);
		insertLimitRow("other", ModelUsageLimitKey.direct(MODEL, "other", "NATIVE"), "2026-01-01 00:00:00", 100);

		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), conn -> {
			SecurityModelUsageLimitUtils.deleteDirectLimits(conn, MODEL, "jdoe");
			return null;
		});

		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT"));
		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTID = 'other'"));
	}

	@Test
	void setLimit_replacesEveryRowTheOwnerHasWithOneAndKeepsWhenItWasFirstAdded() throws Exception {
		ModelUsageLimitKey key = ModelUsageLimitKey.direct(MODEL, "jdoe", null);
		// two rows for one key, as two servers starting at the same time could write
		insertLimitRow("first", key, "2026-01-01 00:00:00", 100);
		insertLimitRow("second", key, "2026-02-01 00:00:00", 200);

		save(key, tokensADay(300));

		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTID = 'jdoe'"));
		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMITRULE"));
		assertEquals(300L, SecurityModelUsageLimitUtils.getLimit(key).getRules().get(0).getAmount());
		assertEquals(Timestamp.valueOf("2026-01-01 00:00:00"),
				timestamp("SELECT DATEADDED FROM MODELUSAGELIMIT WHERE SUBJECTID = 'jdoe'"));

		save(key, null);
		assertEquals(0L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT"));
		assertEquals(0L, count("SELECT COUNT(*) FROM MODELUSAGELIMITRULE"));
	}

	@Test
	void getLimit_readsTheRowChangedLastWhenAnOwnerHasTwo() throws Exception {
		ModelUsageLimitKey key = ModelUsageLimitKey.direct(MODEL, "jdoe", null);
		insertLimitRow("later", key, "2026-02-01 00:00:00", 200);
		insertLimitRow("earlier", key, "2026-01-01 00:00:00", 100);

		ModelUsageLimit read = SecurityModelUsageLimitUtils.getLimit(key);

		assertEquals(1, read.getRules().size());
		assertEquals(200L, read.getRules().get(0).getAmount());
	}

	@Test
	void setLimit_leavesMarkingTheLimitsChangedToItsCallerOnceTheyCommit() throws Exception {
		long before = SecurityModelUsageLimitUtils.version();

		save(ModelUsageLimitKey.direct(MODEL, "jdoe", null), tokensADay(100));
		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), conn -> {
			SecurityModelUsageLimitUtils.deleteEngineLimits(conn, MODEL);
			return null;
		});

		assertEquals(before, SecurityModelUsageLimitUtils.version());
		SecurityModelUsageLimitUtils.markChanged();
		assertEquals(before + 1, SecurityModelUsageLimitUtils.version());
	}

	@Test
	void setLimit_keepsAWindowLength() throws Exception {
		ModelUsageLimitKey key = ModelUsageLimitKey.direct(MODEL, "jdoe", null);
		save(key,
				ModelUsageLimit.of(
						List.of(new ModelUsageLimitRule(Metric.TOKENS, 1000L, Window.MINUTE, 10, Counts.EACH_PERSON),
								new ModelUsageLimitRule(Metric.TOKENS, 100L, Window.MINUTE, Counts.EACH_PERSON))));

		List<ModelUsageLimitRule> rules = SecurityModelUsageLimitUtils.getLimit(key).getRules();
		assertEquals(2, rules.size());
		assertEquals(1, rules.get(0).getWindowLength());
		assertNull(rules.get(0).getWindowLengthOrNull());
		assertEquals(10, rules.get(1).getWindowLength());
	}

	@Test
	void setLimit_keepsAnUnlimitedLimitWithNoRules() throws Exception {
		ModelUsageLimitKey key = ModelUsageLimitKey.team(MODEL, "data", "CUSTOM");
		save(key, ModelUsageLimit.unlimited());

		ModelUsageLimit read = SecurityModelUsageLimitUtils.getLimit(key);
		assertTrue(read.isUnlimited());
		assertTrue(read.getRules().isEmpty());
	}

	@Test
	void getLimits_readsOnlyTheAllModelsDefaultsAskedForAndThePlatformDefault() throws Exception {
		save(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), tokensADay(10));
		save(ModelUsageLimitKey.allModels("other", "NATIVE"), tokensADay(20));
		save(ModelUsageLimitKey.platform(), tokensADay(30));

		Map<ModelUsageLimitKey, ModelUsageLimit> read = SecurityModelUsageLimitUtils
				.getLimits(List.of(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), ModelUsageLimitKey.platform()));

		assertEquals(2, read.size());
		assertEquals(10L, read.get(ModelUsageLimitKey.allModels("jdoe", "NATIVE")).getRules().get(0).getAmount());
		assertEquals(30L, read.get(ModelUsageLimitKey.platform()).getRules().get(0).getAmount());
	}

	@Test
	void deleteDirectLimitsWithoutGrant_keepsTheLimitsOfGrantsThatRemain() throws Exception {
		addGrant("jdoe");
		save(ModelUsageLimitKey.direct(MODEL, "jdoe", null), tokensADay(100));
		save(ModelUsageLimitKey.direct("model-2", "jdoe", null), tokensADay(200));

		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), conn -> {
			SecurityModelUsageLimitUtils.deleteDirectLimitsWithoutGrant(conn, "jdoe");
			return null;
		});

		assertNotNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct(MODEL, "jdoe", null)));
		assertNull(SecurityModelUsageLimitUtils.getLimit(ModelUsageLimitKey.direct("model-2", "jdoe", null)));
	}

	/*
	 * Validation
	 */

	@Test
	void normalize_dropsZeroRulesAndALimitLeftWithNoneIsNotDefined() {
		ModelUsageLimit limit = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 0L, Window.DAY, Counts.EACH_PERSON),
						new ModelUsageLimitRule(Metric.TOKENS, 500L, Window.WEEK, Counts.EACH_PERSON)));

		assertEquals(1, SecurityModelUsageLimitUtils.normalize(limit).getRules().size());
		assertNull(SecurityModelUsageLimitUtils.normalize(tokensADay(0)));
	}

	@Test
	void findProblem_sharedRulesOnlyForTeamsPublicUseAndTheModelTotal() {
		ModelUsageLimit shared = sharedCostADay(1_000_000);

		assertNull(
				SecurityModelUsageLimitUtils.findProblem(shared, ModelUsageLimitKey.team(MODEL, "t", "CUSTOM"), true));
		assertNull(SecurityModelUsageLimitUtils.findProblem(shared, ModelUsageLimitKey.publicUse(MODEL), true));
		assertNull(SecurityModelUsageLimitUtils.findProblem(shared, ModelUsageLimitKey.modelTotal(MODEL), true));
		assertNotNull(
				SecurityModelUsageLimitUtils.findProblem(shared, ModelUsageLimitKey.direct(MODEL, "jdoe", null), true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(shared, ModelUsageLimitKey.platform(), true));
	}

	@Test
	void findProblem_ceilingsHaveSharedRulesOnlyAndCannotBeUnlimited() {
		assertNotNull(
				SecurityModelUsageLimitUtils.findProblem(tokensADay(10), ModelUsageLimitKey.modelTotal(MODEL), true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(tokensADay(10),
				ModelUsageLimitKey.teamTotal("t", "CUSTOM"), true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(ModelUsageLimit.unlimited(),
				ModelUsageLimitKey.modelTotal(MODEL), true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(ModelUsageLimit.unlimited(),
				ModelUsageLimitKey.platform(), true));
		assertNull(SecurityModelUsageLimitUtils.findProblem(ModelUsageLimit.unlimited(),
				ModelUsageLimitKey.allModels("jdoe", "NATIVE"), true));
	}

	@Test
	void findProblem_costNeedsKnownPricesOnAModelButNotAcrossAllModels() {
		ModelUsageLimit cost = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.COST, 5_000_000L, Window.DAY, Counts.EACH_PERSON)));

		assertNotNull(
				SecurityModelUsageLimitUtils.findProblem(cost, ModelUsageLimitKey.direct(MODEL, "jdoe", null), false));
		assertNull(
				SecurityModelUsageLimitUtils.findProblem(cost, ModelUsageLimitKey.allModels("jdoe", "NATIVE"), false));
	}

	@Test
	void findProblem_oneRulePerMetricWindowAndCounts() {
		ModelUsageLimit twice = ModelUsageLimit
				.of(List.of(tokensADay(10).getRules().get(0), tokensADay(20).getRules().get(0)));

		assertNotNull(
				SecurityModelUsageLimitUtils.findProblem(twice, ModelUsageLimitKey.direct(MODEL, "jdoe", null), true));
	}

	@Test
	void findProblem_windowLengthsOnlyOnMinuteAndHourWindowsWithinTheirRanges() {
		ModelUsageLimitKey key = ModelUsageLimitKey.direct(MODEL, "jdoe", null);

		assertNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.MINUTE, 10), key, true));
		assertNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.MINUTE, 59), key, true));
		assertNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.HOUR, 24), key, true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.MINUTE, 60), key, true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.MINUTE, 0), key, true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.HOUR, 25), key, true));
		assertNotNull(SecurityModelUsageLimitUtils.findProblem(lengthRule(Window.DAY, 2), key, true));
	}

	@Test
	void findProblem_rulesOfDifferentLengthsAreDifferentWindows() {
		ModelUsageLimit both = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1000L, Window.MINUTE, Counts.EACH_PERSON),
						new ModelUsageLimitRule(Metric.TOKENS, 5000L, Window.MINUTE, 10, Counts.EACH_PERSON)));

		assertNull(
				SecurityModelUsageLimitUtils.findProblem(both, ModelUsageLimitKey.direct(MODEL, "jdoe", null), true));
	}

	private static ModelUsageLimit lengthRule(Window window, int length) {
		return ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 1000L, window, length, Counts.EACH_PERSON)));
	}

	@Test
	void requireValid_refusesAnUnlimitedLimitWithRules() {
		ModelUsageLimit broken = new ModelUsageLimit(true, tokensADay(10).getRules());

		assertThrows(IllegalArgumentException.class, () -> SecurityModelUsageLimitUtils.requireValid(broken,
				ModelUsageLimitKey.direct(MODEL, "jdoe", null), true));
	}

	@Test
	void readJson_readsALimitAndBlankIsNoLimitDefined() {
		ModelUsageLimit limit = SecurityModelUsageLimitUtils.readJson(
				"{\"unlimited\":false,\"rules\":[{\"metric\":\"TOKENS\",\"amount\":100000,\"window\":\"MINUTE\","
						+ "\"counts\":\"EACH_PERSON\"}]}");

		assertEquals(100000L, limit.getRules().get(0).getAmount());
		assertNull(SecurityModelUsageLimitUtils.readJson(" "));
		assertNull(SecurityModelUsageLimitUtils.readJson("null"));
		assertThrows(IllegalArgumentException.class, () -> SecurityModelUsageLimitUtils.readJson("{\"rules\": 5}"));
	}

	/*
	 * Which limit applies
	 */

	@Test
	void resolve_aDirectLimitWinsOverTeams() throws Exception {
		addGrant("jdoe");
		addTeam("data", "2026-01-01 00:00:00", "2026-02-01 00:00:00");
		save(ModelUsageLimitKey.direct(MODEL, "jdoe", null), tokensADay(100));
		save(ModelUsageLimitKey.team(MODEL, "data", "CUSTOM"), sharedCostADay(1_000_000));

		ModelUsageLimitPath path = resolveJdoe();

		assertEquals(StepLimitType.DIRECT, path.getBilledLimitType());
		assertEquals(State.NOT_REACHED, step(path, StepLimitType.TEAM).getState());
	}

	@Test
	void resolve_directAccessWithNoLimitDefinedPassesOnToTheTeams() throws Exception {
		addGrant("jdoe");
		addTeam("data", "2026-01-01 00:00:00", "2026-02-01 00:00:00");
		save(ModelUsageLimitKey.team(MODEL, "data", "CUSTOM"), sharedCostADay(1_000_000));

		ModelUsageLimitPath path = resolveJdoe();

		assertEquals(StepLimitType.TEAM, path.getBilledLimitType());
		assertEquals(State.NOT_DEFINED, step(path, StepLimitType.DIRECT).getState());
	}

	@Test
	void resolve_unlimitedTeamsFirstThenTheTeamThePersonGotAccessThroughFirst() throws Exception {
		// Data: joined in January, given the model in April; Ops: joined in March,
		// given the model in February; Lab: Unlimited, joined last
		addTeam("data", "2026-01-01 00:00:00", "2026-04-01 00:00:00");
		addTeam("ops", "2026-03-01 00:00:00", "2026-02-01 00:00:00");
		addTeam("lab", "2026-09-01 00:00:00", "2026-09-01 00:00:00");
		save(ModelUsageLimitKey.team(MODEL, "data", "CUSTOM"), sharedCostADay(300_000_000));
		save(ModelUsageLimitKey.team(MODEL, "ops", "CUSTOM"), sharedCostADay(100_000_000));

		List<Step> order = resolveJdoe().getBillingOrder();
		assertEquals(List.of("ops", "data"), order.stream().map(step -> step.getKey().subjectId()).toList());

		save(ModelUsageLimitKey.team(MODEL, "lab", "CUSTOM"), ModelUsageLimit.unlimited());
		order = resolveJdoe().getBillingOrder();
		assertEquals(List.of("lab", "ops", "data"), order.stream().map(step -> step.getKey().subjectId()).toList());
	}

	@Test
	void resolve_aTeamWithNoLimitDefinedLeavesItsMembersOnThePublicUseLimit() throws Exception {
		execute("UPDATE ENGINE SET GLOBAL = TRUE");
		addTeam("ops", "2026-01-01 00:00:00", "2026-01-01 00:00:00");
		save(ModelUsageLimitKey.publicUse(MODEL), tokensADay(50));

		ModelUsageLimitPath path = resolveJdoe();

		assertEquals(StepLimitType.PUBLIC, path.getBilledLimitType());
		assertEquals(State.NOT_DEFINED, step(path, StepLimitType.TEAM).getState());
	}

	@Test
	void resolve_publicUseIsSkippedOnAModelThatIsNotPublic() throws Exception {
		save(ModelUsageLimitKey.publicUse(MODEL), tokensADay(50));
		save(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), tokensADay(70));

		ModelUsageLimitPath path = resolveJdoe();

		assertEquals(StepLimitType.ALL_MODELS, path.getBilledLimitType());
		assertEquals(Reason.NOT_PUBLIC, step(path, StepLimitType.PUBLIC).getReason());
	}

	@Test
	void resolve_thePlatformDefaultIsLastAndAnUnlimitedAllModelsDefaultExemptsThePerson() throws Exception {
		save(ModelUsageLimitKey.platform(), tokensADay(30));
		assertEquals(StepLimitType.PLATFORM, resolveJdoe().getBilledLimitType());

		save(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), ModelUsageLimit.unlimited());
		ModelUsageLimitPath path = resolveJdoe();
		assertEquals(StepLimitType.ALL_MODELS, path.getBilledLimitType());
		assertTrue(path.getBillingOrder().get(0).isUnlimited());
		assertEquals(State.NOT_REACHED, step(path, StepLimitType.PLATFORM).getState());
	}

	@Test
	void person_mustListThePrimaryLoginFirst() {
		assertThrows(IllegalArgumentException.class,
				() -> new ModelUsageLimitResolver.Person("jdoe", "NATIVE", List.of(), List.of()));
		assertThrows(IllegalArgumentException.class, () -> new ModelUsageLimitResolver.Person("jdoe", "NATIVE",
				List.of(Pair.with("jdoe-google-id", "GOOGLE"), Pair.with("jdoe", "NATIVE")), List.of()));
		assertThrows(IllegalArgumentException.class,
				() -> new ModelUsageLimitResolver.Person(" ", "NATIVE", List.of(Pair.with(" ", "NATIVE")), null));
		assertTrue(new ModelUsageLimitResolver.Person("jdoe", "NATIVE", List.of(Pair.with("jdoe", "NATIVE")), null)
				.teams().isEmpty());
	}

	@Test
	void resolve_withNoAllModelsDefaultTheStepIsThePrimaryLogins() throws Exception {
		ModelUsageLimitPath path = resolveJdoe();

		Step allModels = step(path, StepLimitType.ALL_MODELS);
		assertEquals(ModelUsageLimitKey.allModels("jdoe", "NATIVE"), allModels.getKey());
		assertEquals(State.NOT_DEFINED, allModels.getState());
		assertNull(path.getBilledLimitType());
	}

	@Test
	void resolve_anotherLoginsAllModelsDefaultAppliesWhenThePrimaryHasNone() throws Exception {
		save(ModelUsageLimitKey.allModels("jdoe-google-id", "GOOGLE"), tokensADay(40));
		ModelUsageLimitResolver.Person person = new ModelUsageLimitResolver.Person("jdoe", "NATIVE",
				List.of(Pair.with("jdoe", "NATIVE"), Pair.with("jdoe-google-id", "GOOGLE")), List.of());

		ModelUsageLimitPath path = ModelUsageLimitResolver.resolve(person, MODEL);

		assertEquals(StepLimitType.ALL_MODELS, path.getBilledLimitType());
		assertEquals(ModelUsageLimitKey.allModels("jdoe-google-id", "GOOGLE"),
				step(path, StepLimitType.ALL_MODELS).getKey());
	}

	@Test
	void resolve_theModelTotalIsACeilingAndAnEndedTeamGrantIsSkipped() throws Exception {
		addTeam("data", "2026-01-01 00:00:00", "2026-01-01 00:00:00");
		execute("UPDATE GROUPENGINEPERMISSION SET ENDDATE = '2020-01-01 00:00:00'");
		save(ModelUsageLimitKey.team(MODEL, "data", "CUSTOM"), sharedCostADay(1));
		save(ModelUsageLimitKey.modelTotal(MODEL), sharedCostADay(2_000_000_000L));

		ModelUsageLimitPath path = resolveJdoe();

		assertNull(path.getBilledLimitType());
		assertEquals(State.CHECKED, path.getCeilings().get(0).getState());
		assertFalse(path.getSteps().stream().anyMatch(s -> s.getLimitType() == StepLimitType.TEAM));
	}

	private void addGrant(String userId) throws Exception {
		execute("INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, ENDDATE) VALUES ('" + userId + "', '"
				+ MODEL + "', 3, NULL)");
	}

	private void addTeam(String teamId, String joined, String granted) throws Exception {
		execute("INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID, USERID, TYPE, DATEADDED, ENDDATE) VALUES ('" + teamId
				+ "', 'jdoe', 'NATIVE', '" + joined + "', NULL)");
		execute("INSERT INTO GROUPENGINEPERMISSION (ID, TYPE, ENGINEID, PERMISSION, DATEADDED, ENDDATE) VALUES ('"
				+ teamId + "', 'CUSTOM', '" + MODEL + "', 3, '" + granted + "', NULL)");
	}

	/**
	 * Writes a limit row and its one rule directly, as only a write that raced
	 * another could leave a second row for a key.
	 */
	private void insertLimitRow(String id, ModelUsageLimitKey key, String changed, long amount) throws Exception {
		execute("INSERT INTO MODELUSAGELIMIT (ID, ENGINEID, LIMITTYPE, SUBJECTID, SUBJECTTYPE, UNLIMITED, DATEADDED, "
				+ "DATEMODIFIED) VALUES ('" + id + "', '" + key.engineId() + "', '" + key.type().name() + "', '"
				+ key.subjectId() + "', " + (key.subjectType() == null ? "NULL" : "'" + key.subjectType() + "'")
				+ ", FALSE, '" + changed + "', '" + changed + "')");
		execute("INSERT INTO MODELUSAGELIMITRULE (LIMITID, METRIC, AMOUNT, LIMITWINDOW, COUNTS) VALUES ('" + id
				+ "', 'TOKENS', " + amount + ", 'DAY', 'EACH_PERSON')");
	}

	private long count(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
			result.next();
			return result.getLong(1);
		}
	}

	private Timestamp timestamp(String sql) throws Exception {
		try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
			result.next();
			return result.getTimestamp(1);
		}
	}

	private ModelUsageLimitPath resolveJdoe() {
		return ModelUsageLimitResolver.resolve(ModelUsageLimitResolver.storedPerson("jdoe", "NATIVE"), MODEL);
	}

	private static Step step(ModelUsageLimitPath path, StepLimitType limitType) {
		return path.getSteps().stream().filter(s -> s.getLimitType() == limitType).findFirst().orElseThrow();
	}
}
