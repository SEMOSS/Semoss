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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.database.usage.JdbcReadUsage;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.usage.LocalUsageCounterStore;
import prerna.engine.impl.usage.UsageLimit;
import prerna.engine.impl.usage.UsageLimitException;
import prerna.engine.impl.usage.UsageLimitKey;
import prerna.engine.impl.usage.UsageLimitKey.Source;
import prerna.engine.impl.usage.UsageLimiter;
import prerna.engine.impl.usage.UsageRule;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.engine.impl.usage.UsageWindows.Window;
import prerna.om.ThreadStore;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class SecurityEngineUsageLimitUtilsUnitTests {
	private Connection connection;
	private IRDBMSEngine db;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<SecurityEngineUtils> engines;
	private User owner;
	private User reader;

	private static User person(String id) {
		User user = new User();
		user.setPrimaryLogin(AuthProvider.NATIVE);
		AccessToken token = new AccessToken();
		token.setId(id);
		token.setProvider(AuthProvider.NATIVE);
		user.setAccessToken(token);
		return user;
	}

	private static UsageLimitKey key(Source source, String engine, String subject, String type) {
		return new UsageLimitKey(IEngine.CATALOG_TYPE.DATABASE, source, engine, subject, type);
	}

	private static UsageLimit rows(long amount, Counts counts) {
		return new UsageLimit(false, List.of(new UsageRule(Metric.ROWS, amount, Window.DAY, null, counts)));
	}

	@BeforeEach
	void setup() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:engine-usage-" + UUID.randomUUID());
		var queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		db = mock(IRDBMSEngine.class);
		when(db.getConnection()).thenReturn(connection);
		when(db.getQueryUtil()).thenReturn(queryUtil);
		var tables = Set.of("ENGINE", "ENGINEPERMISSION", "GROUPENGINEPERMISSION", "CUSTOMGROUPASSIGNMENT", "MODELUSAGELIMIT",
				"MODELUSAGELIMITRULE");
		AbstractOwlCreator.syncSchema(db, connection, new SecurityOwlCreator(queryUtil).getDBSchema().stream()
				.filter(t -> tables.contains(t.getValue0())).toList());
		doAnswer(call -> {
			execute(call.getArgument(0));
			return null;
		}).when(db).insertData(anyString());
		EngineUsageSchema.initialize(db, connection);
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getSecurityDb).thenReturn(db);
		engines = mockStatic(SecurityEngineUtils.class);
		owner = person("owner");
		reader = person("reader");
		engines.when(() -> SecurityEngineUtils.userIsOwner(owner, "db")).thenReturn(true);
		engines.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype("db"))
				.thenReturn(new Object[] { IEngine.CATALOG_TYPE.DATABASE, "H2" });
		execute("INSERT INTO ENGINE (ENGINEID,GLOBAL,ENGINETYPE) VALUES ('db',TRUE,'DATABASE')");
	}

	@AfterEach
	void cleanup() throws Exception {
		if (engines != null)
			engines.close();
		if (registry != null)
			registry.close();
		if (connection != null)
			connection.close();
	}

	private void execute(String sql) throws Exception {
		try (var statement = connection.createStatement()) {
			statement.execute(sql);
		}
	}

	private long count(String sql) throws Exception {
		try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
			result.next();
			return result.getLong(1);
		}
	}

	private void save(UsageLimitKey key, UsageLimit limit) throws Exception {
		QueryExecutionUtility.write(db,conn -> {SecurityEngineUsageLimitUtils.setLimit(conn,key,limit,Pair.with("owner","NATIVE"));return null;});
		SecurityEngineUsageLimitUtils.markChanged();
	}

	@Test
	void ownerCanCreateReplaceAndRemoveWithoutTouchingModelStorage() throws Exception {
		execute("INSERT INTO MODELUSAGELIMIT (ID,ENGINEID,LIMITTYPE,UNLIMITED) VALUES ('sentinel','model','MODEL',FALSE)");
		var key = key(Source.ENGINE_TOTAL, "db", null, null);
		long version = SecurityEngineUsageLimitUtils.version();
		save(key, rows(10, Counts.SHARED));
		save(key, rows(20, Counts.SHARED));
		assertEquals(20, SecurityEngineUsageLimitUtils.getLimit(key).rules().get(0).amount());
		assertEquals(1, count("SELECT COUNT(*) FROM ENGINEUSAGELIMIT"));
		assertTrue(SecurityEngineUsageLimitUtils.version() > version);
		save(key, null);
		assertNull(SecurityEngineUsageLimitUtils.getLimit(key));
		assertEquals(1, count("SELECT COUNT(*) FROM MODELUSAGELIMIT"));
	}

	@Test
	void deniedUsersCannotReadOrChangePolicy() {
		var key = key(Source.ENGINE_TOTAL, "db", null, null);
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUsageLimitUtils.setLimit(reader, key, rows(1, Counts.SHARED)));
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUsageLimitUtils.getEngineLimits(reader, "db"));
	}

	@Test
	void crossDatabaseDefaultsRequireAdminProofAndStayPrivateFromOwners() throws Exception {
		var total = key(Source.USER_TOTAL, null, "reader", "NATIVE");
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUsageLimitUtils.setLimit(owner,total, rows(10, Counts.EACH_PERSON)));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUsageLimitUtils.setAdminLimit(null, owner, total, rows(10, Counts.EACH_PERSON)));
		SecurityEngineUsageLimitUtils.setAdminLimit(mock(SecurityAdminUtils.class), owner, total,
				rows(10, Counts.EACH_PERSON));
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(total));
		assertTrue(SecurityEngineUsageLimitUtils.getEngineLimits(owner, "db").isEmpty());
	}

	@Test
	void schemaCanRunTwiceAndKeepsPolicies() throws Exception {
		var key = key(Source.PUBLIC, "db", null, null);
		save(key, rows(10, Counts.EACH_PERSON));
		EngineUsageSchema.initialize(db, connection);
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(key));
	}

	@Test
	void invalidMetricsCountsAndScopesAreRejected() {
		var direct = key(Source.DIRECT, "db", "reader", "NATIVE");
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUsageLimitUtils.validate(direct, rows(1, Counts.SHARED)));
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUsageLimitUtils
				.validate(key(Source.ENGINE_TOTAL, "db", null, null), rows(1, Counts.EACH_PERSON)));
		assertThrows(IllegalArgumentException.class, () -> SecurityEngineUsageLimitUtils.validate(direct,
				new UsageLimit(false, List.of(new UsageRule(Metric.TOKENS, 1, Window.DAY, null, Counts.EACH_PERSON)))));
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUsageLimitUtils.validate(
						new UsageLimitKey(IEngine.CATALOG_TYPE.MODEL, Source.DIRECT, "db", "reader", "NATIVE"),
						rows(1, Counts.EACH_PERSON)));
		assertThrows(IllegalArgumentException.class, () -> key(Source.DIRECT, "db", "reader", null).validate());
	}

	@Test
	void directPoliciesRequireAGrantAndStopFallbackWhileItExists() throws Exception {
		var direct = key(Source.DIRECT, "db", "reader", "NATIVE");
		save(direct, rows(1, Counts.EACH_PERSON));
		save(key(Source.PUBLIC, "db", null, null), rows(100, Counts.EACH_PERSON));
		execute("INSERT INTO ENGINEPERMISSION (ENGINEID,USERID,PERMISSION) VALUES ('db','reader',3)");
		var plan = DatabaseUsagePolicyResolver.resolve(reader, "db");
		assertEquals(1, plan.candidates().size());
		assertEquals(direct, plan.candidates().get(0).key());
		execute("DELETE FROM ENGINEPERMISSION WHERE USERID='reader'");
		assertEquals(Source.PUBLIC,DatabaseUsagePolicyResolver.resolve(reader,"db").candidates().get(0).key().source());
		// A stored policy cannot apply once its grant has gone.
		assertEquals(1, plan.candidates().get(0).targets().get(0).rule().amount());
	}

	@Test
	void teamCandidatesRequireCurrentMembershipAndGrantAndKeepTeamTotals() throws Exception {
		execute("INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID,USERID,TYPE,DATEADDED) VALUES ('team','reader','NATIVE',TIMESTAMP '2026-01-01 00:00:00')");
		execute("INSERT INTO GROUPENGINEPERMISSION (ID,TYPE,ENGINEID,PERMISSION,DATEADDED) VALUES ('team','CUSTOM','db',3,TIMESTAMP '2026-02-01 00:00:00')");
		var team = key(Source.TEAM, "db", "team", "CUSTOM");
		save(team, rows(10, Counts.SHARED));
		SecurityEngineUsageLimitUtils.setAdminLimit(mock(SecurityAdminUtils.class), owner,
				key(Source.TEAM_TOTAL, null, "team", "CUSTOM"), rows(20, Counts.SHARED));
		var plan = DatabaseUsagePolicyResolver.resolve(reader, "db");
		assertEquals(team, plan.candidates().get(0).key());
		assertEquals(2, plan.candidates().get(0).targets().size());
		execute("UPDATE CUSTOMGROUPASSIGNMENT SET ENDDATE=TIMESTAMP '2000-01-01 00:00:00'");
		assertTrue(DatabaseUsagePolicyResolver.resolve(reader, "db").candidates().isEmpty());
	}

	@Test
	void unlimitedWinsOverPublicUseButNotEngineTotal() throws Exception {
		save(key(Source.DIRECT, "db", "reader", "NATIVE"), new UsageLimit(true, List.of()));
		execute("INSERT INTO ENGINEPERMISSION (ENGINEID,USERID,PERMISSION) VALUES ('db','reader',3)");
		save(key(Source.PUBLIC, "db", null, null), rows(1, Counts.SHARED));
		save(key(Source.ENGINE_TOTAL, "db", null, null), rows(10, Counts.SHARED));
		var plan = DatabaseUsagePolicyResolver.resolve(reader, "db");
		assertTrue(plan.candidates().get(0).targets().isEmpty());
		assertEquals(1, plan.ceilings().size());
	}

	@Test
	void completedCountersRecoverAfterRestartAndCloseOnlyWritesOnce() throws Exception {
		var clock = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC);
		save(key(Source.ENGINE_TOTAL, "db", null, null), rows(1, Counts.SHARED));
		var plan = DatabaseUsagePolicyResolver.resolve(reader, "db");
		var limiter = new UsageLimiter(new LocalUsageCounterStore(clock), clock,
				SecurityEngineUsageLimitUtils.history());
		var ticket = limiter.admit(plan);
		ticket.consume(Metric.ROWS, 1);
		ticket.close();
		ticket.close();
		assertEquals(1, count("SELECT COUNT(*) FROM ENGINEUSAGEHISTORY"));
		var restarted = new UsageLimiter(new LocalUsageCounterStore(clock), clock,
				SecurityEngineUsageLimitUtils.history());
		assertThrows(UsageLimitException.class, () -> restarted.admit(plan));
	}

	@Test
	void platformDefaultsDoNotMeterUnregisteredInternalDatabasesOrOtherCatalogs() throws Exception {
		SecurityEngineUsageLimitUtils.setAdminLimit(mock(SecurityAdminUtils.class), owner,
				key(Source.PLATFORM, null, null, null), rows(1, Counts.EACH_PERSON));
		execute("INSERT INTO ENGINE (ENGINEID,GLOBAL,ENGINETYPE) VALUES ('model',FALSE,'MODEL')");
		assertTrue(DatabaseUsagePolicyResolver.resolve(reader, "internal-frame").candidates().isEmpty());
		assertTrue(DatabaseUsagePolicyResolver.resolve(reader, "model").candidates().isEmpty());
		assertFalse(DatabaseUsagePolicyResolver.resolve(reader, "db").candidates().isEmpty());
	}

	@Test
	void catalogReadsNeverMixPoliciesFromAnotherEngineType() throws Exception {
		save(key(Source.ENGINE_TOTAL, "db", null, null), rows(10, Counts.SHARED));
		assertEquals(1, SecurityEngineUsageLimitUtils.readForEngine(IEngine.CATALOG_TYPE.DATABASE, "db").size());
		assertTrue(SecurityEngineUsageLimitUtils.readForEngine(IEngine.CATALOG_TYPE.STORAGE, "db").isEmpty());
		var person = EngineUsageIdentity.storedPerson("reader", "NATIVE");
		assertTrue(SecurityEngineUsageLimitUtils.readForPerson(IEngine.CATALOG_TYPE.STORAGE, "db", person).isEmpty());
		assertTrue(SecurityEngineUsageLimitUtils.getEngineLimits(owner, IEngine.CATALOG_TYPE.STORAGE, "db").isEmpty());
	}

	@Test
	void subjectReadsAreBatchedAndDoNotLoadOtherPeoplesTotals() throws Exception {
		save(key(Source.ENGINE_TOTAL, "db", null, null), rows(10, Counts.SHARED));
		save(key(Source.TEAM, "db", "team-500", "CUSTOM"), rows(20, Counts.SHARED));
		SecurityEngineUsageLimitUtils.setAdminLimit(mock(SecurityAdminUtils.class), owner,
				key(Source.USER_TOTAL, null, "someone-else", "NATIVE"), rows(30, Counts.EACH_PERSON));
		var teams = new ArrayList<EngineUsageIdentity.TeamMembership>();
		for (int i = 0; i < 501; i++)
			teams.add(new EngineUsageIdentity.TeamMembership("team-" + i, "CUSTOM", null));
		var person = new EngineUsageIdentity.Person("reader", "NATIVE",
				List.of(Pair.with("reader", "NATIVE")), teams);
		var found = SecurityEngineUsageLimitUtils.readForPerson("db", person);
		assertEquals(2, found.size());
		assertTrue(found.stream().noneMatch(entry -> "someone-else".equals(entry.key().subjectId())));
	}

	@Test
	void grantAndMembershipInvalidationReachesNewCatalogCaches() {
		long before = SecurityEngineUsageLimitUtils.version();
		SecurityModelUsageLimitUtils.markChanged();
		assertTrue(SecurityEngineUsageLimitUtils.version() > before);
	}

	@Test
	void deletionCleanupIsScopedToTheExactIdentityAndResource() throws Exception {
		var direct = key(Source.DIRECT, "db", "reader", "NATIVE");
		var otherLogin = key(Source.DIRECT, "db", "reader", "MICROSOFT");
		var team = key(Source.TEAM, "db", "reader", "CUSTOM");
		save(direct, rows(1, Counts.EACH_PERSON));
		save(otherLogin, rows(2, Counts.EACH_PERSON));
		save(team, rows(3, Counts.SHARED));
		QueryExecutionUtility.write(db, conn -> {
			SecurityEngineUsageLimitUtils.deleteUserPolicies(conn, "reader", "NATIVE");
			return null;
		});
		assertNull(SecurityEngineUsageLimitUtils.getLimit(direct));
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(otherLogin));
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(team));
		QueryExecutionUtility.write(db, conn -> {
			SecurityEngineUsageLimitUtils.deleteTeamPolicies(conn, "reader", "CUSTOM");
			return null;
		});
		assertNull(SecurityEngineUsageLimitUtils.getLimit(team));
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(otherLogin));
		QueryExecutionUtility.write(db, conn -> {
			SecurityEngineUsageLimitUtils.deleteEnginePolicies(conn, "db");
			return null;
		});
		assertNull(SecurityEngineUsageLimitUtils.getLimit(otherLogin));
	}

	@Test
	void teamRenameRekeysPoliciesWithoutLosingTheirRules() throws Exception {
		var old = key(Source.TEAM, "db", "old-team", "CUSTOM");
		save(old, rows(10, Counts.SHARED));
		QueryExecutionUtility.write(db, conn -> {
			SecurityEngineUsageLimitUtils.renameTeamPolicies(conn, "old-team", "new-team", "CUSTOM");
			return null;
		});
		assertNull(SecurityEngineUsageLimitUtils.getLimit(old));
		assertEquals(10, SecurityEngineUsageLimitUtils.getLimit(key(Source.TEAM, "db", "new-team", "CUSTOM")).rules()
				.get(0).amount());
	}

	@Test
	void nativeReadBoundaryEnforcesThePersistedPolicyAndKeepsTheSharedConnection() throws Exception {
		String id = "native-read-" + UUID.randomUUID();
		try (var registration = connection
				.prepareStatement("INSERT INTO ENGINE (ENGINEID,GLOBAL,ENGINETYPE) VALUES (?,FALSE,'DATABASE')")) {
			registration.setString(1, id);
			registration.executeUpdate();
		}
		engines.when(() -> SecurityEngineUtils.userIsOwner(owner, id)).thenReturn(true);
		engines.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype(id))
				.thenReturn(new Object[] { IEngine.CATALOG_TYPE.DATABASE, "H2" });
		QueryExecutionUtility.write(db,conn -> {SecurityEngineUsageLimitUtils.setLimit(conn,key(Source.ENGINE_TOTAL,id,null,null),rows(1,Counts.SHARED),Pair.with("owner","NATIVE"));return null;});
		SecurityEngineUsageLimitUtils.markChanged();
		var previous = ThreadStore.getUser();
		try (var target = DriverManager.getConnection("jdbc:h2:mem:target-" + UUID.randomUUID())) {
			var engine = new RDBMSNativeEngine() {
				@Override
				public Connection getConnection() {
					return JdbcReadUsage.wrap(this, target);
				}

				@Override
				public boolean isConnected() {
					return true;
				}
			};
			engine.setEngineId(id);
			ThreadStore.setUser(reader);
			var result = engine.execQuery("SELECT X FROM SYSTEM_RANGE(1,2)");
			try (var statement = (Statement) result.get(IRDBMSEngine.STATEMENT_OBJECT);
					var data = (ResultSet) result.get(IRDBMSEngine.RESULTSET_OBJECT)) {
				assertTrue(data.next());
				assertThrows(UsageLimitException.class, data::next);
				assertTrue(data.isClosed());
			}
			assertThrows(UsageLimitException.class, () -> engine.execQuery("SELECT 1"));
			assertFalse(target.isClosed());
			assertEquals(1, count("SELECT COUNT(*) FROM ENGINEUSAGEHISTORY"));
		} finally {
			ThreadStore.setUser(previous);
		}
	}
}
