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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonObject;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.auth.utils.ExpiredAccessRemoval.AccessTable;
import prerna.auth.utils.ExpiredAccessRemoval.EndedRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * What SEMOSS does with access whose end date has passed: it removes the row,
 * with the usage limit on it, from every table that keeps an end date, and
 * records each removal in the audit trail as ended, with SEMOSS as the actor.
 */
class ExpiredAccessRemovalUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String MODEL_ID = "claimsmodel";
	private static final String APP_ID = "claimsapp";
	private static final List<String> GRANTEE_COLUMNS = List.of("ENGINEID", "GRANTEEENGINEID", "GRANTEEENGINETYPE");

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() {
		this.securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(this.securityDb.getOwlFilePath().contains("junit"));
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(this.securityDb.getOwlFilePath().contains("junit"));
		this.tables = UnitTestSecurityAuthUtils.clearSecurityDB(this.securityDb, this.tables);
	}

	/**
	 * Each table loses the row that ended and keeps the row that ends later and
	 * the row that never ends.
	 */
	@Test
	void everyTableLosesItsEndedRowAndKeepsTheOthers() throws Exception {
		for (AccessTable table : AccessTable.values()) {
			insert(table, "ended", past());
			insert(table, "later", future());
			insert(table, "never", null);
		}

		int removed = withAudit(new ArrayList<>(), ExpiredAccessRemoval::removeAllEndedAccess);

		assertEquals(AccessTable.values().length, removed);
		for (AccessTable table : AccessTable.values()) {
			assertEquals(List.of("later", "never"), subjectsIn(table), table.tableName());
		}
	}

	/**
	 * Each removal is recorded as ended, with SEMOSS as the actor, the reason and
	 * the level the access gave; a place in a team is recorded as a team member
	 * removed.
	 */
	@Test
	void eachRemovalIsRecordedAsEndedWithSemossAsTheActor() throws Exception {
		for (AccessTable table : AccessTable.values()) {
			insert(table, "ended", past());
		}
		List<JsonObject> events = new ArrayList<>();

		withAudit(events, ExpiredAccessRemoval::removeAllEndedAccess);

		assertEquals(AccessTable.values().length, events.size());
		for (JsonObject event : events) {
			assertEquals(UserAuditTrailUtils.PERMISSION_EXPIRED_ACTION, event.get("action").getAsString());
			assertEquals(UserAuditTrailUtils.ACTOR_TYPE_SYSTEM, event.get("actorUserId").getAsString());
			assertEquals("EXPIRED", event.getAsJsonObject("detailsRaw").get("reason").getAsString());
		}
		assertEquals(1, events.stream().filter(e -> "GROUP_MEMBER_REMOVE".equals(e.get("eventType").getAsString()))
				.count());
		assertEquals(AccessTable.values().length - 1, events.stream()
				.filter(e -> "PERMISSION_DELETE".equals(e.get("eventType").getAsString())).count());
	}

	/**
	 * An ended grant names who had it: the person as the subject, the team by its
	 * id and type, and the app or engine by its id and catalog type, with the
	 * level each had.
	 */
	@Test
	void anEndedGrantNamesWhoHadItAndTheLevel() throws Exception {
		insert(AccessTable.USER_ON_ENGINE, "person", past());
		insert(AccessTable.TEAM_ON_ENGINE, "team", past());
		insert(AccessTable.ENGINE_ON_ENGINE, "app", past());
		insertRow(AccessTable.ENGINE_ON_ENGINE.tableName(), GRANTEE_COLUMNS, List.of("db", "vector-db", "VECTOR"), true,
				past());
		List<JsonObject> events = new ArrayList<>();

		withAudit(events, ExpiredAccessRemoval::removeAllEndedAccess);

		JsonObject person = eventFor(events, "granteeId", "person");
		assertEquals("person", person.get("subjectUserId").getAsString());
		assertEquals("EDIT", person.getAsJsonObject("oldValueRaw").get("permission").getAsString());
		JsonObject team = eventFor(events, "groupId", "team");
		assertEquals("CUSTOM", team.getAsJsonObject("detailsRaw").get("groupType").getAsString());
		assertEquals("PROJECT", eventFor(events, "granteeEngineId", "app").getAsJsonObject("detailsRaw")
				.get("granteeEngineType").getAsString());
		assertEquals("VECTOR", eventFor(events, "granteeEngineId", "vector-db").getAsJsonObject("detailsRaw")
				.get("granteeEngineType").getAsString());
	}

	/**
	 * An app's or engine's ended permission on a model takes its App Limit or
	 * Engine Limit with it, and the audit trail records the limit removed with the
	 * grant.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "PROJECT", "GUARDRAIL" })
	void anEndedAppOrEnginePermissionTakesItsLimitWithIt(String granteeType) throws Exception {
		insertRow(AccessTable.ENGINE_ON_ENGINE.tableName(), GRANTEE_COLUMNS, List.of(MODEL_ID, APP_ID, granteeType),
				true, past());
		ModelUsageLimitKey granteeLimit = ModelUsageLimitKey.grantee(MODEL_ID, APP_ID, granteeType);
		QueryExecutionUtility.write(this.securityDb, connection -> {
			SecurityModelUsageLimitUtils.setLimit(connection, granteeLimit, tokensADay(1000), null);
			return null;
		});
		List<JsonObject> events = new ArrayList<>();

		withAudit(events, ExpiredAccessRemoval::removeAllEndedAccess);

		assertFalse(SecurityModelUsageLimitUtils.getEngineLimits(MODEL_ID).containsKey(granteeLimit));
		JsonObject limit = events.stream().filter(e -> "USAGE_LIMIT_UPDATE".equals(e.get("eventType").getAsString()))
				.findFirst().orElseThrow();
		assertTrue(limit.getAsJsonObject("detailsRaw").get("removedWithGrant").getAsBoolean());
		assertFalse(limit.has("subjectUserId"), "an app's or engine's limit names no person");
	}

	/**
	 * Access given again after the sweep read it stays, since a row is deleted
	 * only while its end date is still past.
	 */
	@Test
	void accessGivenAgainBeforeTheRemovalStays() throws Exception {
		insert(AccessTable.USER_ON_ENGINE, "person", past());
		EndedRow row = ExpiredAccessRemoval.readEndedRows(AccessTable.USER_ON_ENGINE).get(0);
		QueryExecutionUtility.executeUpdate(this.securityDb,
				"UPDATE ENGINEPERMISSION SET ENDDATE = ? WHERE USERID = ?", ps -> {
					ps.setTimestamp(1, future());
					ps.setString(2, "person");
				});
		List<JsonObject> events = new ArrayList<>();

		boolean removed = withAudit(events, () -> ExpiredAccessRemoval.removeEndedRow(row));

		assertFalse(removed);
		assertEquals(List.of("person"), subjectsIn(AccessTable.USER_ON_ENGINE));
		assertTrue(events.isEmpty());
	}

	/**
	 * A row two servers find at once is removed by one of them and recorded once.
	 */
	@Test
	void aRowFoundTwiceIsRemovedAndRecordedOnce() throws Exception {
		insert(AccessTable.TEAM_ON_PROJECT, "team", past());
		EndedRow row = ExpiredAccessRemoval.readEndedRows(AccessTable.TEAM_ON_PROJECT).get(0);
		List<JsonObject> events = new ArrayList<>();

		boolean first = withAudit(events, () -> ExpiredAccessRemoval.removeEndedRow(row));
		boolean second = withAudit(events, () -> ExpiredAccessRemoval.removeEndedRow(row));

		assertTrue(first);
		assertFalse(second);
		assertEquals(1, events.size());
	}

	private <T> T withAudit(List<JsonObject> events, Supplier<T> work) {
		try (var utility = mockStatic(Utility.class, CALLS_REAL_METHODS);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			audit.when(() -> UserAuditTrailUtils.recordEvent(any())).thenAnswer(call -> {
				events.add(ProjectPermissionAuditUnitTests.AUDIT_JSON.toJsonTree(call.getArgument(0))
						.getAsJsonObject());
				return null;
			});
			return work.get();
		}
	}

	private static JsonObject eventFor(List<JsonObject> events, String detail, String value) {
		return events.stream().filter(e -> e.getAsJsonObject("detailsRaw").has(detail)
				&& value.equals(e.getAsJsonObject("detailsRaw").get(detail).getAsString())).findFirst().orElseThrow();
	}

	/**
	 * Adds a row to a table, named by the subject column so the tests can tell the
	 * rows apart: the person, team or app, or the person in a team.
	 */
	private void insert(AccessTable table, String subject, Timestamp endDate) throws Exception {
		switch (table) {
		case USER_ON_ENGINE -> insertRow(table.tableName(), List.of("ENGINEID", "USERID"), List.of("db", subject), true,
				endDate);
		case TEAM_ON_ENGINE -> insertRow(table.tableName(), List.of("ENGINEID", "ID", "TYPE"),
				List.of("db", subject, "CUSTOM"), true, endDate);
		case ENGINE_ON_ENGINE -> insertRow(table.tableName(), GRANTEE_COLUMNS, List.of("db", subject, "PROJECT"), true,
				endDate);
		case USER_ON_PROJECT -> insertRow(table.tableName(), List.of("PROJECTID", "USERID"), List.of("app", subject), true,
				endDate);
		case TEAM_ON_PROJECT -> insertRow(table.tableName(), List.of("PROJECTID", "ID", "TYPE"),
				List.of("app", subject, "CUSTOM"), true, endDate);
		case USER_ON_INSIGHT -> insertRow(table.tableName(), List.of("PROJECTID", "INSIGHTID", "USERID"),
				List.of("app", "insight", subject), true, endDate);
		case TEAM_ON_INSIGHT -> insertRow(table.tableName(), List.of("PROJECTID", "INSIGHTID", "ID", "TYPE"),
				List.of("app", "insight", subject, "CUSTOM"), true, endDate);
		case MEMBER_OF_TEAM -> insertRow(table.tableName(), List.of("GROUPID", "USERID", "TYPE"),
				List.of("team", subject, "NATIVE"), false, endDate);
		}
	}

	private void insertRow(String table, List<String> columns, List<String> values, boolean hasLevel,
			Timestamp endDate) throws Exception {
		List<String> all = new ArrayList<>(columns);
		if (hasLevel) {
			all.add("PERMISSION");
		}
		all.add("ENDDATE");
		String marks = String.join(", ", all.stream().map(column -> "?").toList());
		QueryExecutionUtility.executeUpdate(this.securityDb,
				"INSERT INTO " + table + " (" + String.join(", ", all) + ") VALUES (" + marks + ")", ps -> {
					int index = 1;
					for (String value : values) {
						ps.setString(index++, value);
					}
					if (hasLevel) {
						ps.setInt(index++, 2);
					}
					ps.setTimestamp(index, endDate);
				});
		if (!this.tables.contains(table)) {
			this.tables.add(table);
		}
	}

	private List<String> subjectsIn(AccessTable table) throws Exception {
		String column = switch (table) {
		case USER_ON_ENGINE, USER_ON_PROJECT, USER_ON_INSIGHT, MEMBER_OF_TEAM -> "USERID";
		case TEAM_ON_ENGINE, TEAM_ON_PROJECT, TEAM_ON_INSIGHT -> "ID";
		case ENGINE_ON_ENGINE -> "GRANTEEENGINEID";
		};
		return QueryExecutionUtility.queryList(this.securityDb,
				"SELECT " + column + " FROM " + table.tableName() + " ORDER BY " + column, ps -> {
				}, result -> result.getString(1));
	}

	private static Timestamp past() {
		return Timestamp.from(Utility.getCurrentSqlTimestampUTC().toInstant().minus(Duration.ofDays(1)));
	}

	private static Timestamp future() {
		return Timestamp.from(Utility.getCurrentSqlTimestampUTC().toInstant().plus(Duration.ofDays(1)));
	}

	private static ModelUsageLimit tokensADay(long amount) {
		return ModelUsageLimit.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.SHARED)));
	}
}
