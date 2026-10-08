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
package prerna.usertracking;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

class UserAuditTrailUtilsUnitTests {

	private static void createAuditTable(JdbcTestDatabase db) throws Exception {
		StringBuilder ddl = new StringBuilder("CREATE TABLE USER_AUDIT_EVENTS (");
		for (int i = 0; i < UserAuditTrailUtils.COLUMNS.size(); i++) {
			String column = UserAuditTrailUtils.COLUMNS.get(i);
			String type = switch (column) {
			case "EVENT_TIME", "EVENT_OCCURRED_TIME" -> "TIMESTAMP";
			case "ACTOR_IS_ADMIN" -> "BOOLEAN";
			case "HTTP_STATUS" -> "INT";
			case "OLD_VALUE", "NEW_VALUE", "DETAILS", "ERROR_MESSAGE" -> "CLOB";
			default -> "VARCHAR";
			};
			ddl.append(i == 0 ? "" : ", ").append(column).append(' ').append(type);
		}
		db.execute(ddl.append(')').toString());
		when(db.engine.getPreparedStatement(anyString()))
				.thenAnswer(invocation -> db.connection.prepareStatement(invocation.getArgument(0, String.class)));
	}


	@Test
	void disabledTrackingNeverLoadsTheDatabase() {
		try (var utility = mockStatic(Utility.class); var registry = mockStatic(SystemEngineRegistry.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(false);
			assertDoesNotThrow(() -> UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent()));
			registry.verifyNoInteractions();
		}
	}

	@Test
	void missingDatabaseDoesNotBreakTheCaller() {
		try (var utility = mockStatic(Utility.class); var registry = mockStatic(SystemEngineRegistry.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			assertDoesNotThrow(() -> UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent()));
		}
	}

	@Test
	void failedAuditInsertDoesNotBreakTheCaller() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			when(db.engine.getPreparedStatement(anyString())).thenThrow(new SQLException("Database unavailable"));
			assertDoesNotThrow(() -> UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent()));
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void permissionUpdateWritesOneUpdateWithOldAndNewValues() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordPermissionUpdate(null, "ENGINE", "engine-1", "Model", null, "engine-1",
					null, "target-user", "NATIVE", "READ_ONLY", "EDIT", Map.of("usageFrequency", "DAY"));
			assertEquals(1, db.count("USER_AUDIT_EVENTS"));
			assertEquals("PERMISSION_UPDATE", db.value("SELECT EVENT_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("SUCCESS", db.value("SELECT STATUS FROM USER_AUDIT_EVENTS"));
			assertEquals("engine-1", db.value("SELECT ENGINE_ID FROM USER_AUDIT_EVENTS"));
			assertEquals("{\"permission\":\"READ_ONLY\"}", db.value("SELECT OLD_VALUE FROM USER_AUDIT_EVENTS"));
			assertEquals("{\"permission\":\"EDIT\"}", db.value("SELECT NEW_VALUE FROM USER_AUDIT_EVENTS"));
			assertNotNull(db.value("SELECT EVENT_ID FROM USER_AUDIT_EVENTS"));
		}
	}

	@Test
	void auditInsertCommitsInManualTransactionMode() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			db.manual();
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType("PROJECT_CREATE"));
			verify(db.connection).commit();
			db.connection.rollback();
			assertEquals(1, db.count("USER_AUDIT_EVENTS"));
		}
	}

	@Test
	void permissionChangeFillsSubjectClassificationAndSource() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordPermissionAdd(null, "PROJECT", "project-1", "Analytics", "project-1", null,
					null, "grantee-1", "NATIVE", "EDIT", null);
			assertEquals("grantee-1", db.value("SELECT SUBJECT_USER_ID FROM USER_AUDIT_EVENTS"));
			assertEquals("NATIVE", db.value("SELECT SUBJECT_USER_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("AUTHZ", db.value("SELECT CATEGORY FROM USER_AUDIT_EVENTS"));
			assertEquals("MEDIUM", db.value("SELECT SEVERITY FROM USER_AUDIT_EVENTS"));
			assertEquals("SEMOSS", db.value("SELECT SOURCE_APP FROM USER_AUDIT_EVENTS"));
			assertEquals("UserAuditTrailUtilsUnitTests", db.value("SELECT SOURCE_CLASS FROM USER_AUDIT_EVENTS"));
			assertTrue(String.valueOf(db.value("SELECT HASH_CURRENT FROM USER_AUDIT_EVENTS")).startsWith("sha256:"));
		}
	}

	@Test
	void sessionIdIsOnlyStoredAsAHash() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType("LOGIN")
					.session("raw-session-id", "request-1", "10.0.0.1").httpStatus(200));
			Object hash = db.value("SELECT SESSION_ID_HASH FROM USER_AUDIT_EVENTS");
			assertEquals(UserAuditTrailUtils.hashSessionId("raw-session-id"), hash);
			assertFalse(String.valueOf(hash).contains("raw-session-id"));
			assertEquals("request-1", db.value("SELECT REQUEST_ID FROM USER_AUDIT_EVENTS"));
			assertEquals(200, ((Number) db.value("SELECT HTTP_STATUS FROM USER_AUDIT_EVENTS")).intValue());
			assertEquals("AUTH", db.value("SELECT CATEGORY FROM USER_AUDIT_EVENTS"));
		}
	}

	@Test
	void secretsAreRedactedFromJsonAndErrors() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType("CONFIG_UPDATE")
					.details(Map.of("password", "hunter2", "apiKey", "sk-123", "field", "value"))
					.error("OPERATION_FAILED", "Failed password=hunter2 for jdbc:postgresql://db:5432/x?user=a\n\tat a.b.C.d(C.java:1)"));
			String details = String.valueOf(db.value("SELECT DETAILS FROM USER_AUDIT_EVENTS"));
			assertFalse(details.contains("hunter2"));
			assertFalse(details.contains("sk-123"));
			assertTrue(details.contains("\"field\":\"value\""));
			String error = String.valueOf(db.value("SELECT ERROR_MESSAGE FROM USER_AUDIT_EVENTS"));
			assertFalse(error.contains("hunter2"));
			assertFalse(error.contains("postgresql"));
			assertFalse(error.contains("C.java"));
		}
	}

	@Test
	void authorizationFailuresAreRecordedAsDenied() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordFailure(null, "PROJECT_DELETE", "PROJECT", "project-1",
					new IllegalAccessException("Insufficient privileges to modify this project's permissions."), null);
			assertEquals("AUTHORIZATION_DENIED", db.value("SELECT EVENT_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("PROJECT_DELETE", db.value("SELECT ACTION FROM USER_AUDIT_EVENTS"));
			assertEquals("DENIED", db.value("SELECT STATUS FROM USER_AUDIT_EVENTS"));
			assertEquals("PERMISSION_DENIED", db.value("SELECT ERROR_CODE FROM USER_AUDIT_EVENTS"));
			assertEquals("HIGH", db.value("SELECT SEVERITY FROM USER_AUDIT_EVENTS"));
		}
	}

	@Test
	void pixelFailuresAreOnlyRecordedForDenialsOrAuditedReactors() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordPixelFailure(null, "Frame() | QueryAll() | Collect(10);", null,
					new IllegalArgumentException("Column does not have a value"), null);
			assertEquals(0, db.count("USER_AUDIT_EVENTS"));
			UserAuditTrailUtils.recordPixelFailure(null, "DeleteEngine(engine=[\"secret-query\"]);", null,
					new IllegalArgumentException("Unable to delete engine"), null);
			assertEquals("ENGINE_DELETE", db.value("SELECT EVENT_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("FAILURE", db.value("SELECT STATUS FROM USER_AUDIT_EVENTS"));
			assertFalse(String.valueOf(db.value("SELECT DETAILS FROM USER_AUDIT_EVENTS")).contains("secret-query"));
			UserAuditTrailUtils.recordPixelFailure(null, "AdminSomething();", "AdminSomethingReactor",
					null, "Functionality is only exposed for admins");
			assertEquals(1, ((Number) db.value(
					"SELECT COUNT(*) FROM USER_AUDIT_EVENTS WHERE EVENT_TYPE = 'AUTHORIZATION_DENIED' AND ERROR_CODE = 'ADMIN_REQUIRED'"))
					.intValue());
		}
	}

	@Test
	void eachEventChainsToThePreviousHash() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventId("first").eventType("LOGIN"));
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventId("second").eventType("LOGOUT"));
			Object firstHash = db.value("SELECT HASH_CURRENT FROM USER_AUDIT_EVENTS WHERE EVENT_ID = 'first'");
			assertNotNull(firstHash);
			assertEquals(firstHash, db.value("SELECT HASH_PREVIOUS FROM USER_AUDIT_EVENTS WHERE EVENT_ID = 'second'"));
		}
	}
}
