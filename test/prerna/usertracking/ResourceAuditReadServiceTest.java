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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

class ResourceAuditReadServiceTest {

	JdbcTestDatabase db;
	User user;
	MockedStatic<SecurityAdminUtils> admins;
	MockedStatic<SecurityEngineUtils> engineOwners;
	MockedStatic<SecurityProjectUtils> projectOwners;
	MockedStatic<Utility> utility;
	MockedStatic<UserAuditTrailUtils> audit;

	@BeforeEach
	void setup() throws Exception {
		db = new JdbcTestDatabase();
		db.registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
		// the reads name the tables and columns as created below, with no OWL
		when(db.engine.isBasic()).thenReturn(true);
		admins = mockStatic(SecurityAdminUtils.class);
		engineOwners = mockStatic(SecurityEngineUtils.class);
		projectOwners = mockStatic(SecurityProjectUtils.class);
		utility = mockStatic(Utility.class);
		audit = mockStatic(UserAuditTrailUtils.class);
		user = mock(User.class);
		engineOwners.when(() -> SecurityEngineUtils.userIsOwner(user, "a")).thenReturn(true);
		db.execute(
				"CREATE TABLE ENGINE (ENGINEID VARCHAR, ENGINENAME VARCHAR, ENGINEDISPLAYNAME VARCHAR, ENGINETYPE VARCHAR, ENGINESUBTYPE VARCHAR)",
				"INSERT INTO ENGINE VALUES ('a', 'model_a', 'Model A', 'MODEL', 'OPEN_AI'), ('b', 'storage_b', NULL, 'STORAGE', 'S3')",
				"CREATE TABLE PROJECT (PROJECTID VARCHAR, PROJECTNAME VARCHAR, PROJECTDISPLAYNAME VARCHAR)",
				"INSERT INTO PROJECT VALUES ('p', 'app_p', 'App P')");
		List<String> fields = List.of("EVENT_ID", "EVENT_TYPE", "ACTION", "STATUS", "CATEGORY", "SEVERITY",
				"ACTOR_USER_ID", "ACTOR_USER_TYPE", "ACTOR_USER_NAME", "SUBJECT_USER_ID", "SUBJECT_USER_TYPE",
				"SUBJECT_USER_NAME", "TARGET_TYPE", "TARGET_ID", "TARGET_NAME", "OLD_VALUE", "NEW_VALUE", "ENGINE_ID",
				"PROJECT_ID", "SESSION_ID_HASH", "IP_ADDR", "DETAILS", "ERROR_MESSAGE");
		db.execute("CREATE TABLE USER_AUDIT_EVENTS (EVENT_TIME TIMESTAMP,"
				+ String.join(",", fields.stream().map(f -> f + " VARCHAR").toList()) + ")");
	}

	@AfterEach
	void close() throws Exception {
		audit.close();
		utility.close();
		projectOwners.close();
		engineOwners.close();
		admins.close();
		db.close();
	}

	void event(String eventId, String type, String targetType, String target, String engine, String project)
			throws Exception {
		try (var statement = db.connection.prepareStatement(
				"INSERT INTO USER_AUDIT_EVENTS (EVENT_ID,EVENT_TIME,EVENT_TYPE,TARGET_TYPE,TARGET_ID,ENGINE_ID,PROJECT_ID,STATUS,OLD_VALUE,NEW_VALUE,IP_ADDR,DETAILS) VALUES (?,TIMESTAMP '2026-10-01 12:00:00',?,?,?,?,?,'SUCCESS','{\"permission\":\"READ_ONLY\",\"secret\":\"hidden\"}','{\"privateMetadata\":\"hidden\"}','127.0.0.1','secret')")) {
			statement.setString(1, eventId);
			statement.setString(2, type);
			statement.setString(3, targetType);
			statement.setString(4, target);
			statement.setString(5, engine);
			statement.setString(6, project);
			statement.executeUpdate();
		}
	}

	@Test
	void functionAndStorageLifecycleEventsAreVisibleOnlyForTheirEngine() throws Exception {
		event("function", "FUNCTION_CREATE", "FUNCTION", "a", "a", null);
		event("storage", "STORAGE_CREATE", "STORAGE", "b", "b", null);
		var functionRows = (List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()).get("events");
		assertEquals(1, functionRows.size());
		assertEquals("FUNCTION_CREATE", ((Map<?, ?>) functionRows.get(0)).get("EVENT_TYPE"));
		engineOwners.when(() -> SecurityEngineUtils.userIsOwner(user, "b")).thenReturn(true);
		var storageRows = (List<?>) ResourceAuditReadService.read(user, "ENGINE", "b", Map.of()).get("events");
		assertEquals(1, storageRows.size());
		assertEquals("STORAGE_CREATE", ((Map<?, ?>) storageRows.get(0)).get("EVENT_TYPE"));
	}

	@Test
	void isolatesCanonicalResourceAndRemovesPrivateFields() throws Exception {
		event("1", "PERMISSION_UPDATE", "ENGINE", "a", "a", null);
		event("2", "PERMISSION_UPDATE", "ENGINE", "b", "b", null);
		event("3", "LOGIN", "ENGINE", "a", "a", null);
		event("4", "API_KEY_CREATE", "ENGINE", "a", "a", null);
		event("5", "PERMISSION_UPDATE", "INSIGHT", "private", "a", null);
		var result = ResourceAuditReadService.read(user, "ENGINE", "a", Map.of());
		var rows = (List<?>) result.get("events");
		assertEquals(1, rows.size());
		var row = (Map<?, ?>) rows.get(0);
		assertEquals("READ_ONLY", row.get("OLD_VALUE"));
		assertNull(row.get("NEW_VALUE"));
		assertFalse(row.containsKey("IP_ADDR"));
		assertNull(row.get("DETAILS"));
		assertFalse(row.containsKey("SESSION_ID_HASH"));
	}

	@Test
	void onlyPermissionIdentityDetailsAreProjected() throws Exception {
		event("1", "PERMISSION_ADD", "ENGINE", "a", "a", null);
		db.execute(
				"UPDATE USER_AUDIT_EVENTS SET DETAILS='{\"groupId\":\"team\",\"groupType\":\"CUSTOM\",\"token\":\"secret\",\"requestId\":\"internal\"}'");
		var row = (Map<?, ?>) ((List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()).get("events"))
				.get(0);
		assertEquals("{\"groupId\":\"team\",\"groupType\":\"CUSTOM\"}", row.get("DETAILS"));
	}

	@Test
	void usageLimitChangesShowTheLimitAndNothingElse() throws Exception {
		event("1", "USAGE_LIMIT_UPDATE", "ENGINE", "a", "a", null);
		db.execute("UPDATE USER_AUDIT_EVENTS SET OLD_VALUE='{\"usageLimit\":null}',"
				+ "NEW_VALUE='{\"usageLimit\":{\"unlimited\":false,\"secret\":\"hidden\",\"rules\":[{\"metric\":\"TOKENS\","
				+ "\"amount\":5000000,\"window\":\"DAY\",\"counts\":\"SHARED\",\"note\":\"hidden\"}]}}',"
				+ "DETAILS='{\"limitType\":\"MODEL_TOTAL\",\"token\":\"secret\"}'");
		var rows = (List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()).get("events");
		assertEquals(1, rows.size());
		var row = (Map<?, ?>) rows.get(0);
		assertEquals("{\"usageLimit\":null}", row.get("OLD_VALUE"));
		assertEquals("{\"usageLimit\":{\"unlimited\":false,\"rules\":[{\"metric\":\"TOKENS\",\"amount\":5000000,"
				+ "\"window\":\"DAY\",\"counts\":\"SHARED\"}]}}", row.get("NEW_VALUE"));
		assertEquals("{\"limitType\":\"MODEL_TOTAL\"}", row.get("DETAILS"));
	}

	@Test
	void paginationAndSortAreStable() throws Exception {
		event("1", "ENGINE_CREATE", "ENGINE", "a", "a", null);
		event("2", "ENGINE_CREATE", "ENGINE", "a", "a", null);
		var first = ResourceAuditReadService.read(user, "ENGINE", "a", Map.of("pageSize", "1", "direction", "ASC"));
		assertEquals(true, first.get("hasNextPage"));
		assertEquals("1", ((Map<?, ?>) ((List<?>) first.get("events")).get(0)).get("EVENT_ID"));
		var second = ResourceAuditReadService.read(user, "ENGINE", "a",
				Map.of("pageSize", "1", "page", "1", "direction", "ASC"));
		assertEquals(false, second.get("hasNextPage"));
		assertEquals("2", ((Map<?, ?>) ((List<?>) second.get("events")).get(0)).get("EVENT_ID"));
	}

	@Test
	void ownershipIsRecheckedOnEveryRead() throws Exception {
		assertTrue(ResourceAuditReadService.isOwner(user, "ENGINE", "a"));
		engineOwners.when(() -> SecurityEngineUtils.userIsOwner(user, "a")).thenReturn(false);
		assertFalse(ResourceAuditReadService.isOwner(user, "ENGINE", "a"));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()));
		assertFalse(ResourceAuditReadService.isOwner(user, "ENGINE", "b"));
	}

	@Test
	void severitySortingUsesRiskOrderNotAlphabeticalOrder() throws Exception {
		event("1", "ENGINE_CREATE", "ENGINE", "a", "a", null);
		event("2", "ENGINE_CREATE", "ENGINE", "a", "a", null);
		event("3", "ENGINE_CREATE", "ENGINE", "a", "a", null);
		db.execute("UPDATE USER_AUDIT_EVENTS SET SEVERITY='MEDIUM' WHERE EVENT_ID='1'",
				"UPDATE USER_AUDIT_EVENTS SET SEVERITY='HIGH' WHERE EVENT_ID='2'",
				"UPDATE USER_AUDIT_EVENTS SET SEVERITY='LOW' WHERE EVENT_ID='3'");
		var rows = (List<?>) ResourceAuditReadService
				.read(user, "ENGINE", "a", Map.of("sort", "SEVERITY", "direction", "DESC")).get("events");
		assertEquals("HIGH", ((Map<?, ?>) rows.get(0)).get("SEVERITY"));
		assertEquals("MEDIUM", ((Map<?, ?>) rows.get(1)).get("SEVERITY"));
		assertEquals("LOW", ((Map<?, ?>) rows.get(2)).get("SEVERITY"));
	}

	@Test
	void eachScopeUsesItsOwnOwnerCheck() throws Exception {
		assertFalse(ResourceAuditReadService.isOwner(user, "PROJECT", "p"));
		projectOwners.when(() -> SecurityProjectUtils.userIsOwner(user, "p")).thenReturn(true);
		assertTrue(ResourceAuditReadService.isOwner(user, "PROJECT", "p"));
		engineOwners.verify(() -> SecurityEngineUtils.userIsOwner(user, "p"), never());
	}

	@Test
	void adminsAndAnonymousUsersNeedNoOwnerCheck() throws Exception {
		admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
		assertTrue(ResourceAuditReadService.isOwner(user, "ENGINE", "b"));
		when(user.isAnonymous()).thenReturn(true);
		assertFalse(ResourceAuditReadService.isOwner(user, "ENGINE", "a"));
		engineOwners.verify(() -> SecurityEngineUtils.userIsOwner(any(), any()), never());
	}

	@Test
	void projectOwnershipDoesNotGrantEngineOrInsightHistory() throws Exception {
		projectOwners.when(() -> SecurityProjectUtils.userIsOwner(user, "p")).thenReturn(true);
		event("1", "PROJECT_CREATE", "PROJECT", "p", null, "p");
		event("2", "PERMISSION_ADD", "INSIGHT", "child", null, "p");
		event("3", "ENGINE_CREATE", "ENGINE", "b", "b", "p");
		event("4", "SKILL_CREATE", "SKILL", "p", null, "p");
		event("5", "SKILL_UPDATE", "SKILL", "child", null, "p");
		assertEquals(2, ((List<?>) ResourceAuditReadService.read(user, "PROJECT", "p", Map.of()).get("events")).size());
		assertFalse(ResourceAuditReadService.isOwner(user, "ENGINE", "b"));
	}

	@Test
	void filtersCannotBroadenScopeAndDatesAreInclusive() throws Exception {
		event("1", "ENGINE_CREATE", "ENGINE", "a", "a", null);
		assertEquals(1,
				((List<?>) ResourceAuditReadService
						.read(user, "ENGINE", "a", Map.of("from", "2026-10-01", "to", "2026-10-01")).get("events"))
						.size());
		assertTrue(((List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of("status", "FAILURE"))
				.get("events")).isEmpty());
		assertTrue(((List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of("eventType", "' OR 1=1 --"))
				.get("events")).isEmpty());
		assertThrows(IllegalArgumentException.class,
				() -> ResourceAuditReadService.read(user, "ENGINE", "a", Map.of("engineId", "b")));
		assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user, "ENGINE", "a",
				Map.of("sort", "EVENT_TIME; DROP TABLE ENGINE")));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceAuditReadService.read(user, "ENGINE", "a", Map.of("pageSize", "101")));
		assertThrows(IllegalArgumentException.class,
				() -> ResourceAuditReadService.read(user, "ENGINE", "a", Map.of("from", "2026-02-30")));
		assertThrows(IllegalArgumentException.class, () -> ResourceAuditReadService.read(user, "ENGINE", "a",
				Map.of("from", "2026-10-03", "to", "2026-10-01")));
	}

	@Test
	void disabledTrackingAndAnonymousFailClosed() throws Exception {
		utility.when(Utility::isUserTrackingDisabled).thenReturn(true);
		assertThrows(IllegalStateException.class, () -> ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()));
		when(user.isAnonymous()).thenReturn(true);
		assertFalse(ResourceAuditReadService.isOwner(user, "ENGINE", "a"));
	}

	@Test
	void eventsShowTheirResourceAsTheCatalogHasItNow() throws Exception {
		event("1", "USAGE_LIMIT_UPDATE", "ENGINE", "a", "a", null);
		var row = (Map<?, ?>) ((List<?>) ResourceAuditReadService.read(user, "ENGINE", "a", Map.of()).get("events"))
				.get(0);
		assertEquals("Model A", row.get("TARGET_DISPLAY_NAME"));
		assertEquals("MODEL", row.get("TARGET_ENGINE_TYPE"));
		assertEquals("OPEN_AI", row.get("TARGET_ENGINE_SUBTYPE"));
		projectOwners.when(() -> SecurityProjectUtils.userIsOwner(user, "p")).thenReturn(true);
		event("2", "PROJECT_UPDATE", "PROJECT", "p", null, "p");
		var app = (Map<?, ?>) ((List<?>) ResourceAuditReadService.read(user, "PROJECT", "p", Map.of()).get("events"))
				.get(0);
		assertEquals("App P", app.get("TARGET_DISPLAY_NAME"));
		assertNull(app.get("TARGET_ENGINE_TYPE"));
	}

	@Test
	void deletedResourcesAreAdminOnly() throws Exception {
		db.execute("DELETE FROM ENGINE WHERE ENGINEID='a'");
		assertFalse(ResourceAuditReadService.isOwner(user, "ENGINE", "a"));
		admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
		assertTrue(ResourceAuditReadService.isOwner(user, "ENGINE", "a"));
	}
}
