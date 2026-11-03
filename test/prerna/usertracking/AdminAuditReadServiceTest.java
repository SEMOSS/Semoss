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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.util.JdbcTestDatabase;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

class AdminAuditReadServiceTest {

	@SuppressWarnings("unchecked")
	private static List<List<Object>> rows(Map<String, Object> response) {
		return (List<List<Object>>) ((Map<?, ?>) response.get("data")).get("values");
	}

	@Test
	void snapshotExcludesReadEventsAndBackdatedConcurrentInserts() throws Exception {
		try (var db = new JdbcTestDatabase();
				var utility = mockStatic(Utility.class);
				var security = mockStatic(SecurityAdminUtils.class)) {
			UserAuditTrailUtilsUnitTests.createAuditTable(db);
			// the reads name the audit table and its columns as created, with no OWL
			when(db.engine.isBasic()).thenReturn(true);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			utility.when(Utility::getCurrentSqlTimestampUTC).thenReturn(Timestamp.valueOf("2026-10-13 12:00:00"));
			db.registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
			User user = mock(User.class);
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			for (int i = 0; i < 40; i++) {
				UserAuditTrailUtils.recordEvent(
						new UserAuditTrailUtils.AuditEvent().eventId("event-" + i).eventType(AuditEventType.LOGIN));
			}
			var first = AdminAuditReadService.read(user, Map.of());
			assertEquals("40", first.get("snapshot"));
			utility.when(Utility::getCurrentSqlTimestampUTC).thenReturn(Timestamp.valueOf("2020-01-01 00:00:00"));
			UserAuditTrailUtils.recordEvent(
					new UserAuditTrailUtils.AuditEvent().eventId("backdated").eventType(AuditEventType.LOGIN));
			var second = AdminAuditReadService.read(user, Map.of("page", "1", "snapshot", "40"));
			List<Object> ids = new ArrayList<>();
			rows(first).subList(0, 25).forEach(row -> ids.add(row.get(0)));
			rows(second).forEach(row -> ids.add(row.get(0)));
			assertEquals(40, ids.size());
			assertEquals(40, new HashSet<>(ids).size());
			assertFalse(ids.contains("backdated"));
			assertEquals(43, AuditChainStore.verify(db.engine));
		}
	}

	@Test
	void inclusiveEndDateUsesNextDayAndFailuresNeverClaimSuccess() throws Exception {
		try (var db = new JdbcTestDatabase();
				var utility = mockStatic(Utility.class);
				var security = mockStatic(SecurityAdminUtils.class)) {
			UserAuditTrailUtilsUnitTests.createAuditTable(db);
			// the reads name the audit table and its columns as created, with no OWL
			when(db.engine.isBasic()).thenReturn(true);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			db.registry.when(SystemEngineRegistry::isUserTrackingDbLoaded).thenReturn(true);
			User user = mock(User.class);
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			for (String date : List.of("2026-10-13T23:59:59.999Z", "2026-10-14T00:00:00Z")) {
				utility.when(Utility::getCurrentSqlTimestampUTC).thenReturn(Timestamp
						.valueOf(java.time.LocalDateTime.ofInstant(Instant.parse(date), java.time.ZoneOffset.UTC)));
				UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(AuditEventType.LOGIN));
			}
			var endOfDay = rows(AdminAuditReadService.read(user, Map.of("to", "2026-10-13", "eventType", "LOGIN")));
			assertEquals(1, endOfDay.size());
			assertEquals("2026-10-13T23:59:59.999Z", endOfDay.get(0).get(1));
			assertThrows(IllegalArgumentException.class,
					() -> AdminAuditReadService.read(user, Map.of("to", "2026-02-30")));
			assertEquals(1, ((Number) db.value(
					"SELECT COUNT(*) FROM USER_AUDIT_EVENTS WHERE EVENT_TYPE='AUDIT_QUERY' AND STATUS='FAILURE'"))
					.intValue());
			try (var audit = mockStatic(UserAuditTrailUtils.class)) {
				db.execute("DROP TABLE USER_AUDIT_EVENTS");
				assertThrows(Exception.class, () -> AdminAuditReadService.read(user, Map.of("export", "true")));
				audit.verify(() -> UserAuditTrailUtils.recordAuditAccess(any(), any(AuditEventType.class), any()),
						never());
				audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));
			}
		}
	}

	@Test
	void nonAdminCannotOpenMaterializedReader() {
		User user = mock(User.class);
		try (var security = mockStatic(SecurityAdminUtils.class);
				var registry = mockStatic(SystemEngineRegistry.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			assertThrows(IllegalArgumentException.class, () -> AdminAuditReadService.read(user, Map.of()));
			registry.verifyNoInteractions();
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));
		}
	}
}
