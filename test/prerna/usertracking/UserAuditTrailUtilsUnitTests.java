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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
		db.execute("CREATE TABLE USER_AUDIT_EVENTS (EVENT_ID VARCHAR, EVENT_TIME TIMESTAMP, EVENT_TYPE VARCHAR, "
				+ "ACTION VARCHAR, STATUS VARCHAR, ACTOR_USER_ID VARCHAR, ACTOR_USER_TYPE VARCHAR, "
				+ "ACTOR_USER_NAME VARCHAR, SESSION_ID VARCHAR, REQUEST_ID VARCHAR, IP_ADDR VARCHAR, "
				+ "TARGET_TYPE VARCHAR, TARGET_ID VARCHAR, TARGET_NAME VARCHAR, PROJECT_ID VARCHAR, "
				+ "ENGINE_ID VARCHAR, INSIGHT_ID VARCHAR, ROOM_ID VARCHAR, OLD_VALUE CLOB, NEW_VALUE CLOB, "
				+ "DETAILS CLOB, ERROR_MESSAGE CLOB)");
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
}
