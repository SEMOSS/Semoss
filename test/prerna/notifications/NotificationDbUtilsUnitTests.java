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
package prerna.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class NotificationDbUtilsUnitTests {

	private void schema(JdbcTestDatabase db) throws Exception {
		db.execute(
				"CREATE TABLE NOTIFICATION_EVENT (NOTIFICATION_ID VARCHAR PRIMARY KEY, TYPE VARCHAR, SCOPE_TYPE VARCHAR, SCOPE_ID VARCHAR, AUDIENCE_TYPE VARCHAR, AUDIENCE_ID VARCHAR, AUDIENCE_USER_TYPE VARCHAR, TITLE VARCHAR, MESSAGE CLOB, PRIORITY VARCHAR, DISPLAY_SURFACE VARCHAR, SOURCE_TYPE VARCHAR, SOURCE_ID VARCHAR, TARGET_TYPE VARCHAR, TARGET_ID VARCHAR, METADATA_JSON CLOB, CREATED_BY VARCHAR, CREATED_AT TIMESTAMP)");
	}

	private String insert(String id, String message) {
		return NotificationDbUtils.insertNotificationEventIfAbsent(id, "TYPE", "SYSTEM", null, "USER", "u", "NATIVE",
				"Title", message, "NORMAL", "BELL", "USER", "sender", "NONE", null, "{\"x\":null}", "sender");
	}

	@Test
	void insertionCommitsExactPayloadAndDuplicateReadEndsTransaction() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.manual();
			assertEquals("n", insert("n", "  "));
			db.connection.rollback();
			assertEquals("  ", db.value("SELECT MESSAGE FROM NOTIFICATION_EVENT"));
			assertEquals("n", insert("n", "replacement"));
			assertEquals(1, db.count("NOTIFICATION_EVENT"));
			assertEquals("  ", db.value("SELECT MESSAGE FROM NOTIFICATION_EVENT"));
			verify(db.connection, atLeastOnce()).commit();
		}
	}

	@Test
	void insertionFailureRetainsErrorContractAfterRollback() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.execute("ALTER TABLE NOTIFICATION_EVENT ADD CHECK (TITLE <> 'Title')");
			db.manual();
			assertThrows(IllegalStateException.class, () -> insert("n", null));
			assertEquals(0, db.count("NOTIFICATION_EVENT"));
			verify(db.connection, atLeastOnce()).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void visibilityControlsStateUpsertAndUnreadCount() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			schema(db);
			db.execute(
					"CREATE TABLE NOTIFICATION_USER_STATE (NOTIFICATION_ID VARCHAR, USER_ID VARCHAR, USER_TYPE VARCHAR, IS_READ BOOLEAN, READ_AT TIMESTAMP, IS_DISMISSED BOOLEAN, DISMISSED_AT TIMESTAMP)");
			insert("n", "message");
			assertEquals(1, NotificationDbUtils.fetchNewNotificationCount("u", "NATIVE"));
			var now = java.sql.Timestamp.from(java.time.Instant.now());
			assertEquals(0, NotificationDbUtils.markNotificationRead("n", now,
					java.util.List.of(org.javatuples.Pair.with("other", "NATIVE"))));
			assertEquals(0, db.count("NOTIFICATION_USER_STATE"));
			assertEquals(1, NotificationDbUtils.markNotificationRead("n", now,
					java.util.List.of(org.javatuples.Pair.with("u", "NATIVE"))));
			assertEquals(0, NotificationDbUtils.fetchNewNotificationCount("u", "NATIVE"));
			assertEquals(1, NotificationDbUtils.markNotificationRead("n", now,
					java.util.List.of(org.javatuples.Pair.with("u", "NATIVE"))));
			assertEquals(1, db.count("NOTIFICATION_USER_STATE"));
			assertEquals(1, NotificationDbUtils.deleteNotification("u", "NATIVE", "n"));
			assertEquals(true, db.value("SELECT IS_DISMISSED FROM NOTIFICATION_USER_STATE"));
			assertEquals(0, NotificationDbUtils.fetchNewNotificationCount("u", "NATIVE"));
		}
	}

	@Test
	void listingAndBulkReadKeepRecipientScope() throws Exception {
		try (var db = new JdbcTestDatabase();
				var users = mockStatic(prerna.auth.User.class);
				var projects = mockStatic(prerna.auth.utils.SecurityProjectUtils.class);
				var names = mockStatic(prerna.auth.utils.SecurityUserUtils.class);
				var engines = mockStatic(prerna.auth.utils.SecurityEngineUtils.class)) {
			schema(db);
			db.execute(
					"CREATE TABLE NOTIFICATION_USER_STATE (NOTIFICATION_ID VARCHAR, USER_ID VARCHAR, USER_TYPE VARCHAR, IS_READ BOOLEAN, READ_AT TIMESTAMP, IS_DISMISSED BOOLEAN, DISMISSED_AT TIMESTAMP)");
			insert("n", "message");
			var user = mock(prerna.auth.User.class);
			users.when(() -> prerna.auth.User.getUserIdAndType(user))
					.thenReturn(java.util.List.of(org.javatuples.Pair.with("u", "NATIVE")));
			projects.when(
					() -> prerna.auth.utils.SecurityProjectUtils.getUserProjectIdList(user, null, true, false, true))
					.thenReturn(java.util.List.of());
			// The empty sender id avoids the unrelated security-database hydration query.
			db.execute("UPDATE NOTIFICATION_EVENT SET CREATED_BY = NULL");
			assertEquals(1, NotificationDbUtils.fetchNotifications(user, "ALL", null, "10", "0").size());
			assertEquals(1, NotificationDbUtils.markAllNotificationsRead(user, "ALL", null));
			assertEquals(0, NotificationDbUtils.markAllNotificationsRead(user, "ALL", null));
		}
	}

	@Test
	void legacyDeliveryContinuesToNextRecipientAfterFailedInsert() throws Exception {
		try (var db = new JdbcTestDatabase(); var engines = mockStatic(prerna.auth.utils.SecurityEngineUtils.class)) {
			schema(db);
			db.execute("ALTER TABLE NOTIFICATION_EVENT ADD CHECK (AUDIENCE_ID <> 'reject')");
			engines.when(() -> prerna.auth.utils.SecurityEngineUtils.getEngineAuthors("engine"))
					.thenReturn(java.util.List.of(java.util.Map.of("userId", "reject", "userType", "NATIVE")));
			var actor = mock(prerna.auth.User.class, RETURNS_DEEP_STUBS);
			when(actor.getLogins()).thenReturn(java.util.List.of(prerna.auth.AuthProvider.NATIVE));
			when(actor.getAccessToken(prerna.auth.AuthProvider.NATIVE).getId()).thenReturn("actor");
			NotificationDbUtils.createNotification(actor, "affected", "NATIVE", "engine", "ACCESS_REQUEST",
					"ENGINE_CATALOG", "NORMAL", null, null, "BELL");
			assertEquals(1, db.count("NOTIFICATION_EVENT"));
			assertEquals("affected", db.value("SELECT AUDIENCE_ID FROM NOTIFICATION_EVENT"));
			verify(db.connection).rollback();
		}
	}
}
