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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.sql.Timestamp;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class MicrosoftGraphCredentialDbUnitTests {

	@Test
	void schemaAndUpdatesKeepCredentialsSeparateAndPreserveCreationTime() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var columns = new SecurityOwlCreator(db.engine.getQueryUtil()).getDBSchema().stream()
					.filter(table -> "MS_GRAPH_CREDENTIAL".equals(table.getValue0())).findFirst().orElseThrow()
					.getValue1();
			db.execute("CREATE TABLE MS_GRAPH_CREDENTIAL (" + String.join(", ", columns.stream()
					.map(column -> column.getValue0() + " " + column.getValue1()).toList()) + ")");
			db.execute("CREATE UNIQUE INDEX IX_MSGC_USER_UNIQUE ON MS_GRAPH_CREDENTIAL(USER_ID)");
			Timestamp expiry = Timestamp.valueOf("2026-10-12 12:00:00");
			SecurityExternalConnectorsUtils.upsertMicrosoftGraphCredential("one", "one@example.test", " old ",
					" old-refresh ", expiry);
			SecurityExternalConnectorsUtils.upsertMicrosoftGraphCredential("two", null, "other", "other-refresh", null);
			Object created = db.value("SELECT CREATED_ON FROM MS_GRAPH_CREDENTIAL WHERE USER_ID = 'one'");
			Timestamp shorterExpiry = Timestamp.valueOf("2026-10-12 11:00:00");
			SecurityExternalConnectorsUtils.upsertMicrosoftGraphCredential("one", "updated@example.test", " new ",
					" new-refresh ", shorterExpiry);
			var saved = SecurityExternalConnectorsUtils.getMicrosoftGraphCredential("one");
			assertEquals("updated@example.test", saved.get("userEmail"));
			assertEquals(" new ", saved.get("accessToken"));
			assertEquals(" new-refresh ", saved.get("refreshToken"));
			assertEquals(shorterExpiry, saved.get("tokenExpiration"));
			assertEquals(created, db.value("SELECT CREATED_ON FROM MS_GRAPH_CREDENTIAL WHERE USER_ID = 'one'"));
			assertEquals(2, db.count("MS_GRAPH_CREDENTIAL"));
			assertNull(SecurityExternalConnectorsUtils.getMicrosoftGraphCredential("' OR 1=1 --"));
			assertEquals("other", SecurityExternalConnectorsUtils.getMicrosoftGraphCredential("two").get("accessToken"));
		}
	}

	@Test
	void blanksAreRejectedBeforeAccessingDatabase() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			for (String blank : List.of("", " ")) {
				assertThrows(IllegalArgumentException.class, () -> SecurityExternalConnectorsUtils
						.upsertMicrosoftGraphCredential(blank, null, "access", "refresh", null));
				assertThrows(IllegalArgumentException.class, () -> SecurityExternalConnectorsUtils
						.upsertMicrosoftGraphCredential("one", null, blank, "refresh", null));
				assertThrows(IllegalArgumentException.class, () -> SecurityExternalConnectorsUtils
						.upsertMicrosoftGraphCredential("one", null, "access", blank, null));
			}
			assertNull(SecurityExternalConnectorsUtils.getMicrosoftGraphCredential(null));
			verify(db.engine, never()).getConnection();
		}
	}

	@Test
	void failedUpdateRollsBackAndDoesNotExposeDatabasePayload() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE MS_GRAPH_CREDENTIAL (USER_ID VARCHAR PRIMARY KEY, USER_EMAIL VARCHAR(8), "
					+ "ACCESS_TOKEN CLOB, REFRESH_TOKEN CLOB, TOKEN_EXPIRATION TIMESTAMP, CREATED_ON TIMESTAMP, UPDATED_ON TIMESTAMP)");
			SecurityExternalConnectorsUtils.upsertMicrosoftGraphCredential("one", "before", "access", "refresh", null);
			String testSecret = "test-only-payload-never-exposed";
			IllegalStateException failure = assertThrows(IllegalStateException.class,
					() -> SecurityExternalConnectorsUtils.upsertMicrosoftGraphCredential("one", testSecret,
							testSecret, testSecret, null));
			assertFalse(failure.toString().contains(testSecret));
			assertNull(failure.getCause());
			assertEquals("access", SecurityExternalConnectorsUtils.getMicrosoftGraphCredential("one").get("accessToken"));
			verify(db.connection).rollback();
		}
	}
}
