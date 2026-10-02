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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;
import prerna.util.Utility;

class UserTrackingUtilsUnitTests {

	@Test
	void emailTrackingPreservesNullEmptyAndExactLargeText() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class, CALLS_REAL_METHODS)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			db.execute(
					"CREATE TABLE EMAIL_TRACKING (ID VARCHAR, SENT_TIME TIMESTAMP, SUCCESSFUL BOOLEAN, E_FROM VARCHAR, E_TO CLOB, E_CC CLOB, E_BCC CLOB, E_SUBJECT VARCHAR, BODY CLOB, ATTACHMENTS CLOB, IS_HTML BOOLEAN)");
			String body = "  exact body  ".repeat(9000);
			UserTrackingUtils.trackEmail(new String[] { "a", "b" }, null, new String[0], "sender", " ", body, true,
					null, true);
			assertEquals("a, b", db.value("SELECT E_TO FROM EMAIL_TRACKING"));
			assertNull(db.value("SELECT E_CC FROM EMAIL_TRACKING"));
			assertEquals("", db.value("SELECT E_BCC FROM EMAIL_TRACKING"));
			assertEquals(body, db.value("SELECT BODY FROM EMAIL_TRACKING"));
			verify(db.engine, times(1)).getConnection();
		}
	}

	@Test
	void disabledTrackingDoesNotAcquireAndFailuresStillRollback() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class, CALLS_REAL_METHODS)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(false);
			UserTrackingUtils.deleteProject("project");
			verify(db.engine, never()).getConnection();
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			db.manual();
			assertDoesNotThrow(() -> UserTrackingUtils.deleteInsight("project", "insight"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}

	@Test
	void anonymousQueryTrackingStoresExactSqlWithoutBorrowingExtraConnections() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class, CALLS_REAL_METHODS)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			db.execute(
					"CREATE TABLE QUERY_TRACKING (ID VARCHAR, USERID VARCHAR, USERTYPE VARCHAR, DATABASEID VARCHAR, QUERY_EXECUTED CLOB, START_TIME TIMESTAMP, END_TIME TIMESTAMP, TOTAL_EXECUTION_TIME BIGINT, FAILED_EXECUTION BOOLEAN)");
			String query = "  SELECT 1  ";
			UserTrackingUtils.trackQueryExecution(null, "db", query, java.sql.Timestamp.valueOf("2026-01-01 00:00:00"),
					null, null, true);
			assertEquals(query, db.value("SELECT QUERY_EXECUTED FROM QUERY_TRACKING"));
			assertNull(db.value("SELECT USERID FROM QUERY_TRACKING"));
			assertNull(db.value("SELECT END_TIME FROM QUERY_TRACKING"));
			assertEquals(true, db.value("SELECT FAILED_EXECUTION FROM QUERY_TRACKING"));
			verify(db.engine).getConnection();
		}
	}

	@Test
	void trackingWritesAndDeletesKeepProjectAndInsightFilters() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class, CALLS_REAL_METHODS)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			db.execute("CREATE TABLE ENGINE_USES (ENGINEID VARCHAR, INSIGHTID VARCHAR, PROJECTID VARCHAR)",
					"INSERT INTO ENGINE_USES VALUES ('engine','i1','p1'),('engine','i2','p1'),('engine','i3','p2')",
					"CREATE TABLE ENGINE_VIEWS (ENGINEID VARCHAR)",
					"CREATE TABLE USER_CATALOG_VOTES (ENGINEID VARCHAR)",
					"CREATE TABLE INSIGHT_OPENS (INSIGHTID VARCHAR, USERID VARCHAR, OPENED_ON TIMESTAMP, ORIGIN VARCHAR)");
			UserTrackingUtils.trackInsightOpen("i1", "u", "origin");
			assertEquals(1, db.count("INSIGHT_OPENS"));
			UserTrackingUtils.deleteInsight("p1", "i1");
			assertEquals(2, db.count("ENGINE_USES"));
			UserTrackingUtils.deleteProject("p1");
			assertEquals(1, db.count("ENGINE_USES"));
			UserTrackingUtils.deleteEngine("engine");
			assertEquals(0, db.count("ENGINE_USES"));
		}
	}
}
