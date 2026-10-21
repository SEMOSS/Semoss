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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IEngine;
import prerna.util.JdbcTestDatabase;
import prerna.util.Utility;

class DatabaseWriteAuditUnitTests {

	@Test
	void everyStatementSucceedingIsCommitted() {
		DatabaseWriteAudit.Write write = DatabaseWriteAudit.Write.perStatement("SqlQuery", "sql", 3, 3, 2, null);

		assertEquals(DatabaseWriteAudit.Outcome.COMMITTED, write.outcome());
		assertNull(write.failedStatement());
		assertNull(write.errorMessage());
	}

	@Test
	void aFailureAfterCommittedWritesIsPartial() {
		DatabaseWriteAudit.Write write = DatabaseWriteAudit.Write.perStatement("SqlQuery", "sql", 5, 2, 2,
				new SQLException("relation does not exist"));

		assertEquals(DatabaseWriteAudit.Outcome.PARTIAL, write.outcome());
		assertEquals(3, write.failedStatement());
		assertEquals("relation does not exist", write.errorMessage());
	}

	@Test
	void aFailureBeforeAnyWriteKeepsNothing() {
		// a read ran, then the first write failed
		DatabaseWriteAudit.Write write = DatabaseWriteAudit.Write.perStatement("SqlQuery", "sql", 3, 1, 0,
				new SQLException("syntax error"));

		assertEquals(DatabaseWriteAudit.Outcome.ROLLED_BACK, write.outcome());
		assertEquals(2, write.failedStatement());
	}

	@Test
	void theStatusFollowsTheOutcome() {
		assertEquals(AuditStatus.SUCCESS, DatabaseWriteAudit.status(DatabaseWriteAudit.Outcome.COMMITTED));
		assertEquals(AuditStatus.FAILURE, DatabaseWriteAudit.status(DatabaseWriteAudit.Outcome.PARTIAL));
		assertEquals(AuditStatus.FAILURE, DatabaseWriteAudit.status(DatabaseWriteAudit.Outcome.ROLLED_BACK));
		assertEquals(AuditStatus.ERROR, DatabaseWriteAudit.status(DatabaseWriteAudit.Outcome.UNKNOWN));
	}

	@Test
	void detailsNeverHoldTheSqlAndHashItOnlyWhenTheEngineKeepsInputAndOutput() {
		DatabaseWriteAudit.Write write = DatabaseWriteAudit.Write.perStatement("ExecQuery",
				"UPDATE accounts SET note = 'private' WHERE id = 1", 1, 1, 1, null);

		Map<String, Object> kept = DatabaseWriteAudit.details(write, true);
		assertEquals(List.of("reactor", "transaction", "statements", "succeeded", "outcome", "sqlSha256"),
				new ArrayList<>(kept.keySet()));
		assertEquals(64, ((String) kept.get("sqlSha256")).length());
		assertFalse(kept.toString().contains("private"));

		assertNull(DatabaseWriteAudit.details(write, false).get("sqlSha256"));
	}

	@Test
	void aOneTransactionScriptKeepsItsTimeout() {
		DatabaseWriteAudit.Write write = new DatabaseWriteAudit.Write("SqlQuery",
				DatabaseWriteAudit.Transaction.ONE_TRANSACTION, 40, 12, 13, DatabaseWriteAudit.Outcome.ROLLED_BACK,
				"sql", "timed out", 300);

		Map<String, Object> details = DatabaseWriteAudit.details(write, false);
		assertEquals("ONE_TRANSACTION", details.get("transaction"));
		assertEquals(13, details.get("failedStatement"));
		assertEquals(300, details.get("timeoutSeconds"));
	}

	@Test
	void aWriteIsRecordedAsOneDatabaseWriteEvent() throws Exception {
		try (var db = new JdbcTestDatabase(); var utility = mockStatic(Utility.class)) {
			UserAuditTrailUtilsUnitTests.createAuditTable(db);
			utility.when(Utility::isUserTrackingEnabled).thenReturn(true);
			IEngine engine = mock(IEngine.class);
			when(engine.getEngineId()).thenReturn("database-1");
			when(engine.getEngineName()).thenReturn("Rebuild");
			when(engine.keepInputOutput()).thenReturn(true);

			DatabaseWriteAudit.record(null, engine, DatabaseWriteAudit.Write.perStatement("SqlQuery",
					"DROP TABLE orders; CREATE TABLE orders (id INT)", 2, 1, 1, new SQLException("boom")));

			assertEquals(1, db.count("USER_AUDIT_EVENTS"));
			assertEquals("DATABASE_WRITE", db.value("SELECT EVENT_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("FAILURE", db.value("SELECT STATUS FROM USER_AUDIT_EVENTS"));
			assertEquals("DATABASE", db.value("SELECT TARGET_TYPE FROM USER_AUDIT_EVENTS"));
			assertEquals("database-1", db.value("SELECT ENGINE_ID FROM USER_AUDIT_EVENTS"));
			String details = String.valueOf(db.value("SELECT DETAILS FROM USER_AUDIT_EVENTS"));
			assertTrue(details.contains("\"outcome\":\"PARTIAL\""), details);
			assertTrue(details.contains("sqlSha256"), details);
			assertFalse(details.contains("DROP TABLE"), details);
		}
	}

	@Test
	void nothingIsRecordedWhileUserTrackingIsOff() {
		try (var utility = mockStatic(Utility.class)) {
			utility.when(Utility::isUserTrackingEnabled).thenReturn(false);
			IEngine engine = mock(IEngine.class);

			DatabaseWriteAudit.record(null, engine,
					DatabaseWriteAudit.Write.perStatement("ExecQuery", "DELETE FROM t WHERE id = 1", 1, 1, 1, null));

			verifyNoInteractions(engine);
		}
	}
}
