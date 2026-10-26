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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class EngineUsageUtilsUnitTests {

	@Test
	void insertAndReconcileEachCommitOneWrite() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute("CREATE TABLE ENGINE_USES (ENGINEID VARCHAR, INSIGHTID VARCHAR, PROJECTID VARCHAR, DATE DATE)",
					"INSERT INTO ENGINE_USES VALUES ('other','insight','elsewhere',DATE '2020-01-01')");
			db.manual();
			EngineUsageUtils.add(Set.of("old", "kept"), "insight", "project");
			db.connection.rollback();
			assertEquals(3, db.count("ENGINE_USES"));
			db.execute("UPDATE ENGINE_USES SET \"DATE\" = DATE '2020-01-01'");
			db.connection.commit();
			EngineUsageUtils.update(Set.of("new", "kept"), "insight", "project");
			db.connection.rollback();
			assertEquals(3, db.count("ENGINE_USES"));
			assertEquals(0L, db.value("SELECT COUNT(*) FROM ENGINE_USES WHERE ENGINEID='old'"));
			assertEquals(1L, db
					.value("SELECT COUNT(*) FROM ENGINE_USES WHERE ENGINEID='kept' AND \"DATE\" > DATE '2020-01-01'"));
			assertEquals(1L, db.value("SELECT COUNT(*) FROM ENGINE_USES WHERE ENGINEID='other'"));
			verify(db.engine, times(2)).getConnection();
			verify(db.engine, never()).getPreparedStatement(anyString());
		}
	}

	@Test
	void reconcileFailureLeavesEveryUsageRowAsItWas() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.execute(
					"CREATE TABLE ENGINE_USES (ENGINEID VARCHAR CHECK (ENGINEID <> 'reject'), INSIGHTID VARCHAR, PROJECTID VARCHAR, DATE DATE)",
					"INSERT INTO ENGINE_USES VALUES ('old','insight','project',DATE '2020-01-01')");
			db.manual();
			assertDoesNotThrow(() -> EngineUsageUtils.update(Set.of("reject"), "insight", "project"));
			assertEquals(1, db.count("ENGINE_USES"));
			assertEquals("old", db.value("SELECT ENGINEID FROM ENGINE_USES"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
			assertDoesNotThrow(() -> EngineUsageUtils.add(Set.of("fine", "reject"), "insight", "project"));
			assertEquals(1, db.count("ENGINE_USES"));
		}
	}

	@Test
	void failedInsertKeepsLogAndContinueBehaviorAfterRollback() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.manual();
			assertDoesNotThrow(() -> EngineUsageUtils.add(Set.of("db"), "insight", "project"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}
}
