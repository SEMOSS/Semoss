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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.JdbcTestDatabase;
import prerna.util.QueryExecutionUtility;

class EngineUsageUtilsUnitTests {

	@Test
	void insertAndReconcileUseCommittedIndependentOperations() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			db.execute("CREATE TABLE ENGINE_USES (ENGINEID VARCHAR, INSIGHTID VARCHAR, PROJECTID VARCHAR, DATE DATE)");
			db.manual();
			EngineUsageUtils.add(Set.of("old", "kept"), "insight", "project");
			db.connection.rollback();
			assertEquals(2, db.count("ENGINE_USES"));
			queries.when(() -> QueryExecutionUtility.flushToListString(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(new ArrayList<>(List.of("old", "kept")));
			EngineUsageUtils.update(Set.of("new", "kept"), "insight", "project");
			assertEquals(2, db.count("ENGINE_USES"));
			assertEquals(0L, db.value("SELECT COUNT(*) FROM ENGINE_USES WHERE ENGINEID='old'"));
			verify(db.engine, never()).getPreparedStatement(anyString());
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
