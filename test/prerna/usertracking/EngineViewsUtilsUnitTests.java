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

import org.junit.jupiter.api.Test;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.JdbcTestDatabase;
import prerna.util.QueryExecutionUtility;

class EngineViewsUtilsUnitTests {

	@Test
	void firstViewInsertsAndExistingViewIncrements() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			db.execute("CREATE TABLE ENGINE_VIEWS (ENGINEID VARCHAR, DATE DATE, VIEWS INT)");
			queries.when(() -> QueryExecutionUtility.flushToInteger(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(null).thenReturn(1);
			EngineViewsUtils.add("engine");
			EngineViewsUtils.add("engine");
			assertEquals(1, db.count("ENGINE_VIEWS"));
			assertEquals(2, db.value("SELECT VIEWS FROM ENGINE_VIEWS"));
			verify(db.engine, never()).getPreparedStatement(anyString());
		}
	}

	@Test
	void failedViewWriteRollsBackAndRemainsBestEffort() throws Exception {
		try (var db = new JdbcTestDatabase();
				var queries = mockStatic(QueryExecutionUtility.class, CALLS_REAL_METHODS)) {
			queries.when(() -> QueryExecutionUtility.flushToInteger(eq(db.engine), any(SelectQueryStruct.class)))
					.thenReturn(null);
			db.manual();
			assertDoesNotThrow(() -> EngineViewsUtils.add("engine"));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}
}
