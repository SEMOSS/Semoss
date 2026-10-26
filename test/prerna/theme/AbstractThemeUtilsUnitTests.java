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
package prerna.theme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class AbstractThemeUtilsUnitTests {

	@Test
	void initializerLooksUpTablesInReadsAndClearsNewBlocksInAWrite() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var util = spy(db.engine.getQueryUtil());
			doReturn(false).when(util).allowsIfExistsTableSyntax();
			when(db.engine.getQueryUtil()).thenReturn(util);
			doAnswer(invocation -> {
				db.execute((String) invocation.getArgument(0));
				return null;
			}).when(db.engine).insertData(anyString());
			db.manual();
			initialize();
			assertEquals(0, db.count("ADMIN_THEME"));
			assertEquals(0, db.count("BLOCKS_TABLE"));
			verify(db.engine, times(2)).insertData(anyString());
			// two table lookups end their reads; the block delete commits
			verify(db.engine, times(3)).getConnection();
			verify(db.connection, times(2)).rollback();
			verify(db.connection, times(1)).commit();
			verify(db.engine, never()).commit();
			assertFalse(db.connection.isClosed());

			initialize();
			verify(db.engine, times(2)).insertData(anyString());
			verify(db.connection, times(4)).rollback();
			verify(db.connection, times(1)).commit();
		}
	}

	@Test
	void blockDeleteFailureStopsTheInitializer() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			var util = spy(db.engine.getQueryUtil());
			doReturn(false).when(util).allowsIfExistsTableSyntax();
			when(db.engine.getQueryUtil()).thenReturn(util);
			db.manual();
			var failure = assertThrows(java.lang.reflect.InvocationTargetException.class,
					AbstractThemeUtilsUnitTests::initialize);
			assertTrue(failure.getCause() instanceof java.sql.SQLException);
			// two table lookups end their reads; the failed block delete is rolled back
			verify(db.connection, times(3)).rollback();
			verify(db.connection, never()).commit();
		}
	}

	private static void initialize() throws Exception {
		Method initialize = AbstractThemeUtils.class.getDeclaredMethod("initialize");
		initialize.setAccessible(true);
		initialize.invoke(null);
	}
}
