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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.util.JdbcTestDatabase;

class BlocksThemeUtilsUnitTests {

	@Test
	void validatesBeforeWriteAndRoundTripsJsonBeforeSoftAndHardDeletion() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			assertThrows(IllegalArgumentException.class, () -> BlocksThemeUtils.addBlock(new HashMap<>()));
			verify(db.engine, never()).getConnection();
			db.execute(
					"CREATE TABLE BLOCKS_TABLE (ID VARCHAR, NAME VARCHAR, SECTION VARCHAR, HOVER_TEXT VARCHAR, BLOCK_JSON CLOB, DATE_ADDED TIMESTAMP, IS_LATEST BOOLEAN, CREATED_BY VARCHAR)");
			String json = "  {\"x\":1}  ";
			String id = BlocksThemeUtils
					.addBlock(new HashMap<>(Map.of("name", "name", "section", "test", "json", json)));
			assertEquals(json, db.value("SELECT BLOCK_JSON FROM BLOCKS_TABLE"));
			assertTrue(BlocksThemeUtils.deleteBlock(id, "BLOCKS_TABLE", false));
			assertEquals(false, db.value("SELECT IS_LATEST FROM BLOCKS_TABLE"));
			assertTrue(BlocksThemeUtils.deleteBlock(id, "BLOCKS_TABLE", true));
			assertFalse(BlocksThemeUtils.deleteBlock(id, "BLOCKS_TABLE", true));
		}
	}

	@Test
	void insertFailureRollsBackAndKeepsPublicError() throws Exception {
		try (var db = new JdbcTestDatabase()) {
			db.manual();
			assertThrows(IllegalArgumentException.class,
					() -> BlocksThemeUtils.addBlock(new HashMap<>(Map.of("name", "n", "section", "s", "json", "{}"))));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}
}
