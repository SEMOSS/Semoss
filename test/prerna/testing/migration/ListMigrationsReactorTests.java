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
package prerna.testing.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;

public class ListMigrationsReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testListMigrationsReturnsLatestVersionOfEach() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> firstMigration = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN COLA VARCHAR(50);", "add_cola", null);
		Map<String, Object> secondMigration = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN COLB VARCHAR(50);", "add_colb", null);

		// a second version of the first migration -- list should show version 2, not 1
		MigrationTestUtils.saveMigration(engineId, (String) firstMigration.get("migrationId"),
				"ALTER TABLE TEST ADD COLUMN COLC VARCHAR(50);", "add_colc", null);

		List<Map<String, Object>> latest = MigrationTestUtils.listMigrations(engineId);

		assertEquals(2, latest.size());
		Map<String, Object> firstLatest = latest.stream()
				.filter(m -> m.get("migrationId").equals(firstMigration.get("migrationId"))).findFirst().orElseThrow();
		assertEquals(2, firstLatest.get("version"));

		Map<String, Object> secondLatest = latest.stream()
				.filter(m -> m.get("migrationId").equals(secondMigration.get("migrationId"))).findFirst().orElseThrow();
		assertEquals(1, secondLatest.get("version"));
		assertTrue((boolean) secondLatest.get("lastRunSuccess"));
	}

	@Test
	public void testListMigrationsEmptyForNewEngine() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();
		List<Map<String, Object>> latest = MigrationTestUtils.listMigrations(engineId);
		assertEquals(0, latest.size());
	}

}
