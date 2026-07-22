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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;

public class GetMigrationVersionsReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testVersionHistoryIsNewestFirstAndOldVersionsAreImmutable() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> v1 = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN COLA VARCHAR(50);", "add_cola", "first");
		String migrationId = (String) v1.get("migrationId");
		MigrationTestUtils.saveMigration(engineId, migrationId, "ALTER TABLE TEST ADD COLUMN COLB VARCHAR(50);",
				"add_colb", "second");
		MigrationTestUtils.saveMigration(engineId, migrationId, "ALTER TABLE TEST ADD COLUMN COLC VARCHAR(50);",
				"add_colc", "third");

		List<Map<String, Object>> versions = MigrationTestUtils.getMigrationVersions(migrationId);

		assertEquals(3, versions.size());
		// newest first
		assertEquals(3, versions.get(0).get("version"));
		assertEquals(2, versions.get(1).get("version"));
		assertEquals(1, versions.get(2).get("version"));

		// only the newest is latest; content of old versions never changes
		assertTrue((boolean) versions.get(0).get("isLatest"));
		assertFalse((boolean) versions.get(1).get("isLatest"));
		assertFalse((boolean) versions.get(2).get("isLatest"));
		assertEquals("ALTER TABLE TEST ADD COLUMN COLA VARCHAR(50);", versions.get(2).get("sqlContent"));
		assertEquals("first", versions.get(2).get("notes"));
	}

	@Test
	public void testEachVersionHasItsOwnRunResult() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> v1 = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN COLA VARCHAR(50);", "add_cola", null);
		String migrationId = (String) v1.get("migrationId");
		// version 2 deliberately fails (column already added by version 1)
		MigrationTestUtils.saveMigration(engineId, migrationId, "ALTER TABLE TEST ADD COLUMN COLA VARCHAR(50);",
				"add_cola_again", null);

		List<Map<String, Object>> versions = MigrationTestUtils.getMigrationVersions(migrationId);
		Map<String, Object> versionTwo = versions.stream().filter(v -> ((Number) v.get("version")).intValue() == 2)
				.findFirst().orElseThrow();
		Map<String, Object> versionOne = versions.stream().filter(v -> ((Number) v.get("version")).intValue() == 1)
				.findFirst().orElseThrow();

		assertFalse((boolean) versionTwo.get("lastRunSuccess"));
		assertTrue((boolean) versionOne.get("lastRunSuccess"));
	}

	@Test
	public void testUnknownMigrationIdThrows() {
		String error = MigrationTestUtils.getMigrationVersionsExpectError("not-a-real-migration-id");
		assertNotNull(error);
	}

}
