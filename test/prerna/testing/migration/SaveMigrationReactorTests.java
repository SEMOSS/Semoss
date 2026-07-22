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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;

public class SaveMigrationReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testSaveNewMigrationRunsSuccessfully() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> result = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN NEWCOL VARCHAR(50);", "add_newcol", null);

		assertTrue((boolean) result.get("success"));
		assertEquals(1, result.get("version"));
		String migrationId = (String) result.get("migrationId");
		assertNotNull(migrationId);
		assertNotEquals("", migrationId);
	}

	@Test
	public void testSaveNewVersionOfExistingMigration() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> first = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN NEWCOL VARCHAR(50);", "add_newcol", null);
		String migrationId = (String) first.get("migrationId");

		Map<String, Object> second = MigrationTestUtils.saveMigration(engineId, migrationId,
				"ALTER TABLE TEST ADD COLUMN ANOTHERCOL VARCHAR(50);", "add_anothercol", "second pass");

		assertEquals(migrationId, second.get("migrationId"));
		assertEquals(2, second.get("version"));
		assertTrue((boolean) second.get("success"));

		List<Map<String, Object>> versions = MigrationTestUtils.getMigrationVersions(migrationId);
		assertEquals(2, versions.size());
	}

	@Test
	public void testSaveMigrationWithBadSqlRecordsFailureWithoutThrowing() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> result = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE THIS_TABLE_DOES_NOT_EXIST ADD COLUMN X VARCHAR(50);", "bad_migration", null);

		// a bad migration is expected, visible data -- not a thrown Pixel error
		assertFalse((boolean) result.get("success"));
		assertNotNull(result.get("migrationId"));
	}

	@Test
	public void testSaveMigrationMissingEngineThrows() {
		String error = MigrationTestUtils.saveMigrationExpectError(null, null, "SELECT 1;", "no_engine", null);
		assertNotNull(error);
	}

	@Test
	public void testSaveMigrationMissingSqlContentThrows() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();
		String error = MigrationTestUtils.saveMigrationExpectError(engineId, null, null, "no_sql", null);
		assertNotNull(error);
	}

	@Test
	public void testRestoreIsJustANewVersionWithCopiedContent() {
		String engineId = ApiSemossTestEngineUtils.createBasicEngine();

		Map<String, Object> v1 = MigrationTestUtils.saveMigration(engineId, null,
				"ALTER TABLE TEST ADD COLUMN NEWCOL VARCHAR(50);", "add_newcol", null);
		String migrationId = (String) v1.get("migrationId");

		MigrationTestUtils.saveMigration(engineId, migrationId, "ALTER TABLE TEST ADD COLUMN OTHERCOL VARCHAR(50);",
				"add_othercol", null);

		// "restore" v1: copy its old content forward as a brand new version (v3) --
		// no backend rollback endpoint, this is the whole mechanism
		List<Map<String, Object>> versions = MigrationTestUtils.getMigrationVersions(migrationId);
		Map<String, Object> versionOne = versions.stream().filter(v -> ((Number) v.get("version")).intValue() == 1)
				.findFirst().orElseThrow();
		String restoredContent = (String) versionOne.get("sqlContent");

		Map<String, Object> restored = MigrationTestUtils.saveMigration(engineId, migrationId, restoredContent,
				"restored_add_newcol", "Restored from version 1");

		// "restore" only guarantees a new version is created with the old content --
		// whether re-running that SQL against a table that already has NEWCOL
		// succeeds is a property of the SQL itself (it doesn't, here, since the
		// column already exists), not of the restore mechanism. This is the exact
		// DDL-isn't-safely-reversible tradeoff flagged when this design was chosen.
		assertEquals(3, restored.get("version"));
		assertEquals(restoredContent, MigrationTestUtils.getMigrationVersions(migrationId).stream()
				.filter(v -> ((Number) v.get("version")).intValue() == 3).findFirst().orElseThrow().get("sqlContent"));

		List<Map<String, Object>> allVersions = MigrationTestUtils.getMigrationVersions(migrationId);
		assertEquals(3, allVersions.size());
	}

}
