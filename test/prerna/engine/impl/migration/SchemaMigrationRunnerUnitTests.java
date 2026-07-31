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
package prerna.engine.impl.rdbms.migration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Covers {@link SchemaMigrationRunner#rejectIfPreviouslyFailedUnchanged} --
 * the short-circuit that stops a migration with an already-recorded failure
 * from being re-executed against the live database on every subsequent
 * {@code open()} (e.g. from unrelated read-only actions like browsing assets
 * or viewing the smss), while still allowing a retry once the file content
 * (and therefore its checksum) actually changes.
 */
public class SchemaMigrationRunnerUnitTests {

	private static final String VERSION = "3";
	private static final String FILE_NAME = "V3__broken_migration.sql";
	private static final String SQL_CONTENT = "ALTER TABLE ORDERS ADD COLUMN BAD_COLUMN NOT_A_REAL_TYPE;";

	private MigrationFile migrationFile(String sqlContent) {
		return new MigrationFile(VERSION, "broken_migration", FILE_NAME, sqlContent);
	}

	private MigrationHistoryRecord failedRecord(String checksum, String errorMessage) {
		return new MigrationHistoryRecord(VERSION, FILE_NAME, checksum, MigrationHistoryRecord.SYSTEM_APPLIED_BY,
				new Timestamp(System.currentTimeMillis()), 12L, false, errorMessage);
	}

	private MigrationHistoryRecord successRecord(String checksum) {
		return new MigrationHistoryRecord(VERSION, FILE_NAME, checksum, MigrationHistoryRecord.SYSTEM_APPLIED_BY,
				new Timestamp(System.currentTimeMillis()), 12L, true, null);
	}

	@Test
	void testThrowsWhenSameContentAlreadyFailed() {
		MigrationFile migration = migrationFile(SQL_CONTENT);
		String checksum = MigrationFileUtils.computeChecksum(SQL_CONTENT);
		List<MigrationHistoryRecord> history = List
				.of(failedRecord(checksum, "ERROR: type \"not_a_real_type\" does not exist"));

		SchemaMigrationException ex = assertThrows(SchemaMigrationException.class,
				() -> SchemaMigrationRunner.rejectIfPreviouslyFailedUnchanged(migration, history));
		assertTrue(ex.getMessage().contains(FILE_NAME));
		assertTrue(ex.getMessage().contains("not_a_real_type"));
	}

	@Test
	void testDoesNotThrowWhenContentChangedSinceLastFailure() {
		MigrationFile migration = migrationFile(SQL_CONTENT);
		// a different checksum -- as if the file's content was fixed since the
		// recorded failure -- must NOT be treated as the same known failure
		String staleChecksum = MigrationFileUtils.computeChecksum("some completely different sql content");
		List<MigrationHistoryRecord> history = List.of(failedRecord(staleChecksum, "some earlier failure"));

		assertDoesNotThrow(() -> SchemaMigrationRunner.rejectIfPreviouslyFailedUnchanged(migration, history));
	}

	@Test
	void testDoesNotThrowWhenNoHistoryForVersion() {
		MigrationFile migration = migrationFile(SQL_CONTENT);

		assertDoesNotThrow(
				() -> SchemaMigrationRunner.rejectIfPreviouslyFailedUnchanged(migration, Collections.emptyList()));
	}

	@Test
	void testDoesNotThrowWhenOnlyRecordForVersionSucceeded() {
		MigrationFile migration = migrationFile(SQL_CONTENT);
		String checksum = MigrationFileUtils.computeChecksum(SQL_CONTENT);
		List<MigrationHistoryRecord> history = List.of(successRecord(checksum));

		// a successfully-applied version never reaches this check in
		// runPendingMigrationsLocked (it's filtered into appliedVersions first),
		// but this method itself should still be a no-op against a success row
		assertDoesNotThrow(() -> SchemaMigrationRunner.rejectIfPreviouslyFailedUnchanged(migration, history));
	}

	@Test
	void testDoesNotThrowWhenFailureRecordIsForADifferentVersion() {
		MigrationFile migration = migrationFile(SQL_CONTENT);
		String checksum = MigrationFileUtils.computeChecksum(SQL_CONTENT);
		MigrationHistoryRecord otherVersionFailure = new MigrationHistoryRecord("2", "V2__add_customer_phone.sql",
				checksum, MigrationHistoryRecord.SYSTEM_APPLIED_BY, new Timestamp(System.currentTimeMillis()), 12L,
				false, "unrelated failure");

		assertDoesNotThrow(() -> SchemaMigrationRunner.rejectIfPreviouslyFailedUnchanged(migration,
				List.of(otherVersionFailure)));
	}

}
