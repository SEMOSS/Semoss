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
package prerna.reactor.database.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.migration.MigrationStatus;
import prerna.engine.impl.rdbms.migration.MigrationStatusUtils;
import prerna.engine.impl.rdbms.migration.SchemaMigrationException;

/**
 * Covers {@link SaveEngineMigrationReactor#handleRunFailure} -- a failed
 * migration should never leave a permanently-broken version at the head of
 * the chain (blocking every later migration and the engine itself) when the
 * failure is the newly-saved version's own fault, but a still-valid new file
 * must be left alone if it was never even reached because an earlier,
 * unrelated pending migration is what actually failed.
 */
public class SaveEngineMigrationReactorUnitTests {

	private static final String VERSION = "3";
	private static final String FILE_NAME = "V3__add_status_column.sql";

	private SaveEngineMigrationReactor reactor;
	private IRDBMSEngine engine;

	@TempDir
	Path migrationsFolderPath;

	@BeforeEach
	void setup() {
		reactor = new SaveEngineMigrationReactor();
		engine = mock(IRDBMSEngine.class);
	}

	@Test
	void testDeletesFileWhenOurOwnVersionFailed() throws Exception {
		File migrationsFolder = migrationsFolderPath.toFile();
		File savedFile = migrationsFolderPath.resolve(FILE_NAME).toFile();
		Files.writeString(savedFile.toPath(), "ALTER TABLE ORDERS ADD COLUMN BAD_COLUMN NOT_A_REAL_TYPE;");
		assertTrue(savedFile.exists());

		MigrationStatus ourFailedStatus = new MigrationStatus(VERSION, "add_status_column", FILE_NAME,
				MigrationStatus.State.FAILED, "SYSTEM", new Timestamp(System.currentTimeMillis()), 5L,
				"ERROR: type \"not_a_real_type\" does not exist");
		SchemaMigrationException fallback = new SchemaMigrationException("Migration " + FILE_NAME + " failed: fallback");

		try (MockedStatic<MigrationStatusUtils> statusMock = mockStatic(MigrationStatusUtils.class)) {
			statusMock.when(() -> MigrationStatusUtils.getStatus(engine, migrationsFolder))
					.thenReturn(List.of(ourFailedStatus));

			String errorMessage = reactor.handleRunFailure(engine, migrationsFolder, VERSION, fallback);

			assertEquals("ERROR: type \"not_a_real_type\" does not exist", errorMessage);
		}

		assertFalse(savedFile.exists(), "the failed migration's own file should be deleted, not left blocking "
				+ "every later migration and the engine itself");
	}

	@Test
	void testKeepsFileWhenAnEarlierUnrelatedVersionFailedInstead() throws Exception {
		File migrationsFolder = migrationsFolderPath.toFile();
		File savedFile = migrationsFolderPath.resolve(FILE_NAME).toFile();
		Files.writeString(savedFile.toPath(), "ALTER TABLE ORDERS ADD COLUMN STATUS VARCHAR(50);");
		assertTrue(savedFile.exists());

		// our version was never reached -- still PENDING, because an earlier
		// version in the folder is the one that actually failed
		MigrationStatus stillPending = new MigrationStatus(VERSION, "add_status_column", FILE_NAME,
				MigrationStatus.State.PENDING, null, null, 0L, null);
		SchemaMigrationException fallback = new SchemaMigrationException(
				"Migration V2__broken.sql failed: some earlier problem");

		try (MockedStatic<MigrationStatusUtils> statusMock = mockStatic(MigrationStatusUtils.class)) {
			statusMock.when(() -> MigrationStatusUtils.getStatus(engine, migrationsFolder))
					.thenReturn(List.of(stillPending));

			String errorMessage = reactor.handleRunFailure(engine, migrationsFolder, VERSION, fallback);

			assertTrue(errorMessage.contains("queued behind an earlier pending migration"));
			assertTrue(errorMessage.contains("some earlier problem"));
		}

		assertTrue(savedFile.exists(), "a valid, never-attempted migration must not be deleted just because an "
				+ "earlier, unrelated version failed first");
	}

	@Test
	void testFallsBackToExceptionMessageWhenNoStatusFoundForVersion() {
		File migrationsFolder = migrationsFolderPath.toFile();
		SchemaMigrationException fallback = new SchemaMigrationException("Unable to determine migration status");

		try (MockedStatic<MigrationStatusUtils> statusMock = mockStatic(MigrationStatusUtils.class)) {
			statusMock.when(() -> MigrationStatusUtils.getStatus(engine, migrationsFolder)).thenReturn(List.of());

			String errorMessage = reactor.handleRunFailure(engine, migrationsFolder, VERSION, fallback);

			assertEquals("Unable to determine migration status", errorMessage);
		}
	}

}
