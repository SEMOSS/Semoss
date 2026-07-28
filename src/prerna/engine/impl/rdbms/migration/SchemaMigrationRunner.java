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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;

/**
 * Entry point called from {@code IRDBMSEngine.open(Properties)} when
 * {@code ENABLE_MIGRATIONS=true} is set on the engine's smss. Discovers
 * pending {@code V<version>__<description>.sql} files under the engine's own
 * {@code assets/.migrations} folder, runs each one in version order, records
 * the outcome to {@code SEMOSS_SCHEMA_HISTORY} (inside the target engine's
 * own database), and re-syncs the OWL metamodel once per file that actually
 * ran.
 * <p>
 * Any failure -- a failing SQL statement, an out-of-order version, or a
 * checksum mismatch against an already-applied version -- throws
 * {@link SchemaMigrationException}, which is left to propagate out of
 * {@code open()}. Confirmed in {@code Utility.loadEngine()} that a thrown
 * {@code open()} exception means the engine is never registered in
 * {@code DIHelper} -- so a failed migration leaves the engine fully unusable
 * rather than partially available with a half-migrated schema.
 * <p>
 * Concurrency across a single JVM is already handled upstream --
 * {@code Utility.getDatabase()}/{@code baseGetEngine()} take a per-engine
 * {@code ReentrantLock} (via {@code EngineSyncUtility.getEngineLock}) before
 * calling {@code open()}. Cross-node locking in a clustered deployment is
 * handled by {@link SchemaMigrationLock} -- a real Postgres advisory lock, or
 * a lock-table fallback for MySQL/H2 -- wrapping the entire method below (see
 * {@code docs/database-migrations/locking-research.md} for the full
 * rationale).
 */
public final class SchemaMigrationRunner {

	private static final Logger classLogger = LogManager.getLogger(SchemaMigrationRunner.class);

	private SchemaMigrationRunner() {
		// utility class
	}

	/**
	 * @param engine           the engine to run pending migrations against --
	 *                         must already have a live connection (called after
	 *                         {@code this.engineConnected = true} in
	 *                         {@code RDBMSNativeEngine.open()})
	 * @param migrationsFolder the engine's {@code assets/.migrations} folder
	 */
	public static void runPendingMigrations(IRDBMSEngine engine, File migrationsFolder) {
		try (SchemaMigrationLock lock = SchemaMigrationLock.acquire(engine)) {
			runPendingMigrationsLocked(engine, migrationsFolder);
		} catch (SchemaMigrationLockTimeoutException timeout) {
			handleLockTimeout(engine, migrationsFolder, timeout);
		}
	}

	/**
	 * Called after a lock-acquisition timeout. Most likely explanation: another
	 * node already holds the lock and is (or just finished) applying the same
	 * pending migrations -- in which case there's nothing left for this node to
	 * do and {@code open()} can proceed normally. Only fails {@code open()} if
	 * migrations are genuinely still pending/failed after the timeout, which
	 * means either contention is unusually long-lived or the previous holder
	 * crashed mid-migration and left a real failure behind.
	 */
	private static void handleLockTimeout(IRDBMSEngine engine, File migrationsFolder,
			SchemaMigrationLockTimeoutException timeout) {
		List<MigrationFile> allMigrations = MigrationFileUtils.scanMigrationsFolder(migrationsFolder);
		List<MigrationHistoryRecord> history = MigrationHistoryUtils.getHistory(engine);
		Set<String> appliedVersions = new HashSet<>();
		for (MigrationHistoryRecord record : history) {
			if (record.isSuccess()) {
				appliedVersions.add(record.getVersion());
			}
		}

		boolean stillPending = allMigrations.stream().anyMatch(m -> !appliedVersions.contains(m.getVersion()));
		if (!stillPending) {
			classLogger.info(
					"Migration lock for engine '{}' timed out, but all migrations were already applied by "
							+ "another node -- proceeding.",
					engine.getEngineId());
			return;
		}
		throw new SchemaMigrationException(
				"Could not acquire the migration lock for engine " + engine.getEngineId()
						+ " and migrations are still pending. Another node may still be migrating, or a previous "
						+ "run crashed mid-migration -- check " + "SEMOSS_SCHEMA_HISTORY for details.",
				timeout);
	}

	private static void runPendingMigrationsLocked(IRDBMSEngine engine, File migrationsFolder) {
		ensureMigrationsFolder(migrationsFolder, engine.getEngineId());
		MigrationHistoryUtils.ensureHistoryTable(engine);

		List<MigrationFile> allMigrations = MigrationFileUtils.scanMigrationsFolder(migrationsFolder);
		if (allMigrations.isEmpty()) {
			return;
		}

		List<MigrationHistoryRecord> history = MigrationHistoryUtils.getHistory(engine);
		Set<String> appliedVersions = new HashSet<>();
		String highestAppliedVersion = null;
		for (MigrationHistoryRecord record : history) {
			if (record.isSuccess()) {
				appliedVersions.add(record.getVersion());
				if (highestAppliedVersion == null
						|| MigrationFileUtils.compareVersions(record.getVersion(), highestAppliedVersion) > 0) {
					highestAppliedVersion = record.getVersion();
				}
			}
		}

		boolean ranAtLeastOne = false;
		for (MigrationFile migration : allMigrations) {
			if (appliedVersions.contains(migration.getVersion())) {
				verifyChecksumUnchanged(migration, history);
				continue;
			}

			rejectIfOutOfOrder(migration, highestAppliedVersion);
			runMigration(engine, migration);
			ranAtLeastOne = true;
			// sync once per file -- keeps OWL correct incrementally in case a later
			// file in this same batch fails
			RdbmsMigrationOwlSyncUtils.syncOwlAfterMigration(engine);
		}

		if (ranAtLeastOne) {
			classLogger.info("Completed pending migrations for engine '{}'.", engine.getEngineId());
		}
	}

	/**
	 * Ensures the engine's {@code assets/.migrations} folder exists the moment
	 * {@code ENABLE_MIGRATIONS} is turned on -- not lazily, only whenever
	 * someone happens to save a migration through the UI. An admin (or a
	 * future external tool) should be able to find the folder in place as soon
	 * as the flag is set, even before a single migration has ever been
	 * created.
	 */
	private static void ensureMigrationsFolder(File migrationsFolder, String engineId) {
		try {
			Files.createDirectories(migrationsFolder.toPath());
		} catch (IOException e) {
			classLogger.error("Failed to create migrations folder '{}' for engine '{}'.",
					migrationsFolder.getAbsolutePath(), engineId, e);
			throw new SchemaMigrationException(
					"Unable to create migrations folder for engine " + engineId, e);
		}
	}

	private static void verifyChecksumUnchanged(MigrationFile migration, List<MigrationHistoryRecord> history) {
		String currentChecksum = MigrationFileUtils.computeChecksum(migration.getSqlContent());
		for (MigrationHistoryRecord record : history) {
			if (record.isSuccess() && record.getVersion().equals(migration.getVersion())
					&& !record.getChecksum().equals(currentChecksum)) {
				throw new SchemaMigrationException("Migration " + migration.getFileName()
						+ " has already been applied but its content has changed since then (checksum mismatch). "
						+ "Restore the original file content or create a new version instead of editing it.");
			}
		}
	}

	private static void rejectIfOutOfOrder(MigrationFile migration, String highestAppliedVersion) {
		if (highestAppliedVersion != null
				&& MigrationFileUtils.compareVersions(migration.getVersion(), highestAppliedVersion) < 0) {
			throw new SchemaMigrationException("Migration " + migration.getFileName() + " has version "
					+ migration.getVersion() + ", which is lower than the highest already-applied version "
					+ highestAppliedVersion + ". Out-of-order migrations are not supported.");
		}
	}

	private static void runMigration(IRDBMSEngine engine, MigrationFile migration) {
		long start = System.currentTimeMillis();
		String checksum = MigrationFileUtils.computeChecksum(migration.getSqlContent());
		boolean success = false;
		String failureReason = null;
		Connection conn = null;
		try {
			engine.setAutoCommit(false);
			conn = engine.getConnection();
			for (String statement : MigrationFileUtils.splitStatements(migration.getSqlContent())) {
				try (PreparedStatement ps = conn.prepareStatement(statement)) {
					ps.execute();
				}
			}
			long executionTimeMs = System.currentTimeMillis() - start;
			MigrationHistoryRecord record = new MigrationHistoryRecord(migration.getVersion(), migration.getFileName(),
					checksum, MigrationHistoryRecord.SYSTEM_APPLIED_BY, new Timestamp(System.currentTimeMillis()),
					executionTimeMs, true, null);
			// Insert on the SAME connection/transaction as the migration's own SQL so
			// both commit -- or both roll back -- together. Confirmed from Flyway's own
			// source (SchemaHistory.java: "a migration failure automatically triggers a
			// rollback of all changes, including the ones in the schema history table")
			// that this pairing is the real, correct behavior -- recording success on a
			// separate connection/transaction (the earlier version of this method) left a
			// durability gap: a crash between the two commits would leave the migration
			// permanently applied with no history row, causing it to look "pending" and
			// be retried against non-idempotent DDL next time.
			MigrationHistoryUtils.insertHistoryRow(conn, record);
			engine.commit();
			success = true;
		} catch (Exception e) {
			classLogger.error("Migration '{}' failed against engine '{}'.", migration.getFileName(),
					engine.getEngineId(), e);
			failureReason = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			rollbackQuietly(conn, migration);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}

		if (!success) {
			// the migration's own transaction (and any history row attempted inside it)
			// was just rolled back -- record the failure on its own fresh connection so
			// it's actually durable and visible in the Migrations tab
			long executionTimeMs = System.currentTimeMillis() - start;
			MigrationHistoryRecord failureRecord = new MigrationHistoryRecord(migration.getVersion(),
					migration.getFileName(), checksum, MigrationHistoryRecord.SYSTEM_APPLIED_BY,
					new Timestamp(System.currentTimeMillis()), executionTimeMs, false, failureReason);
			MigrationHistoryUtils.recordMigration(engine, failureRecord);
			throw new SchemaMigrationException(
					"Migration " + migration.getFileName() + " failed: " + failureReason);
		}
	}

	private static void rollbackQuietly(Connection conn, MigrationFile migration) {
		if (conn == null) {
			return;
		}
		try {
			conn.rollback();
		} catch (SQLException rollbackEx) {
			classLogger.error("Failed to roll back migration '{}'.", migration.getFileName(), rollbackEx);
		}
	}

}
