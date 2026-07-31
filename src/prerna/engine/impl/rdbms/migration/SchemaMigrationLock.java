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

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * Cross-node mutex so two nodes in a clustered SEMOSS deployment can't both
 * apply the same pending migration to the same external database at once.
 * Dialect-dispatched, per the locking research
 * ({@code docs/database-migrations/locking-research.md}):
 * <ul>
 * <li><b>Postgres</b> -- a native session-level advisory lock
 * ({@code pg_advisory_lock}/{@code pg_advisory_unlock}), keyed by a hash of
 * the engine id. Session-level (not transaction-level) is required because
 * {@link SchemaMigrationRunner} commits once per migration file, and the
 * lock must survive across those per-file commits for the whole batch.</li>
 * <li><b>MySQL / H2</b> -- no advisory lock support, so a dedicated
 * {@code SEMOSS_SCHEMA_LOCK} table (one row per engine's database) is used
 * instead, guarded by an atomic {@code INSERT ... WHERE NOT EXISTS}. A lock
 * row older than {@link #STALE_LOCK_THRESHOLD_MS} is assumed abandoned by a
 * crashed node and is stolen.</li>
 * </ul>
 * No Redis dependency -- Redis is an optional deployment feature in SEMOSS
 * (only configured when {@code REDIS_ENABLED}/{@code SEMOSS_IS_CLUSTER_REDIS}
 * is set), so a Redis-only lock would silently provide no protection in any
 * single-node or non-Redis-cluster deployment. See the locking research doc
 * for the full comparison; a Redis-based outer guard may be layered on top of
 * this in a future iteration but is not required for correctness.
 * <p>
 * <b>Important:</b> for Postgres, the connection used to acquire the lock is
 * held open for the lifetime of this object and only released in
 * {@link #close()} -- an advisory lock is tied to the session/connection that
 * took it, so returning that connection to the pool early would either drop
 * the lock or let a different caller unknowingly inherit it.
 */
public final class SchemaMigrationLock implements AutoCloseable {

	private static final Logger classLogger = LogManager.getLogger(SchemaMigrationLock.class);

	/** Public so other migration-package classes (e.g. the OWL sync utility) can exclude this reserved table by name. */
	public static final String LOCK_TABLE = "SEMOSS_SCHEMA_LOCK";

	/** How long to retry acquiring the lock before giving up. */
	private static final long LOCK_WAIT_MS = 30_000L;
	private static final long RETRY_SLEEP_MS = 200L;
	/** A MySQL/H2 lock row older than this is assumed abandoned by a crashed node. */
	private static final long STALE_LOCK_THRESHOLD_MS = 600_000L;
	/**
	 * Identifies which process/node holds a lock row, for stale-lock diagnosis --
	 * {@code <pid>@<hostname>} via the JDK's own runtime bean, no new dependency.
	 */
	private static final String LOCK_OWNER_ID = ManagementFactory.getRuntimeMXBean().getName();

	private final IRDBMSEngine engine;
	private final RdbmsTypeEnum dbType;
	private final long postgresLockKey;
	/** Only populated (and kept open) for the Postgres path -- see class Javadoc. */
	private final Connection postgresLockConnection;

	private SchemaMigrationLock(IRDBMSEngine engine, long postgresLockKey, Connection postgresLockConnection) {
		this.engine = engine;
		this.dbType = engine.getDbType();
		this.postgresLockKey = postgresLockKey;
		this.postgresLockConnection = postgresLockConnection;
	}

	/**
	 * @param engine the engine to lock migration execution for
	 * @return an acquired lock -- release it via try-with-resources
	 * @throws SchemaMigrationLockTimeoutException if the lock is still held by
	 *                                              another node/process after
	 *                                              {@link #LOCK_WAIT_MS}
	 */
	public static SchemaMigrationLock acquire(IRDBMSEngine engine) {
		long lockKey = deriveLockKey(engine.getEngineId());
		if (engine.getDbType() == RdbmsTypeEnum.POSTGRES) {
			return acquirePostgresLock(engine, lockKey);
		}
		return acquireTableLock(engine, lockKey);
	}

	@Override
	public void close() {
		if (dbType == RdbmsTypeEnum.POSTGRES) {
			releasePostgresLock();
		} else {
			releaseTableLock(engine);
		}
	}

	// ------------------------------- Postgres --------------------------------

	private static SchemaMigrationLock acquirePostgresLock(IRDBMSEngine engine, long lockKey) {
		long deadline = System.currentTimeMillis() + LOCK_WAIT_MS;
		Connection conn = null;
		try {
			conn = engine.getConnection();
			do {
				if (tryPostgresAdvisoryLock(conn, lockKey)) {
					classLogger.info(
							"Acquired Postgres advisory migration lock for engine '{}' (key={}). To test contention "
									+ "manually, run 'SELECT pg_try_advisory_lock({});' in a separate psql session "
									+ "while this lock is held -- it should return false until this engine releases it.",
							engine.getEngineId(), lockKey, lockKey);
					return new SchemaMigrationLock(engine, lockKey, conn);
				}
				classLogger.info(
						"Migration lock for engine '{}' (key={}) is currently held elsewhere -- retrying.",
						engine.getEngineId(), lockKey);
				sleepQuietly();
			} while (System.currentTimeMillis() < deadline);
		} catch (SQLException e) {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
			throw new SchemaMigrationException(
					"Failed to acquire Postgres advisory migration lock for engine " + engine.getEngineId(), e);
		}
		ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		classLogger.warn("Timed out after {}ms waiting for Postgres advisory migration lock for engine '{}' (key={}).",
				LOCK_WAIT_MS, engine.getEngineId(), lockKey);
		throw new SchemaMigrationLockTimeoutException(engine.getEngineId(), LOCK_WAIT_MS);
	}

	private static boolean tryPostgresAdvisoryLock(Connection conn, long lockKey) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
			ps.setLong(1, lockKey);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() && rs.getBoolean(1);
			}
		}
	}

	private void releasePostgresLock() {
		try (PreparedStatement ps = postgresLockConnection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
			ps.setLong(1, postgresLockKey);
			ps.execute();
			classLogger.info("Released Postgres advisory migration lock for engine '{}' (key={}).",
					engine.getEngineId(), postgresLockKey);
		} catch (SQLException e) {
			classLogger.error("Failed to release Postgres advisory migration lock for engine '{}'.",
					engine.getEngineId(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, postgresLockConnection);
		}
	}

	// ------------------------------ MySQL / H2 --------------------------------

	private static SchemaMigrationLock acquireTableLock(IRDBMSEngine engine, long lockKey) {
		ensureLockTable(engine);
		long deadline = System.currentTimeMillis() + LOCK_WAIT_MS;
		do {
			if (tryInsertLockRow(engine)) {
				return new SchemaMigrationLock(engine, lockKey, null);
			}
			if (isLockRowStale(engine)) {
				classLogger.warn(
						"Migration lock row for engine '{}' is older than {}ms -- assuming the previous holder "
								+ "crashed and stealing it.",
						engine.getEngineId(), STALE_LOCK_THRESHOLD_MS);
				deleteLockRow(engine);
				continue;
			}
			sleepQuietly();
		} while (System.currentTimeMillis() < deadline);
		throw new SchemaMigrationLockTimeoutException(engine.getEngineId(), LOCK_WAIT_MS);
	}

	private static void releaseTableLock(IRDBMSEngine engine) {
		deleteLockRow(engine);
	}

	private static void ensureLockTable(IRDBMSEngine engine) {
		Connection conn = null;
		try {
			conn = engine.getConnection();
			if (!engine.getQueryUtil().tableExists(conn, LOCK_TABLE, engine.getDatabase(), engine.getSchema())) {
				String[] colNames = { "ENGINEID", "LOCKEDBY", "LOCKEDON" };
				String[] types = { "VARCHAR(255)", "VARCHAR(255)", engine.getQueryUtil().getDateWithTimeDataType() };
				// PRIMARY KEY on ENGINEID makes the insert-guarded-by-select in
				// tryInsertLockRow() race-free: two nodes racing to insert the same
				// ENGINEID can no longer both succeed -- the loser gets a constraint
				// violation and reports "not acquired" instead of silently double-locking.
				Object[] customConstraints = { "PRIMARY KEY", null, null };
				String createSql = engine.getQueryUtil().createTableWithCustomConstraints(LOCK_TABLE, colNames, types,
						customConstraints);
				classLogger.info("Creating migration lock table for engine '{}' with sql {}", engine.getEngineId(),
						createSql);
				engine.insertData(createSql);
			}
		} catch (Exception e) {
			classLogger.error("Failed to ensure migration lock table exists for engine '{}'.", engine.getEngineId(),
					e);
			throw new SchemaMigrationException("Unable to create or verify " + LOCK_TABLE + " for engine "
					+ engine.getEngineId(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
	}

	/**
	 * Guarded insert: the pre-check ({@code lockRowExists}) is only a fast-path
	 * short-circuit to avoid a wasted round trip when contention is likely --
	 * the actual race-freedom comes from the {@code PRIMARY KEY} on
	 * {@code ENGINEID} (see {@code ensureLockTable}). If two nodes both pass the
	 * pre-check at the same instant, only one {@code INSERT} can succeed; the
	 * loser gets a primary-key-violation {@link SQLException} and correctly
	 * reports "not acquired" via the existing catch block below.
	 */
	private static boolean tryInsertLockRow(IRDBMSEngine engine) {
		Connection conn = null;
		try {
			conn = engine.getConnection();
			if (lockRowExists(conn, engine.getEngineId())) {
				return false;
			}
			String insertSql = "INSERT INTO " + LOCK_TABLE + " (ENGINEID, LOCKEDBY, LOCKEDON) VALUES (?, ?, ?)";
			try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
				ps.setString(1, engine.getEngineId());
				ps.setString(2, LOCK_OWNER_ID);
				ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
				ps.execute();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
			return true;
		} catch (SQLException e) {
			classLogger.warn("Could not insert migration lock row for engine '{}' -- likely already held.",
					engine.getEngineId(), e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
	}

	private static boolean lockRowExists(Connection conn, String engineId) throws SQLException {
		String selectSql = "SELECT LOCKEDON FROM " + LOCK_TABLE + " WHERE ENGINEID = ?";
		try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
			ps.setString(1, engineId);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		}
	}

	private static boolean isLockRowStale(IRDBMSEngine engine) {
		Connection conn = null;
		String selectSql = "SELECT LOCKEDON FROM " + LOCK_TABLE + " WHERE ENGINEID = ?";
		try {
			conn = engine.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
				ps.setString(1, engine.getEngineId());
				try (ResultSet rs = ps.executeQuery()) {
					if (!rs.next()) {
						// no row at all -- nothing to steal, but not "stale" either
						return false;
					}
					Timestamp lockedOn = rs.getTimestamp("LOCKEDON");
					return lockedOn != null
							&& System.currentTimeMillis() - lockedOn.getTime() > STALE_LOCK_THRESHOLD_MS;
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to check migration lock staleness for engine '{}'.", engine.getEngineId(), e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
	}

	private static void deleteLockRow(IRDBMSEngine engine) {
		Connection conn = null;
		String deleteSql = "DELETE FROM " + LOCK_TABLE + " WHERE ENGINEID = ?";
		try {
			conn = engine.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
				ps.setString(1, engine.getEngineId());
				ps.execute();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to release migration lock row for engine '{}'.", engine.getEngineId(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
	}

	// -------------------------------- shared ----------------------------------

	private static void sleepQuietly() {
		try {
			Thread.sleep(RETRY_SLEEP_MS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Derives a stable advisory-lock key from an engine id. Collisions between
	 * two different engine ids are theoretically possible (32-bit hash space)
	 * but negligible in practice for UUID-based engine ids, and would only
	 * cause unrelated engines to needlessly serialize on the same lock, never
	 * an incorrect migration outcome.
	 */
	static long deriveLockKey(String engineId) {
		return ((long) ("semoss_migration:" + engineId).hashCode()) & 0x7fffffffffffffffL;
	}

}
