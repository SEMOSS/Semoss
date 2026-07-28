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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Reads and writes {@code SEMOSS_SCHEMA_HISTORY} -- the run-history table
 * that lives inside the target engine's own database (not centrally, unlike
 * the earlier migration-poc design), so it travels naturally with a DB
 * export/import. Table creation follows the same
 * check-then-create-if-not-exists convention every other SEMOSS internal
 * table uses (see {@code AbstractSecurityUtils#loadSecurityDatabase}).
 */
public final class MigrationHistoryUtils {

	private static final Logger classLogger = LogManager.getLogger(MigrationHistoryUtils.class);

	/** Public so other migration-package classes (e.g. the OWL sync utility) can exclude this reserved table by name. */
	public static final String HISTORY_TABLE = "SEMOSS_SCHEMA_HISTORY";

	private static final String[] COL_NAMES = { "VERSION", "SCRIPTNAME", "CHECKSUM", "APPLIEDBY", "APPLIEDON",
			"EXECUTIONTIMEMS", "SUCCESS", "DESCRIPTION" };

	private MigrationHistoryUtils() {
		// utility class
	}

	/**
	 * Idempotent check-then-create for {@code SEMOSS_SCHEMA_HISTORY} inside the
	 * given engine's own database.
	 *
	 * @param engine the engine whose database should have the history table
	 */
	public static void ensureHistoryTable(IRDBMSEngine engine) {
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		String[] types = { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
				queryUtil.getDateWithTimeDataType(), "BIGINT", queryUtil.getBooleanDataTypeName(), "VARCHAR(2000)" };

		Connection conn = null;
		try {
			conn = engine.getConnection();
			if (!queryUtil.tableExists(conn, HISTORY_TABLE, engine.getDatabase(), engine.getSchema())) {
				String createSql = queryUtil.createTable(HISTORY_TABLE, COL_NAMES, types);
				classLogger.info("Creating migration history table for engine '{}' with sql {}", engine.getEngineId(),
						createSql);
				engine.insertData(createSql);
			}
		} catch (Exception e) {
			classLogger.error("Failed to ensure migration history table exists for engine '{}'.", engine.getEngineId(),
					e);
			throw new SchemaMigrationException(
					"Unable to create or verify " + HISTORY_TABLE + " for engine " + engine.getEngineId(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
	}

	/**
	 * @param engine the engine to read migration history from
	 * @return every recorded run attempt, ordered by version ascending
	 */
	public static List<MigrationHistoryRecord> getHistory(IRDBMSEngine engine) {
		// Deliberately raw JDBC, not SelectQueryStruct: SEMOSS_SCHEMA_HISTORY *is*
		// registered as an OWL concept (see RdbmsMigrationOwlSyncUtils), but only
		// after the first migration actually runs and its post-run OWL sync
		// fires. This method can be called before that ever happens (e.g. right
		// after ENABLE_MIGRATIONS is turned on, before any migration has run) --
		// SelectQueryStruct resolves column selectors against OWL-registered
		// physical URIs, so relying on it here would fail during that bootstrap
		// window. Raw JDBC keeps this read correct regardless of OWL sync
		// timing.
		String selectSql = "SELECT VERSION, SCRIPTNAME, CHECKSUM, APPLIEDBY, APPLIEDON, EXECUTIONTIMEMS, SUCCESS, "
				+ "DESCRIPTION FROM " + HISTORY_TABLE + " ORDER BY VERSION ASC";
		Connection conn = null;
		List<MigrationHistoryRecord> records = new ArrayList<>();
		try {
			conn = engine.getConnection();
			if (!engine.getQueryUtil().tableExists(conn, HISTORY_TABLE, engine.getDatabase(), engine.getSchema())) {
				// nothing has ever run against this engine yet
				return records;
			}
			try (PreparedStatement ps = conn.prepareStatement(selectSql); ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					records.add(new MigrationHistoryRecord(rs.getString("VERSION"), rs.getString("SCRIPTNAME"),
							rs.getString("CHECKSUM"), rs.getString("APPLIEDBY"), rs.getTimestamp("APPLIEDON"),
							rs.getLong("EXECUTIONTIMEMS"), rs.getBoolean("SUCCESS"), rs.getString("DESCRIPTION")));
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to read migration history for engine '{}'.", engine.getEngineId(), e);
			throw new SchemaMigrationException("Unable to read migration history for engine " + engine.getEngineId(),
					e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
		return records;
	}

	/**
	 * Records a run outcome using a connection/transaction the caller already
	 * owns and will commit itself -- used on the success path so the
	 * migration's SQL and its history row commit atomically together.
	 * Confirmed from Flyway's own source
	 * ({@code SchemaHistory.java}: "a migration failure automatically triggers
	 * a rollback of all changes, including the ones in the schema history
	 * table") that this same-transaction pairing is the real behavior worth
	 * matching, not two independent commits.
	 *
	 * @param conn   an open connection/transaction the caller controls; this
	 *               method does not commit or close it
	 * @param record the run outcome to record
	 */
	public static void insertHistoryRow(Connection conn, MigrationHistoryRecord record) throws SQLException {
		String insertSql = "INSERT INTO " + HISTORY_TABLE
				+ " (VERSION, SCRIPTNAME, CHECKSUM, APPLIEDBY, APPLIEDON, EXECUTIONTIMEMS, SUCCESS, DESCRIPTION) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
			int index = 1;
			ps.setString(index++, record.getVersion());
			ps.setString(index++, record.getScriptName());
			ps.setString(index++, record.getChecksum());
			ps.setString(index++, record.getAppliedBy());
			ps.setTimestamp(index++, record.getAppliedOn());
			ps.setLong(index++, record.getExecutionTimeMs());
			ps.setBoolean(index++, record.isSuccess());
			ps.setString(index++, record.getDescription());
			ps.execute();
		}
	}

	/**
	 * Records a run outcome on its own, independently-committed
	 * connection/transaction -- used on the failure path, where the
	 * migration's own transaction has already been rolled back and recording
	 * the failure needs a fresh connection to actually persist.
	 *
	 * @param engine the engine whose history table this run outcome belongs to
	 * @param record the run outcome to record
	 */
	public static void recordMigration(IRDBMSEngine engine, MigrationHistoryRecord record) {
		Connection conn = null;
		try {
			conn = engine.getConnection();
			insertHistoryRow(conn, record);
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to record migration history for engine '{}', version '{}'.",
					engine.getEngineId(), record.getVersion(), e);
			throw new SchemaMigrationException(
					"Unable to record migration history for version " + record.getVersion(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}
	}

}
