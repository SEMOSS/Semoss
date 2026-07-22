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
package prerna.auth.utils;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.migration.MigrationDefinition;
import prerna.engine.impl.migration.MigrationRecord;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Reads and writes SEMOSS_MIGRATIONS (versioned migration definitions) and
 * SEMOSS_SCHEMA_HISTORY (run outcomes) in the Security DB. Deliberately kept
 * out of each app's own engine schema; see the migration-layer design spec
 * (outputs/review/semoss-migration-layer-spec.md) for why.
 * <p>
 * Versioning follows the same append-only, IS_LATEST-flag convention as
 * {@code PromptUtils.editPrompt}/{@code updatePrompt}
 * (src/prerna/prompt/PromptUtils.java) — content is immutable per version;
 * "editing" always inserts a new version row rather than mutating an old one.
 */
public class SecurityMigrationUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityMigrationUtils.class);

	private static final String MIGRATIONS_TABLE = "SEMOSS_MIGRATIONS";
	private static final String SCHEMA_HISTORY_TABLE = "SEMOSS_SCHEMA_HISTORY";

	/*
	 * ===================== SEMOSS_MIGRATIONS (definitions) =====================
	 */

	/**
	 * Saves a new version of a migration. If {@code migrationId} is null, a new
	 * logical migration is created (version 1); otherwise the current latest
	 * version for that migration id is demoted and a new version is inserted.
	 *
	 * @param engineId   the engine this migration targets
	 * @param migrationId the logical migration id, or null to create a new one
	 * @param scriptName a display name for this version
	 * @param sqlContent the SQL to run
	 * @param notes      optional free-text notes (e.g. "Restored from version 2")
	 * @param userId     attribution for CREATEDBY
	 * @return the newly created version
	 */
	public static MigrationDefinition saveNewVersion(String engineId, String migrationId, String scriptName,
			String sqlContent, String notes, String userId) {
		String resolvedMigrationId = migrationId;
		int nextVersion;
		if (resolvedMigrationId == null || resolvedMigrationId.trim().isEmpty()) {
			resolvedMigrationId = UUID.randomUUID().toString();
			nextVersion = 1;
		} else {
			demoteLatest(resolvedMigrationId);
			nextVersion = getNextVersion(resolvedMigrationId);
		}

		Timestamp createdOn = new Timestamp(System.currentTimeMillis());
		MigrationDefinition definition = new MigrationDefinition(resolvedMigrationId, engineId, nextVersion,
				scriptName, sqlContent, true, userId, createdOn, notes);
		insertMigrationVersion(definition);
		return definition;
	}

	private static int getNextVersion(String migrationId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__VERSION", "version"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MIGRATIONS_TABLE + "__MIGRATIONID", "==", migrationId));
		qs.addOrderBy(new QueryColumnOrderBySelector(MIGRATIONS_TABLE + "__VERSION", "DESC"));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (results.isEmpty()) {
			return 1;
		}
		return ((Number) results.get(0).get("version")).intValue() + 1;
	}

	private static void demoteLatest(String migrationId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String[] colToUpdate = { "ISLATEST" };
		String[] whereCol = { "MIGRATIONID" };
		String updateQuery = securityDb.getQueryUtil().createUpdatePreparedStatementString(MIGRATIONS_TABLE,
				colToUpdate, whereCol);

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			ps.setBoolean(1, false);
			ps.setString(2, migrationId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to demote previous latest version for migration {}", migrationId, e);
			throw new SemossPixelException("Unable to save new migration version: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	private static void insertMigrationVersion(MigrationDefinition definition) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean allowClob = securityDb.getQueryUtil().allowClobJavaObject();
		String sql = "INSERT INTO " + MIGRATIONS_TABLE + " (MIGRATIONID, ENGINEID, VERSION, SCRIPTNAME, SQLCONTENT, "
				+ "ISLATEST, CREATEDBY, CREATEDON, NOTES) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			int i = 1;
			ps.setString(i++, definition.getMigrationId());
			ps.setString(i++, definition.getEngineId());
			ps.setInt(i++, definition.getVersion());
			ps.setString(i++, definition.getScriptName());
			if (allowClob) {
				Clob clob = ps.getConnection().createClob();
				clob.setString(1, definition.getSqlContent());
				ps.setClob(i++, clob);
			} else {
				ps.setString(i++, definition.getSqlContent());
			}
			ps.setBoolean(i++, definition.isLatest());
			ps.setString(i++, definition.getCreatedBy());
			ps.setTimestamp(i++, definition.getCreatedOn());
			ps.setString(i++, definition.getNotes());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert migration version for migration {}", definition.getMigrationId(), e);
			throw new SemossPixelException("Unable to save new migration version: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * @param engineId the engine to list migrations for
	 * @return the latest version of every migration defined for this engine
	 */
	public static List<MigrationDefinition> getLatestMigrationsForEngine(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildMigrationSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MIGRATIONS_TABLE + "__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MIGRATIONS_TABLE + "__ISLATEST", "==", true));

		return toMigrationDefinitions(QueryExecutionUtility.flushRsToMap(securityDb, qs));
	}

	/**
	 * @param migrationId the logical migration id
	 * @return every version of this migration, newest first
	 */
	public static List<MigrationDefinition> getMigrationVersions(String migrationId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildMigrationSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(MIGRATIONS_TABLE + "__MIGRATIONID", "==", migrationId));
		qs.addOrderBy(new QueryColumnOrderBySelector(MIGRATIONS_TABLE + "__VERSION", "DESC"));

		return toMigrationDefinitions(QueryExecutionUtility.flushRsToMap(securityDb, qs));
	}

	private static SelectQueryStruct buildMigrationSelect() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__MIGRATIONID", "migrationId"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__ENGINEID", "engineId"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__VERSION", "version"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__SCRIPTNAME", "scriptName"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__SQLCONTENT", "sqlContent"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__ISLATEST", "isLatest"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__CREATEDBY", "createdBy"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__CREATEDON", "createdOn"));
		qs.addSelector(new QueryColumnSelector(MIGRATIONS_TABLE + "__NOTES", "notes"));
		return qs;
	}

	private static List<MigrationDefinition> toMigrationDefinitions(List<Map<String, Object>> rows) {
		List<MigrationDefinition> definitions = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			definitions.add(new MigrationDefinition((String) row.get("migrationId"), (String) row.get("engineId"),
					((Number) row.get("version")).intValue(), (String) row.get("scriptName"),
					(String) row.get("sqlContent"), (Boolean) row.get("isLatest"), (String) row.get("createdBy"),
					toTimestamp(row.get("createdOn")), (String) row.get("notes")));
		}
		return definitions;
	}

	/**
	 * {@code QueryExecutionUtility.flushRsToMap} returns date/timestamp columns
	 * as {@link SemossDate}, not a raw {@code java.sql.Timestamp} — unlike a
	 * plain JDBC {@code PreparedStatement} read. Handles both since this is
	 * read through the {@code SelectQueryStruct} path, not raw JDBC.
	 */
	private static Timestamp toTimestamp(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Timestamp) {
			return (Timestamp) value;
		}
		if (value instanceof SemossDate) {
			return new Timestamp(((SemossDate) value).getDate().getTime());
		}
		if (value instanceof Date) {
			return new Timestamp(((Date) value).getTime());
		}
		throw new IllegalStateException("Unexpected date type for migration timestamp: " + value.getClass());
	}

	/*
	 * ===================== SEMOSS_SCHEMA_HISTORY (run outcomes) =====================
	 */

	public static void recordMigration(String engineId, MigrationRecord record) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Engine id must not be empty.");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			String sql = "INSERT INTO " + SCHEMA_HISTORY_TABLE + " (ENGINEID, MIGRATIONID, VERSION, DESCRIPTION, "
					+ "SCRIPTNAME, CHECKSUM, APPLIEDBY, APPLIEDON, EXECUTIONTIMEMS, SUCCESS) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				int i = 1;
				ps.setString(i++, engineId);
				ps.setString(i++, record.getMigrationId());
				ps.setInt(i++, record.getVersion());
				ps.setString(i++, record.getDescription());
				ps.setString(i++, record.getScriptName());
				ps.setString(i++, record.getChecksum());
				ps.setString(i++, record.getAppliedBy());
				ps.setTimestamp(i++, record.getAppliedOn());
				ps.setLong(i++, record.getExecutionTimeMs());
				ps.setBoolean(i++, record.isSuccess());
				ps.execute();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to record schema migration for engine {}", engineId, e);
			throw new SemossPixelException("Unable to record schema migration: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * @param migrationId the logical migration id
	 * @param version     the specific version
	 * @return the most recent run outcome for this (migrationId, version), or
	 *         null if it has never been run
	 */
	public static MigrationRecord getRunResult(String migrationId, int version) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildSchemaHistorySelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SCHEMA_HISTORY_TABLE + "__MIGRATIONID", "==", migrationId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SCHEMA_HISTORY_TABLE + "__VERSION", "==", version));
		qs.addOrderBy(new QueryColumnOrderBySelector(SCHEMA_HISTORY_TABLE + "__APPLIEDON", "DESC"));
		qs.setLimit(1);

		List<MigrationRecord> records = toMigrationRecords(QueryExecutionUtility.flushRsToMap(securityDb, qs));
		return records.isEmpty() ? null : records.get(0);
	}

	/**
	 * @param engineId the engine to list run history for
	 * @return every run attempt recorded for this engine, most recent first
	 */
	public static List<MigrationRecord> getAppliedVersions(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildSchemaHistorySelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SCHEMA_HISTORY_TABLE + "__ENGINEID", "==", engineId));
		qs.addOrderBy(new QueryColumnOrderBySelector(SCHEMA_HISTORY_TABLE + "__APPLIEDON", "DESC"));

		return toMigrationRecords(QueryExecutionUtility.flushRsToMap(securityDb, qs));
	}

	private static SelectQueryStruct buildSchemaHistorySelect() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__ENGINEID", "engineId"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__MIGRATIONID", "migrationId"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__VERSION", "version"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__DESCRIPTION", "description"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__SCRIPTNAME", "scriptName"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__CHECKSUM", "checksum"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__APPLIEDBY", "appliedBy"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__APPLIEDON", "appliedOn"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__EXECUTIONTIMEMS", "executionTimeMs"));
		qs.addSelector(new QueryColumnSelector(SCHEMA_HISTORY_TABLE + "__SUCCESS", "success"));
		return qs;
	}

	private static List<MigrationRecord> toMigrationRecords(List<Map<String, Object>> rows) {
		List<MigrationRecord> records = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			records.add(new MigrationRecord((String) row.get("engineId"), (String) row.get("migrationId"),
					((Number) row.get("version")).intValue(), (String) row.get("description"),
					(String) row.get("scriptName"), (String) row.get("checksum"), (String) row.get("appliedBy"),
					toTimestamp(row.get("appliedOn")), ((Number) row.get("executionTimeMs")).longValue(),
					(Boolean) row.get("success")));
		}
		return records;
	}

}
