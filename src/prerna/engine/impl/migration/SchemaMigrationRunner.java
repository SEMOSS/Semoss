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
package prerna.engine.impl.migration;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityMigrationUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;

/**
 * Executes exactly one migration version (already saved to SEMOSS_MIGRATIONS
 * via {@link SecurityMigrationUtils#saveNewVersion}) against its target
 * engine, and records the outcome to SEMOSS_SCHEMA_HISTORY. Content is
 * resolved and versioned entirely by the caller — this class is a pure
 * function of (engine, SQL) plus a result-recording side effect; it does not
 * discover, order, or iterate over multiple migrations, since "save" and
 * "run" happen together for a single version by design.
 */
public class SchemaMigrationRunner {

	private static final Logger classLogger = LogManager.getLogger(SchemaMigrationRunner.class);

	private SchemaMigrationRunner() {
		// static utility class
	}

	/**
	 * @param engineId    the engine to run the migration against
	 * @param migrationId the logical migration id (SEMOSS_MIGRATIONS.MIGRATIONID)
	 * @param version     the version just saved (SEMOSS_MIGRATIONS.VERSION)
	 * @param scriptName  display name, carried through to the history row
	 * @param sqlContent  the SQL to execute
	 * @param userId      attribution for APPLIEDBY
	 * @return the recorded run outcome
	 */
	public static MigrationRecord executeOne(String engineId, String migrationId, int version, String scriptName,
			String sqlContent, String userId) {
		long start = System.currentTimeMillis();
		boolean success = false;
		String errorMessage = null;
		// always fetch the engine fresh — never cache a reference across calls
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(engineId);
		Connection conn = null;
		try {
			engine.setAutoCommit(false);
			conn = engine.getConnection();
			for (String statement : splitStatements(sqlContent)) {
				try (PreparedStatement ps = conn.prepareStatement(statement)) {
					ps.execute();
				}
			}
			engine.commit();
			success = true;
		} catch (Exception e) {
			classLogger.error("Migration {} (version {}) failed against engine {}", migrationId, version, engineId,
					e);
			errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
			rollbackQuietly(conn, migrationId);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
		}

		long executionTimeMs = System.currentTimeMillis() - start;
		// the DESCRIPTION column has no other use in this design -- repurposed to
		// carry the failure reason so it's actually diagnosable from the UI/history
		// instead of only visible in server logs
		MigrationRecord record = new MigrationRecord(engineId, migrationId, version, errorMessage, scriptName,
				computeChecksum(sqlContent), userId, Utility.getCurrentSqlTimestampUTC(), executionTimeMs, success);
		SecurityMigrationUtils.recordMigration(engineId, record);

		// deliberately does not throw on failure: a bad migration is expected,
		// visible data (recorded above and shown in the UI's history), not a system
		// error — the caller (SaveMigrationReactor) decides how to surface it
		return record;
	}

	private static void rollbackQuietly(Connection conn, String migrationId) {
		if (conn == null) {
			return;
		}
		try {
			conn.rollback();
		} catch (SQLException rollbackEx) {
			classLogger.error("Failed to roll back migration {}", migrationId, rollbackEx);
		}
	}

	private static String computeChecksum(String sqlContent) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(sqlContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
			StringBuilder hex = new StringBuilder(hash.length * 2);
			for (byte b : hash) {
				hex.append(String.format("%02x", b));
			}
			return hex.toString();
		} catch (NoSuchAlgorithmException e) {
			// SHA-256 is a guaranteed JDK algorithm; this cannot actually happen
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Minimal statement splitter: one statement per semicolon-terminated group of
	 * lines, ignoring blank lines and "--" comment lines. Does not handle
	 * semicolons inside string literals or stored-procedure bodies — out of scope
	 * for v1 (raw DDL/DML content only).
	 */
	static List<String> splitStatements(String sql) {
		List<String> statements = new ArrayList<>();
		StringBuilder current = new StringBuilder();
		for (String line : sql.split("\n")) {
			String trimmedLine = line.trim();
			if (trimmedLine.isEmpty() || trimmedLine.startsWith("--")) {
				continue;
			}
			current.append(line).append('\n');
			if (trimmedLine.endsWith(";")) {
				addStatement(statements, current.toString());
				current.setLength(0);
			}
		}
		if (current.length() > 0) {
			addStatement(statements, current.toString());
		}
		return statements;
	}

	private static void addStatement(List<String> statements, String rawStatement) {
		String statement = rawStatement.trim();
		if (statement.endsWith(";")) {
			statement = statement.substring(0, statement.length() - 1).trim();
		}
		if (!statement.isEmpty()) {
			statements.add(statement);
		}
	}

}
