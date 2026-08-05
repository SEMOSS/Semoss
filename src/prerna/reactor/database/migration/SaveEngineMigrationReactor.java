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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.migration.MigrationFile;
import prerna.engine.impl.rdbms.migration.MigrationFileUtils;
import prerna.engine.impl.rdbms.migration.MigrationHistoryRecord;
import prerna.engine.impl.rdbms.migration.MigrationHistoryUtils;
import prerna.engine.impl.rdbms.migration.MigrationStatus;
import prerna.engine.impl.rdbms.migration.MigrationStatusUtils;
import prerna.engine.impl.rdbms.migration.SchemaMigrationException;
import prerna.engine.impl.rdbms.migration.SchemaMigrationLock;
import prerna.engine.impl.rdbms.migration.SchemaMigrationRunner;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Creates the next {@code V<version>__<description>.sql} file under an
 * engine's own {@code assets/.migrations} folder -- and only this reactor
 * (via the Migrations tab) does that. The folder/file are never meant to be
 * hand-created on disk; this is the missing piece that makes that true.
 * Immediately runs the newly-created migration afterward (reusing
 * {@link SchemaMigrationRunner} -- the same lock/checksum/OWL-sync pipeline
 * {@code IRDBMSEngine.open()} uses), so the effect of a saved migration is
 * visible right away instead of requiring a full engine reload.
 *
 * <pre>
 * SaveEngineMigration(engine = ["&lt;engineId&gt;"], sql = ["ALTER TABLE ...;"],
 *     description = ["add_status_column"]);
 * </pre>
 *
 * Returns: MAP containing {@code version}, {@code fileName}, {@code success},
 * and {@code errorMessage} (null on success).
 */
public class SaveEngineMigrationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveEngineMigrationReactor.class);

	private static final String NOTES_KEY = "notes";

	public SaveEngineMigrationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.SQL.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), NOTES_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String rawEngineId = this.keyValue.get(this.keysToGet[0]);
		String sqlContent = this.keyValue.get(this.keysToGet[1]);
		String description = this.keyValue.get(this.keysToGet[2]);
		String notes = this.keyValue.get(NOTES_KEY);
		if (rawEngineId == null || rawEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engine id to save a migration against");
		}
		if (sqlContent == null || sqlContent.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide SQL content for the migration");
		}
		if (description == null || description.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a short description for the migration");
		}

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in");
		}
		String engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, rawEngineId);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit it");
		}

		IDatabaseEngine database = Utility.getDatabase(engineId);
		if (database == null) {
			throw new IllegalArgumentException("Engine " + engineId + " could not be loaded");
		}
		if (!(database instanceof IRDBMSEngine rdbmsEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a JDBC database engine");
		}

		File migrationsFolder = MigrationFileUtils.getMigrationsFolder(rdbmsEngine);
		// Hold the same cross-node lock used by SchemaMigrationRunner while
		// allocating the next version number and writing the file -- otherwise two
		// concurrent saves can both scan the folder, compute the same "next
		// version", and write two different files with a colliding version number
		// (the second then fails at run time with a spurious checksum-mismatch
		// error). Released before running the migration below so the runner's own
		// lock acquisition isn't fighting a lock this same call already holds --
		// the lock-table insert path isn't reentrant, so holding across both steps
		// would just make the runner start it as a retryable timeout every time.
		String version;
		try (SchemaMigrationLock lock = SchemaMigrationLock.acquire(rdbmsEngine)) {
			version = writeMigrationFile(migrationsFolder, sqlContent, description);
		}

		String appliedBy = (user.getPrimaryLoginToken() != null)
				? user.getPrimaryLoginToken().getId()
				: MigrationHistoryRecord.SYSTEM_APPLIED_BY;

		Map<String, Object> response = new HashMap<>();
		response.put("version", version);
		try {
			SchemaMigrationRunner.runPendingMigrations(rdbmsEngine, migrationsFolder, appliedBy);
			MigrationHistoryUtils.updateNotesForVersion(rdbmsEngine, version, notes);
			response.put("success", true);
			response.put("errorMessage", null);
		} catch (SchemaMigrationException e) {
			classLogger.error("Saved migration version '{}' for engine '{}' failed to run.", version, engineId, e);
			response.put("success", false);
			response.put("errorMessage", handleRunFailure(rdbmsEngine, migrationsFolder, version, e));
		}
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Scans the folder for the current highest version, writes the new file as
	 * the next one, and creates the folder itself if this is the engine's first
	 * migration -- the folder/file creation this reactor exists to own instead
	 * of a person doing it by hand on disk.
	 */
	private String writeMigrationFile(File migrationsFolder, String sqlContent, String description) {
		try {
			Files.createDirectories(migrationsFolder.toPath());
			List<MigrationFile> existing = MigrationFileUtils.scanMigrationsFolder(migrationsFolder);
			String version = nextVersion(existing);
			String fileName = "V" + version + "__" + sanitizeDescription(description) + ".sql";
			Files.writeString(migrationsFolder.toPath().resolve(fileName), sqlContent, StandardCharsets.UTF_8);
			return version;
		} catch (IOException e) {
			classLogger.error("Failed to write migration file under '{}'.", migrationsFolder.getAbsolutePath(), e);
			throw new SchemaMigrationException(
					"Unable to write migration file under " + migrationsFolder.getAbsolutePath(), e);
		}
	}

	private String nextVersion(List<MigrationFile> existing) {
		if (existing.isEmpty()) {
			return "1";
		}
		String highest = existing.stream().map(MigrationFile::getVersion)
				.max(MigrationFileUtils::compareVersions).orElse("0");
		int majorSegment = Integer.parseInt(highest.split("\\.")[0]);
		return String.valueOf(majorSegment + 1);
	}

	private String sanitizeDescription(String description) {
		String sanitized = description.trim().replaceAll("[^a-zA-Z0-9-_]", "_");
		return sanitized.isEmpty() ? "migration" : sanitized;
	}

	/**
	 * The runner already recorded the failure to {@code SEMOSS_SCHEMA_HISTORY};
	 * surface that recorded reason if available since it's the most accurate
	 * (e.g. an out-of-order/checksum rejection never even reaches SQL
	 * execution, so it never gets a history row -- in that case fall back to
	 * the exception's own message).
	 * <p>
	 * If OUR just-saved version is the one that actually failed, delete the
	 * file we just wrote instead of leaving a permanently-broken version at
	 * the head of the chain -- otherwise it (and the fail-closed design this
	 * feature already documents) blocks every later migration, and the engine
	 * itself, until someone finds and hand-edits that exact file. Nothing has
	 * recorded it as successfully applied, so it's always safe to discard;
	 * the attempt stays auditable regardless, since the runner's FAILED
	 * history row is untouched and simply shows as a {@code MISSING} status
	 * once its file is gone -- same "history row survives, file doesn't"
	 * shape as an old export missing its migrations folder.
	 * <p>
	 * If a <em>different</em>, earlier pending version is what actually
	 * failed -- this one was never even reached -- keep the file: it's a
	 * legitimately valid, unattempted candidate, just queued behind a
	 * problem this save didn't cause.
	 */
	// package-private (not private) so it's directly unit-testable without
	// mocking the full execute() pipeline, same convention as
	// SchemaMigrationLock.deriveLockKey
	String handleRunFailure(IRDBMSEngine engine, File migrationsFolder, String version,
			SchemaMigrationException fallback) {
		String failedVersion = fallback.getFailedVersion();
		if (failedVersion == null || version.equals(failedVersion)) {
			// our version was the direct failure (or infrastructure failed before any
			// version check) -- delete the file so this version does not re-appear as
			// PENDING on the next engine open
			List<MigrationStatus> statuses = MigrationStatusUtils.getStatus(engine, migrationsFolder);
			MigrationStatus ourStatus = statuses.stream().filter(s -> s.getVersion().equals(version)).findFirst()
					.orElse(null);
			if (ourStatus != null) {
				deleteMigrationFile(migrationsFolder, ourStatus.getFileName(), version);
			}
			return fallback.getMessage();
		}
		// an earlier pending migration failed before ours was reached -- keep our file
		// since it has not been attempted and will run once the blocker is resolved
		return "This migration is valid but is queued behind an earlier pending migration that failed to "
				+ "apply. Resolve that one first (see the Migrations tab), then this version will run "
				+ "automatically on the next attempt: " + fallback.getMessage();
	}

	void deleteMigrationFile(File migrationsFolder, String fileName, String version) {
		if (fileName == null) {
			return;
		}
		try {
			Files.deleteIfExists(migrationsFolder.toPath().resolve(fileName));
		} catch (IOException e) {
			classLogger.error("Failed to delete failed migration file '{}' (version '{}') under '{}'.", fileName,
					version, migrationsFolder.getAbsolutePath(), e);
		}
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// this creates AND immediately runs arbitrary DDL/DML -- must never be
		// agent-auto-triggerable
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

}
