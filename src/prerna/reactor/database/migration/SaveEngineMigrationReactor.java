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
import prerna.engine.impl.rdbms.migration.MigrationStatus;
import prerna.engine.impl.rdbms.migration.MigrationStatusUtils;
import prerna.engine.impl.rdbms.migration.SchemaMigrationException;
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

	public SaveEngineMigrationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.SQL.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String rawEngineId = this.keyValue.get(this.keysToGet[0]);
		String sqlContent = this.keyValue.get(this.keysToGet[1]);
		String description = this.keyValue.get(this.keysToGet[2]);
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
		if (!(database instanceof IRDBMSEngine rdbmsEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a JDBC database engine");
		}

		File migrationsFolder = MigrationFileUtils.getMigrationsFolder(rdbmsEngine);
		String version = writeMigrationFile(migrationsFolder, sqlContent, description);

		Map<String, Object> response = new HashMap<>();
		response.put("version", version);
		try {
			SchemaMigrationRunner.runPendingMigrations(rdbmsEngine, migrationsFolder);
			response.put("success", true);
			response.put("errorMessage", null);
		} catch (SchemaMigrationException e) {
			classLogger.error("Saved migration version '{}' for engine '{}' failed to run.", version, engineId, e);
			response.put("success", false);
			response.put("errorMessage", latestErrorMessage(rdbmsEngine, migrationsFolder, version, e));
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
	 */
	private String latestErrorMessage(IRDBMSEngine engine, File migrationsFolder, String version,
			SchemaMigrationException fallback) {
		List<MigrationStatus> statuses = MigrationStatusUtils.getStatus(engine, migrationsFolder);
		return statuses.stream().filter(s -> s.getVersion().equals(version)).findFirst()
				.map(MigrationStatus::getErrorMessage).orElseGet(() -> fallback.getMessage());
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// this creates AND immediately runs arbitrary DDL/DML -- must never be
		// agent-auto-triggerable
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

}
