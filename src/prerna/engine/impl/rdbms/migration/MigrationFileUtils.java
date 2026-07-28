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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.EngineUtility;

/**
 * Scans an engine's {@code assets/.migrations} folder for versioned SQL
 * migration files and computes their checksums. Files are only ever created
 * by the Migrations UI tab (never hand-edited), but this still validates the
 * naming convention defensively — e.g. a folder restored from an old export
 * — and simply ignores/logs anything that doesn't match, the same way other
 * asset folders in SEMOSS are scanned (see {@code FaissDatabaseEngine}'s
 * {@code .pkl} filter, {@code ListPlaywrightScriptsReactor}'s {@code .json}
 * filter).
 */
public final class MigrationFileUtils {

	private static final Logger classLogger = LogManager.getLogger(MigrationFileUtils.class);

	/** {@code V<version>__<description>.sql} — matches Flyway's versioned migration convention. */
	private static final Pattern MIGRATION_FILE_PATTERN = Pattern
			.compile("^V(\\d+(?:\\.\\d+)*)__(.+)\\.sql$", Pattern.CASE_INSENSITIVE);

	private MigrationFileUtils() {
		// utility class
	}

	/**
	 * @param engine the engine whose migrations folder should be resolved
	 * @return the engine's own {@code assets/.migrations} folder -- shared
	 *         resolution logic used both by {@code RDBMSNativeEngine.open()}
	 *         and by the read-only status reactor, so the two never drift
	 *         apart on where this folder lives
	 */
	public static File getMigrationsFolder(IRDBMSEngine engine) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE,
				engine.getEngineId(), engine.getEngineName());
		return new File(assetsFolder, ".migrations");
	}

	/**
	 * @param migrationsFolder the engine's {@code assets/.migrations} folder
	 * @return every validly-named migration file in the folder, ordered by
	 *         version ascending (dotted versions compared numerically, segment
	 *         by segment — e.g. {@code V2} before {@code V2.1} before
	 *         {@code V10})
	 */
	public static List<MigrationFile> scanMigrationsFolder(File migrationsFolder) {
		List<MigrationFile> migrations = new ArrayList<>();
		if (migrationsFolder == null || !migrationsFolder.exists() || !migrationsFolder.isDirectory()) {
			return migrations;
		}

		File[] files = migrationsFolder.listFiles();
		if (files == null) {
			return migrations;
		}

		for (File file : files) {
			if (!file.isFile()) {
				continue;
			}
			MigrationFile migration = parseMigrationFile(file);
			if (migration != null) {
				migrations.add(migration);
			}
		}

		migrations.sort(Comparator.comparing(MigrationFile::getVersion, MigrationFileUtils::compareVersions));
		return migrations;
	}

	private static MigrationFile parseMigrationFile(File file) {
		Matcher matcher = MIGRATION_FILE_PATTERN.matcher(file.getName());
		if (!matcher.matches()) {
			classLogger.warn(
					"Ignoring file '{}' in migrations folder -- does not match the required "
							+ "V<version>__<description>.sql naming convention",
					file.getName());
			return null;
		}

		String version = matcher.group(1);
		String description = matcher.group(2);
		String sqlContent;
		try {
			sqlContent = Files.readString(file.toPath(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			classLogger.error("Failed to read migration file '{}'.", file.getName(), e);
			throw new SchemaMigrationException("Unable to read migration file " + file.getName(), e);
		}
		return new MigrationFile(version, description, file.getName(), sqlContent);
	}

	/**
	 * Compares two dotted version strings numerically, segment by segment (e.g.
	 * {@code "2"} &lt; {@code "2.1"} &lt; {@code "10"}), so folder/lexicographic
	 * ordering never produces the wrong execution order.
	 */
	public static int compareVersions(String left, String right) {
		String[] leftParts = left.split("\\.");
		String[] rightParts = right.split("\\.");
		int maxLength = Math.max(leftParts.length, rightParts.length);
		for (int i = 0; i < maxLength; i++) {
			int leftSegment = i < leftParts.length ? Integer.parseInt(leftParts[i]) : 0;
			int rightSegment = i < rightParts.length ? Integer.parseInt(rightParts[i]) : 0;
			int comparison = Integer.compare(leftSegment, rightSegment);
			if (comparison != 0) {
				return comparison;
			}
		}
		return 0;
	}

	/**
	 * @param sqlContent the migration file's content
	 * @return a hex-encoded SHA-256 checksum, used to detect a version's file
	 *         being edited after it already ran
	 */
	public static String computeChecksum(String sqlContent) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(sqlContent.getBytes(StandardCharsets.UTF_8));
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
	 * Minimal statement splitter: one statement per semicolon-terminated group
	 * of lines, ignoring blank lines and {@code --} comment lines. Does not
	 * handle semicolons inside string literals or stored-procedure bodies --
	 * out of scope for v1 (raw DDL/DML content only), carried over as-is from
	 * the earlier migration-poc design.
	 */
	public static List<String> splitStatements(String sql) {
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
