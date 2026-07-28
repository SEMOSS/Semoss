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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IRDBMSEngine;

/**
 * Merges what's on disk ({@link MigrationFileUtils#scanMigrationsFolder}) with
 * what's recorded in {@code SEMOSS_SCHEMA_HISTORY}
 * ({@link MigrationHistoryUtils#getHistory}) into a single, display-ready
 * status list -- the same merge Flyway's {@code info} command performs.
 * Read-only; this does not run or modify anything, unlike
 * {@link SchemaMigrationRunner}.
 */
public final class MigrationStatusUtils {

	private MigrationStatusUtils() {
		// utility class
	}

	/**
	 * @param engine           the engine to report migration status for
	 * @param migrationsFolder the engine's {@code assets/.migrations} folder
	 * @return one {@link MigrationStatus} per version known either on disk or in
	 *         history, ordered by version ascending
	 */
	public static List<MigrationStatus> getStatus(IRDBMSEngine engine, File migrationsFolder) {
		List<MigrationFile> files = MigrationFileUtils.scanMigrationsFolder(migrationsFolder);
		Map<String, MigrationHistoryRecord> latestHistoryByVersion = latestRecordPerVersion(
				MigrationHistoryUtils.getHistory(engine));

		List<MigrationStatus> statuses = new ArrayList<>();
		Set<String> fileVersions = new HashSet<>();
		for (MigrationFile file : files) {
			fileVersions.add(file.getVersion());
			statuses.add(buildStatusForFile(file, latestHistoryByVersion.get(file.getVersion())));
		}
		for (MigrationHistoryRecord record : latestHistoryByVersion.values()) {
			if (!fileVersions.contains(record.getVersion())) {
				statuses.add(buildStatusForMissingFile(record));
			}
		}

		statuses.sort(Comparator.comparing(MigrationStatus::getVersion, MigrationFileUtils::compareVersions));
		return statuses;
	}

	/**
	 * {@link SchemaMigrationRunner} records one row per attempt, never
	 * overwriting a prior one -- so a version can have several rows if it was
	 * retried after a failure. Only the most recent attempt matters for
	 * display.
	 */
	private static Map<String, MigrationHistoryRecord> latestRecordPerVersion(List<MigrationHistoryRecord> history) {
		Map<String, MigrationHistoryRecord> latest = new HashMap<>();
		for (MigrationHistoryRecord record : history) {
			MigrationHistoryRecord current = latest.get(record.getVersion());
			if (current == null || record.getAppliedOn().after(current.getAppliedOn())) {
				latest.put(record.getVersion(), record);
			}
		}
		return latest;
	}

	private static MigrationStatus buildStatusForFile(MigrationFile file, MigrationHistoryRecord record) {
		if (record == null) {
			return new MigrationStatus(file.getVersion(), file.getDescription(), file.getFileName(),
					MigrationStatus.State.PENDING, null, null, 0L, null);
		}
		if (!record.isSuccess()) {
			return new MigrationStatus(file.getVersion(), file.getDescription(), file.getFileName(),
					MigrationStatus.State.FAILED, record.getAppliedBy(), record.getAppliedOn(),
					record.getExecutionTimeMs(), record.getDescription());
		}

		String currentChecksum = MigrationFileUtils.computeChecksum(file.getSqlContent());
		boolean outdated = !currentChecksum.equals(record.getChecksum());
		MigrationStatus.State state = outdated ? MigrationStatus.State.OUTDATED : MigrationStatus.State.SUCCESS;
		String note = outdated
				? "File content has changed since this version was applied -- checksum no longer matches"
				: null;
		return new MigrationStatus(file.getVersion(), file.getDescription(), file.getFileName(), state,
				record.getAppliedBy(), record.getAppliedOn(), record.getExecutionTimeMs(), note);
	}

	private static MigrationStatus buildStatusForMissingFile(MigrationHistoryRecord record) {
		return new MigrationStatus(record.getVersion(), record.getScriptName(), null, MigrationStatus.State.MISSING,
				record.getAppliedBy(), record.getAppliedOn(), record.getExecutionTimeMs(),
				"File no longer exists in the migrations folder");
	}

}
