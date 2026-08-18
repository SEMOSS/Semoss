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
import java.nio.file.Files;
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
import prerna.engine.impl.rdbms.migration.MigrationFileUtils;
import prerna.engine.impl.rdbms.migration.MigrationHistoryUtils;
import prerna.engine.impl.rdbms.migration.MigrationStatus;
import prerna.engine.impl.rdbms.migration.MigrationStatusUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Removes all {@code SEMOSS_SCHEMA_HISTORY} rows for a given version, clearing
 * it from the Migrations tab. Allowed for {@code MISSING} and {@code FAILED}
 * versions -- both represent cases where no schema change was permanently
 * applied. For {@code FAILED} versions whose SQL file still exists on disk, the
 * file is also deleted so the version can be rewritten from scratch.
 * Dismissing a {@code SUCCESS} or {@code OUTDATED} version is rejected because
 * the history row is the only durable record that those DDL changes ever ran.
 *
 * <pre>
 * DismissEngineMigrationRecord(engine = ["&lt;engineId&gt;"], version = ["1"]);
 * </pre>
 *
 * Returns: {@code true} on success.
 */
public class DismissEngineMigrationRecordReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DismissEngineMigrationRecordReactor.class);

	public DismissEngineMigrationRecordReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.VERSION.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String rawEngineId = this.keyValue.get(this.keysToGet[0]);
		String version = this.keyValue.get(this.keysToGet[1]);
		if (rawEngineId == null || rawEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engine id");
		}
		if (version == null || version.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a version to dismiss");
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

		String trimmedVersion = version.trim();
		File migrationsFolder = MigrationFileUtils.getMigrationsFolder(rdbmsEngine);
		List<MigrationStatus> statuses = MigrationStatusUtils.getStatus(rdbmsEngine, migrationsFolder);
		MigrationStatus target = statuses.stream()
				.filter(s -> s.getVersion().equals(trimmedVersion))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No migration record found for version " + trimmedVersion));

		MigrationStatus.State state = target.getState();
		boolean isDismissable = state == MigrationStatus.State.MISSING
				|| state == MigrationStatus.State.FAILED;
		if (!isDismissable) {
			throw new IllegalArgumentException(
					"Migration version " + trimmedVersion + " is in state " + state
							+ " and cannot be dismissed. Only MISSING and FAILED versions are eligible for dismissal.");
		}

		// For FAILED rows that still have a file on disk, delete the file so the
		// version is completely reset and does not reappear as PENDING after dismiss.
		if (state == MigrationStatus.State.FAILED && target.getFileName() != null) {
			try {
				Files.deleteIfExists(migrationsFolder.toPath().resolve(target.getFileName()));
			} catch (IOException e) {
				classLogger.warn("Could not delete migration file '{}' for version '{}' during dismiss.",
						target.getFileName(), trimmedVersion, e);
			}
		}

		classLogger.info("User '{}' dismissing migration history for engine '{}', version '{}'.",
				user.getPrimaryLoginToken() != null ? user.getPrimaryLoginToken().getId() : "unknown",
				engineId, trimmedVersion);
		MigrationHistoryUtils.deleteHistoryForVersion(rdbmsEngine, trimmedVersion);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "Removes a MISSING or FAILED migration history record from an engine's schema history table";
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// permanently deletes audit history -- must never be agent-auto-triggerable
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

}
