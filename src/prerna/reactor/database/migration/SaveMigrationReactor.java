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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityMigrationUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.migration.MigrationDefinition;
import prerna.engine.impl.migration.MigrationOwlSyncUtility;
import prerna.engine.impl.migration.MigrationRecord;
import prerna.engine.impl.migration.SchemaMigrationRunner;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;

/**
 * The primary entry point for the Migrations UI. Creates a new version of a
 * migration (brand new if {@code migrationId} is absent, otherwise a new
 * version of an existing one — including "restore," which is just a save
 * whose content happens to match an old version) and immediately runs it
 * against the target engine. Save and run are not separate steps by design.
 *
 * <pre>
 * SaveMigration(map = [{
 *     "engine": "&lt;engineId&gt;",
 *     "sqlContent": "ALTER TABLE ...;",
 *     "scriptName": "add_status_column",
 *     "notes": "optional free text",
 *     "migrationId": "&lt;existing-id, omit for a brand new migration&gt;"
 * }]);
 * </pre>
 */
public class SaveMigrationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveMigrationReactor.class);

	public SaveMigrationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> details = getMigrationDetails();

		String rawEngineId = (String) details.get("engine");
		if (rawEngineId == null || rawEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engine id to save a migration against");
		}
		String sqlContent = (String) details.get("sqlContent");
		if (sqlContent == null || sqlContent.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide SQL content for the migration");
		}
		String scriptName = (String) details.get("scriptName");
		String notes = (String) details.get("notes");
		String migrationId = (String) details.get("migrationId");

		User user = this.insight.getUser();
		String engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, rawEngineId);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit it");
		}

		String userId = user.getPrimaryLoginToken().getId();
		MigrationDefinition definition = SecurityMigrationUtils.saveNewVersion(engineId, migrationId, scriptName,
				sqlContent, notes, userId);

		mirrorToAssetFolder(engineId, definition);

		MigrationRecord result = SchemaMigrationRunner.executeOne(engineId, definition.getMigrationId(),
				definition.getVersion(), scriptName, sqlContent, userId);

		// reconcile SEMOSS's OWL metamodel with the real JDBC schema this migration
		// just changed -- only meaningful (and only attempted) if the DDL actually
		// ran; a failed/rolled-back migration didn't change the physical schema
		boolean metadataSynced = false;
		if (result.isSuccess()) {
			metadataSynced = MigrationOwlSyncUtility.syncOwlMetadata(this.insight, engineId);
			if (!metadataSynced) {
				classLogger.warn(
						"Migration {} version {} ran successfully against engine {}, but syncing the OWL "
								+ "metamodel afterward failed -- the Metadata tab may be stale until a manual sync",
						definition.getMigrationId(), definition.getVersion(), engineId);
			}
		}

		Map<String, Object> response = new HashMap<>();
		response.put("migrationId", definition.getMigrationId());
		response.put("version", definition.getVersion());
		response.put("success", result.isSuccess());
		response.put("errorMessage", result.getErrorMessage());
		response.put("executionTimeMs", result.getExecutionTimeMs());
		response.put("metadataSynced", metadataSynced);
		return new NounMetadata(response, PixelDataType.MAP);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMigrationDetails() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MAP.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}
		if (this.curRow != null) {
			List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}
		throw new IllegalArgumentException(
				"Must provide migration details (engine, sqlContent, scriptName) as a map");
	}

	/**
	 * Best-effort write-out of this version's SQL to the engine's own asset
	 * folder — never blocks or fails the save/run. The Security DB is the sole
	 * source of truth (nothing reads these files back in); this purely restores
	 * export-portability, since the file then travels with
	 * {@code ExportEngineReactor}'s wholesale folder zip while the Security DB
	 * row does not.
	 */
	private void mirrorToAssetFolder(String engineId, MigrationDefinition definition) {
		try {
			IEngine.CATALOG_TYPE type = SecurityEngineUtils.getEngineType(engineId);
			String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
			String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(type, engineId, engineName);
			Path migrationsDir = Paths.get(assetsFolder, "migrations");
			Files.createDirectories(migrationsDir);
			String fileName = definition.getMigrationId() + "_V" + definition.getVersion() + "__"
					+ sanitizeFileName(definition.getScriptName()) + ".sql";
			Files.writeString(migrationsDir.resolve(fileName), definition.getSqlContent(), StandardCharsets.UTF_8);
		} catch (Exception e) {
			classLogger.warn(
					"Failed to mirror migration {} version {} to the engine's asset folder; continuing since "
							+ "the Security DB is the source of truth",
					definition.getMigrationId(), definition.getVersion(), e);
		}
	}

	private String sanitizeFileName(String name) {
		if (name == null || name.trim().isEmpty()) {
			return "migration";
		}
		return name.trim().replaceAll("[^a-zA-Z0-9-_]", "_");
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// this saves AND immediately runs arbitrary DDL/DML — must never be
		// agent-auto-triggerable
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

}
