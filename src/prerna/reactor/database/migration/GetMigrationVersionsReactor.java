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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityMigrationUtils;
import prerna.engine.impl.migration.MigrationDefinition;
import prerna.engine.impl.migration.MigrationRecord;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Full version history for a single migration, newest first — backs the
 * Migrations tab's expandable per-row history view (and supplies the content
 * "Restore" pre-fills the editor with).
 *
 * <pre>GetMigrationVersions(migrationId = ["&lt;migrationId&gt;"]);</pre>
 */
public class GetMigrationVersionsReactor extends AbstractReactor {

	public GetMigrationVersionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MIGRATION_ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String migrationId = this.keyValue.get(this.keysToGet[0]);
		if (migrationId == null || migrationId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a migrationId to get version history for");
		}

		List<MigrationDefinition> versions = SecurityMigrationUtils.getMigrationVersions(migrationId);
		if (versions.isEmpty()) {
			throw new IllegalArgumentException("No migration found with id " + migrationId);
		}

		User user = this.insight.getUser();
		String engineId = versions.get(0).getEngineId();
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to view it");
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		for (MigrationDefinition definition : versions) {
			MigrationRecord runResult = SecurityMigrationUtils.getRunResult(definition.getMigrationId(),
					definition.getVersion());
			Map<String, Object> row = new HashMap<>();
			row.put("migrationId", definition.getMigrationId());
			row.put("version", definition.getVersion());
			row.put("scriptName", definition.getScriptName());
			row.put("sqlContent", definition.getSqlContent());
			row.put("isLatest", definition.isLatest());
			row.put("createdBy", definition.getCreatedBy());
			row.put("createdOn", definition.getCreatedOn());
			row.put("notes", definition.getNotes());
			row.put("lastRunSuccess", runResult == null ? null : runResult.isSuccess());
			row.put("lastRunOn", runResult == null ? null : runResult.getAppliedOn());
			row.put("lastRunError", runResult == null ? null : runResult.getErrorMessage());
			rows.add(row);
		}

		return new NounMetadata(rows, PixelDataType.VECTOR);
	}

}
