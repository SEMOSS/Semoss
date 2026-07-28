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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.migration.MigrationFileUtils;
import prerna.engine.impl.rdbms.migration.MigrationStatus;
import prerna.engine.impl.rdbms.migration.MigrationStatusUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Read-only status board backing the Migrations tab -- merges the
 * {@code V<version>__<description>.sql} files on disk with
 * {@code SEMOSS_SCHEMA_HISTORY} run outcomes into one row per version, the
 * same merge Flyway's {@code info} command performs. Does not run, create, or
 * modify any migration; {@code SchemaMigrationRunner} (called automatically
 * from {@code IRDBMSEngine.open()}) is the only thing that does that.
 *
 * <pre>ListEngineMigrations(engine = ["&lt;engineId&gt;"]);</pre>
 *
 * Returns: VECTOR of maps, one per version, ordered ascending, each
 * containing {@code version}, {@code description}, {@code fileName},
 * {@code state} (PENDING / SUCCESS / FAILED / MISSING / OUTDATED),
 * {@code appliedBy}, {@code appliedOn}, {@code executionTimeMs}, and
 * {@code errorMessage}.
 */
public class ListEngineMigrationsReactor extends AbstractReactor {

	public ListEngineMigrationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String rawEngineId = this.keyValue.get(this.keysToGet[0]);
		if (rawEngineId == null || rawEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engine id to list migrations for");
		}

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in");
		}
		String engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, rawEngineId);
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to view it");
		}

		IDatabaseEngine database = Utility.getDatabase(engineId);
		if (!(database instanceof IRDBMSEngine rdbmsEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a JDBC database engine");
		}

		File migrationsFolder = MigrationFileUtils.getMigrationsFolder(rdbmsEngine);
		List<MigrationStatus> statuses = MigrationStatusUtils.getStatus(rdbmsEngine, migrationsFolder);

		List<Map<String, Object>> rows = new ArrayList<>();
		for (MigrationStatus status : statuses) {
			Map<String, Object> row = new HashMap<>();
			row.put("version", status.getVersion());
			row.put("description", status.getDescription());
			row.put("fileName", status.getFileName());
			row.put("state", status.getState().name());
			row.put("appliedBy", status.getAppliedBy());
			row.put("appliedOn", status.getAppliedOn());
			row.put("executionTimeMs", status.getExecutionTimeMs());
			row.put("errorMessage", status.getErrorMessage());
			rows.add(row);
		}

		return new NounMetadata(rows, PixelDataType.VECTOR);
	}

}
