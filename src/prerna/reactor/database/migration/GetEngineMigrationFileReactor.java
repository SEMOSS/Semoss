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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.migration.MigrationFile;
import prerna.engine.impl.rdbms.migration.MigrationFileUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Returns the raw SQL content of a specific migration file -- used by the
 * Migrations tab to let users view a file's SQL without editing it. READ-only;
 * does not run, modify, or record anything.
 *
 * <pre>
 * GetEngineMigrationFile(engine = ["&lt;engineId&gt;"], version = ["1"]);
 * </pre>
 *
 * Returns: the SQL content as a plain string, or throws if the file does not
 * exist on disk (e.g. FAILED or MISSING state rows whose file was removed).
 */
public class GetEngineMigrationFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetEngineMigrationFileReactor.class);

	public GetEngineMigrationFileReactor() {
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
			throw new IllegalArgumentException("Must provide a version");
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
		if (database == null) {
			throw new IllegalArgumentException("Engine " + engineId + " could not be loaded");
		}
		if (!(database instanceof IRDBMSEngine rdbmsEngine)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a JDBC database engine");
		}

		String trimmedVersion = version.trim();
		File migrationsFolder = MigrationFileUtils.getMigrationsFolder(rdbmsEngine);
		List<MigrationFile> files = MigrationFileUtils.scanMigrationsFolder(migrationsFolder);
		MigrationFile file = files.stream()
				.filter(f -> f.getVersion().equals(trimmedVersion))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"No migration file found for version " + trimmedVersion
								+ ". The file may have been removed (check the migration state in the Migrations tab)."));

		classLogger.debug("User '{}' viewing migration file for engine '{}', version '{}'.",
				user.getPrimaryLoginToken() != null ? user.getPrimaryLoginToken().getId() : "unknown",
				engineId, trimmedVersion);
		return new NounMetadata(file.getSqlContent(), PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the raw SQL content of a migration file for a given engine and version number";
	}

}
