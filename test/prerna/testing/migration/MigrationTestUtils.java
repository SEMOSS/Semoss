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
package prerna.testing.migration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.database.migration.GetMigrationVersionsReactor;
import prerna.reactor.database.migration.ListMigrationsReactor;
import prerna.reactor.database.migration.SaveMigrationReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class MigrationTestUtils {

	private static final Logger classLogger = LogManager.getLogger(MigrationTestUtils.class);

	@SuppressWarnings("unchecked")
	public static Map<String, Object> saveMigration(String engineId, String migrationId, String sqlContent,
			String scriptName, String notes) {
		NounMetadata nm = runSaveMigration(engineId, migrationId, sqlContent, scriptName, notes);
		return (Map<String, Object>) nm.getValue();
	}

	public static String saveMigrationExpectError(String engineId, String migrationId, String sqlContent,
			String scriptName, String notes) {
		NounMetadata nm = runSaveMigrationRaw(engineId, migrationId, sqlContent, scriptName, notes);
		return (String) nm.getValue();
	}

	private static NounMetadata runSaveMigration(String engineId, String migrationId, String sqlContent,
			String scriptName, String notes) {
		String pixel = buildSaveMigrationPixel(engineId, migrationId, sqlContent, scriptName, notes);
		classLogger.debug(pixel);
		return ApiSemossTestUtils.processPixel(pixel);
	}

	private static NounMetadata runSaveMigrationRaw(String engineId, String migrationId, String sqlContent,
			String scriptName, String notes) {
		String pixel = buildSaveMigrationPixel(engineId, migrationId, sqlContent, scriptName, notes);
		classLogger.debug(pixel);
		return ApiSemossTestUtils.processRawPixel(pixel);
	}

	private static String buildSaveMigrationPixel(String engineId, String migrationId, String sqlContent,
			String scriptName, String notes) {
		Map<String, Object> details = new HashMap<>();
		details.put("engine", engineId);
		details.put("sqlContent", sqlContent);
		details.put("scriptName", scriptName);
		details.put("notes", notes);
		if (migrationId != null) {
			details.put("migrationId", migrationId);
		}
		return ApiSemossTestUtils.buildPixelCall(SaveMigrationReactor.class, "map", details);
	}

	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> listMigrations(String engineId) {
		String pixel = ApiSemossTestUtils.buildPixelCall(ListMigrationsReactor.class, "engine", engineId);
		classLogger.debug(pixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		return (List<Map<String, Object>>) nm.getValue();
	}

	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getMigrationVersions(String migrationId) {
		String pixel = ApiSemossTestUtils.buildPixelCall(GetMigrationVersionsReactor.class, "migrationId",
				migrationId);
		classLogger.debug(pixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		return (List<Map<String, Object>>) nm.getValue();
	}

	public static String getMigrationVersionsExpectError(String migrationId) {
		String pixel = ApiSemossTestUtils.buildPixelCall(GetMigrationVersionsReactor.class, "migrationId",
				migrationId);
		classLogger.debug(pixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(pixel);
		return (String) nm.getValue();
	}

}
