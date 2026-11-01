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
package prerna.project.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import prerna.SemossUnitTest;
import prerna.util.Constants;

class ProjectHelperUnitTests extends SemossUnitTest {

	@Test
	void testDeleteInsightsDatabase_deletesTheH2File() throws IOException {
		String projectId = UUID.randomUUID().toString();
		Path insightsFile = createProjectFile(projectId, "insights_database.mv.db");

		ProjectHelper.deleteInsightsDatabase(smss(projectId, "project/@PROJECT@/insights_database", "H2_DB"));

		assertFalse(Files.exists(insightsFile));
	}

	@Test
	void testDeleteInsightsDatabase_deletesTheSqliteFile() throws IOException {
		String projectId = UUID.randomUUID().toString();
		Path insightsFile = createProjectFile(projectId, "insights_database.sqlite");

		ProjectHelper.deleteInsightsDatabase(smss(projectId, "project/@PROJECT@/insights_database.sqlite", "SQLITE"));

		assertFalse(Files.exists(insightsFile));
	}

	@Test
	void testDeleteInsightsDatabase_leavesTheRestOfTheProjectFolder() throws IOException {
		String projectId = UUID.randomUUID().toString();
		createProjectFile(projectId, "insights_database.mv.db");
		Path otherFile = createProjectFile(projectId, "pipeline.json");

		ProjectHelper.deleteInsightsDatabase(smss(projectId, "project/@PROJECT@/insights_database", "H2_DB"));

		assertTrue(Files.exists(otherFile));
	}

	@Test
	void testDeleteInsightsDatabase_noFileDoesNothing() {
		String projectId = UUID.randomUUID().toString();

		assertDoesNotThrow(() -> ProjectHelper
				.deleteInsightsDatabase(smss(projectId, "project/@PROJECT@/insights_database", "H2_DB")));
	}

	@Test
	void testDeleteInsightsDatabase_smssNamingNoneDoesNothing() {
		Properties smssProp = new Properties();
		smssProp.setProperty(Constants.PROJECT, UUID.randomUUID().toString());
		smssProp.setProperty(Constants.PROJECT_ALIAS, "pname");

		assertDoesNotThrow(() -> ProjectHelper.deleteInsightsDatabase(smssProp));
	}

	@Test
	void testDeleteInsightsDatabase_unknownDatabaseTypeIsLogged() {
		String projectId = UUID.randomUUID().toString();

		assertDoesNotThrow(() -> ProjectHelper
				.deleteInsightsDatabase(smss(projectId, "project/@PROJECT@/insights_database", "NOT_A_TYPE")));
	}

	/**
	 * Creates a file in the folder of the project "pname" with the given id.
	 */
	private static Path createProjectFile(String projectId, String fileName) throws IOException {
		Path projectFolder = Files.createDirectories(projectDir.resolve("pname__" + projectId));
		return Files.createFile(projectFolder.resolve(fileName));
	}

	/**
	 * The smss of a BLOCKS project "pname" that still names an insights database.
	 */
	private static Properties smss(String projectId, String rdbmsInsights, String rdbmsInsightsType) {
		Properties smssProp = new Properties();
		smssProp.setProperty(Constants.PROJECT, projectId);
		smssProp.setProperty(Constants.PROJECT_ALIAS, "pname");
		smssProp.setProperty(Constants.PROJECT_ENUM_TYPE, "BLOCKS");
		smssProp.setProperty(Constants.RDBMS_INSIGHTS, rdbmsInsights);
		smssProp.setProperty(Constants.RDBMS_INSIGHTS_TYPE, rdbmsInsightsType);
		return smssProp;
	}
}
