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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.SemossUnitTest;
import prerna.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProjectHelperUnitTests extends SemossUnitTest {

	private static final Logger logger = LogManager.getLogger(ProjectHelperUnitTests.class);

	@ParameterizedTest
	@ValueSource(strings = { "insights_database.mv.db", "insights_database.trace.db", "insights_database.sqlite" })
	void testDeleteInsightsDatabase_deletesEachInsightsDatabaseFile(String fileName) throws IOException {
		String projectId = UUID.randomUUID().toString();
		Path insightsFile = createProjectFile(projectId, fileName);

		ProjectHelper.deleteInsightsDatabase(smss(projectId));

		assertFalse(Files.exists(insightsFile));
	}

	@Test
	void testDeleteInsightsDatabase_deletesEveryInsightsDatabaseFileTogether() throws IOException {
		String projectId = UUID.randomUUID().toString();
		List<Path> insightsFiles = new ArrayList<>();
		for (String fileName : ProjectHelper.INSIGHTS_DATABASE_FILES) {
			insightsFiles.add(createProjectFile(projectId, fileName));
		}

		ProjectHelper.deleteInsightsDatabase(smss(projectId));

		for (Path insightsFile : insightsFiles) {
			assertFalse(Files.exists(insightsFile));
		}
	}

	@Test
	void testDeleteInsightsDatabase_leavesTheRestOfTheProjectFolder() throws IOException {
		String projectId = UUID.randomUUID().toString();
		createProjectFile(projectId, "insights_database.mv.db");
		Path otherFile = createProjectFile(projectId, "pipeline.json");

		ProjectHelper.deleteInsightsDatabase(smss(projectId));

		assertTrue(Files.exists(otherFile));
	}

	@Test
	void testDeleteInsightsDatabase_noProjectFolderDoesNothing() {
		String projectId = UUID.randomUUID().toString();

		assertDoesNotThrow(() -> ProjectHelper.deleteInsightsDatabase(smss(projectId)));
	}

	@Test
	void testDeleteInsightsDatabase_deletesWhateverTheSmssNames() throws IOException {
		String projectId = UUID.randomUUID().toString();
		Path insightsFile = createProjectFile(projectId, "insights_database.mv.db");
		Properties smssProp = smss(projectId);
		smssProp.setProperty(Constants.RDBMS_INSIGHTS, "project/@PROJECT@/insights_database.sqlite");
		smssProp.setProperty(Constants.RDBMS_INSIGHTS_TYPE, "SQLITE");

		ProjectHelper.deleteInsightsDatabase(smssProp);

		assertFalse(Files.exists(insightsFile));
	}

	/**
	 * Creates a file in the folder of the project "pname" with the given id.
	 */
	private static Path createProjectFile(String projectId, String fileName) throws IOException {
		Path projectFolder = Files.createDirectories(projectDir.resolve("pname__" + projectId));
		return Files.createFile(projectFolder.resolve(fileName));
	}

	/**
	 * The smss of a BLOCKS project "pname" with the given id.
	 */
	private static Properties smss(String projectId) {
		Properties smssProp = new Properties();
		smssProp.setProperty(Constants.PROJECT, projectId);
		smssProp.setProperty(Constants.PROJECT_ALIAS, "pname");
		smssProp.setProperty(Constants.PROJECT_ENUM_TYPE, "BLOCKS");
		return smssProp;
	}

	@Test
	void testLoadInsightsEngineInvalidConfiguredRdbmsType() {
		Properties mainEngineProp = new Properties();
		mainEngineProp.setProperty(Constants.PROJECT, "pid");
		mainEngineProp.setProperty(Constants.PROJECT_ALIAS, "pname");
		mainEngineProp.setProperty(Constants.RDBMS_INSIGHTS_TYPE, "NOT_A_REAL_RDBMS_TYPE");

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> ProjectHelper.loadInsightsEngine(mainEngineProp, logger));

		assertTrue(e.getMessage().contains("NOT_A_REAL_RDBMS_TYPE"),
				"Error message should name the invalid configured value, was: " + e.getMessage());
		assertTrue(e.getMessage().contains("H2_DB"),
				"Error message should list the valid RdbmsTypeEnum constants, was: " + e.getMessage());
	}
}
