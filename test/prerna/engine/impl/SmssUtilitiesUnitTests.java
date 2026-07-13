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
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SmssUtilitiesUnitTests extends SemossUnitTest {

	@BeforeAll
	public static void setUp() {
		DIHelper.getInstance().setCoreProp(null);
	}

	@AfterAll
	public static void tearDown() {
		DIHelper.getInstance().setCoreProp(null);
	}

	@BeforeEach
	public void beforeEach() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());
		DIHelper.getInstance().setCoreProp(null);
	}

	@Test
	void testGetOwlFileNull() {
		// TODO: fix null input
		Properties prop = new Properties();
		File f = SmssUtilities.getOwlFile(null, prop);
		assertNull(f);
	}

	@Test
	void testGetOwlFileExists() throws IOException {
		Properties prop = new Properties();
		prop.setProperty(Constants.OWL, "loc");
		prop.setProperty(Constants.ENGINE, "eid");
		prop.setProperty(Constants.ENGINE_ALIAS, "ealias");
		prop.setProperty(Constants.OWL, "test.owl");

		Path assets = tempDir.resolve("smss").resolve("assets");
		Files.createDirectories(assets);
		Files.createFile(assets.resolve("test.owl"));

		File f = null;
		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(Utility::getBaseFolder).thenReturn(tempDir.toString());
			util.when(() -> Utility.normalizePath(any())).thenCallRealMethod();
			try (MockedStatic<EngineUtility> eutil = Mockito.mockStatic(EngineUtility.class)) {
				eutil.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, "eid",
						"ealias")).thenReturn(assets.toString());

				f = SmssUtilities.getOwlFile(assets.toAbsolutePath().toString(), prop);
			}
		}
		assertNotNull(f);
		assertTrue(f.exists());
	}

	@Test
	void testGetInsightsRDBMSFile() throws IOException {
		Properties prop = new Properties();
		prop.setProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");

		Path bf = tempDir.resolve("semoss");
		String rdbmsProperty = "@PROJECT@" + File.separator + "data";

		prop.setProperty(Constants.RDBMS_INSIGHTS, rdbmsProperty);
		prop.setProperty(Constants.PROJECT, "uuid-1010");
		prop.setProperty(Constants.PROJECT_ALIAS, "foobar");

		Path rdbms = bf.resolve("foobar__uuid-1010");
		Files.createDirectories(rdbms);
		Path file = rdbms.resolve("data.mv.db");
		Files.createFile(file);

		try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
			utilityMockedStatic.when(Utility::getBaseFolder).thenReturn(bf.toString());
			utilityMockedStatic.when(() -> Utility.normalizePath(any())).thenCallRealMethod();

			File f = SmssUtilities.getInsightsRdbmsFile(prop);

			assertNotNull(f);
			assertTrue(f.exists());
			assertTrue(f.isFile());
			assertEquals("data.mv.db", f.getName());
			Path fp = f.toPath();
			assertEquals("foobar__uuid-1010", fp.getParent().getFileName().toString());
			assertEquals("semoss", fp.getParent().getParent().getFileName().toString());
		}
	}

	@Test
	void testGetEngineProperties() throws IOException {
		Properties prop = new Properties();
		prop.setProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");

		Path bf = tempDir.resolve("semoss");
		String rdbmsProperty = "engine.props";

		prop.setProperty(Constants.ENGINE_PROPERTIES, rdbmsProperty);
		prop.setProperty(Constants.ENGINE, "uuid-1010");
		prop.setProperty(Constants.ENGINE_ALIAS, "foobar");

		Path rdbms = bf.resolve("foobar__uuid-1010");
		Files.createDirectories(rdbms);

		Path assets = rdbms.resolve("assets");
		Files.createDirectory(assets);

		Path props = assets.resolve("engine.props");
		Files.createFile(props);

		try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
			utilityMockedStatic.when(Utility::getBaseFolder).thenReturn(bf.toString());
			try (MockedStatic<EngineUtility> eu = Mockito.mockStatic(EngineUtility.class)) {
				utilityMockedStatic.when(() -> Utility.normalizePath(any())).thenCallRealMethod();
				eu.when(() -> EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, "uuid-1010",
						"foobar")).thenReturn(assets.toString());

				File f = SmssUtilities.getEngineProperties(prop);

				assertNotNull(f);
				assertTrue(f.exists());
				assertTrue(f.isFile());
				assertEquals("engine.props", f.getName());
				Path fp = f.toPath();
				assertEquals("assets", fp.getParent().getFileName().toString());
				assertEquals("foobar__uuid-1010", fp.getParent().getParent().getFileName().toString());
			}
		}
	}

	@Test
	void testGetUniqueNameEnginePropsNull() {
		Properties prop = new Properties();
		prop.setProperty(Constants.PROJECT, "uuid");
		prop.setProperty(Constants.PROJECT_ALIAS, "name");

		assertEquals("name__uuid", SmssUtilities.getUniqueName(prop));
	}

	@Test
	void testGetUniqueName() {
		Properties prop = new Properties();
		prop.setProperty(Constants.ENGINE, "engine");
		prop.setProperty(Constants.ENGINE_ALIAS, "engineName");
		prop.setProperty(Constants.PROJECT, "uuid");
		prop.setProperty(Constants.PROJECT_ALIAS, "name");

		assertEquals("engineName__engine", SmssUtilities.getUniqueName(prop));
	}

	@Test
	void testUniqueNameNameNull() {
		assertEquals("test", SmssUtilities.getUniqueName(null, "test"));
	}

	@Test
	void testGetRDFMap() throws IOException {
		Properties prop = new Properties();
		prop.setProperty(Constants.RDF_FILE_NAME, "rdfName.rdf");
		prop.setProperty(Constants.ENGINE, "eid");
		prop.setProperty(Constants.ENGINE_ALIAS, "eName");

		Path base = tempDir.resolve("Semoss");

		Path engineFolder = Paths.get(base.toAbsolutePath().toString(), "db", "eName__eid");
		Files.createDirectories(engineFolder);

		Files.createFile(engineFolder.resolve("rdfName.rdf"));

		try (MockedStatic<EngineUtility> utilityMockedStatic = Mockito.mockStatic(EngineUtility.class)) {
			utilityMockedStatic
					.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE, "eName__eid"))
					.thenReturn(engineFolder.toAbsolutePath().toString());

			File f = SmssUtilities.getRdfFile(prop);

			assertNotNull(f);
			assertTrue(f.exists());
			assertTrue(f.isFile());
			assertEquals("rdfName.rdf", f.getName());
			Path fp = f.toPath();
			assertEquals("eName__eid", fp.getParent().getFileName().toString());
			assertEquals("db", fp.getParent().getParent().getFileName().toString());
			assertEquals("Semoss", fp.getParent().getParent().getParent().getFileName().toString());
		}
	}

	@Test
	void testGetDataFile() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path engineFolder = Paths.get(base.toAbsolutePath().toString(), "db", "alias__uuid");
		Files.createDirectories(engineFolder);

		Files.createFile(engineFolder.resolve("data.txt"));

		Properties prop = new Properties();
		prop.setProperty(AbstractDatabaseEngine.DATA_FILE, "@BaseFolder@/db/@ENGINE@/data.txt");

		prop.setProperty(Constants.ENGINE, "uuid");
		prop.setProperty(Constants.ENGINE_ALIAS, "alias");

		File f = SmssUtilities.getDataFile(prop);

		assertNotNull(f);
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("data.txt", f.getName());
		Path fp = f.toPath();
		assertEquals("alias__uuid", fp.getParent().getFileName().toString());
		assertEquals("db", fp.getParent().getParent().getFileName().toString());
		assertEquals("Semoss", fp.getParent().getParent().getParent().getFileName().toString());
	}

	@Test
	void testGetTinkerFile() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path engineFolder = Paths.get(base.toAbsolutePath().toString(), "db", "alias__uuid");
		Files.createDirectories(engineFolder);

		Files.createFile(engineFolder.resolve("data.txt"));

		Properties prop = new Properties();
		prop.setProperty(Constants.TINKER_FILE, "@BaseFolder@/db/@ENGINE@/data.txt");

		prop.setProperty(Constants.ENGINE, "uuid");
		prop.setProperty(Constants.ENGINE_ALIAS, "alias");

		File f = SmssUtilities.getTinkerFile(prop);

		assertNotNull(f);
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("data.txt", f.getName());
		Path fp = f.toPath();
		assertEquals("alias__uuid", fp.getParent().getFileName().toString());
		assertEquals("db", fp.getParent().getParent().getFileName().toString());
		assertEquals("Semoss", fp.getParent().getParent().getParent().getFileName().toString());
	}

	@Test
	void testGetJanusFile() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path engineFolder = Paths.get(base.toAbsolutePath().toString(), "db", "alias__uuid");
		Files.createDirectories(engineFolder);

		Files.createFile(engineFolder.resolve("data.txt"));

		Properties prop = new Properties();
		prop.setProperty(Constants.JANUS_CONF, "@BaseFolder@/db/@ENGINE@/data.txt");

		prop.setProperty(Constants.ENGINE, "uuid");
		prop.setProperty(Constants.ENGINE_ALIAS, "alias");

		File f = SmssUtilities.getJanusFile(prop);

		assertNotNull(f);
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("data.txt", f.getName());
		Path fp = f.toPath();
		assertEquals("alias__uuid", fp.getParent().getFileName().toString());
		assertEquals("db", fp.getParent().getParent().getFileName().toString());
		assertEquals("Semoss", fp.getParent().getParent().getParent().getFileName().toString());
	}

	@Test
	void testCreateTempProjectSmss() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path project = base.resolve("project");
		Files.createDirectories(project);

		File f = SmssUtilities.createTemporaryProjectSmss("pid", "pname", IProject.PROJECT_TYPE.INSIGHTS, "github",
				"github.com", RdbmsTypeEnum.H2_DB);

		assertNotNull(f);
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("pname__pid.temp", f.getName());
		Path fp = f.toPath();
		assertEquals("project", fp.getParent().getFileName().toString());
		assertEquals("Semoss", fp.getParent().getParent().getFileName().toString());

		Properties load = new Properties();
		load.load(Files.newInputStream(f.toPath()));

		assertEquals("pid", load.getProperty(Constants.PROJECT));
		assertEquals("pname", load.getProperty(Constants.PROJECT_ALIAS));
		assertEquals("prerna.project.impl.Project", load.getProperty(Constants.PROJECT_TYPE));
		assertEquals("INSIGHTS", load.getProperty(Constants.PROJECT_ENUM_TYPE));
		assertEquals("github", load.getProperty(Constants.PROJECT_GIT_PROVIDER));
		assertEquals("github.com", load.getProperty(Constants.PROJECT_GIT_CLONE));
		assertNull(load.getProperty("PUBLIC_HOME_ENABLE"));
		assertNull(load.getProperty("PORTAL_NAME"));
		assertEquals("project/@PROJECT@/insights_database", load.getProperty(Constants.RDBMS_INSIGHTS));
		assertEquals("H2_DB", load.getProperty(Constants.RDBMS_INSIGHTS_TYPE));
		assertEquals("org.h2.Driver", load.getProperty(Constants.DRIVER));
		assertEquals("sa", load.getProperty(Constants.USERNAME));
		assertEquals("", load.getProperty(Constants.PASSWORD));
		assertEquals(
				"jdbc:h2:nio:@BaseFolder@/project/pname__pid/insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768",
				load.getProperty(Constants.CONNECTION_URL));
	}

	@Test
	void testCreateTempAssetAndWorkspace() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path user = base.resolve("user");
		Files.createDirectories(user);

		File f = SmssUtilities.createTemporaryAssetAndWorkspaceSmss("pid", "pname", false, RdbmsTypeEnum.H2_DB);

		assertNotNull(f);
		assertTrue(f.exists());
		assertTrue(f.isFile());
		assertEquals("pname__pid.temp", f.getName());
		Path fp = f.toPath();
		assertEquals("user", fp.getParent().getFileName().toString());
		assertEquals("Semoss", fp.getParent().getParent().getFileName().toString());

		Properties load = new Properties();
		load.load(Files.newInputStream(f.toPath()));

		assertEquals("pid", load.getProperty(Constants.PROJECT));
		assertEquals("pname", load.getProperty(Constants.PROJECT_ALIAS));
		assertEquals("prerna.project.impl.Project", load.getProperty(Constants.PROJECT_TYPE));
		assertEquals("false", load.getProperty(Constants.IS_ASSET_APP));
		assertEquals("project/@PROJECT@/insights_database", load.getProperty(Constants.RDBMS_INSIGHTS));
		assertEquals("H2_DB", load.getProperty(Constants.RDBMS_INSIGHTS_TYPE));
		assertEquals("org.h2.Driver", load.getProperty(Constants.DRIVER));
		assertEquals("sa", load.getProperty(Constants.USERNAME));
		assertEquals("", load.getProperty(Constants.PASSWORD));
		assertEquals(
				"jdbc:h2:nio:@BaseFolder@/project/pname__pid/insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768",
				load.getProperty(Constants.CONNECTION_URL));
	}

	@ParameterizedTest
	@NullAndEmptySource
	void testValidateProject(String projectName) {
		User u = new User();

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> SmssUtilities.validateProject(u, projectName, "id"));

		assertEquals("Need to provide a name for the project", e.getMessage());
	}

	@Test
	void testValidateProjectAlreadyContains() {
		User u = new User();
		try (MockedStatic<AbstractSecurityUtils> mockedStatic = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			mockedStatic.when(() -> AbstractSecurityUtils.containsProjectName("pname")).thenReturn(true);

			IOException e = assertThrows(IOException.class, () -> SmssUtilities.validateProject(u, "pname", "id"));
			assertEquals("Project name already exists. Please provide a unique project name", e.getMessage());
		}
	}

	@Test
	void testValidateProjectFolderExists() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path project = Paths.get(base.toAbsolutePath().toString(), "project", "pname__id");
		Files.createDirectories(project);

		User u = new User();
		try (MockedStatic<AbstractSecurityUtils> mockedStatic = Mockito.mockStatic(AbstractSecurityUtils.class);
				MockedStatic<EngineUtility> engineUtilityMockedStatic = Mockito.mockStatic(EngineUtility.class)) {
			mockedStatic.when(() -> AbstractSecurityUtils.containsProjectName("pname")).thenReturn(false);
			engineUtilityMockedStatic
					.when(() -> EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, "id", "pname"))
					.thenReturn(project.toAbsolutePath().toString());

			IOException e = assertThrows(IOException.class, () -> SmssUtilities.validateProject(u, "pname", "id"));
			assertEquals(
					"Project folder already contains a project directory with the same name. "
							+ "Please delete the existing project folder or provide a unique project name",
					e.getMessage());
		}
	}

	@Test
	void testValidateProject() throws IOException {
		Path base = tempDir.resolve("Semoss");
		Files.createDirectories(base);
		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, base.toAbsolutePath().toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		Path project = Paths.get(base.toAbsolutePath().toString(), "project");
		Files.createDirectories(project);

		User u = new User();
		try (MockedStatic<AbstractSecurityUtils> mockedStatic = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			mockedStatic.when(() -> AbstractSecurityUtils.containsProjectName("pname")).thenReturn(false);

			File f = SmssUtilities.validateProject(u, "pname", "id");

			assertFalse(f.exists());
			assertEquals("pname__id", f.getName());
		}
	}

	@Test
	void testRunInsightCreateTableQueries() throws SQLException {
		RDBMSNativeEngine engine = mock(RDBMSNativeEngine.class);
		AbstractSqlQueryUtil queryUtil = mock(AbstractSqlQueryUtil.class);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		Connection conn = mock(Connection.class);
		when(engine.getConnection()).thenReturn(conn);
		when(engine.getDatabase()).thenReturn("database");
		when(engine.getSchema()).thenReturn("schema");
		when(queryUtil.allowArrayDatatype()).thenReturn(false);

		// QUESTION ID
		String[] columnsQuestionId = new String[] { "ID", "QUESTION_NAME", "QUESTION_PERSPECTIVE", "QUESTION_LAYOUT",
				"QUESTION_ORDER", "QUESTION_DATA_MAKER", "QUESTION_MAKEUP", "DATA_TABLE_ALIGN", "HIDDEN_INSIGHT",
				"CACHEABLE", "QUESTION_PKQL" };

		// Not allowing array, so switch array to CLOB
		String[] typesQuestionId = new String[] { "VARCHAR(50)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "INT",
				"VARCHAR(255)", "CLOB", "VARCHAR(500)", "BOOLEAN", "BOOLEAN", "CLOB" };

		when(queryUtil.tableExists(conn, "QUESTION_ID", "database", "schema")).thenReturn(false);

		String createQuestionIDQuery = "create question id";
		when(queryUtil.createTable("QUESTION_ID", columnsQuestionId, typesQuestionId))
				.thenReturn(createQuestionIDQuery);

		// INSIGHT META SETUP
		when(queryUtil.tableExists(conn, "INSIGHTMETA", "database", "schema")).thenReturn(false);
		String[] insightMetaColumns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" };
		String[] insightMetaTypes = new String[] { "VARCHAR(255)", "VARCHAR(255)", "CLOB", "INT" };
		String insertInsightMeta = "create insight meta";
		when(queryUtil.createTable("INSIGHTMETA", insightMetaColumns, insightMetaTypes)).thenReturn(insertInsightMeta);

		// Skipping Legacy stuff
		when(queryUtil.tableExists(conn, "PARAMETER_ID", "database", "schema")).thenReturn(true);
		when(queryUtil.tableExists(conn, "UI", "database", "schema")).thenReturn(true);

		// RUN METHOD
		SmssUtilities.runInsightCreateTableQueries(engine);

		// verification
		verify(engine, times(1)).insertData(createQuestionIDQuery);
		verify(engine, times(1)).insertData(insertInsightMeta);

		// verify legacy stuff not added
		verify(engine, times(2)).insertData(any());

		verify(engine, times(1)).commit();
	}

	@ParameterizedTest
	@ValueSource(strings = { "\t", " ", "=" })
	void testConcealSmssSensitiveInfoKeyValueInputs(String separator) {
		String currentSmssContent = Constants.API_KEY + separator + "testkey";
		String concealed = SmssUtilities.concealSmssSensitiveInfo(currentSmssContent);

		String expected = "API_KEY" + "\t" + Constants.SENSITIVE_INFO_MASK + "\n";
		assertEquals(expected, concealed);
	}

	@Test
	void testConcealSmssSensitiveInfo_NoSensitiveKeys() {
		String curr = Constants.BASE_FOLDER + "=" + "test1" + "\n";
		curr = curr + Constants.ENGINE + "=" + "test2" + "\n";

		assertEquals(curr, SmssUtilities.concealSmssSensitiveInfo(curr));
	}

}
