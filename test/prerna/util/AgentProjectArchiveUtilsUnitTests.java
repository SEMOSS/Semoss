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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class AgentProjectArchiveUtilsUnitTests {

	private static final String ID = "agent-id";
	private static final String NAME = "Test Agent";
	private static final String CONFIG = """
			{"system_prompt":"Full prompt", "model_id":"model-id", "budgets":{"max_turns":19},
			 "hooks":[{"kind":"git_commit"}], "skills":[{"skill_id":"python","pinned_version":"v2"}],
			 "subagents":[{"workspaceId":"reviewer-id"}], "greeting":"Hello", "future_field":{"enabled":true}}
			""";

	@TempDir
	Path directory;
	Connection connection;
	MockedStatic<SystemEngineRegistry> registry;
	User user;

	@BeforeEach
	void setup() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:" + UUID.randomUUID());
		try (var statement = connection.createStatement()) {
			statement.execute("CREATE TABLE WORKSPACE (WORKSPACE_ID VARCHAR(255) PRIMARY KEY, OWNER VARCHAR(255), "
					+ "NAME VARCHAR(255), DESCRIPTION CLOB, SYSTEM_PROMPT CLOB, CONFIG_JSON CLOB, IS_ACTIVE BOOLEAN, "
					+ "DATE_CREATED TIMESTAMP, DATE_UPDATED TIMESTAMP)");
			statement.execute("CREATE TABLE WORKSPACE_RESOURCE (WORKSPACE_RESOURCE_ID VARCHAR(255) PRIMARY KEY, "
					+ "WORKSPACE_ID VARCHAR(255), RESOURCE_ID VARCHAR(255), RESOURCE_TYPE VARCHAR(255) "
					+ "CHECK (RESOURCE_TYPE <> 'FAIL'), RESOURCE_SUBTYPE VARCHAR(255))");
			statement.execute("CREATE TABLE ROOM (WORKSPACE_ID VARCHAR(255))");
		}
		IRDBMSEngine engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(() -> SystemEngineRegistry.requireDatabase(anyString(), anyString())).thenCallRealMethod();
		registry.when(SystemEngineRegistry::getModelInferenceLogsDb).thenReturn(engine);
		registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(true);
		user = mock(User.class);
		AccessToken token = new AccessToken();
		token.setId("importing-user");
		when(user.getPrimaryLoginToken()).thenReturn(token);
	}

	@AfterEach
	void cleanup() throws Exception {
		if (registry != null) {
			registry.close();
		}
		if (connection != null) {
			connection.close();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void bothArchiveLayoutsRoundTripAgentDefinitionToDatabase(boolean fullProject) throws Exception {
		JsonObject metadata = exportAndRead(fullProject);
		assertFalse(metadata.getAsJsonObject("workspace").has("owner"));
		assertFalse(metadata.getAsJsonObject("workspace").has("date_created"));
		AgentProjectArchiveUtils.importAgent(ID, user, metadata, false);
		try (var statement = connection.createStatement();
				ResultSet row = statement.executeQuery("SELECT * FROM WORKSPACE")) {
			assertTrue(row.next());
			assertEquals(ID, row.getString("WORKSPACE_ID"));
			assertEquals(NAME, row.getString("NAME"));
			assertEquals("importing-user", row.getString("OWNER"));
			assertEquals("Description", row.getString("DESCRIPTION"));
			assertEquals("Prompt with caf\u00e9\nSecond line", row.getString("SYSTEM_PROMPT"));
			assertEquals(JsonParser.parseString(CONFIG), JsonParser.parseString(row.getString("CONFIG_JSON")));
			assertFalse(row.getBoolean("IS_ACTIVE"));
			assertNotNull(row.getTimestamp("DATE_CREATED"));
			assertFalse(row.next());
		}
		assertEquals(4, count("WORKSPACE_RESOURCE"));
		try (var statement = connection.createStatement();
				ResultSet rows = statement.executeQuery("SELECT * FROM WORKSPACE_RESOURCE")) {
			while (rows.next()) {
				assertEquals(ID, rows.getString("WORKSPACE_ID"));
				assertDoesNotThrow(() -> UUID.fromString(rows.getString("WORKSPACE_RESOURCE_ID")));
				if ("SKILL".equals(rows.getString("RESOURCE_TYPE"))) {
					assertEquals("v2", rows.getString("RESOURCE_SUBTYPE"));
				}
			}
		}
		assertTrue(connection.getAutoCommit());
	}

	@Test
	void replacementClearsOldConfigAndResourcesAndPreservesOwnershipAndRoomLinks() throws Exception {
		JsonObject metadata = exportAndRead(false);
		AgentProjectArchiveUtils.importAgent(ID, user, metadata, false);
		Timestamp created;
		try (var statement = connection.createStatement();
				var row = statement.executeQuery("SELECT DATE_CREATED FROM WORKSPACE")) {
			row.next();
			created = row.getTimestamp(1);
		}
		try (var statement = connection.createStatement()) {
			statement.execute("INSERT INTO ROOM VALUES ('agent-id')");
		}
		JsonObject workspace = metadata.getAsJsonObject("workspace");
		workspace.addProperty("name", "Replacement");
		workspace.addProperty("is_active", true);
		workspace.add("config_json", null);
		metadata.add("resources", new JsonArray());
		AccessToken otherOwner = new AccessToken();
		otherOwner.setId("replacing-user");
		when(user.getPrimaryLoginToken()).thenReturn(otherOwner);
		AgentProjectArchiveUtils.importAgent(ID, user, metadata, true);
		try (var statement = connection.createStatement();
				var row = statement.executeQuery("SELECT * FROM WORKSPACE")) {
			row.next();
			assertEquals("Replacement", row.getString("NAME"));
			assertEquals("importing-user", row.getString("OWNER"));
			assertEquals(created, row.getTimestamp("DATE_CREATED"));
			assertTrue(row.getBoolean("IS_ACTIVE"));
			assertNull(row.getString("CONFIG_JSON"));
		}
		assertEquals(0, count("WORKSPACE_RESOURCE"));
		assertEquals(1, count("ROOM"));
	}

	@ParameterizedTest
	@ValueSource(booleans = { true, false })
	void databaseFailureRollsBackDefinitionAndResources(boolean replace) throws Exception {
		JsonObject metadata = exportAndRead(false);
		if (replace) {
			AgentProjectArchiveUtils.importAgent(ID, user, metadata, false);
		}
		metadata.getAsJsonObject("workspace").addProperty("name", "Must not persist");
		metadata.getAsJsonArray("resources").get(0).getAsJsonObject().addProperty("resource_type", "FAIL");
		connection.setAutoCommit(false);
		assertThrows(SQLException.class, () -> AgentProjectArchiveUtils.importAgent(ID, user, metadata, replace));
		assertFalse(connection.getAutoCommit());
		assertEquals(replace ? 1 : 0, count("WORKSPACE"));
		assertEquals(replace ? 4 : 0, count("WORKSPACE_RESOURCE"));
		if (replace) {
			try (var statement = connection.createStatement();
					var row = statement.executeQuery("SELECT NAME FROM WORKSPACE")) {
				row.next();
				assertEquals(NAME, row.getString(1));
			}
		}
	}

	@Test
	void createCannotOverwriteExistingWorkspace() throws Exception {
		JsonObject metadata = exportAndRead(false);
		AgentProjectArchiveUtils.importAgent(ID, user, metadata, false);
		assertThrows(SQLException.class, () -> AgentProjectArchiveUtils.importAgent(ID, user, metadata, false));
		assertEquals(1, count("WORKSPACE"));
		assertEquals(4, count("WORKSPACE_RESOURCE"));
	}

	@Test
	void replaceCanCreateMissingWorkspace() throws Exception {
		AgentProjectArchiveUtils.importAgent(ID, user, exportAndRead(false), true);
		assertEquals(1, count("WORKSPACE"));
	}

	@Test
	void legacyArchivesAndNonAgentExportsDoNotRequireDatabase() throws Exception {
		registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(false);
		assertNull(AgentProjectArchiveUtils.readAgent(directory.toFile(), properties()));
		AgentProjectArchiveUtils.importAgent(ID, user, null, false);
		IProject project = mock(IProject.class);
		when(project.getProjectType()).thenReturn(IProject.PROJECT_TYPE.CODE);
		try (ZipOutputStream zip = new ZipOutputStream(new ByteArrayOutputStream())) {
			AgentProjectArchiveUtils.exportAgent(zip, project, null);
		}
		registry.verifyNoInteractions();
	}

	@ParameterizedTest
	@ValueSource(strings = { "version", "identity", "resources", "resource_id", "config", "active", "name",
			"project_type", "malformed" })
	void rejectsInvalidMetadataBeforeImport(String invalidField) throws Exception {
		JsonObject metadata = exportAndRead(false);
		Properties properties = properties();
		switch (invalidField) {
		case "version" -> metadata.addProperty("schema_version", 2);
		case "identity" -> metadata.getAsJsonObject("workspace").addProperty("workspace_id", "other-agent");
		case "resources" -> metadata.remove("resources");
		case "resource_id" -> metadata.getAsJsonArray("resources").get(0).getAsJsonObject().remove("resource_id");
		case "config" -> metadata.getAsJsonObject("workspace").addProperty("config_json", "not an object");
		case "active" -> metadata.getAsJsonObject("workspace").addProperty("is_active", "false");
		case "name" -> metadata.getAsJsonObject("workspace").addProperty("name", "");
		case "project_type" -> properties.setProperty(Constants.PROJECT_ENUM_TYPE, "CODE");
		}
		Files.writeString(directory.resolve(NAME + AgentProjectArchiveUtils.FILE_SUFFIX),
				invalidField.equals("malformed") ? "{broken" : metadata.toString());
		assertThrows(IllegalArgumentException.class,
				() -> AgentProjectArchiveUtils.readAgent(directory.toFile(), properties));
		assertEquals(0, count("WORKSPACE"));
	}

	@Test
	void agentImportRequiresInferenceDatabase() throws Exception {
		exportAndRead(false);
		registry.when(SystemEngineRegistry::isModelInferenceLogsDbLoaded).thenReturn(false);
		assertThrows(IllegalArgumentException.class,
				() -> AgentProjectArchiveUtils.readAgent(directory.toFile(), properties()));
	}

	@Test
	void restoresDependenciesWithExportedTypesIncludingPlatformSkills() throws Exception {
		Path file = directory.resolve(NAME + IProject.DEPENDENCIES_FILE_SUFFIX);
		Files.writeString(file, """
				[{"engine_id":"python","engine_type":"PROJECT"}, {"engine_id":"tool","engine_type":"FUNCTION"}]
				""");
		try (var projects = mockStatic(SecurityProjectUtils.class)) {
			AgentProjectArchiveUtils.importDependencies(directory.toFile(), NAME, ID, user);
			projects.verify(() -> SecurityProjectUtils.updateProjectDependencies(user, ID,
					List.of(Map.of("ENGINEID", "python", "ENGINETYPE", "PROJECT"),
							Map.of("ENGINEID", "tool", "ENGINETYPE", "FUNCTION"))));
		}
		assertFalse(Files.exists(file));
	}

	private JsonObject exportAndRead(boolean fullProject) throws Exception {
		IProject project = mock(IProject.class);
		when(project.getProjectType()).thenReturn(IProject.PROJECT_TYPE.WORKSPACE);
		when(project.getProjectId()).thenReturn(ID);
		when(project.getProjectName()).thenReturn(NAME);
		Map<String, Object> workspace = new HashMap<>(Map.of("name", NAME, "description", "Description",
				"system_prompt", "Prompt with caf\u00e9\nSecond line", "is_active", false, "config_json", CONFIG,
				"owner", "source-owner", "date_created", "2020-01-01"));
		List<Map<String, Object>> resources = List.of(
				Map.of("resource_id", "mcp", "resource_type", "PROJECT", "resource_subtype", "CODE"),
				Map.of("resource_id", "python", "resource_type", "SKILL", "resource_subtype", "v2"),
				Map.of("resource_id", "prompt", "resource_type", "PROMPT"),
				Map.of("resource_id", "tool", "resource_type", "FUNCTION"));
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (var source = mockStatic(ModelInferenceLogsUtils.class); ZipOutputStream zip = new ZipOutputStream(bytes)) {
			source.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(ID)).thenReturn(workspace);
			source.when(() -> ModelInferenceLogsUtils.getWorkspaceResourcesByType(ID, null)).thenReturn(resources);
			AgentProjectArchiveUtils.exportAgent(zip, project, fullProject ? NAME + "__" + ID : null);
		}
		Path extracted;
		try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			var entry = zip.getNextEntry();
			String expected = (fullProject ? NAME + "__" + ID + "/" : "") + NAME + AgentProjectArchiveUtils.FILE_SUFFIX;
			assertEquals(expected, entry.getName());
			extracted = directory.resolve(entry.getName());
			Files.createDirectories(extracted.getParent());
			Files.writeString(extracted, new String(zip.readAllBytes(), StandardCharsets.UTF_8));
			assertNull(zip.getNextEntry());
		}
		return AgentProjectArchiveUtils.readAgent(extracted.getParent().toFile(), properties());
	}

	private Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(Constants.PROJECT, ID);
		properties.setProperty(Constants.PROJECT_ALIAS, NAME);
		properties.setProperty(Constants.PROJECT_ENUM_TYPE, "WORKSPACE");
		return properties;
	}

	private int count(String table) throws SQLException {
		try (var statement = connection.createStatement();
				var rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
			rows.next();
			return rows.getInt(1);
		}
	}
}
