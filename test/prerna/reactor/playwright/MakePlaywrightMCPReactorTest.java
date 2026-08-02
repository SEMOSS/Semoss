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
package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.SemossUnitTest;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

class MakePlaywrightMCPReactorTest extends SemossUnitTest {

	private MakePlaywrightMCPReactor reactor;
	private Map<String, String> keyValues;
	private NounStore nounStore;
	private Insight insight;
	private User user;

	@BeforeEach
	void setUp() throws IOException {
		if (Files.exists(tempDir)) {
			FileUtils.cleanDirectory(tempDir.toFile());
		}

		reactor = new MakePlaywrightMCPReactor();
		keyValues = reactor.keyValue;
		nounStore = new NounStore("test");
		insight = mock(Insight.class);
		user = mock(User.class);

		reactor.setNounStore(nounStore);
		reactor.setInsight(insight);

		when(insight.getUser()).thenReturn(user);
		when(user.isAnonymous()).thenReturn(false);
	}

	@Test
	void executeThrowsExceptionWhenAnonymousUser() {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		when(user.isAnonymous()).thenReturn(true);

		try (MockedStatic<AbstractSecurityUtils> securityUtils = Mockito.mockStatic(AbstractSecurityUtils.class)) {
			securityUtils.when(AbstractSecurityUtils::anonymousUsersEnabled).thenReturn(true);

			assertThrows(SemossPixelException.class, () -> {
				reactor.execute();
			});
		}
	}

	@Test
	void executeThrowsExceptionWhenUserCannotEditProject() {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		try (MockedStatic<SecurityProjectUtils> securityUtils = Mockito.mockStatic(SecurityProjectUtils.class)) {
			securityUtils.when(() -> SecurityProjectUtils.userCanEditProject(user, projectId)).thenReturn(false);

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
				reactor.execute();
			});

			assertTrue(exception.getMessage().contains("does not exist or user does not have access to edit"));
		}
	}

	@Test
	void executeThrowsExceptionWhenNoRecordingFilesFound() throws IOException {
		String projectId = "test-project";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);

		IProject project = mock(IProject.class);

		try (MockedStatic<SecurityProjectUtils> securityUtils = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> utilityMock = Mockito.mockStatic(Utility.class);
				MockedStatic<PlaywrightUtility> playwrightUtility = Mockito.mockStatic(PlaywrightUtility.class);
				MockedStatic<AssetUtility> assetUtility = Mockito.mockStatic(AssetUtility.class)) {

			securityUtils.when(() -> SecurityProjectUtils.userCanEditProject(user, projectId)).thenReturn(true);
			utilityMock.when(() -> Utility.getProject(projectId)).thenReturn(project);
			playwrightUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);
			assetUtility.when(() -> AssetUtility.getProjectAssetsFolder(projectId))
					.thenReturn(tempDir.resolve("assets").toString());

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
				reactor.execute();
			});

			assertTrue(exception.getMessage().contains("No Playwright recording files found"));
		}
	}

	@Test
	void executeCreatesPixelMcpJsonFile() throws IOException {
		String projectId = "test-project";
		String projectName = "test-project-name";
		String comment = "Test comment";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);
		keyValues.put(ReactorKeysEnum.COMMENT_KEY.getKey(), comment);

		// Setup directories
		Path recordingsDir = tempDir.resolve("recordings");
		Files.createDirectories(recordingsDir);

		Path assetsDir = tempDir.resolve("assets");
		Files.createDirectories(assetsDir);
		Path mcpDir = assetsDir.resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"),
				"{\"tools\":[{\"name\":\"unrelated_tool\",\"description\":\"keep me\"}]}");

		Path versionDir = tempDir.resolve("version");
		Files.createDirectories(versionDir);

		// Create a test recording file
		String recordingJson = "{\n" + "  \"meta\": {\n" + "    \"title\": \"Test Recording\",\n"
				+ "    \"description\": \"Test description\",\n" + "    \"intent\": \"Test intent\"\n" + "  },\n"
				+ "  \"steps\": {\n" + "    \"tab-1\": [\n" + "      [\n" + "        {\n"
				+ "          \"type\": \"TYPE\",\n"
				+ "          \"selector\": {\"strategy\": \"id\", \"value\": \"username\"},\n"
				+ "          \"text\": \"testuser\",\n" + "          \"label\": \"Username\",\n"
				+ "          \"storeValue\": true,\n" + "          \"isPassword\": false\n" + "        }\n"
				+ "      ]\n" + "    ]\n" + "  }\n" + "}";
		Files.writeString(recordingsDir.resolve("test-recording.json"), recordingJson);

		IProject project = mock(IProject.class);
		when(project.getProjectName()).thenReturn(projectName);
		when(project.getProjectId()).thenReturn(projectId);

		AuthProvider authProvider = mock(AuthProvider.class);
		AccessToken accessToken = mock(AccessToken.class);
		when(accessToken.getEmail()).thenReturn("test@example.com");
		when(accessToken.getUsername()).thenReturn("testuser");
		when(user.getPrimaryLogin()).thenReturn(authProvider);
		when(user.getAccessToken(authProvider)).thenReturn(accessToken);

		try (MockedStatic<SecurityProjectUtils> securityUtils = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> utilityMock = Mockito.mockStatic(Utility.class);
				MockedStatic<PlaywrightUtility> playwrightUtility = Mockito.mockStatic(PlaywrightUtility.class);
				MockedStatic<AssetUtility> assetUtility = Mockito.mockStatic(AssetUtility.class);
				MockedStatic<MCPUtility> mcpUtility = Mockito.mockStatic(MCPUtility.class, Mockito.CALLS_REAL_METHODS);
				MockedStatic<GitRepoUtils> gitRepoUtils = Mockito.mockStatic(GitRepoUtils.class);
				MockedStatic<ClusterUtil> clusterUtil = Mockito.mockStatic(ClusterUtil.class)) {

			securityUtils.when(() -> SecurityProjectUtils.userCanEditProject(user, projectId)).thenReturn(true);
			utilityMock.when(() -> Utility.getProject(projectId)).thenReturn(project);
			playwrightUtility.when(() -> PlaywrightUtility.initRecordingsDir(projectId)).thenReturn(recordingsDir);
			assetUtility.when(() -> AssetUtility.getProjectAssetsFolder(projectId)).thenReturn(assetsDir.toString());
			assetUtility.when(() -> AssetUtility.getProjectVersionFolder(projectName, projectId))
					.thenReturn(versionDir.toString());
			assetUtility.when(() -> AssetUtility.getProjectAssetsFolder(projectName, projectId))
					.thenReturn(assetsDir.toString());
			mcpUtility.when(() -> MCPUtility.addMCPTag(project)).then(invocation -> null);

			gitRepoUtils.when(() -> GitRepoUtils.addSpecificFiles(anyString(), anyList())).then(invocation -> null);
			gitRepoUtils.when(() -> GitRepoUtils.commitAddedFiles(anyString(), anyString(), anyString(), anyString()))
					.then(invocation -> null);
			clusterUtil.when(() -> ClusterUtil.pushProjectFolder(any(), anyString())).then(invocation -> null);

			NounMetadata result = reactor.execute();

			assertEquals(PixelDataType.JSON_OBJECT, result.getNounType());
			JSONObject mcpJson = (JSONObject) result.getValue();

			assertTrue(mcpJson.has("tools"));
			assertTrue(mcpJson.has("_meta"));

			// The recording and its owning project are pinned to a single allowed
			// value so a model cannot replay a different file or reach another project
			JSONObject recordingTool = mcpJson.getJSONArray("tools").getJSONObject(0);
			JSONObject toolProperties = recordingTool.getJSONObject("inputSchema").getJSONObject("properties");

			JSONObject recordedFile = toolProperties.getJSONObject("recording_file");
			assertEquals(1, recordedFile.getJSONArray("enum").length());
			assertEquals("test-recording.json", recordedFile.getJSONArray("enum").getString(0));
			assertEquals("test-recording.json", recordedFile.getString("default"));

			JSONObject projectIdProp = toolProperties.getJSONObject("project_id");
			assertEquals(1, projectIdProp.getJSONArray("enum").length());
			assertEquals(projectId, projectIdProp.getJSONArray("enum").getString(0));
			assertEquals(projectId, projectIdProp.getString("default"));
			assertEquals("play_test_recording", recordingTool.getString("name"));
			assertEquals("PlayPlaywrightSocketsRoomRecording",
					recordingTool.getJSONObject("_meta").getString("SMSS_FUNCTION_NAME"));
			assertEquals("system://browser-automation/",
					recordingTool.getJSONObject("_meta").getJSONObject("SMSS_MCP_UI").getString("resourceURI"));

			// Existing non-Playwright tools must not be replaced when recordings are
			// regenerated.
			assertEquals(2, mcpJson.getJSONArray("tools").length());
			assertEquals("unrelated_tool", mcpJson.getJSONArray("tools").getJSONObject(1).getString("name"));

			// Verify output file was created
			File outputFile = new File(assetsDir.toString() + "/mcp/pixel_mcp.json");
			assertTrue(outputFile.exists());
		}
	}

	/**
	 * The description names the file the reactor writes, which is pixel_mcp.json.
	 */
	@Test
	void getReactorDescriptionReturnsCorrectValue() {
		String description = reactor.getReactorDescription();
		assertTrue(description.contains("mcp/pixel_mcp.json"), description);
	}
}
