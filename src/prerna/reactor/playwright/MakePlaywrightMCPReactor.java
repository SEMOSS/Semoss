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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Generates project scoped Playwright replay tools.
 *
 * <p>
 * Each recording under the project's Playwright folder becomes one tool in the
 * project's {@code mcp/pixel_mcp.json}, with inputs taken from its TYPE steps.
 * {@link MakeRoomPlaywrightMCPReactor} does the same for a room folder.
 */
public class MakePlaywrightMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakePlaywrightMCPReactor.class);
	private static final String OUTPUT_REL = "/mcp/pixel_mcp.json";
	private static final String PLAYWRIGHT_APP_URI = "system://playwright-browser-sockets/";
	private static final String GENERATOR_ID = "MakePlaywrightMCP";
	private static final String PLAYBACK_FUNCTION = "PlayPlaywrightSocketsRoomRecording";

	private final ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public MakePlaywrightMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit.");
		}
		IProject project = Utility.getProject(projectId);
		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);

		// Get the recordings directory
		Path recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);
		File dir = recordingsDir.toFile();

		// Collect all JSON files
		File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"));
		if (files == null || files.length == 0) {
			throw new IllegalArgumentException("No Playwright recording files found in: " + recordingsDir);
		}
		Arrays.sort(files, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));

		// Build tools array
		JSONArray toolsArray = new JSONArray();
		for (File file : files) {
			try {
				JSONObject tool = createToolFromRecording(file);
				toolsArray.put(tool);
			} catch (Exception e) {
				classLogger.error("Failed to process Playwright recording file '{}'", file.getName(), e);
				// Continue processing other files
			}
		}

		if (toolsArray.isEmpty()) {
			throw new IllegalArgumentException("No valid Playwright recording files could be parsed.");
		}

		// Replace only tools owned by this generator. Hand-authored tools and tools
		// written by other generators remain untouched.
		MCPUtility.stampGenerator(toolsArray, GENERATOR_ID);
		String outputFileLoc = projectAssetFolder + OUTPUT_REL;
		JSONArray mergedTools = MCPUtility.mergeGeneratedTools(MCPUtility.readMcpJson(outputFileLoc), toolsArray,
				GENERATOR_ID, true);
		JSONObject mcpJson = PlaywrightMCPToolBuilder.wrapMcpJson(mergedTools);

		File outputFile = new File(outputFileLoc);
		if (!outputFile.getParentFile().exists() || !outputFile.getParentFile().isDirectory()) {
			outputFile.getParentFile().mkdirs();
		}

		try (FileWriter writer = new FileWriter(outputFile)) {
			String prettyJson = mcpJson.toString(4);
			writer.write(prettyJson);
		} catch (IOException e) {
			classLogger.error("Unable to write pixel_mcp.json file to '{}'", outputFileLoc, e);
			throw new IllegalArgumentException(
					"Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
		}

		// Make the selected app discoverable as an MCP toolbox in Playground. This
		// preserves any existing tags and is the same registration used by the
		// standard project MCP generators.
		MCPUtility.addMCPTag(project);

		// Git operations
		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey());
		if (comment == null) {
			comment = "add: configured Playwright MCP tool";
		}

		// Add file to git
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/mcp/pixel_mcp.json");

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushProjectFolder(project, assetFolder);

		classLogger.info("Saved project MCP to {} ({} playback tool(s) generated, {} other tool(s) preserved)",
				outputFileLoc, toolsArray.length(), mergedTools.length() - toolsArray.length());
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	/**
	 * Creates the system-app playback tool for one project recording. Its contract
	 * mirrors the room generator, with a pinned project id added so the system app
	 * loads the recording from the selected app.
	 *
	 * @param file the recording to describe
	 * @return the tool definition
	 * @throws IOException if the recording cannot be read
	 */
	private JSONObject createToolFromRecording(File file) throws IOException {
		// Parse the recording file
		StepsEnvelope envelope = json.readValue(file, StepsEnvelope.class);

		String fileName = file.getName();
		String title = PlaywrightMCPToolBuilder.resolveRecordingTitle(envelope, fileName);
		String baseDescription = PlaywrightMCPToolBuilder.resolveRecordingDescription(envelope, title,
				"Replay project Playwright recording: ");
		String intentHint = PlaywrightMCPToolBuilder.resolveRecordingIntent(envelope, title);
		String richDescription = "Replay: " + intentHint + " - " + baseDescription;

		// Extract input fields from steps where type == TYPE and storeValue == true
		List<PlaywrightStep> inputSteps = PlaywrightMCPToolBuilder.collectStoreValueInputs(envelope);

		// "inputSchema": { "type": "object", "title": "checkout_flow_Arguments",
		// "properties": {...}, "required": [...] }
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", PlaywrightMCPToolBuilder.sanitizeToolName(title, "tool_") + "_Arguments");

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// One tool per recording, so the file name is pinned.
		properties.put("recording_file", PlaywrightMCPToolBuilder.pinnedStringProperty("recording_file",
				"Project recording file to replay.", fileName));
		required.put("recording_file");

		// "intent": {
		// "type": "string", "title": "intent",
		// "description": "The intent or purpose of this recording",
		// "default": "Buy a product end to end"
		// }
		// Not pinned: a model may reword it.
		if (envelope.meta() != null && envelope.meta().intent() != null && !envelope.meta().intent().trim().isEmpty()) {
			JSONObject intentProp = new JSONObject();
			intentProp.put("description", "The intent or purpose of this recording");
			intentProp.put("title", "intent");
			intentProp.put("type", "string");
			intentProp.put("default", envelope.meta().intent());
			properties.put("intent", intentProp);
		}
		// Pinned too: the recording exists only in this project.
		properties.put("project_id", PlaywrightMCPToolBuilder.pinnedStringProperty("project_id",
				"App project containing the recording.", this.keyValue.get(this.keysToGet[0])));
		required.put("project_id");

		JSONObject startUrlProp = new JSONObject();
		startUrlProp.put("type", "string");
		startUrlProp.put("title", "start_url");
		startUrlProp.put("description", "Optional URL override before replay.");
		properties.put("start_url", startUrlProp);

		// "paramValues": {
		// "type": "object", "title": "paramValues",
		// "properties": { "username": { "type": "string", "default": "bob" } },
		// "required": ["username"]
		// }
		// One field per TYPE step flagged storeValue; free-form string map when none.
		properties.put("paramValues",
				PlaywrightMCPToolBuilder.buildParamValuesSchema(inputSteps,
						"Additional parameters (none required for this recording)",
						"Input values for the Playwright script fields"));
		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		JSONObject meta = new JSONObject();
		meta.put("SMSS_MCP_EXECUTION", "ask");
		JSONObject mcpUi = new JSONObject();
		mcpUi.put("displayLocation", "sidebar");
		mcpUi.put("resourceURI", PLAYWRIGHT_APP_URI);
		meta.put("SMSS_MCP_UI", mcpUi);
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, PLAYBACK_FUNCTION);
		meta.put("SMSS_PROJECT_ID", this.keyValue.get(this.keysToGet[0]));
		meta.put("generated_on", PlaywrightMCPToolBuilder.todayUtcDate());

		JSONObject tool = new JSONObject();
		tool.put("name", "play_" + PlaywrightMCPToolBuilder.sanitizeToolName(title, "tool_"));
		tool.put("title", "Play: " + title);
		tool.put("description", richDescription);
		tool.put("inputSchema", inputSchema);
		tool.put("_meta", meta);
		return tool;
	}

	@Override
	public String getReactorDescription() {
		return "Generates a mcp/pixel_mcp.json file from Playwright recording scripts";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}
}
