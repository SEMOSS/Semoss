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

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

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

		toolsArray.put(createAddVisionContextTool());

		// Create the MCP JSON structure
		JSONObject mcpJson = PlaywrightMCPToolBuilder.wrapMcpJson(toolsArray);

		// Write the output file
		String outputFileLoc = projectAssetFolder + "/mcp/pixel_mcp.json";
		File outputFile = new File(outputFileLoc);
		if (!outputFile.getParentFile().exists() || !outputFile.getParentFile().isDirectory()) {
			outputFile.getParentFile().mkdirs();
		}
		if (outputFile.exists()) {
			outputFile.delete();
		}

		try (FileWriter writer = new FileWriter(outputFile)) {
			String prettyJson = mcpJson.toString(4);
			writer.write(prettyJson);
		} catch (IOException e) {
			classLogger.error("Unable to write pixel_mcp.json file to '{}'", outputFileLoc, e);
			throw new IllegalArgumentException(
					"Unable to write pixel_mcp.json file. Detailed error = " + e.getMessage());
		}

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

		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	/**
	 * Creates an MCP tool definition from a Playwright recording file.
	 *
	 * <p>
	 * One entry of the {@code tools} array, assembled by the sections below:
	 *
	 * <pre>
	 * {
	 *   "name": "checkout_flow",
	 *   "title": "Checkout Flow",
	 *   "description": "Replay Playwright recording: Checkout Flow",
	 *   "inputSchema": {
	 *     "type": "object",
	 *     "title": "checkout_flow_Arguments",
	 *     "properties": { "recordedFile": {...}, "intent": {...},
	 *                     "projectID": {...}, "paramValues": {...} },
	 *     "required": ["recordedFile", "intent", "projectID", "paramValues"]
	 *   }
	 * }
	 * </pre>
	 *
	 * <p>
	 * These tools are run by the Chrome extension rather than by a pixel. With no
	 * {@code SMSS_MCP_UI} to iframe, the Playground renders the tool with
	 * {@code ToolsDefaultView}, its generic parameter form. That form branches on
	 * the {@code recordedFile} argument the model sent: instead of calling
	 * {@code RunMCPTool} it runs {@code Session()}, then
	 * {@code GetAllSteps(project, sessionId, fileName)}, and posts the steps to the
	 * extension as {@code SMSS_EXEC_PLAYWRIGHT_SCRIPT} over
	 * {@code window.postMessage}.
	 *
	 * <p>
	 * {@code recordedFile} and {@code projectID} are pinned to single values
	 * because the browser fetches the steps by project id and file name, so the
	 * recording never leaves the server as a file.
	 *
	 * <p>
	 * This path depends on the project having no published portal. When one is
	 * published the Playground iframes {@code public_home/{projectId}/portals/}
	 * instead.
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
				"Replay Playwright recording: ");

		// Extract input fields from steps where type == TYPE and storeValue == true
		List<PlaywrightStep> inputSteps = PlaywrightMCPToolBuilder.collectStoreValueInputs(envelope);

		// "inputSchema": { "type": "object", "title": "checkout_flow_Arguments",
		// "properties": {...}, "required": [...] }
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", PlaywrightMCPToolBuilder.sanitizeToolName(title, "field_") + "_Arguments");

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// "recordedFile": {
		// "type": "string", "title": "recordedFile",
		// "description": "Name of the Playwright recording file to replay",
		// "enum": ["checkout-flow.json"], "default": "checkout-flow.json"
		// }
		// One tool per recording, so the file name is pinned to a single value.
		properties.put("recordedFile", PlaywrightMCPToolBuilder.pinnedStringProperty("recordedFile",
				"Name of the Playwright recording file to replay", fileName));
		required.put("recordedFile");

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
			required.put("intent");
		}
		// "projectID": {
		// "type": "string", "title": "projectID",
		// "description": "The project id that contains the recorded file",
		// "enum": ["e5f1..."], "default": "e5f1..."
		// }
		// Pinned too: the recording exists only in this project.
		properties.put("projectID", PlaywrightMCPToolBuilder.pinnedStringProperty("projectID",
				"The project id that contains the recorded file", this.keyValue.get(this.keysToGet[0])));
		required.put("projectID");

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
		required.put("paramValues");

		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		// Wrap it all up as one entry of the "tools" array.
		JSONObject tool = new JSONObject();
		tool.put("name", sanitizePropertyName(title));
		tool.put("title", title);
		tool.put("description", baseDescription);
		tool.put("inputSchema", inputSchema);

		return tool;
	}

	/**
	 * Sanitizes a label into a valid property name, prefixing {@code field_} when
	 * the label does not start with a letter.
	 */
	private String sanitizePropertyName(String label) {
		return PlaywrightMCPToolBuilder.sanitizeToolName(label, "field_");
	}

	/**
	 * Builds the {@code AddVisionContext} tool, which takes a single
	 * {@code visionContext} string. Its description is a placeholder that will not
	 * match a user request, so the model does not select it from the tool list.
	 *
	 * @return the tool definition
	 */
	private static JSONObject createAddVisionContextTool() {
		JSONObject tool = new JSONObject();
		tool.put("name", "AddVisionContext");
		tool.put("description", "dont_match_ME");
		tool.put("title", "Add Vision Context");

		// Build inputSchema
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", "AddVisionContext_Arguments");

		JSONObject properties = new JSONObject();
		JSONObject visionContextProp = new JSONObject();
		visionContextProp.put("description", "Context from the vision model");
		visionContextProp.put("title", "visionContext");
		visionContextProp.put("type", "string");
		properties.put("visionContext", visionContextProp);

		JSONArray required = new JSONArray();
		required.put("visionContext");

		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		tool.put("inputSchema", inputSchema);

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
