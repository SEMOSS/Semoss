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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
 * Reactor that generates an MCP tools JSON file from Playwright recording
 * files. Each recording file becomes a separate tool entry with inputs
 * extracted from TYPE steps.
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
		JSONObject mcpJson = new JSONObject();
		mcpJson.put("tools", toolsArray);

		JSONObject _meta = new JSONObject();
		LocalDate todayUTC = LocalDate.now(ZoneOffset.UTC);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		_meta.put("last_modified_date", todayUTC.format(formatter));
		mcpJson.put("_meta", _meta);

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
	 * Creates an MCP tool definition from a Playwright recording file
	 */
	private JSONObject createToolFromRecording(File file) throws IOException {
		// Parse the recording file
		StepsEnvelope envelope = json.readValue(file, StepsEnvelope.class);

		String fileName = file.getName();
		String fileNameWithoutExt = fileName.replace(".json", "");
		String title = envelope.meta() != null && envelope.meta().title() != null ? envelope.meta().title()
				: fileNameWithoutExt;

		// Get base description, use fallback if empty or null
		String baseDescription = null;
		if (envelope.meta() != null && envelope.meta().description() != null
				&& !envelope.meta().description().trim().isEmpty()) {
			baseDescription = envelope.meta().description();
		} else {
			baseDescription = "Replay Playwright recording: " + title;
		}

		// Extract input fields from steps where type == TYPE and storeValue == true
		List<PlaywrightStep> inputSteps = new ArrayList<>();
		for (List<List<PlaywrightStep>> stepGroups : envelope.steps().values()) {
			for (List<PlaywrightStep> stepGroup : stepGroups) {
				for (PlaywrightStep step : stepGroup) {
					if (step.type() == PlaywrightStepType.TYPE && step.storeValue()) {
						String label = step.label();
						if (label != null && !label.isEmpty()) {
							inputSteps.add(step);
						}
					}
				}
			}
		}

		// Build the input schema
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", sanitizePropertyName(title) + "_Arguments");

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// Add recordedFile parameter with filename as the DEFAULT VALUE
		JSONObject recordedFileProp = new JSONObject();
		recordedFileProp.put("description", "Name of the Playwright recording file to replay");
		recordedFileProp.put("title", "recordedFile");
		recordedFileProp.put("type", "string");
		recordedFileProp.put("default", fileName); // The filename is the default value
		properties.put("recordedFile", recordedFileProp);
		required.put("recordedFile");

		// Add intent parameter if present in metadata
		if (envelope.meta() != null && envelope.meta().intent() != null && !envelope.meta().intent().trim().isEmpty()) {
			JSONObject intentProp = new JSONObject();
			intentProp.put("description", "The intent or purpose of this recording");
			intentProp.put("title", "intent");
			intentProp.put("type", "string");
			intentProp.put("default", envelope.meta().intent());
			properties.put("intent", intentProp);
			required.put("intent");
		}
		// Add projectID parameter with project id as the DEFAULT VALUE
		JSONObject ProjectProp = new JSONObject();
		ProjectProp.put("description", "The project id that contains the recorded file");
		ProjectProp.put("title", "projectID");
		ProjectProp.put("type", "string");
		ProjectProp.put("default", this.keyValue.get(this.keysToGet[0])); // The project id is the default value
		properties.put("projectID", ProjectProp);
		required.put("projectID");

		// Add paramValues as a flexible object type with nested properties
		JSONObject paramValuesProp = new JSONObject();
		paramValuesProp.put("type", "object");
		paramValuesProp.put("title", "paramValues");

		// Build properties for each input field
		if (!inputSteps.isEmpty()) {
			JSONObject paramProperties = new JSONObject();
			JSONArray paramRequired = new JSONArray();

			for (PlaywrightStep step : inputSteps) {
				String fieldName = sanitizePropertyName(step.label());
				JSONObject fieldProp = new JSONObject();
				fieldProp.put("type", "string");
				fieldProp.put("title", fieldName);
				fieldProp.put("description", step.label());

				// Add default value if present
				if (step.text() != null && !step.text().isEmpty()) {
					fieldProp.put("default", step.text());
				}

				// Add format for password fields
				if (step.isPassword()) {
					fieldProp.put("format", "password");
				}

				paramProperties.put(fieldName, fieldProp);
				paramRequired.put(fieldName);
			}

			paramValuesProp.put("properties", paramProperties);
			paramValuesProp.put("required", paramRequired);
			paramValuesProp.put("description",
					"Input values for the Playwright script fields (" + inputSteps.size() + " fields)");
		} else {
			// No input fields, just allow any object
			paramValuesProp.put("description", "Additional parameters (none required for this recording)");
			JSONObject additionalProps = new JSONObject();
			additionalProps.put("type", "string");
			paramValuesProp.put("additionalProperties", additionalProps);
		}

		properties.put("paramValues", paramValuesProp);
		required.put("paramValues");

		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		// Build the tool object with unique name and title based on recording
		JSONObject tool = new JSONObject();
		tool.put("name", sanitizePropertyName(title)); // Unique name based on recording title
		tool.put("title", title); // Human-readable title from metadata
		tool.put("description", baseDescription); // Keep description clean and concise
		tool.put("inputSchema", inputSchema);

		return tool;
	}

	/**
	 * Sanitizes a label to create a valid property name
	 */
	private String sanitizePropertyName(String label) {
		// Remove special characters and convert to camelCase
		String sanitized = label.replaceAll("[^a-zA-Z0-9\\s]", "").trim().replaceAll("\\s+", "_").toLowerCase();

		// Ensure it starts with a letter
		if (sanitized.isEmpty() || !Character.isLetter(sanitized.charAt(0))) {
			sanitized = "field_" + sanitized;
		}

		return sanitized;
	}

	/**
	 * 
	 * @return
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
		return "Generates a mcp/playwright_mcp.json file from Playwright recording scripts";
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
