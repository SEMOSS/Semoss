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

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

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
 * Saves a recording from Chrome Extension to project recordings folder and auto-updates MCP.
 * Uses session-based authentication (Google OAuth from extension).
 * 
 * <p>Pixel Syntax:</p>
 * <pre>SaveRecordingFromExtension(project=[string], name=[string], jsonPayload=[string], 
 *                             title=[string], description=[string], intent=[string])</pre>
 * 
 * <p>Parameters:</p>
 * <ul>
 *   <li><b>project</b> - Project ID where recording will be saved (required)</li>
 *   <li><b>name</b> - Recording filename (auto-generated if not provided, required)</li>
 *   <li><b>jsonPayload</b> - Complete recording JSON as string (required)</li>
 *   <li><b>title</b> - Recording title (optional)</li>
 *   <li><b>description</b> - Recording description (optional)</li>
 *   <li><b>intent</b> - Purpose of the recording (optional)</li>
 * </ul>
 * 
 * <p>Returns:</p>
 * <pre>
 * {
 *   "success": true,
 *   "fileName": "recording.json",
 *   "filePath": "/path/to/recording.json",
 *   "message": "Recording saved successfully"
 * }
 * </pre>
 * 
 * <p>Note: Uses string literals for "name", "jsonPayload", "title", "intent" as these keys
 * are specific to Playwright recordings and don't exist in ReactorKeysEnum. Uses
 * ReactorKeysEnum.DESCRIPTION for description parameter.</p>
 */
public class SaveRecordingFromExtensionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveRecordingFromExtensionReactor.class);

	public SaveRecordingFromExtensionReactor() {
		// Note: String literals used for name, jsonPayload, title, intent as they are 
		// Playwright-specific and don't exist in ReactorKeysEnum
		this.keysToGet = new String[] { 
			ReactorKeysEnum.PROJECT.getKey(), 
			"name", 
			"jsonPayload",
			"title",
			ReactorKeysEnum.DESCRIPTION.getKey(), 
			"intent"
		};
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// Get user from session (Google OAuth)
		User user = this.insight.getUser();
		
		// Check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		// Note: name, title, intent may need URL decoding if coming from form data
		String name = this.keyValue.get(this.keysToGet[1]);
		String jsonPayload = this.keyValue.get(this.keysToGet[2]);
		String title = this.keyValue.get(this.keysToGet[3]);
		String desc = this.keyValue.get(this.keysToGet[4]);
		String intent = this.keyValue.get(this.keysToGet[5]);

		// Validate user has edit access to project
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit.");
		}

		IProject project = Utility.getProject(projectId);

		// Validate JSON payload
		try {
			GSON.fromJson(jsonPayload, Object.class);
		} catch (Exception e) {
			classLogger.error("Invalid JSON payload: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Invalid JSON payload: " + e.getMessage());
		}

		// Sanitize filename
		String base = PlaywrightUtility.sanitizeFilename(
				name == null || name.isBlank() ? ("Recording-" + PlaywrightUtility.generateTimestamp()) : name);
		String fileName = base.endsWith(".json") ? base : (base + ".json");

		// Get recordings directory
		Path recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);
		Path file = recordingsDir.resolve(fileName);

		// Check if file exists and auto-increment filename to avoid overwrite
		if (Files.exists(file)) {
			String baseName = fileName.substring(0, fileName.length() - 5); // Remove .json
			int counter = 1;
			do {
				fileName = baseName + "_" + counter + ".json";
				file = recordingsDir.resolve(fileName);
				counter++;
			} while (Files.exists(file));
			classLogger.info("File already exists, using auto-incremented name: {}", fileName);
		}

		// Save the JSON file - extension already formats it correctly with JSON.stringify()
		try (FileWriter writer = new FileWriter(file.toFile())) {
			// Validate JSON format but don't reformat (preserve extension's formatting)
			GSON.fromJson(jsonPayload, Object.class);
			// Write original string from extension
			writer.write(jsonPayload);
			classLogger.info("Saved recording to: {}", file.toAbsolutePath());
		} catch (Exception e) {
			classLogger.error("Failed to save recording to: {}", file, e);
			throw new RuntimeException("Failed to save recording to: " + file, e);
		}

		// Auto-update MCP by calling MakePlaywrightMCPReactor
		try {
			MakePlaywrightMCPReactor mcpReactor = new MakePlaywrightMCPReactor();
			mcpReactor.setInsight(this.insight);
			mcpReactor.setNounStore(this.store);
			
			// Set the project parameter
			this.store.makeNoun(ReactorKeysEnum.PROJECT.getKey()).add(new NounMetadata(projectId, PixelDataType.CONST_STRING));
			
			mcpReactor.execute();
			classLogger.info("MCP updated successfully");
		} catch (Exception e) {
			classLogger.error("Failed to update MCP: {}", e.getMessage(), e);
			// Don't fail the whole operation, just log the error
		}

		// Git operations
		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = "Added recording from Chrome Extension: " + fileName;

		// Add recording file to git
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/recordings/" + fileName);

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		try {
			GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
			GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
			ClusterUtil.pushProjectFolder(project, assetFolder);
			classLogger.info("Recording committed to git");
		} catch (Exception e) {
			classLogger.error("Git operations failed: {}", e.getMessage(), e);
			// Don't fail the whole operation
		}

		// Return success response
		JSONObject response = new JSONObject();
		response.put("success", true);
		response.put("fileName", fileName);
		response.put("filePath", file.toAbsolutePath().toString());
		response.put("message", "Recording saved successfully");

		return new NounMetadata(response, PixelDataType.JSON_OBJECT);
	}

	@Override
	public String getReactorDescription() {
		return "Saves a recording from Chrome Extension to project recordings folder and updates MCP";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("name")) {
			return "The name of the recording file";
		} else if (key.equals("jsonPayload")) {
			return "The complete recording JSON as a string";
		} else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
			return "The description of the recording";
		} else if (key.equals("title")) {
			return "The title of the recording";
		} else if (key.equals("intent")) {
			return "The intention or purpose of the recording";
		}
		return super.getDescriptionForKey(key);
	}
}