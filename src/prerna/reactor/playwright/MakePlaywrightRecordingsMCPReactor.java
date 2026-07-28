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
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

/**
 * Generates recording-specific MCP tools for either a Playground room or a
 * Playwright Sockets app project.
 */
public class MakePlaywrightRecordingsMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakePlaywrightRecordingsMCPReactor.class);
	private static final String PROJECT_ID = "projectId";
	private static final String OUTPUT_REL = "/mcp/pixel_mcp.json";
	private static final String ROOM_RECORDINGS_REL = "/playwright/recordings";
	private static final String OPEN_TOOL_NAME = "open_playwright_sockets";
	private static final String OPEN_REACTOR = "OpenPlaywrightSocketsRoomRecording";
	private static final String REPLAY_REACTOR = "PlayPlaywrightSocketsRoomRecording";
	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	private final ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public MakePlaywrightRecordingsMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.PROJECT.getKey(), PROJECT_ID,
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = trim(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		String requestedProjectId = firstNonBlank(this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()),
				this.keyValue.get(PROJECT_ID));
		boolean roomMode = !roomId.isEmpty();

		if (roomMode) {
			return generateRoomMcp(roomId, requestedProjectId);
		}
		if (requestedProjectId.isEmpty()) {
			throw new IllegalArgumentException("Either roomId or project is required");
		}
		return generateProjectMcp(requestedProjectId);
	}

	private NounMetadata generateRoomMcp(String roomId, String portalProjectId) {
		try {
			Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
			this.insight.setRoomForInsight(room);
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not bind to room " + roomId, e);
		}

		if (portalProjectId.isEmpty()) {
			portalProjectId = discoverProjectIdFromRoom(roomId);
		}

		String assetFolder = this.insight.getInsightFolder();
		if (assetFolder == null || assetFolder.isBlank()) {
			throw new IllegalStateException("No insight asset folder is available for this room.");
		}

		File recordingsDir = new File(assetFolder + ROOM_RECORDINGS_REL);
		JSONArray tools = createRecordingTools(recordingsDir, portalProjectId, true);
		if (tools.isEmpty()) {
			throw new IllegalArgumentException(
					"No valid Playwright recordings found under " + ROOM_RECORDINGS_REL);
		}

		JSONObject mcpJson = createMcpDocument(tools, false);
		FileSystemUtil.saveAssetFiles(assetFolder, List.of(OUTPUT_REL), List.of(mcpJson.toString(4)));
		classLogger.info("Saved room Playwright MCP with {} tool(s) to {}{}", tools.length(), assetFolder, OUTPUT_REL);
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	private NounMetadata generateProjectMcp(String requestedProjectId) {
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, requestedProjectId);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit.");
		}

		IProject project = Utility.getProject(projectId);
		String assetFolder = AssetUtility.getProjectAssetsFolder(projectId);
		Path recordingsPath = PlaywrightUtility.initRecordingsDir(projectId);
		JSONArray tools = createRecordingTools(recordingsPath.toFile(), projectId, false);
		tools = prepend(tools, createOpenPlaywrightSocketsTool());

		JSONObject mcpJson = createMcpDocument(tools, true);
		File outputFile = new File(assetFolder + OUTPUT_REL);
		if (!outputFile.getParentFile().isDirectory() && !outputFile.getParentFile().mkdirs()) {
			throw new IllegalArgumentException("Unable to create MCP folder for project " + projectId);
		}
		try (FileWriter writer = new FileWriter(outputFile)) {
			writer.write(mcpJson.toString(4));
		} catch (Exception e) {
			classLogger.error("Unable to write Playwright MCP to '{}'", outputFile, e);
			throw new IllegalArgumentException("Unable to write pixel_mcp.json. Detailed error = " + e.getMessage());
		}

		commitProjectMcp(user, project, assetFolder);
		classLogger.info("Saved app Playwright MCP with {} tool(s) to {}", tools.length(), outputFile);
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	private JSONArray createRecordingTools(File recordingsDir, String projectId, boolean roomMode) {
		File[] files = recordingsDir.isDirectory()
				? recordingsDir.listFiles((directory, name) -> name.toLowerCase().endsWith(".json"))
				: null;
		if (files == null || files.length == 0) {
			return new JSONArray();
		}

		Arrays.sort(files, (left, right) -> left.getName().compareToIgnoreCase(right.getName()));
		JSONArray tools = new JSONArray();
		for (File file : files) {
			try {
				tools.put(createToolFromRecording(file, projectId, roomMode));
			} catch (Exception e) {
				classLogger.warn("Skipping recording file '{}'; could not parse it", file.getName(), e);
			}
		}
		return tools;
	}

	private JSONObject createToolFromRecording(File file, String projectId, boolean roomMode) throws Exception {
		StepsEnvelope envelope = json.readValue(file, StepsEnvelope.class);
		String fileName = file.getName();
		String fallbackTitle = fileName.replaceFirst("(?i)\\.json$", "");
		String title = hasText(envelope.meta() == null ? null : envelope.meta().title())
				? envelope.meta().title()
				: fallbackTitle;
		String description = hasText(envelope.meta() == null ? null : envelope.meta().description())
				? envelope.meta().description()
				: "Replay Playwright recording: " + title;
		String intent = hasText(envelope.meta() == null ? null : envelope.meta().intent())
				? envelope.meta().intent()
				: title;

		List<PlaywrightStep> inputSteps = new ArrayList<>();
		for (List<List<PlaywrightStep>> stepGroups : envelope.steps().values()) {
			for (List<PlaywrightStep> stepGroup : stepGroups) {
				for (PlaywrightStep step : stepGroup) {
					if (step.type() == PlaywrightStepType.TYPE && step.storeValue() && hasText(step.label())) {
						inputSteps.add(step);
					}
				}
			}
		}

		JSONObject properties = new JSONObject();
		properties.put("recording_file", new JSONObject()
				.put("type", "string")
				.put("title", "Recording File")
				.put("description", "Recording file to replay.")
				.put("default", fileName)
				.put("const", fileName));
		if (!roomMode && hasText(projectId)) {
			properties.put("project_id", new JSONObject()
					.put("type", "string")
					.put("title", "Project ID")
					.put("description", "Playwright app project containing the recording.")
					.put("default", projectId)
					.put("const", projectId));
		}
		if (hasText(envelope.meta() == null ? null : envelope.meta().intent())) {
			properties.put("intent", new JSONObject()
					.put("type", "string")
					.put("title", "Intent")
					.put("description", "The purpose of this recording.")
					.put("default", envelope.meta().intent()));
		}
		properties.put("start_url", new JSONObject()
				.put("type", "string")
				.put("title", "Start URL")
				.put("description", "Optional URL override before replay."));
		properties.put("paramValues", createParameterValuesSchema(inputSteps));

		JSONArray required = new JSONArray().put("recording_file");
		if (!roomMode && hasText(projectId)) {
			required.put("project_id");
		}
		JSONObject inputSchema = new JSONObject()
				.put("type", "object")
				.put("title", sanitize(title) + "_Arguments")
				.put("properties", properties)
				.put("required", required);

		JSONObject meta = new JSONObject()
				.put("generated_on", today())
				.put("SMSS_MCP_EXECUTION", "ask")
				.put("SMSS_MCP_UI",
						new JSONObject().put("displayLocation", "sidebar").put("resourceURI", "/"))
				.put("SMSS_FUNCTION_NAME", REPLAY_REACTOR);
		if (hasText(projectId)) {
			meta.put("SMSS_PROJECT_ID", projectId);
		}
		if (roomMode) {
			meta.put("SMSS_ENGINE_ID", MCPUtility.INSIGHT_MCP_ID);
			// Execution belongs to the room insight, but the sidebar UI is hosted
			// by the Playwright Sockets project.
			meta.put("SMSS_ENGINE_TYPE", "PROJECT");
		}

		JSONObject tool = new JSONObject()
				.put("name", "play_" + sanitize(title))
				.put("title", "Play: " + title)
				.put("description", "Replay: " + intent + " - " + description)
				.put("inputSchema", inputSchema)
				.put("_meta", meta);
		return tool;
	}

	private static JSONObject createParameterValuesSchema(List<PlaywrightStep> inputSteps) {
		JSONObject schema = new JSONObject().put("type", "object").put("title", "Parameter Values");
		if (inputSteps.isEmpty()) {
			return schema.put("additionalProperties", new JSONObject().put("type", "string"))
					.put("description", "Additional parameters; none are required for this recording.");
		}

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();
		for (PlaywrightStep step : inputSteps) {
			String fieldName = sanitize(step.label());
			JSONObject field = new JSONObject()
					.put("type", "string")
					.put("title", fieldName)
					.put("description", step.label());
			if (hasText(step.text())) {
				field.put("default", step.text());
			}
			if (step.isPassword()) {
				field.put("format", "password");
			}
			properties.put(fieldName, field);
			required.put(fieldName);
		}
		return schema.put("properties", properties).put("required", required)
				.put("description", "Input values for the recording's form fields.");
	}

	private static JSONObject createOpenPlaywrightSocketsTool() {
		JSONObject properties = new JSONObject()
				.put("start_url", new JSONObject()
						.put("title", "Start URL")
						.put("description", "URL to open in the Playwright Sockets browser.")
						.put("type", "string"))
				.put("recording_name_hint", new JSONObject()
						.put("title", "Recording Name Hint")
						.put("description", "Optional short name hint for the saved recording.")
						.put("type", "string"));
		JSONObject inputSchema = new JSONObject()
				.put("type", "object")
				.put("title", "Open Playwright Sockets Arguments")
				.put("properties", properties)
				.put("required", new JSONArray().put("start_url"));
		JSONObject meta = new JSONObject()
				.put("generated_on", "2026-07-09")
				.put("SMSS_MCP_EXECUTION", "ask")
				.put("SMSS_MCP_UI",
						new JSONObject().put("displayLocation", "sidebar").put("resourceURI", "/"))
				.put("SMSS_FUNCTION_NAME", OPEN_REACTOR);
		return new JSONObject()
				.put("name", OPEN_TOOL_NAME)
				.put("title", "browser app playwright browser app playwright sockets")
				.put("description",
						"browser app that Open the Playwright Sockets remote browser app, navigate to start_url, begin recording, and save the recording into the current Playground room when returned. If the user did not provide a URL, ask for one before calling this tool.")
				.put("inputSchema", inputSchema)
				.put("_meta", meta);
	}

	private static JSONObject createMcpDocument(JSONArray tools, boolean projectMode) {
		JSONObject meta = new JSONObject().put("last_modified_date", today());
		if (projectMode) {
			meta.put("generated_by", "manual");
		}
		return new JSONObject().put("_meta", meta).put("tools", tools);
	}

	private void commitProjectMcp(User user, IProject project, String assetFolder) {
		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
		String comment = trim(this.keyValue.get(ReactorKeysEnum.COMMENT_KEY.getKey()));
		if (comment.isEmpty()) {
			comment = "add: configured Playwright recordings MCP tools";
		}
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		GitRepoUtils.addSpecificFiles(versionGitFolder,
				List.of(Constants.ASSETS_FOLDER + "/mcp/pixel_mcp.json"));
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, accessToken.getUsername(), accessToken.getEmail());
		ClusterUtil.pushProjectFolder(project, assetFolder);
	}

	@SuppressWarnings("unchecked")
	private String discoverProjectIdFromRoom(String roomId) {
		try {
			String userId = this.insight.getUser().getPrimaryLoginToken().getId();
			List<java.util.Map<String, Object>> rows = ModelInferenceLogsUtils.getRoomOptions(roomId, userId);
			if (rows == null || rows.isEmpty()) {
				return "";
			}
			Object optionsObject = rows.get(0).get("OPTIONS");
			if (!(optionsObject instanceof java.util.Map)) {
				return "";
			}
			Object mcpObject = ((java.util.Map<String, Object>) optionsObject).get("mcp");
			if (!(mcpObject instanceof List)) {
				return "";
			}
			for (Object item : (List<?>) mcpObject) {
				if (item instanceof java.util.Map) {
					java.util.Map<String, Object> mcp = (java.util.Map<String, Object>) item;
					if ("PROJECT".equals(mcp.get("type")) && mcp.get("id") instanceof String) {
						return trim((String) mcp.get("id"));
					}
				}
			}
		} catch (Exception e) {
			classLogger.warn("Could not discover the Playwright app project for room '{}'", roomId, e);
		}
		return "";
	}

	private static JSONArray prepend(JSONArray recordings, JSONObject first) {
		JSONArray tools = new JSONArray().put(first);
		for (int i = 0; i < recordings.length(); i++) {
			tools.put(recordings.get(i));
		}
		return tools;
	}

	private static String sanitize(String value) {
		String sanitized = value.replaceAll("[^a-zA-Z0-9\\s]", "").trim().replaceAll("\\s+", "_").toLowerCase();
		return sanitized.isEmpty() || !Character.isLetter(sanitized.charAt(0)) ? "tool_" + sanitized : sanitized;
	}

	private static boolean hasText(String value) {
		return value != null && !value.isBlank();
	}

	private static String firstNonBlank(String... values) {
		for (String value : values) {
			if (hasText(value)) {
				return value.trim();
			}
		}
		return "";
	}

	private static String trim(String value) {
		return value == null ? "" : value.trim();
	}

	private static String today() {
		return LocalDate.now(ZoneOffset.UTC).format(DATE_FORMAT);
	}

	@Override
	public String getReactorDescription() {
		return "Generates mcp/pixel_mcp.json from Playwright recordings saved in a Playground room or app project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Optional Playground room whose recording MCP should be generated.";
		}
		if (ReactorKeysEnum.PROJECT.getKey().equals(key) || PROJECT_ID.equals(key)) {
			return "Playwright app project to update, or the sidebar app project for a room MCP.";
		}
		if (ReactorKeysEnum.COMMENT_KEY.getKey().equals(key)) {
			return "Git commit comment used when updating an app project.";
		}
		return super.getDescriptionForKey(key);
	}
}
