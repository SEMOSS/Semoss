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
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;

/**
 * Generates room scoped Playwright playback tools.
 *
 * <p>
 * Reads every {@code playwright/*.json} recording from the room folder, turns
 * each into one playback tool, and merges the result into the room's
 * {@code mcp/pixel_mcp.json}, leaving tools written by other generators in
 * place. {@link MakePlaywrightMCPReactor} does the same for project assets.
 *
 * <p>
 * Pixel usage:
 * 
 * <pre>
 * MakeRoomPlaywrightMCP(roomId = "..."); // regenerate this room's playback tools
 * MakeRoomPlaywrightMCP(); // use the calling insight's own folder
 * </pre>
 */
public class MakeRoomPlaywrightMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeRoomPlaywrightMCPReactor.class);

	/** Relative path of recording files within the room folder. */
	private static final String RECORDINGS_REL = "/playwright";

	/** Output path relative to the room folder. */
	private static final String OUTPUT_REL = "/mcp/pixel_mcp.json";

	/**
	 * Sidebar UI for a playback tool. The {@code system://} scheme tells the
	 * frontend to load the app from the deployed web app rather than from a
	 * published project portal.
	 */
	private static final String PLAYWRIGHT_APP_URI = "system://playwright-browser-sockets/";

	/**
	 * Stamped into every generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR}
	 * and used on the next run to tell this reactor's own tools apart from tools
	 * that merely share the file.
	 */
	private static final String GENERATOR_ID = "MakeRoomPlaywrightMCP";

	/** Pixel every generated playback tool invokes. */
	private static final String PLAYBACK_FUNCTION = "PlayPlaywrightSocketsRoomRecording";

	private final ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public MakeRoomPlaywrightMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());

		// If roomId is provided, bind the current insight to that room so that
		// getInsightFolder() returns the room's recording folder, not the empty
		// folder of the calling session.
		if (roomId != null && !roomId.isBlank()) {
			try {
				Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
				this.insight.setRoomForInsight(room);
			} catch (Exception e) {
				classLogger.warn("MakeRoomPlaywrightMCP: could not bind to room {}: {}", roomId, e.getMessage());
			}
		}

		String assetFolder = this.insight.getInsightFolder();
		if (assetFolder == null || assetFolder.isBlank()) {
			throw new IllegalStateException("No insight asset folder is available for this session.");
		}

		// Every .json recording in the room folder becomes one playback tool.
		File recordingsDir = new File(assetFolder + RECORDINGS_REL);
		File[] files = recordingsDir.exists()
				? recordingsDir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"))
				: null;

		if (files == null || files.length == 0) {
			throw new IllegalArgumentException(
					"No Playwright recording files found in room insight assets under: " + RECORDINGS_REL);
		}

		// Sort by name for deterministic output
		Arrays.sort(files, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));

		// One unparseable recording is skipped so it cannot take out the whole batch.
		JSONArray toolsArray = new JSONArray();
		for (File file : files) {
			try {
				toolsArray.put(createToolFromRecording(file));
			} catch (Exception e) {
				classLogger.warn("Skipping recording file {} - could not parse: {}", file.getName(), e.getMessage());
			}
		}

		if (toolsArray.isEmpty()) {
			throw new IllegalArgumentException("No valid Playwright recording files could be parsed.");
		}

		// Stamp first: ownership decides which existing tools the merge may replace.
		MCPUtility.stampGenerator(toolsArray, GENERATOR_ID);

		// Every recording was rebuilt, so a stamped tool that is absent had its
		// recording deleted.
		JSONArray mergedTools = MCPUtility.mergeGeneratedTools(MCPUtility.readMcpJson(assetFolder + OUTPUT_REL),
				toolsArray, GENERATOR_ID, true);
		JSONObject mcpJson = PlaywrightMCPToolBuilder.wrapMcpJson(mergedTools);

		String prettyJson = mcpJson.toString(4);
		FileSystemUtil.saveAssetFiles(assetFolder, List.of(OUTPUT_REL), List.of(prettyJson));

		classLogger.info("Saved room MCP to {}{} ({} playback tool(s) generated, {} other tool(s) preserved)",
				assetFolder, OUTPUT_REL, toolsArray.length(), mergedTools.length() - toolsArray.length());
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	/**
	 * Builds the playback tool definition for a single room recording. Schema
	 * mechanics are shared with {@link MakePlaywrightMCPReactor} via
	 * {@link PlaywrightMCPToolBuilder}; the parameter set and {@code _meta} below
	 * are specific to room playback.
	 *
	 * <p>
	 * One entry of the {@code tools} array, assembled by the sections below:
	 *
	 * <pre>
	 * {
	 *   "name": "play_bing_web_search_query",
	 *   "title": "Play: Bing Web Search Query",
	 *   "description": "Replay: Search for kimi news - Navigates to Bing and searches.",
	 *   "inputSchema": {
	 *     "type": "object",
	 *     "title": "bing_web_search_query_Arguments",
	 *     "properties": { "recording_file": {...}, "intent": {...},
	 *                     "start_url": {...}, "paramValues": {...} },
	 *     "required": ["recording_file"]
	 *   },
	 *   "_meta": { "SMSS_MCP_EXECUTION": "ask", "SMSS_MCP_UI": {...}, ... }
	 * }
	 * </pre>
	 *
	 * @param file the recording to describe
	 * @return the tool definition
	 * @throws Exception if the recording cannot be parsed
	 */
	private JSONObject createToolFromRecording(File file) throws Exception {
		StepsEnvelope envelope = json.readValue(file, StepsEnvelope.class);

		String fileName = file.getName();
		String title = PlaywrightMCPToolBuilder.resolveRecordingTitle(envelope, fileName);
		String baseDescription = PlaywrightMCPToolBuilder.resolveRecordingDescription(envelope, title,
				"Replay room Playwright recording: ");

		// Build a richer description using intent so the LLM can match by purpose,
		// not just title. Example: "Replay: Football highlights - opens YouTube
		// and navigates to football highlight videos."
		String intentHint = PlaywrightMCPToolBuilder.resolveRecordingIntent(envelope, title);
		String richDescription = "Replay: " + intentHint + " - " + baseDescription;

		List<PlaywrightStep> inputSteps = PlaywrightMCPToolBuilder.collectStoreValueInputs(envelope);

		// "inputSchema": { "type": "object", "title": "bing_web_search_Arguments",
		// "properties": {...}, "required": [...] }
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", PlaywrightMCPToolBuilder.sanitizeToolName(title, "tool_") + "_Arguments");

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// "recording_file": {
		// "type": "string", "title": "recording_file",
		// "description": "Room recording file to replay.",
		// "enum": ["bing-web-search-20260728.json"],
		// "default": "bing-web-search-20260728.json"
		// }
		// One tool per recording, so the file name is pinned to a single value.
		properties.put("recording_file", PlaywrightMCPToolBuilder.pinnedStringProperty("recording_file",
				"Room recording file to replay.", fileName));
		required.put("recording_file");

		// "intent": {
		// "type": "string", "title": "intent",
		// "description": "The intent or purpose of this recording.",
		// "default": "Search for information using Bing"
		// }
		// Not pinned: a model may reword it.
		if (envelope.meta() != null && envelope.meta().intent() != null && !envelope.meta().intent().isBlank()) {
			JSONObject intentProp = new JSONObject();
			intentProp.put("type", "string");
			intentProp.put("title", "intent");
			intentProp.put("description", "The intent or purpose of this recording.");
			intentProp.put("default", envelope.meta().intent());
			properties.put("intent", intentProp);
		}

		// "start_url": {
		// "type": "string", "title": "start_url",
		// "description": "Optional URL override before replay."
		// }
		// No default: absent means replay whatever the recording navigated to.
		JSONObject startUrlProp = new JSONObject();
		startUrlProp.put("type", "string");
		startUrlProp.put("title", "start_url");
		startUrlProp.put("description", "Optional URL override before replay.");
		properties.put("start_url", startUrlProp);

		// "paramValues": {
		// "type": "object", "title": "paramValues",
		// "properties": { "search_term": { "type": "string", "default": "kimi" } },
		// "required": ["search_term"]
		// }
		// One field per TYPE step flagged storeValue; free-form string map when none.
		properties.put("paramValues",
				PlaywrightMCPToolBuilder.buildParamValuesSchema(inputSteps,
						"Additional parameters (none required for this recording).",
						"Input values for the recording's form fields"));

		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		// "_meta": {
		// "SMSS_MCP_EXECUTION": "ask",
		// "SMSS_MCP_UI": { "displayLocation": "sidebar",
		// "resourceURI": "system://playwright-browser-sockets/" },
		// "SMSS_FUNCTION_NAME": "PlayPlaywrightSocketsRoomRecording",
		// "generated_on": "2026-07-28"
		// }
		// SMSS_MCP_UI opens the remote browser app in the Playground sidebar and that
		// app performs the replay. SMSS_FUNCTION_NAME is the pixel that runs when the
		// tool executes. Both are per-tool choices, so another generator writing into
		// this room's file can omit the UI block and render inline instead.
		JSONObject meta = new JSONObject();
		meta.put("SMSS_MCP_EXECUTION", "ask");
		JSONObject mcpUi = new JSONObject();
		mcpUi.put("displayLocation", "sidebar");
		// system:// resolves to the app shipped in the web app, so the URL needs no
		// project id.
		mcpUi.put("resourceURI", PLAYWRIGHT_APP_URI);
		meta.put("SMSS_MCP_UI", mcpUi);
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, PLAYBACK_FUNCTION);
		meta.put("generated_on", PlaywrightMCPToolBuilder.todayUtcDate());

		// Wrap it all up as one entry of the "tools" array. The name is prefixed with
		// "play_" so the LLM can tell playback tools apart from the recording tools
		// exposed by the platform project.
		String toolName = "play_" + PlaywrightMCPToolBuilder.sanitizeToolName(title, "tool_");
		JSONObject tool = new JSONObject();
		tool.put("name", toolName);
		tool.put("title", "Play: " + title);
		tool.put("description", richDescription);
		tool.put("inputSchema", inputSchema);
		tool.put("_meta", meta);
		return tool;
	}

	@Override
	public String getReactorDescription() {
		return "Generates mcp/pixel_mcp.json for the current room insight from all playwright/*.json recordings "
				+ "saved in the same insight assets. Mirrors MakePlaywrightMCPReactor for room-level recordings.";
	}
}
