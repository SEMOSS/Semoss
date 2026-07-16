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

import prerna.engine.impl.InsightMCP;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;

/**
 * Mirrors {@link MakePlaywrightMCPReactor} but operates on room insight assets
 * instead of project assets.
 *
 * <p>Reads every {@code playwright/*.json} recording from the current insight's
 * asset folder, generates a {@code mcp/pixel_mcp.json} tool-definition file in
 * exactly the same format as the project-level reactor, and saves it back to
 * the same insight folder using {@link FileSystemUtil#saveAssetFiles}.
 *
 * <p>Pixel usage:
 * <pre>
 *   MakeRoomPlaywrightMCP(roomId="...");   // auto-discovers playwright project ID
 *   MakeRoomPlaywrightMCP(projectId="..."); // explicit project ID override
 *   MakeRoomPlaywrightMCP();                // no project ID in tool _meta
 * </pre>
 */
public class MakeRoomPlaywrightMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeRoomPlaywrightMCPReactor.class);

	/** Relative path of recording files within the insight asset folder. */
	private static final String RECORDINGS_REL = "/playwright";

	/** Output path relative to the insight asset folder. */
	private static final String OUTPUT_REL = "/mcp/pixel_mcp.json";

	private final ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public MakeRoomPlaywrightMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), "projectId" };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get("projectId"); // explicit override
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

		// Auto-discover the playwright app project ID from the room's MCP list
		// so the generated tool _meta gets the correct SMSS_PROJECT_ID for the
		// sidebar URL — without requiring the frontend to supply it.
		if ((projectId == null || projectId.isBlank())) {
			if (roomId != null && !roomId.isBlank()) {
				projectId = discoverProjectIdFromRoom(roomId);
			}
		}

		String assetFolder = this.insight.getInsightFolder();
		if (assetFolder == null || assetFolder.isBlank()) {
			throw new IllegalStateException("No insight asset folder is available for this session.");
		}

		// ── Locate recording files ───────────────────────────────────────────
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

		// ── Build tools array ────────────────────────────────────────────────
		JSONArray toolsArray = new JSONArray();
		for (File file : files) {
			try {
				toolsArray.put(createToolFromRecording(file, projectId));
			} catch (Exception e) {
				classLogger.warn("Skipping recording file {} — could not parse: {}", file.getName(), e.getMessage());
			}
		}

		if (toolsArray.isEmpty()) {
			throw new IllegalArgumentException("No valid Playwright recording files could be parsed.");
		}

		// ── Assemble pixel_mcp.json ───────────────────────────────────────────
		JSONObject mcpJson = new JSONObject();
		JSONObject _meta = new JSONObject();
		_meta.put("last_modified_date",
				LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		mcpJson.put("_meta", _meta);
		mcpJson.put("tools", toolsArray);

		// ── Save to insight assets ────────────────────────────────────────────
		String prettyJson = mcpJson.toString(4);
		FileSystemUtil.saveAssetFiles(
				assetFolder,
				List.of(OUTPUT_REL),
				List.of(prettyJson));

		classLogger.info("Saved room MCP ({} tool(s)) to {}{}", toolsArray.length(), assetFolder, OUTPUT_REL);
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	// ── Tool builder (mirrors MakePlaywrightMCPReactor) ──────────────────────

	private JSONObject createToolFromRecording(File file, String projectId) throws Exception {
		StepsEnvelope envelope = json.readValue(file, StepsEnvelope.class);

		String fileName = file.getName();
		String fileNameWithoutExt = fileName.replaceAll("\\.json$", "");
		String title = (envelope.meta() != null && envelope.meta().title() != null
				&& !envelope.meta().title().isBlank())
				? envelope.meta().title()
				: fileNameWithoutExt;

		String baseDescription = (envelope.meta() != null && envelope.meta().description() != null
				&& !envelope.meta().description().isBlank())
				? envelope.meta().description()
				: "Replay room Playwright recording: " + title;

		// Build a richer description using intent so the LLM can match by purpose,
		// not just title. Example: "Replay: Football highlights — opens YouTube
		// and navigates to football highlight videos."
		String intentHint = (envelope.meta() != null && envelope.meta().intent() != null
				&& !envelope.meta().intent().isBlank())
				? envelope.meta().intent()
				: title;
		String richDescription = "Replay: " + intentHint + " — " + baseDescription;

		// ── Collect TYPE steps that have storeValue=true ─────────────────────
		List<PlaywrightStep> inputSteps = new ArrayList<>();
		for (List<List<PlaywrightStep>> stepGroups : envelope.steps().values()) {
			for (List<PlaywrightStep> stepGroup : stepGroups) {
				for (PlaywrightStep step : stepGroup) {
					if (step.type() == PlaywrightStepType.TYPE && step.storeValue()
							&& step.label() != null && !step.label().isBlank()) {
						inputSteps.add(step);
					}
				}
			}
		}

		// ── inputSchema ───────────────────────────────────────────────────────
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", sanitize(title) + "_Arguments");

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// recording_file — fixed to this file's name
		JSONObject recFileProp = new JSONObject();
		recFileProp.put("type", "string");
		recFileProp.put("title", "recording_file");
		recFileProp.put("description", "Room recording file to replay.");
		recFileProp.put("default", fileName);
		recFileProp.put("const", fileName);
		properties.put("recording_file", recFileProp);
		required.put("recording_file");

		// intent (optional default from metadata)
		if (envelope.meta() != null && envelope.meta().intent() != null
				&& !envelope.meta().intent().isBlank()) {
			JSONObject intentProp = new JSONObject();
			intentProp.put("type", "string");
			intentProp.put("title", "intent");
			intentProp.put("description", "The intent or purpose of this recording.");
			intentProp.put("default", envelope.meta().intent());
			properties.put("intent", intentProp);
		}

		// start_url (optional override)
		JSONObject startUrlProp = new JSONObject();
		startUrlProp.put("type", "string");
		startUrlProp.put("title", "start_url");
		startUrlProp.put("description", "Optional URL override before replay.");
		properties.put("start_url", startUrlProp);

		// paramValues — input fields from TYPE steps
		JSONObject paramValuesProp = new JSONObject();
		paramValuesProp.put("type", "object");
		paramValuesProp.put("title", "paramValues");
		if (!inputSteps.isEmpty()) {
			JSONObject paramProps = new JSONObject();
			JSONArray paramReq = new JSONArray();
			for (PlaywrightStep step : inputSteps) {
				String fieldName = sanitize(step.label());
				JSONObject fieldProp = new JSONObject();
				fieldProp.put("type", "string");
				fieldProp.put("title", fieldName);
				fieldProp.put("description", step.label());
				if (step.text() != null && !step.text().isBlank()) {
					fieldProp.put("default", step.text());
				}
				if (step.isPassword()) {
					fieldProp.put("format", "password");
				}
				paramProps.put(fieldName, fieldProp);
				paramReq.put(fieldName);
			}
			paramValuesProp.put("properties", paramProps);
			paramValuesProp.put("required", paramReq);
			paramValuesProp.put("description",
					"Input values for the recording's form fields (" + inputSteps.size() + " field(s)).");
		} else {
			JSONObject addlProps = new JSONObject();
			addlProps.put("type", "string");
			paramValuesProp.put("additionalProperties", addlProps);
			paramValuesProp.put("description", "Additional parameters (none required for this recording).");
		}
		properties.put("paramValues", paramValuesProp);

		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		// ── _meta (SMSS sidebar execution) ────────────────────────────────────
		JSONObject meta = new JSONObject();
		meta.put("SMSS_MCP_EXECUTION", "ask");
		JSONObject mcpUi = new JSONObject();
		mcpUi.put("displayLocation", "sidebar");
		mcpUi.put("resourceURI", "/");
		meta.put("SMSS_MCP_UI", mcpUi);
		meta.put("SMSS_FUNCTION_NAME", "PlayPlaywrightSocketsRoomRecording");
		meta.put("generated_on", LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		// SMSS_PROJECT_ID must be the playwright app project ID so Playground
		// constructs the correct sidebar iframe URL (public_home/{id}/portals/).
		if (projectId != null && !projectId.isBlank()) {
			meta.put("SMSS_PROJECT_ID", projectId);
		}
		// The room MCP owns definition lookup/execution; the project ID above owns
		// only the Playwright sidebar portal.
		meta.put("SMSS_ENGINE_ID", InsightMCP.INSIGHT_MCP_ID);

		// ── Assemble tool — name prefixed with "play_" so the LLM distinguishes
		// playback tools from recording tools ────────────────────────────────
		String toolName = "play_" + sanitize(title);
		JSONObject tool = new JSONObject();
		tool.put("name", toolName);
		tool.put("title", "Play: " + title);
		tool.put("description", richDescription);
		tool.put("inputSchema", inputSchema);
		tool.put("_meta", meta);
		return tool;
	}

	/** Reads room options and returns the first PROJECT-type MCP's ID, or null. */
	@SuppressWarnings("unchecked")
	private String discoverProjectIdFromRoom(String roomId) {
		try {
			String userId = this.insight.getUser().getPrimaryLoginToken().getId();
			java.util.List<java.util.Map<String, Object>> rows =
					ModelInferenceLogsUtils.getRoomOptions(roomId, userId);
			if (rows == null || rows.isEmpty()) return null;
			Object optObj = rows.get(0).get("OPTIONS");
			if (!(optObj instanceof java.util.Map)) return null;
			java.util.Map<String, Object> opts = (java.util.Map<String, Object>) optObj;
			Object mcpObj = opts.get("mcp");
			if (!(mcpObj instanceof java.util.List)) return null;
			for (Object item : (java.util.List<?>) mcpObj) {
				if (!(item instanceof java.util.Map)) continue;
				java.util.Map<String, Object> mcp = (java.util.Map<String, Object>) item;
				if ("PROJECT".equals(mcp.get("type"))) {
					Object id = mcp.get("id");
					if (id instanceof String && !((String) id).isBlank()) {
						classLogger.info("MakeRoomPlaywrightMCP: auto-discovered projectId={} from room={}", id, roomId);
						return (String) id;
					}
				}
			}
		} catch (Exception e) {
			classLogger.warn("MakeRoomPlaywrightMCP: could not discover projectId from room {}: {}", roomId, e.getMessage());
		}
		return null;
	}

	private static String sanitize(String label) {
		String s = label.replaceAll("[^a-zA-Z0-9\\s]", "").trim()
				.replaceAll("\\s+", "_").toLowerCase();
		if (s.isEmpty() || !Character.isLetter(s.charAt(0))) {
			s = "tool_" + s;
		}
		return s;
	}

	@Override
	public String getReactorDescription() {
		return "Generates mcp/pixel_mcp.json for the current room insight from all playwright/*.json recordings "
				+ "saved in the same insight assets. Mirrors MakePlaywrightMCPReactor for room-level recordings.";
	}
}
