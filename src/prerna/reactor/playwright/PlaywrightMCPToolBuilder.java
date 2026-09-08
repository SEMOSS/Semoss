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

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.agent.mcp.MCPUtility;

/**
 * Builds MCP tool definitions from Playwright recordings.
 *
 * <p>
 * Shared by {@link MakePlaywrightMCPReactor} (project scoped) and
 * {@link MakeRoomPlaywrightMCPReactor} (room scoped). The two playback tool
 * contracts differ only in scope wording and in the project id that project
 * recordings pin, so the whole tool is assembled here and each reactor keeps
 * only its own discovery, merge, and persistence work.
 */
public final class PlaywrightMCPToolBuilder {

	/**
	 * Sidebar UI for a playback tool. The {@code system://} scheme tells the
	 * frontend to load the app from the deployed web app rather than from a
	 * published project portal, so the URL needs no project id.
	 */
	public static final String BROWSER_AUTOMATION_APP_URI = "system://browser-automation/";

	/** Pixel every generated playback tool invokes. */
	public static final String PLAYBACK_FUNCTION = "PlayPlaywrightSocketsRoomRecording";

	/**
	 * Prefix on every generated playback tool name, so the LLM can tell playback
	 * tools apart from the recording tools exposed by the platform project.
	 */
	public static final String PLAYBACK_TOOL_PREFIX = "play_";

	/** Fallback prefix for names sanitized from a recording title. */
	public static final String TOOL_NAME_PREFIX = "tool_";

	/** Input schema property holding the pinned recording file name. */
	public static final String RECORDING_FILE = "recording_file";

	/** Input schema property holding the pinned owning project id. */
	public static final String PROJECT_ID = "project_id";

	/** Input schema property holding the optional pre-replay URL override. */
	public static final String START_URL = "start_url";

	/** Input schema property requesting a best-effort full-page capture after replay. */
	public static final String CAPTURE_FULL_PAGE_AT_END = "capture_full_page_at_end";

	/** Input schema property holding the recording's purpose. */
	public static final String INTENT = "intent";

	/** Input schema property holding the recording's form field values. */
	public static final String PARAM_VALUES = "paramValues";

	private PlaywrightMCPToolBuilder() {

	}

	/**
	 * Builds the playback tool for one room recording.
	 *
	 * <p>
	 * One entry of the {@code tools} array:
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
	 *                     "start_url": {...}, "capture_full_page_at_end": {...},
	 *                     "paramValues": {...} },
	 *     "required": ["recording_file"]
	 *   },
	 *   "_meta": { "SMSS_MCP_EXECUTION": "ask", "SMSS_MCP_UI": {...}, ... }
	 * }
	 * </pre>
	 *
	 * @param envelope the parsed recording
	 * @param fileName the recording file name, pinned into the schema
	 * @return the tool definition
	 */
	public static JSONObject buildRoomPlaybackTool(StepsEnvelope envelope, String fileName) {
		return buildPlaybackTool(envelope, fileName, "Room", null);
	}

	/**
	 * Builds the playback tool for one project recording. Same contract as
	 * {@link #buildRoomPlaybackTool}, plus a pinned project id so the system app
	 * loads the recording from the owning app.
	 *
	 * @param envelope  the parsed recording
	 * @param fileName  the recording file name, pinned into the schema
	 * @param projectId the owning project, pinned into the schema and stamped into
	 *                  {@code _meta}
	 * @return the tool definition
	 */
	public static JSONObject buildProjectPlaybackTool(StepsEnvelope envelope, String fileName, String projectId) {
		return buildPlaybackTool(envelope, fileName, "Project", projectId);
	}

	/**
	 * Assembles a playback tool. A non-null {@code projectId} is the only
	 * difference between the project and room contracts.
	 *
	 * @param envelope   the parsed recording
	 * @param fileName   the recording file name, pinned into the schema
	 * @param scopeLabel "Room" or "Project", used in the generated descriptions
	 * @param projectId  the owning project for project recordings, null for room
	 *                   recordings
	 * @return the tool definition
	 */
	private static JSONObject buildPlaybackTool(StepsEnvelope envelope, String fileName, String scopeLabel,
			String projectId) {
		String title = resolveRecordingTitle(envelope, fileName);
		String baseDescription = resolveRecordingDescription(envelope, title,
				"Replay " + scopeLabel.toLowerCase() + " Playwright recording: ");

		// Build a richer description using intent so the LLM can match by purpose,
		// not just title. Example: "Replay: Football highlights - opens YouTube
		// and navigates to football highlight videos."
		String richDescription = "Replay: " + resolveRecordingIntent(envelope, title) + " - " + baseDescription;
		richDescription += " Native browser downloads are captured automatically, saved as individual assets in the current insight under /browser-downloads/<run-id>/, and returned as insight paths; binary contents are not placed in MCP JSON.";

		JSONObject properties = new JSONObject();
		JSONArray required = new JSONArray();

		// One tool per recording, so the file name is pinned to a single value.
		properties.put(RECORDING_FILE,
				pinnedStringProperty(RECORDING_FILE, scopeLabel + " recording file to replay.", fileName));
		required.put(RECORDING_FILE);

		// Pinned too: a project recording exists only in its own project.
		if (projectId != null) {
			properties.put(PROJECT_ID,
					pinnedStringProperty(PROJECT_ID, "App project containing the recording.", projectId));
			required.put(PROJECT_ID);
		}

		// "intent": { "type": "string", "title": "intent",
		// "description": "The intent or purpose of this recording.",
		// "default": "Search for information using Bing" }
		// Not pinned: a model may reword it.
		String intent = envelope.meta() == null ? null : envelope.meta().intent();
		if (intent != null && !intent.isBlank()) {
			JSONObject intentProp = new JSONObject();
			intentProp.put("type", "string");
			intentProp.put("title", INTENT);
			intentProp.put("description", "The intent or purpose of this recording.");
			intentProp.put("default", intent);
			properties.put(INTENT, intentProp);
		}

		// No default: absent means replay whatever the recording navigated to.
		JSONObject startUrlProp = new JSONObject();
		startUrlProp.put("type", "string");
		startUrlProp.put("title", START_URL);
		startUrlProp.put("description", "Optional URL override before replay.");
		properties.put(START_URL, startUrlProp);

		JSONObject fullPageProp = new JSONObject();
		fullPageProp.put("type", "boolean");
		fullPageProp.put("title", CAPTURE_FULL_PAGE_AT_END);
		fullPageProp.put("description",
				"After successful replay, auto-scroll the final page and return its rendered text as context. Native browser downloads are also automatically saved to the current insight and returned as individual insight paths. Best effort; capture errors do not fail replay.");
		fullPageProp.put("default", false);
		properties.put(CAPTURE_FULL_PAGE_AT_END, fullPageProp);

		// One field per TYPE step flagged storeValue; free-form string map when none.
		properties.put(PARAM_VALUES,
				buildParamValuesSchema(collectStoreValueInputs(envelope),
						"Additional parameters (none required for this recording).",
						"Input values for the recording's form fields"));

		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", sanitizeToolName(title, TOOL_NAME_PREFIX) + "_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", required);

		JSONObject tool = new JSONObject();
		tool.put("name", PLAYBACK_TOOL_PREFIX + sanitizeToolName(title, TOOL_NAME_PREFIX));
		tool.put("title", "Play: " + title);
		tool.put("description", richDescription);
		tool.put("inputSchema", inputSchema);
		tool.put("_meta", playbackToolMeta(projectId));
		return tool;
	}

	/**
	 * Builds the {@code _meta} block of a playback tool.
	 *
	 * <p>
	 * {@code SMSS_MCP_UI} opens the remote browser app in the Playground sidebar
	 * and that app performs the replay. {@code SMSS_FUNCTION_NAME} is the pixel
	 * that runs when the tool executes. Both are per-tool choices, so another
	 * generator writing into the same file can omit the UI block and render inline
	 * instead.
	 *
	 * <pre>
	 * {
	 *   "SMSS_MCP_EXECUTION": "ask",
	 *   "SMSS_MCP_UI": { "displayLocation": "sidebar",
	 *                    "resourceURI": "system://browser-automation/" },
	 *   "SMSS_FUNCTION_NAME": "PlayPlaywrightSocketsRoomRecording",
	 *   "generated_on": "2026-07-28"
	 * }
	 * </pre>
	 *
	 * @param projectId stamped as {@code SMSS_ENGINE_ID} and
	 *                  {@code SMSS_PROJECT_ID} when non-null
	 * @return the meta object
	 */
	private static JSONObject playbackToolMeta(String projectId) {
		JSONObject mcpUi = new JSONObject();
		mcpUi.put(MCPUtility.UI_DISPLAY_LOCATION, "sidebar");
		mcpUi.put(MCPUtility.UI_RESOURCE_URI, BROWSER_AUTOMATION_APP_URI);

		JSONObject _meta = new JSONObject();
		_meta.put(MCPUtility.SMSS_MCP_EXECUTION, "ask");
		_meta.put(MCPUtility.SMSS_MCP_UI, mcpUi);
		_meta.put(MCPUtility.SMSS_FUNCTION_NAME, PLAYBACK_FUNCTION);
		if (projectId != null) {
			_meta.put(MCPUtility.SMSS_ENGINE_ID, projectId);
			_meta.put(MCPUtility.SMSS_ENGINE_NAME, BROWSER_AUTOMATION_APP_URI);
		}
		_meta.put("generated_on", todayUtcDate());
		return _meta;
	}

	/**
	 * Turns a human label into a valid MCP tool or property name: strips anything
	 * that is not alphanumeric or whitespace, collapses whitespace to underscores,
	 * and lowercases.
	 *
	 * @param label          the label to sanitize
	 * @param fallbackPrefix prefix applied when the result would not start with a
	 *                       letter, which MCP names require
	 * @return a sanitized name, never blank
	 */
	public static String sanitizeToolName(String label, String fallbackPrefix) {
		String sanitized = label.replaceAll("[^a-zA-Z0-9\\s]", "").trim().replaceAll("\\s+", "_").toLowerCase();
		if (sanitized.isEmpty() || !Character.isLetter(sanitized.charAt(0))) {
			sanitized = fallbackPrefix + sanitized;
		}
		return sanitized;
	}

	/**
	 * Strips a trailing {@code .json} extension. Anchored, so a name that contains
	 * {@code .json} mid-string keeps it.
	 *
	 * @param fileName the recording file name
	 * @return the name without its trailing extension
	 */
	public static String stripJsonExtension(String fileName) {
		return fileName.replaceAll("\\.json$", "");
	}

	/**
	 * Resolves the display title for a recording, preferring the envelope metadata
	 * title and falling back to the file name.
	 *
	 * @param envelope the parsed recording
	 * @param fileName the recording file name, used for the fallback
	 * @return a non-blank title
	 */
	public static String resolveRecordingTitle(StepsEnvelope envelope, String fileName) {
		if (envelope.meta() != null && envelope.meta().title() != null && !envelope.meta().title().isBlank()) {
			return envelope.meta().title();
		}
		return stripJsonExtension(fileName);
	}

	/**
	 * Resolves the description for a recording, preferring the envelope metadata
	 * description.
	 *
	 * @param envelope         the parsed recording
	 * @param title            resolved title, used in the fallback text
	 * @param fallbackTemplate description used when metadata has none, with the
	 *                         title appended
	 * @return a non-blank description
	 */
	public static String resolveRecordingDescription(StepsEnvelope envelope, String title, String fallbackTemplate) {
		if (envelope.meta() != null && envelope.meta().description() != null
				&& !envelope.meta().description().isBlank()) {
			return envelope.meta().description();
		}
		return fallbackTemplate + title;
	}

	/**
	 * Returns the metadata intent when present, otherwise the given fallback.
	 *
	 * @param envelope the parsed recording
	 * @param fallback value to use when no intent is recorded
	 * @return the intent or the fallback
	 */
	public static String resolveRecordingIntent(StepsEnvelope envelope, String fallback) {
		if (envelope.meta() != null && envelope.meta().intent() != null && !envelope.meta().intent().isBlank()) {
			return envelope.meta().intent();
		}
		return fallback;
	}

	/**
	 * Collects the steps that should become tool inputs: TYPE steps flagged
	 * {@code storeValue} that carry a usable label.
	 *
	 * @param envelope the parsed recording
	 * @return the input steps, in recorded order
	 */
	public static List<PlaywrightStep> collectStoreValueInputs(StepsEnvelope envelope) {
		List<PlaywrightStep> inputSteps = new ArrayList<>();
		if (envelope.steps() == null) {
			return inputSteps;
		}
		for (List<List<PlaywrightStep>> stepGroups : envelope.steps().values()) {
			for (List<PlaywrightStep> stepGroup : stepGroups) {
				for (PlaywrightStep step : stepGroup) {
					if (step.type() == PlaywrightStepType.TYPE && step.storeValue() && step.label() != null
							&& !step.label().isBlank()) {
						inputSteps.add(step);
					}
				}
			}
		}
		return inputSteps;
	}

	/**
	 * Builds the {@code paramValues} property of a tool's input schema from the
	 * recording's input steps. With no inputs it degrades to a free-form string map
	 * so the tool still accepts an empty object.
	 *
	 * <pre>
	 * // with input steps
	 * {
	 *   "type": "object", "title": "paramValues",
	 *   "properties": {
	 *     "user_name": { "type": "string", "title": "user_name",
	 *                    "description": "User Name", "default": "bob" },
	 *     "password":  { "type": "string", "title": "password",
	 *                    "description": "Password", "format": "password" }
	 *   },
	 *   "required": ["user_name", "password"],
	 *   "description": "Input values for the recording's form fields (2)"
	 * }
	 *
	 * // with none
	 * {
	 *   "type": "object", "title": "paramValues",
	 *   "additionalProperties": { "type": "string" },
	 *   "description": "Additional parameters (none required for this recording)"
	 * }
	 * </pre>
	 *
	 * @param inputSteps        input steps from {@link #collectStoreValueInputs}
	 * @param emptyDescription  description used when there are no input steps
	 * @param filledDescription description used when there are, with the count
	 *                          appended in parentheses
	 * @return the paramValues schema object
	 */
	public static JSONObject buildParamValuesSchema(List<PlaywrightStep> inputSteps, String emptyDescription,
			String filledDescription) {
		JSONObject paramValuesProp = new JSONObject();
		paramValuesProp.put("type", "object");
		paramValuesProp.put("title", "paramValues");

		if (inputSteps.isEmpty()) {
			JSONObject additionalProps = new JSONObject();
			additionalProps.put("type", "string");
			paramValuesProp.put("additionalProperties", additionalProps);
			paramValuesProp.put("description", emptyDescription);
			return paramValuesProp;
		}

		JSONObject paramProperties = new JSONObject();
		JSONArray paramRequired = new JSONArray();
		for (PlaywrightStep step : inputSteps) {
			String fieldName = sanitizeToolName(step.label(), "field_");
			JSONObject fieldProp = new JSONObject();
			fieldProp.put("type", "string");
			fieldProp.put("title", fieldName);
			fieldProp.put("description", step.label());
			// Password values must never be exposed to the model through an MCP
			// schema default. The field remains required and password-formatted so
			// the user can supply it at execution time.
			if (!step.isPassword() && step.text() != null && !step.text().isBlank()) {
				fieldProp.put("default", step.text());
			}
			if (step.isPassword()) {
				fieldProp.put("format", "password");
			}
			paramProperties.put(fieldName, fieldProp);
			paramRequired.put(fieldName);
		}
		paramValuesProp.put("properties", paramProperties);
		paramValuesProp.put("required", paramRequired);
		paramValuesProp.put("description", filledDescription + " (" + inputSteps.size() + ")");
		return paramValuesProp;
	}

	/**
	 * Builds a string property locked to exactly one allowed value.
	 *
	 * <p>
	 * A bare {@code default} is only a hint - nothing stops a model from sending a
	 * different filename or project id, and the tool then replays the wrong
	 * recording. A single-entry {@code enum} makes the value the only legal input,
	 * which schema-aware providers enforce before the call is ever made, while the
	 * matching {@code default} keeps the parameter fillable without the model
	 * having to echo it back.
	 *
	 * <pre>
	 * {
	 *   "type": "string",
	 *   "title": "recording_file",
	 *   "description": "Room recording file to replay.",
	 *   "enum": ["bing-web-search-20260728.json"],
	 *   "default": "bing-web-search-20260728.json"
	 * }
	 * </pre>
	 *
	 * @param title       property title
	 * @param description property description
	 * @param value       the one permitted value
	 * @return the property schema
	 */
	public static JSONObject pinnedStringProperty(String title, String description, String value) {
		JSONObject property = new JSONObject();
		property.put("type", "string");
		property.put("title", title);
		property.put("description", description);
		property.put("enum", new JSONArray().put(value));
		property.put("default", value);
		return property;
	}

	/**
	 * Today's date in UTC, formatted for the date stamps written into generated MCP
	 * definitions.
	 *
	 * @return the date as {@code yyyy-MM-dd}
	 */
	public static String todayUtcDate() {
		return LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
	}

	/**
	 * Wraps a tools array in the standard MCP definition envelope, stamping today's
	 * date as the last modified date.
	 *
	 * <pre>
	 * {
	 *   "_meta": { "last_modified_date": "2026-07-28" },
	 *   "tools": [ ... ]
	 * }
	 * </pre>
	 *
	 * @param tools the generated tool definitions
	 * @return the object to serialize to {@code mcp/pixel_mcp.json}
	 */
	public static JSONObject wrapMcpJson(JSONArray tools) {
		JSONObject mcpJson = new JSONObject();
		JSONObject meta = new JSONObject();
		meta.put("last_modified_date", todayUtcDate());
		mcpJson.put("_meta", meta);
		mcpJson.put("tools", tools);
		return mcpJson;
	}

}
