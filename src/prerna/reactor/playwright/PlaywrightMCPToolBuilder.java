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

/**
 * Pure helpers for turning Playwright recordings into MCP tool definitions.
 *
 * <p>
 * Shared by {@link MakePlaywrightMCPReactor} (project scoped) and
 * {@link MakeRoomPlaywrightMCPReactor} (room scoped). Those two reactors expose
 * deliberately different tool contracts - different parameter names, required
 * sets, and {@code _meta} - so only the mechanics live here.
 */
public final class PlaywrightMCPToolBuilder {

	private PlaywrightMCPToolBuilder() {

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
			if (step.text() != null && !step.text().isBlank()) {
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
