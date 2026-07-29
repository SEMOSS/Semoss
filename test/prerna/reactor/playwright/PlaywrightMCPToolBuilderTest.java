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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class PlaywrightMCPToolBuilderTest {

	private static PlaywrightStep typeStep(String label, String text, boolean storeValue, boolean isPassword) {
		return new PlaywrightStep(1, PlaywrightStepType.TYPE, null, null, null, null, text, null, null, null, null,
				null, null, label, null, isPassword, storeValue, null, null, null, null, null, null);
	}

	private static PlaywrightStep clickStep() {
		return new PlaywrightStep(2, PlaywrightStepType.CLICK, null, null, null, null, null, null, null, null, null,
				null, null, "ignored", null, false, true, null, null, null, null, null, null);
	}

	private static StepsEnvelope envelope(RecordingMeta meta, List<PlaywrightStep> steps) {
		return new StepsEnvelope("1.0", meta, Map.of("tab-1", List.of(steps)));
	}

	@Test
	void sanitizeToolNameNormalizesAndAppliesFallbackPrefix() {
		assertEquals("test_label", PlaywrightMCPToolBuilder.sanitizeToolName("Test Label!", "field_"));
		assertEquals("user_name", PlaywrightMCPToolBuilder.sanitizeToolName("User Name", "field_"));
		// MCP names have to start with a letter, so the prefix is applied
		assertEquals("field_123invalid", PlaywrightMCPToolBuilder.sanitizeToolName("123invalid", "field_"));
		assertEquals("tool_123invalid", PlaywrightMCPToolBuilder.sanitizeToolName("123invalid", "tool_"));
		// nothing survives sanitizing, so the result is just the prefix
		assertEquals("tool_", PlaywrightMCPToolBuilder.sanitizeToolName("!!!", "tool_"));
	}

	@Test
	void stripJsonExtensionOnlyRemovesTheTrailingExtension() {
		assertEquals("recording", PlaywrightMCPToolBuilder.stripJsonExtension("recording.json"));
		// anchored, so an inner ".json" is preserved
		assertEquals("a.json.b", PlaywrightMCPToolBuilder.stripJsonExtension("a.json.b.json"));
		assertEquals("no-extension", PlaywrightMCPToolBuilder.stripJsonExtension("no-extension"));
	}

	@Test
	void resolveRecordingTitlePrefersMetadataAndFallsBackToFileName() {
		StepsEnvelope withTitle = envelope(new RecordingMeta(null, "Real Title", null, null, null, null), List.of());
		assertEquals("Real Title", PlaywrightMCPToolBuilder.resolveRecordingTitle(withTitle, "ignored.json"));

		StepsEnvelope blankTitle = envelope(new RecordingMeta(null, "   ", null, null, null, null), List.of());
		assertEquals("from-file", PlaywrightMCPToolBuilder.resolveRecordingTitle(blankTitle, "from-file.json"));

		StepsEnvelope noMeta = envelope(null, List.of());
		assertEquals("from-file", PlaywrightMCPToolBuilder.resolveRecordingTitle(noMeta, "from-file.json"));
	}

	@Test
	void resolveRecordingDescriptionAndIntentFallBack() {
		StepsEnvelope noMeta = envelope(null, List.of());
		assertEquals("Replay: My Title",
				PlaywrightMCPToolBuilder.resolveRecordingDescription(noMeta, "My Title", "Replay: "));
		assertEquals("My Title", PlaywrightMCPToolBuilder.resolveRecordingIntent(noMeta, "My Title"));

		StepsEnvelope withIntent = envelope(new RecordingMeta(null, null, "A description", null, null, "An intent"),
				List.of());
		assertEquals("A description",
				PlaywrightMCPToolBuilder.resolveRecordingDescription(withIntent, "My Title", "Replay: "));
		assertEquals("An intent", PlaywrightMCPToolBuilder.resolveRecordingIntent(withIntent, "My Title"));
	}

	@Test
	void collectStoreValueInputsKeepsOnlyLabelledTypeSteps() {
		StepsEnvelope env = envelope(null,
				List.of(typeStep("Username", "bob", true, false), typeStep("Skipped", "x", false, false), // storeValue
																											// false
						typeStep(null, "x", true, false), // no label
						typeStep("   ", "x", true, false), // blank label
						clickStep())); // wrong type

		List<PlaywrightStep> inputs = PlaywrightMCPToolBuilder.collectStoreValueInputs(env);
		assertEquals(1, inputs.size());
		assertEquals("Username", inputs.get(0).label());
	}

	@Test
	void collectStoreValueInputsToleratesMissingSteps() {
		StepsEnvelope env = new StepsEnvelope("1.0", null, null);
		assertTrue(PlaywrightMCPToolBuilder.collectStoreValueInputs(env).isEmpty());
	}

	@Test
	void buildParamValuesSchemaDescribesEachInputField() {
		List<PlaywrightStep> inputs = List.of(typeStep("User Name", "bob", true, false),
				typeStep("Password", "must-not-leak", true, true));

		JSONObject schema = PlaywrightMCPToolBuilder.buildParamValuesSchema(inputs, "empty", "filled");

		assertEquals("object", schema.getString("type"));
		assertEquals("filled (2)", schema.getString("description"));
		JSONObject props = schema.getJSONObject("properties");

		JSONObject userName = props.getJSONObject("user_name");
		assertEquals("bob", userName.getString("default"));
		assertFalse(userName.has("format"));

		JSONObject password = props.getJSONObject("password");
		assertEquals("password", password.getString("format"));
		// Password text can exist in legacy recordings but must not reach the model.
		assertFalse(password.has("default"));

		assertEquals(2, schema.getJSONArray("required").length());
	}

	@Test
	void buildParamValuesSchemaFallsBackToFreeFormMapWhenNoInputs() {
		JSONObject schema = PlaywrightMCPToolBuilder.buildParamValuesSchema(List.of(), "none required", "filled");

		assertEquals("none required", schema.getString("description"));
		assertEquals("string", schema.getJSONObject("additionalProperties").getString("type"));
		assertFalse(schema.has("properties"));
		assertFalse(schema.has("required"));
	}

	@Test
	void pinnedStringPropertyAllowsExactlyOneValue() {
		JSONObject prop = PlaywrightMCPToolBuilder.pinnedStringProperty("recordedFile", "Which recording",
				"my-recording.json");

		assertEquals("string", prop.getString("type"));
		assertEquals("recordedFile", prop.getString("title"));
		assertEquals("Which recording", prop.getString("description"));
		// the enum is what actually constrains the model; the default just prefills
		assertEquals(1, prop.getJSONArray("enum").length());
		assertEquals("my-recording.json", prop.getJSONArray("enum").getString(0));
		assertEquals("my-recording.json", prop.getString("default"));
	}

	@Test
	void wrapMcpJsonStampsTheDateAndKeepsTools() {
		JSONObject wrapped = PlaywrightMCPToolBuilder.wrapMcpJson(new org.json.JSONArray().put(new JSONObject()));

		assertTrue(wrapped.has("tools"));
		assertEquals(1, wrapped.getJSONArray("tools").length());
		assertEquals(PlaywrightMCPToolBuilder.todayUtcDate(),
				wrapped.getJSONObject("_meta").getString("last_modified_date"));
	}
}
