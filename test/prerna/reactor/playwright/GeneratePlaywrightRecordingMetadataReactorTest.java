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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class GeneratePlaywrightRecordingMetadataReactorTest {

	@Test
	void actionTraceNeverContainsTypedValuesSelectorsOrUrlParameters() {
		PlaywrightStep typed = step(2, PlaywrightStepType.TYPE, 200L, null, "Email address",
				"private.user@example.com", new Selector("css", "#private-user-example-com", null));
		PlaywrightStep navigated = step(1, PlaywrightStepType.NAVIGATE, 100L,
				"https://example.com/login?email=private.user@example.com&token=secret#account", null, null, null);
		PlaywrightStep clicked = step(3, PlaywrightStepType.CLICK, 300L, null, "Sign in", null, null);

		Map<String, List<List<PlaywrightStep>>> tabs = new LinkedHashMap<>();
		// Insert tab-2 first to prove chronology is based on timestamps, not map order.
		tabs.put("tab-2", List.of(List.of(clicked)));
		tabs.put("tab-1", List.of(List.of(typed, navigated)));

		String trace = GeneratePlaywrightRecordingMetadataReactor
				.buildActionTrace(new StepsEnvelope("1.0", null, tabs));

		assertTrue(trace.indexOf("Navigated") < trace.indexOf("Entered"));
		assertTrue(trace.indexOf("Entered") < trace.indexOf("Clicked"));
		assertTrue(trace.contains("Entered a redacted value into \"Email address\""));
		assertTrue(trace.contains("https://example.com/login"));
		assertFalse(trace.contains("private.user@example.com"));
		assertFalse(trace.contains("token="));
		assertFalse(trace.contains("#account"));
		assertFalse(trace.contains("#private-user-example-com"));
		assertEquals("private.user@example.com", typed.text(),
				"Metadata generation must not mutate the replayable recording");
	}

	@Test
	void sanitizerRemovesCommonSensitiveValues() {
		String input = "email=jane@example.com password=hunter2 Bearer abcdefghijklmnopqrstuvwxyz "
				+ "phone: +1 212-555-0199 card=4111 1111 1111 1111";
		String sanitized = RecordingMetadataPrivacy.sanitizeText(input, 1000);

		assertFalse(sanitized.contains("jane@example.com"));
		assertFalse(sanitized.contains("hunter2"));
		assertFalse(sanitized.contains("abcdefghijklmnopqrstuvwxyz"));
		assertFalse(sanitized.contains("212-555-0199"));
		assertFalse(sanitized.contains("4111 1111 1111 1111"));
		assertTrue(sanitized.contains(RecordingMetadataPrivacy.REDACTED));
	}

	@Test
	void resolvesPlaygroundModelFromLegacyRoomOptions() {
		String modelId = GeneratePlaywrightRecordingMetadataReactor.modelIdFromOptions(
				Map.of("modelId", "7f471d92-dee2-4880-9322-a8b9a395b2b5"));

		assertEquals("7f471d92-dee2-4880-9322-a8b9a395b2b5", modelId);
	}

	@Test
	void resolvesNestedModelOptionForCompatibility() {
		String modelId = GeneratePlaywrightRecordingMetadataReactor
				.modelIdFromOptions(Map.of("model", Map.of("engine_id", "model-from-options")));

		assertEquals("model-from-options", modelId);
	}

	private static PlaywrightStep step(int id, PlaywrightStepType type, Long timestamp, String url, String label,
			String text, Selector selector) {
		return new PlaywrightStep(id, type, url, null, null, null, text, null, null, null, null, null, timestamp, label,
				null, false, false, selector, null, true, false, null, type == PlaywrightStepType.TYPE ? "input" : "button");
	}
}
