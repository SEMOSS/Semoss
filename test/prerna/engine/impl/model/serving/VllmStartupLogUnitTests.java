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
package prerna.engine.impl.model.serving;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.serving.VllmStartupLog.Milestone;

class VllmStartupLogUnitTests {

	@Test
	void readsMilestonesAndDetailsFromVllm011() {
		String body = Fixtures.text("vllm-startup.log");
		assertTrue(body.contains("\u001B["), "the recorded log keeps vLLM's color codes");

		VllmStartupLog log = VllmStartupLog.parse(List.of(body.split("\n")));

		for (Milestone milestone : Milestone.values()) {
			assertTrue(log.hasReached(milestone), milestone.name());
		}
		assertEquals(Instant.parse("2026-10-26T23:58:17.641115406Z"), log.timeOf(Milestone.WEIGHTS_LOADED));
		assertEquals(Instant.parse("2026-10-26T23:58:59.175699633Z"), log.timeOf(Milestone.STARTUP_COMPLETE));
		assertEquals("Olmo2ForCausalLM", log.getArchitecture());
		assertEquals(4096, log.getMaxModelLen());
		assertEquals(2.7669, log.getWeightsGiB());
		assertEquals(19.63, log.getKvCacheGiB());
	}

	@Test
	void partialLogReachesOnlyEarlyMilestones() {
		VllmStartupLog log = VllmStartupLog.parse(Fixtures.vllmLogUntil("Loading weights took"));
		assertTrue(log.hasReached(Milestone.WEIGHTS_LOADED));
		assertFalse(log.hasReached(Milestone.COMPILED));
		assertNull(log.timeOf(Milestone.STARTUP_COMPLETE));
		assertNull(log.getKvCacheGiB());
	}

	@Test
	void linesDropColorsAndProbeAccessLines() {
		String body = Fixtures.text("vllm-startup.log");

		List<String> quiet = VllmStartupLog.lines(body, false);
		List<String> all = VllmStartupLog.lines(body, true);

		assertTrue(all.size() > quiet.size());
		assertTrue(all.stream().anyMatch(line -> line.contains("\"GET /health HTTP/1.1\" 200")));
		assertTrue(quiet.stream().noneMatch(line -> line.contains("GET /health")));
		assertTrue(all.stream().noneMatch(line -> line.contains("\u001B")));
		assertTrue(quiet.get(quiet.size() - 1).endsWith("Application startup complete."));
	}

	@Test
	void keepsRequestAccessLines() {
		String chat = "2026-10-27T00:10:00Z (APIServer pid=1) INFO:     10.42.0.1:1 - \"POST /v1/chat/completions HTTP/1.1\" 200 OK";
		assertFalse(VllmStartupLog.isAccessLog(chat));
		assertEquals(List.of(chat), VllmStartupLog.lines(chat + "\n", false));
	}

	@Test
	void mostTellingLineIsTheLastError() {
		assertEquals("RuntimeError: Engine core initialization failed.",
				VllmStartupLog.mostTellingLine(List.of("2026-10-27T00:00:01Z (EngineCore_DP0 pid=1) Traceback:",
						"2026-10-27T00:00:02Z (EngineCore_DP0 pid=1) RuntimeError: Engine core initialization failed.",
						"2026-10-27T00:00:03Z (APIServer pid=1) shutting down")));
		assertEquals("plain last line", VllmStartupLog.mostTellingLine(List.of("first", "plain last line")));
		assertNull(VllmStartupLog.mostTellingLine(List.of()));
	}

	@Test
	void aMessageLosesItsLogPrefixes() {
		assertEquals("ValueError: No available memory for the cache blocks.", VllmStartupLog.message("2026-10-27T00:00:02Z "
				+ "(EngineCore_DP0 pid=129) ERROR 10-27 00:00:02 [core.py:708] ValueError: No available memory for the cache blocks."));
		assertEquals("Storage initialization failed: x", VllmStartupLog.message("2026-10-27T15:07:00.775-04:00 2026-10-27 "
				+ "19:07:00.775 1 storage.initializer ERROR [initializer-entrypoint:<module>():28] Storage initialization failed: x"));
		assertEquals("HuggingFace error for hf://a/b: 401 Client Error.",
				VllmStartupLog.message("HuggingFace error for hf://a/b: 401 Client Error. (Request ID: Root=1-abc;def)"));
		assertEquals("plain text stays", VllmStartupLog.message("plain text stays"));
	}
}
