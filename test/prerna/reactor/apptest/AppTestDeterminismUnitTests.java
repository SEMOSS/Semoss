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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

import prerna.util.DIHelper;

/** What makes a replay deterministic: exact checks, a fixed environment, honest failure classes, repeats. */
class AppTestDeterminismUnitTests {
	@TempDir Path temporary;
	private HttpServer server;
	private URI root;
	private final AtomicInteger requests = new AtomicInteger();

	@BeforeEach
	void serveApp() throws Exception {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/Monolith/public_home/app-two/portals/", exchange -> {
			boolean authenticated = "JSESSIONID=test-session".equals(exchange.getRequestHeaders().getFirst("Cookie"));
			String html = "<html><body><div id='display'>19</div><div id='env'></div><div id='clock'></div>"
					+ "<div id='flaky'>" + (requests.getAndIncrement() % 2 == 0 ? "A" : "B") + "</div>"
					+ "<script>document.querySelector('#env').textContent = [navigator.language,"
					+ " Intl.DateTimeFormat().resolvedOptions().timeZone,"
					+ " matchMedia('(prefers-reduced-motion: reduce)').matches].join('|');"
					+ " document.querySelector('#clock').textContent = new Date().toISOString().slice(0, 10);</script>"
					+ "</body></html>";
			byte[] bytes = (authenticated ? html : "Unauthorized").getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/html");
			exchange.sendResponseHeaders(authenticated ? 200 : 401, bytes.length);
			exchange.getResponseBody().write(bytes);
			exchange.close();
		});
		server.start();
		root = AppTestRunner.appRoot("http://127.0.0.1:" + server.getAddress().getPort() + "/Monolith", "app-two");
	}

	@AfterEach
	void stopServer() { server.stop(0); }

	@Test
	void exactTextRejectsASubstringThatTheDefaultAccepts() {
		assertEquals("PASSED", status(run(1, check("#display", "9", null))));
		Map<String, Object> exact = test(run(1, check("#display", "9", true)));
		assertEquals("FAILED", exact.get("status"));
		assertEquals("REGRESSION_SUSPECTED", exact.get("classification"));
		assertEquals("PASSED", status(run(1, check("#display", "19", true))));
	}

	@Test
	void aMissingElementIsNotCalledARegression() {
		Map<String, Object> click = test(run(1, Map.of("type", "CLICK", "selector", "#gone")));
		assertEquals("FAILED", click.get("status"));
		assertEquals("TARGET_NOT_FOUND", click.get("classification"));
		Map<String, Object> visible = test(run(1, Map.of("type", "ASSERT_VISIBLE", "selector", "#gone")));
		assertEquals("TARGET_NOT_FOUND", visible.get("classification"));
	}

	@Test
	void everyReplayRunsInTheSameLocaleTimezoneAndMotionSetting() {
		assertEquals("PASSED", status(run(1, check("#env", "en-US|UTC|true", true))));
	}

	@Test
	void aConfiguredFixedTimeStartsThePageClock() {
		Properties properties = DIHelper.getInstance().getCoreProp();
		Object prior = properties.get(AppTestRunner.FIXED_TIME_PROP);
		try {
			properties.setProperty(AppTestRunner.FIXED_TIME_PROP, "2026-01-02T03:04:05Z");
			assertEquals("PASSED", status(run(1, check("#clock", "2026-01-02", true))));
		} finally {
			if (prior == null) properties.remove(AppTestRunner.FIXED_TIME_PROP); else properties.put(AppTestRunner.FIXED_TIME_PROP, prior);
		}
	}

	@Test
	void repeatedReplaysPassOnlyWhenEveryReplayPasses() {
		Map<String, Object> stable = test(run(3, check("#display", "19", true)));
		assertEquals("PASSED", stable.get("status"));
		assertEquals(3, stable.get("repeats"));
		assertEquals(3, stable.get("passedRepeats"));
	}

	@Test
	void replaysThatDisagreeAreFlakyNotPassed() {
		// the page alternates A, B, A on successive loads
		Map<String, Object> flaky = test(run(3, check("#flaky", "A", true)));
		assertEquals("FAILED", flaky.get("status"));
		assertEquals("FLAKY", flaky.get("classification"));
		assertEquals(2, flaky.get("passedRepeats"));
	}

	@Test
	void aggregationKeepsTheFirstFailureAndNeverReportsAPartialRunAsStable() {
		Map<String, Object> pass = Map.of("status", "PASSED");
		Map<String, Object> fail = Map.of("status", "FAILED", "classification", "REGRESSION_SUSPECTED", "failedStep", 2);
		assertEquals("PASSED", AppTestRunner.aggregateRepeats(List.of(pass, pass, pass), 3).get("status"));
		Map<String, Object> allFail = AppTestRunner.aggregateRepeats(List.of(fail, fail, fail), 3);
		assertEquals("REGRESSION_SUSPECTED", allFail.get("classification"));
		assertEquals(2, allFail.get("failedStep"));
		assertEquals("FLAKY", AppTestRunner.aggregateRepeats(List.of(pass, fail, pass), 3).get("classification"));
		Map<String, Object> cutShort = AppTestRunner.aggregateRepeats(List.of(pass), 3);
		assertEquals("ERROR", cutShort.get("status"));
		assertEquals("PASSED", AppTestRunner.aggregateRepeats(List.of(pass), 1).get("status"));
	}

	@Test
	void exactFlagMustBeABoolean() {
		Map<String, Object> step = new java.util.LinkedHashMap<>(check("#display", "19", null));
		step.put("exact", "yes");
		assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.validate(recording(step)));
		Map<String, Object> saved = AppTestRecordings.validate(recording(check("#display", "19", true)));
		assertEquals(Boolean.TRUE, ((Map<?, ?>) ((List<?>) saved.get("steps")).getFirst()).get("exact"));
	}

	@Test
	void stableSelectorsAreOnlyDataTestIdTargets() {
		assertTrue(AppTestRecordings.stableSelector("button[data-testid=\"button-7\"]"));
		assertTrue(AppTestRecordings.stableSelector("[data-test=\"save\"]"));
		assertFalse(AppTestRecordings.stableSelector("#save"));
		assertFalse(AppTestRecordings.stableSelector("div > button:nth-of-type(2)"));
		assertFalse(AppTestRecordings.stableSelector("button[data-testid=\"a\"] span"));
		assertFalse(AppTestRecordings.stableSelector(null));
	}

	@Test
	void theStablePolicyAppliesWhenSavingNotWhenReadingExistingRecordings() throws Exception {
		Properties properties = DIHelper.getInstance().getCoreProp();
		Object prior = properties.get(AppTestRecordings.REQUIRE_TESTID_PROP);
		try {
			Map<String, Object> weak = recording(Map.of("type", "CLICK", "selector", "#save"));
			properties.setProperty(AppTestRecordings.REQUIRE_TESTID_PROP, "false");
			Map<String, Object> saved = AppTestRecordings.save(temporary, weak);
			properties.setProperty(AppTestRecordings.REQUIRE_TESTID_PROP, "true");
			assertThrows(IllegalArgumentException.class, () -> AppTestRecordings.save(temporary, weak));
			// an already saved recording must still list, or one old file would block every run
			assertEquals(1, AppTestRecordings.list(temporary).size());
			AppTestRecordings.save(temporary, recording(Map.of("type", "CLICK", "selector", "button[data-testid=\"save\"]")));
			assertEquals(2, AppTestRecordings.list(temporary).size());
			assertNotNull(saved.get("id"));
		} finally {
			if (prior == null) properties.remove(AppTestRecordings.REQUIRE_TESTID_PROP); else properties.put(AppTestRecordings.REQUIRE_TESTID_PROP, prior);
		}
	}

	private static Map<String, Object> check(String selector, String text, Boolean exact) {
		return exact == null ? Map.of("type", "ASSERT_TEXT", "selector", selector, "text", text)
				: Map.of("type", "ASSERT_TEXT", "selector", selector, "text", text, "exact", exact);
	}

	private static Map<String, Object> recording(Map<String, Object> step) {
		return Map.of("name", "Example " + UUID.randomUUID(), "description", "Verify deterministic replay of the selected action or assertion.", "startPath", "", "steps", List.of(step));
	}

	private Map<String, Object> run(int repeat, Map<String, Object> step) {
		Map<String, Object> recording = AppTestRecordings.validate(recording(step));
		return AppTestRunner.run(UUID.randomUUID().toString(), "app-two", root, "test-session", "JSESSIONID",
				List.of(recording), temporary, System.nanoTime() + AppTestRunner.RUN_TIMEOUT_MS * 3 * 1_000_000, repeat);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> test(Map<String, Object> report) { return ((List<Map<String, Object>>) report.get("tests")).getFirst(); }

	private String status(Map<String, Object> report) { return String.valueOf(test(report).get("status")); }
}
