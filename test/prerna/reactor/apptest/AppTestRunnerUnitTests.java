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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpServer;

/** Runs real Chromium against a small authenticated app, without a live SEMOSS database. */
class AppTestRunnerUnitTests {
	@TempDir Path temporary;
	private HttpServer server;
	private URI root;
	private final AtomicBoolean throwRuntimeError = new AtomicBoolean();
	private final AtomicBoolean fixedLayout = new AtomicBoolean();

	@BeforeEach
	void serveApp() throws Exception {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/Monolith/public_home/app-one/portals/", exchange -> {
			boolean authenticated = "JSESSIONID=test-session".equals(exchange.getRequestHeaders().getFirst("Cookie"));
			String html = "<html><body><input id='name'><select id='choice'><option value='one'>One</option><option value='two'>Two</option></select>"
					+ "<input id='agree' type='checkbox'><button id='save' onclick=\"document.querySelector('#result').textContent='Saved '+document.querySelector('#name').value\">Save</button>"
					+ "<div id='result'>Ready</div><iframe id='child' srcdoc=\"<span id='nested'>Nested content</span>\"></iframe>"
					+ "<a id='escape' href='/elsewhere'>Leave app</a>"
					+ (throwRuntimeError.get() ? "<script>throw new Error('smoke failure');</script>" : "")
					+ "</body></html>";
			if (fixedLayout.get()) html = "<html><body style='margin:0;height:0'><main style='position:fixed;left:10px;top:10px;width:160px;height:60px'>Fixed app content</main></body></html>";
			byte[] bytes = (authenticated ? html : "Unauthorized").getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/html");
			exchange.sendResponseHeaders(authenticated ? 200 : 401, bytes.length);
			exchange.getResponseBody().write(bytes);
			exchange.close();
		});
		server.start();
		root = AppTestRunner.appRoot("http://127.0.0.1:" + server.getAddress().getPort() + "/Monolith", "app-one");
	}

	@AfterEach
	void stopServer() { server.stop(0); }

	@Test
	void explicitPlatformSmokeWorksWithoutSavedRecordingsAndReportsLimitedCoverage() {
		assertTrue(AppTestRuns.selectRecordings(List.of(), null, List.of()).isEmpty());
		List<Map<String, Object>> selected = AppTestRuns.selectRecordings(List.of(), AppTestRuns.PLATFORM_SMOKE_SUITE, List.of());
		assertEquals(1, selected.size());
		Map<String, Object> report = AppTestRunner.run("smoke", "app-one", root, "test-session", "JSESSIONID", selected, temporary);
		assertEquals("PAGE_VISIBILITY_SMOKE", report.get("scope"));
		assertEquals("PAGE_VISIBILITY_SMOKE", first(report).get("scope"));
		assertEquals("PASSED", first(report).get("status"));
		assertEquals(1L, first(report).get("assertions"));
		assertEquals(0, first(report).get("runtimeErrorCount"));
		assertTrue(String.valueOf(report.get("coverageWarning")).contains("no workflow coverage"));
		assertFalse(java.nio.file.Files.exists(temporary.resolve("recordings")));
	}

	@Test
	void platformSmokeAcceptsVisibleFixedContentWithZeroHeightBody() {
		fixedLayout.set(true);
		Map<String, Object> report = AppTestRunner.run("fixed-smoke", "app-one", root, "test-session", "JSESSIONID",
				AppTestRuns.selectRecordings(List.of(), AppTestRuns.PLATFORM_SMOKE_SUITE, List.of()), temporary);
		assertEquals("PASSED", first(report).get("status"));
		assertEquals("PAGE_VISIBILITY_SMOKE", report.get("scope"));
	}

	@Test
	void platformSmokeFailsOnObservedUnhandledRuntimeErrors() {
		throwRuntimeError.set(true);
		List<Map<String, Object>> selected = AppTestRuns.selectRecordings(List.of(), AppTestRuns.PLATFORM_SMOKE_SUITE, List.of());
		Map<String, Object> report = AppTestRunner.run("smoke-errors", "app-one", root, "test-session", "JSESSIONID", selected, temporary);
		assertEquals("FAILED", first(report).get("status"));
		assertEquals("REGRESSION_SUSPECTED", first(report).get("classification"));
		assertEquals(1, first(report).get("runtimeErrorCount"));
		assertEquals("PAGE_VISIBILITY_SMOKE", report.get("scope"));
	}

	@Test
	void authenticatesReplaysControlsAndChecksNestedFrame() throws Exception {
		List<Map<String, Object>> steps = new ArrayList<>();
		steps.add(Map.of("type", "FILL", "selector", "#name", "value", "Ada"));
		steps.add(Map.of("type", "SELECT", "selector", "#choice", "value", "two"));
		steps.add(Map.of("type", "CHECK", "selector", "#agree", "checked", true));
		steps.add(Map.of("type", "CLICK", "selector", "#save"));
		steps.add(Map.of("type", "ASSERT_TEXT", "selector", "#result", "text", "Saved Ada"));
		steps.add(Map.of("type", "ASSERT_TEXT", "selector", "#nested", "framePath", List.of("#child"), "text", "Nested content"));
		Map<String, Object> report = run("test-session", steps);
		assertEquals("COMPLETED", report.get("status"));
		assertEquals("PASSED", first(report).get("status"));
		assertEquals(2L, first(report).get("assertions"));
		assertTrue(java.nio.file.Files.exists(temporary.resolve(report.get("runId") + ".json")));
		String savedReport = java.nio.file.Files.readString(temporary.resolve(report.get("runId") + ".json"));
		assertFalse(savedReport.contains("test-session"));
	}

	@Test
	void actionOnlyRecordingDoesNotClaimVerification() {
		Map<String, Object> report = run("test-session", List.of(Map.of("type", "CLICK", "selector", "#save")));
		assertEquals("SKIPPED", first(report).get("status"));
		assertEquals("NO_ASSERTIONS", first(report).get("classification"));
		assertEquals(0L, ((Map<?, ?>) report.get("summary")).get("passed"));
	}

	@Test
	void failedAssertionHasStepAndEvidence() {
		Map<String, Object> report = run("test-session", List.of(Map.of("type", "ASSERT_TEXT", "selector", "#result", "text", "Wrong expectation")));
		assertEquals("FAILED", first(report).get("status"));
		assertEquals("REGRESSION_SUSPECTED", first(report).get("classification"));
		assertEquals(1, first(report).get("failedStep"));
		assertNotNull(first(report).get("evidence"));
	}

	@Test
	void expiredSessionIsAnEnvironmentError() {
		Map<String, Object> report = run("expired-session", List.of(Map.of("type", "ASSERT_VISIBLE", "selector", "#result")));
		assertEquals("ERROR", report.get("status"));
		assertEquals("ERROR", first(report).get("classification"));
		assertTrue(String.valueOf(first(report).get("reason")).contains("session"));
	}

	@Test
	void queuedRunExpiresWithoutStartingABrowser() {
		Map<String, Object> report = AppTestRunner.run("expired", "app-one", root, "test-session", "JSESSIONID",
				AppTestRuns.selectRecordings(List.of(), AppTestRuns.PLATFORM_SMOKE_SUITE, List.of()), temporary, System.nanoTime() - 1);
		assertEquals("ERROR", report.get("status"));
		assertEquals("ERROR", first(report).get("classification"));
		assertEquals("PAGE_VISIBILITY_SMOKE", report.get("scope"));
	}

	@Test
	void navigationCannotLeaveTargetApp() {
		assertFalse(AppTestRunner.insideApp(root, root.resolve("../other/").toString()));
		assertFalse(AppTestRunner.insideApp(root, "http://example.com/"));
		Map<String, Object> report = run("test-session", List.of(Map.of("type", "CLICK", "selector", "#escape"), Map.of("type", "ASSERT_VISIBLE", "selector", "#result")));
		assertEquals("ERROR", first(report).get("status"));
		assertTrue(String.valueOf(first(report).get("reason")).contains("Navigation"));
	}

	private Map<String, Object> run(String session, List<Map<String, Object>> steps) {
		Map<String, Object> recording = AppTestRecordings.validate(Map.of("name", "Example", "startPath", "", "steps", steps));
		return AppTestRunner.run(UUID.randomUUID().toString(), "app-one", root, session, "JSESSIONID", List.of(recording), temporary);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> first(Map<String, Object> report) { return ((List<Map<String, Object>>) report.get("tests")).getFirst(); }
}
