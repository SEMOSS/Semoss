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

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;

import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.FrameLocator;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.Response;
import com.microsoft.playwright.TimeoutError;
import com.microsoft.playwright.assertions.LocatorAssertions;
import com.microsoft.playwright.options.Cookie;
import com.microsoft.playwright.options.ServiceWorkerPolicy;
import com.microsoft.playwright.options.ScreenshotType;
import com.microsoft.playwright.options.WaitUntilState;

/** One run owns all Playwright objects, on one worker thread, until they close. */
final class AppTestRunner {
	static final long RUN_TIMEOUT_MS = 120_000;
	private static final double STEP_TIMEOUT_MS = 5_000;

	private AppTestRunner() { }

	static URI appRoot(String applicationUrl, String projectId) {
		if (projectId == null || !projectId.matches("[A-Za-z0-9_-]{1,128}")) throw new IllegalArgumentException("Invalid app project id");
		URI module = URI.create(applicationUrl.endsWith("/") ? applicationUrl : applicationUrl + "/");
		if (!List.of("http", "https").contains(module.getScheme()) || module.getHost() == null
				|| module.getUserInfo() != null || module.getQuery() != null || module.getFragment() != null) {
			throw new IllegalArgumentException("SEMOSS application URL is not configured");
		}
		return module.resolve("public_home/" + projectId + "/portals/");
	}

	static boolean insideApp(URI root, String url) {
		try {
			URI target = URI.create(url).normalize();
			String path = target.getPath();
			if (path == null || path.contains("\\") || path.contains("%")) return false;
			for (String part : path.split("/")) if (part.equals(".") || part.equals("..")) return false;
			return sameOrigin(root, target) && target.getUserInfo() == null && path.startsWith(root.getPath());
		} catch (RuntimeException e) {
			return false;
		}
	}

	private static boolean sameOrigin(URI left, URI right) {
		return left.getScheme().equalsIgnoreCase(String.valueOf(right.getScheme()))
				&& left.getHost().equalsIgnoreCase(String.valueOf(right.getHost()))
				&& port(left) == port(right);
	}

	private static int port(URI uri) {
		return uri.getPort() >= 0 ? uri.getPort() : "https".equals(uri.getScheme()) ? 443 : 80;
	}

	static Map<String, Object> run(String runId, String target, URI root, String sessionId, String cookieName,
			List<Map<String, Object>> recordings, Path reportFolder) {
		return run(runId, target, root, sessionId, cookieName, recordings, reportFolder,
				System.nanoTime() + RUN_TIMEOUT_MS * 1_000_000);
	}

	static Map<String, Object> run(String runId, String target, URI root, String sessionId, String cookieName,
			List<Map<String, Object>> recordings, Path reportFolder, long deadline) {
		List<Map<String, Object>> tests = new ArrayList<>();
		// Browser provisioning belongs to deployment; never download binaries in a user job.
		try {
			if (System.nanoTime() >= deadline) throw new IllegalStateException("Run expired in queue");
			try (Playwright playwright = Playwright.create(new Playwright.CreateOptions().setEnv(Map.of("PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD", "1")));
					Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true).setTimeout(30_000))) {
				for (Map<String, Object> recording : recordings) {
					if (System.nanoTime() >= deadline) throw new IllegalStateException("Run exceeded its time limit");
					tests.add(replay(browser, root, sessionId, cookieName, recording, deadline));
				}
			}
		} catch (RuntimeException | AssertionError e) {
			Map<String, Object> error = new LinkedHashMap<>();
			error.put("name", "Runner");
			error.put("page", "");
			error.put("status", "ERROR");
			error.put("classification", "ERROR");
			error.put("scope", recordings.stream().anyMatch(r -> "PAGE_VISIBILITY_SMOKE".equals(r.get("scope")))
					? "PAGE_VISIBILITY_SMOKE" : "SAVED_WORKFLOW_REGRESSION");
			error.put("reason", "Browser runner could not start or exceeded its time limit. Check server Playwright/browser installation and application URL.");
			tests.add(error);
		}
		Map<String, Object> report = report(runId, target, tests);
		try {
			AppTestRecordings.write(reportFolder.resolve(runId + ".json"), report);
		} catch (java.io.IOException e) {
			report.put("reportWarning", "Run completed, but its report could not be persisted");
		}
		return report;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> replay(Browser browser, URI root, String sessionId, String cookieName,
			Map<String, Object> recording, long deadline) {
		Map<String, Object> test = new LinkedHashMap<>();
		test.put("id", recording.get("id"));
		test.put("name", recording.get("name"));
		test.put("page", recording.get("startPath"));
		boolean smoke = "PAGE_VISIBILITY_SMOKE".equals(recording.get("scope"));
		test.put("scope", smoke ? "PAGE_VISIBILITY_SMOKE" : "SAVED_WORKFLOW_REGRESSION");
		if (smoke) test.put("coverageWarning", "Page visibility and immediate runtime errors only; no workflow coverage");
		List<Map<String, Object>> steps = (List<Map<String, Object>>) recording.get("steps");
		long assertionCount = steps.stream().filter(s -> String.valueOf(s.get("type")).startsWith("ASSERT_")).count();
		test.put("assertions", assertionCount);
		test.put("steps", steps.size());
		int currentStep = 0;
		int completedAssertions = 0;
		AtomicBoolean unauthorized = new AtomicBoolean();
		AtomicBoolean unsafeNavigation = new AtomicBoolean();
		AtomicInteger runtimeErrors = new AtomicInteger();
		try (BrowserContext context = browser.newContext(new Browser.NewContextOptions().setViewportSize(1280, 900)
				.setServiceWorkers(ServiceWorkerPolicy.BLOCK))) {
			String modulePath = root.getPath().substring(0, root.getPath().indexOf("/public_home/"));
			context.addCookies(List.of(new Cookie(cookieName, sessionId).setDomain(root.getHost())
					.setPath(modulePath.isEmpty() ? "/" : modulePath).setHttpOnly(true).setSecure("https".equals(root.getScheme()))));
			context.route("**/*", route -> {
				URI destination;
				try { destination = URI.create(route.request().url()); }
				catch (RuntimeException e) { route.abort(); return; }
				boolean cookieOriginMismatch = root.getHost().equalsIgnoreCase(String.valueOf(destination.getHost()))
						&& !sameOrigin(root, destination);
				if (cookieOriginMismatch || route.request().isNavigationRequest() && !insideApp(root, destination.toString())) {
					unsafeNavigation.set(true);
					route.abort();
				} else route.resume();
			});
			context.onResponse(response -> {
				if (sameOrigin(root, URI.create(response.url())) && (response.status() == 401 || response.status() == 403)) {
					unauthorized.set(true);
				}
			});
			Page page = context.newPage();
			if (smoke) page.onPageError(error -> runtimeErrors.incrementAndGet());
			page.setDefaultTimeout(STEP_TIMEOUT_MS);
			page.setDefaultNavigationTimeout(15_000);
			try {
				Response response = page.navigate(root.resolve(String.valueOf(recording.get("startPath"))).toString(),
						new Page.NavigateOptions().setWaitUntil(WaitUntilState.DOMCONTENTLOADED));
				if (response != null && response.status() >= 400) throw new IllegalStateException("App could not load");
				for (Map<String, Object> step : steps) {
					currentStep++;
					if (System.nanoTime() >= deadline) throw new IllegalStateException("Run time limit exceeded");
					if (unauthorized.get() || unsafeNavigation.get()) throw new IllegalStateException("App session or navigation failed");
					if (smoke && "ASSERT_VISIBLE".equals(step.get("type"))) {
						// Fixed-position apps can have a zero-height body while their content is visible.
						assertThat(page.locator("body:visible, body :visible").first())
								.isVisible(new LocatorAssertions.IsVisibleOptions().setTimeout(STEP_TIMEOUT_MS));
					} else execute(page, root, step);
					if (String.valueOf(step.get("type")).startsWith("ASSERT_")) completedAssertions++;
				}
				// Flush protocol events before evaluating environment failures.
				page.evaluate("() => document.readyState");
				if (smoke) {
					page.waitForTimeout(250);
					if (runtimeErrors.get() > 0) throw new AssertionError("Unhandled app runtime errors observed");
				}
				if (unauthorized.get() || unsafeNavigation.get()) throw new IllegalStateException("App session or navigation failed");
				if (assertionCount == 0) {
					test.put("status", "SKIPPED");
					test.put("classification", "NO_ASSERTIONS");
					test.put("reason", "Actions replayed, but this recording has no checks. Add a text or visibility check to verify behavior.");
				} else test.put("status", "PASSED");
			} catch (RuntimeException | AssertionError e) {
				boolean environment = unauthorized.get() || unsafeNavigation.get()
						|| !(e instanceof TimeoutError || e instanceof AssertionError);
				test.put("status", environment ? "ERROR" : "FAILED");
				test.put("classification", environment ? "ERROR" : "REGRESSION_SUSPECTED");
				test.put("failedStep", currentStep);
				test.put("reason", unauthorized.get() ? "SEMOSS session expired or access was denied. Sign in again before running tests."
						: unsafeNavigation.get() ? "Navigation left the target app or required a different authentication origin."
						: smoke && runtimeErrors.get() > 0 ? "The page visibility smoke check observed unhandled JavaScript errors. This check provides no workflow coverage."
						: environment ? "The app could not load, a step was unsupported, or the runner timed out."
						: "The recorded element or expected check did not match the app.");
				if (currentStep > 0) {
					Map<String, Object> failed = steps.get(currentStep - 1);
					test.put("expected", failed.getOrDefault("text", failed.get("type")));
					test.put("actual", "Recorded action or check could not be completed");
					if ("ASSERT_TEXT".equals(failed.get("type"))) {
						try {
							String actual = locator(page, failed).textContent(new Locator.TextContentOptions().setTimeout(500));
							if (actual != null) test.put("actual", actual.substring(0, Math.min(actual.length(), 2000)));
						} catch (RuntimeException ignored) { /* Missing selectors have no observable text. */ }
					}
					test.put("evidence", new LinkedHashMap<>(Map.of("selector", failed.getOrDefault("selector", ""),
							"framePath", failed.getOrDefault("framePath", List.of()))));
				}
				if (smoke && runtimeErrors.get() > 0) {
					test.put("expected", "No unhandled JavaScript errors during the page visibility smoke check");
					test.put("actual", runtimeErrors.get() + " unhandled JavaScript error(s)");
				}
				try {
					byte[] screenshot = page.screenshot(new Page.ScreenshotOptions().setTimeout(2000).setType(ScreenshotType.JPEG).setQuality(50));
					if (screenshot.length <= 160_000) {
						Map<String, Object> evidence = (Map<String, Object>) test.computeIfAbsent("evidence", ignored -> new LinkedHashMap<>());
						evidence.put("screenshot", "data:image/jpeg;base64," + Base64.getEncoder().encodeToString(screenshot));
					}
				} catch (RuntimeException ignored) { /* Browser may already have closed. */ }
			}
		}
		test.put("assertionsPassed", completedAssertions);
		if (smoke) test.put("runtimeErrorCount", runtimeErrors.get());
		return test;
	}

	@SuppressWarnings("unchecked")
	static void execute(Page page, URI root, Map<String, Object> step) {
		String type = String.valueOf(step.get("type"));
		if (type.equals("NAVIGATE")) {
			page.navigate(root.resolve(AppTestRecordings.relativePath(step.get("path"))).toString(),
					new Page.NavigateOptions().setWaitUntil(WaitUntilState.DOMCONTENTLOADED));
			return;
		}
		Locator locator = locator(page, step);
		switch (type) {
		case "CLICK" -> locator.click();
		case "FILL" -> {
			if ("password".equalsIgnoreCase(locator.getAttribute("type"))) throw new IllegalArgumentException("Password replay is not supported");
			locator.fill(String.valueOf(step.get("value")));
		}
		case "SELECT" -> locator.selectOption(String.valueOf(step.get("value")));
		case "CHECK" -> locator.setChecked((Boolean) step.get("checked"));
		case "KEY" -> locator.press(String.valueOf(step.get("key")));
		case "SCROLL" -> locator.evaluate("(el, p) => el.scrollTo(p.x, p.y)", Map.of("x", step.get("x"), "y", step.get("y")));
		case "ASSERT_TEXT" -> assertThat(locator).containsText(String.valueOf(step.get("text")), new LocatorAssertions.ContainsTextOptions().setTimeout(STEP_TIMEOUT_MS));
		case "ASSERT_VISIBLE" -> assertThat(locator).isVisible(new LocatorAssertions.IsVisibleOptions().setTimeout(STEP_TIMEOUT_MS));
		default -> throw new IllegalArgumentException("Unsupported app test step");
		}
	}

	@SuppressWarnings("unchecked")
	private static Locator locator(Page page, Map<String, Object> step) {
		FrameLocator frame = null;
		for (String selector : (List<String>) step.getOrDefault("framePath", List.of())) {
			frame = frame == null ? page.frameLocator("css=" + selector) : frame.frameLocator("css=" + selector);
		}
		return frame == null ? page.locator("css=" + step.get("selector")) : frame.locator("css=" + step.get("selector"));
	}

	static Map<String, Object> report(String runId, String target, List<Map<String, Object>> tests) {
		Map<String, Object> summary = new LinkedHashMap<>();
		for (String status : List.of("passed", "failed", "error", "skipped")) {
			summary.put(status, tests.stream().filter(t -> status.equals(String.valueOf(t.get("status")).toLowerCase())).count());
		}
		Map<String, Object> report = new LinkedHashMap<>();
		report.put("runId", runId);
		report.put("target", target);
		report.put("mode", "PLAYWRIGHT");
		boolean smoke = tests.stream().anyMatch(t -> "PAGE_VISIBILITY_SMOKE".equals(t.get("scope")));
		report.put("scope", smoke ? "PAGE_VISIBILITY_SMOKE" : "SAVED_WORKFLOW_REGRESSIONS");
		if (smoke) report.put("coverageWarning", "Page visibility and immediate runtime errors only; no workflow coverage. Passing this smoke check does not verify the app's workflows.");
		report.put("status", tests.isEmpty() ? "NO_TESTS" : ((Number) summary.get("error")).intValue() > 0 ? "ERROR" : "COMPLETED");
		report.put("summary", summary);
		report.put("tests", tests);
		report.put("finishedAt", Instant.now().toString());
		if (tests.isEmpty()) report.put("reason", "No saved recordings match this app and selection. Record a workflow and add checks first.");
		return report;
	}
}
