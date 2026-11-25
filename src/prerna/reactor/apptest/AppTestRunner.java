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
import java.util.function.Consumer;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Clock;
import com.microsoft.playwright.FrameLocator;
import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.Response;
import com.microsoft.playwright.TimeoutError;
import com.microsoft.playwright.assertions.LocatorAssertions;
import com.microsoft.playwright.options.ColorScheme;
import com.microsoft.playwright.options.Cookie;
import com.microsoft.playwright.options.ReducedMotion;
import com.microsoft.playwright.options.ServiceWorkerPolicy;
import com.microsoft.playwright.options.ScreenshotType;
import com.microsoft.playwright.options.WaitUntilState;

import prerna.util.Utility;

/** One run owns all Playwright objects, on one worker thread, until they close. */
final class AppTestRunner {
	static final long RUN_TIMEOUT_MS = 120_000;
	private static final double STEP_TIMEOUT_MS = 5_000;
	/** Optional ISO instant the page clock starts at, so date-dependent screens replay the same. */
	static final String FIXED_TIME_PROP = "APP_TEST_FIXED_TIME";
	// animations and transitions make an element move or change while a step is acting on it
	private static final String DISABLE_ANIMATIONS = "(() => { const add = () => { const s = document.createElement('style');"
			+ " s.textContent = '*,*::before,*::after{animation:none!important;transition:none!important;"
			+ "scroll-behavior:auto!important;caret-color:transparent!important}';"
			+ " (document.head || document.documentElement).appendChild(s); };"
			+ " if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', add); else add(); })();";

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

	static boolean sameOrigin(URI left, URI right) {
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
		return run(runId, target, root, sessionId, cookieName, recordings, reportFolder, deadline, 1);
	}

	/** Replays each recording {@code repeat} times in fresh browser contexts; it passes only if every replay does. */
	static Map<String, Object> run(String runId, String target, URI root, String sessionId, String cookieName,
			List<Map<String, Object>> recordings, Path reportFolder, long deadline, int repeat) {
		return run(runId, target, root, sessionId, cookieName, recordings, reportFolder, deadline, repeat, progress -> { });
	}

	/** Publish progress through the backend even when no browser tab is polling. */
	static Map<String, Object> run(String runId, String target, URI root, String sessionId, String cookieName,
			List<Map<String, Object>> recordings, Path reportFolder, long deadline, int repeat,
			Consumer<Map<String, Object>> progress) {
		return run(runId, target, root, sessionId, cookieName, recordings, reportFolder, deadline, repeat, progress, null, null, null);
    }

    static Map<String, Object> run(String runId, String target, URI root, String sessionId, String cookieName,
            List<Map<String, Object>> recordings, Path reportFolder, long deadline, int repeat,
            Consumer<Map<String, Object>> progress, String owner, AppTestEnvironment.Definition database,
            AppTestEnvironment.Portal portal) {
        return run(runId, target, root, sessionId, cookieName, recordings, reportFolder, deadline, repeat, progress,
                owner, database, portal, AppTestBrowserOptions.DEFAULT);
    }

    static Map<String, Object> run(String runId, String target, URI root, String sessionId, String cookieName,
            List<Map<String, Object>> recordings, Path reportFolder, long deadline, int repeat,
            Consumer<Map<String, Object>> progress, String owner, AppTestEnvironment.Definition database,
            AppTestEnvironment.Portal portal, AppTestBrowserOptions options) {
		Instant startedAt = Instant.now();
		List<Map<String, Object>> tests = new ArrayList<>();
		for (Map<String, Object> recording : recordings) tests.add(pendingTest(recording, "QUEUED"));
		progress.accept(progressReport(runId, target, tests, startedAt));
		// Browser provisioning belongs to deployment; never download binaries in a user job.
		try {
			if (System.nanoTime() >= deadline) throw new IllegalStateException("Run expired in queue");
			try (Playwright playwright = Playwright.create(new Playwright.CreateOptions().setEnv(Map.of("PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD", "1")));
					Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(options.headless()).setTimeout(30_000));
                    Inspection inspection = new Inspection()) {
				for (int index = 0; index < recordings.size(); index++) {
					if (System.nanoTime() >= deadline) throw new IllegalStateException("Run exceeded its time limit");
					Map<String, Object> recording = recordings.get(index);
					tests.set(index, pendingTest(recording, "RUNNING"));
					progress.accept(progressReport(runId, target, tests, startedAt));
					tests.set(index, replayRepeated(browser, root, sessionId, cookieName, recording, deadline, repeat, runId, owner, target, database, portal, playwright.request(), options.keepOpen() && index == recordings.size()-1 ? inspection : null));
					progress.accept(progressReport(runId, target, tests, startedAt));
				}
                if (inspection.page != null && !inspection.page.isClosed()) {
                    Map<String,Object> completed = finishReport(runId, target, tests, startedAt, database, portal, reportFolder, options);
                    Map<String,Object> viewing = new LinkedHashMap<>(completed);
                    viewing.put("browserOpen", true);
                    viewing.put("browserClosesAt", Instant.now().plusMillis(AppTestBrowserOptions.INSPECTION_MS).toString());
                    progress.accept(viewing);
                    inspection.awaitClose(AppTestBrowserOptions.INSPECTION_MS);
                    return completed;
                }
			}
		} catch (RuntimeException | AssertionError e) {
			for (Map<String, Object> test : tests) if (List.of("QUEUED", "RUNNING").contains(test.get("status"))) {
				test.put("status", "ERROR");
				test.put("classification", "ERROR");
				test.put("finishedAt", Instant.now().toString());
				test.put("reason", options.headless()
                        ? "The runner could not start or finish this test within its time limit. Check browser installation and application URL."
                        : "Headed Chromium could not start or finish this test. Check that the backend has a graphical desktop and the full Playwright Chromium browser installed, that its window was not closed during playback, and that the application is reachable.");
			}
		}
        return finishReport(runId, target, tests, startedAt, database, portal, reportFolder, options);
    }

    private static Map<String,Object> finishReport(String runId, String target, List<Map<String,Object>> tests,
            Instant startedAt, AppTestEnvironment.Definition database, AppTestEnvironment.Portal portal,
            Path reportFolder, AppTestBrowserOptions options) {
		Map<String, Object> report = report(runId, target, tests);
        report.put("headless", options.headless());
        report.put("keepOpen", options.keepOpen());
        report.put("browserOpen", false);
        if (database != null) report.putAll(Map.of("mode", "ISOLATED_H2", "fixtureHash", database.fixtureHash(), "portalHash", portal.hash()));
		report.put("startedAt", startedAt.toString());
		report.put("durationMs", java.time.Duration.between(startedAt, Instant.now()).toMillis());
		try {
			AppTestRecordings.write(reportFolder.resolve(runId + ".json"), report);
		} catch (java.io.IOException e) {
			report.put("reportWarning", "Run completed, but its report could not be persisted");
		}
		return report;
	}

	@SuppressWarnings("unchecked")
	static Map<String, Object> pendingTest(Map<String, Object> recording, String status) {
		Map<String, Object> test = new LinkedHashMap<>();
		for (String field : List.of("id", "name", "description")) test.put(field, recording.get(field));
		test.put("definitionHash", recording.getOrDefault("templateHash", AppTestHistory.definitionHash(recording)));
		test.put("status", status);
		test.put("scope", recording.getOrDefault("scope", "SAVED_WORKFLOW_REGRESSION"));
		List<Map<String, Object>> steps = (List<Map<String, Object>>) recording.get("steps");
		test.put("steps", steps.size());
		test.put("stepsAttempted", 0);
		test.put("stepsCompleted", 0);
		test.put("assertions", steps.stream().filter(step -> String.valueOf(step.get("type")).startsWith("ASSERT_")).count());
		test.put("assertionsPassed", 0);
		if ("RUNNING".equals(status)) test.put("startedAt", Instant.now().toString());
		return test;
	}

	private static Map<String, Object> progressReport(String runId, String target, List<Map<String, Object>> tests, Instant startedAt) {
		Map<String, Object> report = report(runId, target, tests);
		report.put("status", "RUNNING");
		report.put("startedAt", startedAt.toString());
		report.remove("finishedAt");
		return report;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> replay(Browser browser, URI root, String sessionId, String cookieName,
			Map<String, Object> recording, long deadline, AppTestEnvironment testEnvironment, com.microsoft.playwright.APIRequest requests, Inspection inspection) {
		Instant startedAt = Instant.now();
		Map<String, Object> test = new LinkedHashMap<>();
		test.put("id", recording.get("id"));
		test.put("name", recording.get("name"));
		test.put("description", recording.getOrDefault("description", "Replay the recorded app workflow."));
		test.put("definitionHash", recording.getOrDefault("templateHash", AppTestHistory.definitionHash(recording)));
		test.put("startedAt", startedAt.toString());
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
		int completedSteps = 0;
		int attemptedSteps = 0;
		AtomicBoolean unauthorized = new AtomicBoolean();
		AtomicBoolean unsafeNavigation = new AtomicBoolean();
		AtomicInteger runtimeErrors = new AtomicInteger();
        BrowserContext context = browser.newContext(new Browser.NewContextOptions().setViewportSize(1280, 900)
                .setDeviceScaleFactor(1.0).setLocale("en-US").setTimezoneId("UTC").setColorScheme(ColorScheme.LIGHT)
                .setReducedMotion(ReducedMotion.REDUCE).setServiceWorkers(ServiceWorkerPolicy.BLOCK));
        try {
			context.addInitScript(DISABLE_ANIMATIONS);
			Instant fixedTime = fixedTime();
			if (fixedTime != null) context.clock().install(new Clock.InstallOptions().setTime(fixedTime.toString()));
            if (testEnvironment != null) configureTestBrowser(context, root, testEnvironment, unsafeNavigation, requests, deadline);
            else {
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
            }
			context.onResponse(response -> {
				if (sameOrigin(root, URI.create(response.url())) && (response.status() == 401 || response.status() == 403)) {
					unauthorized.set(true);
				}
			});
			Page page = context.newPage();
			if (smoke || testEnvironment != null) page.onPageError(error -> runtimeErrors.incrementAndGet());
			page.setDefaultTimeout(STEP_TIMEOUT_MS);
			page.setDefaultNavigationTimeout(15_000);
			try {
				Response response = page.navigate(root.resolve(String.valueOf(recording.get("startPath"))).toString(),
						new Page.NavigateOptions().setWaitUntil(WaitUntilState.DOMCONTENTLOADED));
				if (response != null && response.status() >= 400) throw new IllegalStateException("App could not load");
                if (testEnvironment != null) collectPreviewFailures(page, testEnvironment);
				for (Map<String, Object> step : steps) {
                    if (testEnvironment != null) {
                        collectPreviewFailures(page, testEnvironment);
                        if (testEnvironment.failure() != null) throw new IllegalStateException("Test preview startup or previous request failed");
                        testEnvironment.phase("WORKFLOW");
                    }
					currentStep++;
					if (System.nanoTime() >= deadline) throw new IllegalStateException("Run time limit exceeded");
					if (unauthorized.get() || unsafeNavigation.get()) { currentStep--; throw new IllegalStateException("App session or navigation failed"); }
					attemptedSteps++;
					if (smoke && "ASSERT_VISIBLE".equals(step.get("type"))) {
						// Fixed-position apps can have a zero-height body while their content is visible.
						assertThat(page.locator("body:visible, body :visible").first())
								.isVisible(new LocatorAssertions.IsVisibleOptions().setTimeout(STEP_TIMEOUT_MS));
					} else AppTestPlayback.execute(page, root, step, String.valueOf(recording.getOrDefault("routeMatch", "EXACT")), deadline);
					completedSteps++;
					if (String.valueOf(step.get("type")).startsWith("ASSERT_")) completedAssertions++;
				}
				// Flush protocol events before evaluating environment failures.
				page.evaluate("() => document.readyState");
				if (smoke || testEnvironment != null) {
					page.waitForTimeout(250);
					if (runtimeErrors.get() > 0) throw new AssertionError("Unhandled app runtime errors observed");
				}
                if (testEnvironment != null) collectPreviewFailures(page, testEnvironment);
                if (unauthorized.get() || unsafeNavigation.get() || testEnvironment != null && testEnvironment.failure() != null)
                    throw new IllegalStateException("App session, preview request or database operation failed");
				if (assertionCount == 0) {
					test.put("status", "SKIPPED");
					test.put("classification", "NO_ASSERTIONS");
					test.put("reason", "Actions replayed, but this recording has no checks. Add a text or visibility check to verify behavior.");
				} else test.put("status", "PASSED");
			} catch (RuntimeException | AssertionError e) {
                if (testEnvironment != null) collectPreviewFailures(page, testEnvironment);
				boolean ambiguous = AppTestPlayback.ambiguous(e);
                boolean invalidSelector = AppTestPlayback.invalidSelector(e);
                boolean environment = unauthorized.get() || unsafeNavigation.get() || testEnvironment != null && testEnvironment.failure() != null
						|| !(ambiguous || invalidSelector || e instanceof TimeoutError || e instanceof AssertionError);
				// A missing element may be a removal that was intended, so it is not called a regression.
				boolean routeMismatch = e instanceof AppTestPlayback.WrongPage;
				boolean targetMissing = !environment && !routeMismatch && !ambiguous && !invalidSelector && currentStep > 0 && targetMissing(page, steps.get(currentStep - 1));
				test.put("status", environment ? "ERROR" : "FAILED");
				test.put("classification", testEnvironment != null && (unsafeNavigation.get() || testEnvironment.failure() != null) ? previewClassification(testEnvironment) : environment ? "ERROR" : ambiguous ? "AMBIGUOUS_SELECTOR" : invalidSelector ? "INVALID_SELECTOR" : routeMismatch ? "WRONG_PAGE" : targetMissing ? "TARGET_NOT_FOUND" : "REGRESSION_SUSPECTED");
				if (currentStep > 0) test.put("failedStep", currentStep);
				test.put("reason", testEnvironment != null && testEnvironment.failure() != null ? testEnvironment.failure()
                        : testEnvironment != null && unsafeNavigation.get() ? "The test preview requested an unsupported asset, endpoint or database operation. No request was forwarded to the real database."
                        : unauthorized.get() ? "SEMOSS session expired or access was denied. Sign in again before running tests."
						: unsafeNavigation.get() ? "Navigation left the target app or required a different authentication origin."
						: smoke && runtimeErrors.get() > 0 ? "The page visibility smoke check observed unhandled JavaScript errors. This check provides no workflow coverage."
						: ambiguous ? "The selector matches multiple elements. Use a unique data-testid or scope it to the intended row/lane. The runner will not choose the first match."
                        : invalidSelector ? "The selector could not be parsed. Use a valid CSS selector scoped to this app."
                        : routeMismatch ? "The app did not reach this step's pagePath. pagePath is the route expected BEFORE the action, not its destination. Check the preceding navigation, entry file, query and hash."
						: environment ? "The app could not load or execute this step. Check browser availability, app responses and the saved step format."
						: targetMissing ? "The recorded element was not found in the app. It may have been removed or renamed; confirm whether that was intended. Do not weaken the test to make it pass."
						: "The recorded element or expected check did not match the app.");
				if (currentStep > 0) {
					Map<String, Object> failed = steps.get(currentStep - 1);
					test.put("expected", routeMismatch ? AppTestPlayback.safeRoute(root, ((AppTestPlayback.WrongPage)e).expected) : failed.getOrDefault("text", failed.getOrDefault("value", failed.getOrDefault("count", failed.get("type")))));
					test.put("actual", "Recorded action or check could not be completed");
                    test.put("page", AppTestPlayback.safeRoute(root, page.url()));
                    if (routeMismatch) {
                        test.put("actual", AppTestPlayback.safeRoute(root, page.url()));
                        if (!java.util.Objects.equals(URI.create(page.url()).getRawQuery(), URI.create(((AppTestPlayback.WrongPage)e).expected).getRawQuery())) test.put("reason", test.get("reason") + " Query parameters differ (values are redacted).");
                    }
                    if (ambiguous) try { test.put("actual", locator(page, failed).count() + " elements matched"); } catch (RuntimeException ignored) { }
                    if ((targetMissing || ambiguous) && failed.get("selector") != null) test.put("availableTestIds", AppTestPlayback.targets(page, failed));
					if (!routeMismatch && "ASSERT_TEXT".equals(failed.get("type"))) {
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
            if (recording.get("cleanupSteps") instanceof List<?> cleanup && !cleanup.isEmpty()) {
                if (testEnvironment != null) testEnvironment.phase("CLEANUP");
                Map<String,Object> cleanupResult = new LinkedHashMap<>();
                int attempted = 0, completed = 0;
                try {
                    for (Object raw : cleanup) {
                        attempted++;
                        if (unauthorized.get() || unsafeNavigation.get() || testEnvironment != null && testEnvironment.failure() != null) throw new IllegalStateException("Session or preview request failed");
                        AppTestPlayback.execute(page, root, (Map<String,Object>)raw, String.valueOf(recording.getOrDefault("routeMatch", "EXACT")), deadline);
                        completed++;
                    }
                    if (testEnvironment != null) {
                        collectPreviewFailures(page, testEnvironment);
                        if (testEnvironment.failure() != null) throw new IllegalStateException("Cleanup preview request failed");
                    }
                    cleanupResult.put("status","PASSED");
                } catch (RuntimeException | AssertionError failure) {
                    cleanupResult.put("status","FAILED");
                    cleanupResult.put("failedStep",attempted);
                    cleanupResult.put("reason","Cleanup could not complete; check its steps, session and remaining run budget. Original workflow outcome is preserved.");
                    if ("PASSED".equals(test.get("status"))) {
                        test.put("status", testEnvironment != null && testEnvironment.failure() != null ? "ERROR" : "FAILED");
                        test.put("classification", testEnvironment != null && testEnvironment.failure() != null ? previewClassification(testEnvironment) : "CLEANUP_FAILED");
                        test.put("reason", testEnvironment != null && testEnvironment.failure() != null ? testEnvironment.failure() : "The workflow passed but cleanup failed; shared test data may remain changed.");
                    }
                }
                cleanupResult.put("stepsAttempted",attempted); cleanupResult.put("stepsCompleted",completed);
                test.put("cleanup",cleanupResult);
            }
            if (inspection != null && !page.isClosed()) {
                inspection.page = page;
                inspection.context = context;
                inspection.environment = testEnvironment;
            }
        } finally {
            if (inspection == null || inspection.context != context) context.close();
        }
        if (testEnvironment != null && !testEnvironment.diagnostics().isEmpty()) test.put("diagnostics", testEnvironment.diagnostics());
		test.put("assertionsPassed", completedAssertions);
		test.put("stepsAttempted", attemptedSteps);
		test.put("stepsCompleted", completedSteps);
		test.put("finishedAt", Instant.now().toString());
		test.put("durationMs", java.time.Duration.between(startedAt, Instant.now()).toMillis());
		if (smoke) test.put("runtimeErrorCount", runtimeErrors.get());
		return test;
	}

	/** The configured start of the page clock, or null when the real clock should be used. */
	private static Instant fixedTime() {
		String value = Utility.getDIHelperProperty(FIXED_TIME_PROP);
		if (value == null || value.isBlank()) return null;
		try {
			return Instant.parse(value.trim());
		} catch (java.time.format.DateTimeParseException e) {
			return null;
		}
	}

	/** True when the step's element does not exist in the page right now. */
	private static boolean targetMissing(Page page, Map<String, Object> step) {
		if (List.of("NAVIGATE", "ASSERT_URL", "ASSERT_ABSENT", "ASSERT_COUNT", "ASSERT_HIDDEN").contains(String.valueOf(step.get("type"))) || step.get("selector") == null) return false;
		try {
			return locator(page, step).count() == 0;
		} catch (RuntimeException e) {
			return false;
		}
	}

    private static String previewClassification(AppTestEnvironment environment) {
        String code = environment.failureCode();
        return code == null || code.equals("TEST_DATABASE_SQL_UNSUPPORTED") || code.equals("UNMAPPED_TEST_DATABASE")
                ? "TEST_DATABASE_UNSUPPORTED" : code;
    }

    private static void collectPreviewFailures(Page page, AppTestEnvironment environment) {
        try {
            Object value = page.evaluate("() => window.__semossTestViolations?.splice(0,10) ?? []");
            if (value instanceof List<?> values) for (Object entry : values) {
                if (entry instanceof Map<?,?> item) environment.blockedRequest("REQUEST", String.valueOf(item.get("url")), "browser",
                        List.of("TEST_PREVIEW_CSP_BLOCKED", "TEST_PREVIEW_STATIC_ASSET_FAILED").contains(item.get("code")) ? String.valueOf(item.get("code")) : "TEST_PREVIEW_ENDPOINT_UNSUPPORTED");
            }
        } catch (RuntimeException ignored) { /* A failed document may already be gone; network failures are retained separately. */ }
    }

    private static void configureTestBrowser(BrowserContext context, URI root, AppTestEnvironment environment,
            AtomicBoolean denied, com.microsoft.playwright.APIRequest requests, long deadline) {
        AtomicInteger staticRequests = new AtomicInteger();
        context.routeWebSocket("**/*", socket -> {
            environment.blockedRequest("WEBSOCKET", socket.url(), "websocket", "TEST_PREVIEW_ENDPOINT_UNSUPPORTED");
            denied.set(true); socket.close();
        });
        URI module = root.resolve("../../../");
        URI applicationModule = root.resolve("../../../../../../");
        String modulePath = applicationModule.getPath().replaceAll("/$", "");
        context.route("**/*", route -> {
            String code = "TEST_PREVIEW_ENDPOINT_UNSUPPORTED";
            try {
                URI url = URI.create(route.request().url());
                if (!sameOrigin(root, url)) { code = "TEST_PREVIEW_EXTERNAL_REQUEST"; throw new IllegalArgumentException(); }
                String path = url.getPath();
                if ("POST".equals(route.request().method()) && module.resolve("api/engine/runPixel").getPath().equals(path)) {
                    String body = route.request().postData();
                    if (body == null || body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > RegressionPixelAccess.MAX_REQUEST_BYTES) {
                        code = "TEST_PREVIEW_REQUEST_TOO_LARGE"; throw new IllegalArgumentException();
                    }
                    Map<String,Object> response;
                    try { response = environment.execute(body); }
                    catch (RuntimeException failure) { response = environment.pixelError(); }
                    route.fulfill(new com.microsoft.playwright.Route.FulfillOptions().setStatus(200)
                            .setContentType("application/json").setBody(new com.google.gson.Gson().toJson(response)));
                } else if ("GET".equals(route.request().method()) && module.resolve("api/config").getPath().equals(path)) {
                    route.fulfill(new com.microsoft.playwright.Route.FulfillOptions().setStatus(200)
                            .setContentType("application/json").setBody(new com.google.gson.Gson().toJson(environment.sdkConfig())));
                } else if ("GET".equals(route.request().method()) && insideApp(root, url.toString())) {
                    String relative = path.substring(module.getPath().length());
                    AppTestEnvironment.Asset asset = environment.asset(relative, modulePath);
                    if (asset == null) { code = "TEST_PREVIEW_ASSET_MISSING"; throw new IllegalArgumentException(); }
                    route.fulfill(new com.microsoft.playwright.Route.FulfillOptions().setStatus(200)
                            .setContentType(asset.contentType()).setBodyBytes(asset.bytes())
                            .setHeaders(Map.of("Content-Security-Policy", environment.contentSecurityPolicy(applicationModule.toString().replaceAll("/$", "")))));
                } else if (AppTestStaticAssets.allows(root, url, route.request().method()) && !route.request().isNavigationRequest()) {
                    code = "TEST_PREVIEW_STATIC_ASSET_FAILED";
                    if (staticRequests.incrementAndGet() > 256 || System.nanoTime() >= deadline) throw new IllegalArgumentException();
                    // A new request context per download carries no user/browser cookies, credentials or prior Set-Cookie.
                    com.microsoft.playwright.APIRequestContext download = requests.newContext(
                            new com.microsoft.playwright.APIRequest.NewContextOptions().setTimeout(Math.max(100, Math.min(8_000, (deadline-System.nanoTime())/1_000_000.0)))
                                    .setExtraHTTPHeaders(Map.of("Accept", "*/*")));
                    try {
                        var response = download.get(url.toString(), com.microsoft.playwright.options.RequestOptions.create().setMaxRedirects(0));
                        if (response.status() >= 300 && response.status() < 400) {
                            code = "TEST_PREVIEW_STATIC_REDIRECT_BLOCKED"; throw new IllegalArgumentException();
                        }
                        if (response.status() == 404) { code = "TEST_PREVIEW_ASSET_MISSING"; throw new IllegalArgumentException(); }
                        String type = response.headers().get("content-type");
                        if (!response.ok() || !AppTestStaticAssets.validContentType(url.toString(), type)) throw new IllegalArgumentException();
                        byte[] content = response.body();
                        if (content.length > AppTestStaticAssets.MAX_BYTES) throw new IllegalArgumentException();
                        route.fulfill(new com.microsoft.playwright.Route.FulfillOptions().setStatus(200).setContentType(type).setBodyBytes(content));
                    } finally { download.dispose(); }
                } else if ("GET".equals(route.request().method()) && "/favicon.ico".equals(path)) {
                    route.fulfill(new com.microsoft.playwright.Route.FulfillOptions().setStatus(404));
                } else throw new IllegalArgumentException();
            } catch (RuntimeException failure) {
                environment.blockedRequest(route.request().method(), route.request().url(), route.request().resourceType(), code);
                denied.set(true); route.abort();
            }
        });
    }

    private static Map<String, Object> replayRepeated(Browser browser, URI root, String sessionId, String cookieName,
            Map<String, Object> recording, long deadline, int repeat, String runId, String owner, String target,
            AppTestEnvironment.Definition database, AppTestEnvironment.Portal portal, com.microsoft.playwright.APIRequest requests, Inspection inspection) {
        List<Map<String, Object>> outcomes = new ArrayList<>();
        for (int i = 0; i < Math.max(1, repeat); i++) {
            var definition = AppTestPlayback.variables(recording, runId, i + 1);
            Inspection finalInspection = i == Math.max(1, repeat)-1 ? inspection : null;
            if (database == null) outcomes.add(replay(browser, root, sessionId, cookieName, definition, deadline, null, requests, finalInspection));
            else {
                AppTestEnvironment environment = null;
                try {
                    environment = AppTestEnvironment.open(owner, target, database, portal, false);
                    outcomes.add(replay(browser, environment.root(root), null, null, definition, deadline, environment, requests, finalInspection));
                } catch (java.io.IOException | java.sql.SQLException | RuntimeException failure) {
                    var failed = pendingTest(recording, "ERROR");
                    failed.put("classification", "TEST_DATABASE_SETUP_FAILED");
                    failed.put("reason", "Could not prepare the isolated H2 fixture: " + failure.getMessage());
                    failed.put("finishedAt", Instant.now().toString());
                    outcomes.add(failed);
                } finally {
                    if (environment != null && (finalInspection == null || finalInspection.environment != environment)) environment.close();
                }
            }
            if (System.nanoTime() >= deadline) break;
        }
        return aggregateRepeats(outcomes, Math.max(1, repeat));
    }

    /** Owns only the final attempt; test outcomes are frozen before this bounded inspection. */
    static final class Inspection implements AutoCloseable {
        Page page;
        BrowserContext context;
        AppTestEnvironment environment;

        void awaitClose(long maximumMs) {
            long until = System.nanoTime() + maximumMs * 1_000_000;
            try {
                while (!Thread.currentThread().isInterrupted() && !page.isClosed() && System.nanoTime() < until) {
                    // Pump Playwright events on its owning thread so the window remains responsive.
                    page.waitForTimeout(Math.min(200, Math.max(1, (until-System.nanoTime())/1_000_000.0)));
                }
            } catch (RuntimeException ignored) { /* Closing the inspection window cannot change the completed result. */ }
        }

        @Override public void close() {
            try { if (context != null) context.close(); }
            finally { if (environment != null) environment.close(); }
        }
    }

	/**
	 * One result for identical replays of the same recording. It passes only if every
	 * replay passed; replays that disagree are reported FLAKY, never as a pass.
	 */
	static Map<String, Object> aggregateRepeats(List<Map<String, Object>> outcomes, int requested) {
		Map<String, Object> chosen = outcomes.getFirst();
		int passed = 0;
		for (Map<String, Object> outcome : outcomes) {
			if ("PASSED".equals(outcome.get("status"))) passed++;
			else if ("PASSED".equals(chosen.get("status"))) chosen = outcome;
		}
		Map<String, Object> result = new LinkedHashMap<>(chosen);
		if (requested <= 1) return result;
		result.put("repeats", outcomes.size());
		result.put("passedRepeats", passed);
		result.put("attempts", outcomes);
		if (chosen.get("steps") instanceof Number steps) {
			result.put("stepsPerReplay", steps.intValue());
			result.put("steps", steps.intValue() * requested);
		}
		if (chosen.get("assertions") instanceof Number assertions) result.put("assertions", assertions.intValue() * requested);
		for (String field : List.of("stepsAttempted", "stepsCompleted", "assertionsPassed")) {
			result.put(field, outcomes.stream().mapToLong(outcome -> outcome.get(field) instanceof Number count ? count.longValue() : 0).sum());
		}
		result.put("startedAt", outcomes.getFirst().get("startedAt"));
		result.put("finishedAt", outcomes.getLast().get("finishedAt"));
		result.put("durationMs", outcomes.stream().mapToLong(outcome -> outcome.get("durationMs") instanceof Number duration ? duration.longValue() : 0).sum());
		if (outcomes.size() < requested && passed == outcomes.size()) {
			result.put("status", "ERROR");
			result.put("classification", "ERROR");
			result.put("reason", "The run time limit was reached after " + outcomes.size() + " of " + requested
					+ " replays, so stability could not be confirmed.");
		} else if (passed > 0 && passed < outcomes.size()) {
			result.put("status", "FAILED");
			result.put("classification", "FLAKY");
			result.put("reason", "The same recording passed " + passed + " of " + outcomes.size()
					+ " replays. Outcomes differed; inspect attempts and shared data/setup before attributing the cause. Fresh browser contexts do not reset databases.");
		}
		return result;
	}

    static void execute(Page page, URI root, Map<String,Object> step) {
        AppTestPlayback.execute(page, root, step, "EXACT", System.nanoTime() + 30_000_000_000L);
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
