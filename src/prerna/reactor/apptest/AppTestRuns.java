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

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.time.Instant;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.util.Utility;

/** Bounded asynchronous runs; credentials remain only in a worker's ephemeral closure. */
final class AppTestRuns {
    static final String STUB_DIR_PROP = "APP_TEST_STUB_DIR";
    static final String MAX_FIX_ATTEMPTS_PROP = "APP_TEST_MAX_FIX_ATTEMPTS";
    static final String ALLOW_MOCKS_PROP = "APP_TEST_ALLOW_MOCKS";
    static final String PLATFORM_SMOKE_SUITE = "__platform_smoke__";
    private static final Pattern SCENARIO_NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final Map<String, Run> RUNS = new LinkedHashMap<>();
    private static final Set<String> ACTIVE_PROJECTS = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(AppTestRuns.class);
    private static final int MAX_RUNS = 50;
    static final int MAX_REPEAT = 5;
    private static final ThreadPoolExecutor WORKERS = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(8), runnable -> {
                Thread thread = new Thread(runnable, "app-regression-test");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

    private static final class Run {
        final String owner;
        Path historyFolder;
        volatile Map<String, Object> result;
        Run(String owner, Map<String, Object> result) { this.owner = owner; this.result = result; }
    }

    private AppTestRuns() { }

    static int maxFixAttempts() {
        String value = Utility.getDIHelperProperty(MAX_FIX_ATTEMPTS_PROP);
        try {
            int parsed = Integer.parseInt(value == null ? "3" : value.trim());
            return parsed >= 0 ? Math.min(parsed, 20) : 3;
        } catch (NumberFormatException e) { return 3; }
    }

    static String ownerOf(User user) {
        AccessToken token = user.getAccessToken(user.getPrimaryLogin());
        if (token == null || token.getId() == null) throw new IllegalArgumentException("Must be signed in to run app tests");
        return user.getPrimaryLogin().getLabel() + ":" + token.getId();
    }

    static String start(String owner, String projectId, String scenario, String suite, List<String> pages,
            String applicationUrl, String sessionId, String cookieName) {
        return start(owner, projectId, scenario, suite, pages, applicationUrl, sessionId, cookieName, 1);
    }

    /** {@code repeat} replays each recording that many times (1 to 5) and passes it only if every replay passes. */
    static String start(String owner, String projectId, String scenario, String suite, List<String> pages,
            String applicationUrl, String sessionId, String cookieName, int repeat) {
        return start(owner, projectId, scenario, suite, pages, applicationUrl, sessionId, cookieName, repeat, null, false);
    }

    /** Real runs must choose UUIDs, the platform smoke check, or explicitly all recordings. */
    static String start(String owner, String projectId, String scenario, String suite, List<String> pages,
            String applicationUrl, String sessionId, String cookieName, int repeat, List<String> recordingIds, boolean all) {
        return start(owner, projectId, scenario, suite, pages, applicationUrl, sessionId, cookieName, repeat, recordingIds, all, null);
    }

    static String start(String owner, String projectId, String scenario, String suite, List<String> pages,
            String applicationUrl, String sessionId, String cookieName, int repeat, List<String> recordingIds, boolean all,
            AppTestEnvironment.Definition database) {
        return start(owner, projectId, scenario, suite, pages, applicationUrl, sessionId, cookieName, repeat, recordingIds,
                all, database, null, null, Map.of());
    }

    static String startCandidate(String owner, String projectId, int repeat, List<Map<String, Object>> recordings,
            AppTestEnvironment.Definition database, AppTestEnvironment.Portal portal, Map<String, Object> provenance) {
        return start(owner, projectId, null, null, null, null, null, null, repeat, null, true, database,
                recordings, portal, provenance);
    }

    private static String start(String owner, String projectId, String scenario, String suite, List<String> pages,
            String applicationUrl, String sessionId, String cookieName, int repeat, List<String> recordingIds, boolean all,
            AppTestEnvironment.Definition database, List<Map<String, Object>> approvedRecordings,
            AppTestEnvironment.Portal candidatePortal, Map<String, Object> provenance) {
        int replays = Math.max(1, Math.min(MAX_REPEAT, repeat));
        String runId = UUID.randomUUID().toString();
        Map<String, Object> pending = new LinkedHashMap<>();
        pending.put("runId", runId);
        pending.put("target", projectId);
        pending.put("status", "RUNNING");
        Run run = new Run(owner, pending);
        if (scenario != null && !scenario.isBlank()) {
            if (!Boolean.parseBoolean(Utility.getDIHelperProperty(ALLOW_MOCKS_PROP))) {
                throw new IllegalArgumentException("Fixture scenarios require explicit APP_TEST_ALLOW_MOCKS=true development configuration");
            }
            Map<String, Object> result = loadScenario(scenario);
            result.put("runId", runId);
            result.put("target", projectId);
            result.put("suite", suite);
            result.put("mode", "MOCK");
            result.put("warning", "Development fixture only. No browser or app was tested.");
            filterPages(result, pages);
            if (result.get("tests") instanceof List<?> tests && tests.isEmpty()) result.put("status", "NO_TESTS");
            run.result = result;
            register(runId, run);
            return runId;
        }
        selectedIds(suite, recordingIds, all); // Reject implicit broad runs before accessing project files.
        List<Map<String, Object>> recordings;
        try { recordings = approvedRecordings != null ? approvedRecordings : PLATFORM_SMOKE_SUITE.equals(suite)
                ? selectRecordings(List.of(), suite, pages)
                : loadSelected(AppTestRecordings.folder(projectId), suite, recordingIds, all, pages); }
        catch (IOException e) { throw new IllegalArgumentException("Unable to read app test recordings: " + e.getMessage(), e); }
        if (recordings.isEmpty()) {
            run.result = AppTestRunner.report(runId, projectId, List.of());
            Map<String, Object> empty = AppTestHistory.pending(runId, projectId, List.of(), replays, Instant.now());
            empty.putAll(run.result);
            try { String warning=AppTestHistory.publish(projectId, empty); if(warning!=null)empty.put("historyWarning",warning); }
            catch (IOException | RuntimeException e) { throw new IllegalArgumentException("Unable to persist test run history", e); }
            run.result = empty;
            register(runId, run);
            return runId;
        }
        if (database == null && (sessionId == null || sessionId.isBlank())) {
            throw new IllegalArgumentException("A signed-in browser session is required to replay app tests. Run from the app editor after signing in.");
        }
        if (!ACTIVE_PROJECTS.add(projectId)) throw new IllegalArgumentException("An app test run is already active for this app. Wait for it to finish before starting another shared-data workflow.");
        try {
        AppTestEnvironment.Portal portal;
        try { portal = database == null ? null : candidatePortal == null ? AppTestEnvironment.portal(projectId) : candidatePortal; }
        catch (IOException failure) { throw new IllegalArgumentException("TEST_PREVIEW_UNAVAILABLE: " + failure.getMessage(), failure); }
        URI root = database == null
                ? AppTestRunner.appRoot(applicationUrl == null ? Utility.getApplicationUrl() : applicationUrl, projectId)
                : AppTestRunner.appRoot("http://semoss-app-test.invalid/", projectId);
        List<Map<String, Object>> selected = recordings;
        long deadline = System.nanoTime() + AppTestRunner.RUN_TIMEOUT_MS * replays * 1_000_000;
        pending.putAll(AppTestHistory.pending(runId, projectId, selected, replays, Instant.now()));
        if (database != null) {
            pending.put("mode", "ISOLATED_H2");
            pending.put("fixtureHash", database.fixtureHash());
            pending.put("portalHash", portal.hash());
        }
        pending.putAll(provenance);
        try {
            run.historyFolder = AppTestHistory.folder(projectId);
            String warning = AppTestHistory.publishAt(projectId, run.historyFolder, pending);
            if (warning != null) pending.put("historyWarning",warning);
        }
        catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("Unable to persist test run history; no browser was started", e);
        }
        try { register(runId, run); }
        catch (IllegalArgumentException e) {
            pending.put("status", "ERROR");
            pending.put("finishedAt", Instant.now().toString());
            @SuppressWarnings("unchecked") List<Map<String, Object>> tests = (List<Map<String, Object>>) pending.get("tests");
            for (Map<String, Object> test : tests) {
                test.put("status", "ERROR");
                test.put("classification", "ERROR");
                test.put("reason", "The runner could not accept this run; this test was not executed.");
            }
            update(run, projectId, pending, pending);
            throw e;
        }
        try {
            Path reportFolder = AppTestRecordings.legacyFolder(projectId).resolve("reports").resolve(ownerDirectory(owner));
            WORKERS.execute(() -> {
                try {
                Map<String, Object> result = AppTestRunner.run(runId, projectId, root, sessionId, cookieName, selected,
                        reportFolder, deadline, replays, progress -> update(run, projectId, pending, progress), owner, database, portal,
                        provenance);
                if (!result.containsKey("reportWarning")) result.put("privateReportRunId", runId);
                update(run, projectId, pending, result);
                } catch (RuntimeException | AssertionError failure) {
                    LOGGER.error("Unexpected app test worker failure: project={}, run={}",projectId,runId,failure);
                    Map<String,Object> failed = new LinkedHashMap<>(pending);
                    failed.put("status","ERROR"); failed.put("finishedAt",Instant.now().toString());
                    failed.put("error","The app test worker could not finish. Check the server log using this run ID.");
                    update(run,projectId,pending,failed);
                } finally { ACTIVE_PROJECTS.remove(projectId); }
            });
        } catch (RejectedExecutionException e) {
            Map<String, Object> rejected = new LinkedHashMap<>(pending);
            rejected.put("status", "ERROR");
            rejected.put("finishedAt", Instant.now().toString());
            @SuppressWarnings("unchecked") List<Map<String, Object>> tests = (List<Map<String, Object>>) rejected.get("tests");
            for (Map<String, Object> test : tests) {
                test.put("status", "ERROR");
                test.put("classification", "ERROR");
                test.put("reason", "The runner queue was full; this test was not executed.");
            }
            update(run, projectId, pending, rejected);
            throw new IllegalArgumentException("App test runner is busy. Try again after an active run finishes.");
        }
        return runId;
        } catch (RuntimeException failure) { ACTIVE_PROJECTS.remove(projectId); throw failure; }
    }

    /** Store progress independently of the UI lifecycle; failures remain visible on the live result. */
    private static void update(Run run, String projectId, Map<String, Object> initial, Map<String, Object> progress) {
        Map<String, Object> next = new LinkedHashMap<>(progress);
        for (String field : List.of("mode", "fixtureHash", "portalHash", "candidateCommitSha", "baselineCommitSha", "candidateBranch"))
            if (initial.containsKey(field)) next.put(field, initial.get(field));
        next.put("version", 1);
        next.put("expiresAt", initial.get("expiresAt"));
        next.put("repeat", initial.get("repeat"));
        next.put("queuedAt", initial.get("startedAt"));
        try {
            String warning = AppTestHistory.publishAt(projectId, run.historyFolder, next);
            if (warning != null) next.put("historyWarning",warning);
        }
        catch (IOException | RuntimeException e) {
            LOGGER.warn("App test local history write failed: project={}, run={}, status={}, folder={}", projectId, next.get("runId"), next.get("status"), run.historyFolder, e);
            next.put("historyWarning", "The test result is available, but its latest local history could not be saved. Check the server log using this run ID.");
        }
        // Progress maps contain no evidence and are independent of the runner's mutable list.
        Map<String, Object> snapshot = "RUNNING".equals(next.get("status")) ? new LinkedHashMap<>(AppTestHistory.compact(next)) : next;
        if (next.containsKey("historyWarning")) snapshot.put("historyWarning", next.get("historyWarning"));
        run.result = snapshot;
    }

    /** Validate scope independently of filesystem state, including calls that have no tests. */
    static List<String> selectedIds(String suite, List<String> recordingIds, boolean all) {
        boolean hasSuite = suite != null && !suite.isBlank();
        boolean hasIds = recordingIds != null && !recordingIds.isEmpty();
        if ((all ? 1 : 0) + (hasSuite ? 1 : 0) + (hasIds ? 1 : 0) != 1) {
            throw new IllegalArgumentException("Choose recordingIds, suite=<recording UUID or __platform_smoke__>, or all=true; exactly one selection is required");
        }
        if (all || PLATFORM_SMOKE_SUITE.equals(suite)) return List.of();
        List<String> ids = hasSuite ? List.of(suite) : List.copyOf(recordingIds);
        if (ids.size() > AppTestRecordings.MAX_RECORDINGS || ids.stream().distinct().count() != ids.size()) {
            throw new IllegalArgumentException("Choose at most " + AppTestRecordings.MAX_RECORDINGS + " distinct recording UUIDs");
        }
        return ids.stream().map(AppTestRecordings::id).toList();
    }

    static List<Map<String, Object>> loadSelected(Path base, String suite, List<String> recordingIds,
            boolean all, List<String> pages) throws IOException {
        List<String> ids = selectedIds(suite, recordingIds, all);
        List<Map<String, Object>> recordings = new ArrayList<>();
        if (all) recordings = AppTestRecordings.list(base);
        else for (String id : ids) recordings.add(AppTestRecordings.get(base, id));
        return selectRecordings(recordings, null, pages);
    }

    static List<Map<String, Object>> selectRecordings(List<Map<String, Object>> recordings, String suite, List<String> pages) {
        if (PLATFORM_SMOKE_SUITE.equals(suite)) {
            return List.of(Map.of("id", PLATFORM_SMOKE_SUITE, "name", "Page visibility smoke check; no workflow coverage",
                    "description", "Open the published app, check visible content and observe immediate unhandled JavaScript errors; no saved workflow is exercised.",
                    "scope", "PAGE_VISIBILITY_SMOKE", "startPath", "", "steps",
                    List.of(Map.of("type", "ASSERT_VISIBLE", "selector", "body", "framePath", List.of()))));
        }
        return recordings.stream()
                .filter(r -> suite == null || suite.isBlank() || suite.equals(r.get("id")) || suite.equals(r.get("name")))
                .filter(r -> pages == null || pages.isEmpty() || pages.contains(r.get("name")) || pages.contains(r.get("startPath")))
                .toList();
    }

    static String ownerDirectory(String owner) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(owner.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static void register(String id, Run run) {
        synchronized (RUNS) {
            if (RUNS.size() >= MAX_RUNS) {
                String terminal = RUNS.entrySet().stream().filter(e -> !List.of("RUNNING", "QUEUED").contains(e.getValue().result.get("status")))
                        .map(Map.Entry::getKey).findFirst().orElse(null);
                if (terminal == null) throw new IllegalArgumentException("Too many active app test runs");
                RUNS.remove(terminal);
            }
            RUNS.put(id, run);
        }
    }

    static Map<String, Object> get(String owner, String runId) {
        Run run;
        synchronized (RUNS) { run = RUNS.get(runId); }
        if (run == null || !run.owner.equals(owner)) throw new IllegalArgumentException("Unknown run id for this caller. Use the runId returned by RunAppTests; live runs can expire, be evicted, or be lost after a server restart. Use app-scoped persisted history for older runs.");
        return new LinkedHashMap<>(run.result);
    }

	@SuppressWarnings("unchecked")
	private static Map<String, Object> loadScenario(String scenario) {
		if (!SCENARIO_NAME.matcher(scenario).matches()) {
			throw new IllegalArgumentException("Invalid scenario name");
		}
		Path dir = stubDir();
		Path file = dir.resolve(scenario + ".json");
		if (!Files.isRegularFile(file)) {
			throw new IllegalArgumentException(
					"Unknown test scenario '" + scenario + "'. Available: " + listScenarios(dir));
		}
		try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
			Map<String, Object> parsed = new Gson().fromJson(reader, Map.class);
			if (parsed == null) {
				throw new IllegalArgumentException("Scenario file is empty: " + scenario);
			}
			return new LinkedHashMap<>(parsed);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read scenario " + scenario + ": " + e.getMessage(), e);
		}
	}

	private static Path stubDir() {
		String dir = Utility.getDIHelperProperty(STUB_DIR_PROP);
		if (dir == null || dir.isBlank()) {
			dir = Utility.getBaseFolder() + "/app-test-stubs";
		}
		return Paths.get(dir.trim()).toAbsolutePath().normalize();
	}

	private static List<String> listScenarios(Path dir) {
		List<String> names = new ArrayList<>();
		if (Files.isDirectory(dir)) {
			try (var stream = Files.list(dir)) {
				stream.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".json"))
						.map(n -> n.substring(0, n.length() - ".json".length())).sorted().forEach(names::add);
			} catch (IOException e) {
				// an unreadable folder just lists nothing
			}
		}
		return names;
	}

	/** Keeps only the tests of the requested pages and recounts the summary. */
	@SuppressWarnings("unchecked")
	private static void filterPages(Map<String, Object> run, List<String> pages) {
		Object testsObj = run.get("tests");
		if (!(testsObj instanceof List)) {
			return;
		}
		List<Map<String, Object>> tests = new ArrayList<>();
		for (Object t : (List<Object>) testsObj) {
			if (t instanceof Map) {
				Map<String, Object> test = (Map<String, Object>) t;
				if (pages == null || pages.isEmpty() || pages.contains(String.valueOf(test.get("page")))) {
					tests.add(test);
				}
			}
		}
		run.put("tests", tests);

		Map<String, Object> summary = new LinkedHashMap<>();
		for (String key : new String[] { "passed", "failed", "error", "skipped" }) {
			summary.put(key, 0);
		}
		for (Map<String, Object> test : tests) {
			String key = String.valueOf(test.get("status")).toLowerCase();
			if (summary.containsKey(key)) {
				summary.put(key, (Integer) summary.get(key) + 1);
			}
		}
		run.put("summary", summary);
	}
}
