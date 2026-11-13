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
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

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
    private static final int MAX_RUNS = 50;
    private static final ThreadPoolExecutor WORKERS = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(8), runnable -> {
                Thread thread = new Thread(runnable, "app-regression-test");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

    private static final class Run {
        final String owner;
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
        Path folder = AppTestRecordings.folder(projectId);
        List<Map<String, Object>> recordings;
        try { recordings = PLATFORM_SMOKE_SUITE.equals(suite) ? List.of() : AppTestRecordings.list(folder); }
        catch (IOException e) { throw new IllegalArgumentException("Unable to read app test recordings", e); }
        recordings = selectRecordings(recordings, suite, pages);
        if (recordings.isEmpty()) {
            run.result = AppTestRunner.report(runId, projectId, List.of());
            register(runId, run);
            return runId;
        }
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("A signed-in browser session is required to replay app tests. Run from the app editor after signing in.");
        }
        URI root = AppTestRunner.appRoot(applicationUrl == null ? Utility.getApplicationUrl() : applicationUrl, projectId);
        List<Map<String, Object>> selected = recordings;
        register(runId, run);
        long deadline = System.nanoTime() + AppTestRunner.RUN_TIMEOUT_MS * 1_000_000;
        try {
            Path reportFolder = folder.resolve("reports").resolve(ownerDirectory(owner));
            WORKERS.execute(() -> run.result = AppTestRunner.run(runId, projectId, root, sessionId, cookieName, selected, reportFolder, deadline));
        } catch (RejectedExecutionException e) {
            synchronized (RUNS) { RUNS.remove(runId); }
            throw new IllegalArgumentException("App test runner is busy. Try again after an active run finishes.");
        }
        return runId;
    }

    static List<Map<String, Object>> selectRecordings(List<Map<String, Object>> recordings, String suite, List<String> pages) {
        if (PLATFORM_SMOKE_SUITE.equals(suite)) {
            return List.of(Map.of("id", PLATFORM_SMOKE_SUITE, "name", "Page visibility smoke check; no workflow coverage",
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
                String terminal = RUNS.entrySet().stream().filter(e -> !"RUNNING".equals(e.getValue().result.get("status")))
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
        if (run == null || !run.owner.equals(owner)) throw new IllegalArgumentException("Unknown run id");
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
