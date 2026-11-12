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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.util.Utility;

/**
 * Runs of the app regression tests, kept in memory per signed-in user. The
 * results come from fixed scenario files until the Playwright runner replaces
 * {@link #loadScenario}; the result shape is the contract the agent relies on.
 */
final class AppTestRuns {

	/** Folder of scenario files; defaults to app-test-stubs under the base folder. */
	static final String STUB_DIR_PROP = "APP_TEST_STUB_DIR";

	/** How many fix-and-rerun cycles an agent may try before it only reports. */
	static final String MAX_FIX_ATTEMPTS_PROP = "APP_TEST_MAX_FIX_ATTEMPTS";

	static final String DEFAULT_SCENARIO = "all_pass";

	private static final int DEFAULT_MAX_FIX_ATTEMPTS = 3;
	private static final int MAX_RUNS = 200;
	// the scenario name becomes a file name, so no separators or dots
	private static final Pattern SCENARIO_NAME = Pattern.compile("[A-Za-z0-9_-]{1,64}");

	private static final Map<String, Run> RUNS = Collections.synchronizedMap(new LinkedHashMap<String, Run>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean removeEldestEntry(Map.Entry<String, Run> eldest) {
			return size() > MAX_RUNS;
		}
	});

	private record Run(String owner, Map<String, Object> result) {
	}

	private AppTestRuns() {

	}

	static int maxFixAttempts() {
		String value = Utility.getDIHelperProperty(MAX_FIX_ATTEMPTS_PROP);
		if (value != null) {
			try {
				int parsed = Integer.parseInt(value.trim());
				if (parsed >= 0) {
					return parsed;
				}
			} catch (NumberFormatException e) {
				// fall through to the default
			}
		}
		return DEFAULT_MAX_FIX_ATTEMPTS;
	}

	/** The signed-in user's key; runs are only visible to the user who started them. */
	static String ownerOf(User user) {
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		if (token == null || token.getId() == null) {
			throw new IllegalArgumentException("Must be signed in to run app tests");
		}
		return user.getPrimaryLogin() + ":" + token.getId();
	}

	static String start(String owner, String projectId, String scenario, String suite, List<String> pages) {
		Map<String, Object> result = loadScenario(scenario);
		String runId = UUID.randomUUID().toString();

		Map<String, Object> run = new LinkedHashMap<>();
		run.put("runId", runId);
		run.put("target", projectId);
		run.put("suite", suite);
		run.putAll(result);
		// the scenario file must not override the identity of the run
		run.put("runId", runId);
		run.put("target", projectId);
		run.putIfAbsent("status", "COMPLETED");
		filterPages(run, pages);

		RUNS.put(runId, new Run(owner, run));
		return runId;
	}

	static Map<String, Object> get(String owner, String runId) {
		Run run = runId == null ? null : RUNS.get(runId);
		// the same answer for another user's run, so ids cannot be probed
		if (run == null || !run.owner().equals(owner)) {
			throw new IllegalArgumentException("Unknown run id " + runId);
		}
		return run.result();
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
