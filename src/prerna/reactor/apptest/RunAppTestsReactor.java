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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.om.ThreadStore;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.InternalUtility;

// RunAppTests(project=["..."], recordingIds=["<recording UUID>"]);
// Starts the regression tests of an app and returns a run id to poll with GetAppTestRun
public class RunAppTestsReactor extends AbstractAppTestReactor {

	static final String SCENARIO = "scenario";
	static final String SUITE = "suite";
	static final String PAGES = "pages";
	static final String REPEAT = "repeat";
	static final String RECORDING_IDS = "recordingIds";
	static final String ALL = "all";
	static final String CANDIDATE_SHA = "candidateCommitSha";

	public RunAppTestsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), SUITE, PAGES, SCENARIO, REPEAT, RECORDING_IDS, ALL, CANDIDATE_SHA };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = targetProject();

		String suite = getString(SUITE);
		List<String> pages = getNounAsStringList(PAGES);
		String scenario = getString(SCENARIO);
		Object configuredCookie = InternalUtility.getLocalProperty(Constants.SESSION_ID_KEY);
		String cookieName = configuredCookie == null ? "JSESSIONID" : String.valueOf(configuredCookie);
		String owner = AppTestRuns.ownerOf(requireUser());
		int repeat = 1;
		String repeatValue = getString(REPEAT);
		if (repeatValue != null && !repeatValue.isBlank()) {
			try {
				repeat = Integer.parseInt(repeatValue.trim());
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("repeat must be a whole number from 1 to " + AppTestRuns.MAX_REPEAT);
			}
			if (repeat < 1 || repeat > AppTestRuns.MAX_REPEAT) {
				throw new IllegalArgumentException("repeat must be a whole number from 1 to " + AppTestRuns.MAX_REPEAT);
			}
		}

		String candidateSha = getString(CANDIDATE_SHA);
		List<String> recordingIds = getNounAsStringList(RECORDING_IDS);
		boolean all = getBoolean(ALL, false);
		String runId;
		try {
			if (candidateSha != null || this.store.getGenRowStruct(CANDIDATE_SHA) != null) {
				if (!all || (suite != null && !suite.isBlank()) || (scenario != null && !scenario.isBlank())
						|| (pages != null && !pages.isEmpty()) || (recordingIds != null && !recordingIds.isEmpty())) {
					throw new IllegalArgumentException("A candidate run requires all=true for the approved baseline suite");
				}
				runId = AppTestCandidate.rerun(requireUser(), projectId, candidateSha, repeat);
			} else {
				var database = AppTestEnvironment.definition(requireUser(), projectId);
				if (database != null && scenario != null && !scenario.isBlank())
					throw new IllegalArgumentException("Development mock scenarios cannot be combined with an enabled test database");
				runId = AppTestRuns.start(owner, projectId, scenario, suite, pages, null, ThreadStore.getSessionId(), cookieName,
						repeat, recordingIds, all, database);
			}
		} catch (java.io.IOException | java.net.URISyntaxException failure) {
			throw new IllegalArgumentException("TEST_DATABASE_SETUP_FAILED: " + failure.getMessage(), failure);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("runId", runId);
		result.put("target", projectId);
		var run = AppTestRuns.get(owner, runId);
		result.put("status", run.get("status"));
		result.put("scope", AppTestRuns.PLATFORM_SMOKE_SUITE.equals(suite) ? "PAGE_VISIBILITY_SMOKE" : "SAVED_WORKFLOW_REGRESSIONS");
		result.put("maxFixAttempts", AppTestRuns.maxFixAttempts());
		if ("ISOLATED_H2".equals(run.get("mode"))) result.put("mode", "ISOLATED_H2");
		if (candidateSha != null) result.put(CANDIDATE_SHA, candidateSha);
		if (repeat > 1) result.put("repeat", repeat);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, "inline");
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Replay saved app recordings or an explicit limited __platform_smoke__ page visibility check with isolated Playwright. Explicit target project and one selection (recordingIds, suite or all=true) are required. Never default to all tests. An enabled ManageAppTestDatabase configuration automatically uses a fresh isolated H2 fixture per test/replay; setup or unsupported operation failures never fall back to the real database. Poll GetAppTestRun until status is COMPLETED, ERROR or NO_TESTS. Action-only recordings are SKIPPED/NO_ASSERTIONS. "
						+ "maxFixAttempts is how many times you may fix a failure and run again before you only report it.")
				.key(ReactorKeysEnum.PROJECT, "Required target app project id. Never use the toolbox id or infer it from tool context.")
				.key(SUITE, "Single recording UUID (legacy alias for recordingIds), or reserved __platform_smoke__ for a limited page visibility/runtime-error check. Names are not accepted. Mutually exclusive with recordingIds and all=true.")
				.key(PAGES, k -> k.description("Optional additional filter of exact recording names/start paths within the explicit selection; this does not select tests by itself.")
						.list(String.class))
				.key(RECORDING_IDS, k -> k.list(String.class).description("Explicit recording UUIDs returned by ListAppTestRecordings for this app. Read only selected files. Mutually exclusive with suite and all=true."))
				.key(ALL, k -> k.type(Boolean.class).description("Set true only for an intentional full regression run of this app. Mutually exclusive with recordingIds and suite; omission never selects all tests."))
				.key(CANDIDATE_SHA, "Optional exact Git SHA. Requires all=true and an owner-approved baseline, and builds and tests the candidate with isolated H2.")
				.key(SCENARIO, "Development fixture only, requires APP_TEST_ALLOW_MOCKS=true; never verifies an app.")
				.key(REPEAT, k -> k.type(Integer.class).description("Optional 1 to 5. Replays each recording this many times in fresh browsers; it passes only if every replay passes, and differing results are reported FLAKY."))
				.output(Map.class, "Run id, explicit target, initial status and maximum repair attempts");
	}
}
