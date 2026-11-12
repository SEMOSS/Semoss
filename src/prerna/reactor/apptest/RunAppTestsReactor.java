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

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// RunAppTests(project=["..."], scenario=["page_fails"], pages=["orders"]);
// Starts the regression tests of an app and returns a run id to poll with GetAppTestRun
public class RunAppTestsReactor extends AbstractReactor {

	static final String SCENARIO = "scenario";
	static final String SUITE = "suite";
	static final String PAGES = "pages";

	public RunAppTestsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), SUITE, PAGES, SCENARIO };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = requireUser();

		String projectId = resolveContextEngineId(getString(ReactorKeysEnum.PROJECT.getKey()));
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project does not exist or the user does not have access to run its tests");
		}

		String suite = getString(SUITE);
		List<String> pages = getNounAsStringList(PAGES);
		String scenario = getString(SCENARIO, AppTestRuns.DEFAULT_SCENARIO);

		String runId = AppTestRuns.start(AppTestRuns.ownerOf(user), projectId, scenario.trim(), suite, pages);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("runId", runId);
		result.put("target", projectId);
		result.put("status", "RUNNING");
		result.put("maxFixAttempts", AppTestRuns.maxFixAttempts());
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
				"Start the regression tests of an app. Call it after you change the app, then call GetAppTestRun with the returned runId until its status is COMPLETED or ERROR. "
						+ "maxFixAttempts is how many times you may fix a failure and run again before you only report it.")
				.key(ReactorKeysEnum.PROJECT, "The app to test. If not passed, the current app context is used.")
				.key(SUITE, "Optional name of the test suite. All tests of the app run when omitted.")
				.key(PAGES, k -> k.description("Optional list of page names; only the tests of those pages run.")
						.list(String.class))
				.key(SCENARIO, "Development only: name of the fixed result to return until real test runs exist.");
	}
}
