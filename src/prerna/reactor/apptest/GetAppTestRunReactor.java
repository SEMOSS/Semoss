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

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// GetAppTestRun(runId=["..."]);
// The status and, once finished, the per-test results of a run started with RunAppTests
public class GetAppTestRunReactor extends AbstractReactor {

	static final String RUN_ID = "runId";

	public GetAppTestRunReactor() {
		this.keysToGet = new String[] { RUN_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String owner = AppTestRuns.ownerOf(requireUser());
		Map<String, Object> run = AppTestRuns.get(owner, getString(RUN_ID));
		return new NounMetadata(run, PixelDataType.MAP, PixelOperationType.OPERATION);
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
				"Get a caller-owned app test run. status is RUNNING, COMPLETED, ERROR or NO_TESTS. scope PAGE_VISIBILITY_SMOKE verifies page visibility and immediate runtime errors only, never saved workflow coverage. "
						+ "Each failed test has a classification: REGRESSION_SUSPECTED (the change likely broke it, fix the app), "
						+ "TEST_NEEDS_UPDATE (requires confirmation of intended change), NO_ASSERTIONS (actions replayed without checks, unverified) or ERROR (runner/environment failed). Never claim zero tests or skipped tests passed.")
				.key(RUN_ID, "The runId returned by RunAppTests")
				.output(Map.class, "Caller-owned run status, target, summary counts, per-recording results and private failure evidence");
	}
}
