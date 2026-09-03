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
package prerna.reactor.automation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns detail for a single automation run including per-node results.
 *
 * <p>Pixel: {@code GetAutomationRun(project=["appId"], runId=["uuid"])}
 *
 * <p>Reads from AUTOMATION_RUNS and AUTOMATION_NODE_OUTPUTS in the scheduler DB.
 */
public class GetAutomationRunReactor extends AbstractReactor {

	// Not standardized in ReactorKeysEnum — matches the local-key convention used by
	// prerna.reactor.agent (e.g. GetAgentRunReactor.RUN_ID_KEY).
	private static final String RUN_ID_KEY = "runId";

	public GetAutomationRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String runId = this.keyValue.get(this.keysToGet[1]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		if (runId == null || runId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a run id");
		}

		projectId = AutomationProjectUtils.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();

		Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
		// Scope by PROJECT_ID so a user with view access to one project cannot read another
		// project's run detail/node outputs by guessing or reusing a runId.
		if (runDetail == null || !projectId.equals(runDetail.get(AutomationConstants.PROJECT_ID))) {
			Map<String, Object> notFound = new HashMap<>();
			notFound.put(AutomationConstants.RUN_ID, runId);
			notFound.put(AutomationConstants.RESULT_NODE_RESULTS, new ArrayList<>());
			return new NounMetadata(notFound, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		List<Map<String, Object>> nodeOutputs = AutomationDatabaseUtility.getNodeOutputsForRun(runId);
		List<Map<String, Object>> nodeResults = AutomationDatabaseUtility.buildNodeResults(nodeOutputs);

		runDetail.put(AutomationConstants.RESULT_NODE_RESULTS, nodeResults);
		return new NounMetadata(runDetail, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Returns detail for a single automation run, including per-node results.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Run identifier returned when the automation was triggered.";
		}
		return super.getDescriptionForKey(key);
	}
}
