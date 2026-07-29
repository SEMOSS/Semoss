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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the currently active run ID for a project by reading the
 * {@code AUTOMATION_ACTIVE_RUN} lock table directly. This table is populated by
 * {@link AutomationDatabaseUtility#claimActiveRun} <em>before</em>
 * {@code AUTOMATION_RUNS} is written, so the FE can discover the run ID while
 * {@link TriggerAutomationReactor} is still executing synchronously on a virtual
 * thread.
 *
 * <p>Returns {@code { RUN_ID, PROJECT_ID }} when a run is active, or an empty
 * map when no run is in progress.
 *
 * <p>Pixel: {@code GetActiveAutomationRun(project=["appId"])}
 */
public class GetActiveAutomationRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetActiveAutomationRunReactor.class);

	public GetActiveAutomationRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProjectId();

		String runId = AutomationDatabaseUtility.getClaimedActiveRun(projectId);

		Map<String, Object> result = new HashMap<>();
		if (runId != null) {
			result.put(AutomationConstants.RUN_ID, runId);
			result.put(AutomationConstants.PROJECT_ID, projectId);
		}
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private String getProjectId() {
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}
		return projectId;
	}

	@Override
	public String getReactorDescription() {
		return "Returns the active run ID from the AUTOMATION_ACTIVE_RUN table; empty map when no run is in progress.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) return "The project (app) ID or alias to check for an active run.";
		return super.getDescriptionForKey(key);
	}
}
