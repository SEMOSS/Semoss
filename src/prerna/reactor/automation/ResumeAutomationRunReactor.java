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

import java.util.Map;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Continues a durable Automation run after its trace-linked child agent reaches a terminal state.
 *
 * <p>Pixel: {@code ResumeAutomationRun(project=["id"], runId=["id"])}
 *
 * <p>The execution service uses a compare-and-set claim, so repeated calls are safe and concurrent
 * callers cannot execute the remainder of the graph twice.
 */
public class ResumeAutomationRunReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";

	public ResumeAutomationRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String requestedProjectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String runId = this.keyValue.get(RUN_ID_KEY);
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("Must provide a run id");
		}

		IProject project = AutomationProjectUtils.getEditableAutomationProject(
				this.insight.getUser(), requestedProjectId);
		String projectId = project.getProjectId();
		Map<String, Object> run = AutomationDatabaseUtility.getRunDetail(runId);
		if (run == null || !projectId.equals(run.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run not found: " + runId);
		}

		Map<String, Object> wait = AutomationDatabaseUtility.getActiveWait(runId);
		if (wait != null) {
			AutomationAgentRunAccess.authorizeEdit(this.insight, projectId, runId,
					String.valueOf(wait.get(AutomationConstants.NODE_ID)),
					String.valueOf(wait.get(AutomationConstants.AGENT_RUN_ID)));
		}

		Map<String, Object> result = new AutomationRunExecutionService(this.insight, null)
				.resumeWaitingRun(runId, projectId);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Continues a waiting Automation run after its child agent input flow completes.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Waiting Automation run identifier.";
		}
		return super.getDescriptionForKey(key);
	}
}
