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
package prerna.reactor.workflow;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Resumes a failed or interrupted workflow run from the first failed node.
 *
 * <p>Pixel: {@code ResumeWorkflowRun(project=["appId"], runId=["failed-run-id"])}
 *
 * <p>Validates the target run exists and is in FAILED or INTERRUPTED status,
 * then delegates to {@link TriggerWorkflowReactor} with the {@code resumeRunId}
 * parameter set. This creates a new run that skips previously successful nodes
 * and re-executes from the failure point.
 */
public class ResumeWorkflowRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ResumeWorkflowRunReactor.class);

	public ResumeWorkflowRunReactor() {
		this.keysToGet = new String[]{ "project", "runId" };
		this.keyRequired = new int[]{ 1, 1 };
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
			throw new IllegalArgumentException("Must provide the run id to resume");
		}

		// Auth check
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access");
		}

		// Validate the run exists and is resumable
		Map<String, Object> runDetail = WorkflowDatabaseUtility.getRunDetail(runId);
		if (runDetail == null) {
			throw new IllegalArgumentException("Run not found: " + runId);
		}

		String status = (String) runDetail.get(WorkflowConstants.STATUS);
		if (!WorkflowConstants.STATUS_FAILED.equals(status)
				&& !WorkflowConstants.STATUS_INTERRUPTED.equals(status)) {
			throw new IllegalArgumentException(
					"Can only resume FAILED or INTERRUPTED runs. Current status: " + status);
		}

		// Verify the run belongs to this project
		String runProjectId = (String) runDetail.get(WorkflowConstants.PROJECT_ID);
		if (!projectId.equals(runProjectId)) {
			throw new IllegalArgumentException("Run " + runId + " does not belong to project " + projectId);
		}

		classLogger.info("Resuming workflow run {} for project {}", runId, projectId);

		// Both values come from validated/DB sources (projectId from testUserProjectIdForAlias,
		// runId from WORKFLOW_RUNS), so injection is not expected - guard defensively.
		if (projectId.contains("\"") || projectId.contains("]") ||
				runId.contains("\"") || runId.contains("]")) {
			throw new IllegalArgumentException("Invalid characters in project ID or run ID");
		}

		String pixel = "TriggerWorkflow(project=[\"" + projectId + "\"], "
				+ "manual=[\"true\"], resumeRunId=[\"" + runId + "\"]);";
		return new NounMetadata(
				PixelExecutionUtils.runAndCollect(this.insight, pixel, 0),
				PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}
