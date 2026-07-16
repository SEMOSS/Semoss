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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Cancels a running workflow. Takes effect between nodes (cannot interrupt mid-pixel).
 *
 * <p>Pixel: {@code CancelWorkflowRun(project=["appId"], runId=["running-run-id"])}
 *
 * <p>Sets a cancellation flag that the {@link TriggerWorkflowReactor} executor checks
 * between node executions. If the run is not currently active in this JVM, updates
 * the DB status directly to CANCELLED.
 */
public class CancelWorkflowRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CancelWorkflowRunReactor.class);

	public CancelWorkflowRunReactor() {
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
			throw new IllegalArgumentException("Must provide the run id to cancel");
		}

		// Auth check
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have edit access");
		}

		// Validate the run exists and is running
		Map<String, Object> runDetail = WorkflowDatabaseUtility.getRunDetail(runId);
		if (runDetail == null) {
			throw new IllegalArgumentException("Run not found: " + runId);
		}

		String status = (String) runDetail.get(WorkflowConstants.STATUS);
		if (!WorkflowConstants.STATUS_RUNNING.equals(status)) {
			throw new IllegalArgumentException(
					"Can only cancel RUNNING workflows. Current status: " + status);
		}

		// Try to signal the in-process executor
		boolean signalled = TriggerWorkflowReactor.requestCancellation(runId);

		if (!signalled) {
			// Run is not active in this JVM (orphaned RUNNING row) - mark directly
			WorkflowDatabaseUtility.updateRunStatus(runId,
					WorkflowConstants.STATUS_CANCELLED, null, "Cancelled by user");
		}

		classLogger.info("Cancel requested for workflow run {}: signalled={}", runId, signalled);

		Map<String, Object> result = new HashMap<>();
		result.put(WorkflowConstants.RUN_ID, runId);
		result.put(WorkflowConstants.STATUS, WorkflowConstants.STATUS_CANCELLED);
		result.put("signalled", signalled);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}
