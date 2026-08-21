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

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Requests cancellation of a running automation. Takes effect between nodes (cannot interrupt
 * mid-pixel), or mid-wait for nodes that check the flag during blocking operations.
 *
 * <p>Pixel: {@code CancelAutomationRun(project=["appId"], runId=["running-run-id"])}
 *
 * <p>Sets a cluster-safe cancellation flag ({@code AUTOMATION_RUNS.CANCEL_REQUESTED}, via
 * {@link AutomationDatabaseUtility#setCancelRequested(String)}) that the executing pod polls
 * regardless of which pod owns the Python run. The same-pod fast path also interrupts the
 * matching Python socket job, allowing native Python and blocking bridge calls to stop promptly.
 * The run's {@code STATUS} is transitioned to CANCELLED by the executing pod, not by this
 * reactor; a truly orphaned run is caught by the periodic stale-heartbeat sweep.
 */
public class CancelAutomationRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CancelAutomationRunReactor.class);

	// Not standardized in ReactorKeysEnum - matches the local-key convention used by
	// prerna.reactor.agent (e.g. StopAgentRunReactor.RUN_ID_KEY).
	private static final String RUN_ID_KEY = "runId";

	public CancelAutomationRunReactor() {
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
			throw new IllegalArgumentException("Must provide the run id to cancel");
		}

		projectId = AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();

		// Validate the run exists, belongs to this project, and is running. Scoping by
		// PROJECT_ID prevents a user with edit access to their own project from cancelling
		// a run that belongs to a project they were never granted access to.
		Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
		if (runDetail == null || !projectId.equals(runDetail.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Run not found: " + runId);
		}

		String status = (String) runDetail.get(AutomationConstants.STATUS);
		if (!AutomationConstants.STATUS_RUNNING.equals(status)) {
			throw new IllegalArgumentException(
					"Can only cancel RUNNING automations. Current status: " + status);
		}

		// Signal cancellation. The cluster-safe DB flag is always set - this is what the pod
		// actually executing the run (which may not be this pod) polls between nodes via
		// AutomationDatabaseUtility.isCancelRequested(). The in-memory signal is a same-pod fast
		// path only. Unlike the prior implementation, we no longer force the run's STATUS to
		// CANCELLED when the in-memory signal isn't found on this pod - the run may genuinely
		// still be executing on a different pod in a cluster, and overwriting its status here
		// would be a lie. The executing pod transitions STATUS to CANCELLED itself once it
		// observes the flag; a truly orphaned run (crashed, nobody polling) is caught by the
		// periodic stale-heartbeat sweep (AutomationDatabaseUtility.markStaleRunsInterrupted).
		AutomationDatabaseUtility.setCancelRequested(runId);
		boolean signalledLocally = AutomationPythonRunRegistry.requestCancellation(runId);

		classLogger.info("Cancel requested for automation run {}: signalledLocally={}", runId, signalledLocally);

		Map<String, Object> result = new HashMap<>();
		result.put(AutomationConstants.RUN_ID, runId);
		result.put(AutomationConstants.RESULT_CANCEL_REQUESTED, true);
		result.put(AutomationConstants.RESULT_SIGNALLED_LOCALLY, signalledLocally);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Requests cancellation of a running automation run for the given project.";
	}

}
