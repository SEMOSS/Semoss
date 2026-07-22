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
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Requests cancellation of a running automation. Takes effect between nodes (cannot interrupt
 * mid-pixel), or mid-wait for nodes that check the flag during blocking operations.
 *
 * <p>Pixel: {@code CancelAutomationRun(project=["appId"], runId=["running-run-id"])}
 *
 * <p>Sets a cluster-safe cancellation flag ({@code AUTOMATION_RUNS.CANCEL_REQUESTED}, via
 * {@link AutomationDatabaseUtility#setCancelRequested(String)}) that the executing pod's
 * between-node check polls regardless of which pod that is - this is the source of truth. Also
 * attempts an in-memory same-pod signal ({@link TriggerAutomationReactor#requestCancellation(String)})
 * as a fast path when the run happens to be executing on the pod that received this request. The
 * run's {@code STATUS} is transitioned to CANCELLED by whichever pod is actually executing it,
 * not by this reactor - a truly orphaned run (no pod executing it) is instead caught by the
 * periodic stale-heartbeat sweep.
 */
public class CancelAutomationRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CancelAutomationRunReactor.class);

	public CancelAutomationRunReactor() {
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
		Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
		if (runDetail == null) {
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
		boolean signalledLocally = TriggerAutomationReactor.requestCancellation(runId);
		AutomationDatabaseUtility.setCancelRequested(runId);

		classLogger.info("Cancel requested for automation run {}: signalledLocally={}", runId, signalledLocally);

		Map<String, Object> result = new HashMap<>();
		result.put(AutomationConstants.RUN_ID, runId);
		result.put("cancelRequested", true);
		result.put("signalledLocally", signalledLocally);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}
