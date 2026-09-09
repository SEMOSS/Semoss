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
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Requests cancellation of a running or agent-waiting automation. Active execution stops between
 * nodes (or inside blocking operations that observe cancellation). An agent-waiting run cancels
 * its trace-linked child agent and transitions the durable Automation wait to a terminal state.
 *
 * <p>Pixel: {@code CancelAutomationRun(project=["appId"], runId=["running-run-id"])}
 *
 * <p>Sets a cluster-safe cancellation flag ({@code AUTOMATION_RUNS.CANCEL_REQUESTED}, via
 * {@link AutomationDatabaseUtility#setCancelRequested(String)}) that the executing pod polls
 * regardless of which pod owns the Python run. The same-pod fast path also interrupts the
 * matching Python socket job, allowing native Python and blocking bridge calls to stop promptly.
 * A running run's {@code STATUS} is transitioned to CANCELLED by the executing pod, not by this
 * reactor; a truly orphaned run is caught by the periodic stale-heartbeat sweep. A waiting run has
 * no executing pod, so this reactor reconciles its terminal state after stopping the child agent.
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

		// Validate the run exists and belongs to this project. Scoping by
		// PROJECT_ID prevents a user with edit access to their own project from cancelling
		// a run that belongs to a project they were never granted access to.
		Map<String, Object> runDetail = AutomationDatabaseUtility.getRunDetail(runId);
		if (runDetail == null || !projectId.equals(runDetail.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Run not found: " + runId);
		}

		String status = (String) runDetail.get(AutomationConstants.STATUS);
		if (AutomationConstants.STATUS_WAITING_FOR_INPUT.equals(status)) {
			return cancelWaitingAgentRun(projectId, runId);
		}
		if (!AutomationConstants.STATUS_RUNNING.equals(status)) {
			throw new IllegalArgumentException(
					"Can only cancel RUNNING or WAITING_FOR_INPUT automations. Current status: " + status);
		}

		// Persist the cluster-visible request before attempting the same-pod socket fast path.
		// The executing pod owns the terminal status transition; stale recovery handles a run
		// whose owner disappeared before observing the request.
		AutomationDatabaseUtility.setCancelRequested(runId);
		boolean signalledLocally = AutomationPythonRunRegistry.requestCancellation(runId);

		classLogger.info("Cancel requested for automation run {}: signalledLocally={}", runId, signalledLocally);

		Map<String, Object> result = new HashMap<>();
		result.put(AutomationConstants.RUN_ID, runId);
		result.put(AutomationConstants.RESULT_CANCEL_REQUESTED, true);
		result.put(AutomationConstants.RESULT_SIGNALLED_LOCALLY, signalledLocally);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** Cancels the trace-linked child agent and terminally reconciles a durable waiting run. */
	private NounMetadata cancelWaitingAgentRun(String projectId, String runId) {
		Map<String, Object> wait = AutomationDatabaseUtility.getActiveWait(runId);
		if (wait == null) {
			throw new IllegalStateException("Waiting Automation run has no active input boundary: " + runId);
		}
		String nodeId = String.valueOf(wait.get(AutomationConstants.NODE_ID));
		String agentRunId = String.valueOf(wait.get(AutomationConstants.AGENT_RUN_ID));
		AutomationAgentRunAccess.authorizeEdit(this.insight, projectId, runId, nodeId, agentRunId);
		AgentRuntimeManager.get().stopForAutomation(agentRunId, this.insight);
		Map<String, Object> run = new AutomationRunExecutionService(this.insight, null)
				.resumeWaitingRun(runId, projectId);

		Map<String, Object> result = new HashMap<>();
		result.put(AutomationConstants.RUN_ID, runId);
		result.put(AutomationConstants.RESULT_CANCEL_REQUESTED, true);
		result.put(AutomationConstants.RESULT_SIGNALLED_LOCALLY, false);
		result.put(AutomationConstants.STATUS, run.get(AutomationConstants.STATUS));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Requests cancellation of a running or agent-waiting Automation run for the given project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Identifier of the running automation execution to cancel.";
		}
		return super.getDescriptionForKey(key);
	}
}
