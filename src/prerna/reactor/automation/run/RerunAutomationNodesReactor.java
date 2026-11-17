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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;

/**
 * Reprocesses a finished Automation run as a new run, leaving the finished run
 * untouched, with the settings the automation has now.
 *
 * <p>
 * Pixel:
 * {@code RerunAutomationNodes(project=["id"], runId=["id"], nodeIds=["a", "b"])}.
 * The finished run is copied; the selected nodes run chained in run order on the
 * copy, and every other node keeps its recorded result (marked carried over),
 * which the selected nodes read as their upstream scope. The copy records the
 * run it came from and the graph and sources of the nodes that actually ran.
 *
 * <p>
 * With {@code wholeRun=["true"]} (and no {@code nodeIds}) the whole automation
 * runs again from its trigger, with the automation's current defaults, as a new run.
 * Only inputs the finished run was given beyond its own defaults are carried over.
 *
 * <p>
 * To re-run body nodes of one loop iteration, also pass {@code loopNodeId=["loop"]}
 * and the zero-based {@code iteration=["2"]}; {@code nodeIds} are then body nodes
 * and only that iteration is re-run in the new run. The loop's collected results
 * are not recomputed.
 *
 * <p>
 * The caller needs edit access to the Automation project. A run that is not
 * finished, a node that is not part of the run or no longer exists, and trigger,
 * wait, and agent nodes are refused before anything is created. The result is the
 * new run's detail.
 */
public class RerunAutomationNodesReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";
	private static final String NODE_IDS_KEY = "nodeIds";
	private static final String LOOP_NODE_ID_KEY = "loopNodeId";
	private static final String ITERATION_KEY = "iteration";
	private static final String WHOLE_RUN_KEY = "wholeRun";

	public RerunAutomationNodesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_IDS_KEY, LOOP_NODE_ID_KEY,
				ITERATION_KEY, WHOLE_RUN_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = AutomationProjectService.getEditableAutomationProject(this.insight.getUser(),
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
		String runId = this.keyValue.get(RUN_ID_KEY);
		if (runId == null || runId.isBlank()) {
			throw new IllegalArgumentException("Must provide a run id");
		}
		List<String> nodeIds = getListString(NODE_IDS_KEY);
		boolean wholeRun = Boolean.parseBoolean(this.keyValue.get(WHOLE_RUN_KEY));
		if (!wholeRun && (nodeIds == null || nodeIds.isEmpty())) {
			throw new IllegalArgumentException("Select at least one node to re-run.");
		}
		String loopNodeId = this.keyValue.get(LOOP_NODE_ID_KEY);
		boolean inLoop = loopNodeId != null && !loopNodeId.isBlank();
		if (wholeRun && inLoop) {
			throw new IllegalArgumentException("A whole-run reprocess cannot target a loop iteration.");
		}

		AutomationRunExecutionService service = new AutomationRunExecutionService(this.insight,
				ThreadStore.getJobId());
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("wholeRun", wholeRun);
		Map<String, Object> result;
		if (wholeRun) {
			result = service.reprocessWholeRun(runId, projectId, AutomationDefinitionService.load(projectId));
		} else {
			details.put("nodeIds", nodeIds);
			int iteration = -1;
			if (inLoop) {
				iteration = parseIteration(this.keyValue.get(ITERATION_KEY));
				details.put("loopNodeId", loopNodeId);
				details.put("iteration", iteration);
			}
			result = service.reprocessNodes(runId, projectId, nodeIds, inLoop ? loopNodeId : null, iteration,
					AutomationDefinitionService.load(projectId));
		}
		details.put("newRunId", result.get(AutomationConstants.RUN_ID));
		UserAuditTrailUtils.recordAutomationEvent(this.insight.getUser(), AuditEventType.AUTOMATION_RUN_RERUN,
				projectId, runId, null, details);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static int parseIteration(String value) {
		try {
			int iteration = Integer.parseInt(value == null ? "" : value.trim());
			if (iteration >= 0) {
				return iteration;
			}
		} catch (NumberFormatException e) {
			// reported below
		}
		throw new IllegalArgumentException("Re-running loop nodes needs the zero-based iteration to re-run.");
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Reprocesses a finished Automation run as a new run, leaving the finished run untouched: "
						+ "either the whole automation again, or only the chosen nodes, using the automation's "
						+ "current settings and the finished run's recorded results for the rest.")
				.key(ReactorKeysEnum.PROJECT, "The project ID or alias containing the automation workflow.")
				.key(RUN_ID_KEY, "Finished Automation run to reprocess.")
				.key(NODE_IDS_KEY, "IDs of the nodes to run again. They run chained, in run order. Not needed "
						+ "when wholeRun is true.")
				.key(WHOLE_RUN_KEY, "Optional. When true, runs the whole automation again from its trigger with "
						+ "the automation's current defaults.")
				.key(LOOP_NODE_ID_KEY, "Optional loop node. When given, nodeIds are body nodes of that loop and only "
						+ "one iteration of them is re-run.")
				.key(ITERATION_KEY, "Zero-based loop iteration to re-run; required when loopNodeId is given.");
	}
}
