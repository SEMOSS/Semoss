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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns detail for a single automation run including per-node results.
 *
 * <p>
 * Pixel: {@code GetAutomationRun(project=["appId"], runId=["uuid"])}
 *
 * <p>
 * Reads from AUTOMATION_RUNS and AUTOMATION_NODE_OUTPUTS in the scheduler DB.
 */
public class GetAutomationRunReactor extends AbstractReactor {

	private static final String OUTPUT_FRAME_KEY = "OUTPUT_FRAME";
	private static final String OUTPUT_FRAME_UNAVAILABLE_KEY = "OUTPUT_FRAME_UNAVAILABLE";
	private static final String OUTPUT_DATA_AVAILABLE_KEY = "outputDataAvailable";
	private static final String OUTPUT_DATA_REFERENCE_ID_KEY = "outputDataReferenceId";
	private static final String OUTPUT_DATA_ROW_COUNT_KEY = "outputDataRowCount";
	private static final String OUTPUT_DATA_COLUMN_COUNT_KEY = "outputDataColumnCount";
	private static final String OUTPUT_VARIABLE_KEY = "outputVariable";
	private static final String FRAME_UNAVAILABLE_MESSAGE =
			"Frame data is unavailable because this run's execution workspace is closed.";

	// Not standardized in ReactorKeysEnum — matches the local-key convention used
	// by prerna.reactor.agent (e.g. GetAgentRunReactor.RUN_ID_KEY).
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

		projectId = AutomationProjectService.getViewableAutomationProject(this.insight.getUser(), projectId)
				.getProjectId();

		Map<String, Object> runDetail = AutomationRunStore.getRunDetail(runId);
		// Scope by PROJECT_ID so a user with view access to one project cannot read
		// another project's run detail/node outputs by guessing or reusing a runId.
		if (runDetail == null || !projectId.equals(runDetail.get(AutomationConstants.PROJECT_ID))) {
			Map<String, Object> notFound = new HashMap<>();
			notFound.put(AutomationConstants.RUN_ID, runId);
			notFound.put(AutomationConstants.RESULT_NODE_RESULTS, new ArrayList<>());
			return new NounMetadata(notFound, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		List<Map<String, Object>> nodeOutputs = AutomationRunStore.getNodeOutputsForRun(runId);
		List<Map<String, Object>> nodeResults = AutomationRunStore.buildNodeResults(nodeOutputs);

		runDetail.put(AutomationConstants.RESULT_NODE_RESULTS, nodeResults);
		Insight executionInsight = AutomationRunExecutionService.getAvailableExecutionInsight(runId);
		if (executionInsight != null && projectId.equals(executionInsight.getProjectId())) {
			runDetail.put(AutomationConstants.RESULT_EXECUTION_INSIGHT_ID, executionInsight.getInsightId());
		}
		Map<String, Map<String, Object>> outputsByNode = new LinkedHashMap<>();
		for (Map<String, Object> nodeOutput : nodeOutputs) {
			outputsByNode.put(String.valueOf(nodeOutput.get(AutomationConstants.NODE_ID)), nodeOutput);
		}
		Map<String, AutomationFrameHistory.Snapshot> durableData = AutomationFrameHistory.findAvailableByRun(runId);
		for (Map<String, Object> nodeResult : nodeResults) {
			decorateNodeResult(executionInsight, projectId, outputsByNode, durableData, nodeResult);
		}
		Map<String, Object> wait = AutomationRunStore.getActiveWait(runId);
		if (wait != null) {
			runDetail.put("wait", wait);
		}
		return new NounMetadata(runDetail, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/** Returns whether the durable row explicitly identifies a live frame output. */
	static boolean isFrameOutput(Map<String, Object> nodeOutput) {
		return AutomationConstants.OUTPUT_KIND_FRAME.equals(nodeOutput.get(AutomationConstants.OUTPUT_KIND));
	}

	/**
	 * Adds either the live frame payload or the explicit closed-workspace state.
	 *
	 * @param executionInsight live run Insight, or {@code null} after cleanup
	 * @param projectId        owning Automation project
	 * @param nodeOutput       durable node-output row
	 * @param nodeResult       result returned to the caller
	 */
	void decorateFrameResult(Insight executionInsight, String projectId, Map<String, Object> nodeOutput,
			Map<String, Object> nodeResult) {
		decorateFrameResult(executionInsight, projectId, nodeOutput, nodeResult, null);
	}

	private void decorateFrameResult(Insight executionInsight, String projectId, Map<String, Object> nodeOutput,
			Map<String, Object> nodeResult, AutomationFrameHistory.Snapshot durableData) {
		Object outputVariable = nodeOutput.get(AutomationConstants.OUTPUT_VAR_NAME);
		if (outputVariable instanceof String name) {
			nodeResult.put(OUTPUT_VARIABLE_KEY, name);
		}
		if (durableData != null) {
			nodeResult.put(OUTPUT_DATA_AVAILABLE_KEY, true);
			nodeResult.put(OUTPUT_DATA_REFERENCE_ID_KEY, durableData.referenceId());
			nodeResult.put(OUTPUT_DATA_ROW_COUNT_KEY, durableData.rowCount());
			nodeResult.put(OUTPUT_DATA_COLUMN_COUNT_KEY, durableData.columnCount());
		}
		if (executionInsight != null && projectId.equals(executionInsight.getProjectId())
				&& outputVariable instanceof String name) {
			NounMetadata frame = executionInsight.getVarStore().get(name);
			if (frame != null && frame.getNounType() == PixelDataType.FRAME) {
				nodeResult.put(OUTPUT_FRAME_KEY, processNounMetadata(frame));
				return;
			}
		}
		if (isFrameOutput(nodeOutput) && durableData == null) {
			nodeResult.put(OUTPUT_FRAME_UNAVAILABLE_KEY, true);
			nodeResult.put(AutomationConstants.OUTPUT_PREVIEW, FRAME_UNAVAILABLE_MESSAGE);
		}
	}

	private void decorateNodeResult(Insight executionInsight, String projectId,
			Map<String, Map<String, Object>> outputsByNode,
			Map<String, AutomationFrameHistory.Snapshot> durableData, Map<String, Object> nodeResult) {
		String runtimeNodeId = String.valueOf(nodeResult.get(AutomationConstants.NODE_ID));
		Object traceValue = nodeResult.get(AutomationConstants.RESULT_TRACE);
		if (traceValue instanceof Map<?, ?> trace && trace.get(AutomationConstants.TRACE_NODE_ID) != null) {
			runtimeNodeId = String.valueOf(trace.get(AutomationConstants.TRACE_NODE_ID));
		}
		Map<String, Object> output = outputsByNode.get(runtimeNodeId);
		if (output != null) {
			decorateFrameResult(executionInsight, projectId, output, nodeResult, durableData.get(runtimeNodeId));
		}
		Object iterationsValue = nodeResult.get("iterations");
		if (!(iterationsValue instanceof List<?> iterations)) {
			return;
		}
		List<Map<String, Object>> decoratedIterations = new ArrayList<>();
		for (Object iterationValue : iterations) {
			if (!(iterationValue instanceof Map<?, ?> iteration)) {
				continue;
			}
			Object childResults = iteration.get("nodeResults");
			if (!(childResults instanceof List<?> children)) {
				continue;
			}
			List<Map<String, Object>> decoratedChildren = new ArrayList<>();
			for (Object child : children) {
				if (child instanceof Map<?, ?> childResult) {
					Map<String, Object> decoratedChild = new LinkedHashMap<>();
					for (Map.Entry<?, ?> entry : childResult.entrySet()) {
						if (entry.getKey() instanceof String key) {
							decoratedChild.put(key, entry.getValue());
						}
					}
					decorateNodeResult(executionInsight, projectId, outputsByNode, durableData, decoratedChild);
					decoratedChildren.add(decoratedChild);
				}
			}
			Map<String, Object> decoratedIteration = new LinkedHashMap<>();
			decoratedIteration.put("index", iteration.get("index"));
			decoratedIteration.put("nodeResults", decoratedChildren);
			decoratedIterations.add(decoratedIteration);
		}
		nodeResult.put("iterations", decoratedIterations);
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
