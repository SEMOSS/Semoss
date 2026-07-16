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
package prerna.reactor.workflow.nodes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


import prerna.reactor.workflow.WorkflowConditionEvaluator;
import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "while-loop" node: evaluates a condition before each iteration
 * and runs the inner sub-pipeline while it remains true. Iteration stops when the
 * condition becomes false or {@code maxIterations} is reached.
 *
 * <p>Config: {@code {condition, maxIterations, subGraph: {nodes, edges}}}.
 * Inner node outputs are written back into the parent scope after each iteration
 * so the condition can test them.
 */
public final class WhileLoopNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		String runId = ctx.runId();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		Map<String, Object> config = ctx.config();
		String nodeLabel = ctx.nodeLabel();

		String conditionTemplate = (String) config.get("condition");
		if (conditionTemplate == null || conditionTemplate.isBlank()) {
			throw new IllegalArgumentException("While-loop node \"" + nodeLabel + "\" has no condition set");
		}

		int maxIterations = 100;
		Object maxRaw = config.get("maxIterations");
		if (maxRaw != null) {
			try { maxIterations = Integer.parseInt(maxRaw.toString()); } catch (NumberFormatException ignored) {}
		}

		Map<String, Object> subGraph = config.get("subGraph") instanceof Map
				? (Map<String, Object>) config.get("subGraph") : null;
		List<Map<String, Object>> loopNodes = WorkflowNodeContext.graphNodes(subGraph);
		List<Map<String, Object>> loopEdges = WorkflowNodeContext.graphEdges(subGraph);

		AtomicBoolean cancelFlag = ctx.cancelFlag();
		String lastOutput = "0";
		List<Map<String, Object>> iterationSummaries = new ArrayList<>();

		for (int iteration = 0; iteration < maxIterations; iteration++) {
			if (cancelFlag != null && cancelFlag.get()) {
				throw new IllegalStateException("Run cancelled by user");
			}

			String condition = WorkflowExecutionUtils.resolve(conditionTemplate, scope, configMap);
			if (!WorkflowConditionEvaluator.toBoolean(condition)) break;

			if (loopNodes == null || loopNodes.isEmpty()) break;

			List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(loopNodes, loopEdges);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			List<Map<String, Object>> iterNodes = new ArrayList<>();
			for (Map<String, Object> loopNode : ordered) {
				if (cancelFlag != null && cancelFlag.get()) {
					throw new IllegalStateException("Run cancelled by user");
				}
				String loopOutputVar = (String) loopNode.get("outputVar");
				String loopNodeType = (String) loopNode.get("type");
				Map<String, Object> loopResult = ctx.nodeDispatcher().dispatch(loopNode, scope);
				String status = (String) loopResult.get(WorkflowConstants.STATUS);
				if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					throw new IllegalStateException("While-loop body node failed at iteration " +
							iteration + ": " + loopResult.get(WorkflowConstants.ERROR_MESSAGE));
				}
				String outputValue = (String) loopResult.get("outputValue");
				if (outputValue != null) {
					lastOutput = outputValue;
					if (loopOutputVar != null && !loopOutputVar.isEmpty()
							&& !WorkflowConstants.NODE_SET_VARIABLE.equals(loopNodeType)) {
						scope.put(loopOutputVar, outputValue);
					}
				}
				// Capture sub-node summary for history display
				Map<String, Object> nodeSummary = new LinkedHashMap<>();
				nodeSummary.put("label", loopResult.get(WorkflowConstants.NODE_LABEL));
				nodeSummary.put("status", status);
				Object dur = loopResult.get(WorkflowConstants.DURATION_MS);
				if (dur != null) nodeSummary.put("durationMs", dur);
				Object preview = loopResult.get(WorkflowConstants.OUTPUT_PREVIEW);
				if (preview != null) nodeSummary.put("preview", preview);
				iterNodes.add(nodeSummary);
			}
			Map<String, Object> iterSummary = new LinkedHashMap<>();
			iterSummary.put("iteration", iteration);
			iterSummary.put("nodes", iterNodes);
			iterationSummaries.add(iterSummary);
		}

		if (iterationSummaries.isEmpty()) {
			return lastOutput;
		}
		// Wrap result so GetWorkflowRunReactor can surface per-iteration data in history. Returned
		// as a real Map, not pre-serialized to a JSON string - executeSingleNode's
		// applyOutputTransform call serializes it the same way it serializes every other node's
		// raw output, and can detect this shape (the "__whileResult" marker) on the actual Map to
		// pull the iteration-count preview, rather than string-sniffing already-serialized JSON.
		Map<String, Object> wrapper = new LinkedHashMap<>();
		wrapper.put("__whileResult", true);
		wrapper.put("iterationCount", iterationSummaries.size());
		wrapper.put("lastOutput", lastOutput);
		wrapper.put("iterations", iterationSummaries);
		return wrapper;
	}
}
