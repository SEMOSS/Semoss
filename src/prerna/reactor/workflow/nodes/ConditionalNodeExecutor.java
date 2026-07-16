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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.workflow.WorkflowConditionEvaluator;
import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "conditional" node: evaluates the condition expression against the
 * current scope, then runs the chosen branch (trueGraph or falseGraph) synchronously.
 * Branch node outputs are merged into the parent scope so downstream nodes can
 * reference them. Returns the last branch node's output, or the string "true"/"false"
 * if the chosen branch has no nodes.
 */
public final class ConditionalNodeExecutor implements IWorkflowNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(ConditionalNodeExecutor.class);

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		String runId = ctx.runId();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		String nodeLabel = ctx.nodeLabel();

		Map<String, Object> config = ctx.config();
		if (config.isEmpty()) {
			throw new IllegalArgumentException("Conditional node \"" + nodeLabel + "\" has no config");
		}

		String conditionTemplate = (String) config.get("condition");
		if (conditionTemplate == null || conditionTemplate.isBlank()) {
			throw new IllegalArgumentException("Conditional node \"" + nodeLabel + "\" has no condition set");
		}

		// Substitute scope variables into the condition expression, then evaluate as JS
		String condition = WorkflowExecutionUtils.resolve(conditionTemplate, scope, configMap);
		if (condition.equals(conditionTemplate)) {
			classLogger.warn("Conditional node \"{}\": condition template unchanged after resolve - " +
					"check that variable names match outputVar fields. Available scope keys: {}",
					nodeLabel, scope.keySet());
		}
		boolean result = WorkflowConditionEvaluator.toBoolean(condition);

		// Pick the appropriate branch graph
		Map<String, Object> branchGraph = (Map<String, Object>) config.get(result ? "trueGraph" : "falseGraph");
		List<Map<String, Object>> branchNodes = WorkflowNodeContext.graphNodes(branchGraph);
		List<Map<String, Object>> branchEdges = WorkflowNodeContext.graphEdges(branchGraph);

		if (branchNodes == null || branchNodes.isEmpty()) {
			return result ? "true" : "false";
		}

		// Sort and execute branch nodes, inheriting (and writing back into) the parent scope
		List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(branchNodes, branchEdges);

		// Pre-insert branch node rows so markNodeRunning/updateNodeSuccess land correctly
		WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

		// Honour cancellation between branch steps
		AtomicBoolean cancelFlag = ctx.cancelFlag();
		String lastOutput = result ? "true" : "false";
		for (Map<String, Object> branchNode : ordered) {
			if (cancelFlag != null && cancelFlag.get()) {
				throw new IllegalStateException("Run cancelled by user");
			}
			String branchNodeId = (String) branchNode.get("id");
			String branchOutputVar = (String) branchNode.get("outputVar");
			Map<String, Object> branchResult = ctx.nodeDispatcher().dispatch(branchNode, scope);
			String status = (String) branchResult.get(WorkflowConstants.STATUS);
			if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
				String errorMsg = (String) branchResult.get(WorkflowConstants.ERROR_MESSAGE);
				throw new IllegalStateException("Conditional branch node " + branchNodeId +
						" failed: " + errorMsg);
			}
			// Update lastOutput whenever a value is produced, regardless of outputVar
			String outputValue = (String) branchResult.get("outputValue");
			if (outputValue != null) {
				lastOutput = outputValue;
				if (branchOutputVar != null && !branchOutputVar.isEmpty()) {
					scope.put(branchOutputVar, outputValue);
				}
			}
		}
		return lastOutput;
	}
}
