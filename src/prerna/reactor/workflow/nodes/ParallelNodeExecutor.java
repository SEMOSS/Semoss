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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "parallel" node: runs each configured branch's sub-graph against its own scope
 * copy, sequentially (despite the name - see note below), continuing past a failed branch
 * rather than failing the whole node.
 *
 * <p>Note: branches run one after another on this same thread, not concurrently - "parallel"
 * describes the workflow-authoring concept (independent branches that don't depend on each
 * other's output), not the execution mechanism. A branch failure is logged and recorded as a
 * {@code null} result for that branch; it does not stop the remaining branches.
 */
public final class ParallelNodeExecutor implements IWorkflowNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(ParallelNodeExecutor.class);

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		String runId = ctx.runId();
		Map<String, String> scope = ctx.scope();
		Map<String, Object> config = ctx.config();
		List<Map<String, Object>> branches = config.get("branches") instanceof List
				? (List<Map<String, Object>>) config.get("branches") : new ArrayList<>();

		List<Object> branchResults = new ArrayList<>();
		AtomicBoolean cancelFlag = ctx.cancelFlag();

		for (int i = 0; i < branches.size(); i++) {
			if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");

			Map<String, Object> branch = branches.get(i);
			String branchOutputVar = WorkflowExecutionUtils.strCfg(branch.get("outputVar"));
			String branchLabel = WorkflowExecutionUtils.strCfg(branch.getOrDefault("label", "branch_" + i));

			Map<String, Object> branchGraph = branch.get("subGraph") instanceof Map
					? (Map<String, Object>) branch.get("subGraph") : null;
			List<Map<String, Object>> branchNodes = WorkflowNodeContext.graphNodes(branchGraph);
			List<Map<String, Object>> branchEdges = WorkflowNodeContext.graphEdges(branchGraph);

			if (branchNodes == null || branchNodes.isEmpty()) {
				branchResults.add(null);
				continue;
			}

			Map<String, String> branchScope = new HashMap<>(scope);
			try {
				List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(branchNodes, branchEdges);
				WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);
				String lastOutput = "{}";
				for (Map<String, Object> branchNode : ordered) {
					if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");
					String subOutputVar = (String) branchNode.get("outputVar");
					Map<String, Object> subResult = ctx.nodeDispatcher().dispatch(branchNode, branchScope);
					String status = (String) subResult.get(WorkflowConstants.STATUS);
					if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
						throw new IllegalStateException("Parallel branch \"" + branchLabel + "\" failed: " + subResult.get(WorkflowConstants.ERROR_MESSAGE));
					}
					String out = (String) subResult.get("outputValue");
					if (out != null) {
						lastOutput = out;
						if (subOutputVar != null && !subOutputVar.isBlank()) branchScope.put(subOutputVar, out);
					}
				}
				branchResults.add(lastOutput);
				if (branchOutputVar != null && !branchOutputVar.isBlank()) scope.put(branchOutputVar, lastOutput);
			} catch (IllegalStateException e) {
				if (e.getMessage() != null && e.getMessage().contains("cancelled")) throw e;
				classLogger.error("Parallel branch \"{}\" failed (continuing): {}", branchLabel, e.getMessage());
				branchResults.add(null);
			}
		}
		return WorkflowExecutionUtils.GSON.toJson(branchResults);
	}
}
