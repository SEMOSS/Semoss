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

import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "try-catch" node: runs the try-branch; on any failure injects the
 * error message into scope and runs the catch-branch instead.
 *
 * <p>Config: {@code {errorVar, tryGraph: {nodes, edges}, catchGraph: {nodes, edges}}}.
 * The variable named by {@code errorVar} is available in the catch branch as
 * {@code ${errorVar}}.
 */
public final class TryCatchNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		String runId = ctx.runId();
		Map<String, String> scope = ctx.scope();
		Map<String, Object> config = ctx.config();

		String errorVar = config.get("errorVar") instanceof String
				? (String) config.get("errorVar") : "error";
		if (errorVar.isBlank()) errorVar = "error";

		Map<String, Object> tryGraph = config.get("tryGraph") instanceof Map
				? (Map<String, Object>) config.get("tryGraph") : null;
		List<Map<String, Object>> tryNodes = WorkflowNodeContext.graphNodes(tryGraph);
		List<Map<String, Object>> tryEdges = WorkflowNodeContext.graphEdges(tryGraph);

		Map<String, Object> catchGraph = config.get("catchGraph") instanceof Map
				? (Map<String, Object>) config.get("catchGraph") : null;
		List<Map<String, Object>> catchNodes = WorkflowNodeContext.graphNodes(catchGraph);
		List<Map<String, Object>> catchEdges = WorkflowNodeContext.graphEdges(catchGraph);

		// Try branch
		if (tryNodes != null && !tryNodes.isEmpty()) {
			try {
				List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(tryNodes, tryEdges);
				WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

				String lastOutput = "success";
				AtomicBoolean cancelFlag = ctx.cancelFlag();
				for (Map<String, Object> tryNode : ordered) {
					if (cancelFlag != null && cancelFlag.get()) {
						throw new IllegalStateException("Run cancelled by user");
					}
					String tryOutputVar = (String) tryNode.get("outputVar");
					Map<String, Object> tryResult = ctx.nodeDispatcher().dispatch(tryNode, scope);
					String status = (String) tryResult.get(WorkflowConstants.STATUS);
					if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
						throw new IllegalStateException((String) tryResult.get(WorkflowConstants.ERROR_MESSAGE));
					}
					String outputValue = (String) tryResult.get("outputValue");
					if (outputValue != null) {
						lastOutput = outputValue;
						if (tryOutputVar != null && !tryOutputVar.isEmpty()) {
							scope.put(tryOutputVar, outputValue);
						}
					}
				}
				return lastOutput;
			} catch (IllegalStateException e) {
				// Cancellation must always propagate - don't swallow it
				if (e.getMessage() != null && e.getMessage().contains("cancelled")) throw e;
				scope.put(errorVar, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
			}
		}

		// Catch branch
		if (catchNodes != null && !catchNodes.isEmpty()) {
			List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(catchNodes, catchEdges);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);

			String lastOutput = "caught";
			AtomicBoolean cancelFlag = ctx.cancelFlag();
			for (Map<String, Object> catchNode : ordered) {
				if (cancelFlag != null && cancelFlag.get()) {
					throw new IllegalStateException("Run cancelled by user");
				}
				String catchOutputVar = (String) catchNode.get("outputVar");
				Map<String, Object> catchResult = ctx.nodeDispatcher().dispatch(catchNode, scope);
				String status = (String) catchResult.get(WorkflowConstants.STATUS);
				if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					throw new IllegalStateException("Catch branch node failed: " +
							catchResult.get(WorkflowConstants.ERROR_MESSAGE));
				}
				String outputValue = (String) catchResult.get("outputValue");
				if (outputValue != null) {
					lastOutput = outputValue;
					if (catchOutputVar != null && !catchOutputVar.isEmpty()) {
						scope.put(catchOutputVar, outputValue);
					}
				}
			}
			return lastOutput;
		}

		return "caught";
	}
}
