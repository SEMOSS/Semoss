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


import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "switch" node: matches {@code switchVar}'s resolved value against a list of
 * {@code cases}, then runs the matched (or default) sub-graph.
 */
public final class SwitchNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		String runId = ctx.runId();
		Map<String, String> scope = ctx.scope();
		Map<String, String> configMap = ctx.configMap();
		Map<String, Object> config = ctx.config();

		String switchVar = WorkflowExecutionUtils.strCfg(config.get("switchVar"));
		if (switchVar == null || switchVar.isBlank()) throw new IllegalArgumentException("Switch node requires a switchVar");

		String switchValue = WorkflowExecutionUtils.resolve("${" + switchVar + "}", scope, configMap);

		List<Map<String, Object>> cases = config.get("cases") instanceof List
				? (List<Map<String, Object>>) config.get("cases") : new ArrayList<>();

		Map<String, Object> matchedGraph = null;
		String matchedLabel = "default";

		for (Map<String, Object> c : cases) {
			String caseValue = WorkflowExecutionUtils.strCfg(c.get("value"));
			if (switchValue != null && switchValue.equals(caseValue)) {
				matchedGraph = c.get("subGraph") instanceof Map ? (Map<String, Object>) c.get("subGraph") : null;
				matchedLabel = WorkflowExecutionUtils.strCfg(c.getOrDefault("label", caseValue));
				break;
			}
		}
		if (matchedGraph == null) {
			matchedGraph = config.get("defaultSubGraph") instanceof Map
					? (Map<String, Object>) config.get("defaultSubGraph") : null;
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("matched", matchedLabel);
		result.put("switchValue", switchValue);

		if (matchedGraph == null) {
			result.put("executed", false);
			return WorkflowExecutionUtils.GSON.toJson(result);
		}

		List<Map<String, Object>> subNodes = WorkflowNodeContext.graphNodes(matchedGraph);
		List<Map<String, Object>> subEdges = WorkflowNodeContext.graphEdges(matchedGraph);

		String lastOutput = "{}";
		if (subNodes != null && !subNodes.isEmpty()) {
			AtomicBoolean cancelFlag = ctx.cancelFlag();
			List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(subNodes, subEdges);
			WorkflowDatabaseUtility.insertAllNodeOutputs(runId, ordered);
			for (Map<String, Object> subNode : ordered) {
				if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");
				String subOutputVar = (String) subNode.get("outputVar");
				Map<String, Object> subResult = ctx.nodeDispatcher().dispatch(subNode, scope);
				String status = (String) subResult.get(WorkflowConstants.STATUS);
				if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
					throw new IllegalStateException("Switch branch failed: " + subResult.get(WorkflowConstants.ERROR_MESSAGE));
				}
				String out = (String) subResult.get("outputValue");
				if (out != null) {
					lastOutput = out;
					if (subOutputVar != null && !subOutputVar.isBlank()) scope.put(subOutputVar, out);
				}
			}
		}

		result.put("executed", true);
		result.put("output", lastOutput);
		return WorkflowExecutionUtils.GSON.toJson(result);
	}
}
