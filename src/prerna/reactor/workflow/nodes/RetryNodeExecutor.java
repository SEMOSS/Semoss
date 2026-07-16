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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "retry" node: runs the inner sub-pipeline, retrying up to {@code maxAttempts} times
 * (with optional exponential backoff) on failure. Each attempt runs against a fresh copy of the
 * parent scope; only a successful attempt's scope changes are promoted back to the parent.
 */
public final class RetryNodeExecutor implements IWorkflowNodeExecutor {

	private static final Logger classLogger = LogManager.getLogger(RetryNodeExecutor.class);

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) throws InterruptedException {
		Map<String, String> scope = ctx.scope();
		Map<String, Object> config = ctx.config();

		int maxAttempts = 3;
		Object maxRaw = config.get("maxAttempts");
		if (maxRaw != null) try { maxAttempts = Integer.parseInt(maxRaw.toString()); } catch (NumberFormatException ignored) {}

		int backoffSeconds = 5;
		Object backoffRaw = config.get("backoffSeconds");
		if (backoffRaw != null) try { backoffSeconds = Integer.parseInt(backoffRaw.toString()); } catch (NumberFormatException ignored) {}

		boolean exponential = Boolean.parseBoolean(WorkflowExecutionUtils.strCfg(config.getOrDefault("exponential", "false")));

		Map<String, Object> subGraph = config.get("subGraph") instanceof Map
				? (Map<String, Object>) config.get("subGraph") : null;
		List<Map<String, Object>> subNodes = WorkflowNodeContext.graphNodes(subGraph);
		List<Map<String, Object>> subEdges = WorkflowNodeContext.graphEdges(subGraph);

		if (subNodes == null || subNodes.isEmpty()) {
			Map<String, Object> r = new LinkedHashMap<>();
			r.put("attempts", 0);
			r.put("succeeded", false);
			return WorkflowExecutionUtils.GSON.toJson(r);
		}

		AtomicBoolean cancelFlag = ctx.cancelFlag();
		Exception lastError = null;

		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");

			Map<String, String> attemptScope = new HashMap<>(scope);
			try {
				List<Map<String, Object>> ordered = WorkflowExecutionUtils.topoSort(subNodes, subEdges);
				String lastOutput = "{}";
				for (Map<String, Object> subNode : ordered) {
					if (cancelFlag != null && cancelFlag.get()) throw new IllegalStateException("Run cancelled by user");
					String subOutputVar = (String) subNode.get("outputVar");
					Map<String, Object> subResult = ctx.nodeDispatcher().dispatch(subNode, attemptScope);
					String status = (String) subResult.get(WorkflowConstants.STATUS);
					if (!WorkflowConstants.NODE_STATUS_SUCCESS.equals(status)) {
						throw new IllegalStateException((String) subResult.get(WorkflowConstants.ERROR_MESSAGE));
					}
					String out = (String) subResult.get("outputValue");
					if (out != null) {
						lastOutput = out;
						if (subOutputVar != null && !subOutputVar.isBlank()) attemptScope.put(subOutputVar, out);
					}
				}
				// Success - promote attempt scope to parent
				scope.putAll(attemptScope);
				Map<String, Object> r = new LinkedHashMap<>();
				r.put("attempts", attempt);
				r.put("succeeded", true);
				return WorkflowExecutionUtils.GSON.toJson(r);

			} catch (IllegalStateException e) {
				if (e.getMessage() != null && e.getMessage().contains("cancelled")) throw e;
				lastError = e;
				classLogger.warn("Retry attempt {}/{} failed: {}", attempt, maxAttempts, e.getMessage());
				if (attempt < maxAttempts) {
					int delay = exponential ? backoffSeconds * attempt : backoffSeconds;
					TimeUnit.SECONDS.sleep(Math.min(delay, 300));
				}
			}
		}
		throw new IllegalStateException("All " + maxAttempts + " retry attempts failed. Last: " +
				(lastError != null ? lastError.getMessage() : "unknown"), lastError);
	}
}
