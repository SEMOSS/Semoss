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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.workflow.WorkflowConstants;
import prerna.reactor.workflow.WorkflowDatabaseUtility;
import prerna.reactor.workflow.WorkflowExecutionUtils;

/**
 * Executes a "sub-workflow" node: recurses into another project's workflow.json,
 * runs it to completion synchronously (we're already on the background executor thread
 * from the parent run), and returns its final run result as this node's raw output.
 *
 * <p>Config: {@code {targetProjectId, inputMapping: {childVar: "${parentVar} literal"}}}.
 * {@code inputMapping} values are resolved against the parent's current scope before
 * being seeded into the child run's initial scope.
 *
 * <p>Unlike every other executor, this one does real orchestration - its own active-run claim,
 * its own DB run row, and (via {@link WorkflowNodeContext#childWorkflowRunner()}) a full nested
 * run with its own cancellation flag and heartbeat. That stays owned by
 * {@code TriggerWorkflowReactor.executeNodes} rather than being reimplemented here - see the
 * "what does NOT change" scoping note on ticket #2746.
 */
public final class SubWorkflowNodeExecutor implements IWorkflowNodeExecutor {

	@Override
	@SuppressWarnings("unchecked")
	public Object execute(WorkflowNodeContext ctx) {
		Map<String, Object> node = ctx.node();
		Map<String, String> scope = ctx.scope();
		Set<String> ancestorProjectIds = ctx.ancestorProjectIds();
		String parentRunId = ctx.runId();
		String parentNodeId = ctx.nodeId();

		Map<String, Object> config = (Map<String, Object>) node.get("config");
		String targetProjectId = config != null
				? (String) config.get(WorkflowConstants.SUB_WORKFLOW_TARGET_PROJECT) : null;
		if (targetProjectId == null || targetProjectId.isBlank()) {
			throw new IllegalArgumentException("Sub-workflow node \"" + node.get("label") +
					"\" has no targetProjectId configured");
		}

		targetProjectId = SecurityProjectUtils.testUserProjectIdForAlias(ctx.insight().getUser(), targetProjectId);
		// Requires edit access, not just view - this node runs the target project's entire
		// workflow graph, which can include database-write/engine-mutation nodes, so it must be
		// held to the same bar as the top-level trigger path (TriggerWorkflowReactor.getProjectId()
		// requires userCanEditProject). View access to a project the caller can't edit was not
		// sufficient authorization to execute mutating nodes inside it.
		if (!SecurityProjectUtils.userCanEditProject(ctx.insight().getUser(), targetProjectId)) {
			throw new IllegalArgumentException("Target workflow project does not exist or user does not have edit access");
		}

		if (ancestorProjectIds.contains(targetProjectId)) {
			throw new IllegalStateException("Cycle detected: workflow " + targetProjectId +
					" is already running upstream in this call chain (" +
					String.join(" -> ", ancestorProjectIds) + ") - a workflow cannot call itself, " +
					"directly or transitively");
		}
		if (ancestorProjectIds.size() >= WorkflowConstants.MAX_SUB_WORKFLOW_DEPTH) {
			throw new IllegalStateException("Sub-workflow call depth exceeded (" +
					WorkflowConstants.MAX_SUB_WORKFLOW_DEPTH + ") - possible runaway recursion");
		}

		// Atomic cluster-safe claim against the shared scheduler DB - same guarantee as the
		// top-level path in TriggerWorkflowReactor.execute(). Claim happens up front (before the
		// potentially-throwing doc-load/topo-sort work) so every failure path below can release
		// it uniformly via the outer catch; on success, the child run's own executeNodes finally
		// releases it once the child run ends.
		String childRunId = UUID.randomUUID().toString();
		if (!WorkflowDatabaseUtility.claimActiveRun(targetProjectId, childRunId)) {
			String activeRun = WorkflowDatabaseUtility.getActiveRun(targetProjectId);
			throw new IllegalStateException("Target workflow " + targetProjectId +
					" already has an active run (" + activeRun + ") - cannot start a sub-workflow call " +
					"while it is busy");
		}

		try {
			Map<String, Object> childDoc = WorkflowExecutionUtils.loadWorkflowDoc(targetProjectId);
			Map<String, Object> childGraph = (Map<String, Object>) childDoc.get("graph");
			List<Map<String, Object>> childNodes = (List<Map<String, Object>>) childGraph.get("nodes");
			List<Map<String, Object>> childEdges = (List<Map<String, Object>>) childGraph.get("edges");
			Map<String, String> childConfigMap = WorkflowExecutionUtils.loadConfig(targetProjectId);

			List<Map<String, Object>> childOrdered = WorkflowExecutionUtils.topoSort(childNodes, childEdges);
			if (childOrdered.isEmpty()) {
				throw new IllegalArgumentException("Target workflow " + targetProjectId + " has no nodes to execute");
			}

			Map<String, String> childInitialScope = new HashMap<>();
			Object inputMappingRaw = config.get(WorkflowConstants.SUB_WORKFLOW_INPUT_MAPPING);
			Map<String, Object> inputMapping = WorkflowExecutionUtils.coerceToMap(inputMappingRaw);
			for (Map.Entry<String, Object> e : inputMapping.entrySet()) {
				String template = e.getValue() != null ? e.getValue().toString() : "";
				childInitialScope.put(e.getKey(), WorkflowExecutionUtils.resolve(template, scope, Collections.emptyMap()));
			}

			Set<String> childAncestors = new HashSet<>(ancestorProjectIds);
			childAncestors.add(targetProjectId);

			String userId = getUserId(ctx);
			WorkflowDatabaseUtility.insertRun(childRunId, targetProjectId, WorkflowConstants.DEFAULT_WORKFLOW_ID,
					WorkflowConstants.TRIGGER_SUB_WORKFLOW, null, childOrdered.size(), userId,
					parentRunId, parentNodeId);
			WorkflowDatabaseUtility.insertAllNodeOutputs(childRunId, childOrdered);

			Map<String, Object> childResult = ctx.childWorkflowRunner().run(childRunId, targetProjectId,
					childOrdered, childConfigMap, new HashMap<>(), childInitialScope, childAncestors);

			String childStatus = (String) childResult.get(WorkflowConstants.STATUS);
			if (!WorkflowConstants.STATUS_SUCCESS.equals(childStatus)) {
				throw new IllegalStateException("Sub-workflow " + targetProjectId + " (run " + childRunId +
						") did not complete successfully: status=" + childStatus + ", error=" +
						childResult.get(WorkflowConstants.ERROR_MESSAGE));
			}
			return childResult;
		} catch (RuntimeException e) {
			// Any failure before the child run's own executeNodes was reached (bad workflow
			// definition, empty graph, etc.) means its finally never ran to release the slot -
			// release it here. If executeNodes WAS reached, its finally already released it, and
			// this release is a harmless no-op (DELETE affecting zero rows).
			WorkflowDatabaseUtility.releaseActiveRun(targetProjectId, childRunId);
			throw e;
		}
	}

	private static String getUserId(WorkflowNodeContext ctx) {
		if (ctx.insight().getUser() != null && ctx.insight().getUser().getPrimaryLoginToken() != null) {
			return ctx.insight().getUser().getPrimaryLoginToken().getId();
		}
		return "system";
	}
}
