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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import prerna.om.Insight;

/**
 * Single param object bundling everything an {@link IWorkflowNodeExecutor} needs to run one
 * node - replacing the inconsistent, differently-shaped per-method argument lists the previous
 * {@code executeXNode} private methods each had (e.g. {@code executeWaitNode(node, scope,
 * configMap)} took 3 args, {@code executeSwitchNode(runId, node, scope, configMap,
 * ancestorProjectIds)} took 5).
 *
 * <p>Immutable - {@code scope} and {@code ancestorProjectIds} are references to the caller's
 * live, mutable collections (executors write node outputs back into {@code scope} exactly as
 * the previous {@code executeXNode} methods did), but the context object itself carries no
 * mutable state of its own.
 *
 * @param runId              the current run's id
 * @param projectId          the project id this run belongs to
 * @param node               this node's full definition from {@code workflow.json}
 *                           ({@code id}, {@code label}, {@code type}, {@code config}, ...)
 * @param scope              the run's current execution scope (prior node outputs, keyed by
 *                           {@code outputVar}) - mutable, shared with the caller
 * @param configMap          the workflow's {@code workflow-config.json} key/value pairs
 * @param ancestorProjectIds the chain of project ids already executing on this call stack,
 *                           including this run's own project id - used by
 *                           {@code SubWorkflowNodeExecutor} for the self/transitive-call cycle
 *                           guard
 * @param insight            the execution context - engines/reactors this node calls need it
 * @param cancelFlag         the run's cancellation flag, already resolved once by the caller
 *                           (cluster-safe: reflects both the local {@code AtomicBoolean} and
 *                           the DB {@code CANCEL_REQUESTED} column at the time this node
 *                           started) - executors that loop internally (e.g. a future retry/
 *                           backoff inside a single node) should check this between iterations
 * @param nodeDispatcher     recurses into a single inner/branch node - see {@link NodeDispatcher}.
 *                           Only used by composite executors (conditional, while-loop, try-catch,
 *                           switch, retry, parallel); {@code null} is never passed - executors
 *                           that don't need it simply don't call it
 * @param childWorkflowRunner runs an entire target project's workflow graph as a nested child
 *                           run - see {@link ChildWorkflowRunner}. Only used by
 *                           {@code SubWorkflowNodeExecutor}
 */
public record WorkflowNodeContext(
		String runId,
		String projectId,
		Map<String, Object> node,
		Map<String, String> scope,
		Map<String, String> configMap,
		Set<String> ancestorProjectIds,
		Insight insight,
		AtomicBoolean cancelFlag,
		NodeDispatcher nodeDispatcher,
		ChildWorkflowRunner childWorkflowRunner) {

	/** Convenience accessor for this node's {@code id} field. */
	public String nodeId() {
		return (String) node.get("id");
	}

	/** Convenience accessor for this node's {@code label} field. */
	public String nodeLabel() {
		Object label = node.get("label");
		return label != null ? label.toString() : "unnamed";
	}

	/** Convenience accessor for this node's {@code type} field. */
	public String nodeType() {
		return (String) node.get("type");
	}

	/** Convenience accessor for this node's {@code config} field, or an empty map if absent. */
	@SuppressWarnings("unchecked")
	public Map<String, Object> config() {
		Object config = node.get("config");
		return config instanceof Map ? (Map<String, Object>) config : Map.of();
	}

	/**
	 * Same list-of-strings shape used throughout the previous {@code executeXNode} methods for
	 * a branch/loop/case's inner nodes (e.g. {@code trueGraph.nodes}, {@code subGraph.nodes}).
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> graphNodes(Map<String, Object> graph) {
		return graph != null && graph.get("nodes") instanceof List
				? (List<Map<String, Object>>) graph.get("nodes") : null;
	}

	/** See {@link #graphNodes(Map)} - the corresponding {@code edges} list. */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> graphEdges(Map<String, Object> graph) {
		return graph != null && graph.get("edges") instanceof List
				? (List<Map<String, Object>>) graph.get("edges") : null;
	}
}
