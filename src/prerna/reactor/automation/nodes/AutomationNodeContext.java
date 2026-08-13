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
package prerna.reactor.automation.nodes;

import prerna.reactor.automation.utils.AutomationExecutionUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;

/**
 * Param bundle passed to every {@link IAutomationNodeExecutor}.
 *
 * <p>Consolidates all per-node execution inputs so executor implementations have a single,
 * typed object to work with rather than long parameter lists.
 *
 * <p>Note: {@code scope} is intentionally mutable and shared across all node executions within
 * one run. Each node may add output variables to {@code scope} after it completes successfully,
 * making those values available to subsequent nodes via {@code ${varName}} substitution.
 *
 * @param runId      the automation run ID (use {@code "test"} for single-node test runs)
 * @param projectId  the owning project UUID
 * @param node       the raw node definition map from {@code automation.json}
 * @param scope      mutable variable scope — string key→value pairs resolved by {@code ${varName}};
 *                   shared across all nodes in the run and updated after each node completes
 * @param configMap  project automation config key→value pairs resolved by {@code ${config.KEY}}
 * @param insight    caller's {@link Insight} context (carries user, session, pixel engine)
 * @param cancelFlag shared flag the executor checks to honour mid-node cancellation requests
 */
public record AutomationNodeContext(
		String runId,
		String projectId,
		Map<String, Object> node,
		Map<String, String> scope,
		Map<String, String> configMap,
		Insight insight,
		AtomicBoolean cancelFlag) {

	public String nodeId() {
		return (String) node.get(AutomationConstants.NODE_FIELD_ID);
	}

	public String nodeLabel() {
		Object label = node.get(AutomationConstants.NODE_FIELD_LABEL);
		return label != null ? label.toString() : AutomationConstants.UNNAMED_NODE_LABEL;
	}

	public String nodeType() {
		return (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
	}

	/**
	 * Returns the node's config map, or an empty map if absent.
	 *
	 * <p>The cast to {@code Map<String, Object>} is safe: Gson always deserializes JSON objects
	 * as {@code Map<String, Object>}, and the automation document is loaded exclusively via Gson
	 * in {@link prerna.reactor.automation.AutomationExecutionUtils#loadAutomationDoc}.
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> config() {
		Object config = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		return config instanceof Map ? (Map<String, Object>) config : Map.of();
	}
}
