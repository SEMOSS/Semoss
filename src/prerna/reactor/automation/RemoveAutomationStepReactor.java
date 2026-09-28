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
package prerna.reactor.automation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Removes one non-trigger node from the canonical Automation aggregate.
 *
 * <p>
 * The mutation detaches the remaining flow and persists the graph and remaining
 * node sources under the project lock. Execution validation remains responsible
 * for rejecting a draft with detached nodes.
 */
public class RemoveAutomationStepReactor extends AbstractReactor {

	private static final String NODE_ID_KEY = "nodeId";
	private static final String REMOVE_DOWNSTREAM_KEY = "removeDownstream";

	public RemoveAutomationStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, REMOVE_DOWNSTREAM_KEY };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = AutomationProjectUtils
				.getEditableAutomationProject(this.insight.getUser(), required(ReactorKeysEnum.PROJECT.getKey()))
				.getProjectId();
		String nodeId = required(NODE_ID_KEY);
		boolean removeDownstream = optionalBoolean(REMOVE_DOWNSTREAM_KEY, false);
		return AutomationProjectUtils.withLockedDefinition(projectId,
				files -> removeStep(projectId, files, nodeId, removeDownstream));
	}

	private NounMetadata removeStep(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeId, boolean removeDownstream) {
		AutomationDefinitionValidator.ValidatedDefinition validated = AutomationDefinitionValidator
				.parseAndValidateForAuthoring(files.definition());
		Map<String, Object> removedNode = findNode(validated.nodes(), nodeId);
		if (AutomationConstants.NODE_START.equals(removedNode.get(AutomationConstants.NODE_FIELD_TYPE))) {
			throw new IllegalArgumentException("The trigger node cannot be removed.");
		}

		Set<String> removedNodeIds = removeDownstream ? downstreamNodeIds(validated.edges(), nodeId) : Set.of(nodeId);

		List<Map<String, Object>> updatedNodes = new ArrayList<>();
		for (Map<String, Object> node : validated.nodes()) {
			if (!removedNodeIds.contains(node.get(AutomationConstants.NODE_FIELD_ID))) {
				updatedNodes.add(node);
			}
		}
		List<Map<String, Object>> updatedEdges = new ArrayList<>();
		for (Map<String, Object> edge : validated.edges()) {
			if (!removedNodeIds.contains(edge.get(AutomationConstants.EDGE_FIELD_SOURCE))
					&& !removedNodeIds.contains(edge.get(AutomationConstants.EDGE_FIELD_TARGET))) {
				updatedEdges.add(edge);
			}
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> updatedGraph = new LinkedHashMap<>(
				(Map<String, Object>) validated.definition().get(AutomationConstants.DOC_GRAPH));
		updatedGraph.put(AutomationConstants.DOC_NODES, updatedNodes);
		updatedGraph.put(AutomationConstants.DOC_EDGES, updatedEdges);
		Map<String, Object> updatedDefinition = new LinkedHashMap<>(validated.definition());
		updatedDefinition.put(AutomationConstants.DOC_GRAPH, updatedGraph);
		Map<String, String> updatedSources = new LinkedHashMap<>(files.nodeSources());
		removedNodeIds.forEach(updatedSources::remove);

		AutomationDefinitionService.DefinitionFiles saved = AutomationProjectUtils.saveDefinition(projectId,
				AutomationRuntimeUtils.GSON.toJson(updatedDefinition), updatedSources, this.insight.getUser());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("removed", true);
		result.put("nodeId", nodeId);
		result.put("removedNodeIds", removedNodeIds);
		result.put(AutomationConstants.RESULT_REVISION,
				AutomationDefinitionService.calculateRevision(saved.definition(), saved.nodeSources()));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private String required(String key) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	private boolean optionalBoolean(String key, boolean defaultValue) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
			throw new IllegalArgumentException(key + " must be true or false.");
		}
		return Boolean.parseBoolean(value);
	}

	private static Map<String, Object> findNode(List<Map<String, Object>> nodes, String nodeId) {
		for (Map<String, Object> node : nodes) {
			if (nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				return node;
			}
		}
		throw new IllegalArgumentException("Automation does not contain node: " + nodeId);
	}

	/** Returns the selected node and every node reachable after it. */
	static Set<String> downstreamNodeIds(List<Map<String, Object>> edges, String nodeId) {
		Map<String, List<String>> outgoing = new LinkedHashMap<>();
		for (Map<String, Object> edge : edges) {
			if (!AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))) {
				continue;
			}
			String source = edge.get(AutomationConstants.EDGE_FIELD_SOURCE).toString();
			String target = edge.get(AutomationConstants.EDGE_FIELD_TARGET).toString();
			outgoing.computeIfAbsent(source, ignored -> new ArrayList<>()).add(target);
		}
		Set<String> result = new LinkedHashSet<>();
		ArrayDeque<String> pending = new ArrayDeque<>();
		pending.add(nodeId);
		while (!pending.isEmpty()) {
			String current = pending.removeFirst();
			if (result.add(current)) {
				pending.addAll(outgoing.getOrDefault(current, List.of()));
			}
		}
		return result;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		return Map.of(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
	}

	@Override
	public String getReactorDescription() {
		return "Removes and detaches one Automation step, or removes the step and its downstream flow.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (NODE_ID_KEY.equals(key)) {
			return "Existing non-trigger node ID to remove.";
		}
		if (REMOVE_DOWNSTREAM_KEY.equals(key)) {
			return "When true, also removes every step reachable after the selected node; defaults to false.";
		}
		return super.getDescriptionForKey(key);
	}
}
