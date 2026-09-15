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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

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
 * The mutation reconnects compatible control edges, rejects unresolved output references, and
 * persists the graph and remaining node sources under the project lock.
 */
public class RemoveAutomationStepReactor extends AbstractReactor {

	private static final String NODE_ID_KEY = "nodeId";

	public RemoveAutomationStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(),
				required(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
		String nodeId = required(NODE_ID_KEY);
		return AutomationProjectUtils.withLockedDefinition(projectId,
				files -> removeStep(projectId, files, nodeId));
	}

	private NounMetadata removeStep(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeId) {
		AutomationDefinitionValidator.ValidatedDefinition validated =
				AutomationDefinitionValidator.parseAndValidateForAuthoring(files.definition());
		Map<String, Object> removedNode = findNode(validated.nodes(), nodeId);
		if (AutomationConstants.NODE_START.equals(removedNode.get(AutomationConstants.NODE_FIELD_TYPE))) {
			throw new IllegalArgumentException("The trigger node cannot be removed.");
		}

		String outputVar = removedNode.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR) instanceof String value
				? value : null;
		validateNoOutputReferences(validated.nodes(), files.nodeSources(), nodeId, outputVar);

		List<Map<String, Object>> incoming = controlEdges(validated.edges(), nodeId, true);
		List<Map<String, Object>> outgoing = controlEdges(validated.edges(), nodeId, false);
		if (incoming.size() != 1 || outgoing.size() > 1) {
			throw new IllegalArgumentException("Node '" + nodeId
					+ "' does not have a unique sequential predecessor/successor and cannot be safely removed.");
		}

		List<Map<String, Object>> updatedNodes = new ArrayList<>();
		for (Map<String, Object> node : validated.nodes()) {
			if (!nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				updatedNodes.add(node);
			}
		}
		List<Map<String, Object>> updatedEdges = new ArrayList<>();
		for (Map<String, Object> edge : validated.edges()) {
			if (!nodeId.equals(edge.get(AutomationConstants.EDGE_FIELD_SOURCE))
					&& !nodeId.equals(edge.get(AutomationConstants.EDGE_FIELD_TARGET))) {
				updatedEdges.add(edge);
			}
		}
		if (!outgoing.isEmpty()) {
			String predecessor = incoming.get(0).get(AutomationConstants.EDGE_FIELD_SOURCE).toString();
			String successor = outgoing.get(0).get(AutomationConstants.EDGE_FIELD_TARGET).toString();
			if (predecessor.equals(successor)) {
				throw new IllegalArgumentException("Removing node '" + nodeId + "' would create a control self-loop.");
			}
			updatedEdges.add(controlEdge(predecessor, successor,
					incoming.get(0).get(AutomationConstants.EDGE_FIELD_SOURCE_PORT).toString()));
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> updatedGraph = new LinkedHashMap<>((Map<String, Object>) validated.definition()
				.get(AutomationConstants.DOC_GRAPH));
		updatedGraph.put(AutomationConstants.DOC_NODES, updatedNodes);
		updatedGraph.put(AutomationConstants.DOC_EDGES, updatedEdges);
		Map<String, Object> updatedDefinition = new LinkedHashMap<>(validated.definition());
		updatedDefinition.put(AutomationConstants.DOC_GRAPH, updatedGraph);
		Map<String, String> updatedSources = new LinkedHashMap<>(files.nodeSources());
		updatedSources.remove(nodeId);

		AutomationDefinitionService.DefinitionFiles saved = AutomationProjectUtils.saveDefinition(projectId,
				AutomationRuntimeUtils.GSON.toJson(updatedDefinition), updatedSources, this.insight.getUser());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("removed", true);
		result.put("nodeId", nodeId);
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

	private static Map<String, Object> findNode(List<Map<String, Object>> nodes, String nodeId) {
		for (Map<String, Object> node : nodes) {
			if (nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				return node;
			}
		}
		throw new IllegalArgumentException("Automation does not contain node: " + nodeId);
	}

	private static List<Map<String, Object>> controlEdges(List<Map<String, Object>> edges, String nodeId,
			boolean incoming) {
		List<Map<String, Object>> result = new ArrayList<>();
		String endpoint = incoming ? AutomationConstants.EDGE_FIELD_TARGET : AutomationConstants.EDGE_FIELD_SOURCE;
		for (Map<String, Object> edge : edges) {
			if (AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))
					&& nodeId.equals(edge.get(endpoint))) {
				result.add(edge);
			}
		}
		return result;
	}

	private static void validateNoOutputReferences(List<Map<String, Object>> nodes, Map<String, String> sources,
			String removedNodeId, String outputVar) {
		if (outputVar == null || outputVar.isBlank()) {
			return;
		}
		List<String> references = new ArrayList<>();
		for (Map<String, Object> node : nodes) {
			String nodeId = (String) node.get(AutomationConstants.NODE_FIELD_ID);
			if (removedNodeId.equals(nodeId)) {
				continue;
			}
			if (referencesOutput(node.get(AutomationConstants.NODE_FIELD_CONFIG), outputVar)
					|| referencesOutput(sources.get(nodeId), outputVar)) {
				references.add(nodeId);
			}
		}
		if (!references.isEmpty()) {
			throw new IllegalArgumentException("Cannot remove node '" + removedNodeId + "' because output variable '"
					+ outputVar + "' is referenced by nodes " + references + ". Update those nodes first.");
		}
	}

	private static boolean referencesOutput(Object value, String outputVar) {
		if (value instanceof String string) {
			String quoted = Pattern.quote(outputVar);
			return string.contains("${" + outputVar + "}")
					|| Pattern.compile("\\bscope\\s*\\[\\s*(['\"]?)" + quoted + "\\1\\s*\\]")
							.matcher(string).find()
					|| Pattern.compile("\\bscope\\s*\\.\\s*get\\s*\\(\\s*(['\"]?)" + quoted
							+ "\\1\\s*(?:,|\\))")
							.matcher(string).find();
		}
		if (value instanceof Map<?, ?> map) {
			return map.values().stream().anyMatch(item -> referencesOutput(item, outputVar));
		}
		if (value instanceof Iterable<?> iterable) {
			for (Object item : iterable) {
				if (referencesOutput(item, outputVar)) {
					return true;
				}
			}
		}
		return false;
	}

	private static Map<String, Object> controlEdge(String source, String target, String sourcePort) {
		return Map.of(
				"id", "control-" + UUID.randomUUID(),
				AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL,
				AutomationConstants.EDGE_FIELD_SOURCE, source,
				AutomationConstants.EDGE_FIELD_SOURCE_PORT, sourcePort,
				AutomationConstants.EDGE_FIELD_TARGET, target,
				AutomationConstants.EDGE_FIELD_TARGET_PORT, "in");
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		return Map.of(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
	}

	@Override
	public String getReactorDescription() {
		return "Removes one non-trigger automation node after validating dependencies and reconnecting its sequence.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (NODE_ID_KEY.equals(key)) {
			return "Existing non-trigger node ID to remove.";
		}
		return super.getDescriptionForKey(key);
	}
}
