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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Adds a generated, typed node to an automation graph for an MCP authoring turn.
 *
 * <p>The tool deliberately owns node identity, control-edge insertion, and generated source.
 * Models may describe a node configuration, but cannot replace the graph or select source for a
 * generated node.
 */
public class AddAutomationStepReactor extends AbstractReactor {

	private static final String NODE_TYPE_KEY = "nodeType";
	private static final String CONFIG_KEY = "config";
	private static final String LABEL_KEY = "label";
	private static final String OUTPUT_VAR_KEY = "outputVar";
	private static final String AFTER_NODE_ID_KEY = "afterNodeId";
	private static final String BRANCH_CONDITION_KEY = "branchCondition";

	public AddAutomationStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(), NODE_TYPE_KEY, CONFIG_KEY, LABEL_KEY,
				OUTPUT_VAR_KEY, AFTER_NODE_ID_KEY, BRANCH_CONDITION_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = editableProjectId();
		String nodeType = required(NODE_TYPE_KEY);
		String label = required(LABEL_KEY);
		String outputVar = requiredOutputVariable(required(OUTPUT_VAR_KEY));
		String afterNodeId = this.keyValue.get(AFTER_NODE_ID_KEY);
		String branchCondition = this.keyValue.get(BRANCH_CONDITION_KEY);
		Map<String, Object> config = parseConfig(required(CONFIG_KEY));
		if (config.containsKey(AutomationConstants.CONFIG_BRANCH_CONDITION)) {
			throw new IllegalArgumentException("Use the branchCondition argument to branch the parent node; "
					+ "the new node cannot be saved as a branch without both paths.");
		}
		String customSource = customSource(nodeType, config);
		return AutomationProjectUtils.withLockedDefinition(projectId,
				files -> addStep(projectId, files, nodeType, label, outputVar, afterNodeId,
						branchCondition, config, customSource));
	}

	private NounMetadata addStep(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeType, String label, String outputVar, String afterNodeId, String branchCondition,
			Map<String, Object> config, String customSource) {
		@SuppressWarnings("unchecked")
		Map<String, Object> definition = AutomationRuntimeUtils.GSON.fromJson(files.definition(),
				AutomationRuntimeUtils.MAP_TYPE);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) definition.get(AutomationConstants.DOC_GRAPH);
		List<Map<String, Object>> nodes = maps(graph.get(AutomationConstants.DOC_NODES), "graph.nodes");
		List<Map<String, Object>> edges = maps(graph.get(AutomationConstants.DOC_EDGES), "graph.edges");

		if (nodes.stream().anyMatch(node -> outputVar.equals(node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR)))) {
			throw new IllegalArgumentException("Automation already contains output variable: " + outputVar);
		}

		String parentId = afterNodeId == null || afterNodeId.isBlank()
				? nodes.get(nodes.size() - 1).get(AutomationConstants.NODE_FIELD_ID).toString()
				: afterNodeId;
		Map<String, Object> parent = nodes.stream()
				.filter(node -> parentId.equals(node.get(AutomationConstants.NODE_FIELD_ID)))
				.findFirst().orElse(null);
		if (parent == null) {
			throw new IllegalArgumentException("Automation does not contain parent node: " + parentId);
		}
		boolean createBranch = branchCondition != null && !branchCondition.isBlank();
		if (createBranch) {
			if (AutomationConstants.NODE_START.equals(parent.get(AutomationConstants.NODE_FIELD_TYPE))) {
				throw new IllegalArgumentException("trigger.start cannot be converted into a branching node.");
			}
			setBranchCondition(parent, branchCondition);
		}

		String nodeId = uniqueNodeId(nodes, label);
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, nodeId);
		node.put(AutomationConstants.NODE_FIELD_TYPE, nodeType);
		node.put(AutomationConstants.NODE_FIELD_LABEL, label);
		node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, outputVar);
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE, customSource == null
				? AutomationConstants.NODE_CODE_MODE_GENERATED
				: AutomationConstants.NODE_CODE_MODE_CUSTOM);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		node.put("position", Map.of("x", 240, "y", 80 + nodes.size() * 180));

		List<Map<String, Object>> updatedNodes = new ArrayList<>(nodes);
		updatedNodes.add(node);
		List<Map<String, Object>> updatedEdges = createBranch
				? insertElseBranch(edges, parentId, nodeId)
				: insertAfter(edges, parentId, nodeId);
		Map<String, Object> updatedGraph = new LinkedHashMap<>(graph);
		updatedGraph.put(AutomationConstants.DOC_NODES, updatedNodes);
		updatedGraph.put(AutomationConstants.DOC_EDGES, updatedEdges);
		Map<String, Object> updatedDefinition = new LinkedHashMap<>(definition);
		updatedDefinition.put(AutomationConstants.DOC_GRAPH, updatedGraph);
		String json = AutomationRuntimeUtils.GSON.toJson(updatedDefinition);

		Map<String, String> nodeSources = new LinkedHashMap<>(files.nodeSources());
		if (customSource != null) {
			nodeSources.put(nodeId, customSource);
		}
		AutomationDefinitionService.DefinitionFiles saved = AutomationProjectUtils.saveDefinition(projectId, json,
				nodeSources, this.insight.getUser());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("node", node);
		result.put(AutomationConstants.DOC_NODE_SOURCES, saved.nodeSources());
		result.put(AutomationConstants.RESULT_REVISION,
				AutomationDefinitionService.calculateRevision(saved.definition(), saved.nodeSources()));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private String editableProjectId() {
		return AutomationProjectUtils.getEditableAutomationProject(this.insight.getUser(),
				required(ReactorKeysEnum.PROJECT.getKey())).getProjectId();
	}

	private String required(String key) {
		String value = this.keyValue.get(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + key + ".");
		}
		return value;
	}

	private static String requiredOutputVariable(String value) {
		if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			throw new IllegalArgumentException("outputVar must be a valid Python identifier.");
		}
		return value;
	}

	private static String customSource(String nodeType, Map<String, Object> config) {
		if (!AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType)) {
			return null;
		}
		Object value = config.remove("source");
		if (!(value instanceof String source) || source.isBlank()) {
			throw new IllegalArgumentException("developer.python requires config.source defining run(scope).");
		}
		if (!source.contains("def run(scope):")) {
			throw new IllegalArgumentException("developer.python source must define run(scope).");
		}
		return source;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parseConfig(String rawOrBase64) {
		String raw;
		try {
			raw = new String(Base64.getDecoder().decode(rawOrBase64), StandardCharsets.UTF_8);
		} catch (IllegalArgumentException ignored) {
			raw = rawOrBase64;
		}
		Object parsed = AutomationRuntimeUtils.GSON.fromJson(raw, Object.class);
		if (!(parsed instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("config must be a JSON object.");
		}
		return new LinkedHashMap<>((Map<String, Object>) parsed);
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> maps(Object value, String field) {
		if (!(value instanceof List<?> list) || list.stream().anyMatch(item -> !(item instanceof Map<?, ?>))) {
			throw new IllegalArgumentException(field + " must be an array of objects.");
		}
		return (List<Map<String, Object>>) list;
	}

	private static String uniqueNodeId(List<Map<String, Object>> nodes, String label) {
		String prefix = label.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9]+", "-")
				.replaceAll("^-+|-+$", "");
		if (prefix.isBlank()) {
			prefix = "step";
		}
		String id = prefix + "-" + UUID.randomUUID();
		while (containsNodeId(nodes, id)) {
			id = prefix + "-" + UUID.randomUUID();
		}
		return id;
	}

	private static boolean containsNodeId(List<Map<String, Object>> nodes, String id) {
		for (Map<String, Object> node : nodes) {
			if (id.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				return true;
			}
		}
		return false;
	}

	private static List<Map<String, Object>> insertAfter(
			List<Map<String, Object>> edges, String parentId, String nodeId) {
		List<Map<String, Object>> updated = new ArrayList<>();
		Map<String, Object> replaced = null;
		for (Map<String, Object> edge : edges) {
			if (AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))
					&& parentId.equals(edge.get(AutomationConstants.EDGE_FIELD_SOURCE))) {
				if (replaced != null) {
					throw new IllegalArgumentException("Cannot insert after branching node '" + parentId
							+ "'. Insert after a node on the intended branch instead.");
				}
				replaced = edge;
				continue;
			}
			updated.add(edge);
		}
		updated.add(controlEdge(parentId, nodeId));
		if (replaced != null) {
			updated.add(controlEdge(nodeId, replaced.get(AutomationConstants.EDGE_FIELD_TARGET).toString()));
		}
		return updated;
	}

	@SuppressWarnings("unchecked")
	private static void setBranchCondition(Map<String, Object> parent, String branchCondition) {
		Map<String, Object> config = parent.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> map
				? new LinkedHashMap<>((Map<String, Object>) map)
				: new LinkedHashMap<>();
		if (config.containsKey(AutomationConstants.CONFIG_BRANCH_CONDITION)) {
			throw new IllegalArgumentException("Node '" + parent.get(AutomationConstants.NODE_FIELD_ID)
					+ "' is already a branching node.");
		}
		config.put(AutomationConstants.CONFIG_BRANCH_CONDITION, branchCondition);
		parent.put(AutomationConstants.NODE_FIELD_CONFIG, config);
	}

	private static List<Map<String, Object>> insertElseBranch(
			List<Map<String, Object>> edges, String parentId, String elseNodeId) {
		List<Map<String, Object>> updated = new ArrayList<>();
		Map<String, Object> successorEdge = null;
		for (Map<String, Object> edge : edges) {
			if (AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))
					&& parentId.equals(edge.get(AutomationConstants.EDGE_FIELD_SOURCE))) {
				if (successorEdge != null) {
					throw new IllegalArgumentException("Node '" + parentId + "' already has multiple control paths.");
				}
				successorEdge = edge;
				continue;
			}
			updated.add(edge);
		}
		if (successorEdge == null) {
			throw new IllegalArgumentException("To create a branch after node '" + parentId
					+ "', first add the intended Then step, then add the Else step with branchCondition.");
		}
		if (!AutomationConstants.CONTROL_PORT_OUT.equals(
				successorEdge.get(AutomationConstants.EDGE_FIELD_SOURCE_PORT))) {
			throw new IllegalArgumentException("Node '" + parentId + "' is not a sequential node.");
		}
		updated.add(controlEdge(parentId, AutomationConstants.CONTROL_PORT_THEN,
				successorEdge.get(AutomationConstants.EDGE_FIELD_TARGET).toString()));
		updated.add(controlEdge(parentId, AutomationConstants.CONTROL_PORT_ELSE, elseNodeId));
		return updated;
	}

	private static Map<String, Object> controlEdge(String source, String target) {
		return controlEdge(source, AutomationConstants.CONTROL_PORT_OUT, target);
	}

	private static Map<String, Object> controlEdge(String source, String sourcePort, String target) {
		return Map.of(
				"id", "control-" + UUID.randomUUID(),
				AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL,
				AutomationConstants.EDGE_FIELD_SOURCE, source,
				AutomationConstants.EDGE_FIELD_SOURCE_PORT, sourcePort,
				AutomationConstants.EDGE_FIELD_TARGET, target,
				AutomationConstants.EDGE_FIELD_TARGET_PORT, "in");
	}

	@Override
	public String getReactorDescription() {
		return "Adds one typed Python automation node sequentially or as an atomic Else branch.";
	}
}
