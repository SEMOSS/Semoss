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
import java.util.Locale;
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
	private static final String BRANCH_PORT_KEY = "branchPort";

	public AddAutomationStepReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PROJECT.getKey(), NODE_TYPE_KEY, CONFIG_KEY, LABEL_KEY,
				OUTPUT_VAR_KEY, AFTER_NODE_ID_KEY, BRANCH_PORT_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = editableProjectId();
		String nodeType = required(NODE_TYPE_KEY);
		String label = required(LABEL_KEY);
		String outputVar = outputVariable(nodeType, this.keyValue.get(OUTPUT_VAR_KEY));
		String afterNodeId = this.keyValue.get(AFTER_NODE_ID_KEY);
		String branchPort = this.keyValue.get(BRANCH_PORT_KEY);
		Map<String, Object> config = parseConfig(required(CONFIG_KEY));
		String customSource = customSource(nodeType, config);
		return AutomationProjectUtils.withLockedDefinition(projectId,
				files -> addStep(projectId, files, nodeType, label, outputVar, afterNodeId, branchPort, config,
						customSource));
	}

	private NounMetadata addStep(String projectId, AutomationDefinitionService.DefinitionFiles files,
			String nodeType, String label, String outputVar, String afterNodeId, String branchPort,
			Map<String, Object> config, String customSource) {
		@SuppressWarnings("unchecked")
		Map<String, Object> definition = AutomationRuntimeUtils.GSON.fromJson(files.definition(),
				AutomationRuntimeUtils.MAP_TYPE);
		@SuppressWarnings("unchecked")
		Map<String, Object> graph = (Map<String, Object>) definition.get(AutomationConstants.DOC_GRAPH);
		List<Map<String, Object>> nodes = maps(graph.get(AutomationConstants.DOC_NODES), "graph.nodes");
		List<Map<String, Object>> edges = maps(graph.get(AutomationConstants.DOC_EDGES), "graph.edges");

		if (outputVar != null && nodes.stream()
				.anyMatch(node -> outputVar.equals(node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR)))) {
			throw new IllegalArgumentException("Automation already contains output variable: " + outputVar);
		}

		String parentId = afterNodeId == null || afterNodeId.isBlank()
				? nodes.get(nodes.size() - 1).get(AutomationConstants.NODE_FIELD_ID).toString()
				: afterNodeId;
		if (nodes.stream().noneMatch(node -> parentId.equals(node.get(AutomationConstants.NODE_FIELD_ID)))) {
			throw new IllegalArgumentException("Automation does not contain parent node: " + parentId);
		}
		String sourcePort = sourcePort(nodes, parentId, branchPort);

		String nodeId = uniqueNodeId(nodes, label);
		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, nodeId);
		node.put(AutomationConstants.NODE_FIELD_TYPE, nodeType);
		node.put(AutomationConstants.NODE_FIELD_LABEL, label);
		if (outputVar != null) {
			node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, outputVar);
		}
		node.put(AutomationConstants.NODE_FIELD_CODE_MODE, customSource == null
				? AutomationConstants.NODE_CODE_MODE_GENERATED
				: AutomationConstants.NODE_CODE_MODE_CUSTOM);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		node.put("position", Map.of("x", 240, "y", 80 + nodes.size() * 180));

		List<Map<String, Object>> updatedNodes = new ArrayList<>(nodes);
		updatedNodes.add(node);
		List<Map<String, Object>> updatedEdges = insertAfter(edges, parentId, nodeId, nodeType, sourcePort);
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

	private static String outputVariable(String nodeType, String value) {
		if (AutomationConstants.NODE_CONTROL_IF.equals(nodeType)) {
			if (value != null && !value.isBlank()) {
				throw new IllegalArgumentException("control.if does not produce an outputVar.");
			}
			return null;
		}
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide " + OUTPUT_VAR_KEY + ".");
		}
		return requiredOutputVariable(value);
	}

	private static String requiredOutputVariable(String value) {
		if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			throw new IllegalArgumentException("outputVar must be a valid Python identifier.");
		}
		return value;
	}

	private static String sourcePort(List<Map<String, Object>> nodes, String parentId, String branchPort) {
		String parentType = nodes.stream()
				.filter(node -> parentId.equals(node.get(AutomationConstants.NODE_FIELD_ID)))
				.map(node -> (String) node.get(AutomationConstants.NODE_FIELD_TYPE))
				.findFirst()
				.orElseThrow();
		if (!AutomationConstants.NODE_CONTROL_IF.equals(parentType)) {
			if (branchPort != null && !branchPort.isBlank()) {
				throw new IllegalArgumentException("branchPort can only be used when afterNodeId is a control.if node.");
			}
			return AutomationConstants.CONTROL_PORT_OUT;
		}
		if (!AutomationConstants.CONTROL_PORT_THEN.equals(branchPort)
				&& !AutomationConstants.CONTROL_PORT_ELSE.equals(branchPort)) {
			throw new IllegalArgumentException("Adding after a control.if node requires branchPort 'then' or 'else'.");
		}
		return branchPort;
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
		String prefix = label.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-")
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

	private static List<Map<String, Object>> insertAfter(List<Map<String, Object>> edges, String parentId,
			String nodeId, String nodeType, String sourcePort) {
		List<Map<String, Object>> updated = new ArrayList<>();
		Map<String, Object> replaced = null;
		for (Map<String, Object> edge : edges) {
			if (AutomationConstants.EDGE_KIND_CONTROL.equals(edge.get(AutomationConstants.EDGE_FIELD_KIND))
					&& parentId.equals(edge.get(AutomationConstants.EDGE_FIELD_SOURCE))
					&& sourcePort.equals(edge.get(AutomationConstants.EDGE_FIELD_SOURCE_PORT))) {
				replaced = edge;
				continue;
			}
			updated.add(edge);
		}
		updated.add(controlEdge(parentId, nodeId, sourcePort));
		if (replaced != null) {
			String nodePort = AutomationConstants.NODE_CONTROL_IF.equals(nodeType)
					? AutomationConstants.CONTROL_PORT_THEN
					: AutomationConstants.CONTROL_PORT_OUT;
			updated.add(controlEdge(nodeId, replaced.get(AutomationConstants.EDGE_FIELD_TARGET).toString(), nodePort));
		}
		return updated;
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
	public String getReactorDescription() {
		return "Adds one typed automation node after a specified node.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "The Automation project ID or alias to update.";
		}
		if (NODE_TYPE_KEY.equals(key)) {
			return "The supported typed node to add, such as model.chat or developer.python.";
		}
		if (CONFIG_KEY.equals(key)) {
			return "The node configuration as a JSON object or Base64-encoded JSON object.";
		}
		if (LABEL_KEY.equals(key)) {
			return "The user-facing node label.";
		}
		if (OUTPUT_VAR_KEY.equals(key)) {
			return "The Python identifier that stores this node's output; omitted for control.if.";
		}
		if (AFTER_NODE_ID_KEY.equals(key)) {
			return "The existing node after which this node is inserted; defaults to the last graph node.";
		}
		if (BRANCH_PORT_KEY.equals(key)) {
			return "The then or else port when inserting directly after a control.if node.";
		}
		return super.getDescriptionForKey(key);
	}
}
