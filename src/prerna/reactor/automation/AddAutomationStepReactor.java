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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.reactor.automation.utils.PixelExecutionUtils;

import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Adds one managed Python-backed action to an existing automation definition.
 *
 * <p>The source is created before the graph references it, preventing a saved graph from
 * referring to a missing implementation. Existing source is never replaced; edits must use the
 * dedicated preview/apply source-update flow.
 */
public class AddAutomationStepReactor extends AbstractAutomationStepSourceReactor {

	private static final String LABEL_KEY = "label";
	private static final String OUTPUT_VAR_KEY = "outputVar";
	private static final String AFTER_NODE_ID_KEY = "afterNodeId";
	private static final String ACTION_ID_KEY = "actionId";

	public AddAutomationStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), NODE_ID_KEY, ACTION_ID_KEY, CONFIG_KEY,
				LABEL_KEY, OUTPUT_VAR_KEY, AFTER_NODE_ID_KEY };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String actionId = requireNonblank(this.keyValue.get(ACTION_ID_KEY), ACTION_ID_KEY);
		PreparedStep step = prepareStepForAction(actionId);
		if (sourceExists(step)) {
			throw new IllegalArgumentException("Automation step source already exists: " + step.getStepRef()
					+ ". Preview and apply an explicit update instead.");
		}

		String label = requireNonblank(this.keyValue.get(LABEL_KEY), LABEL_KEY);
		String outputVar = requireOutputVar(this.keyValue.get(OUTPUT_VAR_KEY));
		String afterNodeId = this.keyValue.get(AFTER_NODE_ID_KEY);
		Map<String, Object> document = readDefinition(step.getProjectId());
		Map<String, Object> graph = requireMap(document.get(AutomationConstants.DOC_GRAPH), "graph");
		List<Map<String, Object>> nodes = requireMapList(graph.get(AutomationConstants.DOC_NODES), "graph.nodes");
		List<Map<String, Object>> edges = requireMapList(graph.get(AutomationConstants.DOC_EDGES), "graph.edges");

		Map<String, Object> parent = findNode(nodes, afterNodeId == null || afterNodeId.isBlank()
				? "trigger" : afterNodeId);
		if (findNodeOrNull(nodes, this.keyValue.get(NODE_ID_KEY)) != null) {
			throw new IllegalArgumentException("Automation definition already contains node id: "
					+ this.keyValue.get(NODE_ID_KEY) + ".");
		}
		if (nodes.stream().anyMatch(node -> outputVar.equals(node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR)))) {
			throw new IllegalArgumentException("Automation definition already contains output variable: " + outputVar + ".");
		}

		AutomationStepGenerationService.GeneratedStep generated = step.getGenerated();
		Map<String, Object> config = new LinkedHashMap<>(generated.getResolvedConfig());
		config.put(AutomationConstants.CONFIG_OPERATION,
				AutomationStepTemplateRegistry.selectAction(step.getNodeType(), config).getOperation());
		config.put(AutomationConstants.CONFIG_STEP_REF, step.getStepRef());
		config.put("generatedStep", generatedMetadata(generated));
		if (AutomationConstants.NODE_PYTHON_STEP.equals(step.getNodeType())) {
			config.put(AutomationConstants.CONFIG_PURPOSE, label);
		}

		Map<String, Object> node = new LinkedHashMap<>();
		node.put(AutomationConstants.NODE_FIELD_ID, this.keyValue.get(NODE_ID_KEY));
		node.put(AutomationConstants.NODE_FIELD_TYPE, step.getNodeType());
		node.put(AutomationConstants.NODE_FIELD_LABEL, label);
		node.put(AutomationConstants.NODE_FIELD_OUTPUT_VAR, outputVar);
		node.put(AutomationConstants.NODE_FIELD_CONFIG, config);
		node.put("position", Map.of("x", 0, "y", nodes.size() * 160));

		List<Map<String, Object>> updatedNodes = new ArrayList<>(nodes);
		updatedNodes.add(node);
		List<Map<String, Object>> updatedEdges = new ArrayList<>(edges);
		String sourceNodeId = parent.get(AutomationConstants.NODE_FIELD_ID).toString();
		String targetNodeId = this.keyValue.get(NODE_ID_KEY);
		updatedEdges.add(Map.of("id", "e-" + sourceNodeId + "-" + targetNodeId,
				AutomationConstants.EDGE_FIELD_SOURCE, sourceNodeId,
				AutomationConstants.EDGE_FIELD_TARGET, targetNodeId));

		Map<String, Object> updatedGraph = new LinkedHashMap<>(graph);
		updatedGraph.put(AutomationConstants.DOC_NODES, updatedNodes);
		updatedGraph.put(AutomationConstants.DOC_EDGES, updatedEdges);
		Map<String, Object> updatedDocument = new LinkedHashMap<>(document);
		updatedDocument.put(AutomationConstants.DOC_GRAPH, updatedGraph);
		String updatedJson = AutomationExecutionUtils.GSON.toJson(updatedDocument);
		AutomationDefinitionValidator.parseAndValidate(updatedJson);

		saveSource(step, generated.getSource(), "Add managed automation Python step");
		saveAutomation(step.getProjectId(), updatedJson);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("node", node);
		result.put("edge", updatedEdges.get(updatedEdges.size() - 1));
		result.put("stepRef", step.getStepRef());
		result.put("sourceHash", generated.getSourceHash());
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private Map<String, Object> readDefinition(String projectId) {
		File automationFile = Paths.get(AssetUtility.getProjectPortalsFolder(projectId),
				AutomationConstants.AUTOMATION_FILE_NAME).toFile();
		if (!automationFile.isFile()) {
			throw new IllegalArgumentException("Automation definition does not exist for project: " + projectId + ".");
		}
		try {
			return AutomationExecutionUtils.GSON.fromJson(Files.readString(automationFile.toPath(), StandardCharsets.UTF_8),
					AutomationExecutionUtils.MAP_TYPE);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read automation definition.", e);
		}
	}

	private void saveAutomation(String projectId, String json) {
		String encoded = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
		PixelExecutionUtils.runAndCollect(this.insight, "SaveAutomation(project="
				+ pixelStringList(projectId) + ", json=" + pixelStringList(encoded) + ");");
	}

	private static Map<String, Object> generatedMetadata(AutomationStepGenerationService.GeneratedStep generated) {
		Map<String, Object> metadata = new LinkedHashMap<>();
		metadata.put("actionId", generated.getActionId());
		metadata.put("description", generated.getDescription());
		metadata.put("usage", generated.getUsage());
		metadata.put("sourceHash", generated.getSourceHash());
		metadata.put("setupHash", generated.getSetupHash());
		metadata.put("templateVersion", Integer.toString(generated.getTemplateVersion()));
		return metadata;
	}

	private static String requireOutputVar(String value) {
		if (value == null || !value.matches("^[A-Za-z_][A-Za-z0-9_]*$")) {
			throw new IllegalArgumentException("outputVar must start with a letter or underscore and contain only "
					+ "letters, numbers, and underscores.");
		}
		return value;
	}

	private static Map<String, Object> findNode(List<Map<String, Object>> nodes, String nodeId) {
		Map<String, Object> node = findNodeOrNull(nodes, nodeId);
		if (node == null) {
			throw new IllegalArgumentException("Automation definition does not contain parent node: " + nodeId + ".");
		}
		return node;
	}

	private static Map<String, Object> findNodeOrNull(List<Map<String, Object>> nodes, String nodeId) {
		for (Map<String, Object> node : nodes) {
			if (nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				return node;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> requireMap(Object value, String field) {
		if (!(value instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Automation definition field '" + field + "' must be an object.");
		}
		return (Map<String, Object>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> requireMapList(Object value, String field) {
		if (!(value instanceof List<?> list)) {
			throw new IllegalArgumentException("Automation definition field '" + field + "' must be an array.");
		}
		for (Object item : list) {
			if (!(item instanceof Map<?, ?>)) {
				throw new IllegalArgumentException("Automation definition field '" + field + "' must contain objects.");
			}
		}
		return (List<Map<String, Object>>) list;
	}

	private static String pixelStringList(String value) {
		return "[" + AutomationExecutionUtils.GSON.toJson(value) + "]";
	}

	@Override
	public String getReactorDescription() {
		return "Adds one approved managed Python automation action, its source file, and connecting graph edge. "
				+ "Existing source files are never overwritten.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "project" -> "The automation project that owns the new action.";
			case NODE_ID_KEY -> "A safe unique node identifier.";
			case ACTION_ID_KEY -> "A supported business action ID, such as model.llm or python-step.skeleton.";
			case CONFIG_KEY -> "JSON action configuration, optionally base64-encoded. Do not provide operation.";
			case LABEL_KEY -> "Action-oriented label displayed on the workflow.";
			case OUTPUT_VAR_KEY -> "Unique variable name that stores this action's output.";
			case AFTER_NODE_ID_KEY -> "Optional upstream node ID. Defaults to trigger.";
			default -> super.getDescriptionForKey(key);
		};
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		return Map.of(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue());
	}
}
