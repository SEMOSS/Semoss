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
package prerna.reactor.automation.run;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.project.AutomationProjectService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the immutable implementation snapshot executed for one Automation
 * run node.
 *
 * <p>
 * Pixel:
 * {@code GetAutomationRunSource(project=["appId"], runId=["uuid"], nodeId=["nodeId"])}.
 * The response contains persisted Python only for explicit
 * {@code developer.python} nodes. Typed nodes return their immutable config and
 * Java implementation identity instead of generated source.
 */
public class GetAutomationRunSourceReactor extends AbstractReactor {

	/** Pixel input key for the durable Automation run identifier. */
	private static final String RUN_ID_KEY = "runId";
	/** Pixel input key for the canonical node identifier in the executed graph. */
	private static final String NODE_ID_KEY = "nodeId";

	/**
	 * Configures the project, run, and node inputs accepted by this reactor.
	 *
	 * <p>
	 * Project is optional only for user-scoped inline runs. Run and node identifiers
	 * are always required.
	 */
	public GetAutomationRunSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), RUN_ID_KEY, NODE_ID_KEY };
		this.keyRequired = new int[] { 0, 1, 1 };
	}

	/**
	 * Resolves the caller's authorized run scope and returns the requested node's
	 * immutable implementation view.
	 *
	 * @return map containing either a Python source snapshot or a typed Java
	 *         implementation descriptor
	 * @throws IllegalArgumentException when an identifier is missing, inaccessible,
	 *                                  or absent from the executed graph
	 * @throws IllegalStateException    when the persisted run snapshot is missing or
	 *                                  invalid
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String runId = requireNonblank(this.keyValue.get(RUN_ID_KEY), "run id");
		String nodeId = requireNonblank(this.keyValue.get(NODE_ID_KEY), "node id");

		projectId = AutomationProjectService.resolveRunScope(this.insight.getUser(), projectId, false);
		Map<String, Object> runDetail = AutomationRunStore.getRunDetail(runId);
		if (runDetail == null || !projectId.equals(runDetail.get(AutomationConstants.PROJECT_ID))) {
			throw new IllegalArgumentException("Automation run does not exist or user does not have access.");
		}

		String definitionSnapshot = stringValue(runDetail.get(AutomationConstants.DEFINITION_SNAPSHOT));
		String definitionHash = stringValue(runDetail.get(AutomationConstants.DEFINITION_HASH));
		if (definitionSnapshot == null || definitionHash == null) {
			throw new IllegalStateException("Automation run does not contain an implementation snapshot.");
		}

		Map<String, Object> response = buildSourceView(runId, nodeId, definitionSnapshot, definitionHash,
				AutomationRunStore.getRunNodeSources(runId));
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/**
	 * Builds the client view from immutable run artifacts.
	 *
	 * @param runId              run identifier returned to the client
	 * @param nodeId             canonical graph node identifier
	 * @param definitionSnapshot canonical graph JSON captured at submission
	 * @param definitionHash     hash captured with the graph snapshot
	 * @param nodeSources        hash-verified Python sources captured for the run
	 * @return source or typed Java implementation descriptor
	 * @throws IllegalArgumentException when the requested node is absent
	 * @throws IllegalStateException    when the definition or requested node snapshot
	 *                                  is invalid
	 */
	static Map<String, Object> buildSourceView(String runId, String nodeId, String definitionSnapshot,
			String definitionHash, Map<String, String> nodeSources) {
		Map<String, Object> definition;
		try {
			definition = AutomationRuntimeUtils.GSON.fromJson(definitionSnapshot, AutomationRuntimeUtils.MAP_TYPE);
		} catch (RuntimeException e) {
			throw new IllegalStateException("Automation run contains an invalid definition snapshot.", e);
		}

		Map<String, Object> node = findNode(definition, nodeId);
		if (node == null) {
			throw new IllegalArgumentException("Automation run does not contain node '" + nodeId + "'.");
		}
		String nodeType = stringValue(node.get(AutomationConstants.NODE_FIELD_TYPE));
		if (nodeType == null) {
			throw new IllegalStateException("Automation run node '" + nodeId + "' has no type.");
		}

		Map<String, Object> response = new LinkedHashMap<>();
		response.put(AutomationConstants.RUN_ID, runId);
		response.put(AutomationConstants.NODE_ID, nodeId);
		String source = nodeSources == null ? null : nodeSources.get(nodeId);
		if (source != null) {
			boolean compatibility = !AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType);
			response.put("viewKind", "PYTHON");
			response.put("legacyPython", compatibility);
			response.put("nodeType", AutomationConstants.NODE_DEVELOPER_PYTHON);
			response.put("sourceIdentity", compatibility ? "developer.python.compatibility" : "developer.python");
			response.put("sourceNodeId", nodeId);
			if (compatibility) {
				response.put("historicalNodeType", nodeType);
			}
			response.put(AutomationConstants.SOURCE_CODE, source);
			return response;
		}

		if (AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType)) {
			throw new IllegalStateException("Automation run has no Python source snapshot for node '" + nodeId + "'.");
		}
		response.put("viewKind", "IMPLEMENTATION");
		response.put("legacyPython", false);
		response.put("nodeType", nodeType);
		response.put("config", copyConfig(node));
		response.put("implementationId", "java." + nodeType.replace('.', '-') + ".v1");
		response.put("definitionHash", definitionHash);
		return response;
	}

	/**
	 * Finds a canonical node in the top-level graph or any nested loop body.
	 *
	 * @param definition parsed immutable definition snapshot
	 * @param nodeId     canonical node identifier
	 * @return matching node, or {@code null} when no node matches
	 * @throws IllegalStateException when the definition does not contain a valid
	 *                               graph node collection
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> findNode(Map<String, Object> definition, String nodeId) {
		Object graphValue = definition == null ? null : definition.get(AutomationConstants.DOC_GRAPH);
		if (!(graphValue instanceof Map<?, ?> graph)
				|| !(graph.get(AutomationConstants.DOC_NODES) instanceof List<?> nodes)) {
			throw new IllegalStateException("Automation run contains an invalid graph snapshot.");
		}
		for (Object value : nodes) {
			if (!(value instanceof Map<?, ?>)) {
				continue;
			}
			Map<String, Object> node = (Map<String, Object>) value;
			if (nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				return node;
			}
			Map<String, Object> nested = findNode(AutomationRuntime.loopBodyNodes(node), nodeId);
			if (nested != null) {
				return nested;
			}
		}
		return null;
	}

	/**
	 * Recursively searches loop-body nodes without treating a runtime iteration ID
	 * as a graph node identifier.
	 *
	 * @param nodes  canonical nodes in the current graph level
	 * @param nodeId canonical node identifier
	 * @return matching node, or {@code null} when no node matches
	 */
	private static Map<String, Object> findNode(List<Map<String, Object>> nodes, String nodeId) {
		for (Map<String, Object> node : nodes) {
			if (nodeId.equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				return node;
			}
			Map<String, Object> nested = findNode(AutomationRuntime.loopBodyNodes(node), nodeId);
			if (nested != null) {
				return nested;
			}
		}
		return null;
	}

	/**
	 * Copies the persisted node config so the response cannot mutate the parsed run
	 * snapshot.
	 *
	 * @param node canonical node from the immutable run definition
	 * @return mutable copy of the config, or an empty map when no config was stored
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> copyConfig(Map<String, Object> node) {
		Object config = node.get(AutomationConstants.NODE_FIELD_CONFIG);
		return config instanceof Map<?, ?> map ? new LinkedHashMap<>((Map<String, Object>) map) : new LinkedHashMap<>();
	}

	/**
	 * Requires a nonblank Pixel input.
	 *
	 * @param value input value
	 * @param label human-readable input name used in the error
	 * @return the validated value
	 * @throws IllegalArgumentException when the value is absent or blank
	 */
	private static String requireNonblank(String value, String label) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Must provide a " + label);
		}
		return value;
	}

	/**
	 * Converts a persisted scalar to a nonblank string.
	 *
	 * @param value persisted value
	 * @return string value, or {@code null} when absent or blank
	 */
	private static String stringValue(Object value) {
		return value == null || value.toString().isBlank() ? null : value.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getReactorDescription() {
		return "Returns the immutable implementation snapshot executed for one Automation run node.";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (RUN_ID_KEY.equals(key)) {
			return "Run identifier returned when the automation was triggered.";
		}
		if (NODE_ID_KEY.equals(key)) {
			return "Canonical node identifier from the executed automation graph.";
		}
		return super.getDescriptionForKey(key);
	}
}
