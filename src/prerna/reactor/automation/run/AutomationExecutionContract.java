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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/**
 * Immutable executor selections captured when an Automation run is submitted.
 */
final class AutomationExecutionContract {

	static final int CURRENT_VERSION = 3;
	static final String IMPLEMENTATION_START_V1 = "java.start.v1";
	static final String IMPLEMENTATION_CONDITION_V1 = "java.condition.v1";
	static final String IMPLEMENTATION_JEV_V1 = "java.jev.v1";
	static final String IMPLEMENTATION_LOOP_V1 = "java.loop.v1";
	static final String IMPLEMENTATION_WAIT_V1 = "java.wait.v1";
	static final String IMPLEMENTATION_APP_PIXEL_V1 = "pixel.app.v1";
	static final String IMPLEMENTATION_FUNCTION_V1 = "pixel.function.v1";
	static final String IMPLEMENTATION_STORAGE_LIST_V1 = "pixel.storage-list.v1";
	static final String IMPLEMENTATION_DATABASE_WRITE_V1 = "pixel.database-write.v1";
	static final String IMPLEMENTATION_VECTOR_WRITE_V1 = "pixel.vector-write.v1";
	static final String IMPLEMENTATION_MODEL_V1 = "pixel.model.v1";
	static final String IMPLEMENTATION_STORAGE_V1 = "pixel.storage.v1";
	static final String IMPLEMENTATION_AGENT_V1 = "pixel.agent.v1";
	static final String IMPLEMENTATION_DATABASE_QUERY_V2 = "java.database-query.direct-table.v2";
	static final String IMPLEMENTATION_VECTOR_SEARCH_V1 = "pixel.vector-search.v1";
	static final String IMPLEMENTATION_STORAGE_DOWNLOAD_V1 = "pixel.storage-download.v1";
	static final String IMPLEMENTATION_DATA_EXTRACT_V1 = "java.data-extract.v1";
	static final String IMPLEMENTATION_PYTHON_TABLE_V2 = "python.explicit-table-boundary.v2";

	private static final String FIELD_VERSION = "version";
	private static final String FIELD_NODES = "nodes";
	private static final String FIELD_KIND = "kind";
	private static final String FIELD_IMPLEMENTATION = "implementation";
	private static final String FIELD_NODE_TYPE = "nodeType";
	private static final String FIELD_CONFIG = "config";
	private static final String FIELD_DEFINITION_HASH = "definitionHash";

	enum ExecutionKind {
		JAVA_CONTROL,
		JAVA_TYPED,
		DIRECT_PIXEL,
		PYTHON_EXPLICIT
	}

	record Selection(ExecutionKind kind, String implementation) {
	}

	private final int version;
	private final String snapshot;
	private final Map<String, Selection> selections;

	private AutomationExecutionContract(int version, String snapshot, Map<String, Selection> selections) {
		this.version = version;
		this.snapshot = snapshot;
		this.selections = Map.copyOf(selections);
	}

	static AutomationExecutionContract forSubmission(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		Map<String, Selection> selections = selectSubmissionImplementations(definition, nodeSources);
		rejectUnsafeDurableLoopSelections(definition, selections);
		Map<String, Object> serializedNodes = new LinkedHashMap<>();
		for (Map.Entry<String, Selection> entry : selections.entrySet()) {
			Map<String, Object> node = nodesById(definition).get(entry.getKey());
			Map<String, Object> descriptor = new LinkedHashMap<>();
			descriptor.put(FIELD_KIND, entry.getValue().kind().name());
			descriptor.put(FIELD_IMPLEMENTATION, entry.getValue().implementation());
			descriptor.put(FIELD_NODE_TYPE, node.get(AutomationConstants.NODE_FIELD_TYPE));
			descriptor.put(FIELD_CONFIG, node.getOrDefault(AutomationConstants.NODE_FIELD_CONFIG, Map.of()));
			descriptor.put(FIELD_DEFINITION_HASH, nodeDefinitionHash(node));
			serializedNodes.put(entry.getKey(), descriptor);
		}
		Map<String, Object> serialized = new LinkedHashMap<>();
		serialized.put(FIELD_VERSION, CURRENT_VERSION);
		serialized.put(FIELD_NODES, serializedNodes);
		String snapshot = AutomationRuntimeUtils.toBoundedRuntimeJson(serialized,
				AutomationConstants.RUN_EXECUTION_CONTRACT_MAX_BYTES, "Automation run execution contract");
		return new AutomationExecutionContract(CURRENT_VERSION, snapshot, selections);
	}

	@SuppressWarnings("unchecked")
	private static void rejectUnsafeDurableLoopSelections(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, Selection> selections) {
		for (Map<String, Object> node : definition.nodes()) {
			if (!AutomationConstants.NODE_CONTROL_LOOP.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
				continue;
			}
			Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if (config == null || !Boolean.TRUE.equals(
					config.getOrDefault(AutomationConstants.CONFIG_LOOP_DURABLE_ITEMS, false))) {
				continue;
			}
			for (Map<String, Object> bodyNode : AutomationRuntime.loopBodyNodes(node)) {
				String bodyNodeId = String.valueOf(bodyNode.get(AutomationConstants.NODE_FIELD_ID));
				Selection selection = selections.get(bodyNodeId);
				if (selection != null && selection.kind() == ExecutionKind.PYTHON_EXPLICIT) {
					throw new IllegalArgumentException("Loop node '"
							+ node.get(AutomationConstants.NODE_FIELD_ID)
							+ "' durableItems cannot execute Python for body node '"
							+ bodyNodeId + "' until an explicit journaled effect contract is available.");
				}
			}
		}
	}

	static Map<String, Object> submissionReport(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		Map<String, String> sources = nodeSources == null ? Map.of() : nodeSources;
		Map<String, Selection> selections = selectSubmissionImplementations(definition, sources);
		List<Map<String, Object>> nodeReports = new java.util.ArrayList<>();
		int directCount = 0;
		int pythonCount = 0;
		for (Map.Entry<String, Map<String, Object>> entry : nodesById(definition).entrySet()) {
			String nodeId = entry.getKey();
			Map<String, Object> node = entry.getValue();
			String source = sources.get(nodeId);
			Selection selection = selections.get(nodeId);
			boolean python = selection.kind() == ExecutionKind.PYTHON_EXPLICIT;
			if (python) {
				pythonCount++;
			} else {
				directCount++;
			}

			Map<String, Object> report = new LinkedHashMap<>();
			report.put("nodeId", nodeId);
			report.put("nodeType", node.get(AutomationConstants.NODE_FIELD_TYPE));
			report.put("codeMode", node.get(AutomationConstants.NODE_FIELD_CODE_MODE));
			report.put("sourcePresent", source != null);
			report.put("sourceHash", source == null ? null : AutomationDefinitionService.calculateSourceHash(source));
			report.put("definitionHash", nodeDefinitionHash(node));
			report.put("executionKind", selection.kind().name());
			report.put("implementation", selection.implementation());
			report.put("conversionRequired", false);
			report.put("reason", python ? "EXPLICIT_DEVELOPER_PYTHON" : "TYPE_OWNED_IMPLEMENTATION");
			nodeReports.add(report);
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("definitionHash", definition.hash());
		result.put("executionContractVersion", CURRENT_VERSION);
		result.put("nodeCount", nodeReports.size());
		result.put("directNodeCount", directCount);
		result.put("pythonNodeCount", pythonCount);
		result.put("wouldModify", false);
		result.put("nodes", nodeReports);
		return result;
	}

	static AutomationExecutionContract forExecution(
			AutomationRunStore.PersistedExecutionContract persisted,
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		Integer persistedVersion = persisted.version();
		String persistedSnapshot = persisted.snapshot();
		if (persistedVersion == null && (persistedSnapshot == null || persistedSnapshot.isBlank())) {
			throw new IllegalStateException("Automation run predates execution contract v3 and cannot be executed.");
		}
		if (persistedVersion == null || persistedSnapshot == null || persistedSnapshot.isBlank()) {
			throw invalid("version and snapshot must either both be present or both be absent");
		}
		if (persistedVersion != CURRENT_VERSION) {
			throw new IllegalStateException(
					"Automation run uses unsupported execution contract version " + persistedVersion + ".");
		}

		try {
			JsonElement parsed = JsonParser.parseString(persistedSnapshot);
			if (!parsed.isJsonObject()) {
				throw invalid("snapshot root must be an object");
			}
			JsonObject root = parsed.getAsJsonObject();
			if (!root.has(FIELD_VERSION) || !root.get(FIELD_VERSION).isJsonPrimitive()
					|| root.get(FIELD_VERSION).getAsInt() != persistedVersion) {
				throw invalid("snapshot version does not match its persisted version");
			}
			if (!root.has(FIELD_NODES) || !root.get(FIELD_NODES).isJsonObject()) {
				throw invalid("snapshot nodes must be an object");
			}

			Map<String, Map<String, Object>> expectedNodes = nodesById(definition);
			JsonObject serializedNodes = root.getAsJsonObject(FIELD_NODES);
			Set<String> persistedNodeIds = new LinkedHashSet<>(serializedNodes.keySet());
			if (!persistedNodeIds.equals(expectedNodes.keySet())) {
				throw invalid("snapshot node identifiers do not match the definition");
			}

			Map<String, Selection> selections = new LinkedHashMap<>();
			for (Map.Entry<String, Map<String, Object>> expected : expectedNodes.entrySet()) {
				JsonElement rawSelection = serializedNodes.get(expected.getKey());
				if (rawSelection == null || !rawSelection.isJsonObject()) {
					throw invalid("node '" + expected.getKey() + "' has no selection object");
				}
				JsonObject selectionObject = rawSelection.getAsJsonObject();
				ExecutionKind kind = executionKind(selectionObject, expected.getKey());
				String implementation = requiredString(selectionObject, FIELD_IMPLEMENTATION, expected.getKey());
				Selection selection = new Selection(kind, implementation);
				if (!isSupportedSelection(expected.getValue(), selection, persistedVersion)) {
					throw new IllegalStateException("Automation run node '" + expected.getKey()
							+ "' uses an unsupported execution implementation: " + implementation);
				}
				validateImmutableDescriptor(expected.getKey(), expected.getValue(), selectionObject);
				selections.put(expected.getKey(), selection);
			}
			rejectUnsafeDurableLoopSelections(definition, selections);
			return new AutomationExecutionContract(persistedVersion, persistedSnapshot, selections);
		} catch (IllegalStateException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Automation run has an invalid execution contract snapshot.", e);
		}
	}

	int version() {
		return version;
	}

	String snapshot() {
		return snapshot;
	}

	Selection selection(String nodeId) {
		Selection selection = selections.get(nodeId);
		if (selection == null) {
			throw new IllegalStateException("Automation run execution contract has no node '" + nodeId + "'.");
		}
		return selection;
	}

	boolean requiresPythonSource(String nodeId) {
		return selection(nodeId).kind() == ExecutionKind.PYTHON_EXPLICIT;
	}

	Map<String, Object> executionDescriptor(String nodeId, Map<String, Object> historicalNode) {
		if (requiresPythonSource(nodeId) && !AutomationConstants.NODE_DEVELOPER_PYTHON.equals(
				historicalNode.get(AutomationConstants.NODE_FIELD_TYPE))) {
			throw new IllegalStateException("Only explicit developer.python nodes may select Python execution.");
		}
		return historicalNode;
	}

	private static Map<String, Selection> selectSubmissionImplementations(
			AutomationDefinitionValidator.ValidatedDefinition definition, Map<String, String> nodeSources) {
		Map<String, String> sources = nodeSources == null ? Map.of() : nodeSources;
		Map<String, Selection> selections = new LinkedHashMap<>();
		for (Map.Entry<String, Map<String, Object>> entry : nodesById(definition).entrySet()) {
			Map<String, Object> node = entry.getValue();
			String nodeType = String.valueOf(node.get(AutomationConstants.NODE_FIELD_TYPE));
			boolean developerPython = AutomationConstants.NODE_DEVELOPER_PYTHON.equals(nodeType);
			if (developerPython && !sources.containsKey(entry.getKey())) {
				throw new IllegalArgumentException(
						"Explicit Python node '" + entry.getKey() + "' has no persisted source.");
			}
			if (!developerPython && sources.containsKey(entry.getKey())) {
				throw new IllegalArgumentException("Typed node '" + entry.getKey()
						+ "' cannot select Python source. Convert it to developer.python first.");
			}
			if (AutomationConstants.NODE_START.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_START_V1));
			} else if (AutomationConstants.NODE_CONTROL_IF.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_CONDITION_V1));
			} else if (AutomationConstants.NODE_CONTROL_JEV.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_JEV_V1));
			} else if (AutomationConstants.NODE_CONTROL_LOOP.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_LOOP_V1));
			} else if (AutomationConstants.NODE_CONTROL_WAIT.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_WAIT_V1));
			} else if (AutomationConstants.NODE_APP_PIXEL.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_APP_PIXEL_V1));
			} else if (AutomationConstants.NODE_FUNCTION_EXECUTE.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_FUNCTION_V1));
			} else if (AutomationConstants.NODE_STORAGE_LIST.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_STORAGE_LIST_V1));
			} else if (isDatabaseWrite(node)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_DATABASE_WRITE_V1));
			} else if (isVectorWrite(node)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_VECTOR_WRITE_V1));
			} else if (isModel(node)) {
				selections.put(entry.getKey(), new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_MODEL_V1));
			} else if (isSimpleStorage(node)) {
				selections.put(entry.getKey(), new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_STORAGE_V1));
			} else if (AutomationConstants.NODE_AGENT_RUN.equals(nodeType)) {
				selections.put(entry.getKey(), new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_AGENT_V1));
			} else if (AutomationConstants.NODE_DATABASE_QUERY.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_TYPED, IMPLEMENTATION_DATABASE_QUERY_V2));
			} else if (AutomationConstants.NODE_VECTOR_SEARCH.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_VECTOR_SEARCH_V1));
			} else if (AutomationConstants.NODE_STORAGE_DOWNLOAD.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_STORAGE_DOWNLOAD_V1));
			} else if (AutomationConstants.NODE_DATA_EXTRACT.equals(nodeType)) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.JAVA_TYPED, IMPLEMENTATION_DATA_EXTRACT_V1));
			} else if (developerPython) {
				selections.put(entry.getKey(),
						new Selection(ExecutionKind.PYTHON_EXPLICIT, IMPLEMENTATION_PYTHON_TABLE_V2));
			} else {
				throw new IllegalArgumentException(
						"Typed Automation node '" + entry.getKey() + "' has no type-owned implementation.");
			}
		}
		return selections;
	}

	private static Map<String, Map<String, Object>> nodesById(
			AutomationDefinitionValidator.ValidatedDefinition definition) {
		List<Map<String, Object>> nodes = AutomationRuntime.allNodes(definition);
		Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
		for (Map<String, Object> node : nodes) {
			String nodeId = String.valueOf(node.get(AutomationConstants.NODE_FIELD_ID));
			if (nodesById.putIfAbsent(nodeId, node) != null) {
				throw new IllegalArgumentException("Automation definition contains duplicate node identifier: " + nodeId);
			}
		}
		return nodesById;
	}

	private static boolean isSupportedSelection(Map<String, Object> node, Selection selection, int contractVersion) {
		if (AutomationConstants.NODE_DEVELOPER_PYTHON.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
				&& selection.equals(new Selection(ExecutionKind.PYTHON_EXPLICIT,
						IMPLEMENTATION_PYTHON_TABLE_V2))) {
			return true;
		}
		return (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
				&& selection.equals(new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_START_V1)))
				|| (AutomationConstants.NODE_CONTROL_IF.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_CONDITION_V1)))
				|| (AutomationConstants.NODE_CONTROL_JEV.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_JEV_V1)))
				|| (AutomationConstants.NODE_CONTROL_LOOP.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_LOOP_V1)))
				|| (AutomationConstants.NODE_CONTROL_WAIT.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
				&& selection.equals(new Selection(ExecutionKind.JAVA_CONTROL, IMPLEMENTATION_WAIT_V1)))
				|| (AutomationConstants.NODE_APP_PIXEL.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_APP_PIXEL_V1)))
				|| (AutomationConstants.NODE_FUNCTION_EXECUTE.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_FUNCTION_V1)))
				|| (AutomationConstants.NODE_STORAGE_LIST.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(
								new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_STORAGE_LIST_V1)))
				|| (isDatabaseWrite(node) && selection.equals(
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_DATABASE_WRITE_V1)))
				|| (isVectorWrite(node) && selection.equals(
						new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_VECTOR_WRITE_V1)))
				|| (isModel(node)
						&& selection.equals(new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_MODEL_V1)))
				|| (isSimpleStorage(node)
						&& selection.equals(new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_STORAGE_V1)))
				|| (AutomationConstants.NODE_AGENT_RUN.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_AGENT_V1)))
				|| (AutomationConstants.NODE_DATABASE_QUERY.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.JAVA_TYPED,
								IMPLEMENTATION_DATABASE_QUERY_V2)))
				|| (AutomationConstants.NODE_VECTOR_SEARCH.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_VECTOR_SEARCH_V1)))
				|| (AutomationConstants.NODE_STORAGE_DOWNLOAD.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(
								new Selection(ExecutionKind.DIRECT_PIXEL, IMPLEMENTATION_STORAGE_DOWNLOAD_V1)))
				|| (AutomationConstants.NODE_DATA_EXTRACT.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))
						&& selection.equals(new Selection(ExecutionKind.JAVA_TYPED, IMPLEMENTATION_DATA_EXTRACT_V1)));
	}

	private static void validateImmutableDescriptor(String nodeId, Map<String, Object> node,
			JsonObject selectionObject) {
		String nodeType = requiredString(selectionObject, FIELD_NODE_TYPE, nodeId);
		if (!nodeType.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
			throw invalid("node '" + nodeId + "' type does not match its immutable descriptor");
		}
		if (!selectionObject.has(FIELD_CONFIG) || !selectionObject.get(FIELD_CONFIG).isJsonObject()) {
			throw invalid("node '" + nodeId + "' config is missing from its immutable descriptor");
		}
		JsonElement descriptorConfig = selectionObject.get(FIELD_CONFIG);
		String definitionHash = requiredString(selectionObject, FIELD_DEFINITION_HASH, nodeId);
		if (!definitionHash.equals(nodeDefinitionHash(nodeType, descriptorConfig))) {
			throw invalid("node '" + nodeId + "' definition hash does not match its immutable descriptor");
		}
		if (!descriptorConfig.equals(AutomationRuntimeUtils.GSON.toJsonTree(
				node.getOrDefault(AutomationConstants.NODE_FIELD_CONFIG, Map.of())))) {
			throw invalid("node '" + nodeId + "' config does not match its immutable descriptor");
		}
	}

	static String nodeDefinitionHash(Map<String, Object> node) {
		return nodeDefinitionHash(String.valueOf(node.get(AutomationConstants.NODE_FIELD_TYPE)),
				AutomationRuntimeUtils.GSON.toJsonTree(
						node.getOrDefault(AutomationConstants.NODE_FIELD_CONFIG, Map.of())));
	}

	private static String nodeDefinitionHash(String nodeType, JsonElement config) {
		JsonObject definition = new JsonObject();
		definition.addProperty(FIELD_NODE_TYPE, nodeType);
		definition.add(FIELD_CONFIG, config);
		return AutomationDefinitionService.calculateSourceHash(
				AutomationRuntimeUtils.GSON.toJson(definition));
	}

	private static boolean isDatabaseWrite(Map<String, Object> node) {
		Object type = node.get(AutomationConstants.NODE_FIELD_TYPE);
		return AutomationConstants.NODE_DATABASE_INSERT.equals(type)
				|| AutomationConstants.NODE_DATABASE_UPDATE.equals(type)
				|| AutomationConstants.NODE_DATABASE_DELETE.equals(type);
	}

	private static boolean isVectorWrite(Map<String, Object> node) {
		Object type = node.get(AutomationConstants.NODE_FIELD_TYPE);
		return AutomationConstants.NODE_VECTOR_ADD.equals(type)
				|| AutomationConstants.NODE_VECTOR_DELETE.equals(type);
	}

	private static boolean isModel(Map<String, Object> node) {
		Object type = node.get(AutomationConstants.NODE_FIELD_TYPE);
		return AutomationConstants.NODE_MODEL_CHAT.equals(type)
				|| AutomationConstants.NODE_MODEL_EMBEDDINGS.equals(type)
				|| AutomationConstants.NODE_MODEL_VISION.equals(type)
				|| AutomationConstants.NODE_MODEL_NER.equals(type);
	}

	private static boolean isSimpleStorage(Map<String, Object> node) {
		Object type = node.get(AutomationConstants.NODE_FIELD_TYPE);
		return AutomationConstants.NODE_STORAGE_READ.equals(type)
				|| AutomationConstants.NODE_STORAGE_UPLOAD.equals(type)
				|| AutomationConstants.NODE_STORAGE_DELETE.equals(type);
	}

	@SuppressWarnings("unchecked")
	private static boolean supportsDirectAppPixel(Map<String, Object> node) {
		if (!AutomationConstants.NODE_APP_PIXEL.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
			return false;
		}
		Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
		Object pixel = config.get(AutomationConstants.CONFIG_PIXEL);
		return pixel instanceof String expression && !expression.isBlank() && expression.trim().endsWith(";");
	}

	private static ExecutionKind executionKind(JsonObject selection, String nodeId) {
		String value = requiredString(selection, FIELD_KIND, nodeId);
		try {
			return ExecutionKind.valueOf(value);
		} catch (IllegalArgumentException e) {
			throw invalid("node '" + nodeId + "' has unknown execution kind '" + value + "'");
		}
	}

	private static String requiredString(JsonObject object, String field, String nodeId) {
		JsonElement value = object.get(field);
		if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()
				|| value.getAsString().isBlank()) {
			throw invalid("node '" + nodeId + "' has invalid '" + field + "'");
		}
		return value.getAsString();
	}

	private static IllegalStateException invalid(String detail) {
		return new IllegalStateException("Automation run has an invalid execution contract: " + detail + ".");
	}
}
