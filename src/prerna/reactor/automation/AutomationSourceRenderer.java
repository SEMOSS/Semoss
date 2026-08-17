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
 *******************************************************************************/
package prerna.reactor.automation;

import java.util.Map;

import prerna.reactor.automation.utils.AutomationRuntimeUtils;

/** Renders the default implementation for exactly one automation node. */
public final class AutomationSourceRenderer {

	private static final String LEGACY_DEFAULT_SOURCE = """
			# Generated SEMOSS Automation node implementation.
			# Java binds this script to one immutable run-snapshot node.
			def run(scope):
			    return automation.run_current_node(scope)
			""";

	private AutomationSourceRenderer() {
	}

	/**
	 * Produces source that can execute only the node bound by Java in its invocation wrapper.
	 * The source receives a scope and returns the bridge response for that one node.
	 *
	 * @param node canonical non-start node
	 * @return executable Python source with a {@code run(scope)} entry point
	 */
	public static String renderNode(Map<String, Object> node) {
		if (AutomationConstants.NODE_START.equals(node.get(AutomationConstants.NODE_FIELD_TYPE))) {
			throw new IllegalArgumentException("trigger.start is executed natively and has no Python source.");
		}
		String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);
		@SuppressWarnings("unchecked")
		Map<String, Object> config = node.get(AutomationConstants.NODE_FIELD_CONFIG) instanceof Map<?, ?> map
				? (Map<String, Object>) map : Map.of();
		return switch (type) {
			case AutomationConstants.NODE_DATABASE_QUERY -> databaseQuerySource(config);
			case AutomationConstants.NODE_DATABASE_INSERT -> databaseWriteSource(config, "insertData");
			case AutomationConstants.NODE_DATABASE_UPDATE -> databaseWriteSource(config, "updateData");
			case AutomationConstants.NODE_MODEL_CHAT -> modelChatSource(config);
			case AutomationConstants.NODE_MODEL_EMBEDDINGS -> modelEmbeddingsSource(config);
			case AutomationConstants.NODE_MODEL_VISION -> modelVisionSource(config);
			case AutomationConstants.NODE_MODEL_NER -> modelNerSource(config);
			case AutomationConstants.NODE_STORAGE_LIST -> storageSource(config, "list", "STORAGE_PATH");
			case AutomationConstants.NODE_STORAGE_READ -> storageSource(config, "readBlobToMemory", "STORAGE_PATH");
			case AutomationConstants.NODE_STORAGE_UPLOAD -> storageTransferSource(config, "copyToStorage");
			case AutomationConstants.NODE_STORAGE_DOWNLOAD -> storageTransferSource(config, "copyToLocal");
			case AutomationConstants.NODE_STORAGE_DELETE -> storageSource(config, "deleteFromStorage", "STORAGE_PATH");
			case AutomationConstants.NODE_STORAGE_ACTION -> storageActionSource(config);
			case AutomationConstants.NODE_VECTOR_SEARCH -> vectorSearchSource(config);
			case AutomationConstants.NODE_VECTOR_ADD -> vectorAddSource(config);
			case AutomationConstants.NODE_VECTOR_DELETE -> vectorDeleteSource(config);
			case AutomationConstants.NODE_VECTOR_ACTION -> vectorActionSource(config);
			case AutomationConstants.NODE_FUNCTION_EXECUTE -> functionSource(config);
			case AutomationConstants.NODE_APP_PIXEL -> appPixelSource(config);
			case AutomationConstants.NODE_CONTROL_WAIT -> waitSource(config);
			case AutomationConstants.NODE_DEVELOPER_PYTHON -> developerSource();
			default -> developerSource();
		};
	}

	private static String databaseQuerySource(Map<String, Object> config) {
		return """
				# Query a SEMOSS database directly through the Python SDK.
				from ai_server import DatabaseEngine

				ENGINE_ID = %s
				QUERY = %s

				def run(scope):
				    database = DatabaseEngine(engine_id=ENGINE_ID)
				    return database.execQuery(query=QUERY, return_pandas=False)
				""".formatted(value(config, "engineId"), value(config, "query"));
	}

	private static String databaseWriteSource(Map<String, Object> config, String method) {
		return """
				# Write to a SEMOSS database directly through the Python SDK.
				from ai_server import DatabaseEngine

				ENGINE_ID = %s
				QUERY = %s

				def run(scope):
				    database = DatabaseEngine(engine_id=ENGINE_ID)
				    return database.%s(query=QUERY)
				""".formatted(value(config, "engineId"), value(config, "query"), method);
	}

	private static String modelChatSource(Map<String, Object> config) {
		return """
				# Call a SEMOSS model directly through the Python SDK.
				from ai_server import ModelEngine

				ENGINE_ID = %s
				PROMPT = %s
				SYSTEM_PROMPT = %s
				PARAMETERS = %s

				def run(scope):
				    model = ModelEngine(engine_id=ENGINE_ID)
				    return model.ask(command=PROMPT, context=SYSTEM_PROMPT, param_dict=PARAMETERS)
				""".formatted(value(config, "engineId"), value(config, "prompt"),
				value(config, "systemPrompt"), value(config, "paramValues"));
	}

	private static String modelEmbeddingsSource(Map<String, Object> config) {
		return """
				# Create embeddings directly through the SEMOSS Python SDK.
				from ai_server import ModelEngine

				ENGINE_ID = %s
				VALUES = %s

				def run(scope):
				    model = ModelEngine(engine_id=ENGINE_ID)
				    return model.embeddings(strings_to_embed=VALUES)
				""".formatted(value(config, "engineId"), value(config, "text"));
	}

	private static String modelVisionSource(Map<String, Object> config) {
		return """
				# Send an image prompt directly through the SEMOSS Python SDK.
				from ai_server import ModelEngine

				ENGINE_ID = %s
				PROMPT = %s
				IMAGE = %s

				def run(scope):
				    model = ModelEngine(engine_id=ENGINE_ID)
				    return model.ask(command=PROMPT, image=[IMAGE])
				""".formatted(value(config, "engineId"), value(config, "prompt"), value(config, "image"));
	}

	private static String modelNerSource(Map<String, Object> config) {
		return """
				# Run named-entity recognition directly through the SEMOSS Python SDK.
				from ai_server import ModelEngine

				ENGINE_ID = %s
				TEXT = %s
				ENTITIES = %s

				def run(scope):
				    model = ModelEngine(engine_id=ENGINE_ID)
				    return model.ner(text=TEXT, entities=ENTITIES)
				""".formatted(value(config, "engineId"), value(config, "text"), value(config, "entities"));
	}

	private static String storageSource(Map<String, Object> config, String method, String argument) {
		return """
				# Access SEMOSS storage directly through the Python SDK.
				from ai_server import StorageEngine

				ENGINE_ID = %s
				STORAGE_PATH = %s

				def run(scope):
				    storage = StorageEngine(engine_id=ENGINE_ID)
				    return storage.%s(%s)
				""".formatted(value(config, "engineId"), value(config, "path"), method, argument);
	}

	private static String storageTransferSource(Map<String, Object> config, String method) {
		return """
				# Transfer files with SEMOSS storage through the Python SDK.
				from ai_server import StorageEngine

				ENGINE_ID = %s
				STORAGE_PATH = %s
				FILE_PATH = %s

				def run(scope):
				    storage = StorageEngine(engine_id=ENGINE_ID)
				    return storage.%s(storagePath=STORAGE_PATH, localPath=FILE_PATH)
				""".formatted(value(config, "engineId"), value(config, "path"), value(config, "destination"), method);
	}

	private static String vectorSearchSource(Map<String, Object> config) {
		return """
				# Search a SEMOSS vector engine directly through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				QUERY = %s
				LIMIT = %s

				def run(scope):
				    vector = VectorEngine(engine_id=ENGINE_ID)
				    return vector.nearestNeighbor(search_statement=QUERY, limit=LIMIT)
				""".formatted(value(config, "engineId"), value(config, "value"), value(config, "limit"));
	}

	private static String vectorAddSource(Map<String, Object> config) {
		return """
				# Add documents to a SEMOSS vector engine through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				FILE_PATHS = %s.split(",")

				def run(scope):
				    vector = VectorEngine(engine_id=ENGINE_ID)
				    return vector.addDocument(file_paths=FILE_PATHS)
				""".formatted(value(config, "engineId"), value(config, "value"));
	}

	private static String vectorDeleteSource(Map<String, Object> config) {
		return """
				# Remove documents from a SEMOSS vector engine through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				FILE_NAMES = %s.split(",")

				def run(scope):
				    vector = VectorEngine(engine_id=ENGINE_ID)
				    return vector.removeDocument(file_names=FILE_NAMES)
				""".formatted(value(config, "engineId"), value(config, "value"));
	}

	private static String storageActionSource(Map<String, Object> config) {
		return """
				# Access SEMOSS storage directly through the Python SDK.
				from ai_server import StorageEngine

				ENGINE_ID = %s
				OPERATION = %s
				STORAGE_PATH = %s
				FILE_PATH = %s

				def run(scope):
				    storage = StorageEngine(engine_id=ENGINE_ID)
				    if OPERATION == "list":
				        return storage.list(STORAGE_PATH)
				    if OPERATION == "read-base64":
				        return storage.readBlobToMemory(STORAGE_PATH)
				    if OPERATION == "upload":
				        return storage.copyToStorage(storagePath=STORAGE_PATH, localPath=FILE_PATH)
				    if OPERATION == "download":
				        return storage.copyToLocal(storagePath=STORAGE_PATH, localPath=FILE_PATH)
				    if OPERATION == "delete":
				        return storage.deleteFromStorage(STORAGE_PATH)
				    raise ValueError(f"Unsupported storage operation: {OPERATION}")
				""".formatted(value(config, "engineId"), value(config, "operation"),
				value(config, "path"), value(config, "destination"));
	}

	private static String vectorActionSource(Map<String, Object> config) {
		return """
				# Access a SEMOSS vector engine directly through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				OPERATION = %s
				VALUE = %s
				LIMIT = %s

				def run(scope):
				    vector = VectorEngine(engine_id=ENGINE_ID)
				    if OPERATION == "search":
				        return vector.nearestNeighbor(search_statement=VALUE, limit=LIMIT)
				    if OPERATION in ("add", "add-file"):
				        return vector.addDocument(file_paths=VALUE.split(","))
				    if OPERATION == "delete":
				        return vector.removeDocument(file_names=VALUE.split(","))
				    if OPERATION == "list":
				        return vector.listDocuments()
				    raise ValueError(f"Unsupported vector operation: {OPERATION}")
				""".formatted(value(config, "engineId"), value(config, "operation"),
				value(config, "value"), value(config, "limit"));
	}

	private static String functionSource(Map<String, Object> config) {
		return """
				# Execute a SEMOSS function engine directly through the Python SDK.
				from ai_server import FunctionEngine
				import json

				ENGINE_ID = %s
				ARGUMENTS = %s

				def run(scope):
				    function = FunctionEngine(engine_id=ENGINE_ID)
				    return function.execute(parameterMap=json.loads(ARGUMENTS))
				""".formatted(value(config, "engineId"), value(config, "arguments"));
	}

	private static String appPixelSource(Map<String, Object> config) {
		return """
				# Run the application Pixel directly in this authenticated Python insight.
				from semoss import Insight

				PIXEL = %s

				def run(scope):
				    return Insight().run_pixel(PIXEL)
				""".formatted(value(config, "pixel"));
	}

	private static String waitSource(Map<String, Object> config) {
		return """
				# Pause this automation node in Python.
				import time

				SECONDS = %s

				def run(scope):
				    time.sleep(SECONDS)
				    return {"waitedSeconds": SECONDS}
				""".formatted(value(config, "durationSeconds"));
	}

	private static String developerSource() {
		return """
				# Write arbitrary Python for this automation node here.
				# Return a JSON-shaped value to persist it as this node's output.
				def run(scope):
				    return {}
				""";
	}

	private static String value(Map<String, Object> config, String key) {
		return AutomationRuntimeUtils.GSON.toJson(config.get(key));
	}

	static boolean isLegacyDefaultSource(String source) {
		return LEGACY_DEFAULT_SOURCE.equals(source)
				|| source.startsWith("# Generated SEMOSS Automation ")
				&& source.contains("NODE_CONFIG =")
				&& source.contains("automation.run_current_node(scope, NODE_CONFIG)");
	}
}
