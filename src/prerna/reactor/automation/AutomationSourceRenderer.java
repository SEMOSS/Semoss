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

	private static final String RESOLVE_HELPER = """
			import re

			def resolve(value, scope):
			    if not isinstance(value, str):
			        return value

			    def replace(match):
			        key = match.group(1)
			        if key.startswith("config."):
			            config = scope.get("_automation_config", {})
			            return config.get(key[7:], match.group(0))
			        return scope.get(key, match.group(0))

			    return re.sub(r"\\$\\{([^}]+)\\}", replace, value)

			""";

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
		String source = switch (type) {
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
		return RESOLVE_HELPER + source;
	}

	private static String databaseQuerySource(Map<String, Object> config) {
		return """
				# Query a SEMOSS database directly through the Python SDK.
				from ai_server import DatabaseEngine

				ENGINE_ID = %s
				QUERY = %s

				def run(scope):
				    database = DatabaseEngine(engine_id=resolve(ENGINE_ID, scope))
				    return database.execQuery(query=resolve(QUERY, scope), return_pandas=False)
				""".formatted(value(config, "engineId"), value(config, "query"));
	}

	private static String databaseWriteSource(Map<String, Object> config, String method) {
		return """
				# Write to a SEMOSS database directly through the Python SDK.
				from ai_server import DatabaseEngine

				ENGINE_ID = %s
				QUERY = %s

				def run(scope):
				    database = DatabaseEngine(engine_id=resolve(ENGINE_ID, scope))
				    return database.%s(query=resolve(QUERY, scope))
				""".formatted(value(config, "engineId"), value(config, "query"), method);
	}

	private static String modelChatSource(Map<String, Object> config) {
		return """
				# Call a SEMOSS model directly through the Python SDK.
				from ai_server import ModelEngine
				import json

				ENGINE_ID = %s
				PROMPT = %s
				SYSTEM_PROMPT = %s
				PARAMETERS_JSON = %s

				def run(scope):
				    model = ModelEngine(engine_id=resolve(ENGINE_ID, scope))
				    parameters = json.loads(resolve(PARAMETERS_JSON, scope) or "{}")
				    return model.ask(command=resolve(PROMPT, scope), context=resolve(SYSTEM_PROMPT, scope), param_dict=parameters)
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
				    model = ModelEngine(engine_id=resolve(ENGINE_ID, scope))
				    return model.embeddings(strings_to_embed=resolve(VALUES, scope))
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
				    model = ModelEngine(engine_id=resolve(ENGINE_ID, scope))
				    return model.ask(command=resolve(PROMPT, scope), image=[resolve(IMAGE, scope)])
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
				    model = ModelEngine(engine_id=resolve(ENGINE_ID, scope))
				    return model.ner(text=resolve(TEXT, scope), entities=resolve(ENTITIES, scope))
				""".formatted(value(config, "engineId"), value(config, "text"), value(config, "entities"));
	}

	private static String storageSource(Map<String, Object> config, String method, String argument) {
		return """
				# Access SEMOSS storage directly through the Python SDK.
				from ai_server import StorageEngine

				ENGINE_ID = %s
				STORAGE_PATH = %s

				def run(scope):
				    storage = StorageEngine(engine_id=resolve(ENGINE_ID, scope))
				    return storage.%s(resolve(%s, scope))
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
				    storage = StorageEngine(engine_id=resolve(ENGINE_ID, scope))
				    return storage.%s(storagePath=resolve(STORAGE_PATH, scope), localPath=resolve(FILE_PATH, scope))
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
				    vector = VectorEngine(engine_id=resolve(ENGINE_ID, scope))
				    return vector.nearestNeighbor(search_statement=resolve(QUERY, scope), limit=resolve(LIMIT, scope))
				""".formatted(value(config, "engineId"), value(config, "value"), value(config, "limit"));
	}

	private static String vectorAddSource(Map<String, Object> config) {
		return """
				# Add documents to a SEMOSS vector engine through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				FILE_PATHS = %s

				def run(scope):
				    vector = VectorEngine(engine_id=resolve(ENGINE_ID, scope))
				    return vector.addDocument(file_paths=resolve(FILE_PATHS, scope).split(","))
				""".formatted(value(config, "engineId"), value(config, "value"));
	}

	private static String vectorDeleteSource(Map<String, Object> config) {
		return """
				# Remove documents from a SEMOSS vector engine through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				FILE_NAMES = %s

				def run(scope):
				    vector = VectorEngine(engine_id=resolve(ENGINE_ID, scope))
				    return vector.removeDocument(file_names=resolve(FILE_NAMES, scope).split(","))
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
				    storage = StorageEngine(engine_id=resolve(ENGINE_ID, scope))
				    operation = resolve(OPERATION, scope)
				    storage_path = resolve(STORAGE_PATH, scope)
				    file_path = resolve(FILE_PATH, scope)
				    if operation == "list":
				        return storage.list(storage_path)
				    if operation == "read-base64":
				        return storage.readBlobToMemory(storage_path)
				    if operation == "upload":
				        return storage.copyToStorage(storagePath=storage_path, localPath=file_path)
				    if operation == "download":
				        return storage.copyToLocal(storagePath=storage_path, localPath=file_path)
				    if operation == "delete":
				        return storage.deleteFromStorage(storage_path)
				    raise ValueError(f"Unsupported storage operation: {operation}")
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
				    vector = VectorEngine(engine_id=resolve(ENGINE_ID, scope))
				    operation = resolve(OPERATION, scope)
				    value = resolve(VALUE, scope)
				    if operation == "search":
				        return vector.nearestNeighbor(search_statement=value, limit=resolve(LIMIT, scope))
				    if operation in ("add", "add-file"):
				        return vector.addDocument(file_paths=value.split(","))
				    if operation == "delete":
				        return vector.removeDocument(file_names=value.split(","))
				    if operation == "list":
				        return vector.listDocuments()
				    raise ValueError(f"Unsupported vector operation: {operation}")
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
				    function = FunctionEngine(engine_id=resolve(ENGINE_ID, scope))
				    return function.execute(parameterMap=json.loads(resolve(ARGUMENTS, scope)))
				""".formatted(value(config, "engineId"), value(config, "arguments"));
	}

	private static String appPixelSource(Map<String, Object> config) {
		return """
				# Run the application Pixel directly in this authenticated Python insight.
				from semoss import Insight

				PIXEL = %s

				def run(scope):
				    return Insight().run_pixel(resolve(PIXEL, scope))
				""".formatted(value(config, "pixel"));
	}

	private static String waitSource(Map<String, Object> config) {
		return """
				# Pause this automation node in Python.
				import time

				SECONDS = %s

				def run(scope):
				    seconds = float(resolve(SECONDS, scope))
				    time.sleep(seconds)
				    return {"waitedSeconds": seconds}
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
		Object value = config.get(key);
		if (value == null) {
			return "None";
		}
		if (value instanceof Boolean bool) {
			return bool ? "True" : "False";
		}
		return AutomationRuntimeUtils.GSON.toJson(value);
	}

	static boolean isLegacyDefaultSource(String source) {
		return LEGACY_DEFAULT_SOURCE.equals(source)
				|| source.startsWith("# Generated SEMOSS Automation ")
				&& source.contains("NODE_CONFIG =")
				&& source.contains("automation.run_current_node(scope, NODE_CONFIG)");
	}
}
