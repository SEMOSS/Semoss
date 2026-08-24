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
			return triggerSource();
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
			case AutomationConstants.NODE_STORAGE_READ -> storageReadSource(config);
			case AutomationConstants.NODE_STORAGE_UPLOAD -> storageTransferSource(config, "copyToStorage");
			case AutomationConstants.NODE_STORAGE_DOWNLOAD -> storageTransferSource(config, "copyToLocal");
			case AutomationConstants.NODE_STORAGE_DELETE -> storageSource(config, "deleteFromStorage", "STORAGE_PATH");
			case AutomationConstants.NODE_VECTOR_SEARCH -> vectorSearchSource(config);
			case AutomationConstants.NODE_VECTOR_ADD -> vectorAddSource(config);
			case AutomationConstants.NODE_VECTOR_DELETE -> vectorDeleteSource(config);
			case AutomationConstants.NODE_FUNCTION_EXECUTE -> functionSource(config);
			case AutomationConstants.NODE_APP_PIXEL -> appPixelSource(config);
			case AutomationConstants.NODE_AGENT_RUN -> agentRunSource(config);
			case AutomationConstants.NODE_CONTROL_WAIT -> waitSource(config);
			case AutomationConstants.NODE_DEVELOPER_PYTHON -> developerSource();
			default -> developerSource();
		};
		return source;
	}

	private static String triggerSource() {
		return """
				# Declare globals in trigger.start config.globals.
				# Define optional setup here; return a map only for additional runtime values.
				def run(scope):
				    return {}
				""";
	}

	private static String databaseQuerySource(Map<String, Object> config) {
		return """
				# Query a SEMOSS database directly through the Python SDK.
				from ai_server import DatabaseEngine

				ENGINE_ID = %s
				QUERY = %s

				def run(scope):
				    database = DatabaseEngine(engine_id=scope.resolve(ENGINE_ID))
				    return database.execQuery(query=scope.resolve(QUERY), return_pandas=False)
				""".formatted(value(config, "engineId"), value(config, "query"));
	}

	private static String databaseWriteSource(Map<String, Object> config, String method) {
		return """
				# Write to a SEMOSS database directly through the Python SDK.
				from ai_server import DatabaseEngine

				ENGINE_ID = %s
				QUERY = %s

				def run(scope):
				    database = DatabaseEngine(engine_id=scope.resolve(ENGINE_ID))
				    return database.%s(query=scope.resolve(QUERY))
				""".formatted(value(config, "engineId"), value(config, "query"), method);
	}

	private static String modelChatSource(Map<String, Object> config) {
		return """
				# Call a SEMOSS model directly through the Python SDK.
				from ai_server import ModelEngine

				ENGINE_ID = %s
				PROMPT = %s
				SYSTEM_PROMPT = %s
				PARAMETERS_JSON = %s
				ROOM_ID = "${_automation_room_id}"

				def run(scope):
				    model = ModelEngine(engine_id=scope.resolve(ENGINE_ID))
				    parameters = scope.resolve_config(PARAMETERS_JSON)
				    return model.ask(
				        command=scope.resolve(PROMPT),
				        context=scope.resolve(SYSTEM_PROMPT),
				        param_dict=parameters,
				        room_id=scope.resolve(ROOM_ID),
				    )
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
				    model = ModelEngine(engine_id=scope.resolve(ENGINE_ID))
				    return model.embeddings(strings_to_embed=scope.resolve(VALUES))
				""".formatted(value(config, "engineId"), value(config, "text"));
	}

	private static String modelVisionSource(Map<String, Object> config) {
		return """
				# Send an image prompt directly through the SEMOSS Python SDK.
				from ai_server import ModelEngine

				ENGINE_ID = %s
				PROMPT = %s
				IMAGE = %s
				ROOM_ID = "${_automation_room_id}"

				def run(scope):
				    model = ModelEngine(engine_id=scope.resolve(ENGINE_ID))
				    return model.ask(
				        command=scope.resolve(PROMPT),
				        image=[scope.resolve(IMAGE)],
				        room_id=scope.resolve(ROOM_ID),
				    )
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
				    model = ModelEngine(engine_id=scope.resolve(ENGINE_ID))
				    return model.ner(text=scope.resolve(TEXT), entities=scope.resolve(ENTITIES))
				""".formatted(value(config, "engineId"), value(config, "text"), value(config, "entities"));
	}

	private static String storageSource(Map<String, Object> config, String method, String argument) {
		return """
				# Access SEMOSS storage directly through the Python SDK.
				from ai_server import StorageEngine

				ENGINE_ID = %s
				STORAGE_PATH = %s

				def run(scope):
				    storage = StorageEngine(engine_id=scope.resolve(ENGINE_ID))
				    return storage.%s(scope.resolve(%s))
				""".formatted(value(config, "engineId"), value(config, "path"), method, argument);
	}

	private static String storageReadSource(Map<String, Object> config) {
		return """
				# Read storage content through the existing SEMOSS reactor; the Python SDK has no read method.
				from semoss import Insight
				import json

				ENGINE_ID = %s
				STORAGE_PATH = %s

				def _pixel_value(name, value):
				    return name + "=[" + json.dumps(value) + "]"

				def run(scope):
				    pixel = "GetStorageFileAsBase64(" + ", ".join([
				        _pixel_value("storage", scope.resolve(ENGINE_ID)),
				        _pixel_value("storagePath", scope.resolve(STORAGE_PATH)),
				    ]) + ");"
				    return Insight().run_pixel(pixel, raw=False)
				""".formatted(value(config, "engineId"), value(config, "path"));
	}

	private static String storageTransferSource(Map<String, Object> config, String method) {
		return """
				# Transfer files with SEMOSS storage through the Python SDK.
				from ai_server import StorageEngine

				ENGINE_ID = %s
				STORAGE_PATH = %s
				FILE_PATH = %s

				def run(scope):
				    storage = StorageEngine(engine_id=scope.resolve(ENGINE_ID))
				    return storage.%s(storagePath=scope.resolve(STORAGE_PATH), localPath=scope.resolve(FILE_PATH))
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
				    vector = VectorEngine(engine_id=scope.resolve(ENGINE_ID))
				    return vector.nearestNeighbor(search_statement=scope.resolve(QUERY), limit=scope.resolve(LIMIT))
				""".formatted(value(config, "engineId"), value(config, "value"), value(config, "limit"));
	}

	private static String vectorAddSource(Map<String, Object> config) {
		return """
				# Add documents to a SEMOSS vector engine through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				FILE_PATHS = %s

				def run(scope):
				    vector = VectorEngine(engine_id=scope.resolve(ENGINE_ID))
				    return vector.addDocument(file_paths=scope.resolve(FILE_PATHS).split(","))
				""".formatted(value(config, "engineId"), value(config, "value"));
	}

	private static String vectorDeleteSource(Map<String, Object> config) {
		return """
				# Remove documents from a SEMOSS vector engine through the Python SDK.
				from ai_server import VectorEngine

				ENGINE_ID = %s
				FILE_NAMES = %s

				def run(scope):
				    vector = VectorEngine(engine_id=scope.resolve(ENGINE_ID))
				    return vector.removeDocument(file_names=scope.resolve(FILE_NAMES).split(","))
				""".formatted(value(config, "engineId"), value(config, "value"));
	}

	private static String functionSource(Map<String, Object> config) {
		return """
				# Execute a SEMOSS function engine directly through the Python SDK.
				from ai_server import FunctionEngine

				ENGINE_ID = %s
				ARGUMENTS = %s

				def run(scope):
				    function = FunctionEngine(engine_id=scope.resolve(ENGINE_ID))
				    arguments = scope.resolve_config(ARGUMENTS)
				    return function.execute(parameterMap=arguments)
				""".formatted(value(config, "engineId"), value(config, "arguments"));
	}

	private static String appPixelSource(Map<String, Object> config) {
		return """
				# Load an optional application context, then run the validated static Pixel.
				from semoss import Insight
				import json

				APP_ID = %s
				PIXEL = %s

				def run(scope):
				    app_id = scope.resolve(APP_ID)
				    pixel = PIXEL
				    if app_id:
				        pixel = "LoadApp(project=" + json.dumps(app_id) + "); " + pixel
				    return Insight().run_pixel(pixel, raw=False)
				""".formatted(value(config, AutomationConstants.CONFIG_APP_ID), value(config, "pixel"));
	}

	private static String agentRunSource(Map<String, Object> config) {
		return """
				# Start a durable SEMOSS agent run in the room assigned by the automation runtime.
				from semoss import Insight
				import json

				ROOM_ID = "${_automation_room_id}"
				COMMAND = %s
				ENGINE_ID = %s
				HARNESS_TYPE = %s
				WORKSPACE_ID = %s
				MAX_TURNS = %s
				MAX_REFLECTIONS = %s
				WAIT = %s
				WAIT_TIMEOUT_MS = %s
				PARAM_MAP = %s
				AGENT_PARAMS = %s

				def _pixel_value(name, value):
				    return name + "=[" + json.dumps(value) + "]"

				def run(scope):
				    arguments = [
				        _pixel_value("roomId", scope.resolve(ROOM_ID)),
				        _pixel_value("command", scope.resolve(COMMAND)),
				    ]
				    for name, value in (
				        ("engine", ENGINE_ID),
				        ("harnessType", HARNESS_TYPE),
				        ("workspaceId", WORKSPACE_ID),
				        ("maxTurns", MAX_TURNS),
				        ("maxReflections", MAX_REFLECTIONS),
				        ("wait", WAIT),
				        ("waitTimeoutMs", WAIT_TIMEOUT_MS),
				    ):
				        if value is not None:
				            arguments.append(_pixel_value(name, scope.resolve(value)))
				    if PARAM_MAP is not None:
				        arguments.append("paramValues=" + json.dumps(scope.resolve_config(PARAM_MAP)))
				    if AGENT_PARAMS is not None:
				        arguments.append("agentParams=" + json.dumps(scope.resolve_config(AGENT_PARAMS)))
				    result = Insight().run_pixel(
				        "RunAgent(" + ", ".join(arguments) + ");",
				        raw=False,
				    )
				    if isinstance(result, list) and len(result) == 1 and isinstance(result[0], dict):
				        return result[0]
				    return result
				""".formatted(value(config, AutomationConstants.CONFIG_COMMAND),
				value(config, AutomationConstants.CONFIG_ENGINE_ID),
				value(config, AutomationConstants.CONFIG_HARNESS_TYPE),
				value(config, AutomationConstants.CONFIG_WORKSPACE_ID),
				value(config, AutomationConstants.CONFIG_MAX_TURNS),
				value(config, AutomationConstants.CONFIG_MAX_REFLECTIONS),
				valueOrDefault(config, AutomationConstants.CONFIG_WAIT, true),
				value(config, AutomationConstants.CONFIG_WAIT_TIMEOUT_MS),
				agentMapValue(config.containsKey("paramMap")
						? config.get("paramMap")
						: config.get(AutomationConstants.CONFIG_PARAM_VALUES)),
				agentMapValue(config.get("agentParams")));
	}

	private static String waitSource(Map<String, Object> config) {
		return """
				# Pause this automation node in Python.
				import time

				SECONDS = %s

				def run(scope):
				    seconds = float(scope.resolve(SECONDS))
				    time.sleep(seconds)
				    return {"waitedSeconds": seconds}
				""".formatted(value(config, "durationSeconds"));
	}

	private static String developerSource() {
		return """
				# Write arbitrary Python for this automation node here.
				# scope is a read-only, run-local mapping: inputs, globals, metadata, and prior outputs by outputVar.
				# Read required values with scope["outputVar"] and optional values with scope.get("outputVar").
				# Return a JSON-shaped value to pass data to the next node.
				def run(scope):
				    return {}
				""";
	}

	private static String value(Map<String, Object> config, String key) {
		return pythonValue(config.get(key));
	}

	private static String pythonValue(Object value) {
		if (value == null) {
			return "None";
		}
		if (value instanceof Boolean bool) {
			return bool ? "True" : "False";
		}
		if (value instanceof Map<?, ?> || value instanceof Iterable<?>) {
			String json = AutomationRuntimeUtils.toRuntimeJson(value);
			return "__import__(\"json\").loads(" + AutomationRuntimeUtils.GSON.toJson(json) + ")";
		}
		return AutomationRuntimeUtils.GSON.toJson(value);
	}

	private static String valueOrDefault(Map<String, Object> config, String key, Object defaultValue) {
		return pythonValue(config.getOrDefault(key, defaultValue));
	}

	private static String agentMapValue(Object value) {
		return pythonValue(value);
	}

	static boolean isLegacyDefaultSource(String source) {
		return LEGACY_DEFAULT_SOURCE.equals(source)
				|| source.startsWith("# Generated SEMOSS Automation ")
				&& source.contains("NODE_CONFIG =")
				&& source.contains("automation.run_current_node(scope, NODE_CONFIG)");
	}
}
