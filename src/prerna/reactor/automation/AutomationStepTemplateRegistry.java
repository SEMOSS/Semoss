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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonSyntaxException;

import prerna.engine.api.IEngine;
import prerna.reactor.automation.utils.AutomationExecutionUtils;

/**
 * Catalogs supported managed-Python automation actions and renders their project-owned step
 * source. This catalog is intentionally independent from the canvas so a future client action
 * picker can consume its action IDs, descriptions, usage, and required engine catalog types.
 */
public final class AutomationStepTemplateRegistry {

	private static final String SAFE_NODE_ID_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_.-]*$";

	private static final String AS_LIST_HELPER = """
			def _as_list(value):
			    if isinstance(value, list):
			        return value
			    if isinstance(value, str):
			        return [item.strip() for item in value.split(",") if item.strip()]
			    raise ValueError("Expected a string or list of strings")

			""";

	private static final String AUTOMATION_FILE_HELPER = """
			import posixpath

			def _automation_file(path):
			    if not isinstance(path, str) or not path.strip():
			        raise ValueError("Automation file path must be a nonblank relative path")
			    if path.startswith(("/", "\\\\")) or "\\\\" in path or ":" in path:
			        raise ValueError("Automation file path must be relative to automation-files")
			    normalized = posixpath.normpath(path)
			    if normalized in (".", "..") or normalized.startswith("../"):
			        raise ValueError("Automation file path cannot leave automation-files")
			    return "automation-files/" + normalized

			""";

	private static final List<ActionDefinition> ACTIONS = List.of(
			action("model.llm", AutomationConstants.NODE_MODEL_ENGINE, AutomationConstants.OP_LLM,
					IEngine.CATALOG_TYPE.MODEL, false, "Runs an LLM prompt with a managed ModelEngine.",
					"Config: engineId, command; optional context and paramValues. Map runtime values with "
							+ "inputs.command, inputs.context, or inputs.paramValues.", AutomationStepTemplateRegistry::modelLlm),
			action("model.embeddings", AutomationConstants.NODE_MODEL_ENGINE, AutomationConstants.OP_EMBEDDINGS,
					IEngine.CATALOG_TYPE.MODEL, false, "Creates embeddings with a managed ModelEngine.",
					"Config: engineId, values; optional paramValues. Set inputs.values to map a runtime value.",
					AutomationStepTemplateRegistry::modelEmbeddings),
			action("database.query", AutomationConstants.NODE_DATABASE_ENGINE, "query",
					IEngine.CATALOG_TYPE.DATABASE, false, "Runs a read query with a managed DatabaseEngine.",
					"Config: engineId and expression. Set inputs.expression to map a runtime query value.",
					AutomationStepTemplateRegistry::databaseQuery),
			action("database.write", AutomationConstants.NODE_DATABASE_ENGINE, AutomationConstants.OP_WRITE,
					IEngine.CATALOG_TYPE.DATABASE, true, "Runs a write statement with a managed DatabaseEngine.",
					"Config: engineId and expression. Set inputs.expression to map a runtime statement value.",
					AutomationStepTemplateRegistry::databaseWrite),
			action("vector.search", AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_SEARCH,
					IEngine.CATALOG_TYPE.VECTOR, false, "Searches documents with a managed VectorEngine.",
					"Config: engineId and command; optional limit and paramValues.",
					AutomationStepTemplateRegistry::vectorSearch),
			action("vector.list", AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_LIST,
					IEngine.CATALOG_TYPE.VECTOR, false, "Lists documents with a managed VectorEngine.",
					"Config: engineId; optional paramValues.", AutomationStepTemplateRegistry::vectorList),
			action("vector.delete", AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_DELETE,
					IEngine.CATALOG_TYPE.VECTOR, true, "Deletes documents with a managed VectorEngine.",
					"Config: engineId and fileNames (a list or comma-separated string).",
					AutomationStepTemplateRegistry::vectorDelete),
			action("vector.add-file", AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_ADD_FILE,
					IEngine.CATALOG_TYPE.VECTOR, true, "Adds documents with a managed VectorEngine.",
					"Config: engineId and filePath (a list or comma-separated string); optional paramValues.",
					AutomationStepTemplateRegistry::vectorAddFile),
			action("vector.add-csv", AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_ADD_CSV,
					IEngine.CATALOG_TYPE.VECTOR, true, "Adds vector CSV documents with a managed VectorEngine.",
					"Config: engineId and filePath (a list or comma-separated string); optional paramValues.",
					AutomationStepTemplateRegistry::vectorAddCsv),
			action("vector.download", AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_DOWNLOAD,
					IEngine.CATALOG_TYPE.VECTOR, false, "Downloads source files from a vector engine.",
					"Config: engineId and fileNames. Uses Insight because VectorEngine has no download wrapper.",
					AutomationStepTemplateRegistry::vectorDownload),
			action("storage.list", AutomationConstants.NODE_STORAGE_ENGINE, AutomationConstants.OP_LIST,
					IEngine.CATALOG_TYPE.STORAGE, false, "Lists paths with a managed StorageEngine.",
					"Config: engineId; optional storagePath (defaults to /).",
					AutomationStepTemplateRegistry::storageList),
			action("storage.download", AutomationConstants.NODE_STORAGE_ENGINE, AutomationConstants.OP_DOWNLOAD,
					IEngine.CATALOG_TYPE.STORAGE, false, "Downloads a file with a managed StorageEngine.",
					"Config: engineId, storagePath, and a relative filePath under automation-files.",
					AutomationStepTemplateRegistry::storageDownload),
			action("storage.upload", AutomationConstants.NODE_STORAGE_ENGINE, AutomationConstants.OP_UPLOAD,
					IEngine.CATALOG_TYPE.STORAGE, true, "Uploads a file with a managed StorageEngine.",
					"Config: engineId, storagePath, and a relative filePath under automation-files.",
					AutomationStepTemplateRegistry::storageUpload),
			action("storage.delete", AutomationConstants.NODE_STORAGE_ENGINE, AutomationConstants.OP_DELETE,
					IEngine.CATALOG_TYPE.STORAGE, true, "Deletes a path with a managed StorageEngine.",
					"Config: engineId and storagePath.", AutomationStepTemplateRegistry::storageDelete),
			action("storage.read-base64", AutomationConstants.NODE_STORAGE_ENGINE, AutomationConstants.OP_READ_BASE64,
					IEngine.CATALOG_TYPE.STORAGE, true, "Reads storage content as base64.",
					"Config: engineId and storagePath. Uses Insight because StorageEngine has no base64-read wrapper.",
					AutomationStepTemplateRegistry::storageReadBase64),
			action("function.execute", AutomationConstants.NODE_FUNCTION_ENGINE, "execute",
					IEngine.CATALOG_TYPE.FUNCTION, true, "Executes a managed FunctionEngine.",
					"Config: engineId; optional params object. Set inputs.params to map runtime parameters.",
					AutomationStepTemplateRegistry::functionExecute),
			action("app.run-pixel", AutomationConstants.NODE_APP, AutomationConstants.OP_RUN_PIXEL,
					null, true, "Runs Pixel in the current automation insight.",
					"Config: pixel. Set inputs.pixel to map a runtime Pixel expression. appId is not supported by this template.",
					AutomationStepTemplateRegistry::appRunPixel),
			action("python-step.skeleton", AutomationConstants.NODE_PYTHON_STEP, AutomationConstants.OP_SKELETON,
					null, true, "Creates an editable custom Python step skeleton.",
					"Config is optional. Edit the generated run(context, inputs) function to add custom managed-Python code.",
					AutomationStepTemplateRegistry::pythonSkeleton));

	private static final Map<String, String> DEFAULT_OPERATIONS = Map.of(
			AutomationConstants.NODE_MODEL_ENGINE, AutomationConstants.OP_LLM,
			AutomationConstants.NODE_DATABASE_ENGINE, "query",
			AutomationConstants.NODE_VECTOR_ENGINE, AutomationConstants.OP_SEARCH,
			AutomationConstants.NODE_STORAGE_ENGINE, AutomationConstants.OP_LIST,
			AutomationConstants.NODE_FUNCTION_ENGINE, "execute",
			AutomationConstants.NODE_APP, AutomationConstants.OP_RUN_PIXEL,
			AutomationConstants.NODE_PYTHON_STEP, AutomationConstants.OP_SKELETON);

	private AutomationStepTemplateRegistry() {
	}

	/**
	 * Returns the supported action catalog for UI and API consumers.
	 *
	 * @return immutable action definitions
	 */
	public static List<ActionDefinition> getActions() {
		return ACTIONS;
	}

	/**
	 * Locates a supported action by its stable business-facing identifier.
	 *
	 * @param actionId action identifier, such as {@code model.llm}
	 * @return matching action definition
	 */
	public static ActionDefinition getAction(String actionId) {
		if (actionId == null || actionId.isBlank()) {
			throw new IllegalArgumentException("Automation actionId is required.");
		}
		return ACTIONS.stream()
				.filter(action -> action.actionId.equals(actionId))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Unsupported automation actionId: \"" + actionId + "\"."));
	}

	/**
	 * Locates a supported action for a node type and its configured operation.
	 *
	 * @param nodeType canvas node type
	 * @param config node configuration
	 * @return matching action definition
	 */
	public static ActionDefinition selectAction(String nodeType, Map<String, Object> config) {
		if (config == null) {
			throw new IllegalArgumentException("Automation step config must be a JSON object.");
		}
		if (nodeType == null || nodeType.isBlank()) {
			throw new IllegalArgumentException("Automation step node type is required.");
		}

		String operation = configuredOperation(nodeType, config);
		return ACTIONS.stream()
				.filter(action -> action.nodeType.equals(nodeType) && action.operation.equals(operation))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Unsupported automation step action: node type \""
						+ nodeType + "\" with operation \"" + operation + "\"."));
	}

	/**
	 * Generates managed Python source for a validated action.
	 *
	 * @param nodeType canvas node type
	 * @param config node configuration, including a resolved engine ID when applicable
	 * @return generated source and action metadata
	 */
	public static GeneratedStep generate(String nodeType, Map<String, Object> config) {
		ActionDefinition action = selectAction(nodeType, config);
		String source = action.generator.generate(new LinkedHashMap<>(config));
		return new GeneratedStep(action.actionId, action.description, action.usage, source);
	}

	/**
	 * Validates that a loaded engine matches the selected action's catalog type.
	 *
	 * @param action selected action
	 * @param actualType loaded engine catalog type
	 */
	public static void validateEngineCatalog(ActionDefinition action, IEngine.CATALOG_TYPE actualType) {
		if (action.expectedCatalogType == null) {
			return;
		}
		if (actualType != action.expectedCatalogType) {
			throw new IllegalArgumentException("Automation action \"" + action.actionId + "\" requires a "
					+ action.expectedCatalogType + " engine, but the selected engine is "
					+ (actualType == null ? "unknown" : actualType) + ".");
		}
	}

	/**
	 * Uses the same safe basename rules as {@code PythonStepNodeExecutor}.
	 *
	 * @param nodeId node identifier
	 * @return whether the ID can safely name a step file
	 */
	public static boolean isSafeStepNodeId(String nodeId) {
		return nodeId != null && nodeId.matches(SAFE_NODE_ID_PATTERN);
	}

	private static ActionDefinition action(String actionId, String nodeType, String operation,
			IEngine.CATALOG_TYPE expectedCatalogType, boolean requiresEditAccess, String description,
			String usage, SourceGenerator generator) {
		return new ActionDefinition(actionId, nodeType, operation, expectedCatalogType, requiresEditAccess,
				description, usage, generator);
	}

	private static String configuredOperation(String nodeType, Map<String, Object> config) {
		Object configured = config.get(AutomationConstants.CONFIG_OPERATION);
		if (configured == null) {
			String defaultOperation = DEFAULT_OPERATIONS.get(nodeType);
			if (defaultOperation == null) {
				throw new IllegalArgumentException("Unsupported automation step node type: \"" + nodeType + "\".");
			}
			return defaultOperation;
		}
		if (!(configured instanceof String operation) || operation.isBlank()) {
			throw new IllegalArgumentException("Automation step config field \"operation\" must be a nonblank string.");
		}
		return operation;
	}

	private static String modelLlm(Map<String, Object> config) {
		String engineId = optionalString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String command = requiredString(config, AutomationConstants.CONFIG_COMMAND);
		String context = optionalString(config, AutomationConstants.CONFIG_CONTEXT);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAM_VALUES);
		return sourceHeader("from ai_server import ModelEngine", "")
				+ assignment("_engine_id", literal(engineId == null ? "" : engineId))
				+ assignment("_command", mappedInput(config, AutomationConstants.CONFIG_COMMAND, command))
				+ assignment("_context", mappedInput(config, AutomationConstants.CONFIG_CONTEXT, context))
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAM_VALUES, params))
				+ """
				    if not _engine_id:
				        raise ValueError("Select an AI Engine for this model action before running the automation")
				"""
				+ "    model = ModelEngine(engine_id=_engine_id)\n"
				+ "    return model.ask(command=_command, context=_context, param_dict=_params)\n";
	}

	private static String modelEmbeddings(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		List<String> values = requiredStringList(config, AutomationConstants.CONFIG_VALUES);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAM_VALUES);
		return sourceHeader("from ai_server import ModelEngine", AS_LIST_HELPER)
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_values", "_as_list(" + mappedInput(config, AutomationConstants.CONFIG_VALUES, values) + ")")
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAM_VALUES, params))
				+ "    model = ModelEngine(engine_id=_engine_id)\n"
				+ "    return model.embeddings(strings_to_embed=_values, param_dict=_params)\n";
	}

	private static String databaseQuery(Map<String, Object> config) {
		return databaseSource(config, false);
	}

	private static String databaseWrite(Map<String, Object> config) {
		return databaseSource(config, true);
	}

	private static String databaseSource(Map<String, Object> config, boolean write) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String expression = requiredString(config, AutomationConstants.CONFIG_EXPRESSION);
		String execution = write
				? "    return database.runQuery(query=_expression)\n"
				: """
						    _result = database.execQuery(query=_expression, return_pandas=False)
						    try:
						        return json.loads(_result)
						    except (TypeError, json.JSONDecodeError):
						        return _result
						""";
		return sourceHeader("from ai_server import DatabaseEngine", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_expression", mappedInput(config, AutomationConstants.CONFIG_EXPRESSION, expression))
				+ "    database = DatabaseEngine(engine_id=_engine_id)\n"
				+ execution;
	}

	private static String vectorSearch(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String command = requiredString(config, AutomationConstants.CONFIG_COMMAND);
		int limit = optionalPositiveInt(config, AutomationConstants.CONFIG_LIMIT,
				AutomationConstants.DEFAULT_VECTOR_SEARCH_LIMIT);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAM_VALUES);
		return sourceHeader("from ai_server import VectorEngine", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_command", mappedInput(config, AutomationConstants.CONFIG_COMMAND, command))
				+ assignment("_limit", mappedInput(config, AutomationConstants.CONFIG_LIMIT, limit))
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAM_VALUES, params))
				+ "    vector = VectorEngine(engine_id=_engine_id)\n"
				+ "    return vector.nearestNeighbor(search_statement=_command, limit=_limit, param_dict=_params)\n";
	}

	private static String vectorList(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAM_VALUES);
		return sourceHeader("from ai_server import VectorEngine", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAM_VALUES, params))
				+ "    vector = VectorEngine(engine_id=_engine_id)\n"
				+ "    return vector.listDocuments(param_dict=_params)\n";
	}

	private static String vectorDelete(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		List<String> fileNames = requiredStringList(config, AutomationConstants.CONFIG_FILE_NAMES);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAM_VALUES);
		return sourceHeader("from ai_server import VectorEngine", AS_LIST_HELPER)
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_file_names", "_as_list("
						+ mappedInput(config, AutomationConstants.CONFIG_FILE_NAMES, fileNames) + ")")
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAM_VALUES, params))
				+ "    vector = VectorEngine(engine_id=_engine_id)\n"
				+ "    return vector.removeDocument(file_names=_file_names, param_dict=_params)\n";
	}

	private static String vectorAddFile(Map<String, Object> config) {
		return vectorAdd(config, "addDocument");
	}

	private static String vectorAddCsv(Map<String, Object> config) {
		return vectorAdd(config, "addVectorCSVFile");
	}

	private static String vectorAdd(Map<String, Object> config, String method) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		List<String> filePaths = requiredStringList(config, AutomationConstants.CONFIG_FILE_PATH);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAM_VALUES);
		return sourceHeader("from ai_server import VectorEngine", AS_LIST_HELPER)
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_file_paths", "_as_list("
						+ mappedInput(config, AutomationConstants.CONFIG_FILE_PATH, filePaths) + ")")
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAM_VALUES, params))
				+ "    vector = VectorEngine(engine_id=_engine_id)\n"
				+ "    return vector." + method + "(file_paths=_file_paths, param_dict=_params)\n";
	}

	private static String vectorDownload(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		List<String> fileNames = requiredStringList(config, AutomationConstants.CONFIG_FILE_NAMES);
		return sourceHeader("from semoss import Insight", AS_LIST_HELPER)
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_file_names", "_as_list("
						+ mappedInput(config, AutomationConstants.CONFIG_FILE_NAMES, fileNames) + ")")
				+ """
						    _pixel = ("VectorFileDownload(engine=[" + json.dumps(_engine_id)
						              + "], fileNames=" + json.dumps(_file_names) + ");")
						    return Insight().run_pixel(pixel=_pixel, raw=False)
						""";
	}

	private static String storageList(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String storagePath = optionalString(config, AutomationConstants.CONFIG_STORAGE_PATH,
				AutomationConstants.DEFAULT_STORAGE_PATH);
		return sourceHeader("from ai_server import StorageEngine", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_storage_path", mappedInput(config, AutomationConstants.CONFIG_STORAGE_PATH, storagePath))
				+ "    storage = StorageEngine(engine_id=_engine_id)\n"
				+ "    return storage.list(storagePath=_storage_path)\n";
	}

	private static String storageDownload(Map<String, Object> config) {
		return storageTransfer(config, "copyToLocal");
	}

	private static String storageUpload(Map<String, Object> config) {
		return storageTransfer(config, "copyToStorage");
	}

	private static String storageTransfer(Map<String, Object> config, String method) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String storagePath = requiredString(config, AutomationConstants.CONFIG_STORAGE_PATH);
		String filePath = requiredAutomationFilePath(config);
		return sourceHeader("from ai_server import StorageEngine", AUTOMATION_FILE_HELPER)
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_storage_path", mappedInput(config, AutomationConstants.CONFIG_STORAGE_PATH, storagePath))
				+ assignment("_file_path", "_automation_file("
						+ mappedInput(config, AutomationConstants.CONFIG_FILE_PATH, filePath) + ")")
				+ "    storage = StorageEngine(engine_id=_engine_id)\n"
				+ "    return storage." + method
				+ "(storagePath=_storage_path, localPath=_file_path, space=context.get(\"projectId\"))\n";
	}

	private static String storageDelete(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String storagePath = requiredString(config, AutomationConstants.CONFIG_STORAGE_PATH);
		return sourceHeader("from ai_server import StorageEngine", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_storage_path", mappedInput(config, AutomationConstants.CONFIG_STORAGE_PATH, storagePath))
				+ "    storage = StorageEngine(engine_id=_engine_id)\n"
				+ "    return storage.deleteFromStorage(storagePath=_storage_path)\n";
	}

	private static String storageReadBase64(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		String storagePath = requiredString(config, AutomationConstants.CONFIG_STORAGE_PATH);
		return sourceHeader("from semoss import Insight", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_storage_path", mappedInput(config, AutomationConstants.CONFIG_STORAGE_PATH, storagePath))
				+ """
						    _pixel = ("GetStorageFileAsBase64(storage=[" + json.dumps(_engine_id)
						              + "], storagePath=[" + json.dumps(_storage_path) + "]);")
						    return Insight().run_pixel(pixel=_pixel, raw=False)
						""";
	}

	private static String functionExecute(Map<String, Object> config) {
		String engineId = requiredString(config, AutomationConstants.CONFIG_ENGINE_ID);
		Map<String, Object> params = optionalJsonObject(config, AutomationConstants.CONFIG_PARAMS);
		return sourceHeader("from ai_server import FunctionEngine", "")
				+ assignment("_engine_id", literal(engineId))
				+ assignment("_params", mappedInput(config, AutomationConstants.CONFIG_PARAMS, params))
				+ "    function = FunctionEngine(engine_id=_engine_id)\n"
				+ "    return function.execute(parameterMap=_params)\n";
	}

	private static String appRunPixel(Map<String, Object> config) {
		rejectAppProjectContext(config);
		String pixel = requiredString(config, AutomationConstants.CONFIG_PIXEL);
		return sourceHeader("from semoss import Insight", "")
				+ assignment("_pixel", mappedInput(config, AutomationConstants.CONFIG_PIXEL, pixel))
				+ "    return Insight().run_pixel(pixel=_pixel, raw=False)\n";
	}

	private static String pythonSkeleton(Map<String, Object> config) {
		return """
				# Generated custom SEMOSS automation step. Edit this function to add managed Python code.

				def run(context, inputs):
				    inputs = inputs or {}
				    return inputs
				""";
	}

	private static String sourceHeader(String imports, String helpers) {
		return """
				# Generated managed SEMOSS automation step. Changes are project-owned and versioned.
				import json
				%s

				%sdef run(context, inputs):
				    inputs = inputs or {}
				""".formatted(imports, helpers);
	}

	private static String assignment(String name, String expression) {
		return "    " + name + " = " + expression + "\n";
	}

	private static String mappedInput(Map<String, Object> config, String field, Object fallback) {
		Object rawMappings = config.get(AutomationConstants.CONFIG_INPUTS);
		if (rawMappings != null && !(rawMappings instanceof Map<?, ?>)) {
			throw new IllegalArgumentException("Automation step config field \"inputs\" must be an object.");
		}
		return "inputs.get(" + literal(field) + ", " + literal(fallback) + ")";
	}

	private static String literal(Object value) {
		String json = AutomationExecutionUtils.GSON.toJson(value);
		return "json.loads(" + AutomationExecutionUtils.GSON.toJson(json) + ")";
	}

	private static String requiredString(Map<String, Object> config, String field) {
		Object value = config.get(field);
		if (!(value instanceof String string) || string.isBlank()) {
			throw new IllegalArgumentException("Automation step config field \"" + field
					+ "\" must be a nonblank string.");
		}
		return string;
	}

	private static String optionalString(Map<String, Object> config, String field) {
		return optionalString(config, field, null);
	}

	private static String optionalString(Map<String, Object> config, String field, String defaultValue) {
		Object value = config.get(field);
		if (value == null) {
			return defaultValue;
		}
		if (!(value instanceof String string)) {
			throw new IllegalArgumentException("Automation step config field \"" + field + "\" must be a string.");
		}
		return string;
	}

	private static Map<String, Object> optionalJsonObject(Map<String, Object> config, String field) {
		Object value = config.get(field);
		if (value == null || value instanceof String string && string.isBlank()) {
			return Map.of();
		}
		if (value instanceof String string) {
			try {
				value = AutomationExecutionUtils.GSON.fromJson(string, Object.class);
			} catch (JsonSyntaxException e) {
				throw new IllegalArgumentException("Automation step config field \"" + field
						+ "\" must be valid JSON object text.", e);
			}
		}
		if (!(value instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException("Automation step config field \"" + field + "\" must be an object.");
		}
		Map<String, Object> result = new LinkedHashMap<>();
		for (Map.Entry<?, ?> entry : map.entrySet()) {
			if (!(entry.getKey() instanceof String key)) {
				throw new IllegalArgumentException("Automation step config field \"" + field
						+ "\" must use string keys.");
			}
			result.put(key, entry.getValue());
		}
		return result;
	}

	private static List<String> requiredStringList(Map<String, Object> config, String field) {
		Object value = config.get(field);
		if (value instanceof String string) {
			List<String> result = List.of(string.split(",")).stream()
					.map(String::trim)
					.filter(item -> !item.isEmpty())
					.toList();
			if (!result.isEmpty()) {
				return result;
			}
		} else if (value instanceof Collection<?> collection && !collection.isEmpty()) {
			List<String> result = collection.stream().map(item -> {
				if (!(item instanceof String string) || string.isBlank()) {
					throw new IllegalArgumentException("Automation step config field \"" + field
							+ "\" must contain only nonblank strings.");
				}
				return string;
			}).toList();
			if (!result.isEmpty()) {
				return result;
			}
		}
		throw new IllegalArgumentException("Automation step config field \"" + field
				+ "\" must be a nonempty comma-separated string or array of strings.");
	}

	private static int optionalPositiveInt(Map<String, Object> config, String field, int defaultValue) {
		Object value = config.get(field);
		if (value == null) {
			return defaultValue;
		}
		int parsed;
		if (value instanceof Number number) {
			parsed = number.intValue();
			if (number.doubleValue() != parsed) {
				throw new IllegalArgumentException("Automation step config field \"" + field + "\" must be an integer.");
			}
		} else if (value instanceof String string) {
			try {
				parsed = Integer.parseInt(string);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Automation step config field \"" + field + "\" must be an integer.", e);
			}
		} else {
			throw new IllegalArgumentException("Automation step config field \"" + field + "\" must be an integer.");
		}
		if (parsed <= 0) {
			throw new IllegalArgumentException("Automation step config field \"" + field + "\" must be positive.");
		}
		return parsed;
	}

	private static String requiredAutomationFilePath(Map<String, Object> config) {
		String filePath = requiredString(config, AutomationConstants.CONFIG_FILE_PATH);
		if (filePath.startsWith("/") || filePath.startsWith("\\") || filePath.contains("\\")
				|| filePath.contains(":") || filePath.split("/").length == 0) {
			throw new IllegalArgumentException("Automation storage filePath must be relative to automation-files.");
		}
		for (String segment : filePath.split("/")) {
			if (segment.isBlank() || ".".equals(segment) || "..".equals(segment)) {
				throw new IllegalArgumentException("Automation storage filePath must be relative to automation-files.");
			}
		}
		return filePath;
	}

	private static void rejectAppProjectContext(Map<String, Object> config) {
		String appId = optionalString(config, AutomationConstants.CONFIG_APP_ID);
		if (appId != null && !appId.isBlank()) {
			throw new IllegalArgumentException("The managed Python app template does not support appId. "
					+ "Use an app node without appId or retain the existing Java executor.");
		}
	}

	@FunctionalInterface
	private interface SourceGenerator {
		String generate(Map<String, Object> config);
	}

	/**
	 * Metadata for one supported Python-first automation action.
	 */
	public static final class ActionDefinition {
		private final String actionId;
		private final String nodeType;
		private final String operation;
		private final IEngine.CATALOG_TYPE expectedCatalogType;
		private final boolean requiresEditAccess;
		private final String description;
		private final String usage;
		private final SourceGenerator generator;

		private ActionDefinition(String actionId, String nodeType, String operation,
				IEngine.CATALOG_TYPE expectedCatalogType, boolean requiresEditAccess,
				String description, String usage, SourceGenerator generator) {
			this.actionId = actionId;
			this.nodeType = nodeType;
			this.operation = operation;
			this.expectedCatalogType = expectedCatalogType;
			this.requiresEditAccess = requiresEditAccess;
			this.description = description;
			this.usage = usage;
			this.generator = generator;
		}

		public String getActionId() {
			return actionId;
		}

		public String getNodeType() {
			return nodeType;
		}

		public String getOperation() {
			return operation;
		}

		public IEngine.CATALOG_TYPE getExpectedCatalogType() {
			return expectedCatalogType;
		}

		public boolean requiresEditAccess() {
			return requiresEditAccess;
		}

		public String getDescription() {
			return description;
		}

		public String getUsage() {
			return usage;
		}
	}

	/**
	 * Generated source and metadata returned to the authoring client.
	 */
	public static final class GeneratedStep {
		private final String actionId;
		private final String description;
		private final String usage;
		private final String source;

		private GeneratedStep(String actionId, String description, String usage, String source) {
			this.actionId = actionId;
			this.description = description;
			this.usage = usage;
			this.source = source;
		}

		public String getActionId() {
			return actionId;
		}

		public String getDescription() {
			return description;
		}

		public String getUsage() {
			return usage;
		}

		public String getSource() {
			return source;
		}
	}
}
