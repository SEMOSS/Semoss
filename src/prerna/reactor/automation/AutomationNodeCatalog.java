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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.reactor.automation.AutomationNodeDefinition.ConfigField;
import prerna.reactor.automation.AutomationNodeDefinition.ConfigFieldType;
import prerna.reactor.automation.AutomationNodeDefinition.Port;
import prerna.reactor.automation.AutomationNodeDefinition.PortDirection;
import prerna.reactor.automation.AutomationNodeDefinition.PortKind;

/**
 * Server-owned catalog of Automation node authoring contracts.
 *
 * <p>
 * Runtime validation remains authoritative. This catalog exposes the same stable
 * defaults, bounds, resource categories, and ports so clients do not duplicate
 * or infer backend behavior from node type strings.
 */
public final class AutomationNodeCatalog {

	public static final int SCHEMA_VERSION = 1;

	private static final Port CONTROL_INPUT = port("in", "In", PortKind.CONTROL, PortDirection.INPUT, null);
	private static final Port CONTROL_OUTPUT = port("out", "Next", PortKind.CONTROL, PortDirection.OUTPUT, null);
	private static final Port RESULT_OUTPUT = port("result", "Result", PortKind.DATA, PortDirection.OUTPUT, "unknown");
	private static final List<AutomationNodeDefinition> DEFINITIONS = createDefinitions();
	private static final Map<AutomationNodeType, AutomationNodeDefinition> DEFINITIONS_BY_TYPE = indexDefinitions();

	private AutomationNodeCatalog() {
	}

	/**
	 * @return immutable definitions in display order
	 */
	public static List<AutomationNodeDefinition> getDefinitions() {
		return DEFINITIONS;
	}

	/**
	 * Returns the definition for a supported node type.
	 *
	 * @param nodeType node type
	 * @return matching definition
	 */
	public static AutomationNodeDefinition getDefinition(AutomationNodeType nodeType) {
		AutomationNodeDefinition definition = DEFINITIONS_BY_TYPE.get(nodeType);
		if (definition == null) {
			throw new IllegalArgumentException("Missing Automation node definition for " + nodeType.getType());
		}
		return definition;
	}

	/**
	 * @return versioned Pixel response consumed by Automation authoring clients
	 */
	public static Map<String, Object> getResponse() {
		List<Map<String, Object>> nodes = new ArrayList<>(DEFINITIONS.size());
		for (AutomationNodeDefinition definition : DEFINITIONS) {
			nodes.add(definition.toMap());
		}
		Map<String, Object> response = new LinkedHashMap<>();
		response.put("schemaVersion", SCHEMA_VERSION);
		response.put("nodes", nodes);
		return response;
	}

	private static List<AutomationNodeDefinition> createDefinitions() {
		List<AutomationNodeDefinition> definitions = new ArrayList<>();
		definitions.add(definition(AutomationNodeType.TRIGGER_START, "Start",
				"Begins this automation when it is triggered.", Map.of(),
				List.of(field("globals", ConfigFieldType.GLOBALS, "Inputs", false, List.of()),
						field("pythonSource", ConfigFieldType.CODE, "Setup Python", false, "")),
				List.of(), List.of(CONTROL_OUTPUT)));

		definitions.add(definition(AutomationNodeType.DATABASE_QUERY, "Query database",
				"Retrieve bounded rows with a database query.",
				orderedConfig("engineId", "", "query", "", "limit", AutomationConstants.DEFAULT_DB_QUERY_LIMIT),
				List.of(engineField(IEngine.CATALOG_TYPE.DATABASE),
						field("query", ConfigFieldType.SQL, "Query", true, ""),
						boundedIntegerField("limit", "Result limit", AutomationConstants.DEFAULT_DB_QUERY_LIMIT,
								AutomationConstants.DB_QUERY_MIN_LIMIT, AutomationConstants.DB_QUERY_MAX_LIMIT)),
				controlInputs(), controlAndResultOutputs()));
		definitions.add(databaseWriteDefinition(AutomationNodeType.DATABASE_INSERT, "Insert database rows",
				"Add records to a database table."));
		definitions.add(databaseWriteDefinition(AutomationNodeType.DATABASE_UPDATE, "Update database rows",
				"Update matching records in a database table."));

		definitions.add(definition(AutomationNodeType.MODEL_CHAT, "Chat model",
				"Ask a language model to generate a response.",
				orderedConfig("engineId", "", "systemPrompt", "", "prompt", "", "paramValues", Map.of()),
				List.of(engineField(IEngine.CATALOG_TYPE.MODEL),
						field("systemPrompt", ConfigFieldType.TEXT, "Instructions for the model", false, ""),
						field("prompt", ConfigFieldType.TEXT, "Prompt", true, ""),
						field("paramValues", ConfigFieldType.JSON, "Model parameters", false, Map.of())),
				controlInputs(), controlAndResultOutputs()));
		definitions.add(definition(AutomationNodeType.MODEL_EMBEDDINGS, "Create embeddings",
				"Turn text into vectors with a model engine.", orderedConfig("engineId", "", "text", ""),
				List.of(engineField(IEngine.CATALOG_TYPE.MODEL),
						field("text", ConfigFieldType.TEXT, "Text to embed", true, "")),
				controlInputs(), controlAndResultOutputs()));
		definitions.add(definition(AutomationNodeType.MODEL_VISION, "Analyze image",
				"Ask a vision model about an image.", orderedConfig("engineId", "", "image", "", "prompt", ""),
				List.of(engineField(IEngine.CATALOG_TYPE.MODEL),
						field("image", ConfigFieldType.STRING, "Image path or URL", true, ""),
						field("prompt", ConfigFieldType.TEXT, "Question about the image", true, "")),
				controlInputs(), controlAndResultOutputs()));
		definitions.add(definition(AutomationNodeType.MODEL_NER, "Extract entities",
				"Find named entities in text.", orderedConfig("engineId", "", "text", "", "entities", List.of()),
				List.of(engineField(IEngine.CATALOG_TYPE.MODEL),
						field("text", ConfigFieldType.TEXT, "Text to analyze", true, ""),
						field("entities", ConfigFieldType.STRING_LIST, "Entity types", true, List.of())),
				controlInputs(), controlAndResultOutputs()));

		definitions.add(storageDefinition(AutomationNodeType.STORAGE_LIST, "List files",
				"List files in connected storage.", false, false));
		definitions.add(storageDefinition(AutomationNodeType.STORAGE_READ, "Read file",
				"Read content from connected storage.", true, false));
		definitions.add(storageDefinition(AutomationNodeType.STORAGE_UPLOAD, "Upload file",
				"Upload a local file to connected storage.", true, true));
		definitions.add(storageDefinition(AutomationNodeType.STORAGE_DOWNLOAD, "Download file",
				"Download a storage file to a local destination.", true, true));
		definitions.add(storageDefinition(AutomationNodeType.STORAGE_DELETE, "Delete file",
				"Delete a file from connected storage.", true, false));

		definitions.add(vectorDefinition(AutomationNodeType.VECTOR_SEARCH, "Search vectors",
				"Search records in a vector database.", true));
		definitions.add(vectorDefinition(AutomationNodeType.VECTOR_ADD, "Add vector documents",
				"Add documents to a vector database.", false));
		definitions.add(vectorDefinition(AutomationNodeType.VECTOR_DELETE, "Delete vector documents",
				"Delete documents from a vector database.", false));

		definitions.add(definition(AutomationNodeType.FUNCTION_EXECUTE, "Execute function",
				"Call a reusable function engine.", orderedConfig("engineId", "", "arguments", Map.of()),
				List.of(engineField(IEngine.CATALOG_TYPE.FUNCTION),
						field("arguments", ConfigFieldType.JSON, "Input parameters", true, Map.of())),
				controlInputs(), controlAndResultOutputs()));
		definitions.add(definition(AutomationNodeType.APP_PIXEL, "Run an app action",
				"Run a static Pixel action in an optional app context.", orderedConfig("appId", "", "pixel", ""),
				List.of(field("appId", ConfigFieldType.STRING, "App", false, ""),
						field("pixel", ConfigFieldType.CODE, "Pixel", true, "")),
				controlInputs(), controlAndResultOutputs()));
		definitions.add(definition(AutomationNodeType.AGENT_RUN, "Run agent",
				"Run a configured SEMOSS agent with a prompt.",
				orderedConfig("workspaceId", "", "engineId", "", "command", ""),
				List.of(field("workspaceId", ConfigFieldType.STRING, "Agent", true, ""),
						engineField(IEngine.CATALOG_TYPE.MODEL),
						field("command", ConfigFieldType.TEXT, "Instruction", true, "")),
				controlInputs(), controlAndResultOutputs()));

		definitions.add(definition(AutomationNodeType.CONTROL_WAIT, "Wait",
				"Pause the automation before continuing.", Map.of("durationSeconds", 5),
				List.of(boundedIntegerField("durationSeconds", "Wait for (seconds)", 5,
						AutomationConstants.WAIT_MIN_SECONDS, AutomationConstants.WAIT_MAX_SECONDS)),
				controlInputs(), List.of(CONTROL_OUTPUT)));
		definitions.add(definition(AutomationNodeType.CONTROL_IF, "Decision",
				"Evaluate ordered conditions and route to the first matching path.",
				Map.of("clauses", List.of(Map.of("id", "initial", "condition", ""))),
				List.of(field("clauses", ConfigFieldType.BRANCH_CLAUSES, "Conditions", true, List.of())),
				controlInputs(), List.of(
						port("case:<clause-id>", "Condition", PortKind.CONTROL, PortDirection.OUTPUT, null),
						port("else", "Else", PortKind.CONTROL, PortDirection.OUTPUT, null))));
		definitions.add(new AutomationNodeDefinition(AutomationNodeType.DEVELOPER_PYTHON, "Python",
				"Run custom Python for advanced transformations.", AutomationConstants.NODE_CODE_MODE_CUSTOM,
				Map.of(), List.of(), controlInputs(), controlAndResultOutputs()));
		return Collections.unmodifiableList(definitions);
	}

	private static AutomationNodeDefinition databaseWriteDefinition(AutomationNodeType nodeType, String label,
			String description) {
		return definition(nodeType, label, description,
				orderedConfig("engineId", "", "query", ""),
				List.of(engineField(IEngine.CATALOG_TYPE.DATABASE),
						field("query", ConfigFieldType.SQL, "SQL", true, "")),
				controlInputs(), List.of(CONTROL_OUTPUT));
	}

	private static AutomationNodeDefinition storageDefinition(AutomationNodeType nodeType, String label,
			String description, boolean pathRequired, boolean destinationRequired) {
		Map<String, Object> defaultConfig = destinationRequired
				? orderedConfig("engineId", "", "path", "", "destination", "")
				: orderedConfig("engineId", "", "path", "");
		List<ConfigField> fields = new ArrayList<>();
		fields.add(engineField(IEngine.CATALOG_TYPE.STORAGE));
		fields.add(field("path", ConfigFieldType.STRING, "Storage path", pathRequired, ""));
		if (destinationRequired) {
			fields.add(field("destination", ConfigFieldType.STRING, "Destination", true, ""));
		}
		return definition(nodeType, label, description, defaultConfig, fields, controlInputs(),
				controlAndResultOutputs());
	}

	private static AutomationNodeDefinition vectorDefinition(AutomationNodeType nodeType, String label,
			String description, boolean includeLimit) {
		Map<String, Object> defaultConfig = includeLimit
				? orderedConfig("engineId", "", "value", "", "limit", AutomationConstants.DEFAULT_VECTOR_SEARCH_LIMIT)
				: orderedConfig("engineId", "", "value", "");
		List<ConfigField> fields = new ArrayList<>();
		fields.add(engineField(IEngine.CATALOG_TYPE.VECTOR));
		fields.add(field("value", ConfigFieldType.TEXT, includeLimit ? "Search query" : "Records", true, ""));
		if (includeLimit) {
			fields.add(boundedIntegerField("limit", "Result limit", AutomationConstants.DEFAULT_VECTOR_SEARCH_LIMIT,
					1, null));
		}
		return definition(nodeType, label, description, defaultConfig, fields, controlInputs(),
				controlAndResultOutputs());
	}

	private static AutomationNodeDefinition definition(AutomationNodeType nodeType, String label, String description,
			Map<String, Object> defaultConfig, List<ConfigField> fields, List<Port> inputs, List<Port> outputs) {
		return new AutomationNodeDefinition(nodeType, label, description,
				AutomationConstants.NODE_CODE_MODE_GENERATED, defaultConfig, fields, inputs, outputs);
	}

	private static ConfigField engineField(IEngine.CATALOG_TYPE engineType) {
		return new ConfigField("engineId", ConfigFieldType.ENGINE, "Engine", true, "", null, null, engineType);
	}

	private static ConfigField field(String key, ConfigFieldType type, String label, boolean required,
			Object defaultValue) {
		return new ConfigField(key, type, label, required, defaultValue, null, null, null);
	}

	private static ConfigField boundedIntegerField(String key, String label, int defaultValue, Integer minimum,
			Integer maximum) {
		return new ConfigField(key, ConfigFieldType.INTEGER, label, true, defaultValue, minimum, maximum, null);
	}

	private static Port port(String id, String label, PortKind kind, PortDirection direction, String dataType) {
		return new Port(id, label, kind, direction, dataType);
	}

	private static List<Port> controlInputs() {
		return List.of(CONTROL_INPUT);
	}

	private static List<Port> controlAndResultOutputs() {
		return List.of(CONTROL_OUTPUT, RESULT_OUTPUT);
	}

	private static Map<AutomationNodeType, AutomationNodeDefinition> indexDefinitions() {
		Map<AutomationNodeType, AutomationNodeDefinition> definitionsByType = new LinkedHashMap<>();
		for (AutomationNodeDefinition definition : DEFINITIONS) {
			AutomationNodeDefinition previous = definitionsByType.put(definition.nodeType(), definition);
			if (previous != null) {
				throw new IllegalStateException("Duplicate Automation node definition for "
						+ definition.nodeType().getType());
			}
		}
		if (definitionsByType.size() != AutomationNodeType.values().length) {
			throw new IllegalStateException("Every Automation node type must have one authoring definition.");
		}
		return Collections.unmodifiableMap(definitionsByType);
	}

	private static Map<String, Object> orderedConfig(Object... values) {
		Map<String, Object> config = new LinkedHashMap<>();
		for (int index = 0; index < values.length; index += 2) {
			config.put((String) values[index], values[index + 1]);
		}
		return config;
	}
}
