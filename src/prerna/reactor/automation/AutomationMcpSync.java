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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.reactor.function.GetFunctionEngineDefinitionReactor;
import prerna.reactor.project.GetProjectAvailableReactorsReactor;
import prerna.reactor.project.GetProjectReactorSignatureReactor;
import prerna.reactor.project.MyProjectsReactor;
import prerna.reactor.security.MyEnginesReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Maintains the generated, project-scoped MCP tools used by Automation
 * Workspace chat.
 *
 * <p>
 * Synchronization replaces only tools stamped with this generator's ID.
 * User-authored MCP tools remain untouched, and all generated tools bind
 * authoring operations to the Automation project supplied by the caller.
 */
public final class AutomationMcpSync {

	private static final Logger classLogger = LogManager.getLogger(AutomationMcpSync.class);
	private static final String GENERATOR_ID = "AutomationMCP";

	private AutomationMcpSync() {

	}

	/**
	 * Rebuilds the managed Automation MCP tool set from the persisted graph.
	 *
	 * @param projectId      Automation project identifier
	 * @param definitionJson persisted graph definition
	 * @param user           user whose accessible engines and projects shape
	 *                       authoring guidance
	 */
	public static void sync(String projectId, String definitionJson, User user) {
		IProject project = Utility.getProject(projectId);
		if (project == null || definitionJson == null || definitionJson.isBlank()) {
			return;
		}
		try {
			// @formatter:off
			JSONArray generated = new JSONArray()
					.put(myEnginesTool())
					.put(functionDefinitionTool())
					.put(myProjectsTool())
					.put(projectReactorsTool())
					.put(projectReactorSignatureTool())
					.put(getAutomationTool(projectId))
					.put(nodeDefinitionsTool())
					.put(triggerTool(projectId, definitionJson))
					.put(addStepTool(projectId))
					.put(updateStepTool(projectId))
					.put(updateCustomStepTool(projectId))
					.put(removeStepTool(projectId));
			// @formatter:on
			MCPUtility.stampGenerator(generated, GENERATOR_ID);

			Path output = Path.of(AssetUtility.getProjectAssetsFolder(projectId), "mcp", "pixel_mcp.json");
			JSONArray merged = MCPUtility.mergeGeneratedTools(MCPUtility.readMcpJson(output.toString()), generated,
					GENERATOR_ID, true);
			Files.createDirectories(output.getParent());
			JSONObject document = new JSONObject();
			document.put("tools", merged);
			document.put("_meta", new JSONObject().put("last_modified_date",
					LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE)));
			Files.writeString(output, document.toString(4), StandardCharsets.UTF_8);
			MCPUtility.addMCPTag(project);
		} catch (Exception e) {
			classLogger.warn("Unable to synchronize automation MCP tools for project {}", projectId, e);
		}
	}

	private static JSONObject myEnginesTool() {
		JSONObject tool = new MyEnginesReactor().asMcpTool();
		JSONObject engineTypes = new JSONObject().put("type", "array");
		engineTypes.put("items",
				new JSONObject().put("type", "string").put("enum",
						new JSONArray().put(IEngine.CATALOG_TYPE.DATABASE.name()).put(IEngine.CATALOG_TYPE.MODEL.name())
								.put(IEngine.CATALOG_TYPE.STORAGE.name()).put(IEngine.CATALOG_TYPE.VECTOR.name())
								.put(IEngine.CATALOG_TYPE.FUNCTION.name())));
		engineTypes.put("description", """
				Optional engine catalog types. Filter to the node type being configured and use the \
				returned engine_id exactly; never invent or normalize an engine name.""");

		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.FILTER_WORD.getKey(), new JSONObject().put("type", "string").put("description",
				"Optional search text for an engine name or ID."));
		properties.put(ReactorKeysEnum.ENGINE_TYPE.getKey(), engineTypes);
		properties.put(ReactorKeysEnum.LIMIT.getKey(), new JSONObject().put("type", "integer").put("minimum", 1)
				.put("description", "Optional maximum number of engines to return."));
		properties.put(ReactorKeysEnum.OFFSET.getKey(), new JSONObject().put("type", "integer").put("minimum", 0)
				.put("description", "Optional result offset."));

		JSONObject inputSchema = tool.getJSONObject("inputSchema");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray());
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject myProjectsTool() {
		JSONObject tool = new MyProjectsReactor().asMcpTool();
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.FILTER_WORD.getKey(), new JSONObject().put("type", "string").put("description",
				"Optional search text for an app or agent name or ID."));
		properties.put(ReactorKeysEnum.PROJECT_TYPE.getKey(), new JSONObject().put("type", "array")
				.put("items", new JSONObject().put("type", "string").put("enum",
						new JSONArray().put(IProject.PROJECT_TYPE.CODE.name()).put(IProject.PROJECT_TYPE.BLOCKS.name())
								.put(IProject.PROJECT_TYPE.WORKSPACE.name())))
				.put("minItems", 1).put("maxItems", 2).put("uniqueItems", true).put("description",
						"Required catalog filter. Pass ['WORKSPACE'] for agents or ['CODE','BLOCKS'] for apps."));
		properties.put(ReactorKeysEnum.LIMIT.getKey(), new JSONObject().put("type", "integer").put("minimum", 1)
				.put("description", "Optional maximum number of accessible projects to return."));
		properties.put(ReactorKeysEnum.OFFSET.getKey(), new JSONObject().put("type", "integer").put("minimum", 0)
				.put("description", "Optional result offset."));

		tool.put("title", "My Projects");
		tool.put("description", """
				List apps or agents the current user can access. Call with projectType=['WORKSPACE'] for \
				agent.run or projectType=['CODE','BLOCKS'] for app.pixel. Use a returned project_id \
				exactly; never invent or normalize a project ID.""");
		JSONObject inputSchema = tool.getJSONObject("inputSchema");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT_TYPE.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject functionDefinitionTool() {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.ENGINE.getKey(),
				stringProperty("Exact FUNCTION engine_id returned by MyEngines."));
		JSONObject tool = tool(new GetFunctionEngineDefinitionReactor().asMcpTool().getString("name"),
				"Get Function Engine Definition", """
						Return the declared parameters and required parameters for one accessible function \
						engine. Call this after MyEngines and before authoring function.execute. Use the \
						returned parameter names exactly as keys in config.arguments and include every \
						required parameter.""", properties, new JSONArray().put(ReactorKeysEnum.ENGINE.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject projectReactorsTool() {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(),
				stringProperty("Exact CODE or BLOCKS project_id returned by MyProjects."));
		JSONObject tool = tool(new GetProjectAvailableReactorsReactor().asMcpTool().getString("name"),
				"Get Project Available Reactors", """
						List the exact custom reactor names available in an accessible app. Call this after \
						MyProjects and before authoring app.pixel; never guess a reactor name or its \
						capitalization.""", properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject projectReactorSignatureTool() {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(),
				stringProperty("Exact CODE or BLOCKS project_id returned by MyProjects."));
		properties.put("reactor",
				stringProperty("Exact case-sensitive reactor name returned by GetProjectAvailableReactors."));
		JSONObject tool = tool(new GetProjectReactorSignatureReactor().asMcpTool().getString("name"),
				"Get Project Reactor Signature", """
						Return the required parameters and executable template for one accessible app \
						reactor. Call this before authoring app.pixel and use its exact template with \
						concrete values. Generated app.pixel does not accept ${...} placeholders; use \
						developer.python with Insight().run_pixel(...) only when runtime values require a \
						dynamic Pixel expression, reading those values from scope.""", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("reactor"));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject getAutomationTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		JSONObject tool = tool("GetAutomation", "Get Automation Definition", """
				Read this project's current graph, node IDs, configuration, node sources, sourceHashes, \
				globals, and revision. Always call this before deciding how to add, update, or remove a \
				step. This tool only inspects the automation; it does not run it.""", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	/**
	 * Returns the backend-owned node catalog used by both authoring clients and
	 * graph validation.
	 */
	private static JSONObject nodeDefinitionsTool() {
		JSONObject tool = tool(new GetAutomationNodeDefinitionsReactor().asMcpTool().getString("name"),
				"Get Automation Node Definitions", """
						Return the versioned backend-owned definitions for every supported Automation node \
						type, including configuration fields, defaults, engine category, ports, and \
						capabilities. Call this before adding a node and treat it as the schema source of \
						truth.""", new JSONObject(), new JSONArray());
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject triggerTool(String projectId, String definitionJson) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		JSONObject inputs = new JSONObject().put("type", "object").put("description",
				"Optional values overriding globals declared by trigger Python.");
		JSONObject inputProperties = new JSONObject();
		for (Map<String, Object> global : triggerGlobalDefinitions(projectId)) {
			String name = (String) global.get("name");
			Object defaultValue = global.get(AutomationConstants.CONFIG_DEFAULT_VALUE);
			Object rawDescription = global.get(AutomationConstants.CONFIG_DESCRIPTION);
			JSONObject property = new JSONObject().put("description",
					rawDescription instanceof String description && !description.isBlank() ? description
							: "Override trigger global '" + name + "'.");
			property.put("default", defaultValue != null ? defaultValue : JSONObject.NULL);
			property.put("type", jsonType(defaultValue));
			inputProperties.put(name, property);
		}
		if (!inputProperties.isEmpty()) {
			inputs.put("properties", inputProperties);
		}
		properties.put(AutomationConstants.AUTOMATION_INPUTS_KEY, inputs);
		JSONObject tool = tool("TriggerAutomation", "Trigger Automation",
				"""
						%s Run this project only when the user explicitly asks to execute it. Never use this tool \
						to inspect or edit the automation; call GetAutomation instead."""
						.formatted(description(definitionJson)),
				properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		JSONObject ui = new JSONObject();
		ui.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		ui.put(MCPUtility.UI_RESOURCE_URI, "system://automation/?readOnly=1&mode=trigger");
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_UI, ui);
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		return tool;
	}

	private static JSONObject addStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeType", nodeTypeProperty());
		properties.put("config", stringProperty("""
				Complete JSON configuration matching the selected node type's fields from \
				GetAutomationNodeDefinitions. Discover and use exact engine, project, reactor, and function \
				identifiers before supplying resource-backed fields."""));
		properties.put("label", stringProperty("Short user-facing action label."));
		properties.put("outputVar", stringProperty("""
				Required unique Python-style variable name for this node's business output. Omit it for \
				control.if and control.jev, which route control flow instead of producing an output."""));
		properties.put("afterNodeId", stringProperty("Optional existing node ID after which to insert this node."));
		properties.put("branchPort", stringProperty("""
				Required only when afterNodeId identifies a routing node. Use 'case:<clause-id>' for one \
				of that node's configured clauses or 'else' for its fallback. Omit it for every other \
				parent node."""));
		return tool("AddAutomationStep", "Add Automation Step", """
				Call GetAutomation and GetAutomationNodeDefinitions first, then add one server-validated \
				action. Use direct executable configuration and preserve the existing graph's control-flow \
				intent.""", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeType").put("config").put("label"));
	}

	private static JSONObject updateCustomStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing node with codeMode custom."));
		properties.put("source", stringProperty("""
				Complete Python source defining run(scope). Read upstream values from scope, for example \
				scope['prior_output']; ${...} is not resolved in custom source."""));
		properties.put("expectedSourceHash",
				stringProperty("Exact sourceHashes[nodeId] value returned by the latest GetAutomation call."));
		return tool("UpdateAutomationCustomStep", "Update Custom Automation Step", """
				Call GetAutomation first. Replace source for an explicitly custom node only when its \
				current hash matches. Custom source must read upstream values directly from scope; ${...} \
				is not resolved there.""", properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey())
				.put("nodeId").put("source").put("expectedSourceHash"));
	}

	private static JSONObject updateStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing generated node ID."));
		properties.put("config", stringProperty("""
				Complete replacement JSON configuration matching this node type's fields from \
				GetAutomationNodeDefinitions. Preserve unaffected fields and use exact discovered resource \
				identifiers."""));
		properties.put("label", stringProperty("Optional replacement user-facing label."));
		return tool("UpdateAutomationStep", "Update Automation Step", """
				Call GetAutomation and GetAutomationNodeDefinitions first. Apply the requested change to one \
				generated node's direct configuration and regenerate its managed Python source. Preserve \
				unaffected configuration fields.""", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId").put("config"));
	}

	private static JSONObject removeStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing non-trigger node ID to remove."));
		JSONObject tool = tool("RemoveAutomationStep", "Remove Automation Step", """
				Call GetAutomation first. Remove one action only when the user requested its removal. The \
				operation rejects nodes whose output is still referenced and reconnects an unambiguous \
				sequential control path.""", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId"));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		return tool;
	}

	private static JSONObject tool(String name, String title, String description, JSONObject properties,
			JSONArray required) {
		JSONObject result = new JSONObject();
		result.put("name", name);
		result.put("title", title);
		result.put("description", description);
		result.put("inputSchema", new JSONObject().put("type", "object").put("title", name + "_Arguments")
				.put("properties", properties).put("required", required));
		result.put("_meta", new JSONObject().put(MCPUtility.SMSS_FUNCTION_NAME, name).put(MCPUtility.SMSS_MCP_EXECUTION,
				MCPExecution.ASK.getValue()));
		return result;
	}

	private static JSONObject fixedProject(String projectId) {
		return new JSONObject().put("type", "string").put("default", projectId).put("const", projectId)
				.put("description", "The automation project ID.");
	}

	private static JSONObject stringProperty(String description) {
		return new JSONObject().put("type", "string").put("description", description);
	}

	private static JSONObject nodeTypeProperty() {
		JSONArray values = new JSONArray();
		for (AutomationNodeDefinition definition : AutomationNodeCatalog.getDefinitions()) {
			if (definition.nodeType() != AutomationNodeType.TRIGGER_START) {
				values.put(definition.nodeType().getType());
			}
		}
		return stringProperty("""
				The typed action to add. control.if and control.jev are standalone routing nodes with an ordered \
				config.clauses array. control.jev requires a TYPESAFE engine. Use questionType=choice for arbitrary described routes, or questionType=noul for exactly two described routes with explicit boolean answer values. Route edges use case:<stable-route-id>; the fallback edge uses else. Add each case child and the final else child in later calls using \
				afterNodeId and branchPort.""").put("enum", values);
	}

	private static String description(String definitionJson) {
		try {
			String value = new JSONObject(definitionJson).optString(AutomationConstants.DOC_DESCRIPTION, "").trim();
			return value.isEmpty() ? "Automation workflow." : value;
		} catch (Exception e) {
			return "Automation workflow.";
		}
	}

	private static List<Map<String, Object>> triggerGlobalDefinitions(String projectId) {
		try {
			AutomationDefinitionService.DefinitionFiles files = AutomationDefinitionService.load(projectId);
			return AutomationRuntime.triggerGlobalDefinitions(
					AutomationDefinitionValidator.parseAndValidateForAuthoring(files.definition()));
		} catch (RuntimeException e) {
			classLogger.warn("Unable to read trigger globals for automation project {}", projectId, e);
			return List.of();
		}
	}

	private static String jsonType(Object value) {
		if (value instanceof Boolean) {
			return "boolean";
		}
		if (value instanceof Number) {
			return "number";
		}
		if (value instanceof List<?>) {
			return "array";
		}
		if (value instanceof Map<?, ?>) {
			return "object";
		}
		return "string";
	}
}
