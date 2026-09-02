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
import prerna.reactor.project.GetProjectAvailableReactorsReactor;
import prerna.reactor.project.GetProjectReactorSignatureReactor;
import prerna.reactor.project.MyProjectsReactor;
import prerna.reactor.security.MyEnginesReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Maintains the generated, project-scoped MCP tools used by Automation Workspace chat.
 *
 * <p>
 * Synchronization replaces only tools stamped with this generator's ID. User-authored MCP tools
 * remain untouched, and all generated tools bind authoring operations to the Automation project
 * supplied by the caller.
 */
public final class AutomationMcpSync {

	private static final Logger classLogger = LogManager.getLogger(AutomationMcpSync.class);
	private static final String GENERATOR_ID = "AutomationMCP";

	private AutomationMcpSync() {
	}

	/**
	 * Rebuilds the managed Automation MCP tool set from the persisted graph.
	 *
	 * @param projectId Automation project identifier
	 * @param definitionJson persisted graph definition
	 * @param user user whose accessible engines and projects shape authoring guidance
	 */
	public static void sync(String projectId, String definitionJson, User user) {
		IProject project = Utility.getProject(projectId);
		if (project == null || definitionJson == null || definitionJson.isBlank()) {
			return;
		}
		try {
			JSONArray generated = new JSONArray()
					.put(myEnginesTool())
					.put(myProjectsTool())
					.put(projectReactorsTool())
					.put(projectReactorSignatureTool())
					.put(getAutomationTool(projectId))
					.put(triggerTool(projectId, definitionJson))
					.put(addStepTool(projectId))
					.put(updateStepTool(projectId))
					.put(updateCustomStepTool(projectId))
					.put(removeStepTool(projectId));
			MCPUtility.stampGenerator(generated, GENERATOR_ID);

			Path output = Path.of(AssetUtility.getProjectAssetsFolder(projectId), "mcp", "pixel_mcp.json");
			JSONArray merged = MCPUtility.mergeGeneratedTools(
					MCPUtility.readMcpJson(output.toString()), generated, GENERATOR_ID, true);
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
		engineTypes.put("items", new JSONObject().put("type", "string").put("enum", new JSONArray()
				.put(IEngine.CATALOG_TYPE.DATABASE.name())
				.put(IEngine.CATALOG_TYPE.MODEL.name())
				.put(IEngine.CATALOG_TYPE.STORAGE.name())
				.put(IEngine.CATALOG_TYPE.VECTOR.name())
				.put(IEngine.CATALOG_TYPE.FUNCTION.name())));
		engineTypes.put("description", "Optional engine catalog types. Filter to the node type being configured and "
				+ "use the returned engine_id exactly; never invent or normalize an engine name.");

		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.FILTER_WORD.getKey(), new JSONObject().put("type", "string")
				.put("description", "Optional search text for an engine name or ID."));
		properties.put(ReactorKeysEnum.ENGINE_TYPE.getKey(), engineTypes);
		properties.put(ReactorKeysEnum.LIMIT.getKey(), new JSONObject().put("type", "integer")
				.put("minimum", 1).put("description", "Optional maximum number of engines to return."));
		properties.put(ReactorKeysEnum.OFFSET.getKey(), new JSONObject().put("type", "integer")
				.put("minimum", 0).put("description", "Optional result offset."));

		JSONObject inputSchema = tool.getJSONObject("inputSchema");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray());
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject myProjectsTool() {
		JSONObject tool = new MyProjectsReactor().asMcpTool();
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.FILTER_WORD.getKey(), new JSONObject().put("type", "string")
				.put("description", "Optional search text for an app or agent name or ID."));
		properties.put(ReactorKeysEnum.PROJECT_TYPE.getKey(), new JSONObject().put("type", "array")
				.put("items", new JSONObject().put("type", "string")
						.put("enum", new JSONArray()
								.put(IProject.PROJECT_TYPE.CODE.name())
								.put(IProject.PROJECT_TYPE.BLOCKS.name())
								.put(IProject.PROJECT_TYPE.WORKSPACE.name())))
				.put("minItems", 1).put("maxItems", 2).put("uniqueItems", true)
				.put("description", "Required catalog filter. Pass ['WORKSPACE'] for agents or "
						+ "['CODE','BLOCKS'] for apps."));
		properties.put(ReactorKeysEnum.LIMIT.getKey(), new JSONObject().put("type", "integer")
				.put("minimum", 1).put("description", "Optional maximum number of accessible projects to return."));
		properties.put(ReactorKeysEnum.OFFSET.getKey(), new JSONObject().put("type", "integer")
				.put("minimum", 0).put("description", "Optional result offset."));

		tool.put("title", "My Projects");
		tool.put("description", "List apps or agents the current user can access. Call with "
				+ "projectType=['WORKSPACE'] for agent.run or projectType=['CODE','BLOCKS'] for app.pixel. "
				+ "Use a returned project_id exactly; never invent or normalize a project ID.");
		JSONObject inputSchema = tool.getJSONObject("inputSchema");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT_TYPE.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject projectReactorsTool() {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), stringProperty(
				"Exact CODE or BLOCKS project_id returned by MyProjects."));
		JSONObject tool = tool(new GetProjectAvailableReactorsReactor().asMcpTool().getString("name"),
				"Get Project Available Reactors",
				"List the exact custom reactor names available in an accessible app. Call this after MyProjects "
						+ "and before authoring app.pixel; never guess a reactor name or its capitalization.",
				properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject projectReactorSignatureTool() {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), stringProperty(
				"Exact CODE or BLOCKS project_id returned by MyProjects."));
		properties.put("reactor", stringProperty(
				"Exact case-sensitive reactor name returned by GetProjectAvailableReactors."));
		JSONObject tool = tool(new GetProjectReactorSignatureReactor().asMcpTool().getString("name"),
				"Get Project Reactor Signature",
				"Return the required parameters and executable template for one accessible app reactor. Call this "
						+ "before authoring app.pixel and use its exact template with concrete values. Generated app.pixel "
						+ "does not accept ${...} placeholders; use developer.python with Insight().run_pixel(...) only "
						+ "when runtime values require a dynamic Pixel expression, reading those values from scope.",
				properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("reactor"));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject getAutomationTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		JSONObject tool = tool("GetAutomation", "Get Automation Definition",
				"Read this project's current graph, node IDs, configuration, node sources, sourceHashes, globals, "
						+ "and revision. "
						+ "Always call this before deciding how to add, update, or remove a step. This tool only "
						+ "inspects the automation; it does not run it.",
				properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		return tool;
	}

	private static JSONObject triggerTool(String projectId, String definitionJson) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		JSONObject inputs = new JSONObject()
				.put("type", "object")
				.put("description", "Optional values overriding globals declared by trigger Python.");
		JSONObject inputProperties = new JSONObject();
		for (Map<String, Object> global : triggerGlobalDefinitions(projectId)) {
			String name = (String) global.get("name");
			Object defaultValue = global.get(AutomationConstants.CONFIG_DEFAULT_VALUE);
			Object rawDescription = global.get(AutomationConstants.CONFIG_DESCRIPTION);
			JSONObject property = new JSONObject()
					.put("description", rawDescription instanceof String description && !description.isBlank()
							? description
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
				description(definitionJson) + " Run this project only when the user explicitly asks to execute it. "
						+ "Never use this tool to inspect or edit the automation; call GetAutomation instead.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		JSONObject ui = new JSONObject();
		ui.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		ui.put(MCPUtility.UI_RESOURCE_URI, "system://automation-workspace/?readOnly=1&mode=trigger");
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_UI, ui);
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		return tool;
	}

	private static JSONObject addStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeType", nodeTypeProperty());
		properties.put("config", stringProperty("JSON configuration. Before configuring any engine-backed node, call "
				+ "MyEngines filtered to the required engine type and use the returned engine_id exactly. Never invent, "
				+ "shorten, or normalize an engine name. database.query, database.insert, and "
				+ "database.update require engineId and query. model.chat requires engineId and prompt; "
				+ "optionally systemPrompt and paramValues as a JSON object or valid JSON-object string. "
				+ "model.embeddings requires engineId and text. "
				+ "model.ner requires engineId, text, and entities as a non-empty JSON array of strings. "
				+ "storage nodes require engineId and path; upload/download also require destination. "
				+ "vector nodes require engineId and value; vector.search may include limit. "
				+ "function.execute requires engineId and arguments as a JSON object or valid JSON-object string. "
				+ "For app.pixel, call MyProjects with "
				+ "projectType=['CODE','BLOCKS'], use its project_id exactly as appId, then call "
				+ "GetProjectAvailableReactors and GetProjectReactorSignature; pixel must use the exact reactor "
				+ "name and every required argument from the returned template, with concrete values only. "
				+ "Generated app.pixel rejects ${...} placeholders. Do not put APP or LoadApp inside pixel because "
				+ "appId owns the scoped app context. Use developer.python with Insight().run_pixel(...) only when "
				+ "runtime values require a dynamic Pixel expression. "
				+ "Before configuring agent.run, call MyProjects with projectType=['WORKSPACE'] and use a returned "
				+ "project_id exactly as workspaceId; also call MyEngines filtered to MODEL and use its engine_id. "
				+ "agent.run requires workspaceId, engineId, and command; the runtime creates its room. It supports "
				+ "harnessType, "
				+ "maxTurns, maxReflections, waitTimeoutMs, paramValues, and agentParams; it always waits "
				+ "for a durable terminal or input-required status before continuing. "
				+ "Generated model.chat and model.vision outputs are response content strings; model.embeddings "
				+ "outputs the response vectors; model.ner outputs the response result map; agent.run outputs "
				+ "finalText. Room, message, and agent-run identifiers are retained separately in run trace. "
				+ "Within accepted configuration objects, ${...} placeholders resolve recursively in object values "
				+ "and array items; object keys remain literal. A JSON-string configuration must contain a valid JSON "
				+ "object with placeholders kept as quoted string values. Prefer native JSON shapes when preserving "
				+ "typed values. "
				+ "control.wait requires durationSeconds. "
				+ "control.if requires an ordered clauses array shaped as "
				+ "[{\"id\":\"stable-id\",\"condition\":\"${prior_output} == true\"}]. "
				+ "The first matching clause is selected; else is the fallback. "
				+ "developer.python is only for an external integration "
				+ "that no supported engine node can perform or for dynamic Pixel that generated app.pixel rejects; "
				+ "it requires config.source defining run(scope). scope is a read-only, run-local mapping of "
				+ "trigger inputs, globals, "
				+ "metadata, and prior outputs keyed by outputVar. Custom Python reads it directly, for example "
				+ "scope['prior_output']; ${...} is not resolved in custom source. Never use "
				+ "developer.python to invoke a SEMOSS agent or emulate agent.run. "
				+ "Use ${prior_output} to reference an upstream output only in supported configuration fields; "
				+ "generated database queries and app.pixel expressions reject placeholders. Field values are "
				+ "executable configuration, "
				+ "not AI suggestions: use the user-requested intent to write the concrete query, prompt, path, "
				+ "or arguments."));
		properties.put("label", stringProperty("Short user-facing action label."));
		properties.put("outputVar", stringProperty("Required unique Python-style variable name for this node's "
				+ "business output. Omit it for control.if, which does not produce an output."));
		properties.put("afterNodeId", stringProperty("Optional existing node ID after which to insert this node."));
		properties.put("branchPort", stringProperty("Required only when afterNodeId identifies a control.if node. "
				+ "Use 'case:<clause-id>' for one of that node's configured clauses or 'else' for its fallback. "
				+ "Omit it for every other parent node."));
		return tool("AddAutomationStep", "Add Automation Step",
				"Call GetAutomation first, then add one validated action from the user's chat request. "
						+ "Prefer an engine-backed node whenever it "
						+ "supports the task. App reactor work with concrete arguments must use app.pixel with a MyProjects "
						+ "result and inspected reactor signature. Dynamic Pixel values require explicit developer.python "
						+ "source using Insight().run_pixel(...). Agent work must use agent.run with a MyProjects result; "
						+ "never create a "
						+ "Python agent client or workspace wrapper. Use developer.python only for an unavailable "
						+ "external integration or an explicitly dynamic Pixel expression. Custom Python must read "
						+ "upstream values directly from its scope argument; ${...} is not an upstream reference there. "
						+ "Use direct, executable configuration rather than leaving a natural-language placeholder.",
				properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeType").put("config")
						.put("label"));
	}

	private static JSONObject updateCustomStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing node with codeMode custom."));
		properties.put("source", stringProperty("Complete Python source defining run(scope). Read upstream values "
				+ "from scope, for example scope['prior_output']; ${...} is not resolved in custom source."));
		properties.put("expectedSourceHash", stringProperty(
				"Exact sourceHashes[nodeId] value returned by the latest GetAutomation call."));
		return tool("UpdateAutomationCustomStep", "Update Custom Automation Step",
				"Call GetAutomation first. Replace source for an explicitly custom node only when its current "
						+ "hash matches. Custom source must read upstream values directly from scope; ${...} is not "
						+ "resolved there.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId").put("source")
						.put("expectedSourceHash"));
	}

	private static JSONObject updateStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing generated node ID."));
		properties.put("config", stringProperty("Complete replacement JSON configuration for the node. For an "
				+ "engine-backed node, call MyEngines and use the returned engine_id exactly. For agent.run, also "
				+ "call MyProjects with projectType=['WORKSPACE'] and use a returned project_id as workspaceId. "
				+ "Generated model nodes expose their response business value and agent.run exposes finalText; "
				+ "do not configure downstream nodes to parse their transport envelopes. "
				+ "For app.pixel, use an appId from MyProjects projectType=['CODE','BLOCKS'] and a pixel template "
				+ "from GetProjectReactorSignature with concrete values; ${...} placeholders are rejected. "
				+ "Within accepted configuration objects, placeholders resolve recursively in object values and "
				+ "array items; object keys remain literal. JSON-string configurations must contain a valid JSON "
				+ "object with placeholders as quoted string values."));
		properties.put("label", stringProperty("Optional replacement user-facing label."));
		return tool("UpdateAutomationStep", "Update Automation Step",
				"Call GetAutomation first. Apply the user's requested change to one generated node's direct "
						+ "configuration and regenerate its "
						+ "managed Python source. Preserve unaffected configuration fields.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId").put("config"));
	}

	private static JSONObject removeStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing non-trigger node ID to remove."));
		JSONObject tool = tool("RemoveAutomationStep", "Remove Automation Step",
				"Call GetAutomation first. Remove one action only when the user requested its removal. "
						+ "The operation rejects nodes whose "
						+ "output is still referenced and reconnects an unambiguous sequential control path.",
				properties, new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId"));
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		return tool;
	}

	private static JSONObject tool(String name, String title, String description, JSONObject properties,
			JSONArray required) {
		JSONObject result = new JSONObject();
		result.put("name", name);
		result.put("title", title);
		result.put("description", description);
		result.put("inputSchema", new JSONObject().put("type", "object")
				.put("title", name + "_Arguments").put("properties", properties).put("required", required));
		result.put("_meta", new JSONObject().put(MCPUtility.SMSS_FUNCTION_NAME, name)
				.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue()));
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
		JSONArray values = new JSONArray()
				.put(AutomationConstants.NODE_DATABASE_QUERY)
				.put(AutomationConstants.NODE_DATABASE_INSERT)
				.put(AutomationConstants.NODE_DATABASE_UPDATE)
				.put(AutomationConstants.NODE_MODEL_CHAT)
				.put(AutomationConstants.NODE_MODEL_EMBEDDINGS)
				.put(AutomationConstants.NODE_MODEL_VISION)
				.put(AutomationConstants.NODE_MODEL_NER)
				.put(AutomationConstants.NODE_STORAGE_LIST)
				.put(AutomationConstants.NODE_STORAGE_READ)
				.put(AutomationConstants.NODE_STORAGE_UPLOAD)
				.put(AutomationConstants.NODE_STORAGE_DOWNLOAD)
				.put(AutomationConstants.NODE_STORAGE_DELETE)
				.put(AutomationConstants.NODE_VECTOR_SEARCH)
				.put(AutomationConstants.NODE_VECTOR_ADD)
				.put(AutomationConstants.NODE_VECTOR_DELETE)
				.put(AutomationConstants.NODE_FUNCTION_EXECUTE)
				.put(AutomationConstants.NODE_APP_PIXEL)
				.put(AutomationConstants.NODE_AGENT_RUN)
				.put(AutomationConstants.NODE_CONTROL_WAIT)
				.put(AutomationConstants.NODE_CONTROL_IF)
				.put(AutomationConstants.NODE_DEVELOPER_PYTHON);
		return stringProperty("The typed action to add. control.if is a standalone branch node with an ordered "
				+ "config.clauses array; add each case child and the final else child in later calls using "
				+ "afterNodeId and branchPort.").put("enum", values);
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
					AutomationDefinitionValidator.parseAndValidateForAuthoring(files.definition()), files.nodeSources());
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
