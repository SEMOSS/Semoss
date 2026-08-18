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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.reactor.automation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Maintains the project-scoped MCP tools used by the Automation Workspace chat. */
public final class AutomationMcpSync {

	private static final Logger classLogger = LogManager.getLogger(AutomationMcpSync.class);
	private static final String GENERATOR_ID = "AutomationMCP";

	private AutomationMcpSync() {
	}

	public static void sync(String projectId, String definitionJson, User user) {
		IProject project = Utility.getProject(projectId);
		if (project == null || definitionJson == null || definitionJson.isBlank()) {
			return;
		}
		try {
			JSONArray generated = new JSONArray()
					.put(triggerTool(projectId, definitionJson))
					.put(addStepTool(projectId))
					.put(updateStepTool(projectId))
					.put(updateCustomStepTool(projectId));
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
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to synchronize automation MCP tools.", e);
		} catch (Exception e) {
			classLogger.warn("Unable to synchronize automation MCP tools for project {}", projectId, e);
		}
	}

	private static JSONObject triggerTool(String projectId, String definitionJson) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put(AutomationConstants.AUTOMATION_INPUTS_KEY, new JSONObject()
				.put("type", "object")
				.put("description", "Optional values for this automation's declared runtime inputs."));
		JSONObject tool = tool("TriggerAutomation", "Trigger Automation",
				description(definitionJson) + " Run this project's automation.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		JSONObject ui = new JSONObject();
		ui.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		ui.put(MCPUtility.UI_RESOURCE_URI, "system://automation-workspace/?readOnly=1");
		tool.getJSONObject("_meta").put(MCPUtility.SMSS_MCP_UI, ui);
		return tool;
	}

	private static JSONObject addStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeType", nodeTypeProperty());
		properties.put("config", stringProperty("JSON configuration. database.query, database.insert, and "
				+ "database.update require engineId and query. model.chat requires engineId and prompt; "
				+ "optionally systemPrompt and paramValues. model.embeddings requires engineId and text. "
				+ "storage nodes require engineId and path; upload/download also require destination. "
				+ "vector nodes require engineId and value; vector.search may include limit. "
				+ "function.execute requires engineId and arguments. app.pixel requires pixel. "
				+ "control.wait requires durationSeconds. developer.python is only for an external integration "
				+ "that no supported engine node can perform; it requires source defining run(scope). "
				+ "Use ${prior_output} to reference an upstream output."));
		properties.put("label", stringProperty("Short user-facing action label."));
		properties.put("outputVar", stringProperty("Unique Python-style variable name for this node's output."));
		properties.put("afterNodeId", stringProperty("Optional existing node ID after which to insert this node."));
		return tool("AddAutomationStep", "Add Automation Step",
				"Add one validated action. Prefer an engine-backed node whenever it supports the task; use "
						+ "developer.python only for an unavailable external integration.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeType").put("config")
						.put("label").put("outputVar"));
	}

	private static JSONObject updateCustomStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing node with codeMode custom."));
		properties.put("source", stringProperty("Complete Python source defining run(scope)."));
		properties.put("expectedSourceHash", stringProperty("SHA-256 hash returned by the prior source read."));
		return tool("UpdateAutomationCustomStep", "Update Custom Automation Step",
				"Replace source for an explicitly custom node only when its current hash matches.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId").put("source")
						.put("expectedSourceHash"));
	}

	private static JSONObject updateStepTool(String projectId) {
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProject(projectId));
		properties.put("nodeId", stringProperty("Existing generated node ID."));
		properties.put("config", stringProperty("Complete replacement JSON configuration for the node."));
		properties.put("label", stringProperty("Optional replacement user-facing label."));
		return tool("UpdateAutomationStep", "Update Automation Step",
				"Update configuration for one generated node and regenerate its managed Python source.", properties,
				new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId").put("config"));
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
				.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue()));
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
				.put(AutomationConstants.NODE_CONTROL_WAIT)
				.put(AutomationConstants.NODE_DEVELOPER_PYTHON);
		return stringProperty("The typed action to add.").put("enum", values);
	}

	private static String description(String definitionJson) {
		try {
			String value = new JSONObject(definitionJson).optString(AutomationConstants.DOC_DESCRIPTION, "").trim();
			return value.isEmpty() ? "Automation workflow." : value;
		} catch (Exception e) {
			return "Automation workflow.";
		}
	}
}
