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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPDisplayOption;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.git.GitRepoUtils;

/**
 * Maintains the generated Automation MCP catalog for a project.
 */
public final class AutomationMcpSync {

	private static final Logger classLogger = LogManager.getLogger(AutomationMcpSync.class);
	private static final String AUTOMATION_MCP_GENERATOR_ID = "AutomationMCP";

	private AutomationMcpSync() {
	}

	/**
	 * Syncs the net-new Automation MCP contract: trigger the workflow or add one managed action.
	 */
	public static void syncTriggerAutomationTool(IProject project, String projectId, User user, String automationJson) {
		if (project == null || automationJson == null || automationJson.isBlank()) {
			classLogger.warn("Skipping Automation MCP sync for project {}: project or definition is unavailable.", projectId);
			return;
		}

		try {
			JSONArray generated = new JSONArray()
					.put(buildTriggerAutomationTool(projectId, automationJson))
					.put(buildAddAutomationStepTool(projectId))
					.put(buildUpdateCustomStepTool(projectId));
			MCPUtility.stampGenerator(generated, AUTOMATION_MCP_GENERATOR_ID);

			String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
			String outputFileLoc = Paths.get(assetsFolder, "mcp", "pixel_mcp.json").toString();
			JSONArray merged = MCPUtility.mergeGeneratedTools(
					MCPUtility.readMcpJson(outputFileLoc), generated, AUTOMATION_MCP_GENERATOR_ID, true);
			writeMcpJson(outputFileLoc, merged);

			MCPUtility.addMCPTag(project);
			commitAndPush(project, projectId, assetsFolder, user);
		} catch (Exception e) {
			classLogger.warn("Failed to sync Automation MCP tools for project {}", projectId, e);
		}
	}

	private static JSONObject buildTriggerAutomationTool(String projectId, String automationJson) {
		JSONObject tool = new JSONObject();
		tool.put("name", "TriggerAutomation");
		tool.put("title", "Trigger Automation");
		tool.put("description", readDescription(automationJson)
				+ " Triggers this project's configured automation.");

		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProjectProperty(projectId));
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", "TriggerAutomation_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		tool.put("inputSchema", inputSchema);

		JSONObject uiJson = new JSONObject();
		uiJson.put(MCPUtility.UI_DISPLAY_LOCATION, MCPDisplayOption.SIDEBAR.getValue());
		uiJson.put(MCPUtility.UI_RESOURCE_URI, "system://automation-workspace/?readOnly=1");
		JSONObject meta = new JSONObject();
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, "TriggerAutomation");
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		meta.put(MCPUtility.SMSS_MCP_UI, uiJson);
		tool.put("_meta", meta);
		return tool;
	}

	private static JSONObject buildAddAutomationStepTool(String projectId) {
		JSONObject tool = new JSONObject();
		tool.put("name", "AddAutomationStep");
		tool.put("title", "Add Automation Action");
		tool.put("description",
				"Adds one action to this automation, creates its managed Python implementation, and connects it "
						+ "to an upstream node. Supported actions are model.llm, model.embeddings, database.query, "
						+ "database.write, vector.search, vector.list, vector.delete, vector.add-file, vector.add-csv, "
						+ "vector.download, storage.list, storage.download, storage.upload, storage.delete, "
						+ "storage.read-base64, function.execute, app.run-pixel, and python-step.skeleton.");

		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProjectProperty(projectId));
		properties.put("nodeId", stringProperty("Unique safe node ID, such as query_claims."));
		properties.put("actionId", actionIdProperty());
		properties.put("config", stringProperty("JSON configuration for the selected action. Do not include nodeType or operation; the approved actionId determines both. For model.llm, engineId may be omitted to create an incomplete draft that the user configures in the inspector before running."));
		properties.put("label", stringProperty("Short action-oriented label shown in the workflow."));
		properties.put("outputVar", stringProperty("Unique output variable, such as claims_rows."));
		properties.put("afterNodeId", stringProperty("Optional upstream node ID. Defaults to trigger when omitted."));

		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", "AddAutomationStep_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId")
				.put("actionId").put("config").put("label").put("outputVar"));
		tool.put("inputSchema", inputSchema);

		JSONObject meta = new JSONObject();
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, "AddAutomationStep");
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		tool.put("_meta", meta);
		return tool;
	}

	private static JSONObject buildUpdateCustomStepTool(String projectId) {
		JSONObject tool = new JSONObject();
		tool.put("name", "UpdateAutomationCustomStep");
		tool.put("title", "Write Custom Automation Step");
		tool.put("description",
				"Replaces the source for an existing custom python-step node. Use this after AddAutomationStep "
						+ "creates a python-step.skeleton. Provide the sourceHash returned when that step was created. "
						+ "Use custom steps for editable integrations such as GitHub or email; never embed credentials.");

		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), fixedProjectProperty(projectId));
		properties.put("nodeId", stringProperty("Existing python-step node ID."));
		properties.put("source", stringProperty("Complete Python source defining run(context, inputs)."));
		properties.put("expectedSourceHash",
				stringProperty("Current sourceHash returned when the step was created or last updated."));
		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", "UpdateAutomationCustomStep_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put("nodeId")
				.put("source").put("expectedSourceHash"));
		tool.put("inputSchema", inputSchema);

		JSONObject meta = new JSONObject();
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, "UpdateAutomationCustomStep");
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		tool.put("_meta", meta);
		return tool;
	}

	private static String readDescription(String automationJson) {
		try {
			String description = new JSONObject(automationJson).optString(AutomationConstants.DOC_DESCRIPTION, "").trim();
			if (!description.isEmpty()) {
				return description;
			}
		} catch (Exception e) {
			classLogger.warn("Failed to read Automation description for MCP catalog", e);
		}
		return "Runs this project's automation workflow.";
	}

	private static JSONObject fixedProjectProperty(String projectId) {
		JSONObject projectProp = new JSONObject();
		projectProp.put("type", "string");
		projectProp.put("description", "The project ID for this automation. Always use: " + projectId);
		projectProp.put("default", projectId);
		projectProp.put("const", projectId);
		return projectProp;
	}

	private static JSONObject stringProperty(String description) {
		JSONObject property = new JSONObject();
		property.put("type", "string");
		property.put("description", description);
		return property;
	}

	private static JSONObject actionIdProperty() {
		JSONObject property = stringProperty("Approved business action to add. This determines the internal canvas node type and operation.");
		JSONArray values = new JSONArray();
		for (AutomationStepTemplateRegistry.ActionDefinition action : AutomationStepTemplateRegistry.getActions()) {
			values.put(action.getActionId());
		}
		property.put("enum", values);
		return property;
	}

	private static void writeMcpJson(String outputFileLoc, JSONArray tools) throws IOException {
		JSONObject mcpJson = new JSONObject();
		mcpJson.put("tools", tools);
		JSONObject fileMeta = new JSONObject();
		fileMeta.put("last_modified_date", LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE));
		mcpJson.put("_meta", fileMeta);

		File outputFile = new File(outputFileLoc);
		outputFile.getParentFile().mkdirs();
		Files.writeString(outputFile.toPath(), mcpJson.toString(4), StandardCharsets.UTF_8);
	}

	private static void commitAndPush(IProject project, String projectId, String assetsFolder, User user) {
		String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/mcp/pixel_mcp.json");
		GitRepoUtils.addSpecificFiles(versionFolder, gitRelativeFilePaths);
		GitRepoUtils.commitAddedFiles(versionFolder, "sync: automation MCP tool", user);
		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushProjectFolder(project, assetsFolder);
		}
	}
}
