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
 * Keeps each project's own {@code assets/mcp/pixel_mcp.json} in sync with a project-scoped
 * {@code TriggerAutomation} MCP tool entry, called by {@link SaveAutomationReactor} on every save
 * so the project's automation is always discoverable as an MCP tool without a separate manual
 * "make this an MCP tool" step.
 *
 * <p>A single-purpose class (not folded into {@link AutomationExecutionUtils}, which is scoped to
 * run-execution concerns) so the MCP-catalog-sync responsibility stays isolated and easy to find.
 *
 * <p>Uses {@code org.json} (JSONObject/JSONArray) throughout because {@link MCPUtility} is built
 * on org.json, and converting between org.json and Gson just to call those helpers would add
 * unnecessary overhead and type-safety risk. Gson is used everywhere else in the automation package.
 */
public final class AutomationMcpSync {

	private static final Logger classLogger = LogManager.getLogger(AutomationMcpSync.class);

	/** Stamped into the generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR} so re-saves replace it in place. */
	private static final String AUTOMATION_MCP_GENERATOR_ID = "AutomationMCP";

	private AutomationMcpSync() {
		// utility class
	}

	/**
	 * Writes/updates the project-scoped {@code TriggerAutomation} entry. Uses the same
	 * merge/generator-stamp helpers as {@code MakePixelMCPReactor} (a distinct generator id), so
	 * it never disturbs tools the user authored by hand or generated through other flows in the
	 * same file.
	 *
	 * <p>{@code project} is kept as a required argument on the generated tool (matching the
	 * existing convention for reactor-scanned MCP tools) rather than hardcoded - the id is still
	 * fixed to this project by construction of the pixel expression itself, so callers only ever
	 * need to (re-)confirm which project, never guess a different one.
	 *
	 * <p>Failures here are logged and swallowed - the automation save itself must not fail just
	 * because the MCP catalog couldn't be refreshed.
	 *
	 * @param project   the resolved project, or {@code null} if it could not be loaded (e.g.
	 *                  deleted concurrently) - a no-op in that case, logged as a warning
	 * @param projectId the project id (used even when {@code project} is present, for clarity)
	 * @param user      the user performing the save, used as the git commit author
	 */
	public static void syncTriggerAutomationTool(IProject project, String projectId, User user, String automationJson) {
		if (project == null) {
			classLogger.warn("Skipping automation MCP tool sync for project {}: project could not be loaded.",
					projectId);
			return;
		}
		if (automationJson == null || automationJson.isBlank()) {
			classLogger.warn("Skipping automation MCP tool sync for project {}: automation JSON is empty.", projectId);
			return;
		}

		try {
			boolean hasDbNodes = hasPlaygroundDbNodes(automationJson);
			JSONArray generated = new JSONArray().put(buildTriggerAutomationTool(projectId, automationJson, hasDbNodes));
			if (hasDbNodes) {
				generated.put(buildGetAutomationSchemaTool(projectId));
			}
			generated.put(buildBuildAutomationTool(projectId));
			MCPUtility.stampGenerator(generated, AUTOMATION_MCP_GENERATOR_ID);

			String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
			String outputFileLoc = Paths.get(assetsFolder, "mcp", "pixel_mcp.json").toString();
			JSONArray merged = MCPUtility.mergeGeneratedTools(
					MCPUtility.readMcpJson(outputFileLoc), generated, AUTOMATION_MCP_GENERATOR_ID, true);

			writeMcpJson(outputFileLoc, merged);

			MCPUtility.addMCPTag(project);
			commitAndPush(project, projectId, assetsFolder, user);
		} catch (Exception e) {
			classLogger.warn("Failed to sync TriggerAutomation MCP tool for project {}", projectId, e);
		}
	}

	// -- Private helpers -------------------------------------------------------------

	private static JSONObject buildTriggerAutomationTool(String projectId, String automationJson, boolean hasDbNodes) {
		JSONObject tool = new JSONObject();
		tool.put("name", "TriggerAutomation");
		tool.put("title", "Trigger Automation");

		String docDescription = null;
		try {
			if (automationJson != null && !automationJson.isBlank()) {
				JSONObject doc = new JSONObject(automationJson);
				String raw = doc.optString(AutomationConstants.DOC_DESCRIPTION, "").trim();
				if (!raw.isEmpty()) {
					docDescription = raw;
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to read description from automation JSON for project {}", projectId, e);
		}

		String description;
		if (docDescription != null) {
			description = docDescription + " Triggers the automation and returns a per-workflow summary once complete.";
		} else {
			description = "Manually triggers the automation configured for this project/app and returns a "
					+ "per-workflow summary once complete (e.g. \"Indexed 20 files\").";
		}
		if (hasDbNodes) {
			description += " This automation has database nodes that accept SQL queries  - call GetAutomationSchema first"
					+ " to discover the exact table and column names before writing SQL.";
		}
		tool.put("description", description);

		JSONObject projectProp = new JSONObject();
		projectProp.put("type", "string");
		projectProp.put("title", "Project");
		projectProp.put("description", "The project ID for this automation. Always use: " + projectId);
		projectProp.put("default", projectId);
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), projectProp);

		JSONObject inputsProperties = new JSONObject();
		try {
			if (automationJson != null && !automationJson.isBlank()) {
				JSONObject doc = new JSONObject(automationJson);
				JSONObject graph = doc.optJSONObject("graph");
				JSONArray nodes = graph != null ? graph.optJSONArray("nodes") : null;
				if (nodes != null) {
					for (int i = 0; i < nodes.length(); i++) {
						JSONObject node = nodes.optJSONObject(i);
						if (node == null) continue;
						String nodeLabel = node.optString("label", "");
						JSONArray fillable = node.optJSONArray("playgroundFillable");
						if (fillable == null || fillable.length() == 0) continue;
						String nodeType = node.optString("type", "");
						for (int j = 0; j < fillable.length(); j++) {
							String fieldName = fillable.optString(j);
							if (fieldName == null || fieldName.isBlank()) continue;
							String paramName = AutomationExecutionUtils.buildPlaygroundParamName(nodeLabel, fieldName);
							String paramDescription = buildPlaygroundParamDescription(nodeType, fieldName);
							JSONObject paramProp = new JSONObject();
							paramProp.put("type", "string");
							paramProp.put("description", paramDescription);
							inputsProperties.put(paramName, paramProp);
						}
					}
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to scan automation nodes for playground inputs for project {}", projectId, e);
		}

		if (!inputsProperties.isEmpty()) {
			JSONObject inputsProp = new JSONObject();
			inputsProp.put("type", "object");
			inputsProp.put("description", "Optional inputs to inject into automation nodes before running. Populate fields with values relevant to the user's request.");
			inputsProp.put("properties", inputsProperties);
			properties.put(AutomationConstants.AUTOMATION_INPUTS_KEY, inputsProp);
		}

		JSONObject triggerTypeProp = new JSONObject();
		triggerTypeProp.put("type", "string");
		triggerTypeProp.put("title", "Trigger Type");
		triggerTypeProp.put("description", "How this automation was triggered. Always use: " + AutomationConstants.TRIGGER_PLAYGROUND);
		triggerTypeProp.put("default", AutomationConstants.TRIGGER_PLAYGROUND);
		properties.put(AutomationConstants.AUTOMATION_TRIGGER_TYPE_KEY, triggerTypeProp);

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
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		meta.put(MCPUtility.SMSS_MCP_UI, uiJson);
		tool.put("_meta", meta);

		return tool;
	}

	/** Returns true if any database-engine node has {@code expression} in its {@code playgroundFillable} list. */
	private static boolean hasPlaygroundDbNodes(String automationJson) {
		if (automationJson == null || automationJson.isBlank()) return false;
		try {
			JSONObject doc = new JSONObject(automationJson);
			JSONObject graph = doc.optJSONObject("graph");
			JSONArray nodes = graph != null ? graph.optJSONArray("nodes") : null;
			if (nodes == null) return false;
			for (int i = 0; i < nodes.length(); i++) {
				JSONObject node = nodes.optJSONObject(i);
				if (node == null) continue;
				if (!AutomationConstants.NODE_DATABASE_ENGINE.equals(node.optString("type"))) continue;
				JSONArray fillable = node.optJSONArray("playgroundFillable");
				if (fillable == null) continue;
				for (int j = 0; j < fillable.length(); j++) {
					if (AutomationConstants.CONFIG_EXPRESSION.equals(fillable.optString(j))) return true;
				}
			}
		} catch (Exception e) {
			classLogger.warn("Failed to parse automation JSON while checking for DB nodes", e);
		}
		return false;
	}

	/**
	 * Builds the {@code BuildAutomation} MCP tool  - lets an agent in Playground generate or edit
	 * this project's automation from a plain-English description. Execution mode is ASK so the user
	 * can review the generated document before it is saved.
	 */
	private static JSONObject buildBuildAutomationTool(String projectId) {
		JSONObject tool = new JSONObject();
		tool.put("name", "BuildAutomation");
		tool.put("title", "Build / Edit Automation");
		tool.put("description",
				"Generates or edits this project's automation from a plain-English description. "
				+ "The model iteratively gathers context (database schema, available reactors) before producing a complete automation document. "
				+ "Does NOT save automatically  - call SaveAutomation with the returned JSON to persist. "
				+ "Use currentDoc to pass the existing automation JSON (base64-encoded) for edit mode.");

		JSONObject projectProp = new JSONObject();
		projectProp.put("type", "string");
		projectProp.put("description", "The project ID for this automation. Always use: " + projectId);
		projectProp.put("default", projectId);

		JSONObject descProp = new JSONObject();
		descProp.put("type", "string");
		descProp.put("description", "Plain-English description of what the automation should do, or how to modify the existing one. Will be base64-encoded automatically if needed.");

		JSONObject currentDocProp = new JSONObject();
		currentDocProp.put("type", "string");
		currentDocProp.put("description", "Optional base64-encoded JSON of the current automation document. Include this to edit rather than generate from scratch.");

		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), projectProp);
		properties.put(AutomationConstants.DOC_DESCRIPTION, descProp);
		properties.put("currentDoc", currentDocProp);

		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", "BuildAutomation_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()).put(AutomationConstants.DOC_DESCRIPTION));
		tool.put("inputSchema", inputSchema);

		JSONObject meta = new JSONObject();
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, "BuildAutomation");
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.ASK.getValue());
		tool.put("_meta", meta);

		return tool;
	}

	/** Builds the auto-executable {@code GetAutomationSchema} companion tool. */
	private static JSONObject buildGetAutomationSchemaTool(String projectId) {
		JSONObject tool = new JSONObject();
		tool.put("name", "GetAutomationSchema");
		tool.put("title", "Get Automation Database Schema");
		tool.put("description",
				"Returns the physical table and column names for database nodes in this automation that accept SQL input. "
						+ "Call this before TriggerAutomation when you need to write a SQL query  - it gives you the exact "
						+ "table and column names available in the database.");

		JSONObject projectProp = new JSONObject();
		projectProp.put("type", "string");
		projectProp.put("description", "The project ID for this automation. Always use: " + projectId);
		projectProp.put("default", projectId);
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), projectProp);

		JSONObject inputSchema = new JSONObject();
		inputSchema.put("type", "object");
		inputSchema.put("title", "GetAutomationSchema_Arguments");
		inputSchema.put("properties", properties);
		inputSchema.put("required", new JSONArray().put(ReactorKeysEnum.PROJECT.getKey()));
		tool.put("inputSchema", inputSchema);

		JSONObject meta = new JSONObject();
		meta.put(MCPUtility.SMSS_FUNCTION_NAME, "GetAutomationSchema");
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
		tool.put("_meta", meta);

		return tool;
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

	private static String buildPlaygroundParamDescription(String nodeType, String fieldName) {
		if ("database-engine".equals(nodeType) && "expression".equals(fieldName)) {
			return "SQL query to execute against the connected database";
		}
		if ("model-engine".equals(nodeType) && "command".equals(fieldName)) {
			return "Natural language prompt to send to the language model";
		}
		if ("model-engine".equals(nodeType) && "context".equals(fieldName)) {
			return "System instructions for the language model's behavior";
		}
		if ("vector-engine".equals(nodeType) && "command".equals(fieldName)) {
			return "Search query to run against the vector database";
		}
		if ("function-engine".equals(nodeType) && "params".equals(fieldName)) {
			return "JSON parameters to pass to the function";
		}
		return "Input for the " + fieldName + " field";
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
