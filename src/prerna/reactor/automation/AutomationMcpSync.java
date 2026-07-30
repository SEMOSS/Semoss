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
	public static void syncTriggerAutomationTool(IProject project, String projectId, User user) {
		if (project == null) {
			classLogger.warn("Skipping automation MCP tool sync for project {}: project could not be loaded.",
					projectId);
			return;
		}

		try {
			JSONArray generated = new JSONArray().put(buildTriggerAutomationTool(projectId));
			MCPUtility.stampGenerator(generated, AUTOMATION_MCP_GENERATOR_ID);

			String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
			String outputFileLoc = assetsFolder + "/mcp/pixel_mcp.json";
			JSONArray merged = MCPUtility.mergeGeneratedTools(
					MCPUtility.readMcpJson(outputFileLoc), generated, AUTOMATION_MCP_GENERATOR_ID, true);

			writeMcpJson(outputFileLoc, merged);

			MCPUtility.addMCPTag(project);
			commitAndPush(project, projectId, assetsFolder, user);
		} catch (Exception e) {
			classLogger.warn("Failed to sync TriggerAutomation MCP tool for project {}: {}", projectId, e.getMessage(), e);
		}
	}

	// -- Private helpers -------------------------------------------------------------

	private static JSONObject buildTriggerAutomationTool(String projectId) {
		JSONObject tool = new JSONObject();
		tool.put("name", "TriggerAutomation");
		tool.put("title", "Trigger Automation");
		tool.put("description",
				"Manually triggers the automation configured for this project/app and returns a "
						+ "per-workflow summary once complete (e.g. \"Indexed 20 files\").");

		JSONObject projectProp = new JSONObject();
		projectProp.put("type", "string");
		projectProp.put("title", "Project");
		projectProp.put("description", "The project ID for this automation. Always use: " + projectId);
		projectProp.put("default", projectId);
		JSONObject properties = new JSONObject();
		properties.put(ReactorKeysEnum.PROJECT.getKey(), projectProp);

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
