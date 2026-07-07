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
package prerna.reactor.workflow.templates;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.workflow.WorkflowConstants;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Creates a workflow from a template, saving it to the project's portals folder.
 *
 * <p>Pixel: {@code CreateWorkflowFromTemplate(project=["appId"], templateId=["sync-etl"])}
 *
 * <p>Loads the template definition, writes workflow.json and workflow-config.json
 * with the template's default config values, and git-commits the change.
 */
public class CreateWorkflowFromTemplateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateWorkflowFromTemplateReactor.class);
	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

	public CreateWorkflowFromTemplateReactor() {
		this.keysToGet = new String[]{ "project", "templateId" };
		this.keyRequired = new int[]{ 1, 1 };
	}

	@Override
	@SuppressWarnings("unchecked")
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String templateId = this.keyValue.get(this.keysToGet[1]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}
		if (templateId == null || templateId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a template id");
		}

		// Auth check
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have edit access");
		}

		// Load template
		Map<String, Object> template = GetWorkflowTemplatesReactor.getTemplateById(templateId);
		if (template == null) {
			throw new IllegalArgumentException("Template not found: " + templateId);
		}

		// Extract workflow definition
		Map<String, Object> workflow = (Map<String, Object>) template.get("workflow");
		if (workflow == null) {
			throw new IllegalArgumentException("Template has no workflow definition");
		}

		// Write workflow.json
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File workflowFile = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_FILE_NAME);

		try {
			java.nio.file.Files.writeString(workflowFile.toPath(),
					GSON.toJson(workflow), java.nio.charset.StandardCharsets.UTF_8);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to write workflow.json: " + e.getMessage(), e);
		}

		// Write workflow-config.json with template defaults
		List<Map<String, Object>> configKeys = (List<Map<String, Object>>) template.get("configKeys");
		if (configKeys != null && !configKeys.isEmpty()) {
			List<Map<String, String>> configEntries = new ArrayList<>();
			for (Map<String, Object> keyDef : configKeys) {
				Map<String, String> entry = new HashMap<>();
				entry.put("key", (String) keyDef.get("key"));
				entry.put("value", keyDef.get("default") != null ? (String) keyDef.get("default") : "");
				entry.put("label", (String) keyDef.get("label"));
				entry.put("description", (String) keyDef.get("description"));
				configEntries.add(entry);
			}

			File configFile = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_CONFIG_FILE_NAME);
			try {
				java.nio.file.Files.writeString(configFile.toPath(),
						GSON.toJson(configEntries), java.nio.charset.StandardCharsets.UTF_8);
			} catch (Exception e) {
				classLogger.warn("Failed to write workflow-config.json: {}", e.getMessage(), e);
			}
		}

		classLogger.info("Created workflow from template '{}' for project {}", templateId, projectId);

		// Return success
		Map<String, Object> result = new HashMap<>();
		result.put("templateId", templateId);
		result.put("templateName", template.get("name"));
		result.put("projectId", projectId);
		result.put("status", "created");
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}
