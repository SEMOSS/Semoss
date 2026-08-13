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
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class SaveAutomationReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(SaveAutomationReactor.class);

    public SaveAutomationReactor() {
        this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.JSON.getKey() };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);
        String jsonEncoded = this.keyValue.get(this.keysToGet[1]);

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a project id");
        }
        if (jsonEncoded == null || jsonEncoded.isEmpty()) {
            throw new IllegalArgumentException("Must provide automation JSON");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have edit access");
        }

        String json;
        try {
            json = new String(Base64.getDecoder().decode(jsonEncoded), StandardCharsets.UTF_8);
        } catch (Exception e) {
            json = jsonEncoded;
        }
        AutomationDefinitionValidator.parseAndValidate(json);

        IProject project = Utility.getProject(projectId);
        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File automationFile = Paths.get(portalsFolder, AutomationConstants.AUTOMATION_FILE_NAME).toFile();
        String normalizedPath = Utility.normalizePath(automationFile.getAbsolutePath());
        if (!normalizedPath.startsWith(portalsFolder)) {
            throw new IllegalArgumentException("Invalid file path");
        }

        try {
            automationFile.getParentFile().mkdirs();
            Files.writeString(automationFile.toPath(), json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            classLogger.error("Error saving automation JSON for project {}", projectId, e);
            throw new IllegalArgumentException("Unable to save automation: " + e.getMessage());
        }

        if (project == null) {
            classLogger.warn("Project {} not found in registry  - skipping git commit and cluster push", projectId);
            SecurityProjectUtils.updateProjectLastEditedDate(projectId);
            AutomationMcpSync.syncTriggerAutomationTool(null, projectId, this.insight.getUser(), json);
            return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
        }

        List<String> files = new ArrayList<>();
        files.add(automationFile.getAbsolutePath());
        String versionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
        try {
            GitRepoUtils.addSpecificFiles(versionFolder, files);
            GitRepoUtils.commitAddedFiles(versionFolder, "Update automation graph", this.insight.getUser());
        } catch (Exception e) {
            classLogger.warn("Git commit failed for automation save", e);
        }

        if (ClusterUtil.IS_CLUSTER) {
            try {
                ClusterUtil.pushProjectFolder(project, versionFolder);
            } catch (Exception e) {
                classLogger.warn("Cluster push failed", e);
            }
        }

        SecurityProjectUtils.updateProjectLastEditedDate(projectId);

        // Keep this project's own MCP tool catalog in sync with every save, so the automation is
        // always discoverable as a "TriggerAutomation" tool without a separate manual step.
        AutomationMcpSync.syncTriggerAutomationTool(project, projectId, this.insight.getUser(), json);

        return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "Saves the automation graph (automation.json) for a project.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
            return "The project ID of the automation to save.";
        } else if (ReactorKeysEnum.JSON.getKey().equals(key)) {
            return "Base64-encoded JSON document representing the automation graph (nodes, edges, transforms).";
        }
        return super.getDescriptionForKey(key);
    }

    @Override
    public Map<String, String> getMcpToolMetadata() {
        Map<String, String> meta = new HashMap<>();
        // Overwrites the saved automation graph and commits it to source control  -
        // requires explicit human confirmation.
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
        return meta;
    }
}
