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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Returns the automation <em>definition</em> (the saved {@code automation.json} graph) for a project.
 * Returns an empty graph document when no automation has been saved yet.
 *
 * <p>There are three "get" reactors — each reads something different:
 * <ul>
 *   <li>{@code GetAutomation} (this reactor) — reads {@code automation.json}: the pipeline graph
 *       (nodes, edges, output transforms). Static config written by {@link SaveAutomationReactor}.</li>
 *   <li>{@link GetAutomationConfigReactor GetAutomationConfig} — reads {@code automation_config.json}:
 *       key/value env vars and secrets; sensitive values are masked in the response.</li>
 *   <li>{@link GetAutomationRunReactor GetAutomationRun} — reads live run state from the DB
 *       (AUTOMATION_RUNS + AUTOMATION_NODE_OUTPUTS); used by the FE to poll execution progress.</li>
 * </ul>
 *
 * <p>Pixel: {@code GetAutomation(project=["appId"])}
 */
public class GetAutomationReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetAutomationReactor.class);

    public GetAutomationReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a project id");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have access");
        }

        IProject project = Utility.getProject(projectId);
        if (project != null && project.requirePublish(true)) {
            classLogger.info("Pulled project {} from cluster", projectId);
        }

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File automationFile = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_FILE_NAME);

        if (!automationFile.exists() || !automationFile.isFile()) {
            // return empty graph document for brand-new automations
            Map<String, Object> empty = new HashMap<>();
            empty.put(AutomationConstants.DOC_VERSION, AutomationConstants.DOC_CURRENT_VERSION);
            Map<String, Object> graph = new HashMap<>();
            graph.put(AutomationConstants.DOC_NODES, new ArrayList<>());
            graph.put(AutomationConstants.DOC_EDGES, new ArrayList<>());
            empty.put(AutomationConstants.DOC_GRAPH, graph);
            return new NounMetadata(empty, PixelDataType.MAP, PixelOperationType.OPERATION);
        }

        try {
            String json = Files.readString(automationFile.toPath(), StandardCharsets.UTF_8);
            Map<String, Object> doc = AutomationExecutionUtils.GSON.fromJson(json,
                    new TypeToken<Map<String, Object>>() {}.getType());
            return new NounMetadata(doc, PixelDataType.MAP, PixelOperationType.OPERATION);
        } catch (IOException e) {
            classLogger.error("Error reading automation.json for project {}", projectId, e);
            throw new IllegalArgumentException("Unable to read automation: " + e.getMessage());
        }
    }

    @Override
    public String getReactorDescription() {
        return "Returns the automation pipeline definition (automation.json) for a project; returns an empty graph when none has been saved.";
    }
}
