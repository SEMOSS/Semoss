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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.automation.utils.AutomationExecutionUtils;
import prerna.util.Utility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Returns a compact summary of the automation's current structure — description and a flat
 * node list (label, type, operation, engineId) — for use as LLM context before editing.
 *
 * <p>Trigger nodes are excluded; they are infrastructure, not user-authored steps.
 * Returns {@code { description: "", nodes: [] }} for a blank or missing automation.
 *
 * <p>Pixel: {@code GetAutomationStructure(project=["appId"])}
 */
public class GetAutomationStructureReactor extends AbstractReactor {

    private static final String RESULT_KEY_DESCRIPTION = "description";
    private static final String RESULT_KEY_NODES = "nodes";

    public GetAutomationStructureReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
        this.keyRequired = new int[] { 1 };
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

        Map<String, Object> result = new LinkedHashMap<>();
        result.put(RESULT_KEY_DESCRIPTION, "");
        result.put(RESULT_KEY_NODES, new ArrayList<>());

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File automationFile = Paths.get(portalsFolder, AutomationConstants.AUTOMATION_FILE_NAME).toFile();
        String normalizedPath = Utility.normalizePath(automationFile.getAbsolutePath());
        if (!normalizedPath.startsWith(portalsFolder)) {
            throw new IllegalArgumentException("Invalid file path");
        }

        if (!automationFile.exists() || !automationFile.isFile()) {
            return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
        }

        Map<String, Object> doc = AutomationExecutionUtils.loadAutomationDoc(projectId);

        String description = (String) doc.getOrDefault(AutomationConstants.DOC_DESCRIPTION, "");
        result.put(RESULT_KEY_DESCRIPTION, description != null ? description : "");

        Map<?, ?> graph = (Map<?, ?>) doc.get(AutomationConstants.DOC_GRAPH);
        if (graph != null) {
            List<?> nodes = (List<?>) graph.get(AutomationConstants.DOC_NODES);
            if (nodes != null) {
                result.put(RESULT_KEY_NODES, buildNodeSummary(nodes));
            }
        }

        return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    /**
     * Extracts a compact summary for each non-trigger node.
     * Preserves label, type, and the two most useful config fields for LLM context.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> buildNodeSummary(List<?> nodes) {
        List<Map<String, Object>> summary = new ArrayList<>();
        for (Object raw : nodes) {
            if (!(raw instanceof Map)) {
                continue;
            }
            Map<String, Object> node = (Map<String, Object>) raw;
            String type = (String) node.get(AutomationConstants.NODE_FIELD_TYPE);

            // Trigger nodes are not user-authored steps — skip them
            if (AutomationConstants.NODE_TRIGGER.equals(type)) {
                continue;
            }

            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put(AutomationConstants.NODE_FIELD_LABEL,
                    node.getOrDefault(AutomationConstants.NODE_FIELD_LABEL, AutomationConstants.UNNAMED_NODE_LABEL));
            entry.put(AutomationConstants.NODE_FIELD_TYPE, type != null ? type : "");

            Map<?, ?> config = (Map<?, ?>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
            if (config != null) {
                if (config.containsKey(AutomationConstants.CONFIG_OPERATION)) {
                    entry.put(AutomationConstants.CONFIG_OPERATION, config.get(AutomationConstants.CONFIG_OPERATION));
                }
                if (config.containsKey(AutomationConstants.CONFIG_ENGINE_ID)) {
                    entry.put(AutomationConstants.CONFIG_ENGINE_ID, config.get(AutomationConstants.CONFIG_ENGINE_ID));
                }
            }

            summary.add(entry);
        }
        return summary;
    }

    @Override
    public String getReactorDescription() {
        return "Returns the automation's description and a compact node list (label, type, operation, engineId) "
                + "for LLM context. Trigger nodes are excluded. Returns an empty node list for a blank automation.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
            return "The project ID of the automation to inspect.";
        }
        return super.getDescriptionForKey(key);
    }
}
