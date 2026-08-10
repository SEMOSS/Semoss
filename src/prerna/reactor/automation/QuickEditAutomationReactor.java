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
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Headless automation editor. The LLM provides a plain-English description of the desired change;
 * this reactor chains {@code GenerateAutomation} (edit mode) and {@code SaveAutomation} silently
 * without opening any UI. Returns a compact summary so the LLM can narrate the result.
 *
 * <p>Use {@code EditAutomation} instead when the change is complex or when the user wants to
 * review the result before saving.
 *
 * <p>Threading: {@link PixelExecutionUtils#runAndCollect} blocks the caller thread for the full
 * LLM round-trip (up to {@link AutomationConstants#DEFAULT_TIMEOUT_SECONDS} seconds). Acceptable
 * for MVP usage volumes — revisit under concurrent load.
 *
 * <p>Pixel: {@code QuickEditAutomation(project=["appId"], editDescription=["change the SQL to last 7 days"])}
 */
public class QuickEditAutomationReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(QuickEditAutomationReactor.class);

    private static final String EDIT_DESCRIPTION_KEY = "editDescription";

    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_NODE_COUNT = "nodeCount";
    private static final String RESULT_MESSAGE = "message";

    public QuickEditAutomationReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), EDIT_DESCRIPTION_KEY };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);
        String editDescription = this.keyValue.get(EDIT_DESCRIPTION_KEY);

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a project id");
        }
        if (editDescription == null || editDescription.trim().isEmpty()) {
            throw new IllegalArgumentException("Must provide an editDescription");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have edit access");
        }

        classLogger.info("QuickEditAutomationReactor: starting edit for project {}", projectId);

        String currentDocJson = loadCurrentDoc(projectId);

        // GenerateAutomation decodes base64 to prevent Pixel injection — same pattern used by the FE
        String encodedDesc = Base64.getEncoder()
                .encodeToString(editDescription.trim().getBytes(StandardCharsets.UTF_8));
        String encodedDoc = Base64.getEncoder()
                .encodeToString(currentDocJson.getBytes(StandardCharsets.UTF_8));

        String generatePixel = String.format(
                "GenerateAutomation(project=[\"%s\"], description=[\"%s\"], currentDoc=[\"%s\"]);",
                projectId, encodedDesc, encodedDoc);

        classLogger.info("QuickEditAutomationReactor: calling GenerateAutomation for project {}", projectId);
        Object raw;
        try {
            raw = PixelExecutionUtils.runAndCollect(this.insight, generatePixel);
        } catch (PixelExecutionUtils.AutomationPixelException e) {
            classLogger.error("GenerateAutomation pixel error for project {}", projectId, e);
            throw new IllegalArgumentException("AI generation failed: " + e.getMessage());
        }

        if (raw == null) {
            throw new IllegalArgumentException("GenerateAutomation returned no result for project: " + projectId);
        }
        String generatedJson = raw instanceof String ? (String) raw : raw.toString();
        if (generatedJson.isBlank()) {
            throw new IllegalArgumentException("GenerateAutomation returned an empty result for project: " + projectId);
        }

        Map<String, Object> generatedDoc = parseAndValidate(generatedJson, projectId);

        String encodedSave = Base64.getEncoder()
                .encodeToString(generatedJson.getBytes(StandardCharsets.UTF_8));
        String savePixel = String.format(
                "SaveAutomation(project=[\"%s\"], json=[\"%s\"]);",
                projectId, encodedSave);

        classLogger.info("QuickEditAutomationReactor: calling SaveAutomation for project {}", projectId);
        try {
            PixelExecutionUtils.runAndCollect(this.insight, savePixel);
        } catch (PixelExecutionUtils.AutomationPixelException e) {
            classLogger.error("SaveAutomation pixel error for project {}", projectId, e);
            throw new IllegalArgumentException("Failed to save automation: " + e.getMessage());
        }

        Map<String, Object> summary = buildSummary(generatedDoc);
        classLogger.info("QuickEditAutomationReactor: completed for project {}", projectId);
        return new NounMetadata(summary, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    /**
     * Reads the current automation.json from disk.
     * Returns a minimal blank document if none exists — GenerateAutomation treats the absence of
     * meaningful nodes as a fresh-start signal.
     */
    private String loadCurrentDoc(String projectId) {
        try {
            String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
            File automationFile = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_FILE_NAME);
            if (automationFile.exists() && automationFile.isFile()) {
                return Files.readString(automationFile.toPath(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            classLogger.warn("Could not read current automation.json for project {} — treating as blank", projectId, e);
        }
        return "{\"version\":1,\"graph\":{\"nodes\":[],\"edges\":[]}}";
    }

    /**
     * Parses the generated JSON and validates its structure before allowing a save.
     * Throws if the document is malformed or missing the trigger node — prevents corrupt saves.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseAndValidate(String json, String projectId) {
        Map<String, Object> doc;
        try {
            doc = AutomationExecutionUtils.GSON.fromJson(json, AutomationExecutionUtils.MAP_TYPE);
        } catch (Exception e) {
            classLogger.error("GenerateAutomation returned invalid JSON for project {}", projectId, e);
            throw new IllegalArgumentException("AI generation returned invalid JSON — not saved.");
        }

        Map<?, ?> graph = (Map<?, ?>) doc.get(AutomationConstants.DOC_GRAPH);
        if (graph == null) {
            classLogger.error("GenerateAutomation result missing 'graph' field for project {}", projectId);
            throw new IllegalArgumentException("AI generation result is missing 'graph' field — not saved.");
        }

        List<?> nodes = (List<?>) graph.get(AutomationConstants.DOC_NODES);
        if (nodes == null || nodes.isEmpty()) {
            classLogger.error("GenerateAutomation result has no nodes for project {}", projectId);
            throw new IllegalArgumentException("AI generation result has no nodes — not saved.");
        }

        boolean hasTrigger = nodes.stream()
                .filter(n -> n instanceof Map)
                .map(n -> (Map<?, ?>) n)
                .anyMatch(n -> AutomationConstants.NODE_TRIGGER.equals(n.get(AutomationConstants.NODE_FIELD_TYPE)));

        if (!hasTrigger) {
            classLogger.error("GenerateAutomation result has no trigger node for project {}", projectId);
            throw new IllegalArgumentException("AI generation result has no trigger node — not saved.");
        }

        return doc;
    }

    /**
     * Builds the summary Map returned to the LLM — concise enough to fit in context without
     * sending the full automation JSON.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> buildSummary(Map<String, Object> doc) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put(RESULT_SUCCESS, true);

        try {
            String desc = (String) doc.getOrDefault(AutomationConstants.DOC_DESCRIPTION, "");
            Map<?, ?> graph = (Map<?, ?>) doc.get(AutomationConstants.DOC_GRAPH);
            List<?> nodes = graph != null ? (List<?>) graph.get(AutomationConstants.DOC_NODES) : null;
            int nodeCount = nodes != null ? nodes.size() : 0;
            summary.put(RESULT_NODE_COUNT, nodeCount);

            String message = (desc != null && !desc.isBlank())
                    ? "Automation updated successfully. Description: \"" + desc.trim() + "\". " + nodeCount + " steps."
                    : "Automation updated successfully. " + nodeCount + " steps.";
            summary.put(RESULT_MESSAGE, message);
        } catch (Exception e) {
            classLogger.warn("Could not build summary from generated doc", e);
            summary.put(RESULT_MESSAGE, "Automation updated successfully.");
        }

        return summary;
    }

    @Override
    public Map<String, String> getMcpToolMetadata() {
        Map<String, String> meta = new HashMap<>();
        meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
        return meta;
    }

    @Override
    public String getReactorDescription() {
        return "Modifies an automation using AI generation without opening any UI. "
                + "Chains GenerateAutomation (edit mode) and SaveAutomation silently. "
                + "Use EditAutomation instead when the user needs to review the change before saving.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
            return "The project ID of the automation to edit.";
        } else if (EDIT_DESCRIPTION_KEY.equals(key)) {
            return "Plain-language description of the change to make. "
                    + "Example: 'Change the SQL filter to pull records from the last 7 days'. "
                    + "Call GetAutomationStructure first to understand the current automation structure.";
        }
        return super.getDescriptionForKey(key);
    }
}
