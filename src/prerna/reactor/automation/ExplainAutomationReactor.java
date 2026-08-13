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
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Generates a plain-English explanation of what an automation does when run.
 * Reads the saved automation.json, sends it to a model engine with a narration prompt,
 * and returns the explanation as a string.
 *
 * <p>Suitable for sharing with non-technical stakeholders or onboarding new team members.
 *
 * <p>Uses the same model-engine resolution logic as {@link GenerateAutomationReactor}:
 * uses the {@code engine} param if provided, otherwise falls back to the first accessible MODEL engine.
 *
 * <p>Pixel: {@code ExplainAutomation(project=["appId"])}
 */
public class ExplainAutomationReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(ExplainAutomationReactor.class);

    /**
     * Narration prompt for the full explain mode (saved doc, 2-3 sentences for stakeholders).
     * Used when no {@code content} parameter is provided.
     */
    private static final String NARRATION_SYSTEM_PROMPT =
        "You are a helpful assistant that explains software automation workflows to non-technical users. "
        + "Given the following automation JSON definition, write a 2-3 sentence plain-English explanation "
        + "of what this automation does when it runs. "
        + "Write for a non-technical audience — do not mention JSON, nodes, config keys, or technical terms. "
        + "Start your response with: 'When you run this automation, it will...'";

    /**
     * Suggest prompt for the description field (in-memory doc, 1 sentence for the description field).
     * Used when a {@code content} parameter is provided (current unsaved state from the FE).
     */
    private static final String SUGGEST_SYSTEM_PROMPT =
        "You generate one-sentence descriptions for workflow automations. "
        + "Given an automation graph JSON, write a single clear sentence (under 20 words) describing what the automation does. "
        + "Be specific about the actions taken, not about node types or technical structure. "
        + "Start with an active verb. No quotes, no period at the end, no explanation. "
        + "Examples: 'Queries open claims and drafts a daily summary email for case managers', "
        + "'Searches the knowledge base for relevant documents and generates a response using AI', "
        + "'Uploads processed files to cloud storage and updates the claims database'.";

    private static final String CONTENT_KEY = "content";

    public ExplainAutomationReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.PROJECT.getKey(),
            ReactorKeysEnum.ENGINE.getKey(),
            CONTENT_KEY,
        };
        this.keyRequired = new int[] { 1, 0, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in.");
        }

        String projectId = this.keyValue.get(this.keysToGet[0]);
        String engineId = this.keyValue.get(this.keysToGet[1]);
        String contentEncoded = this.keyValue.get(CONTENT_KEY);

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
        if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
            throw new IllegalArgumentException(
                "Project does not exist or user does not have access.");
        }

        // Determine mode: suggest (in-memory content provided) vs. narrate (read saved file)
        boolean suggestMode = contentEncoded != null && !contentEncoded.trim().isEmpty();
        String doc;
        String systemPrompt;
        if (suggestMode) {
            try {
                doc = new String(
                    java.util.Base64.getDecoder().decode(contentEncoded.trim()),
                    StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                doc = contentEncoded; // not base64-encoded, use as-is
            }
            if (doc.length() > 50_000) {
                doc = doc.substring(0, 50_000);
            }
            systemPrompt = SUGGEST_SYSTEM_PROMPT;
            classLogger.info("ExplainAutomationReactor: suggest mode (in-memory content), project={}", projectId);
        } else {
            classLogger.info("ExplainAutomationReactor: narrate mode (saved file), project={}", projectId);
            doc = loadCurrentDoc(projectId);
            if (doc.length() > 50_000) {
                doc = doc.substring(0, 50_000);
            }
            systemPrompt = NARRATION_SYSTEM_PROMPT;
        }

        // Resolve model engine — provided ID or first accessible MODEL engine
        if (engineId == null || engineId.trim().isEmpty()) {
            engineId = AutomationExecutionUtils.findFirstModelEngine(user);
        }
        if (engineId == null || engineId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "No AI model engine is available. Add a model engine connection to use this feature.");
        }
        if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
            throw new IllegalArgumentException(
                "Model engine " + engineId + " does not exist or user does not have access.");
        }

        IModelEngine modelEngine = Utility.getModel(engineId);
        if (modelEngine == null) {
            throw new IllegalArgumentException(
                "Model engine " + engineId + " could not be loaded. It may no longer exist.");
        }

        classLogger.info("ExplainAutomationReactor: calling model engine {} for project {}", engineId, projectId);

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("use_history", false);

        Map<String, Object> response;
        try {
            String userMessage = "Automation definition:\n" + doc;
            response = modelEngine.ask(systemPrompt + "\n\n" + userMessage,
                    null, this.insight, paramMap).toMap();
        } catch (Exception e) {
            classLogger.error("LLM call failed for ExplainAutomation on project {}", projectId, e);
            throw new RuntimeException("AI explanation failed: " + e.getMessage(), e);
        }

        String explanation = AutomationExecutionUtils.extractResponseText(response);
        if (explanation == null || explanation.isBlank()) {
            throw new IllegalStateException(
                "The AI model did not return an explanation. Try again.");
        }

        classLogger.info("ExplainAutomationReactor: completed for project {}", projectId);
        return new NounMetadata(explanation.trim(), PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    }

    private String loadCurrentDoc(String projectId) {
        try {
            String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
            File automationFile = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_FILE_NAME);
            if (automationFile.exists() && automationFile.isFile()) {
                return Files.readString(automationFile.toPath(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            classLogger.warn("Could not read automation.json for project {} — returning empty doc", projectId, e);
        }
        return "{\"version\":1,\"graph\":{\"nodes\":[],\"edges\":[]}}";
    }

    @Override
    public String getReactorDescription() {
        return "Generates a plain-English explanation of what a saved automation does when run, "
            + "suitable for sharing with non-technical stakeholders. "
            + "Reads the saved automation.json and narrates it using a model engine.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        return switch (key) {
            case "project" -> "The project ID of the automation to explain.";
            case "engine" -> "Optional model engine ID to use. Defaults to the first accessible MODEL engine.";
            case CONTENT_KEY -> "Optional base64-encoded JSON of the current automation (from FE in-memory state). "
                + "When provided, produces a one-sentence description suitable for the description field "
                + "instead of reading the saved file and narrating 2-3 sentences.";
            default -> super.getDescriptionForKey(key);
        };
    }
}
