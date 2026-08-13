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

import prerna.reactor.automation.utils.AutomationGenerationUtils;

import prerna.reactor.automation.utils.AutomationExecutionUtils;

import java.util.Base64;
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
import prerna.util.Utility;

/**
 * Generates a short, action-oriented label (2–4 words) for an automation node
 * based on its type and configuration. Uses the first available MODEL engine.
 *
 * <p>Pixel: {@code GenerateNodeLabel(project=["appId"], type=["model-engine"], config=["base64json"])}
 */
public class GenerateNodeLabelReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GenerateNodeLabelReactor.class);

    private static final String KEY_TYPE = "type";
    private static final String KEY_CONFIG = "config";

    private static final String SYSTEM_PROMPT =
        "You generate short, action-oriented labels for workflow automation steps. "
        + "Given the step type and key configuration details, respond with ONLY the label  - "
        + "2 to 4 words maximum, plain English, no quotes, no punctuation at the end, no explanation. "
        + "The label should describe what the step DOES, not what type of node it is. "
        + "Examples: 'Search claims data', 'Draft email reply', 'Fetch open tickets', 'Summarize results', "
        + "'Upload report file', 'Query veteran records', 'Pause 30 seconds'.";

    public GenerateNodeLabelReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.PROJECT.getKey(),
            KEY_TYPE,
            KEY_CONFIG,
        };
        this.keyRequired = new int[] { 1, 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in.");
        }

        String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
        String nodeType = this.keyValue.get(KEY_TYPE);
        String configEncoded = this.keyValue.get(KEY_CONFIG);

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
        if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have access.");
        }

        // Decode base64 config JSON sent by FE
        String configJson;
        try {
            configJson = new String(Base64.getDecoder().decode(configEncoded.trim()), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            configJson = configEncoded;
        }

        String engineId = AutomationGenerationUtils.findFirstModelEngine(user);
        if (engineId == null || engineId.isBlank()) {
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
                "Model engine " + engineId + " could not be loaded.");
        }

        String userMessage = buildUserMessage(nodeType, configJson);
        classLogger.info("GenerateNodeLabel: project={}, type={}", projectId, nodeType);

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("use_history", false);

        Map<String, Object> response;
        try {
            response = modelEngine.ask(SYSTEM_PROMPT + "\n\n" + userMessage,
                    null, this.insight, paramMap).toMap();
        } catch (Exception e) {
            classLogger.error("LLM call failed for GenerateNodeLabel on project {}", projectId, e);
            throw new RuntimeException("Label generation failed: " + e.getMessage(), e);
        }

        String label = AutomationGenerationUtils.extractResponseText(response);
        if (label == null || label.isBlank()) {
            throw new IllegalStateException("The AI model did not return a label. Try again.");
        }

        // Strip surrounding quotes the model sometimes adds, and cap length
        label = label.strip().replaceAll("^[\"']+|[\"']+$", "");
        if (label.length() > 50) {
            label = label.substring(0, 50);
        }

        return new NounMetadata(label, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    }

    /**
     * Builds a human-readable summary of the node config for the LLM prompt.
     * Skips engine IDs and other technical fields the model doesn't need to understand.
     */
    @SuppressWarnings("unchecked")
    private static String buildUserMessage(String nodeType, String configJson) {
        StringBuilder sb = new StringBuilder();
        sb.append("Step type: ").append(humanNodeType(nodeType)).append("\n");

        try {
            Map<String, Object> cfg = AutomationExecutionUtils.GSON.fromJson(configJson, AutomationExecutionUtils.MAP_TYPE);
            if (cfg == null) {
                sb.append("Config: (none)");
                return sb.toString();
            }

            switch (nodeType) {
                case "model-engine" -> {
                    appendField(sb, "Instruction", cfg.get("command"));
                    appendField(sb, "Input data", cfg.get("context"));
                    appendField(sb, "Operation", cfg.get("operation"));
                }
                case "database-engine" -> {
                    appendField(sb, "SQL query", cfg.get("expression"));
                    appendField(sb, "Operation", cfg.get("operation"));
                }
                case "vector-engine" -> {
                    appendField(sb, "Search query", cfg.get("command"));
                    appendField(sb, "Operation", cfg.get("operation"));
                    appendField(sb, "File", cfg.get("filePath"));
                }
                case "storage-engine" -> {
                    appendField(sb, "Operation", cfg.get("operation"));
                    appendField(sb, "Path", cfg.get("storagePath"));
                    appendField(sb, "File", cfg.get("filePath"));
                }
                case "function-engine" -> {
                    appendField(sb, "Parameters", cfg.get("params"));
                    appendField(sb, "Operation", cfg.get("operation"));
                }
                case "app" -> {
                    appendField(sb, "Pixel expression", cfg.get("pixel"));
                }
                case "wait" -> {
                    appendField(sb, "Seconds", cfg.get("seconds"));
                }
                default -> {
                    // No config details for trigger or unknown types
                }
            }
        } catch (Exception e) {
            sb.append("Config: (unparseable)");
        }

        return sb.toString();
    }

    private static void appendField(StringBuilder sb, String fieldName, Object value) {
        if (value == null) return;
        String s = value.toString().trim();
        if (s.isEmpty() || s.equals("PENDING_SQL_GENERATION") || s.equals("PENDING_PIXEL_EXPRESSION")) return;
        // Truncate very long values so they don't dominate the prompt
        if (s.length() > 200) s = s.substring(0, 200) + "...";
        sb.append(fieldName).append(": ").append(s).append("\n");
    }

    private static String humanNodeType(String type) {
        return switch (type) {
            case "model-engine" -> "Ask AI (model engine)";
            case "database-engine" -> "Query database";
            case "vector-engine" -> "Search documents (vector engine)";
            case "storage-engine" -> "File storage";
            case "function-engine" -> "Custom function";
            case "app" -> "Run pixel / app reactor";
            case "wait" -> "Pause / wait";
            case "trigger" -> "Trigger";
            default -> type;
        };
    }

    @Override
    public String getReactorDescription() {
        return "Generates a short 2–4 word action-oriented label for an automation node based on its type and config. "
            + "Uses the first available MODEL engine. Returns the suggested label as a string.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        return switch (key) {
            case "project" -> "The project ID the automation belongs to.";
            case "type" -> "The node type (e.g. model-engine, database-engine, vector-engine).";
            case "config" -> "Base64-encoded JSON of the node's current config object.";
            default -> super.getDescriptionForKey(key);
        };
    }
}
