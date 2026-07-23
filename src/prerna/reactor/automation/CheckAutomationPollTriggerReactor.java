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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Poll-trigger reactor — called on a Quartz cron to check whether storage or
 * database state has changed since the last run. Fires {@code TriggerAutomation}
 * if a change is detected, then persists the new "last-seen" hash.
 *
 * <p>This reactor is designed to be used as the recipe for a Quartz scheduled
 * job (registered via {@code ScheduleJob(...)}) with the recipe:
 * <pre>
 *   CheckAutomationPollTrigger(project=["projectId"], type=["storage-poll"]);
 * </pre>
 * or
 * <pre>
 *   CheckAutomationPollTrigger(project=["projectId"], type=["db-poll"]);
 * </pre>
 *
 * <p>The poll configuration (engineId, path/query) is read from the automation's
 * trigger node config inside {@code automation.json}. The "last seen" state hash
 * is persisted in {@code automation-poll-state.json} in the portals folder.
 *
 * <p>State file format:
 * <pre>
 *   { "storage-poll": "&lt;sha256 of ListStoragePath output&gt;",
 *     "db-poll":      "&lt;sha256 of SQL result&gt;" }
 * </pre>
 */
public class CheckAutomationPollTriggerReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(CheckAutomationPollTriggerReactor.class);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private static final String POLL_STATE_FILE = "automation-poll-state.json";

    public CheckAutomationPollTriggerReactor() {
        this.keysToGet = new String[]{ "project", "type" };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);
        String pollType = this.keyValue.get(this.keysToGet[1]); // "storage-poll" or "db-poll"

        if (projectId == null || projectId.isBlank()) {
            throw new IllegalArgumentException("Must provide a project id");
        }
        if (pollType == null || pollType.isBlank()) {
            throw new IllegalArgumentException("Must provide a poll type (storage-poll or db-poll)");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);

        // Load trigger config from automation.json
        Map<String, Object> triggerConfig = loadTriggerConfig(projectId);
        if (triggerConfig == null) {
            classLogger.warn("No trigger config found for project {}", projectId);
            return noChange("No trigger config found");
        }

        // Execute the check pixel based on poll type
        String currentResult;
        String triggerType;
        if ("storage-poll".equals(pollType)) {
            currentResult = executeStorageCheck(triggerConfig);
            triggerType = AutomationConstants.TRIGGER_STORAGE_POLL;
        } else if ("db-poll".equals(pollType)) {
            currentResult = executeDbCheck(triggerConfig);
            triggerType = AutomationConstants.TRIGGER_DB_POLL;
        } else {
            throw new IllegalArgumentException("Unknown poll type: " + pollType + ". Expected storage-poll or db-poll");
        }

        if (currentResult == null) {
            return noChange("Check pixel returned no result");
        }

        String currentHash = sha256(currentResult);
        Map<String, String> state = loadPollState(projectId);
        String previousHash = state.getOrDefault(pollType, "");

        if (currentHash.equals(previousHash)) {
            classLogger.debug("Poll trigger for project {} ({}): no change detected", projectId, pollType);
            return noChange("No change detected");
        }

        // State changed — fire automation
        classLogger.info("Poll trigger for project {} ({}): change detected, firing automation", projectId, pollType);
        state.put(pollType, currentHash);
        savePollState(projectId, state);

        // Fire TriggerAutomation with the appropriate trigger type
        String pixel = "TriggerAutomation(project=[\"" + projectId + "\"], triggerType=[\"" + triggerType + "\"]);";
        try {
            this.insight.runPixel(pixel);
        } catch (Exception e) {
            classLogger.error("Failed to fire automation for project {} after change detected: {}", projectId, e.getMessage(), e);
            throw new IllegalStateException("Change detected but automation trigger failed: " + e.getMessage(), e);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("triggered", true);
        result.put("pollType", pollType);
        result.put("projectId", projectId);
        return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadTriggerConfig(String projectId) {
        try {
            String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
            File automationFile = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_FILE_NAME);
            if (!automationFile.exists()) return null;

            String json = Files.readString(automationFile.toPath(), StandardCharsets.UTF_8);
            Map<String, Object> doc = GSON.fromJson(json, new TypeToken<Map<String, Object>>(){}.getType());
            Map<String, Object> graph = (Map<String, Object>) doc.get("graph");
            if (graph == null) return null;
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) graph.get("nodes");
            if (nodes == null) return null;

            for (Map<String, Object> node : nodes) {
                if (AutomationConstants.NODE_TRIGGER.equals(node.get("type"))) {
                    Object cfg = node.get("config");
                    if (cfg instanceof Map) return (Map<String, Object>) cfg;
                }
            }
        } catch (Exception e) {
            classLogger.warn("Failed to load trigger config for {}: {}", projectId, e.getMessage());
        }
        return null;
    }

    private String executeStorageCheck(Map<String, Object> triggerConfig) {
        String engineId = str(triggerConfig.get("storagePollEngineId"));
        String path = str(triggerConfig.get("storagePollPath"));
        if (engineId == null || path == null) {
            classLogger.warn("Storage poll config incomplete: engineId={}, path={}", engineId, path);
            return null;
        }
        String pixel = "ListStoragePath(storage=[\"" + engineId + "\"], storagePath=[\"" + path + "\"]);";
        try {
            Object out = this.insight.runPixel(pixel);
            return out != null ? GSON.toJson(out) : null;
        } catch (Exception e) {
            classLogger.warn("Storage poll check failed: {}", e.getMessage());
            return null;
        }
    }

    private String executeDbCheck(Map<String, Object> triggerConfig) {
        String engineId = str(triggerConfig.get("dbPollEngineId"));
        String query = str(triggerConfig.get("dbPollQuery"));
        if (engineId == null || query == null) {
            classLogger.warn("DB poll config incomplete: engineId={}, query={}", engineId, query);
            return null;
        }
        String pixel = "SqlQuery(database=[\"" + engineId + "\"], query=[\"<encode>" + query + "</encode>\"]);";
        try {
            Object out = this.insight.runPixel(pixel);
            return out != null ? GSON.toJson(out) : null;
        } catch (Exception e) {
            classLogger.warn("DB poll check failed: {}", e.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> loadPollState(String projectId) {
        try {
            String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
            File stateFile = new File(portalsFolder + "/" + POLL_STATE_FILE);
            if (!stateFile.exists()) return new HashMap<>();
            String json = Files.readString(stateFile.toPath(), StandardCharsets.UTF_8);
            Map<String, String> state = GSON.fromJson(json, new TypeToken<Map<String, String>>(){}.getType());
            return state != null ? state : new HashMap<>();
        } catch (Exception e) {
            classLogger.warn("Could not load poll state for {}: {}", projectId, e.getMessage());
            return new HashMap<>();
        }
    }

    private void savePollState(String projectId, Map<String, String> state) {
        try {
            String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
            File stateFile = new File(portalsFolder + "/" + POLL_STATE_FILE);
            stateFile.getParentFile().mkdirs();
            Files.writeString(stateFile.toPath(), GSON.toJson(state), StandardCharsets.UTF_8);
        } catch (Exception e) {
            classLogger.warn("Could not save poll state for {}: {}", projectId, e.getMessage());
        }
    }

    private static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return String.valueOf(input.hashCode());
        }
    }

    private static String str(Object v) {
        return (v != null && !v.toString().isBlank()) ? v.toString() : null;
    }

    private static NounMetadata noChange(String reason) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("triggered", false);
        r.put("reason", reason);
        return new NounMetadata(r, PixelDataType.MAP, PixelOperationType.OPERATION);
    }
}
