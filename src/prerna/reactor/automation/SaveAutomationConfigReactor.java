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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
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

public class SaveAutomationConfigReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(SaveAutomationConfigReactor.class);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public SaveAutomationConfigReactor() {
        this.keysToGet = new String[]{ "project", "config" };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);
        String configEncoded = this.keyValue.get(this.keysToGet[1]);

        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a project id");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have edit access");
        }

        String config;
        try {
            config = URLDecoder.decode(configEncoded != null ? configEncoded : "[]", StandardCharsets.UTF_8);
        } catch (Exception e) {
            config = configEncoded != null ? configEncoded : "[]";
        }

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File configFile = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_CONFIG_FILE_NAME);

        // GetAutomationConfig masks sensitive values as SENSITIVE_MASK.
        // Restore the real values from disk so a config round-trip cannot silently destroy them.
        config = restoreMaskedSensitiveValues(config, configFile);

        try {
            configFile.getParentFile().mkdirs();
            Files.writeString(configFile.toPath(), config, StandardCharsets.UTF_8);
        } catch (IOException e) {
            classLogger.error("Error saving automation config", e);
            throw new IllegalArgumentException("Unable to save automation config: " + e.getMessage());
        }

        SecurityProjectUtils.updateProjectLastEditedDate(projectId);
        return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
    }

    /**
     * For every incoming entry marked {@code sensitive} whose value is still the
     * {@link AutomationConstants#SENSITIVE_MASK} placeholder, restore the real value from the
     * existing on-disk config (matched by {@code key}). Prevents a save from the UI - which
     * only ever received the masked value - from overwriting the stored secret.
     */
    @SuppressWarnings("unchecked")
    private String restoreMaskedSensitiveValues(String incomingJson, File existingFile) {
        if (existingFile == null || !existingFile.exists()) {
            return incomingJson;
        }
        try {
            List<Map<String, Object>> incoming = GSON.fromJson(incomingJson,
                    new TypeToken<List<Map<String, Object>>>() {}.getType());
            String existingJson = Files.readString(existingFile.toPath(), StandardCharsets.UTF_8);
            List<Map<String, Object>> existing = GSON.fromJson(existingJson,
                    new TypeToken<List<Map<String, Object>>>() {}.getType());
            if (incoming == null || existing == null || existing.isEmpty()) {
                return incomingJson;
            }

            Map<String, Object> existingValueByKey = new HashMap<>();
            for (Map<String, Object> e : existing) {
                existingValueByKey.put(String.valueOf(e.get("key")), e.get("value"));
            }

            boolean restoredAny = false;
            for (Map<String, Object> entry : incoming) {
                if (Boolean.TRUE.equals(entry.get("sensitive"))
                        && AutomationConstants.SENSITIVE_MASK.equals(entry.get("value"))) {
                    Object real = existingValueByKey.get(String.valueOf(entry.get("key")));
                    if (real != null) {
                        entry.put("value", real);
                        restoredAny = true;
                    }
                }
            }
            return restoredAny ? GSON.toJson(incoming) : incomingJson;
        } catch (Exception e) {
            // Never let a merge problem overwrite good secrets - keep the existing file untouched.
            classLogger.warn("Could not merge sensitive automation config; keeping existing file. Cause: {}",
                    e.getMessage());
            try {
                return Files.readString(existingFile.toPath(), StandardCharsets.UTF_8);
            } catch (IOException io) {
                return incomingJson;
            }
        }
    }
}
