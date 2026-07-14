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
package prerna.reactor.workflow;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Generates (or regenerates) a webhook secret for a workflow project.
 *
 * <p>Pixel: {@code GenerateWorkflowWebhookSecret(project=["projectId"])}
 *
 * <p>Stores the secret in {@code workflow-config.json} under the key
 * {@code WEBHOOK_SECRET} (marked sensitive=true). Returns the plain-text
 * secret once — it is not retrievable again from the API.
 */
public class GenerateWorkflowWebhookSecretReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GenerateWorkflowWebhookSecretReactor.class);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private static final String WEBHOOK_SECRET_KEY = "WEBHOOK_SECRET";
    private static final String WEBHOOK_USER_KEY = "WEBHOOK_USER";
    private static final String WORKFLOW_CONFIG_FILE = "workflow-config.json";

    public GenerateWorkflowWebhookSecretReactor() {
        this.keysToGet = new String[]{ "project" };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectId = this.keyValue.get(this.keysToGet[0]);
        if (projectId == null || projectId.isBlank()) {
            throw new IllegalArgumentException("Must provide a project id");
        }

        projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
        if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
            throw new IllegalArgumentException("Project does not exist or user does not have edit access");
        }

        String secret = UUID.randomUUID().toString().replace("-", "") +
                        UUID.randomUUID().toString().replace("-", "");

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File configFile = new File(portalsFolder + "/" + WORKFLOW_CONFIG_FILE);

        List<Map<String, Object>> entries = new ArrayList<>();
        if (configFile.exists()) {
            try {
                String json = Files.readString(configFile.toPath(), StandardCharsets.UTF_8);
                List<Map<String, Object>> existing = GSON.fromJson(json,
                        new TypeToken<List<Map<String, Object>>>(){}.getType());
                if (existing != null) {
                    // copy all entries except the existing WEBHOOK_SECRET / WEBHOOK_USER
                    for (Map<String, Object> e : existing) {
                        Object key = e.get("key");
                        if (!WEBHOOK_SECRET_KEY.equals(key) && !WEBHOOK_USER_KEY.equals(key)) {
                            entries.add(e);
                        }
                    }
                }
            } catch (Exception e) {
                classLogger.warn("Could not parse existing workflow config for {}: {}", projectId, e.getMessage());
            }
        }

        // Build "PROVIDER:id,PROVIDER:id" string for the executing user
        User callingUser = this.insight.getUser();
        StringBuilder userAccessBuilder = new StringBuilder();
        for (AuthProvider provider : callingUser.getLogins()) {
            AccessToken token = callingUser.getAccessToken(provider);
            if (token != null) {
                if (userAccessBuilder.length() > 0) userAccessBuilder.append(",");
                userAccessBuilder.append(provider.name()).append(":").append(token.getId());
            }
        }

        Map<String, Object> secretEntry = new LinkedHashMap<>();
        secretEntry.put("key", WEBHOOK_SECRET_KEY);
        secretEntry.put("value", secret);
        secretEntry.put("sensitive", true);
        entries.add(secretEntry);

        Map<String, Object> userEntry = new LinkedHashMap<>();
        userEntry.put("key", WEBHOOK_USER_KEY);
        userEntry.put("value", userAccessBuilder.toString());
        userEntry.put("sensitive", true);
        entries.add(userEntry);

        try {
            configFile.getParentFile().mkdirs();
            Files.writeString(configFile.toPath(), GSON.toJson(entries), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to save webhook secret: " + e.getMessage(), e);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("secret", secret);
        result.put("projectId", projectId);
        result.put("note", "Store this secret securely — it cannot be retrieved again. Pass it in the X-Webhook-Secret header when calling the webhook endpoint.");
        return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
    }
}
