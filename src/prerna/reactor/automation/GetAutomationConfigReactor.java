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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Returns the automation environment config ({@code automation_config.json}) for a project.
 * Sensitive values are masked in the response — only the key and a placeholder are returned.
 *
 * <p>There are three "get" reactors — each reads something different:
 * <ul>
 *   <li>{@link GetAutomationReactor GetAutomation} — reads {@code automation.json}: the pipeline graph</li>
 *   <li>{@code GetAutomationConfig} (this reactor) — reads {@code automation_config.json}: key/value env vars and secrets</li>
 *   <li>{@link GetAutomationRunReactor GetAutomationRun} — reads live run state from the DB</li>
 * </ul>
 *
 * <p>Pixel: {@code GetAutomationConfig(project=["appId"])}
 */
public class GetAutomationConfigReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetAutomationConfigReactor.class);

    public GetAutomationConfigReactor() {
        this.keysToGet = new String[] { "project" };
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

        String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
        File configFile = new File(portalsFolder + "/" + AutomationConstants.AUTOMATION_CONFIG_FILE_NAME);

        if (!configFile.exists()) {
            return new NounMetadata(new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        }

        try {
            String json = Files.readString(configFile.toPath(), StandardCharsets.UTF_8);
            List<Map<String, Object>> entries = AutomationExecutionUtils.GSON.fromJson(json, new TypeToken<List<Map<String, Object>>>() {}.getType());
            if (entries != null) {
                for (Map<String, Object> entry : entries) {
                    Object sensitive = entry.get("sensitive");
                    if (Boolean.TRUE.equals(sensitive)) {
                        entry.put("value", AutomationConstants.SENSITIVE_MASK);
                    }
                }
            }
            return new NounMetadata(entries != null ? entries : new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        } catch (IOException e) {
            classLogger.error("Error reading automation_config.json for project {}", projectId, e);
            return new NounMetadata(new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Returns the automation environment config (automation_config.json) for a project; sensitive values are masked.";
    }
}
