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
import java.io.IOException;
import java.util.ArrayList;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class GetWorkflowConfigReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetWorkflowConfigReactor.class);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public GetWorkflowConfigReactor() {
        this.keysToGet = new String[]{ "project" };
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
        File configFile = new File(portalsFolder + "/" + WorkflowConstants.WORKFLOW_CONFIG_FILE_NAME);

        if (!configFile.exists()) {
            return new NounMetadata(new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        }

        try {
            String json = Files.readString(configFile.toPath(), StandardCharsets.UTF_8);
            // strip sensitive values before returning
            List<Map<String, Object>> entries = GSON.fromJson(json, new TypeToken<List<Map<String, Object>>>() {}.getType());
            if (entries != null) {
                for (Map<String, Object> entry : entries) {
                    Object sensitive = entry.get("sensitive");
                    if (Boolean.TRUE.equals(sensitive)) {
                        entry.put("value", WorkflowConstants.SENSITIVE_MASK);
                    }
                }
            }
            return new NounMetadata(entries != null ? entries : new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        } catch (IOException e) {
            classLogger.error("Error reading workflow config", e);
            return new NounMetadata(new ArrayList<>(), PixelDataType.VECTOR, PixelOperationType.OPERATION);
        }
    }
}
