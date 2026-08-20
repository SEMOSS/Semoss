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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a new automation project and returns its ID.
 *
 * <p>The project name must start with a letter and contain only letters, numbers, and spaces
 * (enforced by {@code CreateProject}).
 *
 * <p>Pixel: {@code CreateAutomation(projectName=["My Claims Intake"])}
 */
public class CreateAutomationReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(CreateAutomationReactor.class);

    private static final String PROJECT_NAME_KEY = "projectName";

    private static final String RESULT_SUCCESS = "success";
    private static final String RESULT_PROJECT_ID = "projectId";
    private static final String RESULT_PROJECT_NAME = "projectName";
    private static final String RESULT_MESSAGE = "message";

    public CreateAutomationReactor() {
        this.keysToGet = new String[] { PROJECT_NAME_KEY };
        this.keyRequired = new int[] { 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String projectName = this.keyValue.get(PROJECT_NAME_KEY);

        if (projectName == null || projectName.trim().isEmpty()) {
            throw new IllegalArgumentException("Must provide a projectName");
        }
        projectName = projectName.trim();

        classLogger.info("CreateAutomationReactor: creating project '{}'", projectName);

        // Validate before injecting into the pixel string — only letters, numbers, and spaces; must start
        // with a letter. CreateProject enforces this too, but we reject here to prevent pixel injection.
        if (!projectName.matches("^[a-zA-Z][a-zA-Z0-9 ]*$")) {
            throw new IllegalArgumentException(
                    "Project name must start with a letter and contain only letters, numbers, and spaces. Got: " + projectName);
        }

        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You must be signed in to create an automation.");
        }

        IProject project = ProjectHelper.generateNewProject(projectName, IProject.PROJECT_TYPE.AUTOMATION,
                false, null, null, user, classLogger);
        String projectId = project.getProjectId();
        try {
            AutomationProjectUtils.createStarterDefinition(project, user);
        } catch (RuntimeException e) {
            classLogger.error("Failed to scaffold automation project '{}'", projectName, e);
            throw new IllegalStateException(
                    "Automation project was created but its starter assets could not be scaffolded: " + e.getMessage(), e);
        }

        classLogger.info("CreateAutomationReactor: created project '{}' with id {}", projectName, projectId);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put(RESULT_SUCCESS, true);
        result.put(RESULT_PROJECT_ID, projectId);
        result.put("project_id", projectId);
        result.put(RESULT_PROJECT_NAME, projectName);
        result.put(RESULT_MESSAGE,
                "Created automation project \"" + projectName + "\" (id: " + projectId + "). "
                + "Use GetAutomation and SaveAutomation to edit the workflow.");

        return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    @Override
    public String getReactorDescription() {
        return "Creates a new blank automation project and returns its ID. "
                + "Project names must start with a letter and contain only letters, numbers, and spaces.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (PROJECT_NAME_KEY.equals(key)) {
            return "Display name for the new automation project. "
                    + "Must start with a letter and contain only letters, numbers, and spaces.";
        }
        return super.getDescriptionForKey(key);
    }
}
