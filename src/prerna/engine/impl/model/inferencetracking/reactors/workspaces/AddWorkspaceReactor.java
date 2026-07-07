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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddWorkspaceReactor extends AbstractWorkspaceReactor {

	private static final Logger classLogger = LogManager.getLogger(AddWorkspaceReactor.class);
	private static final String CLASS_NAME = AddWorkspaceReactor.class.getName();

	public AddWorkspaceReactor() {
		this.keysToGet = new String[] { NAME, DESCRIPTION, SYSTEM_PROMPT, ReactorKeysEnum.MCP.getKey(), PROMPTS,
				SKILLS, PLATFORM_SKILLS };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();

		User user = this.insight.getUser();

		String workspaceId = UUID.randomUUID().toString();
		String workspaceName = this.keyValue.get(NAME);

		if (!Utility.validateName(workspaceName)) {
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		String workspaceDescription = this.keyValue.get(DESCRIPTION);
		String workspaceSystemPrompt = this.keyValue.get(SYSTEM_PROMPT);

		Set<String> engines = new HashSet<>();
		Set<String> projectDependencies = new HashSet<>();
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		List<Map<String, String>> workspaceResources = new ArrayList<>();
		Set<String> skillIds = new LinkedHashSet<>();
		Set<String> platformSkills = getGenRowStruct(PLATFORM_SKILLS) != null ? new LinkedHashSet<>() : null;
		try {
			validateWorkspaceInputs(user, workspaceId, null, null, engines, projectDependencies, dependencyList,
					workspaceResources, skillIds, platformSkills);
		} catch (IllegalArgumentException e) {
			return getError(e.getMessage());
		}

		IProject workspaceProject = null;
		try {
			workspaceProject = ProjectHelper.createWorkspaceProject(workspaceId, workspaceName,
					IProject.PROJECT_TYPE.WORKSPACE, false, false, null, null, null, user, logger);
			SecurityProjectUtils.updateProjectDependencies(user, workspaceId, dependencyList);
			ModelInferenceLogsUtils.createNewWorkspaceEntry(workspaceId, user.getPrimaryLoginToken().getId(),
					workspaceName, workspaceDescription, workspaceSystemPrompt, workspaceResources);
			try {
				mirrorCoreFieldsIntoConfigJson(workspaceId, workspaceSystemPrompt, engines, projectDependencies,
						skillIds, platformSkills);
			} catch (Exception mirrorEx) {
				classLogger.warn(
						"Created workspace '{}' but failed to mirror system_prompt/mcps/skills into CONFIG_JSON (legacy writes already succeeded)",
						workspaceId, mirrorEx);
			}
		} catch (Exception e) {
			classLogger.error("Failed to create workspace '{}' (ID: {}).", workspaceName, workspaceId, e);
			if (workspaceProject != null) {
				try {
					workspaceProject.delete();
				} catch (IOException e2) {
					classLogger.error("Failed to delete partially created workspace project '{}'.", workspaceId, e2);
				}
			}
			try {
				ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
			} catch (Exception e2) {
				classLogger.error("Failed to rollback workspace inference log entry for workspace '{}'.", workspaceId,
						e2);
			}

			return getError("Failed to create workspace: " + e.getMessage());
		}

		return getSuccess(workspaceId);
	}

}
