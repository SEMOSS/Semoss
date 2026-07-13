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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EditWorkspaceReactor extends AbstractWorkspaceReactor {

	private static final Logger classLogger = LogManager.getLogger(EditWorkspaceReactor.class);

	public EditWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), NAME, DESCRIPTION, SYSTEM_PROMPT,
				IS_ACTIVE, ReactorKeysEnum.MCP.getKey(), PROMPTS, SKILLS, MODEL_ID };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String workspaceName = this.keyValue.get(NAME);
		String workspaceDescription = this.keyValue.get(DESCRIPTION);
		String workspaceSystemPrompt = this.keyValue.get(SYSTEM_PROMPT);
		boolean isActive = !"false".equalsIgnoreCase(this.keyValue.get(IS_ACTIVE));

		Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (current == null) {
			throw new IllegalArgumentException("Workspace not found");
		}

		Boolean currentlyActive = (Boolean) current.get("is_active");

		if (!SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}

		if (!currentlyActive && isActive) {
			if (SecurityProjectUtils.userIsOwner(user, workspaceId)) {
				ModelInferenceLogsUtils.doSetWorksapceToActive(workspaceId);
			} else {
				throw new IllegalArgumentException("User must be an owner to set the workspace to active");
			}
		}

		if (currentlyActive && !isActive) {
			if (SecurityProjectUtils.userIsOwner(user, workspaceId)) {
				ModelInferenceLogsUtils.doSetWorksapceToActive(workspaceId);
			} else {
				throw new IllegalArgumentException("User must be an owner to set the workspace to inactive");
			}
		}

		Set<String> curDepList = SecurityProjectUtils.getProjectDependencies(workspaceId, false).stream()
				.map(map -> (String) map.get("engine_id")).collect(Collectors.toSet());
		Set<String> curSkillList = ModelInferenceLogsUtils
				.getWorkspaceResources(workspaceId, SKILL_RESOURCE_TYPE, null).stream()
				.map(map -> (String) map.get("resource_id")).collect(Collectors.toSet());

		Set<String> engines = new HashSet<>();
		Set<String> projectDependencies = new HashSet<>();
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		List<Map<String, String>> workspaceResources = new ArrayList<>();
		Set<String> skillIds = new LinkedHashSet<>();
		try {
			validateWorkspaceInputs(user, workspaceId, curDepList, curSkillList, engines, projectDependencies,
					dependencyList, workspaceResources, skillIds);
		} catch (IllegalArgumentException e) {
			return getError(e.getMessage());
		}

		SecurityProjectUtils.updateProjectDependencies(user, workspaceId, dependencyList);

		// Default/fallback model engine for the agent. Presence-detected so omitting
		// the key leaves any existing CONFIG_JSON.model_id untouched; passing it blank
		// clears it (falling back to room MODEL_ID / options at run time).
		boolean modelIdProvided = getGenRowStruct(MODEL_ID) != null;
		String workspaceModelId = modelIdProvided ? this.keyValue.get(MODEL_ID) : null;

		try {
			ModelInferenceLogsUtils.updateWorkspaceEntry(workspaceId, workspaceName, workspaceDescription,
					workspaceSystemPrompt, isActive, workspaceResources);
		} catch (Exception e) {
			classLogger.error("Failed to update workspace '{}' (ID: {}).", workspaceName, workspaceId, e);
			return getError("Error during workspace update: " + e.getMessage());
		}

		// Dual-write CONFIG_JSON: mirror system_prompt + MCP engine/project refs +
		// skill refs into WORKSPACE.CONFIG_JSON. Preserve any other CONFIG_JSON fields
		// (e.g. hooks) the workspace already has. Best-effort - a failure here is logged
		// but does not fail the reactor, since the legacy SYSTEM_PROMPT column +
		// WORKSPACE_RESOURCE rows already landed and AgentConfigLoader still resolves
		// correctly from those.
		try {
			mirrorCoreFieldsIntoConfigJson(workspaceId, workspaceSystemPrompt, engines, projectDependencies, skillIds,
					modelIdProvided, workspaceModelId);
		} catch (Exception e) {
			classLogger.warn(
					"Failed to mirror system_prompt/mcps/skills into CONFIG_JSON for workspaceId '{}' (legacy writes already succeeded)",
					workspaceId, e);
			Map<String, Object> partial = new HashMap<>();
			partial.put("success", true);
			partial.put("warning", "Workspace saved but CONFIG_JSON sync failed: " + e.getMessage());
			return new NounMetadata(partial, PixelDataType.MAP);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
