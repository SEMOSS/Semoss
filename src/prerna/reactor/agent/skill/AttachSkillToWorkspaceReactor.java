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
package prerna.reactor.agent.skill;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

/**
 * Attaches a skill from the registry to a workspace by inserting a
 * {@code WORKSPACE_RESOURCE__} row with {@code RESOURCE_TYPE='SKILL'}.
 *
 * <p>Since a skill is itself a Project (type=SKILL), authorization piggybacks on
 * project permissions: the user must be able to edit the workspace project and
 * view the skill project. Idempotent - a second call with the same
 * {@code (workspaceId, skillId)} no-ops.
 *
 * <p>Inputs:
 * <ul>
 *   <li>{@code workspaceId} - target workspace (required)</li>
 *   <li>{@code skillId}     - skill to attach (required)</li>
 * </ul>
 */
public class AttachSkillToWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AttachSkillToWorkspaceReactor.class);

	private static final String SKILL_ID = "skillId";

	public AttachSkillToWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), SKILL_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String skillId     = this.keyValue.get(SKILL_ID);

		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		if (skillId == null || skillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}

		User user = this.insight.getUser();

		if (!SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have permission to edit it");
		}

		Map<String, Object> skillRow = ModelInferenceLogsUtils.getSkillEntry(skillId);
		if (skillRow == null) {
			throw new IllegalArgumentException("Skill not found: " + skillId);
		}
		if (!SecurityProjectUtils.userCanViewProject(user, skillId)) {
			throw new IllegalArgumentException("User does not have permission to attach skill: " + skillId);
		}

		try {
			Map<String, Object> existing = ModelInferenceLogsUtils.findWorkspaceResource(workspaceId, skillId,
					AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE);
			String workspaceResourceId;
			boolean created;
			if (existing != null) {
				workspaceResourceId = (String) existing.get("workspace_resource_id");
				created = false;
			} else {
				workspaceResourceId = GUID.v7().toString();
				ModelInferenceLogsUtils.createNewWorkspaceResource(workspaceResourceId, workspaceId, skillId,
						AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE, null);
				created = true;
			}

			Map<String, Object> response = new HashMap<>();
			response.put("workspace_resource_id", workspaceResourceId);
			response.put("workspace_id", workspaceId);
			response.put("skill_id", skillId);
			response.put("created", created);
			return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to attach skill to workspace: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Attaches a skill from the registry to a workspace (WORKSPACE_RESOURCE__ with RESOURCE_TYPE='SKILL'). Idempotent.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.WORKSPACE_ID.getKey().equals(key)) {
			return "Target workspace identifier";
		}
		if (SKILL_ID.equals(key)) {
			return "Identifier of the skill to attach";
		}
		return super.getDescriptionForKey(key);
	}
}
