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

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Removes the {@code WORKSPACE_RESOURCE__} row that links a skill to a workspace.
 * No-op when no such attachment exists. Does not delete the skill itself; use
 * {@code DeleteSkillReactor} for that.
 *
 * <p>Inputs:
 * <ul>
 *   <li>{@code workspaceId} - workspace identifier (required)</li>
 *   <li>{@code skillId}     - skill identifier to detach (required)</li>
 * </ul>
 *
 * <p>Authorization: user must have edit access to the workspace
 * ({@link SecurityProjectUtils#userCanEditProject}). No skill-side check -
 * detaching is always allowed for someone who can edit the workspace.
 */
public class DetachSkillFromWorkspaceReactor extends AbstractReactor {

	private static final String SKILL_ID = "skillId";

	public DetachSkillFromWorkspaceReactor() {
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

		int deleted = ModelInferenceLogsUtils.deleteWorkspaceResource(workspaceId, skillId,
				AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE);

		Map<String, Object> response = new HashMap<>();
		response.put("workspace_id", workspaceId);
		response.put("skill_id", skillId);
		response.put("removed", deleted > 0);
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Detaches a skill from a workspace by removing the WORKSPACE_RESOURCE__ row";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.WORKSPACE_ID.getKey().equals(key)) {
			return "Workspace identifier";
		}
		if (SKILL_ID.equals(key)) {
			return "Identifier of the skill to detach";
		}
		return super.getDescriptionForKey(key);
	}
}
