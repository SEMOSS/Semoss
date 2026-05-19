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
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
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
 * <p>The attachment uses the same {@code WORKSPACE_RESOURCE__} pattern as MCPs
 * and prompts so {@code AgentConfigLoader} surfaces the skill in the merged
 * config that {@code SkillStager} consumes at agent run time. Idempotent — a
 * second call with the same {@code (workspaceId, skillId)} updates the pinned
 * version (or no-ops when the subtype already matches).
 *
 * <p>Inputs:
 * <ul>
 *   <li>{@code workspaceId} — target workspace (required)</li>
 *   <li>{@code skillId}     — skill to attach (required)</li>
 *   <li>{@code version}     — optional pinned version number; null/omitted means "track CURRENT_VERSION"</li>
 * </ul>
 *
 * <p>Authorization:
 * <ul>
 *   <li>User must have edit access to the workspace ({@link SecurityProjectUtils#userCanEditProject})</li>
 *   <li>User must be able to view the skill. v1 rule: the skill must have {@code ORIGIN='PLATFORM'} OR the user
 *       must be its {@code CREATED_BY}. Full SKILLPERMISSION / GROUPSKILLPERMISSION resolution lands with
 *       {@code SecuritySkillUtils} in a later step.</li>
 * </ul>
 */
public class AttachSkillToWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AttachSkillToWorkspaceReactor.class);

	private static final String SKILL_ID = "skillId";
	private static final String VERSION  = "version";

	public AttachSkillToWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), SKILL_ID, VERSION };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String skillId     = this.keyValue.get(SKILL_ID);
		String versionStr  = this.keyValue.get(VERSION);

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
		if (!userCanViewSkill(user, skillRow)) {
			throw new IllegalArgumentException("User does not have permission to attach skill: " + skillId);
		}

		String pinnedVersion = null;
		if (versionStr != null && !versionStr.isEmpty()) {
			int version;
			try {
				version = Integer.parseInt(versionStr);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("version must be an integer: " + versionStr);
			}
			if (ModelInferenceLogsUtils.getSkillVersion(skillId, version) == null) {
				throw new IllegalArgumentException("Skill " + skillId + " has no version " + version);
			}
			pinnedVersion = Integer.toString(version);
		}

		try {
			Map<String, Object> existing = ModelInferenceLogsUtils.findWorkspaceResource(workspaceId, skillId,
					AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE);
			String workspaceResourceId;
			boolean created;
			if (existing != null) {
				if (Objects.equals(existing.get("resource_subtype"), pinnedVersion)) {
					// already attached with same pin — no-op
					workspaceResourceId = (String) existing.get("workspace_resource_id");
					created = false;
				} else {
					// updating the version pin — delete + re-insert
					ModelInferenceLogsUtils.deleteWorkspaceResource(workspaceId, skillId,
							AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE);
					workspaceResourceId = GUID.v7().toString();
					ModelInferenceLogsUtils.createNewWorkspaceResource(workspaceResourceId, workspaceId, skillId,
							AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE, pinnedVersion);
					created = true;
				}
			} else {
				workspaceResourceId = GUID.v7().toString();
				ModelInferenceLogsUtils.createNewWorkspaceResource(workspaceResourceId, workspaceId, skillId,
						AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE, pinnedVersion);
				created = true;
			}

			Map<String, Object> response = new HashMap<>();
			response.put("workspace_resource_id", workspaceResourceId);
			response.put("workspace_id", workspaceId);
			response.put("skill_id", skillId);
			response.put("pinned_version", pinnedVersion);
			response.put("created", created);
			return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to attach skill to workspace: " + e.getMessage(), e);
		}
	}

	/**
	 * v1 visibility rule. Replace with {@code SecuritySkillUtils.userCanViewSkill}
	 * when the security utility lands.
	 */
	private static boolean userCanViewSkill(User user, Map<String, Object> skillRow) {
		String origin = (String) skillRow.get("origin");
		if (Skill.ORIGIN_PLATFORM.equals(origin)) {
			return true;
		}
		String createdBy = (String) skillRow.get("created_by");
		String userId = resolveUserId(user);
		if (userId != null && userId.equals(createdBy)) {
			return true;
		}
		return Boolean.TRUE.equals(SecurityAdminUtils.userIsAdmin(user));
	}

	private static String resolveUserId(User user) {
		if (user == null || user.getLogins() == null || user.getLogins().isEmpty()) {
			return null;
		}
		AuthProvider login = user.getLogins().get(0);
		return user.getAccessToken(login) == null ? null : user.getAccessToken(login).getId();
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
		if (VERSION.equals(key)) {
			return "Optional pinned skill version. Omit/null tracks SKILL.CURRENT_VERSION";
		}
		return super.getDescriptionForKey(key);
	}
}
