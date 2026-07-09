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
 * Detaches a skill from a workspace. Mirrors
 * {@link AttachSkillToWorkspaceReactor} and handles both skill kinds, keyed by
 * which identifier you pass:
 *
 * <ul>
 * <li>{@code skillId} - a registry skill: removes the
 * {@code WORKSPACE_RESOURCE__} row and the matching
 * {@code CONFIG_JSON.skills[]} entry.</li>
 * <li>{@code slug} - a platform skill: removes the slug from
 * {@code CONFIG_JSON.platform_skills[]} (see {@link PlatformSkills}).</li>
 * </ul>
 *
 * <p>
 * Exactly one of {@code skillId} / {@code slug} must be supplied. No-op when
 * the attachment does not exist. Does not delete the skill itself; use
 * {@code DeleteSkillReactor} for registry skills (platform skills are read-only
 * built-ins and are never deleted through reactors).
 *
 * <p>
 * Authorization: user must have edit access to the workspace
 * ({@link SecurityProjectUtils#userCanEditProject}). No skill-side check -
 * detaching is always allowed for someone who can edit the workspace.
 *
 * <p>
 * Inputs:
 * <ul>
 * <li>{@code workspaceId} - workspace identifier (required)</li>
 * <li>{@code skillId} - registry skill to detach (required unless {@code slug}
 * given)</li>
 * <li>{@code slug} - platform skill to detach (required unless {@code skillId}
 * given)</li>
 * </ul>
 */
public class DetachSkillFromWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DetachSkillFromWorkspaceReactor.class);

	private static final String SKILL_ID = "skillId";
	private static final String SLUG = "slug";

	public DetachSkillFromWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), SKILL_ID, SLUG };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String skillId = nullIfBlank(this.keyValue.get(SKILL_ID));
		String slug = nullIfBlank(this.keyValue.get(SLUG));

		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		if (skillId == null && slug == null) {
			throw new IllegalArgumentException("either skillId or slug is required");
		}
		if (skillId != null && slug != null) {
			throw new IllegalArgumentException("provide exactly one of skillId or slug (slug => platform skill)");
		}

		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have permission to edit it");
		}

		Map<String, Object> response = new HashMap<>();
		response.put("workspace_id", workspaceId);

		// A slug (and no id) means a disk-backed platform skill.
		if (slug != null) {
			try {
				ModelInferenceLogsUtils.removePlatformSkillFromWorkspaceConfigJson(workspaceId, slug);
			} catch (Exception e) {
				classLogger.error("Failed to detach platform skill '{}' from workspace '{}'", slug, workspaceId, e);
				throw new IllegalArgumentException("Failed to detach platform skill from workspace: " + e.getMessage(),
						e);
			}
			response.put("slug", slug);
			response.put("type", PlatformSkills.PLATFORM_SKILL_TYPE);
			response.put("removed", true);
			return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
		}

		// Registry skill path.
		int deleted = ModelInferenceLogsUtils.deleteWorkspaceResource(workspaceId, skillId,
				AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE);
		response.put("skill_id", skillId);
		response.put("removed", deleted > 0);

		try {
			ModelInferenceLogsUtils.removeSkillFromWorkspaceConfigJson(workspaceId, skillId);
		} catch (Exception mirrorEx) {
			classLogger.warn(
					"Detached skill '{}' from workspace '{}' but failed to mirror removal out of CONFIG_JSON.skills",
					skillId, workspaceId, mirrorEx);
			response.put("warning", "Skill detached but CONFIG_JSON sync failed: " + mirrorEx.getMessage());
		}

		if (deleted > 0) {
			try {
				SecurityProjectUtils.removeProjectDependency(user, workspaceId, skillId);
			} catch (Exception depEx) {
				classLogger.warn(
						"Detached skill '{}' from workspace '{}' but failed to remove PROJECTDEPENDENCIES entry",
						skillId, workspaceId, depEx);
				response.put("dependency_warning",
						"Skill detached but PROJECTDEPENDENCIES sync failed: " + depEx.getMessage());
			}
		}

		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String nullIfBlank(String s) {
		if (s == null) {
			return null;
		}
		String t = s.trim();
		return t.isEmpty() ? null : t;
	}

	@Override
	public String getReactorDescription() {
		return "Detaches a skill from a workspace. Pass skillId for a registry skill (removes WORKSPACE_RESOURCE__ + "
				+ "CONFIG_JSON.skills[]) or slug for a platform skill (removes from CONFIG_JSON.platform_skills[]).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.WORKSPACE_ID.getKey().equals(key)) {
			return "Workspace identifier";
		}
		if (SKILL_ID.equals(key)) {
			return "Identifier of the registry skill to detach (omit when detaching a platform skill by slug)";
		}
		if (SLUG.equals(key)) {
			return "Folder name (slug) of the platform skill to detach (omit when detaching a registry skill by id)";
		}
		return super.getDescriptionForKey(key);
	}
}
