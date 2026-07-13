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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Attaches a skill to a workspace. A skill is a Project of type {@code SKILL}
 * (this includes the built-in platform skill projects, whose ids are their
 * folder names, e.g. {@code database}). Inserts a {@code WORKSPACE_RESOURCE__}
 * row with {@code RESOURCE_TYPE='SKILL'} and mirrors it into
 * {@code CONFIG_JSON.skills[]}. Authorization piggybacks on project
 * permissions: edit on the workspace, view on the skill. Idempotent.
 *
 * <p>Inputs:
 * <ul>
 *   <li>{@code workspaceId} - target workspace (required)</li>
 *   <li>{@code skillId}     - skill project to attach (required)</li>
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
		String skillId     = nullIfBlank(this.keyValue.get(SKILL_ID));

		if (workspaceId == null || workspaceId.isEmpty()) {
			throw new IllegalArgumentException("workspaceId is required");
		}
		if (skillId == null) {
			throw new IllegalArgumentException("skillId is required");
		}

		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have permission to edit it");
		}

		return attachSkill(user, workspaceId, skillId);
	}

	/** Insert the WORKSPACE_RESOURCE__ row and mirror into CONFIG_JSON.skills[]. */
	private NounMetadata attachSkill(User user, String workspaceId, String skillId) {
		if (!SkillProjects.isSkillProject(skillId)) {
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

			try {
				ModelInferenceLogsUtils.addSkillToWorkspaceConfigJson(workspaceId, skillId, null);
			} catch (Exception mirrorEx) {
				classLogger.warn("Attached skill '{}' to workspace '{}' but failed to mirror into CONFIG_JSON.skills",
						skillId, workspaceId, mirrorEx);
				response.put("warning", "Skill attached but CONFIG_JSON sync failed: " + mirrorEx.getMessage());
			}

			// Mirror the attachment into PROJECTDEPENDENCIES so the skill shows up as a
			// project dependency alongside MCP engines/projects. Bulk update via merge:
			// read current deps, append the skill if not already present, rewrite.
			// Best-effort - WORKSPACE_RESOURCE row is the source of truth.
			try {
				List<Map<String, Object>> current = SecurityProjectUtils.getProjectDependencies(workspaceId, false);
				List<Map<String, Object>> merged = new ArrayList<>();
				boolean alreadyPresent = false;
				for (Map<String, Object> row : current) {
					String existingId = (String) row.get("engine_id");
					String existingType = (String) row.get("engine_type");
					Map<String, Object> entry = new HashMap<>();
					entry.put("ENGINEID", existingId);
					entry.put("ENGINETYPE", existingType);
					merged.add(entry);
					if (skillId.equals(existingId)) {
						alreadyPresent = true;
					}
				}
				if (!alreadyPresent) {
					Map<String, Object> skillDep = new HashMap<>();
					skillDep.put("ENGINEID", skillId);
					skillDep.put("ENGINETYPE", CATALOG_TYPE.PROJECT.name());
					merged.add(skillDep);
					SecurityProjectUtils.updateProjectDependencies(user, workspaceId, merged);
				}
			} catch (Exception depEx) {
				classLogger.warn("Attached skill '{}' to workspace '{}' but failed to record PROJECTDEPENDENCIES entry",
						skillId, workspaceId, depEx);
				response.put("dependency_warning",
						"Skill attached but PROJECTDEPENDENCIES sync failed: " + depEx.getMessage());
			}

			return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to attach skill '{}' to workspace '{}'", skillId, workspaceId, e);
			throw new IllegalArgumentException("Failed to attach skill to workspace: " + e.getMessage(), e);
		}
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
		return "Attaches a skill (a SKILL-type project) to a workspace: inserts the WORKSPACE_RESOURCE__ row and "
				+ "mirrors it into CONFIG_JSON.skills[]. Idempotent.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.WORKSPACE_ID.getKey().equals(key)) {
			return "Target workspace identifier";
		}
		if (SKILL_ID.equals(key)) {
			return "Identifier of the skill project to attach (== the skill's project id)";
		}
		return super.getDescriptionForKey(key);
	}
}
