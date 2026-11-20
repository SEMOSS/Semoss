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

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.SystemDefaultEngines;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Deletes a skill: removes any {@code WORKSPACE_RESOURCE__} rows that attach it
 * to a workspace, the matching {@code CONFIG_JSON.skills[]} mirror entries, and
 * the underlying skill-project (security rows, DIHelper entry, on-disk folder).
 *
 * <p>Mirrors {@code DeleteWorkspaceReactor}. The generic {@code DeleteProjectReactor}
 * also handles skill cleanup via its {@code PROJECT_TYPE.SKILL} branch, so callers
 * that already have a project-id may use that; this reactor exists for discoverability
 * and so the API surface for skills is symmetric (Create / Update / Delete / Clone).
 *
 * <p>Built-in platform skill projects cannot be deleted - they are restored from
 * the distribution on every boot, so deleting one would only leave the install
 * broken until redeploy.
 *
 * <p>Authorization: caller must be the project owner (matches the workspace and
 * project delete reactors), and the global "admin-only delete" toggle is honored.
 *
 * <p>Inputs:
 * <ul>
 *   <li>{@code skillId} - skill / project identifier (required)</li>
 * </ul>
 */
public class DeleteSkillReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteSkillReactor.class);

	private static final String SKILL_ID = "skillId";

	public DeleteSkillReactor() {
		this.keysToGet = new String[] { SKILL_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String skillId = this.keyValue.get(SKILL_ID);
		if (skillId == null || skillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}

		if (AbstractSecurityUtils.adminOnlySkillDelete()) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		// built-in platform skills ship with the distribution and reload at boot;
		// deleting one would remove its folder + smss with no way to restore it
		if (SystemDefaultEngines.getSystemSkills().contains(skillId)) {
			throw new IllegalArgumentException("Skill " + skillId
					+ " is a built-in platform skill and cannot be deleted");
		}

		if (!SecurityProjectUtils.userIsOwner(user, skillId)) {
			throw new IllegalArgumentException("Skill " + skillId
					+ " does not exist or user does not have permissions to delete it. "
					+ "User must be the owner to perform this function.");
		}

		// Verify this project is actually a skill before we tear it down, so a stray
		// project id can't accidentally invoke skill-specific cleanup paths.
		if (!AbstractSecurityUtils.containsProjectId(skillId)) {
			throw new IllegalArgumentException("Skill " + skillId + " does not exist");
		}
		IProject project = Utility.getProject(skillId);
		if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.SKILL) {
			throw new IllegalArgumentException("Project " + skillId + " is not a skill");
		}

		try {
			ModelInferenceLogsUtils.detachSkillFromAllWorkspaces(skillId);
			deleteProject(project);
			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteProjectRunner(skillId));
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete skill '{}'.", skillId, e);
			return getError("Error during skill delete: " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private boolean deleteProject(IProject project) {
		String projectId = project.getProjectId();
		UploadUtilities.removeProjectFromDIHelper(projectId);
		SecurityProjectUtils.deleteProject(projectId);
		UserTrackingUtils.deleteProject(projectId);
		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete skill project files for project '{}'.", projectId, e);
		}
		return true;
	}

	@Override
	public String getReactorDescription() {
		return "Deletes a skill: WORKSPACE_RESOURCE__ refs, CONFIG_JSON mirrors, and the underlying skill-project. "
				+ "Built-in platform skills cannot be deleted";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Identifier of the skill to delete (== underlying project id)";
		}
		return super.getDescriptionForKey(key);
	}
}
