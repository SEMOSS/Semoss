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
package prerna.auth.utils.reactors.admin;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.SystemDefaultEngines;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Deletes a skill for an admin, whatever permission the admin holds on it. The
 * skill's links to workspaces go first, then its project is removed from the
 * security database, from user tracking and from disk, and in a cluster from
 * cloud storage. The delete is recorded in the audit trail. The skills built
 * into the platform are kept. This is the admin counterpart to
 * {@link prerna.reactor.agent.skill.DeleteSkillReactor}, which deletes a skill
 * its user owns.
 */
public class AdminDeleteSkillReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminDeleteSkillReactor.class);

	private static final String SKILL_ID = "skillId";

	/**
	 * Creates the reactor with the skill key.
	 */
	public AdminDeleteSkillReactor() {
		this.keysToGet = new String[] { SKILL_ID };
		this.keyRequired = new int[] { 1 };
	}

	/**
	 * Deletes the skill given, once the user is known to be an admin. A skill that
	 * does not exist, is built into the platform, or is a project of another type
	 * stops the run with an error.
	 *
	 * @return true once the skill is deleted, or an error when the delete fails
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String skillId = this.keyValue.get(SKILL_ID);
		if (skillId == null || skillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}
		if (SystemDefaultEngines.getSystemSkills().contains(skillId)) {
			throw new IllegalArgumentException(
					"Skill " + skillId + " is a built-in platform skill and cannot be deleted");
		}
		if (!SecurityProjectUtils.projectExists(skillId)) {
			throw new IllegalArgumentException("Skill " + skillId + " does not exist");
		}
		IProject project = Utility.getProject(skillId);
		if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.SKILL) {
			throw new IllegalArgumentException("Project " + skillId + " is not a skill");
		}

		String skillName = project.getProjectName();
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
		UserAuditTrailUtils.recordResourceEvent(user, AuditEventType.SKILL_DELETE, AuditTargetType.SKILL, skillId,
				skillName, skillId, null, null, null);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Removes the skill's project from the loaded projects, the security database
	 * and user tracking, then from disk. A failure to remove the files is logged
	 * and the rest of the delete stands.
	 *
	 * @param project the skill's project
	 */
	private void deleteProject(IProject project) {
		String projectId = project.getProjectId();
		UploadUtilities.removeProjectFromDIHelper(projectId);
		SecurityProjectUtils.deleteProject(this.insight.getUser(), projectId);
		UserTrackingUtils.deleteProject(projectId);
		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete skill project files for project '{}'.", projectId, e);
		}
	}

	/**
	 * Describes the reactor, its skill key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Deletes a skill regardless of the caller's permission on it. The calling user must be \
				an application admin or the reactor throws "User must be an admin to perform this function".

				This is the admin counterpart to DeleteSkill, which deletes a skill the caller owns.

				The skill's links to workspaces are removed, then its project is removed from the security database \
				and from disk, and in a cluster from cloud storage. The delete is recorded in the audit trail. The \
				skills built into the platform are refused, and so is a project that is not a skill.

				Inputs:
				  skillId (required) - the id of the skill to delete, which is the id of its project.

				Returns true (BOOLEAN) once the skill is deleted.
				""").key(SKILL_ID, "The id of the skill to delete, which is the id of its project")
				.output(Boolean.class, "True once the skill is deleted");
	}

}
