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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.SystemDefaultEngines;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Deletes projects for an admin, whatever permission the admin holds on them.
 * The references a project's type keeps elsewhere are removed first: the
 * scheduled jobs of an automation, and a skill's links to workspaces. The
 * project is then removed from the security database, from user tracking and
 * from disk, and in a cluster from cloud storage on a background thread. Every
 * delete is recorded in the audit trail. The built-in apps, MCPs, skills and
 * agents of the platform are never deleted. This is the admin counterpart to
 * {@link prerna.reactor.utils.DeleteProjectReactor}, which deletes the
 * projects its user owns.
 */
public class AdminDeleteProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminDeleteProjectReactor.class);

	/**
	 * Creates the reactor with the project key.
	 */
	public AdminDeleteProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	/**
	 * Deletes every project given, in order, once the user is known to be an
	 * admin and every project is known to exist and not to be built into the
	 * platform. One that does not exist or is built in refuses the whole run
	 * before any project is deleted.
	 *
	 * @return true once the projects are deleted
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		List<String> projectIds = getProjectIds();
		for (String projectId : projectIds) {
			if (!SecurityProjectUtils.projectExists(projectId)) {
				throw new IllegalArgumentException("Project " + projectId + " does not exist");
			}
			if (SystemDefaultEngines.getSystemApps().contains(projectId)
					|| SystemDefaultEngines.getSystemSkills().contains(projectId)
					|| SystemDefaultEngines.getSystemMCPs().contains(projectId)
					|| SystemDefaultEngines.getSystemAgents().contains(projectId)) {
				throw new IllegalArgumentException(
						"Project " + projectId + " is a built-in platform app/MCP/skill/agent and cannot be deleted");
			}
		}

		for (String projectId : projectIds) {
			IProject project = Utility.getProject(projectId);
			IProject.PROJECT_TYPE projectType = project.getProjectType();
			String projectName = project.getProjectName();
			deleteProject(project);
			UserAuditTrailUtils.recordProjectLifecycle(user, AuditEventType.PROJECT_DELETE, projectId, projectName,
					projectType == null ? null : Map.of("projectType", projectType.name()));
			if (projectId.equals(this.insight.getContextProjectId())) {
				this.insight.setContextProjectId(null);
				this.insight.setContextProjectName(null);
			}

			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteProjectRunner(projectId));
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_PROJECT);
	}

	/**
	 * Removes one project: the scheduled jobs of an automation, a skill's links to
	 * workspaces, then the project itself from the loaded projects, the security
	 * database, user tracking and disk.
	 *
	 * @param project the project to delete
	 */
	private void deleteProject(IProject project) {
		String projectId = project.getProjectId();
		if (project.getProjectType() == IProject.PROJECT_TYPE.AUTOMATION) {
			SchedulerDatabaseUtility.removeJobsForProject(projectId);
		}
		if (project.getProjectType() == IProject.PROJECT_TYPE.SKILL) {
			try {
				ModelInferenceLogsUtils.detachSkillFromAllWorkspaces(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to detach skill project '{}' from workspaces.", projectId, e);
			}
		}
		UploadUtilities.removeProjectFromDIHelper(projectId);
		SecurityProjectUtils.deleteProject(this.insight.getUser(), projectId);
		UserTrackingUtils.deleteProject(projectId);

		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete project files for project '{}'.", projectId, e);
		}
	}

	/**
	 * Reads the project ids to delete, from the project key when it is set and
	 * otherwise from every input in the current row.
	 *
	 * @return the ids of the projects to delete
	 */
	private List<String> getProjectIds() {
		List<String> projectIds = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				projectIds.add(grs.get(i).toString());
			}
			return projectIds;
		}

		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			projectIds.add(this.curRow.get(i).toString());
		}
		return projectIds;
	}

	/**
	 * Marks the tool as one that asks before it runs and shows in the sidebar.
	 *
	 * @return the MCP metadata of the tool
	 */
	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

	/**
	 * Describes the reactor, its project key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Deletes projects regardless of the caller's project permissions. The calling user must \
				be an application admin or the reactor throws "User must be an admin to perform this function".

				This is the admin counterpart to DeleteProject, which deletes the projects the caller owns.

				Each project is removed from the security database and from disk, and in a cluster from cloud \
				storage, along with the scheduled jobs of an automation and a skill's links to workspaces. Every \
				delete is recorded in the audit trail. The built-in apps, MCPs, skills and agents of the platform \
				are refused.

				Inputs:
				  project (required) - the ids of the projects to delete.

				Returns true (DELETE_PROJECT/BOOLEAN) once the projects are deleted.
				""").key(ReactorKeysEnum.PROJECT, key -> key.list(String.class).description(
				"The ids of the projects to delete")).output(Boolean.class, "True once the projects are deleted");
	}

}
