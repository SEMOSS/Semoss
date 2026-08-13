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
package prerna.reactor.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.SystemDefaultEngines;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class DeleteProjectReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteProjectReactor.class);

	public DeleteProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		List<String> projectIds = getProjectIds();
		for (String projectId : projectIds) {
			User user = this.insight.getUser();

			// we may have the alias
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if (!isAdmin) {
				if (AbstractSecurityUtils.adminOnlyProjectDelete()) {
					throwFunctionalityOnlyExposedForAdminsError();
				}

				boolean isOwner = SecurityProjectUtils.userIsOwner(user, projectId);
				if (!isOwner) {
					throw new IllegalArgumentException("Project " + projectId
							+ " does not exist or user does not have permissions to delete the project. "
							+ "User must be the owner to perform this function.");
				}
			}

			if (SystemDefaultEngines.getSystemSkills().contains(projectId)
					|| SystemDefaultEngines.getSystemMCPs().contains(projectId)
					|| SystemDefaultEngines.getSystemAgents().contains(projectId)) {
				throw new IllegalArgumentException(
						"Project " + projectId + " is a built-in platform MCP/skill/agent and cannot be deleted");
			}

				IProject project = Utility.getProject(projectId);
				IProject.PROJECT_TYPE projectType = project.getProjectType();
				deleteProject(project);
				UserAuditTrailUtils.recordProjectLifecycle(user, "PROJECT_DELETE", projectId, null,
						projectType == null ? null : Map.of("projectType", projectType.name()));
				// also remove this project in case it is the current insight's project id
			if (projectId.equals(this.insight.getContextProjectId())) {
				this.insight.setContextProjectId(null);
				this.insight.setContextProjectName(null);
			}

			// Run the delete thread in the background for removing from cloud storage
			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteProjectRunner(projectId));
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_PROJECT);
	}

	/**
	 * 
	 * @param project
	 * @return
	 */
	private boolean deleteProject(IProject project) {
		String projectId = project.getProjectId();
		// skill-projects carry WORKSPACE_RESOURCE__ refs + CONFIG_JSON.skills[]
		// mirrors in modellogs; scrub those before tearing down the project itself
		if (project.getProjectType() == IProject.PROJECT_TYPE.SKILL) {
			try {
				ModelInferenceLogsUtils.detachSkillFromAllWorkspaces(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to detach skill project '{}' from workspaces.", projectId, e);
			}
		}
		// remove from DIHelper
		UploadUtilities.removeProjectFromDIHelper(projectId);
		// remove from security
		SecurityProjectUtils.deleteProject(projectId);
		// remove from user tracking
		UserTrackingUtils.deleteProject(projectId);

		// now try to actually remove from disk
		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete project files for project '{}'.", projectId, e);
		}

		return true;
	}

	/**
	 * Get inputs
	 * 
	 * @return list of projects to delete
	 */
	public List<String> getProjectIds() {
		List<String> projectIds = new Vector<String>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				projectIds.add(grs.get(i).toString());
			}
			return projectIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			projectIds.add(this.curRow.get(i).toString());
		}
		return projectIds;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		// default to auto execution for reactors
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		// sidebar to view default json for reactor input+output
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}
}
