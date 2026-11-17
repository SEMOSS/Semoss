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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.SystemDefaultEngines;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Deletes an agent, which is a workspace, for an admin, whatever permission the
 * admin holds on it. The workspace's entry in the model inference logs goes
 * first, then its project is removed from the security database, from user
 * tracking and from disk, and in a cluster from cloud storage. The delete is
 * recorded in the audit trail. The agents built into the platform are kept.
 * This is the admin counterpart to
 * {@link prerna.engine.impl.model.inferencetracking.reactors.workspaces.DeleteWorkspaceReactor},
 * which deletes a workspace its user owns.
 */
public class AdminDeleteWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminDeleteWorkspaceReactor.class);

	/**
	 * Creates the reactor with the workspace key.
	 */
	public AdminDeleteWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	/**
	 * Deletes the workspace given, once the user is known to be an admin. A
	 * workspace that does not exist, is built into the platform, or is a project of
	 * another type stops the run with an error.
	 *
	 * @return true once the workspace is deleted, or an error when the delete fails
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		if (SystemDefaultEngines.getSystemAgents().contains(workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " is a built-in system agent and cannot be deleted");
		}
		if (!SecurityProjectUtils.projectExists(workspaceId)) {
			throw new IllegalArgumentException("Workspace " + workspaceId + " does not exist");
		}
		IProject project = Utility.getProject(workspaceId);
		if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.WORKSPACE) {
			throw new IllegalArgumentException("Project " + workspaceId + " is not a workspace");
		}

		try {
			ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
			deleteProject(project);
			if (ClusterUtil.IS_CLUSTER) {
				Thread.ofVirtual().start(new DeleteProjectRunner(workspaceId));
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete workspace '{}'.", workspaceId, e);
			return getError("Error during workspace delete: " + e.getMessage());
		}
		UserAuditTrailUtils.recordWorkspaceLifecycle(user, AuditEventType.WORKSPACE_DELETE, workspaceId, null, null);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Removes the workspace's project from the loaded projects, the security
	 * database and user tracking, then from disk. A failure to remove the files is
	 * logged and the rest of the delete stands.
	 *
	 * @param project the workspace's project
	 */
	private void deleteProject(IProject project) {
		String projectId = project.getProjectId();
		UploadUtilities.removeProjectFromDIHelper(projectId);
		SecurityProjectUtils.deleteProject(projectId);
		UserTrackingUtils.deleteProject(projectId);
		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete workspace project files for project '{}'.", projectId, e);
		}
	}

	/**
	 * Describes the reactor, its workspace key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Deletes an agent (a workspace) regardless of the caller's permission on it. The calling \
				user must be an application admin or the reactor throws "User must be an admin to perform this \
				function".

				This is the admin counterpart to DeleteWorkspace, which deletes a workspace the caller owns.

				The workspace's entry in the model inference logs is removed, then its project is removed from the \
				security database and from disk, and in a cluster from cloud storage. The delete is recorded in the \
				audit trail. The agents built into the platform are refused, and so is a project that is not a \
				workspace.

				Inputs:
				  workspaceId (required) - the id of the workspace to delete.

				Returns true (BOOLEAN) once the workspace is deleted.
				""").key(ReactorKeysEnum.WORKSPACE_ID, "The id of the workspace to delete")
				.output(Boolean.class, "True once the workspace is deleted");
	}

}
