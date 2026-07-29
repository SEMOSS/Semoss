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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.SystemDefaultEngines;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class DeleteWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteWorkspaceReactor.class);

	public DeleteWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		if (SystemDefaultEngines.getSystemAgents().contains(workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " is a built-in system agent and cannot be deleted");
		}

		if (AbstractSecurityUtils.adminOnlyProjectDelete()) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		boolean isOwner = SecurityProjectUtils.userIsOwner(user, workspaceId);
		if (!isOwner) {
			throw new IllegalArgumentException("Workspace " + workspaceId
					+ " does not exist or user does not have permissions to delete the workspace. "
					+ "User must be the owner to perform this function.");
		}
		try {
			ModelInferenceLogsUtils.deleteWorkspaceEntry(workspaceId);
			if (AbstractSecurityUtils.containsProjectId(workspaceId)) {
				IProject project = Utility.getProject(workspaceId);
				deleteProject(project);
				if (ClusterUtil.IS_CLUSTER) {
					Thread.ofVirtual().start(new DeleteProjectRunner(workspaceId));
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete workspace '{}'.", workspaceId, e);
			return getError("Error during workspace delete: " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * 
	 * @param project
	 * @return
	 */
	private boolean deleteProject(IProject project) {
		String projectId = project.getProjectId();
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
			classLogger.error("Failed to delete workspace project files for project '{}'.", projectId, e);
		}

		return true;
	}
}
