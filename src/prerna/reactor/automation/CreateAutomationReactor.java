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
package prerna.reactor.automation;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Creates a new automation project and returns its ID.
 *
 * <p>The project name follows the same validation used by the standard project creation reactors.
 *
 * <p>Pixel: {@code CreateAutomation(projectName=["My Claims Intake"])}
 */
public class CreateAutomationReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateAutomationReactor.class);

	private static final String PROJECT_NAME_KEY = "projectName";

	public CreateAutomationReactor() {
		this.keysToGet = new String[] { PROJECT_NAME_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR,
					PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException error = new SemossPixelException(noun);
			error.setContinueThreadOfExecution(false);
			throw error;
		}

		String projectName = this.keyValue.get(PROJECT_NAME_KEY);
		if (projectName == null || !Utility.validateName(projectName.trim())) {
			throw new IllegalArgumentException("Invalid Name: It must start with a letter and can only contain "
					+ "letters, numbers, spaces, underscores, and hyphens.");
		}
		projectName = projectName.trim();

		classLogger.info("Creating automation project '{}'", projectName);

		IProject project = ProjectHelper.generateNewProject(projectName, IProject.PROJECT_TYPE.AUTOMATION,
				false, null, null, user, classLogger);
		String projectId = project.getProjectId();
		try {
			AutomationProjectUtils.createStarterDefinition(project, user);
		} catch (RuntimeException e) {
			classLogger.error("Failed to scaffold automation project '{}' (id {})", projectName, projectId, e);
			try {
				cleanupFailedProject(project);
			} catch (RuntimeException cleanupFailure) {
				e.addSuppressed(cleanupFailure);
				classLogger.error("Failed to fully clean incomplete automation project '{}'.", projectId,
						cleanupFailure);
			}
			throw new IllegalStateException(
					"Unable to create automation project: " + e.getMessage(), e);
		}

		classLogger.info("Created automation project '{}' with id {}", projectName, projectId);

		Map<String, Object> result = UploadUtilities.getProjectReturnData(user, projectId);
		return new NounMetadata(result, PixelDataType.UPLOAD_RETURN_MAP,
				PixelOperationType.MARKET_PLACE_ADDITION);
	}

	private static void cleanupFailedProject(IProject project) {
		String projectId = project.getProjectId();
		UploadUtilities.removeProjectFromDIHelper(projectId);
		SecurityProjectUtils.deleteProject(projectId);
		UserTrackingUtils.deleteProject(projectId);
		try {
			project.delete();
		} catch (IOException e) {
			classLogger.error("Failed to delete incomplete automation project files for '{}'.", projectId, e);
		}
		if (ClusterUtil.IS_CLUSTER) {
			Thread.ofVirtual().start(new DeleteProjectRunner(projectId));
		}
	}

	@Override
	public String getReactorDescription() {
		return "Creates a new blank automation project and returns the standard project creation result.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (PROJECT_NAME_KEY.equals(key)) {
			return "Display name for the new automation project. "
					+ "Must follow the standard SEMOSS project naming rules.";
		}
		return super.getDescriptionForKey(key);
	}
}
