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
package prerna.reactor.project;

import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class CreateProjectReactor extends AbstractReactor {

	private static final String CLASS_NAME = CreateProjectReactor.class.getName();

	/*
	 * This class is used to construct a new project This project only contains
	 * insights
	 */

	public CreateProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.PROJECT_TYPE.getKey(),
				ReactorKeysEnum.GLOBAL.getKey(), ReactorKeysEnum.PROVIDER.getKey(), ReactorKeysEnum.URL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		this.organizeKeys();
		IProject.PROJECT_TYPE projectType = null;

		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		int index = 0;

		String projectName = this.keyValue.get(this.keysToGet[index++]);
		// if projectName is valid then set the name, else throw error
		if (!Utility.validateName(projectName)) {
			// error and redirect to try again
			throw new IllegalArgumentException(
					"Invalid Name: It must start with a letter and can only contain letters, numbers, and spaces.");
		}

		// String projectName = this.keyValue.get(this.keysToGet[index++]);
		String projectTypeStr = this.keyValue.get(this.keysToGet[index++]);
		boolean global = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[index++]) + "");

		NounMetadata warning = null;
		if (global) {
			if (AbstractSecurityUtils.adminOnlyProjectSetPublic() && !SecurityAdminUtils.userIsAdmin(user)) {
				warning = NounMetadata.getWarningNounMessage(
						"Public access can only be enabled by administrators. This item will be created as private.");
				global = false;
			}
		}

		// Allow-list: CreateProject can only create CODE, BLOCKS, or INSIGHTS
		// projects. WORKSPACE, SKILL, and NOTEBOOK projects have additional setup
		// requirements (inference-tracking WORKSPACE row + WORKSPACE_RESOURCE
		// links for workspaces; skill metadata wiring for skills; sample .ipynb
		// scaffold for notebooks) that this reactor does not perform — calling
		// CreateProject for those types leaves the system in a half-created state
		// where downstream readers (e.g. GetAgentHooks, ListWorkspaces) cannot see
		// the new row. Reject up-front and direct the caller at the right reactor.
		if (projectTypeStr == null || (projectTypeStr = projectTypeStr.trim()).isEmpty()) {
			projectType = IProject.PROJECT_TYPE.INSIGHTS;
		} else {
			try {
				projectType = IProject.PROJECT_TYPE.valueOf(projectTypeStr);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(
						"Invalid projectType '" + projectTypeStr + "'. Allowed values: CODE, BLOCKS, INSIGHTS.");
			}
			if (projectType == IProject.PROJECT_TYPE.WORKSPACE) {
				throw new IllegalArgumentException("CreateProject cannot create WORKSPACE-type projects. "
						+ "Use AddWorkspace(name='...') instead — it performs the additional "
						+ "inference-tracking WORKSPACE row + WORKSPACE_RESOURCE inserts that "
						+ "CreateProject skips.");
			}
			if (projectType == IProject.PROJECT_TYPE.SKILL) {
				throw new IllegalArgumentException("CreateProject cannot create SKILL-type projects. "
						+ "Use CreateSkill(...) instead — it performs the additional skill-metadata "
						+ "wiring that CreateProject skips.");
			}
			if (projectType == IProject.PROJECT_TYPE.NOTEBOOK) {
				throw new IllegalArgumentException("CreateProject cannot create NOTEBOOK-type projects. "
						+ "Use CreateNotebook(project='...') instead — it scaffolds the sample .ipynb "
						+ "file that CreateProject skips.");
			}
			if (projectType == IProject.PROJECT_TYPE.AUTOMATION) {
				throw new IllegalArgumentException("CreateProject cannot create AUTOMATION-type projects. "
						+ "Use CreateAutomation(projectName='...') instead — it scaffolds the automation "
						+ "definition, configuration, and MCP tool metadata.");
			}
		}
		String gitProvider = this.keyValue.get(this.keysToGet[index++]);
		String gitCloneUrl = this.keyValue.get(this.keysToGet[index++]);

		IProject project = ProjectHelper.generateNewProject(projectName, projectType, global, gitProvider, gitCloneUrl,
				this.insight.getUser(), logger);

		Map<String, Object> retMap = UploadUtilities.getProjectReturnData(this.insight.getUser(),
				project.getProjectId());
		NounMetadata retNoun = new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP,
				PixelOperationType.MARKET_PLACE_ADDITION);
		if (warning != null) {
			retNoun.addAdditionalReturn(warning);
		}
		return retNoun;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The name for this project. Note: the project ID is randomly generated and is not passed into this method";
		} else if (key.equals(ReactorKeysEnum.PROVIDER.getKey())) {
			return "The GIT provider - user must be logged in with this provider for credentials";
		} else if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return "The GIT repository URL to clone for this project";
		}
		return super.getDescriptionForKey(key);
	}

}
