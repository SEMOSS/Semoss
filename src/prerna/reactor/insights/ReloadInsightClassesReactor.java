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
package prerna.reactor.insights;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

@Deprecated
public class ReloadInsightClassesReactor extends AbstractReactor {

	@Deprecated
	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.RELEASE.getKey() };
	}

	@Deprecated
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		Boolean release = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]) + "");

		List<String> messages = new ArrayList<>();
		if (projectId != null && !projectId.isEmpty()) {
			// make sure valid id for user
			if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
				// you dont have access
				throw new IllegalArgumentException(
						"Project does not exist or user does not have access to the project");
			}

			IProject project = Utility.getProject(projectId);
			try {
				clearProjectAssets(project, release);
				messages.add("Compiled reactors for project '" + project.getProjectId() + "'.");
			} catch (IllegalArgumentException e) {
				messages.add(e.getMessage());
			}
		} else {
			// clear the context project
			if (insight.getContextProjectId() != null) {
				IProject project = Utility.getProject(insight.getContextProjectId());
				try {
					clearProjectAssets(project, release);
					messages.add("Compiled reactors for project '" + project.getProjectId() + "'.");
				} catch (IllegalArgumentException e) {
					messages.add(e.getMessage());
				}
			}
			// clear the insight saved reactor
			if (insight.getProjectId() != null) {
				IProject project = Utility.getProject(insight.getProjectId());
				try {
					clearProjectAssets(project, release);
					messages.add("Compiled reactors for project '" + project.getProjectId() + "'.");
				} catch (IllegalArgumentException e) {
					messages.add(e.getMessage());
				}
			}
		}

		return new NounMetadata(String.join(" ", messages), PixelDataType.CONST_STRING);
	}

	/**
	 * 
	 * @param project
	 */
	private void clearProjectAssets(IProject project, boolean release) {
		project.clearClassCache();
		project.compileReactors();
		if (release) {
			User user = this.insight.getUser();
			String projectId = project.getProjectId();
			String projectName = project.getProjectName();
			if (!SecurityProjectUtils.userIsOwner(user, projectId)) {
				throw new IllegalArgumentException("Project '" + project.getProjectId()
						+ "' does not exist or user is not an owner of the project.");
			}

			// push the compiled code
			String projectVersionFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
			ClusterUtil.pushProjectFolder(project, projectVersionFolder, Constants.ASSETS_FOLDER + "/" + "java");

			// might need to also push the classes folder
			String projectAssetFolder = AssetUtility.getProjectVersionFolder(projectName, projectId) + "/"
					+ Constants.ASSETS_FOLDER;
			File compiledClasses = new File(projectAssetFolder + DIR_SEPARATOR + "classes");
			if (compiledClasses.exists() && compiledClasses.isDirectory()) {
				ClusterUtil.pushProjectFolder(project, projectVersionFolder, Constants.ASSETS_FOLDER + "/" + "classes");
			}

			SecurityProjectUtils.setReactorCompilation(user, projectId);
		}

		// if we are doing reactors on socket side
		boolean executeOnSocket = false;
		if (Utility.getDIHelperProperty(Settings.CUSTOM_REACTOR_EXECUTION) != null) {
			executeOnSocket = Boolean.parseBoolean(Utility.getDIHelperProperty(Settings.CUSTOM_REACTOR_EXECUTION) + "");
		}

		if (executeOnSocket && this.insight.getUser() != null
				&& this.insight.getUser().getPythonSocketClient(false) != null) {
			PayloadStruct ps = new PayloadStruct();
			ps.operation = PayloadStruct.OPERATION.PROJECT;
			ps.projectId = insight.getContextProjectId();
			ps.methodName = "clearClassCache";
			ps.hasReturn = false;

			this.insight.getUser().getPythonSocketClient(false).executeCommand(ps);
		}
	}

	@Deprecated
	@Override
	public String getReactorDescription() {
		return "This reactor is deprecated. Please use CompileAppReactors(project='', release='') instead";
	}

}
