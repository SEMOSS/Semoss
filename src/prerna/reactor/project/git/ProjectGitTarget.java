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
package prerna.reactor.project.git;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitReactorTarget;
import prerna.util.git.GitTargetHandle;

/**
 * Binds the shared git working-tree reactors to projects: the project pixel
 * key, project security, and the project version folder.
 */
public final class ProjectGitTarget implements GitReactorTarget {

	public static final GitReactorTarget INSTANCE = new ProjectGitTarget();

	private ProjectGitTarget() {

	}

	@Override
	public String getIdKey() {
		return ReactorKeysEnum.PROJECT.getKey();
	}

	@Override
	public String getLabel() {
		return "project";
	}

	@Override
	public String getLabelWithArticle() {
		return "a project";
	}

	@Override
	public PixelOperationType getOpType() {
		return PixelOperationType.PROJECT_INFO;
	}

	@Override
	public boolean userCanView(User user, String targetId) {
		return SecurityProjectUtils.userCanViewProject(user, targetId);
	}

	@Override
	public boolean userCanEdit(User user, String targetId) {
		return SecurityProjectUtils.userCanEditProject(user, targetId);
	}

	@Override
	public GitTargetHandle resolve(String targetId) {
		IProject project = Utility.getProject(targetId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT, targetId,
				project.getEngineName());
		return new GitTargetHandle(targetId, versionFolder,
				() -> ClusterUtil.pushProjectFolder(project, versionFolder));
	}
}
