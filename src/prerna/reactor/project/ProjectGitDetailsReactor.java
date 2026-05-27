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

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * This reactor returns the git provider and repository details for a given project.
 */
public class ProjectGitDetailsReactor extends AbstractReactor {

	public ProjectGitDetailsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new SemossPixelException("Must input a project id");
		}

		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new SemossPixelException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);

		String gitProvider = project.getProjectGitProvider();
		if (gitProvider == null) {
			gitProvider = "";
		}
		String gitRepo = project.getProjectGitRepo();
		if (gitRepo == null) {
			gitRepo = "";
		}

		Map<String, String> gitDetails = new HashMap<>();
		gitDetails.put("gitProvider", gitProvider);
		gitDetails.put("gitRepo", gitRepo);
		return new NounMetadata(gitDetails, PixelDataType.MAP);
	}

}