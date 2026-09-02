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

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.ProjectGitBranchUtils;
import prerna.util.git.ProjectGitBranchUtils.GitBranchInfo;
import prerna.util.git.ProjectGitCommonUtils;
import prerna.util.git.ProjectGitCommonUtils.GitHeadState;

/**
 * Returns the local and remote branches for a project's git repository, with
 * tracking (upstream/ahead/behind) info for local branches.
 */
public class ProjectGitBranchesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ProjectGitBranchesReactor.class);

	public ProjectGitBranchesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the project id");
		}

		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new SemossPixelException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		String versionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				project.getEngineName());
		File gitDir = new File(versionFolder, ".git");
		if (!gitDir.exists()) {
			throw new SemossPixelException("Project does not have a git repository yet");
		}

		Map<String, Object> resultMap = new LinkedHashMap<>();
		try (Git thisGit = Git.open(new File(versionFolder))) {
			GitHeadState headState = ProjectGitCommonUtils.resolveHeadState(thisGit.getRepository());
			List<GitBranchInfo> branches = ProjectGitBranchUtils.listBranches(thisGit.getRepository());

			resultMap.put("currentBranch", headState.branch);
			resultMap.put("detached", headState.detached);

			List<Map<String, Object>> branchList = new ArrayList<>();
			for (GitBranchInfo branch : branches) {
				Map<String, Object> branchMap = new LinkedHashMap<>();
				branchMap.put("name", branch.name);
				branchMap.put("fullName", branch.fullName);
				branchMap.put("remote", branch.remote);
				branchMap.put("current", branch.current);
				branchMap.put("commitId", branch.commitId);
				branchMap.put("upstream", branch.upstream);
				branchMap.put("ahead", branch.ahead);
				branchMap.put("behind", branch.behind);
				branchList.add(branchMap);
			}
			resultMap.put("branches", branchList);
		} catch (Exception e) {
			classLogger.error("Error occurred listing git branches for project {}", projectId, e);
			throw new SemossPixelException(
					"Error occurred listing the git branches. Detailed error = " + e.getMessage(), e);
		}

		return new NounMetadata(resultMap, PixelDataType.MAP, PixelOperationType.PROJECT_INFO);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the local and remote branches for a project's git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The project id";
		}
		return super.getDescriptionForKey(key);
	}
}
