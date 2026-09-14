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
package prerna.util.git;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;

import prerna.util.git.GitBranchUtils.GitBranchInfo;
import prerna.util.git.GitCommonUtils.GitHeadState;

/**
 * Returns the local and remote branches for the target's git repository, with
 * tracking (upstream/ahead/behind) info for local branches.
 */
public abstract class AbstractGitBranchesReactor extends AbstractGitWorktreeReactor {

	protected AbstractGitBranchesReactor(GitReactorTarget target) {
		super(target, new String[] {}, new int[] {});
	}

	@Override
	protected boolean requiresEditPermission() {
		return false;
	}

	@Override
	protected Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception {
		GitHeadState headState = GitCommonUtils.resolveHeadState(thisGit.getRepository());
		List<GitBranchInfo> branches = GitBranchUtils.listBranches(thisGit.getRepository());

		Map<String, Object> resultMap = new LinkedHashMap<>();
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

		return resultMap;
	}

	@Override
	protected String getOperationLogPhrase() {
		return "listing git branches";
	}

	@Override
	protected String getOperationErrorMessage() {
		return "Error occurred listing the git branches.";
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the local and remote branches for " + this.target.getLabelWithArticle()
				+ "'s git repository";
	}
}
