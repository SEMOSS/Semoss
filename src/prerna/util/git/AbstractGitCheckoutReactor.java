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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.git.GitBranchUtils.GitCheckoutTarget;

/**
 * Switches an existing local branch, or creates a local tracking branch when a
 * unique remote branch is selected. Never forces the checkout and never
 * auto-stashes; refuses when local changes or conflicts would be overwritten.
 */
public abstract class AbstractGitCheckoutReactor extends AbstractGitWorktreeReactor {

	private static final String BRANCH_KEY = "branch";

	private String branch;

	protected AbstractGitCheckoutReactor(GitReactorTarget target) {
		super(target, new String[] { BRANCH_KEY }, new int[] { 1 });
	}

	@Override
	protected boolean requiresEditPermission() {
		return true;
	}

	@Override
	protected void validateOperationInput() {
		String branchValue = this.keyValue.get(BRANCH_KEY);
		if (branchValue == null || (branchValue = branchValue.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the branch");
		}
		this.branch = branchValue;
	}

	@Override
	protected Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception {
		Repository repo = thisGit.getRepository();
		final String branchName = this.branch;

		if (GitCommonUtils.resolveHeadState(repo).headCommitId == null) {
			throw new SemossPixelException("Cannot checkout a branch in a repository with no commits");
		}

		GitCheckoutTarget checkoutTarget = GitBranchUtils.resolveCheckoutTarget(repo, branchName);
		if (!checkoutTarget.ambiguousRemotes.isEmpty()) {
			throw new SemossPixelException("Branch '" + branchName + "' exists on multiple remotes ("
					+ String.join(", ", checkoutTarget.ambiguousRemotes)
					+ "); qualify the branch name to disambiguate");
		}
		if (!checkoutTarget.existsLocally && checkoutTarget.uniqueRemoteRef == null) {
			throw new SemossPixelException("Branch '" + branchName + "' was not found locally or on any remote");
		}

		String refToResolve = checkoutTarget.existsLocally ? Constants.R_HEADS + branchName
				: checkoutTarget.uniqueRemoteRef;
		ObjectId targetCommitId = repo.resolve(refToResolve);
		if (targetCommitId != null) {
			GitRepoUtils.assertNoSymlinks(repo, targetCommitId, branchName);
		}

		try {
			if (checkoutTarget.existsLocally) {
				thisGit.checkout().setName(branchName).call();
			} else {
				thisGit.checkout().setCreateBranch(true).setName(branchName)
						.setStartPoint(checkoutTarget.uniqueRemoteRef).setUpstreamMode(SetupUpstreamMode.TRACK).call();
			}
		} catch (CheckoutConflictException e) {
			List<String> conflicts = e.getConflictingPaths();
			Collections.sort(conflicts);
			throw new SemossPixelException(
					"Cannot checkout '" + branchName + "': local changes would be overwritten in: "
							+ String.join(", ", conflicts) + ". Commit, stage, or discard these changes first.");
		}

		handle.pushToCluster();

		ObjectId headCommitId = repo.resolve(Constants.HEAD);
		Map<String, Object> resultMap = new LinkedHashMap<>();
		resultMap.put("branch", branchName);
		resultMap.put("headCommitId", headCommitId == null ? null : headCommitId.getName());
		resultMap.put("status", GitReactorResponseUtils.buildStatusMap(GitStatusUtils.computeStatus(repo)));

		return resultMap;
	}

	@Override
	protected String getOperationLogPhrase() {
		return "checking out branch";
	}

	@Override
	protected String getOperationErrorMessage() {
		return "Error occurred checking out the branch.";
	}

	@Override
	public String getReactorDescription() {
		return "This reactor checks out an existing local or remote-tracking branch in "
				+ this.target.getLabelWithArticle() + "'s git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(BRANCH_KEY)) {
			return "The name of the branch to check out";
		}
		return super.getDescriptionForKey(key);
	}
}
