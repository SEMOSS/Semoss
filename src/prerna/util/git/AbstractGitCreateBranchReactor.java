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

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;

import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * Creates a new local branch at an optional start point (default {@code HEAD})
 * and checks it out only after successful creation.
 */
public abstract class AbstractGitCreateBranchReactor extends AbstractGitWorktreeReactor {

	private static final String BRANCH_KEY = "branch";
	private static final String START_POINT_KEY = "startPoint";

	private String branch;
	private String startPoint;

	protected AbstractGitCreateBranchReactor(GitReactorTarget target) {
		super(target, new String[] { BRANCH_KEY, START_POINT_KEY }, new int[] { 1, 0 });
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
		String startPointValue = this.keyValue.get(START_POINT_KEY);
		if (startPointValue == null || (startPointValue = startPointValue.trim()).isEmpty()) {
			startPointValue = Constants.HEAD;
		}

		if (!GitBranchUtils.isValidBranchName(branchValue)) {
			throw new SemossPixelException("'" + branchValue + "' is not a valid branch name");
		}

		this.branch = branchValue;
		this.startPoint = startPointValue;
	}

	@Override
	protected Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception {
		Repository repo = thisGit.getRepository();
		final String branchName = this.branch;
		final String resolvedStartPoint = this.startPoint;

		if (repo.findRef(Constants.R_HEADS + branchName) != null) {
			throw new SemossPixelException("Branch '" + branchName + "' already exists");
		}

		ObjectId startPointId = repo.resolve(resolvedStartPoint);
		if (startPointId == null) {
			throw new SemossPixelException("Could not resolve start point: " + resolvedStartPoint);
		}
		GitRepoUtils.assertNoSymlinks(repo, startPointId, branchName);

		try {
			GitBranchUtils.createAndCheckoutBranch(thisGit, branchName, resolvedStartPoint);
		} catch (CheckoutConflictException e) {
			List<String> conflicts = e.getConflictingPaths();
			Collections.sort(conflicts);
			throw new SemossPixelException(
					"Cannot create and checkout '" + branchName + "': local changes would be overwritten in: "
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
		return "creating branch";
	}

	@Override
	protected String getOperationErrorMessage() {
		return "Error occurred creating the branch.";
	}

	@Override
	public String getReactorDescription() {
		return "This reactor creates and checks out a new local branch in " + this.target.getLabelWithArticle()
				+ "'s git repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(BRANCH_KEY)) {
			return "The name of the new branch";
		} else if (key.equals(START_POINT_KEY)) {
			return "Optional start point for the new branch, defaults to HEAD";
		}
		return super.getDescriptionForKey(key);
	}
}
