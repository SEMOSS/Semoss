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
package prerna.io.connector.github;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Manages branch listing, creation, and deletion for a GitHub repository.
 */
public class GitHubBranchReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubBranchReactor.class);

	private static final String ACTION = "action";
	private static final String OWNER = "owner";
	private static final String REPO = "repo";
	private static final String BRANCH = "branch";
	private static final String FROM_BRANCH = "fromBranch";
	private static final String PAGE = "page";
	private static final String PER_PAGE = "perPage";

	/**
	 * Configures supported keys for branch actions.
	 */
	public GitHubBranchReactor() {
		this.keysToGet = new String[] { ACTION, OWNER, REPO, BRANCH, FROM_BRANCH, PAGE, PER_PAGE };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0, 0, 0 };
	}

	/**
	 * Executes the branch management action requested by {@code action}.
	 *
	 * @return branch operation result metadata
	 */
	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String action = null;
		try {
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			action = this.keyValue.get(ACTION);
			String owner = this.keyValue.get(OWNER);
			String repo = this.keyValue.get(REPO);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException(ACTION + " is required.");
			}
			if (owner == null || owner.trim().isEmpty()) {
				throw new SemossPixelException(OWNER + " is required.");
			}
			if (repo == null || repo.trim().isEmpty()) {
				throw new SemossPixelException(REPO + " is required.");
			}
			action = action.trim().toLowerCase();
			owner = owner.trim();
			repo = repo.trim();

			Object result;
			switch (action) {
			case "list":
				int page = 1;
				String pageValue = this.keyValue.get(PAGE);
				if (pageValue != null && !pageValue.trim().isEmpty()) {
					try {
						page = Integer.parseInt(pageValue.trim());
					} catch (NumberFormatException e) {
						throw new SemossPixelException("Invalid number for pagination: '" + pageValue.trim() + "'.", e);
					}
				}
				int perPage = 30;
				String perPageValue = this.keyValue.get(PER_PAGE);
				if (perPageValue != null && !perPageValue.trim().isEmpty()) {
					try {
						perPage = Integer.parseInt(perPageValue.trim());
					} catch (NumberFormatException e) {
						throw new SemossPixelException("Invalid number for pagination: '" + perPageValue.trim() + "'.",
								e);
					}
				}
				result = GitHubHelper.listBranches(accessToken, owner, repo, page, perPage);
				break;
			case "create":
				String branch = this.keyValue.get(BRANCH);
				if (branch == null || branch.trim().isEmpty()) {
					throw new SemossPixelException(BRANCH + " is required.");
				}
				String fromBranch = this.keyValue.get(FROM_BRANCH);
				if (fromBranch != null) {
					fromBranch = fromBranch.trim();
					if (fromBranch.isEmpty()) {
						fromBranch = null;
					}
				}
				result = GitHubHelper.createBranch(accessToken, owner, repo, branch.trim(), fromBranch);
				break;
			case "delete":
				branch = this.keyValue.get(BRANCH);
				if (branch == null || branch.trim().isEmpty()) {
					throw new SemossPixelException(BRANCH + " is required.");
				}
				result = GitHubHelper.deleteBranch(accessToken, owner, repo, branch.trim());
				break;
			default:
				throw new SemossPixelException(
						"Invalid action '" + action + "'. Valid values are: list, create, delete.");
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to execute GitHubBranchReactor for action '{}'.", action, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected error in GitHubBranchReactor for action '{}'.", action, e);
			throw new SemossPixelException("An error occurred in GitHubBranchReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getReactorDescription() {
		return "Lists, creates, or deletes branches in a GitHub repository.";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (ACTION.equals(key)) {
			return "Required action: list, create, or delete.";
		} else if (OWNER.equals(key)) {
			return "Required repository owner, either a user or organization.";
		} else if (REPO.equals(key)) {
			return "Required repository name.";
		} else if (BRANCH.equals(key)) {
			return "Branch name. Required for create and delete.";
		} else if (FROM_BRANCH.equals(key)) {
			return "Optional source branch to branch from. Defaults to the repository's default branch.";
		} else if (PAGE.equals(key)) {
			return "Pagination page number for list. Defaults to 1.";
		} else if (PER_PAGE.equals(key)) {
			return "Pagination size per page for list. Defaults to 30 and is capped at 100.";
		}
		return super.getDescriptionForKey(key);
	}
}
