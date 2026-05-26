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
 * Handles GitHub repository and organization discovery operations.
 */
public class GitHubRepoReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubRepoReactor.class);

	private static final String ACTION = "action";
	private static final String OWNER = "owner";
	private static final String QUERY = "query";
	private static final String PAGE = "page";
	private static final String PER_PAGE = "perPage";

	/**
	 * Configures supported keys for repository actions.
	 */
	public GitHubRepoReactor() {
		this.keysToGet = new String[] { ACTION, OWNER, QUERY, PAGE, PER_PAGE };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	/**
	 * Executes the repository action specified by {@code action}.
	 *
	 * @return repository or organization metadata for the selected action
	 */
	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String action = null;
		try {
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException(ACTION + " is required.");
			}
			action = action.trim().toLowerCase();

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
					throw new SemossPixelException("Invalid number for pagination: '" + perPageValue.trim() + "'.", e);
				}
			}

			Object result;
			switch (action) {
			case "list_repos": {
				String owner = this.keyValue.get(OWNER);
				if (owner != null) {
					owner = owner.trim();
					if (owner.isEmpty()) {
						owner = null;
					}
				}
				result = GitHubHelper.listRepositories(accessToken, owner, page, perPage);
				break;
			}
			case "search_repos": {
				String query = this.keyValue.get(QUERY);
				if (query == null || query.trim().isEmpty()) {
					throw new SemossPixelException(QUERY + " is required.");
				}
				result = GitHubHelper.searchRepositories(accessToken, query.trim(), page, perPage);
				break;
			}
			case "list_orgs": {
				result = GitHubHelper.listUserOrganizations(accessToken, page, perPage);
				break;
			}
			case "list_org_repos": {
				String owner = this.keyValue.get(OWNER);
				if (owner == null || owner.trim().isEmpty()) {
					throw new SemossPixelException(OWNER + " (organization name) is required.");
				}
				result = GitHubHelper.listOrgRepositories(accessToken, owner.trim(), page, perPage);
				break;
			}
			default:
				throw new SemossPixelException("Invalid action '" + action
						+ "'. Valid values are: list_repos, search_repos, list_orgs, list_org_repos.");
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to execute GitHubRepoReactor for action '{}'.", action, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected error in GitHubRepoReactor for action '{}'.", action, e);
			throw new SemossPixelException("An error occurred in GitHubRepoReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getReactorDescription() {
		return "Lists or searches repositories, and lists organizations and organization repositories for the authenticated GitHub user.";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (ACTION.equals(key)) {
			return "Required action: list_repos, search_repos, list_orgs, or list_org_repos.";
		} else if (OWNER.equals(key)) {
			return "Owner login (user or organization). Optional for list_repos. Required for list_org_repos.";
		} else if (QUERY.equals(key)) {
			return "Repository search query. Required for search_repos.";
		} else if (PAGE.equals(key)) {
			return "Pagination page number. Defaults to 1.";
		} else if (PER_PAGE.equals(key)) {
			return "Pagination size per page. Defaults to 30 and is capped at 100.";
		}
		return super.getDescriptionForKey(key);
	}
}
