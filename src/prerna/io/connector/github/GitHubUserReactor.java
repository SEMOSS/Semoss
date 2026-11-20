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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Resolves GitHub user profile information for either the authenticated user or
 * a specific username.
 */
public class GitHubUserReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubUserReactor.class);
	private static final String USERNAME = "username";

	/**
	 * Configures supported keys for user lookup requests.
	 */
	public GitHubUserReactor() {
		this.keysToGet = new String[] { USERNAME };
		this.keyRequired = new int[] { 0 };
	}

	/**
	 * Executes the user lookup action.
	 *
	 * @return user profile metadata
	 */
	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String username = null;
		try {
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			username = this.keyValue.get(USERNAME);
			if (username != null) {
				username = username.trim();
				if (username.isEmpty()) {
					username = null;
				}
			}

			Map<String, Object> result;
			if (username == null) {
				result = GitHubHelper.getAuthenticatedUser(accessToken);
			} else {
				result = GitHubHelper.getUserByUsername(accessToken, username);
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to execute GitHubUserReactor for username '{}'.", username, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected error in GitHubUserReactor for username '{}'.", username, e);
			throw new SemossPixelException("An error occurred in GitHubUserReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getReactorDescription() {
		return "Retrieves a GitHub user profile. When username is omitted, returns the authenticated user profile.";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (USERNAME.equals(key)) {
			return "Optional GitHub username. Leave blank to return the authenticated user's profile.";
		}
		return super.getDescriptionForKey(key);
	}
}
