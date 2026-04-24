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

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GHMyself;
import org.kohsuke.github.GitHub;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;

/**
 * Populates GitHub-specific user attributes on an {@link AccessToken}.
 */
public class GithubTokenFiller implements IAccessTokenFiller {

	private static final Logger classLogger = LogManager.getLogger(GithubTokenFiller.class);

	/**
	 * Populates the access token with data fetched from GitHub's authenticated user
	 * endpoint.
	 *
	 * @param gitAccessToken GitHub access token to enrich
	 * @param userInfoUrl    configured user info URL (not used by this filler)
	 * @param jsonPattern    configured parsing pattern (not used by this filler)
	 * @param beanProps      configured bean properties (not used by this filler)
	 * @param params         additional connector parameters
	 */
	@Override
	public void fillAccessToken(AccessToken gitAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		// add specific Git values
		GHMyself myGit = null;
		try {
			myGit = GitHub.connectUsingOAuth(gitAccessToken.getAccess_token()).getMyself();
			gitAccessToken.setId(myGit.getId() + "");
			gitAccessToken.setEmail(myGit.getEmail());
			gitAccessToken.setName(myGit.getName());
			gitAccessToken.setLocale(myGit.getLocation());
			gitAccessToken.setUsername(myGit.getLogin());
		} catch (IOException e) {
			classLogger.error("Failed to populate GitHub access token details for user info URL '{}'.", userInfoUrl, e);
		}
	}

	/**
	 * Delegates to
	 * {@link #fillAccessToken(AccessToken, String, String, String[], Map)} because
	 * GitHub token enrichment does not require response sanitization.
	 *
	 * @param accessToken      GitHub access token to enrich
	 * @param userInfoUrl      configured user info URL
	 * @param jsonPattern      configured parsing pattern
	 * @param beanProps        configured bean properties
	 * @param params           additional connector parameters
	 * @param sanitizeResponse unused for this implementation
	 */
	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}

}
