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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.kohsuke.github.GHMyself;
import org.kohsuke.github.GitHub;

import prerna.auth.AccessToken;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.git.GitRepoUtils;

/**
 * GitHub OAuth2 provider. GitHub differs from a standard OAuth2 provider in two
 * ways, both handled here:
 * <ul>
 * <li>its token endpoint returns a <em>form-encoded</em> body (not JSON), so
 * the exchange requests a non-JSON response;</li>
 * <li>its TLS certificate is registered for the git domain before the login
 * redirect and after the token exchange.</li>
 * </ul>
 * The profile is read through the {@code org.kohsuke.github} client rather than
 * a JMESPath userinfo mapping, so {@code jsonPattern}/{@code beanProps} do not
 * apply.
 */
public class GithubTokenFiller extends AbstractOAuthTokenFiller {

	private static final String AUTH_URL = "https://github.com/login/oauth/authorize";
	private static final String TOKEN_URL = "https://github.com/login/oauth/access_token";
	private static final String GITHUB_DOMAIN = "https://github.com";

	@Override
	protected String getDefaultAuthorizeUrl(String prefix) {
		return AUTH_URL;
	}

	@Override
	protected String getDefaultTokenUrl(String prefix) {
		return TOKEN_URL;
	}

	@Override
	protected boolean includeResponseMode() {
		return false;
	}

	@Override
	protected Map<String, String> getExtraAuthorizeParams(String prefix) {
		Map<String, String> extra = new LinkedHashMap<>();
		extra.put("allow_signup", "true");
		return extra;
	}

	@Override
	public String buildAuthorizeRedirect(String prefix, String state) {
		// register GitHub's cert up front so the later token exchange trusts the domain
		addGithubCert(GITHUB_DOMAIN);
		return super.buildAuthorizeRedirect(prefix, state);
	}

	@Override
	public AccessToken exchangeCodeForToken(String prefix, String code) {
		String clientId = socialData.getProperty(prefix + "client_id");
		String clientSecret = socialData.getProperty(prefix + "secret_key");
		String redirectUri = resolveRedirectUri(prefix);
		String tokenUrl = resolve(socialData.getProperty(prefix + "token_url"), TOKEN_URL);

		Map<String, String> params = new HashMap<>();
		params.put("client_id", clientId);
		params.put("redirect_uri", redirectUri);
		params.put("code", code);
		params.put("client_secret", clientSecret);

		// GitHub returns the token as a form-encoded body -> json = false
		AccessToken accessToken = HttpHelperUtility.getAccessToken(tokenUrl, params, false, true);
		addGithubCert(tokenUrl);
		return accessToken;
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String prefix) {
		fillFromGitHub(accessToken);
	}

	// GitHub enrichment ignores the JMESPath userinfo config; keep the legacy
	// signatures (used outside the login flow) pointed at the same logic.
	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		fillFromGitHub(accessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		fillFromGitHub(accessToken);
	}

	/**
	 * Populate the access token from GitHub's authenticated-user endpoint via the
	 * {@code org.kohsuke.github} client.
	 */
	private void fillFromGitHub(AccessToken gitAccessToken) {
		try {
			GHMyself myGit = GitHub.connectUsingOAuth(gitAccessToken.getAccess_token()).getMyself();
			gitAccessToken.setId(myGit.getId() + "");
			gitAccessToken.setEmail(myGit.getEmail());
			gitAccessToken.setName(myGit.getName());
			gitAccessToken.setLocale(myGit.getLocation());
			gitAccessToken.setUsername(myGit.getLogin());
		} catch (IOException e) {
			classLogger.error("Failed to populate GitHub access token details.", e);
		}
	}

	private void addGithubCert(String domain) {
		try {
			GitRepoUtils.addCertForDomain(domain);
		} catch (Exception e) {
			classLogger.error("Unexpected error adding certificate for GitHub domain {}", domain, e);
		}
	}

}
