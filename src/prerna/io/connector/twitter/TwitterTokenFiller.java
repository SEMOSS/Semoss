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
package prerna.io.connector.twitter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.security.PKCEUtil;

/**
 * X (Twitter) OAuth2 provider using the API v2 authorization-code flow with
 * PKCE. The authorize redirect carries the S256 {@code code_challenge}; the
 * token exchange is an HTTP POST that sends the {@code code_verifier} and, for
 * a confidential client (when a secret is configured), an HTTP Basic
 * {@code Authorization} header. The profile is read from the v2
 * {@code users/me} endpoint.
 */
public class TwitterTokenFiller extends AbstractOAuthTokenFiller {

	private static final String AUTH_URL = "https://twitter.com/i/oauth2/authorize";
	private static final String TOKEN_URL = "https://api.twitter.com/2/oauth2/token";
	private static final String USER_INFO_URL = "https://api.twitter.com/2/users/me";
	// jsonPattern: JMESPath query projecting values out of the users/me JSON
	// (X wraps the user under "data").
	// beanProps: AccessToken property each projected value maps to, by position.
	private static final String DEFAULT_JSON_PATTERN = "[data.name, data.username, data.id]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "username", "id" };

	@Override
	protected String getDefaultAuthorizeUrl(String prefix) {
		return AUTH_URL;
	}

	@Override
	protected String getDefaultTokenUrl(String prefix) {
		return TOKEN_URL;
	}

	@Override
	protected String getDefaultUserInfoUrl(String prefix) {
		return USER_INFO_URL;
	}

	@Override
	protected String getDefaultJsonPattern() {
		return DEFAULT_JSON_PATTERN;
	}

	@Override
	protected String[] getDefaultBeanProps() {
		return DEFAULT_BEAN_PROPS;
	}

	@Override
	protected boolean includeResponseMode() {
		return false;
	}

	@Override
	public boolean usesPKCE() {
		return true;
	}

	@Override
	public String buildAuthorizeRedirect(String prefix, String state, String codeChallenge) {
		return super.buildAuthorizeRedirect(prefix, state) + "&code_challenge=" + codeChallenge
				+ "&code_challenge_method=" + PKCEUtil.CODE_CHALLENGE_METHOD;
	}

	@Override
	public AccessToken exchangeCodeForToken(String prefix, String code, String codeVerifier) {
		String clientId = socialData.getProperty(prefix + "client_id");
		String clientSecret = socialData.getProperty(prefix + "secret_key");
		String redirectUri = socialData.getProperty(prefix + "redirect_uri");
		String tokenUrl = resolve(socialData.getProperty(prefix + "token_url"), TOKEN_URL);

		Map<String, String> params = new HashMap<>();
		params.put("grant_type", "authorization_code");
		params.put("code", code);
		params.put("redirect_uri", redirectUri);
		params.put("client_id", clientId);
		params.put("code_verifier", codeVerifier);

		Map<String, String> headers = null;
		if (!isBlank(clientSecret)) {
			// confidential client: authenticate with HTTP Basic (client_id:client_secret)
			String credentials = clientId + ":" + clientSecret;
			String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
			headers = new HashMap<>();
			headers.put("Authorization", "Basic " + encoded);
		}

		return HttpHelperUtility.getAccessToken(tokenUrl, params, headers, true, true);
	}

}
