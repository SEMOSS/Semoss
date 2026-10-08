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
package prerna.io.connector.google;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.security.HttpHelperUtility;

/**
 * Google OAuth2 provider. Uses the fixed Google authorize/token/userinfo
 * endpoints, adds Google's {@code access_type} authorize parameter and does not
 * send {@code response_mode} or the scope in the token exchange.
 */
public class GoogleTokenFiller extends AbstractOAuthTokenFiller {

	private static final String AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth";
	private static final String TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token";
	private static final String USER_INFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo";
	// jsonPattern: JMESPath query projecting values out of the userinfo JSON.
	// beanProps: AccessToken property each projected value maps to, by position.
	private static final String DEFAULT_JSON_PATTERN = "[name, gender, locale, email, sub]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "gender", "locale", "email", "id" };

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

	/**
	 * Trade the refresh token a sign in was given for a new access token.
	 *
	 * <p>
	 * Google issues a refresh token only to a sign in that asked for offline
	 * access, and usually does not issue a new one on a refresh, so the one the
	 * user already holds is kept on the refreshed token.
	 * </p>
	 *
	 * @param currentAccessToken the token that has run out
	 * @param params             not used
	 * @return the refreshed token, or null when there is no refresh token or Google
	 *         refused it
	 */
	@Override
	public AccessToken refreshAccessToken(AccessToken currentAccessToken, Map<String, Object> params) {
		String refreshToken = getRefreshToken(currentAccessToken);
		String prefix = AuthProvider.GOOGLE.getSocialPrefix() + "_";
		String clientId = socialData.getProperty(prefix + "client_id");
		if (isBlank(refreshToken) || isBlank(clientId)) {
			return null;
		}

		Map<String, String> refreshParams = new HashMap<>();
		refreshParams.put("client_id", clientId);
		refreshParams.put("grant_type", "refresh_token");
		refreshParams.put("refresh_token", refreshToken);
		String clientSecret = socialData.getProperty(prefix + "secret_key");
		if (!isBlank(clientSecret)) {
			refreshParams.put("client_secret", clientSecret);
		}

		String tokenUrl = resolve(socialData.getProperty(prefix + "token_url"), TOKEN_URL);
		AccessToken refreshedToken = HttpHelperUtility.getAccessToken(tokenUrl, refreshParams, true, true);
		if (refreshedToken == null || isBlank(refreshedToken.getAccess_token())) {
			return null;
		}

		AccessToken mergedToken = AccessToken.copyToken(currentAccessToken);
		mergedToken.setAccess_token(refreshedToken.getAccess_token());
		mergedToken.setToken_type(refreshedToken.getToken_type());
		mergedToken.setExpires_in(refreshedToken.getExpires_in());
		mergedToken.setStartTime(refreshedToken.getStartTime());
		String updatedRefreshToken = getRefreshToken(refreshedToken);
		mergedToken.addMetaValue(REFRESH_TOKEN_KEY, isBlank(updatedRefreshToken) ? refreshToken : updatedRefreshToken);
		if (mergedToken.getProvider() == null) {
			mergedToken.setProvider(AuthProvider.GOOGLE);
		}
		return mergedToken;
	}

	private static String getRefreshToken(AccessToken accessToken) {
		if (accessToken == null) {
			return null;
		}
		Collection<String> refreshTokens = accessToken.getMetaValues(REFRESH_TOKEN_KEY);
		if (refreshTokens == null) {
			return null;
		}
		for (String refreshToken : refreshTokens) {
			if (!isBlank(refreshToken)) {
				return refreshToken;
			}
		}
		return null;
	}

	@Override
	protected Map<String, String> getExtraAuthorizeParams(String prefix) {
		Map<String, String> extra = new LinkedHashMap<>();
		String accessType = socialData.getProperty(prefix + "access_type");
		if (!isBlank(accessType)) {
			extra.put("access_type", accessType);
		}
		return extra;
	}

}
