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
package prerna.io.connector.ms;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.SocialPropertiesUtil;

/**
 * Microsoft (Azure AD) OAuth2 provider. The authorize/token endpoints default
 * to the Microsoft tenant endpoints derived from the {@code tenant} property,
 * scope is included in the token exchange, and the profile is read from
 * Microsoft Graph. Also enforces the {@code login_external} allowlist and
 * supports delegated refresh-token flows.
 */
public class MicrosoftTokenFiller extends AbstractOAuthTokenFiller {

	public static final String MS_GRAPH_BASE_API = "https://graph.microsoft.com";
	private static final String MS_BASE = "https://login.microsoftonline.com/";
	private static final String USER_INFO_URL = MS_GRAPH_BASE_API + "/v1.0/me/";
	// jsonPattern: JMESPath query projecting values out of the Graph "me" JSON.
	// beanProps: AccessToken property each projected value maps to, by position.
	private static final String DEFAULT_JSON_PATTERN = "[displayName,id,mail]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "id", "email" };

	@Override
	protected String getDefaultAuthorizeUrl(String prefix) {
		String tenant = socialData.getProperty(prefix + "tenant");
		return isBlank(tenant) ? null : MS_BASE + tenant + "/oauth2/v2.0/authorize";
	}

	@Override
	protected String getDefaultTokenUrl(String prefix) {
		String tenant = socialData.getProperty(prefix + "tenant");
		return isBlank(tenant) ? null : MS_BASE + tenant + "/oauth2/v2.0/token";
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
	protected boolean includeScopeInTokenRequest() {
		return true;
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String prefix) {
		super.fillAccessToken(accessToken, prefix);
		// enforce the external-user allowlist once the display name is populated
		boolean loginExternalAllowed = Boolean.parseBoolean(socialData.getProperty(prefix + "login_external"));
		if (!loginExternalAllowed && accessToken.getName() != null && accessToken.getName().contains("External")) {
			throw new IllegalArgumentException("External users are not allowed");
		}
	}

	@Override
	public AccessToken refreshAccessToken(AccessToken currentAccessToken, Map<String, Object> params) {
		String refreshToken = getRefreshToken(currentAccessToken);
		if (isBlank(refreshToken)) {
			return null;
		}

		String clientId = SocialPropertiesUtil.getInstance().getProperty("ms_client_id");
		if (isBlank(clientId)) {
			clientId = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_client_id");
		}
		String clientSecret = SocialPropertiesUtil.getInstance().getProperty("ms_secret_key");
		if (isBlank(clientSecret)) {
			clientSecret = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_secret_key");
		}
		String tenantId = SocialPropertiesUtil.getInstance().getProperty("ms_tenant");
		if (isBlank(clientId) || isBlank(tenantId)) {
			return null;
		}

		String tokenEndpoint = SocialPropertiesUtil.getInstance().getProperty("ms_token_url");
		if (isBlank(tokenEndpoint)) {
			tokenEndpoint = MS_BASE + tenantId + "/oauth2/v2.0/token";
		}

		Map<String, String> refreshParams = new HashMap<>();
		refreshParams.put("client_id", clientId);
		refreshParams.put("grant_type", "refresh_token");
		refreshParams.put("refresh_token", refreshToken);
		if (!isBlank(clientSecret)) {
			refreshParams.put("client_secret", clientSecret);
		}

		AccessToken refreshedToken = HttpHelperUtility.getAccessToken(tokenEndpoint, refreshParams, true, true);
		if (refreshedToken == null || isBlank(refreshedToken.getAccess_token())) {
			return null;
		}

		AccessToken mergedToken = AccessToken.copyToken(currentAccessToken);
		mergedToken.setAccess_token(refreshedToken.getAccess_token());
		mergedToken.setToken_type(refreshedToken.getToken_type());
		mergedToken.setExpires_in(refreshedToken.getExpires_in());
		mergedToken.setStartTime(refreshedToken.getStartTime());

		String updatedRefreshToken = getRefreshToken(refreshedToken);
		if (!isBlank(updatedRefreshToken)) {
			mergedToken.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, updatedRefreshToken);
		} else {
			mergedToken.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, refreshToken);
		}

		if (mergedToken.getProvider() == null) {
			mergedToken.setProvider(AuthProvider.MICROSOFT);
		}
		return mergedToken;
	}

	private String getRefreshToken(AccessToken accessToken) {
		if (accessToken == null) {
			return null;
		}
		Collection<String> refreshTokens = accessToken.getMetaValues(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY);
		if (refreshTokens == null || refreshTokens.isEmpty()) {
			return null;
		}
		for (String refreshToken : refreshTokens) {
			if (!isBlank(refreshToken)) {
				return refreshToken;
			}
		}
		return null;
	}

}
