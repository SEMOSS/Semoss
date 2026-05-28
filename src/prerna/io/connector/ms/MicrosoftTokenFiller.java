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
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.SocialPropertiesUtil;

public class MicrosoftTokenFiller implements IAccessTokenFiller {

	public static final String MS_GRAPH_BASE_API = "https://graph.microsoft.com";
	public static final String REFRESH_TOKEN_KEY = "refresh_token";
	private static final String USER_INFO_URL = MS_GRAPH_BASE_API + "/v1.0/me/";
	private static String[] beanProps = { "name", "id", "email" };
	private static String jsonPattern = "[displayName,id,mail]";

	@Override
	public void fillAccessToken(AccessToken msAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		if (userInfoUrl == null || (userInfoUrl = userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if (jsonPattern == null || (jsonPattern = jsonPattern.trim()).isEmpty()) {
			jsonPattern = MicrosoftTokenFiller.jsonPattern;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = MicrosoftTokenFiller.beanProps;
		}

		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = msAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, msAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
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
			tokenEndpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
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
			mergedToken.addMetaValue(REFRESH_TOKEN_KEY, updatedRefreshToken);
		} else {
			mergedToken.addMetaValue(REFRESH_TOKEN_KEY, refreshToken);
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
		Collection<String> refreshTokens = accessToken.getMetaValues(REFRESH_TOKEN_KEY);
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

	private boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

}
