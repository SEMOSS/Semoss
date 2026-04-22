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
package prerna.graph;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.io.connector.IAccessTokenFiller;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.SocialPropertiesUtil;

public class MSGraphAPICall {

	private static final Logger classLogger = LogManager.getLogger(MSGraphAPICall.class);

	private static AccessToken systemAccessToken = null;
	private static long systemAccessTokenExpirationTime = 0;

	private static final long TOKEN_EXPIRY_BUFFER_MILLIS = 300_000L;

	public static class GraphApiResponse {
		private final String responseBody;
		private final AccessToken accessToken;

		public GraphApiResponse(String responseBody, AccessToken accessToken) {
			this.responseBody = responseBody;
			this.accessToken = accessToken;
		}

		public String getResponseBody() {
			return responseBody;
		}

		public AccessToken getAccessToken() {
			return accessToken;
		}
	}

	/**
	 * This method use to get users data from graph api with next link having
	 * subsequent userdata.
	 * 
	 * Returns Graph API response along with the access token that was used so
	 * callers can persist refreshed delegated tokens back into the session.
	 * 
	 * @param accessToken
	 * @param groupId
	 * @param searchTerm
	 * @param nextLink
	 * @return
	 * @throws Exception
	 */
	public GraphApiResponse getUserDetails(AccessToken accessToken, String groupId, String searchTerm, String nextLink)
			throws Exception {
		classLogger.info("getUserDetails based on Graph Api");
		AccessToken resolvedAccessToken = resolveAccessToken(accessToken);
		String uri = buildUri(groupId, searchTerm, nextLink);

		try {
			String jsonResponse = executeGraphRequest(uri, resolvedAccessToken);
			return new GraphApiResponse(jsonResponse, resolvedAccessToken);
		} catch (IllegalArgumentException e) {
			if (!isExpiredAuthTokenError(e)) {
				throw e;
			}
			classLogger.info("MS Graph access token expired, attempting one refresh retry");

			AccessToken retryAccessToken = refreshAccessTokenAfterApiFailure(accessToken);
			if (retryAccessToken == null || isBlank(retryAccessToken.getAccess_token())) {
				throw e;
			}
			String jsonResponse = executeGraphRequest(uri, retryAccessToken);
			return new GraphApiResponse(jsonResponse, retryAccessToken);
		}
	}

	/**
	 * 
	 * @param accessToken
	 * @return
	 * @throws IOException
	 */
	private AccessToken resolveAccessToken(AccessToken accessToken) throws IOException {
		if (accessToken == null) {
			classLogger.info("Graph call for Access Token is null, will attempt to use system credentials");
			if (systemAccessToken == null || isSystemTokenExpired()) {
				systemAccessToken = refreshSystemAccessToken();
			}
			if (systemAccessToken == null || isBlank(systemAccessToken.getAccess_token())) {
				throw new IllegalArgumentException("MS Graph access token is not available from system credentials");
			}
			return systemAccessToken;
		}

		// If delegated user token is stale, refresh it before the request.
		if (isTokenExpired(accessToken)) {
			AccessToken refreshedUserToken = refreshUserAccessToken(accessToken);
			if (refreshedUserToken != null && !isBlank(refreshedUserToken.getAccess_token())) {
				return refreshedUserToken;
			}
			if (shouldUseSystemCredentials()) {
				if (systemAccessToken == null || isSystemTokenExpired()) {
					systemAccessToken = refreshSystemAccessToken();
				}
				if (systemAccessToken == null || isBlank(systemAccessToken.getAccess_token())) {
					throw new IllegalArgumentException(
							"MS Graph access token is not available from system credentials");
				}
				return systemAccessToken;
			}
		}
		return accessToken;
	}

	/**
	 * Build the actual graph api request
	 * 
	 * @param groupId
	 * @param searchTerm
	 * @param nextLink
	 * @return
	 * @throws IOException
	 */
	private String buildUri(String groupId, String searchTerm, String nextLink) throws IOException {
		String uri = "";
		if (nextLink == null) {
			List<String> queryParams = new ArrayList<>();
			queryParams.add("$orderby=displayName");
			queryParams.add("$top=999");
			queryParams.add("$count=true");

			if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
				queryParams
						.add("$search=" + URLEncoder.encode(
								"\"displayName:" + searchTerm + "\" OR \"mail:" + searchTerm
										+ "\" OR \"userPrincipalName:" + searchTerm + "\"",
								java.nio.charset.StandardCharsets.UTF_8.toString()));
			}

			if (groupId == null || groupId.isEmpty()) {
				queryParams.add("$select=displayName,id,mail,userType,givenName,surname");
				queryParams.add("$filter=" + URLEncoder.encode("(userType eq 'Member')",
						java.nio.charset.StandardCharsets.UTF_8.toString()));
				uri = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0/users?" + String.join("&", queryParams);
			} else {
				uri = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0/groups/" + groupId
						+ "/members/microsoft.graph.user?" + String.join("&", queryParams);
			}
		} else {
			uri = nextLink;
		}
		return uri;
	}

	/**
	 * 
	 * @param uri
	 * @param accessToken
	 * @return
	 */
	private String executeGraphRequest(String uri, AccessToken accessToken) {
		Map<String, String> headerMap = new HashMap<>();
		headerMap.put("Authorization", "Bearer " + accessToken.getAccess_token());
		headerMap.put("Accept", "application/json");
		headerMap.put("ConsistencyLevel", "eventual");

		return HttpHelperUtility.getRequest(uri, headerMap, null, null, null);
	}

	/**
	 * 
	 * @param originalAccessToken
	 * @return
	 * @throws IOException
	 */
	private AccessToken refreshAccessTokenAfterApiFailure(AccessToken originalAccessToken) throws IOException {
		if (originalAccessToken != null) {
			AccessToken refreshedUserToken = refreshUserAccessToken(originalAccessToken);
			if (refreshedUserToken != null && !isBlank(refreshedUserToken.getAccess_token())) {
				return refreshedUserToken;
			}
		}

		if (originalAccessToken == null || shouldUseSystemCredentials()) {
			systemAccessToken = refreshSystemAccessToken();
			return systemAccessToken;
		}
		return null;
	}

	/**
	 * Refresh the current access token using the refresh token. Requires you to set
	 * in ms_scope offline_access for user delegated flows.
	 * 
	 * @param currentAccessToken
	 * @return
	 * @throws IOException
	 */
	private AccessToken refreshUserAccessToken(AccessToken currentAccessToken) throws IOException {
		IAccessTokenFiller tokenFiller = getTokenFiller(currentAccessToken);
		if (tokenFiller == null) {
			return null;
		}
		try {
			return tokenFiller.refreshAccessToken(currentAccessToken, null);
		} catch (RuntimeException e) {
			classLogger.error("Unable to refresh delegated Microsoft access token through provider token filler", e);
			return null;
		}
	}

	/*
	 * Refresh the system token below, user level token refreshed in
	 * IAccessTokenFiller
	 */

	/**
	 * This method exchanges application-specific access keys and secret keys for an
	 * access token.
	 * 
	 * @return The access token.
	 * @throws IOException If an error occurs during the token exchange.
	 */
	private synchronized AccessToken refreshSystemAccessToken() throws IOException {
		String clientId = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_client_id");
		if (isBlank(clientId)) {
			clientId = SocialPropertiesUtil.getInstance().getProperty("ms_client_id");
		}
		String clientSecret = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_secret_key");
		if (isBlank(clientSecret)) {
			clientSecret = SocialPropertiesUtil.getInstance().getProperty("ms_secret_key");
		}
		String tenantId = SocialPropertiesUtil.getInstance().getProperty("ms_tenant");
		if (isBlank(clientId) || isBlank(clientSecret) || isBlank(tenantId)) {
			classLogger.warn("MS Graph system credentials are not configured, cannot refresh app token");
			return null;
		}

		String tokenEndpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
		String scope = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_scope");
		if (isBlank(scope)) {
			scope = "https://graph.microsoft.com/.default";
		}

		Map<String, String> params = new HashMap<>();
		params.put("client_id", clientId);
		params.put("scope", scope);
		params.put("grant_type", "client_credentials");
		params.put("client_secret", clientSecret);

		AccessToken newToken = HttpHelperUtility.getAccessToken(tokenEndpoint, params, true, true);
		if (newToken != null) {
			// Set the token expiration time based on the current time and the expires_in
			// value, subtracting 5 minutes (300 seconds)
			systemAccessTokenExpirationTime = System.currentTimeMillis() + ((newToken.getExpires_in() - 300) * 1000);
		}
		return newToken;
	}

	/**
	 * 
	 * @param currentAccessToken
	 * @return
	 */
	private IAccessTokenFiller getTokenFiller(AccessToken currentAccessToken) {
		AuthProvider provider = currentAccessToken != null && currentAccessToken.getProvider() != null
				? currentAccessToken.getProvider()
				: AuthProvider.MICROSOFT;
		String tokenFillerClass = provider.getTokenFillerClass();
		if (isBlank(tokenFillerClass)) {
			return null;
		}
		try {
			return (IAccessTokenFiller) Class.forName(tokenFillerClass).getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			classLogger.error("Unable to instantiate token filler class {} for provider {}", tokenFillerClass, provider,
					e);
			return null;
		}
	}

	/**
	 * 
	 * @param e
	 * @return
	 */
	private boolean isExpiredAuthTokenError(IllegalArgumentException e) {
		if (e == null || e.getMessage() == null) {
			return false;
		}
		String errorMessage = e.getMessage().toLowerCase();
		return errorMessage.contains("invalidauthenticationtoken")
				&& (errorMessage.contains("token is expired") || errorMessage.contains("lifetime validation failed"));
	}

	/**
	 * 
	 * @return
	 */
	private boolean shouldUseSystemCredentials() {
		return Boolean.parseBoolean(
				"" + SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_application_credentials"));
	}

	/**
	 * 
	 * @param accessToken
	 * @return
	 */
	private boolean isTokenExpired(AccessToken accessToken) {
		if (accessToken == null || isBlank(accessToken.getAccess_token())) {
			return true;
		}

		int expiresIn = accessToken.getExpires_in();
		long startTime = accessToken.getStartTime();
		if (expiresIn <= 0 || startTime <= 0) {
			return false;
		}

		long tokenExpiry = startTime + (expiresIn * 1000L) - TOKEN_EXPIRY_BUFFER_MILLIS;
		return System.currentTimeMillis() >= tokenExpiry;
	}

	/**
	 * Checks if the current system access token is expired.
	 * 
	 * @return True if the token is expired, false otherwise.
	 */
	private boolean isSystemTokenExpired() {
		return System.currentTimeMillis() >= systemAccessTokenExpirationTime;
	}

	/**
	 * 
	 * @param value
	 * @return
	 */
	private boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}

}
