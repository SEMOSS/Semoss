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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IAccessTokenFiller;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Shared login helpers for the Microsoft Graph connectors.
 */
public final class MicrosoftLoginUtils {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftLoginUtils.class);

	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String BEARER = "Bearer ";
	private static final String PROVIDER_TYPE = AuthProvider.MICROSOFT.getLabel();
	private static final String LOGIN_MESSAGE = "Please login to your Microsoft account";

	private MicrosoftLoginUtils() {

	}

	/**
	 * Retrieves a valid Microsoft Graph access token for the given user. Retained
	 * for callers using the original helper name.
	 *
	 * @param user user executing the pixel
	 * @return the OAuth access token for the Microsoft provider
	 * @throws Exception if the user is not logged in to Microsoft, or their login
	 *                   can no longer be refreshed
	 * @see #getValidAccessToken(User)
	 */
	public static String getMicrosoftAccessToken(User user) throws Exception {
		return getValidAccessToken(user);
	}

	/**
	 * Retrieves a Microsoft Graph access token that is good to use now, refreshing
	 * it first when it has run out.
	 *
	 * <p>
	 * Reactors and background work should use this helper before calling Microsoft
	 * Graph so an expired session token is refreshed before the request is sent.
	 * </p>
	 *
	 * <p>
	 * The refresh is the provider's own, so the refreshed token lands back on the
	 * user object and whatever reads it next gets the new one.
	 * </p>
	 *
	 * @param user the user to act for
	 * @return an access token for the Microsoft provider
	 * @throws Exception if the user is not logged in to Microsoft, or their login
	 *                   can no longer be refreshed
	 */
	public static String getValidAccessToken(User user) throws Exception {
		if (user == null) {
			throwLoginError(getLoginErrorDetails());
		}
		AccessToken msToken = user.getAccessToken(AuthProvider.MICROSOFT);
		if (msToken == null) {
			throwLoginError(getLoginErrorDetails());
		}
		if (!isExpired(msToken)) {
			return msToken.getAccess_token();
		}

		IAccessTokenFiller filler = getTokenFiller(msToken);
		if (filler == null) {
			throwLoginError(getLoginErrorDetails());
		}
		AccessToken refreshed = filler.refreshAccessToken(msToken, new HashMap<>());
		if (refreshed == null || refreshed.getAccess_token() == null) {
			throwLoginError(getLoginErrorDetails());
		}
		// put it back where the next reader looks, so one refresh serves them all
		user.setAccessToken(refreshed);
		return refreshed.getAccess_token();
	}

	/**
	 * Whether a token has run out, counted a minute early so it does not expire
	 * between being read and being used.
	 *
	 * @param token the token to check
	 * @return true when it should be refreshed before use
	 */
	private static boolean isExpired(AccessToken token) {
		if (token.getExpires_in() <= 0 || token.getStartTime() <= 0) {
			// nothing said when it runs out, so it is taken as it is rather than
			// refreshed on every call
			return false;
		}
		long expiresAt = token.getStartTime() + (token.getExpires_in() * 1000L);
		return System.currentTimeMillis() >= expiresAt - 60_000L;
	}

	/**
	 * The thing that knows how to refresh this provider's tokens.
	 *
	 * @param token the token to refresh
	 * @return the filler, or null when the provider names none
	 */
	private static IAccessTokenFiller getTokenFiller(AccessToken token) {
		AuthProvider provider = token.getProvider() == null ? AuthProvider.MICROSOFT : token.getProvider();
		String fillerClass = provider.getTokenFillerClass();
		if (fillerClass == null || fillerClass.trim().isEmpty()) {
			return null;
		}
		try {
			return (IAccessTokenFiller) Class.forName(fillerClass).getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			classLogger.error("Unable to instantiate the token filler {} for provider {}", fillerClass, provider, e);
			return null;
		}
	}

	/**
	 * Returns the email address attached to a user's Microsoft login.
	 *
	 * @param user the signed-in user
	 * @return the Microsoft account email, or null when it is unavailable
	 */
	public static String getMicrosoftEmail(User user) {
		if (user == null) {
			return null;
		}
		AccessToken token = user.getAccessToken(AuthProvider.MICROSOFT);
		return token == null ? null : token.getEmail();
	}

	/**
	 * Throws the pixel level error that prompts the front end to start the
	 * Microsoft login flow.
	 *
	 * @param details map describing the provider and the message to display
	 * @throws SemossPixelException always
	 */
	public static void throwLoginError(Map<String, Object> details) throws SemossPixelException {
		SemossPixelException exception = new SemossPixelException(
				NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/**
	 * Builds standard bearer headers for JSON based Microsoft Graph requests.
	 *
	 * @param accessToken OAuth access token
	 * @return header map containing the authorization and content type headers
	 * @throws IllegalArgumentException if {@code accessToken} is null or blank
	 */
	public static Map<String, String> getBearerHeader(String accessToken) {
		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new IllegalArgumentException("Access token is required to build Microsoft Graph headers.");
		}
		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
		return headers;
	}

	/**
	 * Builds bearer headers without a content type, for requests where the entity
	 * dictates the content type.
	 *
	 * @param accessToken OAuth access token
	 * @return header map containing only the authorization header
	 * @throws IllegalArgumentException if {@code accessToken} is null or blank
	 */
	public static Map<String, String> getAuthorizationHeader(String accessToken) {
		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new IllegalArgumentException("Access token is required to build Microsoft Graph headers.");
		}
		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		return headers;
	}

	private static Map<String, Object> getLoginErrorDetails() {
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("type", PROVIDER_TYPE);
		retMap.put("message", LOGIN_MESSAGE);
		return retMap;
	}
}
