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

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class GoogleLoginUtils {

	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String BEARER = "Bearer ";

	private GoogleLoginUtils() {

	}

	/**
	 * Retrieves a Google access token that is good to use now, refreshing it first
	 * when it has run out.
	 *
	 * <p>
	 * The refresh needs the refresh token Google hands out only to a sign in that
	 * asked for offline access, so {@code google_access_type} has to be
	 * {@code offline} in social.properties. Without one, a token that has run out
	 * asks the user to sign in again.
	 * </p>
	 *
	 * <p>
	 * The refreshed token lands back on the user object, so whatever reads it next
	 * gets the new one.
	 * </p>
	 *
	 * @param user the user to act for
	 * @return an access token for the Google provider
	 * @throws SemossPixelException if the user is not signed in to Google, or their
	 *                              sign in can no longer be refreshed
	 */
	public static String getValidAccessToken(User user) {
		AccessToken token = user == null ? null : user.getAccessToken(AuthProvider.GOOGLE);
		if (token == null || token.getAccess_token() == null || token.getAccess_token().isBlank()) {
			throwLoginError(getLoginErrorDetails());
		}
		if (!isExpired(token)) {
			return token.getAccess_token();
		}
		AccessToken refreshed = new GoogleTokenFiller().refreshAccessToken(token, new HashMap<>());
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

	private static Map<String, Object> getLoginErrorDetails() {
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("type", AuthProvider.GOOGLE.getLabel());
		retMap.put("message", "Please login to your Google account");
		return retMap;
	}

	/**
	 * Retrieves the user's Google access token as it is, without refreshing it.
	 *
	 * @param user the user to act for
	 * @return the access token
	 * @throws Exception if the user is not signed in to Google
	 * @see #getValidAccessToken(User)
	 */
	public static String getGoogleAccessToken(User user) throws Exception {
		String accessToken = null;
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", AuthProvider.GOOGLE.getLabel());
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			} else {
				AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken = googleToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", AuthProvider.GOOGLE.getLabel());
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		return accessToken;
	}

	/**
	 * 
	 * @param details
	 * @throws SemossPixelException
	 */
	public static void throwLoginError(Map<String, Object> details) throws SemossPixelException {
		SemossPixelException exception = new SemossPixelException(
				NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

	/**
	 * 
	 * @param accessToken
	 * @return
	 */
	public static Map<String, String> getBearerHeader(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
		headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
		return headers;
	}
}
