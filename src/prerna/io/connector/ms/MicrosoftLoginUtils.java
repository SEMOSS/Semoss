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

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Shared login helpers for the Microsoft Graph connectors.
 */
public final class MicrosoftLoginUtils {

	private static final String HEADER_AUTHORIZATION = "Authorization";
	private static final String HEADER_CONTENT_TYPE = "Content-Type";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String BEARER = "Bearer ";
	private static final String PROVIDER_TYPE = "microsoft";
	private static final String LOGIN_MESSAGE = "Please login to your Microsoft account";

	private MicrosoftLoginUtils() {

	}

	/**
	 * Retrieves the Microsoft Graph access token for the given user.
	 *
	 * @param user user executing the pixel
	 * @return the OAuth access token for the Microsoft provider
	 * @throws Exception if the user is not logged in to Microsoft
	 */
	public static String getMicrosoftAccessToken(User user) throws Exception {
		String accessToken = null;
		try {
			if (user == null) {
				throwLoginError(getLoginErrorDetails());
			} else {
				AccessToken msToken = user.getAccessToken(AuthProvider.MICROSOFT);
				accessToken = msToken.getAccess_token();
			}
		} catch (Exception e) {
			throwLoginError(getLoginErrorDetails());
		}
		return accessToken;
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
