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
	 * 
	 * @param user
	 * @return
	 * @throws Exception
	 */
	public static String getGoogleAccessToken(User user) throws Exception {
		String accessToken = null;
		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			} else {
				AccessToken googleAccessToken = user.getAccessToken(AuthProvider.GOOGLE);
				if (googleAccessToken == null) {
					AccessToken googleResourceToken = user.getResourceAccessToken(AuthProvider.GOOGLE);
					if (googleResourceToken == null) {
						Map<String, Object> retMap = new HashMap<>();
						retMap.put("type", "google");
						retMap.put("message", "Please login/connect to your Google account");
						throwLoginError(retMap);
					} else {
						accessToken = googleResourceToken.getId();
					}
				} else {
					accessToken = googleAccessToken.getId();
				}
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login/connect to your Google account");
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