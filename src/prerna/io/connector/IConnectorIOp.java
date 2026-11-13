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
package prerna.io.connector;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AppTokens;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;

/**
 * A single call to an external service's REST API, made on behalf of a user.
 * Each implementation owns one endpoint, such as Google sentiment analysis,
 * Google geocoding or a Twitter search. It knows the endpoint URL, the
 * {@link AuthProvider} whose access token authorizes the call, and the JMESPath
 * pattern that maps the JSON response into the objects it returns. Callers,
 * such as the task lambdas, build the request parameters, call
 * {@link #execute(User, Map)} and read the returned objects.
 */
public interface IConnectorIOp {

	/**
	 * Calls the service with the given request parameters and returns the parsed
	 * response. An implementation should:
	 * <ul>
	 * <li>get the access token for its auth provider with
	 * {@link #requireAccessToken(User, AuthProvider)}</li>
	 * <li>treat null parameters as an empty map, and add any entries the service
	 * needs, such as an API key</li>
	 * <li>send the parameters as the query string of a GET or the JSON body of a
	 * POST</li>
	 * <li>map the JSON response through its JMESPath pattern with
	 * {@link prerna.util.BeanFiller} into the objects it returns</li>
	 * </ul>
	 *
	 * @param user   the signed in user whose access token authorizes the call
	 * @param params the request parameters for the service; the implementation may
	 *               add entries to the map
	 * @return the parsed response, typed by the implementation: a single object
	 *         such as a {@link prerna.om.GeoLocation}, a list of objects such as
	 *         {@link prerna.om.SentimentAnalysis} records, or a value such as the
	 *         path of a downloaded file
	 */
	public Object execute(User user, Map<String, Object> params);

	/**
	 * Returns the access token that authorizes a call to the provider. The user's
	 * token is used when the user has connected the provider, otherwise the
	 * application token from {@link AppTokens}. When neither is available, this
	 * throws the same login required error as the provider reactors, so the user is
	 * asked to log in to the provider.
	 *
	 * @param user     the signed in user, or null when there is none
	 * @param provider the auth provider whose token authorizes the call
	 * @return the access token, which always holds an access token value
	 */
	static AccessToken requireAccessToken(User user, AuthProvider provider) {
		AccessToken token = user == null ? null : user.getAccessToken(provider);
		if (!hasAccessToken(token)) {
			token = AppTokens.getInstance().getAccessToken(provider);
		}
		if (!hasAccessToken(token)) {
			Map<String, Object> details = new HashMap<>();
			details.put("type", provider.getLabel());
			details.put("message", "Please login to your " + provider.getDisplayName() + " account");
			AbstractReactor.throwLoginError(details);
		}
		return token;
	}

	/**
	 * Checks that the token holds an access token value.
	 *
	 * @param token the token to check, or null
	 * @return true when the token has a non blank access token value
	 */
	private static boolean hasAccessToken(AccessToken token) {
		return token != null && token.getAccess_token() != null && !token.getAccess_token().isBlank();
	}

}
