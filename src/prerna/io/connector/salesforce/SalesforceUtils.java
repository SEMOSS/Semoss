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
package prerna.io.connector.salesforce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class SalesforceUtils {

	private static final Logger classLogger = LogManager.getLogger(SalesforceUtils.class);

	/**
	 * Retrieves the active Salesforce OAuth access token for the current user.
	 *
	 * @param user current session user
	 * @return Salesforce access token value
	 * @throws Exception if user context or Salesforce token is unavailable
	 */
	public static String getSalesforceAccessToken(User user) throws Exception {
		AccessToken salesforceToken = getSalesforceToken(user);
		return salesforceToken.getAccess_token();
	}

	/**
	 * Retrieves the Salesforce instance URL for the current user.
	 *
	 * @param user current session user
	 * @return Salesforce instance URL
	 * @throws Exception if user context, Salesforce token, or instance URL is
	 *                   unavailable
	 */
	public static String getSalesforceInstanceUrl(User user) throws Exception {
		AccessToken salesforceToken = getSalesforceToken(user);
		String instanceUrl = salesforceToken.getInstance_url();
		if (instanceUrl == null || instanceUrl.trim().isEmpty()) {
			classLogger.error("Salesforce instance URL is missing from the token.");
			throw new SemossPixelException("Instance URL is missing.");
		}
		return instanceUrl;
	}

	/**
	 * Gets the Salesforce token object for a user with consistent validation.
	 *
	 * @param user current session user
	 * @return Salesforce {@link AccessToken}
	 */
	private static AccessToken getSalesforceToken(User user) {
		if (user == null) {
			classLogger.error("User not found in session.");
			throw new SemossPixelException("User not found in session.");
		}

		AccessToken salesforceToken = user.getAccessToken(AuthProvider.SALESFORCE);
		if (salesforceToken == null) {
			classLogger.error("No Salesforce access token found for user.");
			throw new SemossPixelException("No Salesforce Access Token fetched.");
		}

		return salesforceToken;
	}
}
