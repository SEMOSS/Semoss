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

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class SalesforceTokenFiller implements IAccessTokenFiller {

	private static final String USER_INFO_URL = "https://login.salesforce.com/services/oauth2/userinfo";
	private static final String[] DEFAULT_BEAN_PROPS = { "username", "email", "id" };
	private static final String DEFAULT_JSON_PATTERN = "[name, email, user_id]";

	@Override
	public void fillAccessToken(AccessToken salesforceAccessToken, String userInfoUrl, String jsonPattern,
			String[] beanProps, Map<String, Object> params) {
		if (userInfoUrl == null || (userInfoUrl = userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if (jsonPattern == null || (jsonPattern = jsonPattern.trim()).isEmpty()) {
			jsonPattern = DEFAULT_JSON_PATTERN;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = DEFAULT_BEAN_PROPS;
		}

		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = salesforceAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, salesforceAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// Salesforce payload is controlled and immediately mapped into AccessToken.
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}

}
