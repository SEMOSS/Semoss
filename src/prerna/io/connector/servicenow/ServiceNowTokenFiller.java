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
package prerna.io.connector.servicenow;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.SocialPropertiesUtil;

public class ServiceNowTokenFiller implements IAccessTokenFiller {

	private static final SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
	private static final String PREFIX = "servicenow";
	private static final String USER_INFO_URL_PROP = socialData.getProperty(PREFIX + "_userinfo_url");

	// Updated for array-based response
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "email", "id" };
	private static final String DEFAULT_JSON_PATTERN = "[result.name, result.email, result.sys_id]";

	@Override
	public void fillAccessToken(AccessToken snAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		// Use defaults if parameters are not provided
		if (userInfoUrl == null || userInfoUrl.trim().isEmpty()) {
			userInfoUrl = USER_INFO_URL_PROP;
		}
		if (jsonPattern == null || jsonPattern.trim().isEmpty()) {
			jsonPattern = DEFAULT_JSON_PATTERN;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = DEFAULT_BEAN_PROPS;
		}
		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = snAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, null, true);

		// Fill the bean with the returned JSON
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, snAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// ServiceNow payload is controlled and immediately mapped into AccessToken
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
}
