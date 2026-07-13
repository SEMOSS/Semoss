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

import prerna.io.connector.AbstractOAuthTokenFiller;

/**
 * ServiceNow OAuth2 provider. The authorize and token endpoints are derived
 * from the configured {@code instance_url} ({@code /oauth_auth.do} and
 * {@code /oauth_token.do}); the userinfo endpoint is configured directly. The
 * authorize redirect does not use {@code response_mode}/{@code state}.
 */
public class ServiceNowTokenFiller extends AbstractOAuthTokenFiller {

	// jsonPattern: JMESPath query projecting values out of the userinfo JSON
	// (ServiceNow wraps the user under "result").
	// beanProps: AccessToken property each projected value maps to, by position.
	private static final String DEFAULT_JSON_PATTERN = "[result.name, result.email, result.sys_id]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "email", "id" };

	@Override
	protected String getDefaultAuthorizeUrl(String prefix) {
		String instanceUrl = socialData.getProperty(prefix + "instance_url");
		return isBlank(instanceUrl) ? null : instanceUrl + "/oauth_auth.do";
	}

	@Override
	protected String getDefaultTokenUrl(String prefix) {
		String instanceUrl = socialData.getProperty(prefix + "instance_url");
		return isBlank(instanceUrl) ? null : instanceUrl + "/oauth_token.do";
	}

	@Override
	protected String getDefaultJsonPattern() {
		return DEFAULT_JSON_PATTERN;
	}

	@Override
	protected String[] getDefaultBeanProps() {
		return DEFAULT_BEAN_PROPS;
	}

	@Override
	protected boolean includeResponseMode() {
		return false;
	}

	@Override
	protected boolean includeState() {
		return false;
	}

}
