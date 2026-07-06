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
package prerna.io.connector.siteminder;

import prerna.io.connector.AbstractOAuthTokenFiller;

/**
 * SiteMinder fronting an Azure AD tenant. When
 * {@code auth_url}/{@code token_url} are not configured they default to the
 * Microsoft tenant endpoints derived from the {@code tenant} property. Scope is
 * included in the token exchange and the profile is read from Microsoft Graph
 * (the token is an Azure AD token).
 */
public class SiteminderTokenFiller extends AbstractOAuthTokenFiller {

	private static final String MS_BASE = "https://login.microsoftonline.com/";
	// Azure AD access token -> Microsoft Graph "me" for the profile
	private static final String USER_INFO_URL = "https://graph.microsoft.com/v1.0/me/";
	// jsonPattern: JMESPath query projecting values out of the Graph "me" JSON.
	// beanProps: AccessToken property each projected value maps to, by position.
	private static final String DEFAULT_JSON_PATTERN = "[displayName, id, mail]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "id", "email" };

	@Override
	protected String getDefaultAuthorizeUrl(String prefix) {
		String tenant = socialData.getProperty(prefix + "tenant");
		return isBlank(tenant) ? null : MS_BASE + tenant + "/oauth2/v2.0/authorize";
	}

	@Override
	protected String getDefaultTokenUrl(String prefix) {
		String tenant = socialData.getProperty(prefix + "tenant");
		return isBlank(tenant) ? null : MS_BASE + tenant + "/oauth2/v2.0/token";
	}

	@Override
	protected String getDefaultUserInfoUrl(String prefix) {
		return USER_INFO_URL;
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
	protected boolean includeScopeInTokenRequest() {
		return true;
	}

}
