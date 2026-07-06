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
package prerna.io.connector.producthunt;

import prerna.io.connector.AbstractOAuthTokenFiller;

/**
 * Product Hunt OAuth2 provider (API v1). Uses the fixed Product Hunt
 * authorize/token endpoints with a scope and no
 * {@code response_mode}/{@code state}, and reads the profile from the v1
 * {@code /me} endpoint.
 */
public class ProductHuntTokenFiller extends AbstractOAuthTokenFiller {

	private static final String AUTH_URL = "https://api.producthunt.com/v1/oauth/authorize";
	private static final String TOKEN_URL = "https://api.producthunt.com/v1/oauth/token";
	// v1 /me returns { "user": { id, name, username, ... } }
	private static final String USER_INFO_URL = "https://api.producthunt.com/v1/me";
	// jsonPattern: JMESPath query projecting values out of the /me JSON (nested
	// under "user").
	// beanProps: AccessToken property each projected value maps to, by position.
	private static final String DEFAULT_JSON_PATTERN = "[user.name, user.username, user.id]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "username", "id" };

	@Override
	protected String getDefaultAuthorizeUrl(String prefix) {
		return AUTH_URL;
	}

	@Override
	protected String getDefaultTokenUrl(String prefix) {
		return TOKEN_URL;
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
	protected boolean includeResponseMode() {
		return false;
	}

	@Override
	protected boolean includeState() {
		return false;
	}

}
