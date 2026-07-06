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
package prerna.io.connector.adfs;

import prerna.io.connector.AbstractOAuthTokenFiller;

/**
 * ADFS OIDC provider. The authorize/token endpoints and the claim parsing
 * ({@code jsonPattern}/{@code beanProps}) are configured entirely via social
 * properties. The user's claims are carried in the {@code id_token} JWT, so the
 * token exchange requests an id token and the profile is read from the decoded
 * JWT payload instead of a userinfo endpoint. Scope is sent in the token
 * exchange.
 */
public class AdfsTokenFiller extends AbstractOAuthTokenFiller {

	// jsonPattern: JMESPath query projecting values out of the decoded id_token
	// claims.
	// beanProps: AccessToken property each projected value maps to, by position.
	// NOTE: ADFS claim names depend on the relying-party claim rules; these are a
	// sensible default (standard OIDC "name"/"email" claims) and are meant to be
	// overridden per deployment via the {prefix}jsonPattern/{prefix}beanProps
	// props.
	private static final String DEFAULT_JSON_PATTERN = "[name, email]";
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "email" };

	@Override
	protected boolean usesIdToken() {
		return true;
	}

	@Override
	protected boolean includeScopeInTokenRequest() {
		return true;
	}

	@Override
	protected String getDefaultJsonPattern() {
		return DEFAULT_JSON_PATTERN;
	}

	@Override
	protected String[] getDefaultBeanProps() {
		return DEFAULT_BEAN_PROPS;
	}

}
