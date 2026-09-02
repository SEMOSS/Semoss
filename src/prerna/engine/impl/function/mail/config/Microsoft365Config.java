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
package prerna.engine.impl.function.mail.config;

import java.util.Properties;

import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;

/**
 * The app registration a Microsoft 365 engine signs in with.
 *
 * <p>
 * The same five keys serve every Exchange engine and both transports, which is
 * the point: an engine moved between {@code graph} and {@code jakarta} needs
 * nothing rewritten. What differs is the scope, because a token is issued for
 * one resource at a time, so the protocols ask for
 * {@code outlook.office365.com} and Graph asks for {@code graph.microsoft.com}.
 * The caller passes the default for whichever it is, and an SMSS can still
 * override it.
 *
 * @param tenant       the directory (tenant) id, or the tenant domain
 * @param clientId     the application (client) id
 * @param clientSecret the client secret on the app registration
 * @param scope        the scope to ask a token for
 * @param graphBaseUrl where Graph lives, or null for the public endpoint
 */
public record Microsoft365Config(String tenant, String clientId, String clientSecret, String scope,
		String graphBaseUrl) {

	/**
	 * Read the app registration out of an engine's SMSS.
	 *
	 * @param properties   the engine's SMSS properties
	 * @param defaultScope the scope to ask for when the SMSS does not name one,
	 *                     which is the resource the caller intends to reach
	 * @return the credentials, which are not checked until a token is asked for
	 */
	public static Microsoft365Config from(Properties properties, String defaultScope) {
		String tenant = MailProperties.trimToNull(properties.getProperty(MailProperties.EXCHANGE_TENANT));
		String clientId = MailProperties.trimToNull(properties.getProperty(MailProperties.EXCHANGE_CLIENT_ID));
		String clientSecret = MailProperties.trimToNull(properties.getProperty(MailProperties.EXCHANGE_CLIENT_SECRET));
		String scope = MailProperties.firstNonNull(
				MailProperties.trimToNull(properties.getProperty(MailProperties.EXCHANGE_SCOPE)), defaultScope);
		return new Microsoft365Config(tenant, clientId, clientSecret, scope,
				MailProperties.trimToNull(properties.getProperty(MailProperties.GRAPH_BASE_URL)));
	}

	/**
	 * Open the provider that holds tokens for these credentials.
	 *
	 * <p>
	 * The provider validates what it was given, so an engine missing a tenant or a
	 * secret fails on open rather than on the first send or read.
	 *
	 * @return the token provider
	 * @throws IllegalArgumentException when the app registration is incomplete
	 */
	public MicrosoftGraphAppTokenProvider tokenProvider() {
		return new MicrosoftGraphAppTokenProvider(this.tenant, this.clientId, this.clientSecret, this.scope);
	}

}
