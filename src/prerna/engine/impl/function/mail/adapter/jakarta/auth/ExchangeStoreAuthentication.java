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
package prerna.engine.impl.function.mail.adapter.jakarta.auth;

import java.util.Properties;

import prerna.engine.impl.function.mail.auth.microsoft365.Microsoft365MailOAuth;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;

/**
 * Signing in to a Microsoft 365 mailbox with a token instead of a password.
 *
 * <p>
 * Exchange Online no longer accepts a mailbox password over IMAP or POP3, so an
 * app registration's token is presented over XOAUTH2 instead. jakarta.mail
 * sends it as the password argument either way; what makes it a token rather
 * than a password is the session being told which mechanism to use, which is
 * {@link #configure(Properties, String)}.
 *
 * <p>
 * Two things follow from a token expiring where a password does not. It is
 * fetched on every connect rather than held, and a refusal is worth one retry
 * with a fresh one.
 */
public final class ExchangeStoreAuthentication implements MailStoreAuthentication {

	private final MicrosoftGraphAppTokenProvider tokenProvider;
	private final String permission;

	/**
	 * @param tokenProvider the provider holding tokens for the app registration
	 * @param permission    the application permission this protocol needs, named in
	 *                      the hint when Exchange refuses the sign in
	 */
	public ExchangeStoreAuthentication(MicrosoftGraphAppTokenProvider tokenProvider, String permission) {
		this.tokenProvider = tokenProvider;
		this.permission = permission;
	}

	@Override
	public void configure(Properties mailProperties, String protocol) {
		Microsoft365MailOAuth.addXoauth2Properties(mailProperties, protocol);
	}

	@Override
	public String connectSecret() {
		// a mailbox token lasts about an hour, so it is fetched per connect. the
		// provider hands back its cached one until it is close to expiring
		return this.tokenProvider.getAccessToken();
	}

	@Override
	public String description() {
		return Microsoft365MailOAuth.credentialDescription(this.tokenProvider.getClientId());
	}

	@Override
	public boolean refreshAfterRejection() {
		// the token was accepted when it was issued, so a refusal means it stopped
		// being valid before its stated expiry - dropping it is worth one retry
		this.tokenProvider.invalidate();
		return true;
	}

	@Override
	public String failureHint() {
		return Microsoft365MailOAuth.authenticationHint(this.permission, null);
	}

	@Override
	public String diagnostic() {
		return Microsoft365MailOAuth.tokenDiagnostic(this.tokenProvider);
	}
}
