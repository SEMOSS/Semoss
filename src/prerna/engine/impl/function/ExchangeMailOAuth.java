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
package prerna.engine.impl.function;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;

/**
 * The Exchange Online half of the mail engines: the app registration an engine
 * is configured with, the token it obtains, and the jakarta.mail properties
 * that make the mail library present that token rather than a password.
 *
 * <p>
 * Exchange Online accepts no mailbox password on any of the three protocols, so
 * IMAP, POP3 and SMTP all sign in the same way and only differ in which
 * application permission the token has to carry. That shared part lives here so
 * each engine is only the protocol it speaks.
 */
public final class ExchangeMailOAuth {

	// public so a caller building one of these engines in memory rather than from
	// a catalogued SMSS can populate the properties by name
	public static final String EXCHANGE_TENANT_KEY = "EXCHANGE_TENANT";
	public static final String EXCHANGE_CLIENT_ID_KEY = "EXCHANGE_CLIENT_ID";
	public static final String EXCHANGE_CLIENT_SECRET_KEY = "EXCHANGE_CLIENT_SECRET";
	public static final String EXCHANGE_SCOPE_KEY = "EXCHANGE_SCOPE";

	/**
	 * The resource a mailbox token is issued for. Asking for {@code .default} means
	 * whatever application permissions the app registration was granted, which is
	 * why the same scope serves all three protocols.
	 */
	public static final String DEFAULT_SCOPE = "https://outlook.office365.com/.default";

	/** Where a Microsoft 365 mailbox is read. */
	public static final String MAILBOX_HOST = "outlook.office365.com";

	/** Where a Microsoft 365 mailbox sends, which is not where it is read. */
	public static final String SEND_HOST = "smtp.office365.com";

	// the mechanism Exchange expects the token to arrive through
	private static final String XOAUTH2_MECHANISM = "XOAUTH2";

	private ExchangeMailOAuth() {

	}

	/**
	 * Build the token provider for the app registration in an SMSS.
	 *
	 * @param smssProp the engine properties
	 * @return the provider, which caches its token and refreshes ahead of expiry
	 * @throws IllegalArgumentException when the app registration is incomplete
	 */
	public static MicrosoftGraphAppTokenProvider openTokenProvider(Properties smssProp) {
		return new MicrosoftGraphAppTokenProvider(smssProp.getProperty(EXCHANGE_TENANT_KEY),
				smssProp.getProperty(EXCHANGE_CLIENT_ID_KEY), smssProp.getProperty(EXCHANGE_CLIENT_SECRET_KEY),
				StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(EXCHANGE_SCOPE_KEY)), DEFAULT_SCOPE));
	}

	/**
	 * Tell jakarta.mail to authenticate with a token rather than a password.
	 *
	 * @param mailProps the session properties being built
	 * @param protocol  the protocol the connection is made over, since the property
	 *                  names carry it
	 */
	public static void addXoauth2Properties(Properties mailProps, String protocol) {
		String prefix = "mail." + protocol + ".";
		mailProps.setProperty(prefix + "auth", "true");
		mailProps.setProperty(prefix + "auth.mechanisms", XOAUTH2_MECHANISM);
		// the token is the only credential these engines have, so the mechanisms
		// that would send it as a plaintext password are turned off rather than left
		// as a fallback for the mail library to try
		mailProps.setProperty(prefix + "auth.login.disable", "true");
		mailProps.setProperty(prefix + "auth.plain.disable", "true");
	}

	/**
	 * What to check when Exchange issues a token and then refuses the sign in.
	 *
	 * <p>
	 * This is the usual first failure, and it is worth spelling out because the two
	 * halves of the setup live in different places: the permission is granted on
	 * the app registration in Azure, and the access to a particular mailbox is
	 * granted in Exchange Online PowerShell.
	 *
	 * @param permission the application permission this protocol needs, such as
	 *                   {@code IMAP.AccessAsApp}
	 * @param extra      anything else specific to the protocol, or null
	 * @return the sentence to append to the error
	 */
	public static String authenticationHint(String permission, String extra) {
		StringBuilder hint = new StringBuilder("A token was obtained, so the app registration exists. Check that it ")
				.append("carries the ").append(permission).append(" application permission with admin consent, and ")
				.append("that Exchange has been told to let this application use this mailbox, which is ")
				.append("New-ServicePrincipal and a mailbox permission in Exchange Online PowerShell.");
		if (extra != null && !extra.isEmpty()) {
			hint.append(" ").append(extra);
		}
		return hint.toString();
	}

	/**
	 * Describe the credential for the log line that records a connection.
	 *
	 * @param clientId the application the token belongs to
	 * @return the description
	 */
	public static String credentialDescription(String clientId) {
		return "an oauth token for " + clientId;
	}

	private static String trimToNull(String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return null;
		}
		return value;
	}

}
