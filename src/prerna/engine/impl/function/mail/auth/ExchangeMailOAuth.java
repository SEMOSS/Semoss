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
package prerna.engine.impl.function.mail.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.gson.Gson;

import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.config.Microsoft365Config;
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
	// a cataloged SMSS can populate the properties by name
	public static final String EXCHANGE_TENANT_KEY = MailProperties.EXCHANGE_TENANT;
	public static final String EXCHANGE_CLIENT_ID_KEY = MailProperties.EXCHANGE_CLIENT_ID;
	public static final String EXCHANGE_CLIENT_SECRET_KEY = MailProperties.EXCHANGE_CLIENT_SECRET;
	public static final String EXCHANGE_SCOPE_KEY = MailProperties.EXCHANGE_SCOPE;

	/**
	 * The resource a mailbox token is issued for. Asking for {@code .default} means
	 * whatever application permissions the app registration was granted, which is
	 * why the same scope serves all three protocols.
	 */
	public static final String DEFAULT_SCOPE = "https://outlook.office365.com/.default";

	/** Which way an engine reaches the mailbox. */
	public static final String MAIL_TRANSPORT_KEY = MailProperties.MAIL_TRANSPORT;

	/** Go through the Microsoft Graph API. */
	public static final String GRAPH_TRANSPORT = MailProperties.GRAPH_TRANSPORT;

	/** Go through the mail protocol with the jakarta.mail library. */
	public static final String JAKARTA_TRANSPORT = MailProperties.JAKARTA_TRANSPORT;

	/** Where Graph lives, overridable for a sovereign cloud. */
	public static final String GRAPH_BASE_URL_KEY = MailProperties.GRAPH_BASE_URL;

	/**
	 * The resource a Graph token is issued for, which is a different resource from
	 * the one the protocols want even though the app registration is the same.
	 */
	public static final String GRAPH_SCOPE = "https://graph.microsoft.com/.default";

	/** The application permission a Graph token has to carry to send. */
	public static final String GRAPH_SEND_PERMISSION = "Mail.Send";

	/** The application permission a Graph token has to carry to read a mailbox. */
	public static final String GRAPH_READ_PERMISSION = "Mail.Read";

	/**
	 * The application permission a Graph token has to carry to change a mailbox as
	 * well as read it, which an engine only needs when one of the changes is turned
	 * on.
	 */
	public static final String GRAPH_READ_WRITE_PERMISSION = "Mail.ReadWrite";

	/** Where a Microsoft 365 mailbox is read. */
	public static final String MAILBOX_HOST = "outlook.office365.com";

	/** Where a Microsoft 365 mailbox sends, which is not where it is read. */
	public static final String SEND_HOST = "smtp.office365.com";

	/** The application permission a token has to carry to send over SMTP AUTH. */
	public static final String SMTP_PERMISSION = "SMTP.SendAsApp";

	/**
	 * The part of the send setup that is neither in Azure nor about permissions,
	 * and the one most likely to be missed, since it is off until somebody turns it
	 * on.
	 */
	public static final String SMTP_AUTH_HINT = """
			Also check that SMTP AUTH is enabled for this mailbox and for the tenant, since Microsoft \
			disables it by default and the tenant wide setting overrides the per mailbox one.\
			""";

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
		return openTokenProvider(smssProp, DEFAULT_SCOPE);
	}

	/**
	 * Build the token provider for the app registration in an SMSS, for a resource
	 * other than the mail protocols.
	 *
	 * <p>
	 * One app registration serves both ways into a mailbox, but a token is issued
	 * for one resource at a time: the protocols want a token for Exchange, and
	 * Graph wants one for Graph. Same credentials, different scope.
	 *
	 * @param smssProp     the engine properties
	 * @param defaultScope the scope to ask for when the SMSS names none
	 * @return the provider, which caches its token and refreshes ahead of expiry
	 * @throws IllegalArgumentException when the app registration is incomplete
	 */
	public static MicrosoftGraphAppTokenProvider openTokenProvider(Properties smssProp, String defaultScope) {
		return Microsoft365Config.from(smssProp, defaultScope).tokenProvider();
	}

	/**
	 * Which way an engine was told to reach the mailbox.
	 *
	 * @param smssProp         the engine properties
	 * @param defaultTransport what to use when the SMSS does not say
	 * @return the transport name
	 */
	public static String resolveTransport(Properties smssProp, String defaultTransport) {
		MailProperties.Backend defaultBackend = GRAPH_TRANSPORT.equalsIgnoreCase(defaultTransport)
				? MailProperties.Backend.GRAPH
				: MailProperties.Backend.JAKARTA;
		return MailProperties.backend(smssProp, defaultBackend).name().toLowerCase();
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
	 * What to check when Graph refuses a mailbox call.
	 *
	 * <p>
	 * Shorter than the protocol version because Graph has less to line up: there is
	 * no service principal, no mailbox grant in Exchange PowerShell, and nothing to
	 * enable per mailbox. Either the application permission is there or it is not.
	 *
	 * @param permission the application permission this call needs
	 * @return the sentence to append to the error
	 */
	public static String graphAuthenticationHint(String permission) {
		return "Reading through Graph needs the " + permission
				+ " application permission on the app registration, with admin consent. It is granted under "
				+ "Microsoft Graph rather than Office 365 Exchange Online, and is a different permission from the "
				+ "one the mail protocols use.";
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

	/**
	 * What the token this engine holds actually says, for when Exchange refuses it.
	 *
	 * <p>
	 * A refused token has four distinguishable causes and the claims tell them
	 * apart: an audience that is not Exchange means the wrong scope was asked for,
	 * no roles at all means admin consent was never granted, and the right role
	 * with the right audience means Azure is set up and what is missing is on the
	 * Exchange side. The object id separates the last case in two, since Exchange
	 * matches a mailbox grant against that rather than against the client id: an
	 * object id it holds no service principal for is refused exactly like a mailbox
	 * that was never granted. Without this the four look identical from here.
	 *
	 * @param tokenProvider the provider holding the token
	 * @return a description of the token, or why it could not be read
	 */
	public static String tokenDiagnostic(MicrosoftGraphAppTokenProvider tokenProvider) {
		try {
			String token = tokenProvider.getAccessToken();
			String audience = readClaim(token, "aud");
			String objectId = readClaim(token, "oid");
			Set<String> roles = tokenProvider.getGrantedRoles();
			return "the token is for audience " + (audience == null ? "unknown" : audience)
					+ ", is held by service principal object id " + (objectId == null ? "unknown" : objectId)
					+ " and carries application permissions "
					+ (roles.isEmpty() ? "[none, so admin consent is missing]" : roles.toString());
		} catch (RuntimeException e) {
			return "the token could not be read back: " + e.getMessage();
		}
	}

	/**
	 * Check that a mailbox is named the way Exchange expects to be signed in to,
	 * which is the bare address on its own.
	 *
	 * <p>
	 * A display name form is easy to paste in by mistake, since that is what the
	 * sender field takes, and Exchange answers it with the same refusal it gives a
	 * missing permission.
	 *
	 * @param mailbox the configured mailbox
	 * @param key     the SMSS key it came from, for the error
	 */
	public static void validateMailbox(String mailbox, String key) {
		if (mailbox == null) {
			return;
		}
		if (mailbox.indexOf('<') > -1 || mailbox.indexOf('>') > -1 || mailbox.indexOf(' ') > -1) {
			throw new IllegalArgumentException("The " + key + " of '" + mailbox
					+ "' has to be the mailbox address on its own, with no display name around it");
		}
	}

	/**
	 * Read one claim out of a JWT payload. The token is read for diagnostics only
	 * and is never treated as a security decision, so the signature is deliberately
	 * not validated.
	 *
	 * @param token the access token
	 * @param claim the claim to read
	 * @return the claim as a string, or null when it is not there
	 */
	private static String readClaim(String token, String claim) {
		if (token == null) {
			return null;
		}
		String[] segments = token.split("\\.");
		if (segments.length < 2) {
			return null;
		}
		// base64url without padding is what a JWT uses, so pad it back out
		String payload = segments[1];
		payload = payload + "=".repeat((4 - (payload.length() % 4)) % 4);
		String json = new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);
		Map<?, ?> claims = new Gson().fromJson(json, Map.class);
		Object value = claims == null ? null : claims.get(claim);
		return value == null ? null : value.toString();
	}

	private static String trimToNull(String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return null;
		}
		return value;
	}

}
