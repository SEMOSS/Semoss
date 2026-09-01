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
package prerna.engine.impl.function.mail.adapter.jakarta;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import prerna.engine.impl.function.mail.auth.microsoft365.Microsoft365MailOAuth;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.OutboundMail;
import prerna.engine.impl.function.mail.model.SendResult;
import prerna.engine.impl.function.mail.spi.MailSender;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.util.EmailUtility;

/**
 * Sends through a mail server over SMTP.
 *
 * <p>
 * This is the transport for any relay that speaks SMTP: an internal one that
 * does no encryption, a provider that wants a username and password, or a
 * Microsoft 365 mailbox reached with a token. The last of those is the same
 * connection with a different credential, so it is a branch here rather than a
 * transport of its own - when an app registration is configured the sign in
 * switches to XOAUTH2 and the token stands in for the password.
 *
 * <p>
 * The TLS defaults are deliberately strict: encryption is required rather than
 * merely offered, the server certificate has to match the host, and the
 * protocol floor is TLS 1.2. Anything spelled out as a raw {@code mail.}
 * property is layered on last and wins.
 */
public class JakartaSmtpMailSender implements MailSender {

	private static final Logger classLogger = LogManager.getLogger(JakartaSmtpMailSender.class);

	// jakarta.mail key that turns a plain smtp connection into an encrypted one.
	// read off the raw properties so a configuration that only ever spoke in
	// jakarta.mail keys still gets the matching socket factory settings
	private static final String SSL_ENABLE_PROPERTY = "mail.smtp.ssl.enable";

	private String host = null;
	private String port = "587";
	private String username = null;
	private String password = null;
	private String security = MailProperties.STARTTLS_SECURITY;

	// set only when the relay is a microsoft 365 mailbox, which takes a token
	// where every other relay takes a password
	private MicrosoftGraphAppTokenProvider tokenProvider = null;

	private Session emailSession = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// read from here on rather than from the argument, so a raw jakarta.mail key
		// is found whichever case it arrived in
		smssProp = normalizeRawMailKeys(smssProp);

		if (trimToNull(smssProp.getProperty(MailProperties.EXCHANGE_CLIENT_ID)) != null) {
			// the provider validates the credentials, so an engine missing one of
			// them fails on open rather than on the first send
			this.tokenProvider = Microsoft365MailOAuth.openTokenProvider(smssProp);
		}

		this.host = trimToNull(smssProp.getProperty(MailProperties.SMTP_HOST));
		if (this.host == null && this.tokenProvider != null) {
			// a microsoft 365 mailbox is always sent from the same place
			this.host = Microsoft365MailOAuth.SEND_HOST;
		}
		if (this.host == null && trimToNull(smssProp.getProperty("mail.smtp.host")) == null) {
			throw new IllegalArgumentException("Must have key " + MailProperties.SMTP_HOST
					+ " or mail.smtp.host in SMSS to know which mail server to use");
		}

		// the UI writes a blank line for every optional field left empty, so an
		// unset key arrives as "" rather than absent - defaultIfEmpty covers both
		this.port = StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(MailProperties.SMTP_PORT)), this.port);
		this.username = trimToNull(smssProp.getProperty(MailProperties.SMTP_USERNAME));
		this.password = trimToNull(smssProp.getProperty(MailProperties.SMTP_PASSWORD));

		if (this.tokenProvider != null) {
			if (this.username == null) {
				// there is no anonymous way into a microsoft 365 mailbox, and the
				// token names the application rather than the mailbox
				throw new IllegalArgumentException(
						"Must define " + MailProperties.SMTP_USERNAME + " in SMSS to know which mailbox to send as");
			}
			Microsoft365MailOAuth.validateMailbox(this.username, MailProperties.SMTP_USERNAME);
			if (this.password != null) {
				classLogger.warn("A {} was set but this mailbox signs in with a token, so the password is ignored",
						MailProperties.SMTP_PASSWORD);
				this.password = null;
			}
		} else if ((this.username == null) != (this.password == null)) {
			// half a credential cannot authenticate, so drop it and connect
			// unauthenticated rather than fail to open. which one happened is in the
			// session log line below
			classLogger.warn("Only one of {} and {} is set, so the connection to {} will not authenticate",
					MailProperties.SMTP_USERNAME, MailProperties.SMTP_PASSWORD, this.host);
			this.username = null;
			this.password = null;
		}

		this.security = StringUtils
				.defaultIfEmpty(trimToNull(smssProp.getProperty(MailProperties.SMTP_SECURITY)), this.security)
				.toLowerCase();
		if (!this.security.equals(MailProperties.STARTTLS_SECURITY)
				&& !this.security.equals(MailProperties.SSL_SECURITY)
				&& !this.security.equals(MailProperties.NO_SECURITY)) {
			throw new IllegalArgumentException("Sending over SMTP only supports " + MailProperties.STARTTLS_SECURITY
					+ ", " + MailProperties.SSL_SECURITY + ", or " + MailProperties.NO_SECURITY + " for the "
					+ MailProperties.SMTP_SECURITY + " key");
		}

		this.emailSession = buildEmailSession(smssProp);
	}

	@Override
	public SendResult send(OutboundMail message) {
		boolean success = EmailUtility.sendEmail(this.emailSession, message.toArray(), message.ccArray(),
				message.bccArray(), message.from(), message.subject(), message.body(), message.html(),
				message.attachmentArray());
		if (!success && this.tokenProvider != null) {
			// what the refused token carried, which is what tells a missing consent
			// apart from a missing grant on the mailbox
			classLogger.error("The send as {} failed and {}", this.username,
					Microsoft365MailOAuth.tokenDiagnostic(this.tokenProvider));
			// and then dropped, because the usual reason a send starts failing is a
			// permission that is about to be changed, and a cached token would go on
			// being refused for the rest of its hour after the fix
			this.tokenProvider.invalidate();
		}
		return new SendResult(success, message.from());
	}

	/**
	 * Build the mail session this transport sends through.
	 *
	 * @param smssProp the engine properties
	 * @return the session to send every message through
	 */
	private Session buildEmailSession(Properties smssProp) {
		boolean onlyCustomProps = parseBoolean(smssProp.getProperty(MailProperties.ONLY_CUSTOM_PROPERTIES), false);

		// the server can be described either by this engine's own keys or purely in
		// jakarta.mail keys, so resolve both before anything reads the port
		String effectiveHost = this.host;
		String effectivePort = this.host == null ? null : this.port;
		if (effectiveHost == null) {
			effectiveHost = trimToNull(smssProp.getProperty("mail.smtp.host"));
			effectivePort = trimToNull(smssProp.getProperty("mail.smtp.port"));
		}

		Properties mailProps = new Properties();
		if (this.host != null) {
			mailProps.setProperty("mail.smtp.host", this.host);
			mailProps.setProperty("mail.smtp.port", this.port);
		}

		final String authUsername = this.username;

		if (!onlyCustomProps) {
			mailProps.setProperty("mail.transport.protocol", "smtp");

			// a raw ssl.enable counts the same as picking ssl, so a configuration
			// written entirely in jakarta.mail keys is read the same way
			boolean sslEnabled = this.security.equals(MailProperties.SSL_SECURITY)
					|| Boolean.parseBoolean(smssProp.getProperty(SSL_ENABLE_PROPERTY));
			if (!this.security.equals(MailProperties.NO_SECURITY)) {
				if (sslEnabled) {
					// ssl.enable on its own, with no socketFactory.class. naming a
					// socket factory is the legacy way to do this and it takes the
					// connection off the path where jakarta mail applies
					// checkserveridentity, so it would quietly cost the hostname
					// verification set below
					mailProps.setProperty(SSL_ENABLE_PROPERTY, "true");
				}
				mailProps.setProperty("mail.smtp.starttls.enable", "true");
				if (!sslEnabled) {
					// required, not just enabled, so a relay that quietly drops
					// STARTTLS cannot downgrade the message to plaintext. it is only
					// meaningful on a connection that did not start out encrypted
					mailProps.setProperty("mail.smtp.starttls.required", "true");
				}
				// for no man-in-the-middle attacks
				mailProps.setProperty("mail.smtp.ssl.checkserveridentity", "true");
				mailProps.setProperty("mail.smtp.ssl.protocols", "TLSv1.2 TLSv1.3");
			}

			mailProps.setProperty("mail.smtp.connectiontimeout", Integer
					.toString(NumberUtils.toInt(smssProp.getProperty(MailProperties.CONNECTION_TIMEOUT), 10_000)));
			int readTimeout = NumberUtils.toInt(smssProp.getProperty(MailProperties.READ_TIMEOUT), 30_000);
			mailProps.setProperty("mail.smtp.timeout", Integer.toString(readTimeout));
			mailProps.setProperty("mail.smtp.writetimeout", Integer.toString(readTimeout));

			if (authUsername != null) {
				// without this jakarta mail never asks the authenticator for the
				// credentials, so a username alone would go unused
				mailProps.setProperty("mail.smtp.auth", "true");
			}
		}

		if (this.tokenProvider != null) {
			Microsoft365MailOAuth.addXoauth2Properties(mailProps, "smtp");
		}

		// applied last so a raw mail. key wins over anything above
		for (String key : smssProp.stringPropertyNames()) {
			if (key.startsWith(MailProperties.RAW_MAIL_PROPERTY_PREFIX)) {
				mailProps.setProperty(key, smssProp.getProperty(key));
			}
		}

		if (authUsername == null) {
			classLogger.info("Creating an unauthenticated smtp session against {}:{}", effectiveHost, effectivePort);
			return Session.getInstance(mailProps);
		}
		classLogger.info("Creating an smtp session against {}:{}, signing in as {} with {}", effectiveHost,
				effectivePort, authUsername, credentialDescription());
		return Session.getInstance(mailProps, new jakarta.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				// asked for on every connect, which is what lets a mailbox whose
				// token expires hand over a current one per send
				return new PasswordAuthentication(authUsername, connectPassword());
			}
		});
	}

	/**
	 * The secret to sign in with, read per connect so an expiring token is replaced
	 * rather than reused.
	 *
	 * @return the password or token
	 */
	private String connectPassword() {
		if (this.tokenProvider != null) {
			return this.tokenProvider.getAccessToken();
		}
		return this.password;
	}

	/**
	 * @return what this transport signs in with, for the session log line
	 */
	private String credentialDescription() {
		if (this.tokenProvider != null) {
			return Microsoft365MailOAuth.credentialDescription(this.tokenProvider.getClientId());
		}
		return "a password";
	}

	@Override
	public String describe() {
		return this.host + ":" + this.port;
	}

	@Override
	public String failureHint() {
		if (this.tokenProvider == null) {
			return null;
		}
		return "If the log shows the sign in was refused: " + Microsoft365MailOAuth
				.authenticationHint(Microsoft365MailOAuth.SMTP_PERMISSION, Microsoft365MailOAuth.SMTP_AUTH_HINT);
	}

	/**
	 * Put every raw jakarta.mail key back in the form the mail library answers to.
	 *
	 * <p>
	 * jakarta.mail property names are lower case and it ignores anything else,
	 * while the reactor that writes an SMSS upper cases every key it is given.
	 * Without this, a relay configured through the UI with a property this
	 * transport does not model - the whole point of the {@code mail.} passthrough -
	 * would arrive as {@code MAIL.SMTP.SSL.TRUST} and be silently ignored.
	 *
	 * @param smssProp the engine properties as they were written
	 * @return the same properties with the raw mail keys lower cased
	 */
	private static Properties normalizeRawMailKeys(Properties smssProp) {
		Properties normalized = new Properties();
		for (String key : smssProp.stringPropertyNames()) {
			if (key.toLowerCase().startsWith(MailProperties.RAW_MAIL_PROPERTY_PREFIX)) {
				normalized.setProperty(key.toLowerCase(), smssProp.getProperty(key));
			} else {
				normalized.setProperty(key, smssProp.getProperty(key));
			}
		}
		return normalized;
	}

	private static boolean parseBoolean(String value, boolean defaultValue) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}

	private static String trimToNull(String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return null;
		}
		return value;
	}

}
