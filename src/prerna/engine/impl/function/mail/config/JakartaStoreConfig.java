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

import org.apache.commons.lang3.math.NumberUtils;

/**
 * Which mail server a Jakarta store connects to, and how.
 *
 * <p>
 * The same shape describes an IMAP store and a POP3 one, with the protocol
 * names passed in, so the two clients share their whole connection path.
 * Reading the settings here rather than in the client is what makes a
 * misconfigured engine fail while it is being cataloged, with a message naming
 * the key that is missing, instead of on somebody's first search.
 *
 * <p>
 * There are two spellings of every setting: the engine's own keys, such as
 * {@code IMAP_HOST}, and the raw jakarta.mail ones, such as
 * {@code mail.imaps.host}. The engine keys win where both are set. The raw ones
 * exist because no set of named keys covers every mail server, and are why
 * {@code sourceProperties} is carried along rather than discarded.
 *
 * @param protocol             the plain protocol name, {@code imap} or
 *                             {@code pop3}
 * @param secureProtocol       its implicit TLS name, {@code imaps} or
 *                             {@code pop3s}
 * @param storeProtocol        the one this connection actually uses, which
 *                             follows the security setting unless the SMSS
 *                             names it outright
 * @param host                 the mail server to reach
 * @param port                 the port to reach it on, or -1 for the protocol
 *                             default
 * @param username             the mailbox to open
 * @param password             the password, or null when a token is used
 *                             instead
 * @param security             {@code ssl}, {@code starttls} or {@code none}
 * @param onlyCustomProperties whether to skip the settings this class would
 *                             otherwise assemble and use only the raw
 *                             {@code mail.} keys
 * @param connectionTimeout    how long to wait to reach the server, in ms
 * @param readTimeout          how long to wait on a reply, in ms
 * @param sourceProperties     the SMSS properties this was read from, kept for
 *                             the raw {@code mail.} keys
 */
public record JakartaStoreConfig(String protocol, String secureProtocol, String storeProtocol, String host, int port,
		String username, String password, String security, boolean onlyCustomProperties, int connectionTimeout,
		int readTimeout, Properties sourceProperties) {

	/**
	 * Read the connection settings for one protocol out of an engine's SMSS.
	 *
	 * @param source            the engine's SMSS properties
	 * @param protocol          the plain protocol name
	 * @param secureProtocol    its implicit TLS name
	 * @param defaultHost       the host to assume when nothing names one, or null
	 *                          to insist on it
	 * @param defaultPort       the port for the plain protocol
	 * @param defaultSecurePort the port for the implicit TLS one
	 * @param passwordRequired  whether a missing password is an error, which it is
	 *                          not for an engine that signs in with a token
	 * @return the settings
	 * @throws IllegalArgumentException when the SMSS does not describe a mailbox
	 *                                  that can be opened
	 */
	public static JakartaStoreConfig from(Properties source, String protocol, String secureProtocol, String defaultHost,
			String defaultPort, String defaultSecurePort, boolean passwordRequired) {
		Properties properties = MailProperties.normalize(source);
		String security = MailProperties
				.firstNonNull(MailProperties.trimToNull(properties.getProperty(protocol.toUpperCase() + "_SECURITY")),
						MailProperties.SSL_SECURITY)
				.toLowerCase();
		if (!MailProperties.SSL_SECURITY.equals(security) && !MailProperties.STARTTLS_SECURITY.equals(security)
				&& !MailProperties.NO_SECURITY.equals(security)) {
			throw new IllegalArgumentException("Unsupported " + protocol + " security '" + security + "'");
		}

		String storeProtocol = MailProperties.firstNonNull(
				MailProperties.trimToNull(properties.getProperty(MailProperties.STORE_PROTOCOL)),
				MailProperties.SSL_SECURITY.equals(security) ? secureProtocol : protocol);
		String host = MailProperties.firstNonNull(
				MailProperties.trimToNull(properties.getProperty(protocol.toUpperCase() + "_HOST")),
				MailProperties.trimToNull(properties.getProperty(MailProperties.rawProperty(storeProtocol, "host"))),
				MailProperties.trimToNull(properties.getProperty(MailProperties.rawProperty(protocol, "host"))),
				MailProperties.trimToNull(properties.getProperty(MailProperties.rawProperty(secureProtocol, "host"))),
				defaultHost);
		if (host == null) {
			throw new IllegalArgumentException("Must define " + protocol.toUpperCase()
					+ "_HOST or a Jakarta Mail host property to know which mail server to read");
		}
		String port = MailProperties.firstNonNull(
				MailProperties.trimToNull(properties.getProperty(protocol.toUpperCase() + "_PORT")),
				MailProperties.trimToNull(properties.getProperty(MailProperties.rawProperty(storeProtocol, "port"))),
				secureProtocol.equalsIgnoreCase(storeProtocol) ? defaultSecurePort : defaultPort);
		String username = MailProperties.trimToNull(properties.getProperty(protocol.toUpperCase() + "_USERNAME"));
		String password = MailProperties.trimToNull(properties.getProperty(protocol.toUpperCase() + "_PASSWORD"));
		if (username == null) {
			throw new IllegalArgumentException(
					"Must define " + protocol.toUpperCase() + "_USERNAME to know which mailbox to open");
		}
		if (passwordRequired && password == null) {
			throw new IllegalArgumentException(
					"Must define " + protocol.toUpperCase() + "_PASSWORD to sign in to the mailbox");
		}

		return new JakartaStoreConfig(protocol, secureProtocol, storeProtocol, host, NumberUtils.toInt(port, -1),
				username, password, security,
				MailProperties.parseBoolean(properties.getProperty(MailProperties.ONLY_CUSTOM_PROPERTIES), false),
				NumberUtils.toInt(properties.getProperty(MailProperties.CONNECTION_TIMEOUT), 10_000),
				NumberUtils.toInt(properties.getProperty(MailProperties.READ_TIMEOUT), 30_000), properties);
	}

	/**
	 * @return true when this connection speaks TLS from the first byte, rather than
	 *         starting plain and upgrading
	 */
	public boolean secureProtocolSelected() {
		return this.secureProtocol.equalsIgnoreCase(this.storeProtocol);
	}
}
