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
package prerna.engine.impl.function.mail.engine;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.ExchangeStoreAuthentication;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.MailStoreAuthentication;
import prerna.engine.impl.function.mail.auth.microsoft365.Microsoft365MailOAuth;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;

/**
 * Function engine that reads a Microsoft 365 mailbox through Graph by default,
 * or through IMAP when {@code MAIL_TRANSPORT} selects Jakarta Mail.
 *
 * <p>
 * Both transports use the configured app registration, but each obtains a token
 * for the resource it calls. Graph uses its mail application permission. IMAP
 * uses {@value #IMAP_PERMISSION}, XOAUTH2 and the corresponding Exchange
 * mailbox grant.
 *
 * <p>
 * Folder selection, searching, read policy and optional mailbox changes retain
 * the contract defined by {@link IMAPFunctionEngine}, regardless of transport.
 */
public class ExchangeIMAPFunctionEngine extends IMAPFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(ExchangeIMAPFunctionEngine.class);

	/** The application permission a token has to carry to read over IMAP. */
	public static final String IMAP_PERMISSION = "IMAP.AccessAsApp";

	/**
	 * Build an Exchange engine that is not in the catalog, for a caller that
	 * already holds an app registration and wants this engine's connection handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox and app registration properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static ExchangeIMAPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		ExchangeIMAPFunctionEngine engine = new ExchangeIMAPFunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Read the email in the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	protected void openProtocolProperties(Properties smssProp) {
		super.openProtocolProperties(smssProp);

		Microsoft365MailOAuth.validateMailbox(trimToNull(smssProp.getProperty(key(USERNAME_SUFFIX))),
				key(USERNAME_SUFFIX));

		if (trimToNull(smssProp.getProperty(key(PASSWORD_SUFFIX))) != null) {
			// a password here is a sign the engine was set up as though it were a
			// plain mailbox, and Exchange would refuse it anyway
			classLogger.warn("A {} was set but Exchange Online signs in with a token, so the password is ignored",
					key(PASSWORD_SUFFIX));
		}
	}

	@Override
	protected MailStoreAuthentication createStoreAuthentication(Properties properties, JakartaStoreConfig config) {
		return new ExchangeStoreAuthentication(Microsoft365MailOAuth.openTokenProvider(properties), IMAP_PERMISSION);
	}

	/**
	 * How this engine reads when the SMSS does not say. Graph, because it needs one
	 * application permission where the protocol needs that plus a service principal
	 * and a mailbox grant, and because Microsoft keeps narrowing what the protocol
	 * endpoints will do.
	 *
	 * @return the transport name
	 */
	@Override
	protected String getDefaultTransport() {
		return Microsoft365MailOAuth.GRAPH_TRANSPORT;
	}

	@Override
	protected boolean requiresPassword() {
		return false;
	}

	@Override
	protected String getDefaultHost() {
		return Microsoft365MailOAuth.MAILBOX_HOST;
	}

	@Override
	protected String getAuthenticationHint() {
		if (isGraphTransport()) {
			// graph asks for one permission and nothing else, and only needs the
			// write one when this engine was told it may change the mailbox
			return Microsoft365MailOAuth
					.graphAuthenticationHint(changesTheMailbox() ? Microsoft365MailOAuth.GRAPH_READ_WRITE_PERMISSION
							: Microsoft365MailOAuth.GRAPH_READ_PERMISSION);
		}
		return Microsoft365MailOAuth.authenticationHint(IMAP_PERMISSION, null);
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.EXCHANGE_IMAP.name();
	}

}
