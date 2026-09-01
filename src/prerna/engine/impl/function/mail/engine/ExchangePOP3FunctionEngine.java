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
import prerna.engine.impl.function.mail.adapter.jakarta.ExchangeStoreAuthentication;
import prerna.engine.impl.function.mail.auth.ExchangeMailOAuth;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.spi.MailStoreAuthentication;

/**
 * Function engine that reads a Microsoft 365 mailbox over POP3.
 *
 * <p>
 * Same sign in as {@link ExchangeIMAPFunctionEngine} - an app registration and
 * a token rather than a mailbox password - and the same single inbox with no
 * folders and no read state that POP3 always has. The application permission is
 * the POP one rather than the IMAP one, and they are granted separately, so an
 * app registration set up for one does not serve the other.
 *
 * <p>
 * On a Microsoft 365 mailbox this is the harder of the two to justify: it costs
 * the folders, the search, and the record of what has been read, and gains
 * nothing, since both protocols are reached with the same token. It exists for
 * a process that already speaks POP3, or a mailbox where only POP was turned
 * on.
 */
public class ExchangePOP3FunctionEngine extends POP3FunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(ExchangePOP3FunctionEngine.class);

	/** The application permission a token has to carry to read over POP3. */
	public static final String POP3_PERMISSION = "POP.AccessAsApp";

	/**
	 * Build an Exchange POP3 engine that is not in the catalog, for a caller that
	 * already holds an app registration and wants this engine's connection handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mailbox and app registration properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mailbox
	 */
	public static ExchangePOP3FunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		ExchangePOP3FunctionEngine engine = new ExchangePOP3FunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Read the email in the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	protected void openProtocolProperties(Properties smssProp) {
		super.openProtocolProperties(smssProp);

		ExchangeMailOAuth.validateMailbox(trimToNull(smssProp.getProperty(key(USERNAME_SUFFIX))), key(USERNAME_SUFFIX));

		if (trimToNull(smssProp.getProperty(key(PASSWORD_SUFFIX))) != null) {
			// a password here is a sign the engine was set up as though it were a
			// plain mailbox, and Exchange would refuse it anyway
			classLogger.warn("A {} was set but Exchange Online signs in with a token, so the password is ignored",
					key(PASSWORD_SUFFIX));
		}
	}

	@Override
	protected MailStoreAuthentication createStoreAuthentication(Properties properties, JakartaStoreConfig config) {
		return new ExchangeStoreAuthentication(ExchangeMailOAuth.openTokenProvider(properties), POP3_PERMISSION);
	}

	/**
	 * How this engine reads when the SMSS does not say. Graph, for the same reason
	 * the other Microsoft engines default to it.
	 *
	 * <p>
	 * Over Graph the difference between this engine and the IMAP one all but
	 * disappears, since the same API answers both. What remains is this engine's
	 * own promise of a single inbox, which {@link #resolveFolderName(String)} still
	 * keeps.
	 *
	 * @return the transport name
	 */
	@Override
	protected String getDefaultTransport() {
		return ExchangeMailOAuth.GRAPH_TRANSPORT;
	}

	@Override
	protected boolean requiresPassword() {
		return false;
	}

	@Override
	protected String getDefaultHost() {
		return ExchangeMailOAuth.MAILBOX_HOST;
	}

	@Override
	protected String getAuthenticationHint() {
		if (isGraphTransport()) {
			// this engine never changes a mailbox, so reading is all it ever needs
			return ExchangeMailOAuth.graphAuthenticationHint(ExchangeMailOAuth.GRAPH_READ_PERMISSION);
		}
		return ExchangeMailOAuth.authenticationHint(POP3_PERMISSION, null);
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.EXCHANGE_POP3.name();
	}

}
