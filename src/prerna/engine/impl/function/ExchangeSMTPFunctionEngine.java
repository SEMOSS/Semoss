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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.FunctionTypeEnum;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;

/**
 * Function engine that sends email through Microsoft 365.
 *
 * <p>
 * Exchange Online stopped accepting a mailbox password on SMTP AUTH, so this
 * sends with an app registration and a token in the same way
 * {@link ExchangeIMAPFunctionEngine} reads with one. Everything else about
 * sending - the pinned sender, the recipient domains, the recipient cap, the
 * attachment switch - is {@link SMTPFunctionEngine} unchanged.
 *
 * <p>
 * Sending has more to line up than reading does, and none of it is in the same
 * place:
 *
 * <ul>
 * <li>the app registration needs the {@code SMTP.SendAsApp} application
 * permission with admin consent, which is a different permission from the IMAP
 * and POP ones and is granted separately</li>
 * <li>Exchange has to let the application use the mailbox, registered through
 * {@code New-ServicePrincipal}</li>
 * <li>SMTP AUTH has to be enabled, both for the tenant and for the mailbox -
 * Microsoft turns it off by default, and a tenant wide switch overrides the
 * per mailbox one</li>
 * <li>the address a message is sent as has to be the mailbox that was signed in
 * to, so {@code SMTP_SENDER} and {@code SMTP_USERNAME} normally match</li>
 * </ul>
 *
 * <p>
 * Sending goes out on a different host from reading, {@code smtp.office365.com}
 * rather than {@code outlook.office365.com}, on the submission port with
 * STARTTLS. Those are the defaults here, so an engine only names them when it is
 * pointed somewhere else.
 */
public class ExchangeSMTPFunctionEngine extends SMTPFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(ExchangeSMTPFunctionEngine.class);

	/** The application permission a token has to carry to send. */
	public static final String SMTP_PERMISSION = "SMTP.SendAsApp";

	// the part of the setup that is neither in azure nor about permissions, and
	// the one most likely to be missed, since it is off until somebody turns it on
	private static final String SMTP_AUTH_HINT = """
			Also check that SMTP AUTH is enabled for this mailbox and for the tenant, since Microsoft \
			disables it by default and the tenant wide setting overrides the per mailbox one.\
			""";

	private MicrosoftGraphAppTokenProvider tokenProvider = null;

	/**
	 * Build an Exchange SMTP engine that is not in the catalog, for a caller that
	 * already holds an app registration and wants this engine's connection handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mail server and app registration properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mail server
	 */
	public static ExchangeSMTPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		ExchangeSMTPFunctionEngine engine = new ExchangeSMTPFunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Send an email as the " + engineId + " mailbox"));
		return engine;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		// the provider validates the credentials, so an engine missing one of them
		// fails on open rather than on the first send. built before the session,
		// since building the session asks how this engine signs in
		this.tokenProvider = ExchangeMailOAuth.openTokenProvider(smssProp);

		super.open(smssProp);

		if (trimToNull(smssProp.getProperty(SMTP_PASSWORD_KEY)) != null) {
			// a password here is a sign the engine was set up as though it were a
			// plain relay, and Exchange would refuse it anyway
			classLogger.warn("A {} was set but Exchange Online signs in with a token, so the password is ignored",
					SMTP_PASSWORD_KEY);
		}
	}

	@Override
	protected void addAuthenticationProperties(Properties mailProps) {
		// smtp is always configured under the plain protocol name here, even when
		// the connection ends up encrypted
		ExchangeMailOAuth.addXoauth2Properties(mailProps, "smtp");
	}

	@Override
	protected String getConnectPassword() {
		// asked for on every send, so a token that has expired since the last one is
		// replaced rather than retried. the provider hands back its cached token
		// until it is close to expiring
		return this.tokenProvider.getAccessToken();
	}

	@Override
	protected String getCredentialDescription() {
		return ExchangeMailOAuth.credentialDescription(this.tokenProvider.getClientId());
	}

	@Override
	protected boolean requiresPassword() {
		return false;
	}

	@Override
	protected String getDefaultHost() {
		return ExchangeMailOAuth.SEND_HOST;
	}

	@Override
	protected String getSendFailureMessage() {
		// the send is handed off inside the mail library, so which of the two it was
		// is only in the log. the sign in is the one worth spelling out, since it has
		// four separate things that all have to be right
		return super.getSendFailureMessage() + ". If the log shows the sign in was refused: "
				+ ExchangeMailOAuth.authenticationHint(SMTP_PERMISSION, SMTP_AUTH_HINT);
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.EXCHANGE_SMTP.name();
	}

}
