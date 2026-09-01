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
package prerna.engine.impl.function.mail;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailTracking;

/**
 * Sends through Microsoft Graph.
 *
 * <p>
 * The call itself is {@link MicrosoftOutlookMailHelper}, which knows nothing
 * about engines or SMSS files, so the same code serves a delegated caller
 * sending as the signed in user. This class is only the app-only half: it reads
 * the app registration out of the SMSS, holds the token, and hands the helper a
 * mailbox to send as.
 *
 * <p>
 * Sending this way needs the {@code Mail.Send} application permission and
 * nothing else - no SMTP AUTH on the tenant or the mailbox, no basic protocol
 * to keep enabled, and no separate {@code SMTP.SendAsApp} grant.
 */
public class GraphMailTransport implements MailTransport {

	private static final Logger classLogger = LogManager.getLogger(GraphMailTransport.class);

	private MicrosoftGraphAppTokenProvider tokenProvider = null;
	private MicrosoftOutlookMailHelper mail = null;
	private boolean saveToSentItems = true;

	// graph posts against a mailbox, so the sender is part of the address rather
	// than only a header
	private String sender = null;
	private String senderName = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// the provider validates the credentials, so an engine missing one of them
		// fails on open rather than on the first send
		this.tokenProvider = new MicrosoftGraphAppTokenProvider(
				smssProp.getProperty(SMTPFunctionEngine.GRAPH_TENANT_KEY),
				smssProp.getProperty(SMTPFunctionEngine.GRAPH_CLIENT_ID_KEY),
				smssProp.getProperty(SMTPFunctionEngine.GRAPH_CLIENT_SECRET_KEY),
				StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(SMTPFunctionEngine.GRAPH_SCOPE_KEY)),
						SMTPFunctionEngine.DEFAULT_GRAPH_SCOPE));

		this.mail = new MicrosoftOutlookMailHelper(smssProp.getProperty(SMTPFunctionEngine.GRAPH_BASE_URL_KEY));
		this.saveToSentItems = parseBoolean(smssProp.getProperty(SMTPFunctionEngine.SAVE_TO_SENT_ITEMS_KEY),
				this.saveToSentItems);

		this.sender = trimToNull(smssProp.getProperty(SMTPFunctionEngine.SMTP_SENDER_KEY));
		this.senderName = trimToNull(smssProp.getProperty(SMTPFunctionEngine.SMTP_SENDER_NAME_KEY));
		if (this.sender == null) {
			// unlike a relay, there is no way to post a message without naming the
			// mailbox it comes from
			throw new IllegalArgumentException("Must define " + SMTPFunctionEngine.SMTP_SENDER_KEY
					+ " in SMSS, since Graph sends as a particular mailbox");
		}
		ExchangeMailOAuth.validateMailbox(this.sender, SMTPFunctionEngine.SMTP_SENDER_KEY);
	}

	@Override
	public boolean send(String[] to, String[] cc, String[] bcc, String from, String subject, String message,
			boolean html, String[] attachments) {
		String requested = addressOf(from);
		if (requested != null && !requested.equalsIgnoreCase(this.sender)) {
			// graph sends as the mailbox the request is posted against, so a different
			// address would be silently ignored rather than honored. a display name is
			// not a different address, and is carried on the message below
			classLogger.warn("Graph sends as the mailbox it posts against, so this is sent as {} rather than {}",
					this.sender, requested);
		}

		boolean success = false;
		try {
			Map<String, Object> built = MicrosoftOutlookMailHelper.buildMessage(subject, message, html, to, cc, bcc,
					this.sender, this.senderName, attachments);
			this.mail.sendMail(getAccessToken(), this.sender, built, this.saveToSentItems);
			success = true;
		} catch (IOException e) {
			classLogger.error("Could not build the email with subject '{}' to send", subject, e);
		} catch (RuntimeException e) {
			classLogger.error("Error sending the email as {} through Graph", this.sender, e);
			// the usual reason a send starts failing is a permission that is about to
			// be changed, and a cached token would go on being refused for its hour
			this.tokenProvider.invalidate();
		}

		// the smtp transport records its sends by going through EmailUtility, which
		// this does not, so the row is written here instead. otherwise the tracking
		// table would only hold the sends that happened to go over a relay
		MicrosoftOutlookMailTracking.trackSend(to, cc, bcc, this.sender, subject, message, html, attachments, success);
		return success;
	}

	/**
	 * The bearer token for this send. Asked for per send rather than held, so a
	 * token that expired since the last one is replaced. The provider hands back
	 * its cached token until it is close to expiring.
	 *
	 * @return the access token
	 */
	protected String getAccessToken() {
		return this.tokenProvider.getAccessToken();
	}

	@Override
	public String describe() {
		return "Microsoft Graph";
	}

	@Override
	public String failureHint() {
		return "If the log shows Graph refused the request, " + ExchangeMailOAuth.tokenDiagnostic(this.tokenProvider)
				+ ", and sending needs the " + SMTPFunctionEngine.GRAPH_SEND_PERMISSION
				+ " application permission with admin consent";
	}

	/**
	 * The bare address out of a sender that may carry a display name.
	 *
	 * @param from the sender as the caller gave it
	 * @return the address on its own, or null when there was none
	 */
	private static String addressOf(String from) {
		if (from == null) {
			return null;
		}
		int open = from.lastIndexOf('<');
		int close = from.lastIndexOf('>');
		if (open > -1 && close > open) {
			return from.substring(open + 1, close).trim();
		}
		return from.trim();
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
