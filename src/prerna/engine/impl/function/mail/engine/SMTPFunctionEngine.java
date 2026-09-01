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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.function.mail.adapter.graph.GraphMailSender;
import prerna.engine.impl.function.mail.adapter.smtp.SmtpMailSender;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.OutboundMail;
import prerna.engine.impl.function.mail.model.SendResult;
import prerna.engine.impl.function.mail.spi.MailSender;
import prerna.util.Constants;

/**
 * Function engine that sends email through a mail server.
 *
 * <p>
 * The mail server belongs to the engine, so one can be created per team, shared
 * through the normal engine permissions, and pointed at whichever relay that
 * team is allowed to send through.
 *
 * <p>
 * Sending is a side effect that cannot be undone, so the SMSS carries the send
 * policy rather than the caller: the sender address is fixed unless an admin
 * opts into overrides, recipients can be limited to a set of domains, the total
 * recipient count is capped, and attachments are off until turned on. Those are
 * applied by {@link AbstractSendMailFunctionEngine} before anything is sent, so
 * they hold however the message actually leaves.
 *
 * <p>
 * How it leaves is {@link #MAIL_TRANSPORT_KEY}. A plain relay only speaks SMTP,
 * which is what this engine defaults to; {@link ExchangeSMTPFunctionEngine} is
 * the same engine pointed at Microsoft 365, where Graph is the better default.
 */
public class SMTPFunctionEngine extends AbstractSendMailFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(SMTPFunctionEngine.class);

	/** Which way a message leaves. */
	public static final String MAIL_TRANSPORT_KEY = MailProperties.MAIL_TRANSPORT;

	/** Post the message to Microsoft Graph. */
	public static final String GRAPH_TRANSPORT = MailProperties.GRAPH_TRANSPORT;

	/** Hand the message to a mail server with the jakarta.mail library. */
	public static final String JAKARTA_TRANSPORT = MailProperties.JAKARTA_TRANSPORT;

	// public so a caller building this engine in memory rather than from a
	// cataloged SMSS can populate the properties by name
	public static final String SMTP_HOST_KEY = MailProperties.SMTP_HOST;
	public static final String SMTP_PORT_KEY = MailProperties.SMTP_PORT;
	public static final String SMTP_USERNAME_KEY = MailProperties.SMTP_USERNAME;
	public static final String SMTP_PASSWORD_KEY = MailProperties.SMTP_PASSWORD;
	public static final String SMTP_SECURITY_KEY = MailProperties.SMTP_SECURITY;
	public static final String ONLY_CUSTOM_PROPS_KEY = MailProperties.ONLY_CUSTOM_PROPERTIES;
	public static final String CONNECTION_TIMEOUT_KEY = MailProperties.CONNECTION_TIMEOUT;
	public static final String READ_TIMEOUT_KEY = MailProperties.READ_TIMEOUT;

	public static final String GRAPH_BASE_URL_KEY = MailProperties.GRAPH_BASE_URL;
	public static final String SAVE_TO_SENT_ITEMS_KEY = MailProperties.SAVE_TO_SENT_ITEMS;

	// a key starting with this is copied onto the mail session verbatim, so a
	// relay needing a jakarta.mail property this engine does not model can still
	// be configured without a code change
	public static final String RAW_MAIL_PROPERTY_PREFIX = MailProperties.RAW_MAIL_PROPERTY_PREFIX;

	// how an smtp connection is secured. starttls upgrades a plaintext connection
	// on the submission port, ssl opens an encrypted socket directly, and none is
	// only reasonable for an internal relay that does no TLS at all
	public static final String STARTTLS_SECURITY = MailProperties.STARTTLS_SECURITY;
	public static final String SSL_SECURITY = MailProperties.SSL_SECURITY;
	public static final String NONE_SECURITY = MailProperties.NO_SECURITY;

	/** The submission port. 465 is the usual pairing with ssl. */
	public static final String DEFAULT_SMTP_PORT = "587";

	/** The application permission a Graph token has to carry to send. */
	public static final String GRAPH_SEND_PERMISSION = "Mail.Send";

	/** The resource a Graph token is issued for. */
	public static final String DEFAULT_GRAPH_SCOPE = "https://graph.microsoft.com/.default";

	/** Where Graph lives, overridable for a sovereign cloud. */
	public static final String DEFAULT_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0";

	private String transportName = null;
	private MailSender transport = null;

	/**
	 * Build a send engine that is not in the catalog, for a caller that already
	 * holds a mail configuration and wants this engine's send handling rather than
	 * its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mail properties
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mail service
	 */
	public static SMTPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		SMTPFunctionEngine engine = new SMTPFunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Send an email through the " + engineId + " mail server"));
		return engine;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		// the shared send settings first, then whichever way this one sends
		super.open(smssProp);

		this.transportName = MailProperties
				.backend(smssProp, GRAPH_TRANSPORT.equals(getDefaultTransport()) ? MailProperties.Backend.GRAPH
						: MailProperties.Backend.JAKARTA)
				.name().toLowerCase();
		this.transport = createTransport(this.transportName);
		this.transport.open(smssProp);
		classLogger.info("{} sends through {}", smssProp.getProperty(Constants.ENGINE), this.transport.describe());
	}

	/**
	 * Build the way this engine sends.
	 *
	 * <p>
	 * Returned unopened, since the caller has the SMSS properties to open it with.
	 * An unrecognized value is refused rather than quietly falling back, because a
	 * typo would otherwise send mail through something nobody chose.
	 *
	 * @param transportName the configured transport
	 * @return the sender, not yet opened
	 * @throws IllegalArgumentException when the transport is not one this engine
	 *                                  has
	 */
	protected MailSender createTransport(String transportName) {
		if (GRAPH_TRANSPORT.equals(transportName)) {
			return new GraphMailSender();
		}
		if (JAKARTA_TRANSPORT.equals(transportName)) {
			return new SmtpMailSender();
		}
		throw new IllegalArgumentException("The " + MAIL_TRANSPORT_KEY + " of '" + transportName
				+ "' is not one this engine can send through, which is " + GRAPH_TRANSPORT + " or "
				+ JAKARTA_TRANSPORT);
	}

	/**
	 * How this engine sends when the SMSS does not say. A plain relay is only
	 * reachable over SMTP, so the Microsoft engine is the one that defaults to
	 * Graph.
	 *
	 * @return the transport name
	 */
	protected String getDefaultTransport() {
		return JAKARTA_TRANSPORT;
	}

	/**
	 * Which way this engine sends.
	 *
	 * @return the transport name
	 */
	public String getTransportName() {
		return this.transportName;
	}

	/**
	 * Send one email directly, for a caller holding the engine rather than going
	 * through the function parameters.
	 *
	 * <p>
	 * This is how the instance wide mail server is used - registration mail, access
	 * requests, account warnings - where the message is assembled in code and there
	 * is no function call to route it through. It goes straight to the sender, so
	 * the send policy that {@code execute} applies is not in play, and the caller
	 * is trusted to have addressed it correctly.
	 *
	 * @param to          the recipients, or null
	 * @param cc          the copied recipients, or null
	 * @param bcc         the blind copied recipients, or null
	 * @param from        the address to send as
	 * @param subject     the subject line
	 * @param message     the body
	 * @param html        whether the body is html rather than plain text
	 * @param attachments the files to attach, or null
	 * @return whether the mail server took the message
	 */
	public boolean sendEmail(String[] to, String[] cc, String[] bcc, String from, String subject, String message,
			boolean html, String[] attachments) {
		OutboundMail outgoing = new OutboundMail(asList(to), asList(cc), asList(bcc), from, subject, message, html,
				asList(attachments));
		return this.transport.send(outgoing).delivered();
	}

	/**
	 * @param values the addresses or paths, which may be null
	 * @return them as a list, empty when there are none
	 */
	private static List<String> asList(String[] values) {
		return values == null ? List.of() : Arrays.asList(values);
	}

	@Override
	protected SendResult sendMessage(OutboundMail message) {
		return this.transport.send(message);
	}

	@Override
	protected String getSendDescription() {
		return this.transport.describe();
	}

	@Override
	protected String getSendFailureMessage() {
		String hint = this.transport.failureHint();
		if (hint == null || hint.isEmpty()) {
			return super.getSendFailureMessage();
		}
		return super.getSendFailureMessage() + ". " + hint;
	}

	@Override
	protected String getDefaultFunctionDescription() {
		return """
				Send an email. Use this to notify someone of a result, deliver a summary, or route a request \
				onward. The message is sent immediately and cannot be recalled, so confirm the recipients and \
				the wording before calling this.\
				""";
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.SMTP.name();
	}

	@Override
	public void close() throws IOException {
		if (this.transport != null) {
			this.transport.close();
		}
	}

}
