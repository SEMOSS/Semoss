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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import prerna.engine.api.FunctionTypeEnum;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.EmailUtility;

/**
 * Function engine that sends email through an SMTP server.
 *
 * <p>
 * The mail server belongs to the engine, so one can be created per team, shared
 * through the normal engine permissions, and pointed at whichever relay that
 * team is allowed to send through.
 *
 * <p>
 * Sending is a side effect that cannot be undone, so the SMSS carries
 * additional guardrails rather than the caller: the sender address is fixed
 * unless an admin opts into overrides, recipients can be limited to a set of
 * domains, the total recipient count is capped, and attachments are off until
 * turned on.
 */
public class SMTPFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(SMTPFunctionEngine.class);

	// public so a caller building this engine in memory rather than from a
	// catalogued SMSS can populate the properties by name
	public static final String SMTP_HOST_KEY = "SMTP_HOST";
	public static final String SMTP_PORT_KEY = "SMTP_PORT";
	public static final String SMTP_USERNAME_KEY = "SMTP_USERNAME";
	public static final String SMTP_PASSWORD_KEY = "SMTP_PASSWORD";
	public static final String SMTP_SENDER_KEY = "SMTP_SENDER";
	public static final String SMTP_SENDER_NAME_KEY = "SMTP_SENDER_NAME";
	public static final String SMTP_SECURITY_KEY = "SMTP_SECURITY";
	public static final String ONLY_CUSTOM_PROPS_KEY = "ONLY_CUSTOM_PROPS";
	public static final String ALLOW_SENDER_OVERRIDE_KEY = "ALLOW_SENDER_OVERRIDE";
	public static final String ALLOWED_RECIPIENT_DOMAINS_KEY = "ALLOWED_RECIPIENT_DOMAINS";
	public static final String DEFAULT_TO_KEY = "DEFAULT_TO";
	public static final String DEFAULT_CC_KEY = "DEFAULT_CC";
	public static final String DEFAULT_BCC_KEY = "DEFAULT_BCC";
	public static final String SUBJECT_PREFIX_KEY = "SUBJECT_PREFIX";
	public static final String HTML_KEY = "HTML";
	public static final String MAX_RECIPIENTS_KEY = "MAX_RECIPIENTS";
	public static final String CONNECTION_TIMEOUT_KEY = "CONNECTION_TIMEOUT";
	public static final String READ_TIMEOUT_KEY = "READ_TIMEOUT";
	public static final String ALLOW_ATTACHMENTS_KEY = "ALLOW_ATTACHMENTS";

	// a key starting with this is copied onto the mail session verbatim, so a
	// relay needing a jakarta.mail property this engine does not model can still
	// be configured without a code change
	public static final String RAW_MAIL_PROPERTY_PREFIX = "mail.";

	// how the connection is secured. starttls upgrades a plaintext connection on
	// the submission port, ssl opens an encrypted socket directly, and none is
	// only reasonable for an internal relay that does no TLS at all
	public static final String STARTTLS_SECURITY = "starttls";
	public static final String SSL_SECURITY = "ssl";
	public static final String NONE_SECURITY = "none";

	// the submission port. 465 is the usual pairing with ssl
	private static String DEFAULT_PORT = "587";

	// jakarta.mail key that turns a plain smtp connection into an encrypted one.
	// read off the raw properties so a configuration that only ever spoke in
	// jakarta.mail keys still gets the matching socket factory settings
	private static String SSL_ENABLE_PROPERTY = "mail.smtp.ssl.enable";

	// parameters execute understands
	private static String TO_PARAM = "to";
	private static String CC_PARAM = "cc";
	private static String BCC_PARAM = "bcc";
	private static String SUBJECT_PARAM = "subject";
	private static String MESSAGE_PARAM = "message";
	private static String HTML_PARAM = "html";
	private static String FROM_PARAM = "from";
	private static String ATTACHMENTS_PARAM = "attachments";

	private String host = null;
	private String port = DEFAULT_PORT;
	private String username = null;
	private String password = null;
	private String security = STARTTLS_SECURITY;

	// the address every message is sent as, unless overrides are turned on
	private String sender = null;
	private String senderName = null;
	private boolean allowSenderOverride = false;

	// blank means any recipient is allowed
	private Set<String> allowedRecipientDomains = new LinkedHashSet<>();

	private List<String> defaultTo = new ArrayList<>();
	private List<String> defaultCc = new ArrayList<>();
	private List<String> defaultBcc = new ArrayList<>();

	private String subjectPrefix = null;
	private boolean html = false;
	private int maxRecipients = 25;
	private boolean allowAttachments = false;

	private Session emailSession = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		// read from here on rather than from the argument, so a raw jakarta.mail key
		// is found whichever case it arrived in
		smssProp = normalizeRawMailKeys(smssProp);

		this.host = trimToNull(smssProp.getProperty(SMTP_HOST_KEY));
		if (this.host == null) {
			this.host = getDefaultHost();
		}
		if (this.host == null && trimToNull(smssProp.getProperty("mail.smtp.host")) == null) {
			throw new IllegalArgumentException(
					"Must have key " + SMTP_HOST_KEY + " or mail.smtp.host in SMSS to know which mail server to use");
		}

		// the sender is not required here. a cataloged engine pins it so every
		// message goes out as the same address, but an engine built in memory to
		// serve as a shared mail connection takes the sender per send, so this is
		// checked when a message is actually built instead
		this.sender = trimToNull(smssProp.getProperty(SMTP_SENDER_KEY));
		if (this.sender != null) {
			validateEmailAddress(this.sender, SMTP_SENDER_KEY);
		}

		// the UI writes a blank line for every optional field left empty, so an
		// unset key arrives as "" rather than absent - defaultIfEmpty covers both
		this.port = StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(SMTP_PORT_KEY)), this.port);
		this.username = trimToNull(smssProp.getProperty(SMTP_USERNAME_KEY));
		this.password = trimToNull(smssProp.getProperty(SMTP_PASSWORD_KEY));
		if (!requiresPassword()) {
			if (this.username == null) {
				throw new IllegalArgumentException(
						"Must define " + SMTP_USERNAME_KEY + " in SMSS to know which mailbox to send as");
			}
		} else if ((this.username == null) != (this.password == null)) {
			// half a credential cannot authenticate, so drop it and connect
			// unauthenticated rather than fail to open. which one happened is in the
			// session log line below
			classLogger.warn("Only one of {} and {} is set, so the connection to {} will not authenticate",
					SMTP_USERNAME_KEY, SMTP_PASSWORD_KEY, this.host);
			this.username = null;
			this.password = null;
		}
		this.senderName = trimToNull(smssProp.getProperty(SMTP_SENDER_NAME_KEY));

		this.security = StringUtils.defaultIfEmpty(trimToNull(smssProp.getProperty(SMTP_SECURITY_KEY)), this.security)
				.toLowerCase();
		if (!this.security.equals(STARTTLS_SECURITY) && !this.security.equals(SSL_SECURITY)
				&& !this.security.equals(NONE_SECURITY)) {
			throw new IllegalArgumentException("SMTPFunctionEngine only supports " + STARTTLS_SECURITY + ", "
					+ SSL_SECURITY + ", or " + NONE_SECURITY + " for the " + SMTP_SECURITY_KEY + " key");
		}

		this.allowSenderOverride = parseBoolean(smssProp.getProperty(ALLOW_SENDER_OVERRIDE_KEY),
				this.allowSenderOverride);
		this.allowAttachments = parseBoolean(smssProp.getProperty(ALLOW_ATTACHMENTS_KEY), this.allowAttachments);
		this.html = parseBoolean(smssProp.getProperty(HTML_KEY), this.html);

		for (String domain : splitList(smssProp.getProperty(ALLOWED_RECIPIENT_DOMAINS_KEY))) {
			// stored bare so both "@blah.org" and "blah.org" are the same
			this.allowedRecipientDomains.add(domain.toLowerCase().replaceFirst("^@", ""));
		}

		this.defaultTo = splitList(smssProp.getProperty(DEFAULT_TO_KEY));
		this.defaultCc = splitList(smssProp.getProperty(DEFAULT_CC_KEY));
		this.defaultBcc = splitList(smssProp.getProperty(DEFAULT_BCC_KEY));
		validateRecipients(this.defaultTo, DEFAULT_TO_KEY);
		validateRecipients(this.defaultCc, DEFAULT_CC_KEY);
		validateRecipients(this.defaultBcc, DEFAULT_BCC_KEY);

		this.subjectPrefix = trimToNull(smssProp.getProperty(SUBJECT_PREFIX_KEY));
		this.maxRecipients = Math.max(1,
				NumberUtils.toInt(smssProp.getProperty(MAX_RECIPIENTS_KEY), this.maxRecipients));

		this.emailSession = buildEmailSession(smssProp);

		// the SMSS does not have to spell out the function metadata since we know
		// what execute supports. anything defined in the SMSS wins
		setDefaultFunctionMetadata();
	}

	/**
	 * Build the mail session this engine sends through.
	 *
	 * <p>
	 * The defaults are deliberately strict: TLS is required rather than merely
	 * offered, the server certificate has to match the host, and the protocol floor
	 * is TLS 1.2.
	 *
	 * <p>
	 * Anything the caller spelled out as a raw {@code mail.} property is layered on
	 * last and wins, and {@link #ONLY_CUSTOM_PROPS_KEY} skips the defaults entirely
	 * for a server that needs to be described purely in jakarta.mail terms.
	 *
	 * @param smssProp the engine properties
	 * @return the session to send every message through
	 */
	private Session buildEmailSession(Properties smssProp) {
		boolean onlyCustomProps = parseBoolean(smssProp.getProperty(ONLY_CUSTOM_PROPS_KEY), false);

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
			boolean sslEnabled = this.security.equals(SSL_SECURITY)
					|| Boolean.parseBoolean(smssProp.getProperty(SSL_ENABLE_PROPERTY));
			if (!this.security.equals(NONE_SECURITY)) {
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

			mailProps.setProperty("mail.smtp.connectiontimeout",
					Integer.toString(NumberUtils.toInt(smssProp.getProperty(CONNECTION_TIMEOUT_KEY), 10_000)));
			int readTimeout = NumberUtils.toInt(smssProp.getProperty(READ_TIMEOUT_KEY), 30_000);
			mailProps.setProperty("mail.smtp.timeout", Integer.toString(readTimeout));
			mailProps.setProperty("mail.smtp.writetimeout", Integer.toString(readTimeout));

			if (authUsername != null) {
				// without this jakarta mail never asks the authenticator for the
				// credentials, so a username alone would go unused
				mailProps.setProperty("mail.smtp.auth", "true");
			}
		}

		// how this engine signs in, for a mail server that does not take a password
		addAuthenticationProperties(mailProps);

		// applied last so a raw mail. key wins over anything above
		for (String key : smssProp.stringPropertyNames()) {
			if (key.startsWith(RAW_MAIL_PROPERTY_PREFIX)) {
				mailProps.setProperty(key, smssProp.getProperty(key));
			}
		}

		if (authUsername == null) {
			classLogger.info("Creating an unauthenticated smtp session against {}:{}", effectiveHost, effectivePort);
			return Session.getInstance(mailProps);
		}
		classLogger.info("Creating an smtp session against {}:{}, signing in as {} with {}", effectiveHost,
				effectivePort, authUsername, getCredentialDescription());
		return Session.getInstance(mailProps, new jakarta.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				// asked for on every connect, which is what lets an engine whose
				// credential expires hand over a current one per send
				return new PasswordAuthentication(getConnectUsername(), getConnectPassword());
			}
		});
	}

	/**
	 * The mail server to use when the SMSS does not name one, for a hosted service
	 * that always lives at the same address.
	 *
	 * @return the host, or null when it has to be configured
	 */
	protected String getDefaultHost() {
		return null;
	}

	/**
	 * Whether signing in needs a password in the SMSS. False for an engine that
	 * obtains its own credential, such as an access token.
	 *
	 * @return true when a username without a password means no authentication
	 */
	protected boolean requiresPassword() {
		return true;
	}

	/**
	 * Add the jakarta.mail properties that describe how this engine signs in.
	 * Called while the session is being built, before the raw {@code mail.} keys
	 * from the SMSS, so a caller can still override any of them.
	 *
	 * @param mailProps the session properties being built
	 */
	protected void addAuthenticationProperties(Properties mailProps) {
		// password authentication needs nothing beyond the credentials themselves
	}

	/**
	 * The user to sign in as.
	 *
	 * @return the username
	 */
	protected String getConnectUsername() {
		return this.username;
	}

	/**
	 * The secret to sign in with. Asked for on every connect rather than held, so
	 * an engine whose credential expires can hand over a current one.
	 *
	 * @return the password or token
	 */
	protected String getConnectPassword() {
		return this.password;
	}

	/**
	 * What this engine signs in with, for the log line that records the session.
	 *
	 * @return a short description of the credential
	 */
	protected String getCredentialDescription() {
		return "a password";
	}

	/**
	 * What to tell a caller when the mail server would not take the message. The
	 * reason itself is in the log, since by then the send has already been handed
	 * off, so this is about where to look.
	 *
	 * @return the error message
	 */
	protected String getSendFailureMessage() {
		return "The email was not sent. Check the smtp server settings on this function engine and the logs for details";
	}

	/**
	 * Build an SMTP engine that is not in the catalog, for a caller that already
	 * holds a mail server configuration and wants this engine's connection handling
	 * rather than its own.
	 *
	 * @param engineId the id to open under, used only for logging
	 * @param props    the mail server properties, either this engine's own keys or
	 *                 raw {@code mail.} keys
	 * @return the opened engine
	 * @throws Exception when the properties do not describe a usable mail server
	 */
	public static SMTPFunctionEngine openTransientEngine(String engineId, Properties props) throws Exception {
		SMTPFunctionEngine engine = new SMTPFunctionEngine();
		// no folder structure, no secret store lookup, no catalog entry
		engine.setBasic(true);
		engine.open(transientProperties(engineId, props, "Send an email through the " + engineId + " mail server"));
		return engine;
	}

	/**
	 * Fill in what {@link AbstractFunctionEngine} needs for an engine that is built
	 * in memory rather than read out of a catalogued SMSS.
	 *
	 * @param engineId           the id to open under, used only for logging
	 * @param props              the mail server properties
	 * @param defaultDescription the function description to use when there is none
	 * @return the properties to open the engine with
	 */
	protected static Properties transientProperties(String engineId, Properties props, String defaultDescription) {
		Properties engineProps = new Properties();
		engineProps.putAll(props);
		engineProps.put(Constants.ENGINE, engineId);

		// the function metadata only matters to a model picking this out of a tool
		// list, which a transient engine is never in, but AbstractFunctionEngine
		// requires both to be present
		if (trimToNull(engineProps.getProperty(NAME_KEY)) == null) {
			engineProps.put(NAME_KEY, engineId);
		}
		if (trimToNull(engineProps.getProperty(DESCRIPTION_KEY)) == null) {
			engineProps.put(DESCRIPTION_KEY, defaultDescription);
		}
		return engineProps;
	}

	/**
	 * Send one email through this engine's mail server, with the message already
	 * assembled by the caller.
	 *
	 * <p>
	 * This is the raw send, for server side code that resolved and authorized the
	 * message itself - a pixel call that already knows which files it may attach
	 * and who it may write to. The guardrails in {@link #execute(Map)} are the
	 * contract this engine offers a model that picked it out of a tool list, so
	 * they are applied there rather than here. Both go out through this method, so
	 * the session stays inside the engine either way.
	 *
	 * @param to          the to recipients, or null when there are none
	 * @param cc          the cc recipients, or null when there are none
	 * @param bcc         the bcc recipients, or null when there are none
	 * @param from        the sender address
	 * @param subject     the subject line
	 * @param message     the body of the email
	 * @param html        whether the body is html rather than plain text
	 * @param attachments the file paths to attach, or null when there are none
	 * @return true when the message was handed off to the mail server
	 */
	public boolean sendEmail(String[] to, String[] cc, String[] bcc, String from, String subject, String message,
			boolean html, String[] attachments) {
		return EmailUtility.sendEmail(this.emailSession, to, cc, bcc, from, subject, message, html, attachments);
	}

	/**
	 * Fill in the function metadata that the SMSS did not define. The parameters
	 * describe what {@link #execute(Map)} understands, and their descriptions carry
	 * the defaults this engine was opened with so a caller knows what it gets when
	 * it leaves one out. A value set in the SMSS is never overwritten.
	 */
	private void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = """
					Send an email through a configured SMTP server. Use this to notify someone of a result, \
					deliver a summary, or route a request onward. The message is sent immediately and cannot \
					be recalled, so confirm the recipients and the wording before calling this.\
					""";
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			defaultParameters.add(new FunctionParameter(TO_PARAM, "string", """
					Comma separated list of recipient email addresses.\
					""" + defaultText(joinList(this.defaultTo)) + allowedDomainText()));
			defaultParameters.add(new FunctionParameter(CC_PARAM, "string", """
					Optional comma separated list of addresses to copy.\
					""" + defaultText(joinList(this.defaultCc))));
			defaultParameters.add(new FunctionParameter(BCC_PARAM, "string", """
					Optional comma separated list of addresses to blind copy.\
					""" + defaultText(joinList(this.defaultBcc))));
			defaultParameters.add(new FunctionParameter(SUBJECT_PARAM, "string", """
					The subject line. Keep it short and specific to what the message is about.\
					"""));
			defaultParameters.add(new FunctionParameter(MESSAGE_PARAM, "string", """
					The body of the email. Write the full message, not a summary of it.\
					"""));
			defaultParameters.add(new FunctionParameter(HTML_PARAM, "boolean", """
					Optional. Set to true when the body is html rather than plain text. Defaults to %s.\
					""".formatted(this.html)));
			if (this.sender == null) {
				defaultParameters.add(new FunctionParameter(FROM_PARAM, "string", """
						The sender address. This mail server does not have one configured, so every email has \
						to say who it is from.\
						"""));
			} else if (this.allowSenderOverride) {
				defaultParameters.add(new FunctionParameter(FROM_PARAM, "string", """
						Optional sender address.\
						""" + defaultText(this.sender)));
			}
			if (this.allowAttachments) {
				defaultParameters.add(new FunctionParameter(ATTACHMENTS_PARAM, "string", """
						Optional comma separated list of file names to attach. Each must already exist in the \
						files of the insight making this call.\
						"""));
			}
			this.parameters = defaultParameters;
		}

		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			List<String> defaultRequired = new ArrayList<>(Arrays.asList(SUBJECT_PARAM, MESSAGE_PARAM));
			// to is only required when there is nothing to fall back on, otherwise an
			// engine pointed at a fixed distribution list would reject every call
			if (this.defaultTo.isEmpty() && this.defaultCc.isEmpty() && this.defaultBcc.isEmpty()) {
				defaultRequired.add(0, TO_PARAM);
			}
			if (this.sender == null) {
				defaultRequired.add(FROM_PARAM);
			}
			this.requiredParameters = defaultRequired;
		}
	}

	/**
	 * Send a single email.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return a map describing what was sent, so the caller can repeat the
	 *         recipients back rather than guess at them
	 */
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		if (parameterValues == null) {
			parameterValues = new HashMap<>();
		}
		Insight executingInsight = (Insight) parameterValues.remove(Constants.INSIGHT);

		validateRequiredParameters(parameterValues);

		List<String> to = resolveRecipients(parameterValues, TO_PARAM, this.defaultTo);
		List<String> cc = resolveRecipients(parameterValues, CC_PARAM, this.defaultCc);
		List<String> bcc = resolveRecipients(parameterValues, BCC_PARAM, this.defaultBcc);
		if (to.isEmpty() && cc.isEmpty() && bcc.isEmpty()) {
			throw new IllegalArgumentException(
					"Must define at least one recipient through " + TO_PARAM + ", " + CC_PARAM + ", or " + BCC_PARAM);
		}
		int recipientCount = to.size() + cc.size() + bcc.size();
		if (recipientCount > this.maxRecipients) {
			throw new IllegalArgumentException("This function engine sends to at most " + this.maxRecipients
					+ " recipients per email but " + recipientCount + " were provided");
		}

		String from = resolveSender(parameterValues);
		String subject = applySubjectPrefix(getParameterValue(parameterValues, SUBJECT_PARAM, ""));
		String message = getParameterValue(parameterValues, MESSAGE_PARAM, "");
		boolean runTimeHtml = getBooleanParameterValue(parameterValues, HTML_PARAM, this.html);
		String[] attachments = resolveAttachments(parameterValues, executingInsight);

		classLogger.info("Sending an email from {} to {} recipient(s) via {}:{}", from, recipientCount, this.host,
				this.port);

		boolean success = sendEmail(toArray(to), toArray(cc), toArray(bcc), from, subject, message, runTimeHtml,
				attachments);
		if (!success) {
			throw new IllegalArgumentException(getSendFailureMessage());
		}

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("success", true);
		output.put("from", from);
		output.put("to", to);
		output.put("cc", cc);
		output.put("bcc", bcc);
		output.put("subject", subject);
		if (attachments != null) {
			// the names, not the resolved paths, so the caller is not handed the
			// server side layout of the insight folder
			List<String> attachmentNames = new ArrayList<>();
			for (String attachment : attachments) {
				attachmentNames.add(new File(attachment).getName());
			}
			output.put("attachments", attachmentNames);
		}
		return output;
	}

	/**
	 * Pull one set of recipients for this call, falling back to the SMSS default
	 * when the caller did not pass any, and reject anything that is not a valid
	 * address or is outside the allowed domains.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @param key             the recipient parameter to read
	 * @param defaultValue    the SMSS level recipients for this parameter
	 * @return the addresses to send to, empty when there are none
	 */
	private List<String> resolveRecipients(Map<String, Object> parameterValues, String key, List<String> defaultValue) {
		String value = getParameterValue(parameterValues, key, null);
		if (value == null) {
			// copied so the returned list, which ends up in the execute output, is
			// never the engine's own default list
			return new ArrayList<>(defaultValue);
		}
		List<String> recipients = splitList(value);
		validateRecipients(recipients, key);
		return recipients;
	}

	/**
	 * Work out what address this message is sent as. The SMSS sender is used unless
	 * overrides are turned on, since most relays reject a from that does not match
	 * the authenticated account anyway.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return the from header to send
	 */
	private String resolveSender(Map<String, Object> parameterValues) {
		String runTimeFrom = getParameterValue(parameterValues, FROM_PARAM, null);
		if (runTimeFrom != null) {
			// with no sender configured there is no pinned identity to protect, so
			// the caller has to supply one and the override flag does not apply
			if (this.sender != null && !this.allowSenderOverride) {
				classLogger.warn("A sender of {} was passed in but {} is not enabled, sending as {} instead",
						runTimeFrom, ALLOW_SENDER_OVERRIDE_KEY, this.sender);
			} else {
				validateEmailAddress(runTimeFrom, FROM_PARAM);
				return runTimeFrom;
			}
		}
		if (this.sender == null) {
			throw new IllegalArgumentException("Must define the " + FROM_PARAM + " parameter, or the " + SMTP_SENDER_KEY
					+ " key in the SMSS, to know who this email is from");
		}
		if (this.senderName == null) {
			return this.sender;
		}
		try {
			// gives the recipient a readable from rather than a bare address
			return new InternetAddress(this.sender, this.senderName, StandardCharsets.UTF_8.name()).toString();
		} catch (UnsupportedEncodingException e) {
			classLogger.warn("Could not apply the sender name {}, sending as {} instead", this.senderName, this.sender,
					e);
			return this.sender;
		}
	}

	/**
	 * Prepend the configured prefix to a subject, so every message from this engine
	 * is recognizable in an inbox. A subject that already carries the prefix is
	 * left alone rather than doubled up.
	 *
	 * @param subject the subject for this call
	 * @return the subject to send
	 */
	private String applySubjectPrefix(String subject) {
		if (this.subjectPrefix == null) {
			return subject;
		}
		if (subject.startsWith(this.subjectPrefix)) {
			return subject;
		}
		return this.subjectPrefix + " " + subject;
	}

	/**
	 * Resolve the files to attach to this message. Attachments are read out of the
	 * calling insight's own folder and the resolved path is checked to still be
	 * inside it, so a caller cannot walk out of the insight and mail an arbitrary
	 * file off the server.
	 *
	 * @param parameterValues  the runtime parameters for this call
	 * @param executingInsight the insight this call is running under
	 * @return the file paths to attach, or null when there are none
	 */
	private String[] resolveAttachments(Map<String, Object> parameterValues, Insight executingInsight) {
		String value = getParameterValue(parameterValues, ATTACHMENTS_PARAM, null);
		if (value == null) {
			return null;
		}
		if (!this.allowAttachments) {
			classLogger.warn("Attachments were passed in but {} is not enabled, sending the email without them",
					ALLOW_ATTACHMENTS_KEY);
			return null;
		}
		if (executingInsight == null) {
			throw new IllegalArgumentException(
					"Attachments can only be sent from within an insight that holds the files");
		}

		File insightFolder = new File(executingInsight.getInsightFolder());
		String insightFolderPath = null;
		try {
			insightFolderPath = insightFolder.getCanonicalPath();
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not resolve the insight folder to attach files from", e);
		}

		List<String> attachments = new ArrayList<>();
		for (String fileName : splitList(value)) {
			File attachment = new File(insightFolder, fileName);
			String attachmentPath = null;
			try {
				attachmentPath = attachment.getCanonicalPath();
			} catch (IOException e) {
				throw new IllegalArgumentException("Could not resolve the attachment " + fileName, e);
			}
			if (!attachmentPath.startsWith(insightFolderPath + File.separator)) {
				throw new IllegalArgumentException(
						"The attachment " + fileName + " is not a file in this insight and cannot be sent");
			}
			if (!attachment.exists() || !attachment.isFile()) {
				throw new IllegalArgumentException("Could not find the attachment " + fileName + " in this insight");
			}
			attachments.add(attachmentPath);
		}

		if (attachments.isEmpty()) {
			return null;
		}
		return attachments.toArray(new String[0]);
	}

	/**
	 * Check that every address parses and, when the engine limits who it will send
	 * to, that it is inside one of the allowed domains.
	 *
	 * @param recipients the addresses to check
	 * @param source     the SMSS key or parameter they came from, for the error
	 */
	private void validateRecipients(List<String> recipients, String source) {
		for (String recipient : recipients) {
			validateEmailAddress(recipient, source);
			if (this.allowedRecipientDomains.isEmpty()) {
				continue;
			}
			String domain = recipient.substring(recipient.indexOf('@') + 1).toLowerCase();
			boolean allowed = false;
			for (String allowedDomain : this.allowedRecipientDomains) {
				// a subdomain of an allowed domain is allowed as well, so listing
				// semoss.org also covers mail.semoss.org
				if (domain.equals(allowedDomain) || domain.endsWith("." + allowedDomain)) {
					allowed = true;
					break;
				}
			}
			if (!allowed) {
				throw new IllegalArgumentException("The recipient " + recipient + " passed in " + source
						+ " is not in the domains this function engine is allowed to send to = "
						+ this.allowedRecipientDomains);
			}
		}
	}

	/**
	 * Throw when a value is not a single valid email address. Strict parsing is
	 * used so a malformed address fails here rather than at send time, when part of
	 * the message may already have gone out.
	 *
	 * @param address the address to check
	 * @param source  the SMSS key or parameter it came from, for the error
	 */
	private static void validateEmailAddress(String address, String source) {
		try {
			InternetAddress parsed = new InternetAddress(address, true);
			parsed.validate();
		} catch (AddressException e) {
			throw new IllegalArgumentException(
					"The value '" + address + "' passed in " + source + " is not a valid email address", e);
		}
	}

	/**
	 * Build the sentence that tells a caller which domains this engine will send
	 * to, so a model does not waste a call on a recipient that will be rejected.
	 *
	 * @return the sentence to append, or an empty string when there is no limit
	 */
	private String allowedDomainText() {
		if (this.allowedRecipientDomains.isEmpty()) {
			return "";
		}
		return " Only addresses in these domains can be used: " + String.join(", ", this.allowedRecipientDomains) + ".";
	}

	/**
	 * Split a comma or semicolon separated value into its entries, dropping blanks.
	 *
	 * @param value the raw value, possibly null
	 * @return the entries, empty when there are none
	 */
	private static List<String> splitList(String value) {
		List<String> entries = new ArrayList<>();
		if (value == null) {
			return entries;
		}
		for (String entry : value.split("[,;]")) {
			entry = entry.trim();
			if (!entry.isEmpty()) {
				entries.add(entry);
			}
		}
		return entries;
	}

	/**
	 * Render a list back into the comma separated form a caller would pass in, used
	 * when composing the parameter descriptions.
	 *
	 * @param values the values to join
	 * @return the joined value, or null when the list is empty
	 */
	private static String joinList(List<String> values) {
		if (values == null || values.isEmpty()) {
			return null;
		}
		return String.join(", ", values);
	}

	/**
	 * Read an SMSS flag, treating an unset or blank key as the default rather than
	 * as false, since the UI writes a blank line for every field left empty.
	 *
	 * @param value        the raw SMSS value
	 * @param defaultValue value to use when the key is not set
	 * @return the flag
	 */
	private static boolean parseBoolean(String value, boolean defaultValue) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}

	/**
	 * Convert a recipient list into what {@link EmailUtility} expects, which is
	 * null rather than an empty array when there is nobody to send to.
	 *
	 * @param recipients the addresses for one header
	 * @return the addresses as an array, or null when there are none
	 */
	private static String[] toArray(List<String> recipients) {
		if (recipients == null || recipients.isEmpty()) {
			return null;
		}
		return recipients.toArray(new String[0]);
	}

	/**
	 * Put every raw jakarta.mail key back in the form the mail library answers to.
	 *
	 * <p>
	 * jakarta.mail property names are lower case and it ignores anything else,
	 * while the reactor that writes an SMSS upper cases every key it is given.
	 * Without this, a relay configured through the UI with a property this engine
	 * does not model - the whole point of the {@code mail.} passthrough - would
	 * arrive as {@code MAIL.SMTP.SSL.TRUST} and be silently ignored.
	 *
	 * @param smssProp the engine properties as they were written
	 * @return the same properties with the raw mail keys lower cased
	 */
	private static Properties normalizeRawMailKeys(Properties smssProp) {
		Properties normalized = new Properties();
		for (String key : smssProp.stringPropertyNames()) {
			if (key.toLowerCase().startsWith(RAW_MAIL_PROPERTY_PREFIX)) {
				normalized.setProperty(key.toLowerCase(), smssProp.getProperty(key));
			} else {
				normalized.setProperty(key, smssProp.getProperty(key));
			}
		}
		return normalized;
	}

	/**
	 * Trim a raw SMSS value, treating blank as unset.
	 *
	 * @param value the raw value, possibly null
	 * @return the trimmed value, or null when there is nothing there
	 */
	protected static String trimToNull(String value) {
		if (value == null || (value = value.trim()).isEmpty()) {
			return null;
		}
		return value;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.SMTP.name();
	}

	@Override
	public void close() throws IOException {
		// a mail session holds no connection between sends
	}

}
