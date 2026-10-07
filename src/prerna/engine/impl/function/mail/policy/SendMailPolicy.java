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
package prerna.engine.impl.function.mail.policy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import prerna.engine.impl.function.mail.config.MailProperties;
import prerna.engine.impl.function.mail.model.OutboundMail;
import prerna.om.Insight;

/**
 * What a sending engine is willing to send, and on whose behalf.
 *
 * <p>
 * Sending cannot be undone, so this sits between the caller and the mail server
 * and is deliberately strict. Whoever catalogs the engine decides what it may
 * do; whoever calls it can only work within that. The sender is pinned unless
 * an override was turned on, recipients outside the allowed domains are
 * refused, the recipient count is capped, and attachments are refused until
 * they are turned on and then only from the calling insight's own folder.
 *
 * <p>
 * Some of those refuse the call and some quietly do the safe thing instead. The
 * rule is which mistake is worse: a message sent to somebody it should not
 * reach is worth an error, where a sender that could not be honored is worth a
 * warning and sending as the pinned address, because refusing would only mean
 * the mail does not go.
 *
 * <p>
 * Everything is settled here, before any {@link OutboundMail} exists, so the
 * senders underneath never have to ask whether they should send what they were
 * handed - and a message sent over SMTP is checked exactly the same way as one
 * sent over Graph.
 */
public final class SendMailPolicy {

	private static final Logger classLogger = LogManager.getLogger(SendMailPolicy.class);

	private final String sender;
	private final String senderName;
	private final boolean allowSenderOverride;
	private final Set<String> allowedRecipientDomains;
	private final List<String> defaultTo;
	private final List<String> defaultCc;
	private final List<String> defaultBcc;
	private final String subjectPrefix;
	private final boolean html;
	private final int maxRecipients;
	private final boolean allowAttachments;

	private SendMailPolicy(String sender, String senderName, boolean allowSenderOverride,
			Set<String> allowedRecipientDomains, List<String> defaultTo, List<String> defaultCc,
			List<String> defaultBcc, String subjectPrefix, boolean html, int maxRecipients, boolean allowAttachments) {
		this.sender = sender;
		this.senderName = senderName;
		this.allowSenderOverride = allowSenderOverride;
		this.allowedRecipientDomains = Set.copyOf(allowedRecipientDomains);
		this.defaultTo = List.copyOf(defaultTo);
		this.defaultCc = List.copyOf(defaultCc);
		this.defaultBcc = List.copyOf(defaultBcc);
		this.subjectPrefix = subjectPrefix;
		this.html = html;
		this.maxRecipients = maxRecipients;
		this.allowAttachments = allowAttachments;
	}

	/**
	 * Read what this engine is allowed to do out of its SMSS.
	 *
	 * <p>
	 * The configured addresses are validated here rather than at send time, so an
	 * engine with a malformed default recipient fails while it is being cataloged
	 * instead of the first time somebody relies on it.
	 *
	 * @param properties the engine's SMSS properties
	 * @return the policy
	 * @throws IllegalArgumentException when an address in the SMSS is not one, or a
	 *                                  default recipient is outside the domains the
	 *                                  same SMSS allows
	 */
	public static SendMailPolicy from(Properties properties) {
		String sender = MailProperties.trimToNull(properties.getProperty(MailProperties.SMTP_SENDER));
		if (sender != null) {
			validateEmailAddress(sender, MailProperties.SMTP_SENDER);
		}
		String senderName = MailProperties.trimToNull(properties.getProperty(MailProperties.SMTP_SENDER_NAME));
		boolean allowOverride = MailProperties
				.parseBoolean(properties.getProperty(MailProperties.ALLOW_SENDER_OVERRIDE), false);
		boolean allowAttachments = MailProperties.parseBoolean(properties.getProperty(MailProperties.ALLOW_ATTACHMENTS),
				false);
		boolean html = MailProperties.parseBoolean(properties.getProperty(MailProperties.HTML), false);

		Set<String> domains = normalizedDomains(properties.getProperty(MailProperties.ALLOWED_RECIPIENT_DOMAINS));
		List<String> defaultTo = MailProperties.splitList(properties.getProperty(MailProperties.DEFAULT_TO));
		List<String> defaultCc = MailProperties.splitList(properties.getProperty(MailProperties.DEFAULT_CC));
		List<String> defaultBcc = MailProperties.splitList(properties.getProperty(MailProperties.DEFAULT_BCC));
		validateRecipients(defaultTo, MailProperties.DEFAULT_TO, domains);
		validateRecipients(defaultCc, MailProperties.DEFAULT_CC, domains);
		validateRecipients(defaultBcc, MailProperties.DEFAULT_BCC, domains);

		return new SendMailPolicy(sender, senderName, allowOverride, domains, defaultTo, defaultCc, defaultBcc,
				MailProperties.trimToNull(properties.getProperty(MailProperties.SUBJECT_PREFIX)), html,
				Math.max(1, NumberUtils.toInt(properties.getProperty(MailProperties.MAX_RECIPIENTS), 25)),
				allowAttachments);
	}

	/**
	 * Turn one call's parameters into a message that is ready to send.
	 *
	 * <p>
	 * This is where every restriction is applied, so anything that comes back has
	 * already been allowed.
	 *
	 * @param parameters the runtime parameters for this call
	 * @param insight    the insight this call is running under, or null when there
	 *                   is none, in which case no attachment can be read
	 * @return the message to send
	 * @throws IllegalArgumentException when the call asks for something this engine
	 *                                  does not permit, or names nobody to send to
	 */
	public OutboundMail prepare(Map<String, Object> parameters, Insight insight) {
		List<String> to = recipients(parameters, "to", this.defaultTo);
		List<String> cc = recipients(parameters, "cc", this.defaultCc);
		List<String> bcc = recipients(parameters, "bcc", this.defaultBcc);
		int recipientCount = to.size() + cc.size() + bcc.size();
		if (recipientCount == 0) {
			throw new IllegalArgumentException("Must define at least one recipient through to, cc, or bcc");
		}
		if (recipientCount > this.maxRecipients) {
			throw new IllegalArgumentException("This function engine sends to at most " + this.maxRecipients
					+ " recipients per email but " + recipientCount + " were provided");
		}

		String from = resolveSender(stringParameter(parameters, "from", null));
		String subject = applySubjectPrefix(stringParameter(parameters, "subject", ""));
		String body = stringParameter(parameters, "message", "");
		boolean runTimeHtml = booleanParameter(parameters, "html", this.html);
		return new OutboundMail(to, cc, bcc, from, subject, body, runTimeHtml,
				resolveAttachments(stringParameter(parameters, "attachments", null), insight));
	}

	/**
	 * Read one set of recipients, falling back to the ones the SMSS names.
	 *
	 * <p>
	 * The defaults are not checked again here, since they were checked when the
	 * engine opened, and are trusted in a way a caller's are not.
	 *
	 * @param parameters the runtime parameters for this call
	 * @param key        the recipient parameter to read
	 * @param defaults   the addresses to use when the call names none
	 * @return the recipients
	 * @throws IllegalArgumentException when a passed in address is malformed or
	 *                                  outside the allowed domains
	 */
	private List<String> recipients(Map<String, Object> parameters, String key, List<String> defaults) {
		String value = stringParameter(parameters, key, null);
		if (value == null) {
			return new ArrayList<>(defaults);
		}
		List<String> recipients = MailProperties.splitList(value);
		validateRecipients(recipients, key, this.allowedRecipientDomains);
		return recipients;
	}

	/**
	 * Work out who this message is from.
	 *
	 * <p>
	 * A sender the engine will not honor is logged and ignored rather than refused,
	 * because the caller asking to send as somebody else is not a reason to drop
	 * the mail - it is a reason to send it as the address the engine was pinned to.
	 * The display name is only a label in front of that address, so failing to
	 * apply it costs nothing worth stopping for.
	 *
	 * @param requestedSender the address the call asked to send as, or null
	 * @return the address to send as
	 * @throws IllegalArgumentException when neither the call nor the SMSS names a
	 *                                  sender
	 */
	private String resolveSender(String requestedSender) {
		if (requestedSender != null) {
			if (this.sender != null && !this.allowSenderOverride) {
				classLogger.warn("A sender of {} was passed in but {} is not enabled, sending as {} instead",
						requestedSender, MailProperties.ALLOW_SENDER_OVERRIDE, this.sender);
			} else {
				validateEmailAddress(requestedSender, "from");
				return requestedSender;
			}
		}
		if (this.sender == null) {
			throw new IllegalArgumentException("Must define the from parameter, or the " + MailProperties.SMTP_SENDER
					+ " key in the SMSS, to know who this email is from");
		}
		if (this.senderName == null) {
			return this.sender;
		}
		try {
			return new InternetAddress(this.sender, this.senderName, StandardCharsets.UTF_8.name()).toString();
		} catch (IOException e) {
			classLogger.warn("Could not apply the sender name {}, sending as {} instead", this.senderName, this.sender,
					e);
			return this.sender;
		}
	}

	/**
	 * Put the engine's prefix in front of a subject, unless it is already there.
	 *
	 * <p>
	 * The check matters for a reply or a resend, which would otherwise collect the
	 * prefix once per send.
	 *
	 * @param subject the subject the call asked for
	 * @return the subject as it will be sent
	 */
	private String applySubjectPrefix(String subject) {
		if (this.subjectPrefix == null || subject.startsWith(this.subjectPrefix)) {
			return subject;
		}
		return this.subjectPrefix + " " + subject;
	}

	/**
	 * Resolve the files to attach.
	 *
	 * <p>
	 * Attachments are read out of the calling insight's own folder and the resolved
	 * path is checked to still be inside it, so a caller cannot walk out of the
	 * insight and mail an arbitrary file off the server. That check is on the
	 * canonical path rather than the requested one, which is what makes a symlink
	 * or a {@code ..} no different from any other way of naming a file.
	 *
	 * @param value   the attachments the call asked for, or null
	 * @param insight the insight this call is running under, or null
	 * @return the resolved paths, empty when there are none to send
	 * @throws IllegalArgumentException when a named file is not a file of this
	 *                                  insight
	 */
	private List<String> resolveAttachments(String value, Insight insight) {
		if (value == null) {
			return List.of();
		}
		if (!this.allowAttachments) {
			classLogger.warn("Attachments were passed in but {} is not enabled, sending the email without them",
					MailProperties.ALLOW_ATTACHMENTS);
			return List.of();
		}
		if (insight == null) {
			throw new IllegalArgumentException(
					"Attachments can only be sent from within an insight that holds the files");
		}

		try {
			File insightFolder = new File(insight.getInsightFolder()).getCanonicalFile();
			List<String> attachments = new ArrayList<>();
			for (String fileName : MailProperties.splitList(value)) {
				File attachment = new File(insightFolder, fileName).getCanonicalFile();
				if (!attachment.toPath().startsWith(insightFolder.toPath())) {
					throw new IllegalArgumentException(
							"The attachment " + fileName + " is not a file in this insight and cannot be sent");
				}
				if (!attachment.isFile()) {
					throw new IllegalArgumentException(
							"Could not find the attachment " + fileName + " in this insight");
				}
				attachments.add(attachment.getPath());
			}
			return attachments;
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not resolve the insight folder or an attachment", e);
		}
	}

	/**
	 * Check that a value is an address a mail server would accept.
	 *
	 * @param address the value to check
	 * @param source  where it came from, named in the error so the reader knows
	 *                which key or parameter to correct
	 * @throws IllegalArgumentException when it is not an address
	 */
	public static void validateEmailAddress(String address, String source) {
		try {
			InternetAddress parsed = new InternetAddress(address, true);
			parsed.validate();
		} catch (AddressException e) {
			throw new IllegalArgumentException(
					"The value '" + address + "' passed in " + source + " is not a valid email address", e);
		}
	}

	/**
	 * Check that every recipient is an address, and one this engine may write to.
	 *
	 * <p>
	 * A subdomain of an allowed domain counts, so allowing {@code example.com} also
	 * allows {@code mail.example.com}.
	 *
	 * @param recipients     the addresses to check
	 * @param source         where they came from, named in the error
	 * @param allowedDomains the domains that are permitted, or empty to permit any
	 * @throws IllegalArgumentException on the first address that is malformed or
	 *                                  outside those domains
	 */
	private static void validateRecipients(List<String> recipients, String source, Set<String> allowedDomains) {
		for (String recipient : recipients) {
			validateEmailAddress(recipient, source);
			if (allowedDomains.isEmpty()) {
				continue;
			}
			String address;
			try {
				address = new InternetAddress(recipient, true).getAddress();
			} catch (AddressException e) {
				throw new IllegalArgumentException("Could not read recipient " + recipient, e);
			}
			String domain = address.substring(address.lastIndexOf('@') + 1).toLowerCase();
			if (allowedDomains.stream().noneMatch(value -> domain.equals(value) || domain.endsWith("." + value))) {
				throw new IllegalArgumentException("The recipient " + recipient + " passed in " + source
						+ " is not in the domains this function engine is allowed to send to = " + allowedDomains);
			}
		}
	}

	/**
	 * Read a domain allowlist into the form it is compared in.
	 *
	 * @param value the configured domains, which may carry a leading {@code @}
	 * @return the domains, lower case and without it
	 */
	private static Set<String> normalizedDomains(String value) {
		Set<String> domains = new LinkedHashSet<>();
		for (String domain : MailProperties.splitList(value)) {
			domains.add(domain.toLowerCase().replaceFirst("^@", ""));
		}
		return domains;
	}

	/**
	 * @param parameters   the runtime parameters for this call
	 * @param key          the parameter to read
	 * @param defaultValue what it is when the call left it out or passed a blank
	 * @return the value
	 */
	private static String stringParameter(Map<String, Object> parameters, String key, String defaultValue) {
		Object value = parameters == null ? null : parameters.get(key);
		String text = value == null ? null : MailProperties.trimToNull(value.toString());
		return text == null ? defaultValue : text;
	}

	/**
	 * Read a parameter that says yes or no.
	 *
	 * <p>
	 * The several spellings are accepted because this reaches an engine from a
	 * pixel, a model calling a function, or a person, and none of them agree on
	 * one. Anything unrecognized falls back to the engine's own setting rather than
	 * being read as false, so a value nobody meant does not silently turn a feature
	 * off.
	 *
	 * @param parameters   the runtime parameters for this call
	 * @param key          the parameter to read
	 * @param defaultValue what it is when the call left it out
	 * @return the value
	 */
	private static boolean booleanParameter(Map<String, Object> parameters, String key, boolean defaultValue) {
		Object value = parameters == null ? null : parameters.get(key);
		if (value instanceof Boolean flag) {
			return flag.booleanValue();
		}
		String text = value == null ? null : value.toString().trim().toLowerCase();
		if ("true".equals(text) || "yes".equals(text) || "y".equals(text) || "1".equals(text)) {
			return true;
		}
		if ("false".equals(text) || "no".equals(text) || "n".equals(text) || "0".equals(text)) {
			return false;
		}
		return defaultValue;
	}

	/**
	 * @return the address this engine sends as, or null when every call has to name
	 *         one
	 */
	public String sender() {
		return this.sender;
	}

	/**
	 * @return the display name shown in front of that address, or null
	 */
	public String senderName() {
		return this.senderName;
	}

	/**
	 * @return whether a call may send as an address other than the pinned one
	 */
	public boolean allowSenderOverride() {
		return this.allowSenderOverride;
	}

	/**
	 * @return the domains this engine will send to, or empty to allow any recipient
	 */
	public Set<String> allowedRecipientDomains() {
		return this.allowedRecipientDomains;
	}

	/**
	 * @return the recipients used when a call names none
	 */
	public List<String> defaultTo() {
		return this.defaultTo;
	}

	/**
	 * @return the copied recipients used when a call names none
	 */
	public List<String> defaultCc() {
		return this.defaultCc;
	}

	/**
	 * @return the blind copied recipients used when a call names none
	 */
	public List<String> defaultBcc() {
		return this.defaultBcc;
	}

	/**
	 * @return whether a body is html when a call does not say
	 */
	public boolean html() {
		return this.html;
	}

	/**
	 * @return whether this engine will attach files at all
	 */
	public boolean allowAttachments() {
		return this.allowAttachments;
	}
}
