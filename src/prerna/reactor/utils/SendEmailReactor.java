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
package prerna.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.function.mail.engine.SMTPFunctionEngine;
import prerna.om.FileReference;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.export.mustache.MustacheUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SocialPropertiesUtil;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class SendEmailReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SendEmailReactor.class);

	// the engine id a per call smtp connection is opened under. it is never in the
	// catalog, so this only shows up in the logs
	private static final String ONE_TIME_ENGINE_ID = "SEND_EMAIL_ONE_TIME_SMTP";

	private static final String SMTP_HOST = "smtpHost";
	private static final String SMTP_PORT = "smtpPort";
	private static final String SMTP_SECURITY = "smtpSecurity";
	private static final String EMAIL_SUBJECT = "subject";
	private static final String EMAIL_TO_RECEIVER = "to";
	private static final String EMAIL_CC_RECEIVER = "cc";
	private static final String EMAIL_BCC_RECEIVER = "bcc";
	private static final String EMAIL_SENDER = "from";
	private static final String EMAIL_MESSAGE = "message";
	private static final String EMAIL_MESSAGE_ENCODED = "messageEncoded";
	private static final String MESSAGE_HTML = "html";
	private static final String ATTACHMENTS = "attachments";

	public SendEmailReactor() {
		this.keysToGet = new String[] { SMTP_HOST, SMTP_PORT, SMTP_SECURITY, EMAIL_SUBJECT, EMAIL_SENDER, EMAIL_MESSAGE,
				EMAIL_MESSAGE_ENCODED, ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.EMAIL_SESSION.getKey(), MESSAGE_HTML, ReactorKeysEnum.MUSTACHE.getKey(),
				ReactorKeysEnum.MUSTACHE_VARMAP.getKey(), ReactorKeysEnum.USERNAME.getKey(),
				ReactorKeysEnum.PASSWORD.getKey(), EMAIL_TO_RECEIVER, EMAIL_CC_RECEIVER, EMAIL_BCC_RECEIVER,
				ATTACHMENTS };
	}

	@Override
	public NounMetadata execute() {
		// get pixel inputs
		organizeKeys();

		// validate as many inputs first before establishing the email session
		String subject = this.keyValue.get(EMAIL_SUBJECT);
		String sender = this.keyValue.get(EMAIL_SENDER);
		if (sender == null) {
			sender = SocialPropertiesUtil.getInstance().getSmtpSender();
			if (sender == null) {
				throw new IllegalArgumentException("Need to define " + EMAIL_SENDER);
			}
		}
		String message = this.keyValue.get(EMAIL_MESSAGE);
		if (message == null || (message = message.trim()).isEmpty()) {
			String messageFileLocation = null;
			try {
				messageFileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
			} catch (IllegalArgumentException e) {
				// ignore
			}
			if (messageFileLocation != null) {
				File messageFile = new File(messageFileLocation);
				if (messageFile.exists() && messageFile.isFile()) {
					try {
						message = FileUtils.readFileToString(messageFile, "UTF-8");
					} catch (IOException e) {
						classLogger.error("Could not read the message file {}", messageFile, e);
						throw new IllegalArgumentException("Error reading message file. Check logs for details.");
					}
				}
			}
		} else if (Boolean.parseBoolean(this.keyValue.get(EMAIL_MESSAGE_ENCODED) + "")) {
			message = Utility.decodeURIComponent(message);
		}
		boolean isHtml = Boolean.parseBoolean(this.keyValue.get(MESSAGE_HTML) + "");
		// see if using mustache template format that needs modifications
		if (Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.MUSTACHE.getKey()) + "")) {
			Map<String, Object> variables = mustacheVariables();
			try {
				message = MustacheUtility.compile(message, variables);
			} catch (Exception e) {
				throw new IllegalArgumentException(
						"Invalid mustache template or variables. Detailed error message = " + e.getMessage(), e);
			}
			classLogger.error("Generating final html as: " + message);
		}

		String[] to = getEmailRecipients(EMAIL_TO_RECEIVER);
		String[] cc = getEmailRecipients(EMAIL_CC_RECEIVER);
		String[] bcc = getEmailRecipients(EMAIL_BCC_RECEIVER);

		if (to == null && cc == null && bcc == null) {
			throw new IllegalArgumentException(
					"Need to define " + EMAIL_TO_RECEIVER + " or " + EMAIL_CC_RECEIVER + " or " + EMAIL_BCC_RECEIVER);
		}

		SMTPFunctionEngine mailEngine = null;

		String smtpHost = this.keyValue.get(SMTP_HOST);
		String smtpPort = this.keyValue.get(SMTP_PORT);
		if ((smtpHost == null || smtpHost.isEmpty()) && (smtpPort == null || smtpPort.isEmpty())) {
			warnIfEmailSessionPassedIn();
			// the instance wide mail server, which is null when smtp is not enabled
			mailEngine = SocialPropertiesUtil.getInstance().getSmtpEngine();
			if (mailEngine == null) {
				throw new IllegalArgumentException("Need to define an smtp server to utilize this function");
			}
		} else {
			String username = this.keyValue.get(ReactorKeysEnum.USERNAME.getKey());
			String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
			mailEngine = constructOneTimeEngine(smtpHost, smtpPort, username, password);
		}

		// attachments are optional
		String[] attachments = getAttachmentLocations();

		// send email. the mail server is held by the engine, which is the only thing
		// that touches the underlying mail session
		boolean success = mailEngine.sendEmail(to, cc, bcc, sender, subject, message, isHtml, attachments);
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}

	/**
	 * Log when a caller passes in an email session. The mail server is reached
	 * through an SMTP engine rather than a session, so the key is accepted and
	 * ignored, and the instance wide mail server is used instead.
	 */
	private void warnIfEmailSessionPassedIn() {
		GenRowStruct emailSessionGrs = this.store.getGenRowStruct(ReactorKeysEnum.EMAIL_SESSION.getKey());
		if (emailSessionGrs != null && !emailSessionGrs.isEmpty()) {
			classLogger.warn("An {} was passed in but is not used, sending through the instance wide mail server",
					ReactorKeysEnum.EMAIL_SESSION.getKey());
		}
	}

	/**
	 * Open a mail server connection for the passed in inputs, used for a single
	 * send and never put in the catalog.
	 *
	 * The connection is an SMTPFunctionEngine opened against these inputs rather
	 * than jakarta.mail properties assembled here, so a one time send gets the same
	 * TLS handling as any other mail connection.
	 *
	 * @param smtpHost
	 * @param smtpPort
	 * @param username
	 * @param password
	 * @return
	 */
	private SMTPFunctionEngine constructOneTimeEngine(String smtpHost, String smtpPort, String username,
			String password) {
		if (smtpHost == null) {
			throw new IllegalArgumentException("Need to define " + SMTP_HOST);
		}
		if (smtpPort == null) {
			throw new IllegalArgumentException("Need to define " + SMTP_PORT);
		}

		Properties props = new Properties();
		props.put(SMTPFunctionEngine.SMTP_HOST_KEY, smtpHost);
		props.put(SMTPFunctionEngine.SMTP_PORT_KEY, smtpPort);

		// a call that passes credentials is talking to a real relay, so it gets
		// encrypted, on the implicit ssl port if that is the one it asked for. a
		// call that passes none is the plaintext internal relay case this reactor
		// has always allowed, and silently requiring TLS would break it. either way
		// smtpSecurity lets the caller say so outright
		String security = this.keyValue.get(SMTP_SECURITY);
		if (security == null || (security = security.trim()).isEmpty()) {
			if (username == null || password == null) {
				security = SMTPFunctionEngine.NONE_SECURITY;
			} else if ("465".equals(smtpPort.trim())) {
				security = SMTPFunctionEngine.SSL_SECURITY;
			} else {
				security = SMTPFunctionEngine.STARTTLS_SECURITY;
			}
		}
		props.put(SMTPFunctionEngine.SMTP_SECURITY_KEY, security);

		if (username != null && password != null) {
			props.put(SMTPFunctionEngine.SMTP_USERNAME_KEY, username);
			props.put(SMTPFunctionEngine.SMTP_PASSWORD_KEY, password);
		}

		try {
			return SMTPFunctionEngine.openTransientEngine(ONE_TIME_ENGINE_ID, props);
		} catch (Exception e) {
			classLogger.error("Error connecting to the mail server " + smtpHost + ":" + smtpPort, e);
			throw new IllegalArgumentException(
					"Error occurred connecting to the smtp server defined. Detailed error: " + e.getMessage(), e);
		}
	}

	private String[] getEmailRecipients(String recipientKey) {
		GenRowStruct grs = this.store.getGenRowStruct(recipientKey);
		if (grs != null) {
			String[] input = new String[grs.size()];
			for (int i = 0; i < input.length; i++) {
				input[i] = grs.getNoun(i).getValue().toString();
			}

			if (input.length == 0) {
				return null;
			}
			return input;
		}

		return null;
	}

	private String[] getAttachmentLocations() {
		GenRowStruct grs = this.store.getGenRowStruct(ATTACHMENTS);
		if (grs != null) {
			String[] input = new String[grs.size()];
			for (int i = 0; i < input.length; i++) {
				NounMetadata noun = grs.getNoun(i);
				if (noun.getOpType().contains(PixelOperationType.FILE_DOWNLOAD)) {
					input[i] = this.insight.getExportFileLocation((String) noun.getValue());
				} else if (noun.getOpType().contains(PixelOperationType.FILE_REFERENCE)) {
					FileReference fileRef = (FileReference) noun.getValue();
					input[i] = UploadInputUtility.getFilePath(this.insight, fileRef);
				} else {
					input[i] = grs.getNoun(i).getValue().toString();
				}
			}
			return input;
		}
		return null;
	}

	private Map<String, Object> mustacheVariables() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if (grs != null && !grs.isEmpty()) {
			Object obj = grs.get(0);
			if (!(obj instanceof Map)) {
				throw new IllegalArgumentException(ReactorKeysEnum.MUSTACHE_VARMAP.getKey() + " must be a map object");
			}
			return (Map<String, Object>) obj;
		}

		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		if (mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}

		return null;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Sends an email to the given to, cc and bcc recipients and returns whether it was sent. It uses the instance mail server, or a one-time SMTP connection when smtpHost and smtpPort are given, and the message can be text or HTML, read from a file, or filled from a mustache template.")
				.key(ReactorKeysEnum.FILE_PATH,
						"The relative path of a file to read the message from when message is empty.")
				.key(ReactorKeysEnum.MUSTACHE, key -> key.type(Boolean.class))
				.key(ReactorKeysEnum.MUSTACHE_VARMAP, key -> key.type(Map.class)).key(SMTP_HOST, "The smtp host.")
				.key(SMTP_PORT, "The smtp port.")
				.key(SMTP_SECURITY,
						"How the smtp connection is encrypted: starttls, ssl, or none. Defaults to none when no credentials are passed, ssl on port 465, and starttls otherwise.")
				.key(EMAIL_SUBJECT, "The subject of the email.").key(EMAIL_SENDER, "The email sender.")
				.key(EMAIL_MESSAGE, "The message of the email to send.")
				.key(EMAIL_MESSAGE_ENCODED, key -> key.type(Boolean.class).description(
						"Has the message of the email been passed in encoded using <encode></encode> blocks. Default false"))
				.key(MESSAGE_HTML, key -> key.type(Boolean.class).description("Boolean is the message is html"))
				.key(EMAIL_TO_RECEIVER, key -> key.list(String.class).description("The to receipient(s) of the email."))
				.key(EMAIL_CC_RECEIVER, key -> key.list(String.class).description("The cc receipient(s) of the email."))
				.key(EMAIL_BCC_RECEIVER,
						key -> key.list(String.class).description("The bcc receipient(s) of the email."))
				.key(ATTACHMENTS, key -> key.list(String.class).description("The file path of email attachments"));
	}

}
