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
package prerna.auth.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.PasswordRequirements;
import prerna.engine.impl.function.mail.engine.SMTPFunctionEngine;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;

public final class UserRegistrationEmailService {

	private static final Logger classLogger = LogManager.getLogger(UserRegistrationEmailService.class);

	private static volatile UserRegistrationEmailService instance;

	private String emailTemplatesFolder = "";

	private final String EMAIL_TEMPLATES_FOLDER = "emailTemplates";
	private final String REPLACE_LINK = "{{{REPLACE_LINK}}}";

	public static UserRegistrationEmailService getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
			synchronized (PasswordRequirements.class) {
				if (instance != null) {
					return instance;
				}

				instance = new UserRegistrationEmailService();
				String baseFolder = Utility.getBaseFolder();
				if (baseFolder.endsWith("/") || baseFolder.endsWith("\\")) {
					instance.emailTemplatesFolder = baseFolder + instance.EMAIL_TEMPLATES_FOLDER + "/";
				} else {
					instance.emailTemplatesFolder = baseFolder + "/" + instance.EMAIL_TEMPLATES_FOLDER + "/";
				}
			}
		}

		return instance;
	}

	public boolean sendPasswordResetRequestEmail(String recipient, String customUrl, String customEmailSubject) {
		SocialPropertiesUtil socialProps = SocialPropertiesUtil.getInstance();
		SMTPFunctionEngine mailEngine = socialProps.getSmtpEngine();
		if (mailEngine == null) {
			classLogger.error("Cannot send the password reset request email, no mail server is configured");
			return false;
		}
		String sender = socialProps.getSmtpSender();
		String subject = customEmailSubject;
		if (subject == null || (subject = subject.trim()).isEmpty()) {
			subject = "SEMOSS Reset Password : Request";
		}

		boolean isHtml = true;
		String[] ccRecipients = null;
		String[] bccRecipients = null;
		String[] attachments = null;

		String[] recipients = new String[] { recipient };

		// construct the message
		String message;
		String templatePath = this.emailTemplatesFolder + "passResetRequest.html";
		try {
			message = new String(Files.readAllBytes(Paths.get(templatePath)));
			message = message.replace(this.REPLACE_LINK, customUrl);
		} catch (IOException e) {
			classLogger.error("Failed to load password reset request email template from path='{}'.", templatePath, e);
			return false;
		}

		// send email
		boolean success = mailEngine.sendEmail(recipients, ccRecipients, bccRecipients, sender, subject, message,
				isHtml, attachments);
		return success;
	}

	public boolean sendPasswordResetSuccessEmail(String recipient, String customEmailSubject) {
		SocialPropertiesUtil socialProps = SocialPropertiesUtil.getInstance();
		SMTPFunctionEngine mailEngine = socialProps.getSmtpEngine();
		if (mailEngine == null) {
			classLogger.error("Cannot send the password reset success email, no mail server is configured");
			return false;
		}
		String sender = socialProps.getSmtpSender();
		String subject = customEmailSubject;
		if (subject == null || (subject = subject.trim()).isEmpty()) {
			subject = "SEMOSS Reset Password : Success";
		}

		boolean isHtml = true;
		String[] ccRecipients = null;
		String[] bccRecipients = null;
		String[] attachments = null;

		String[] recipients = new String[] { recipient };

		// construct the message
		String message;
		String templatePath = this.emailTemplatesFolder + "passResetSuccess.html";
		try {
			message = new String(Files.readAllBytes(Paths.get(templatePath)));
		} catch (IOException e) {
			classLogger.error("Failed to load password reset success email template from path='{}'.", templatePath, e);
			return false;
		}

		// send email
		boolean success = mailEngine.sendEmail(recipients, ccRecipients, bccRecipients, sender, subject, message,
				isHtml, attachments);
		return success;
	}

}
