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
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.AuthenticationFailedException;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.SendFailedException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.impl.function.mail.engine.SMTPFunctionEngine;
import prerna.usertracking.UserTrackingUtils;

public class EmailUtility {

	private static final Logger classLogger = LogManager.getLogger(EmailUtility.class);

	private static final String PROJECT_ACCESS_APPROVAL_TEMPLATE = "projectAccessApproval.html";
	private static final String ENGINE_ACCESS_APPROVAL_TEMPLATE = "engineAccessApproval.html";
	private static final String SMSS_UPDATE_TEMPLATE = "smssFileUpdate.html";
	private static final String PROJECT_ACCESS_REQUEST_TEMPLATE = "requestProject.html";
	private static final String ENGINE_ACCESS_REQUEST_TEMPLATE = "requestEngine.html";
	private static final String INSIGHT_ACCESS_REQUEST_TEMPLATE = "requestInsight.html";

	private static final String PROJECT_ACCESS_APPROVAL_SUBJECT = "Project Access Request";
	private static final String ENGINE_ACCESS_APPROVAL_SUBJECT = "Engine Access Request";
	private static final String PROJECT_SMSS_UPDATE_SUBJECT = "Project SMSS File Updated";
	private static final String ENGINE_SMSS_UPDATE_SUBJECT = "Engine SMSS File Updated";
	private static final String PROJECT_ACCESS_REQUEST_SUBJECT = "Project Access Request";
	private static final String ENGINE_ACCESS_REQUEST_SUBJECT = "Database Access Request";
	private static final String INSIGHT_ACCESS_REQUEST_SUBJECT = "Insight Access Request";

	private static final String PROJECT_NAME_REPLACEMENT = "$projectName$";
	private static final String ENGINE_NAME_REPLACEMENT = "$engineName$";
	private static final String INSIGHT_NAME_REPLACEMENT = "$insightName$";
	private static final String ENGINE_TYPE_REPLACEMENT = "$engineType$";
	private static final String PERMISSION_REPLACEMENT = "$permission$";
	private static final String USER_NAME_REPLACEMENT = "$userName$";
	private static final String USER_EMAIL_REPLACEMENT = "$userEmail$";
	private static final String REQUEST_REASON_REPLACEMENT = "$requestReason$";
	private static final String ACTION_CREATEDBY_USERNAME_REPLACEMENT = "$actionCreatedBy$";
	private static final String ENGINE_BLOCK_REPLACEMENT = "$engineBlock$";
	private static final String PROJECT_BLOCK_REPLACEMENT = "$projectBlock$";

	private static final String DEFAULT_REQUEST_REASON = "I'd like access, please.";

	public enum RESOURCE_TYPE {
		PROJECT, ENGINE
	}

	/**
	 * What is recorded about one attempt to send.
	 *
	 * <p>
	 * Held apart from the delivery itself because the two come from different
	 * places. A provider that posts a message to an API has this in front of it,
	 * where jakarta.mail builds its own from the same fields, and the record is
	 * what lets both be tracked identically.
	 *
	 * <p>
	 * The arrays are copied on the way in, so what is recorded is what was sent
	 * even if the caller reuses its own array afterwards.
	 *
	 * @param toRecipients  the recipients, or null
	 * @param ccRecipients  the copied recipients, or null
	 * @param bccRecipients the blind copied recipients, or null
	 * @param from          the address it is sent as
	 * @param subject       the subject line
	 * @param emailMessage  the body
	 * @param html          whether the body is html
	 * @param attachments   the files attached, or null
	 */
	public record EmailMetadata(String[] toRecipients, String[] ccRecipients, String[] bccRecipients, String from,
			String subject, String emailMessage, boolean html, String[] attachments) {

		public EmailMetadata {
			toRecipients = copy(toRecipients);
			ccRecipients = copy(ccRecipients);
			bccRecipients = copy(bccRecipients);
			attachments = copy(attachments);
		}

		private static String[] copy(String[] values) {
			return values == null ? null : values.clone();
		}
	}

	/**
	 * Send one email over SMTP, and record the attempt.
	 *
	 * <p>
	 * The tracking is in a finally block rather than after the send, so a delivery
	 * that throws is still recorded as an attempt that failed.
	 *
	 * @param emailSession  the mail session to send through
	 * @param toRecipients  the recipients, or null
	 * @param ccRecipients  the copied recipients, or null
	 * @param bccRecipients the blind copied recipients, or null
	 * @param from          the address to send as
	 * @param subject       the subject line
	 * @param emailMessage  the body
	 * @param isHtml        whether the body is html rather than plain text
	 * @param attachments   the files to attach, or null
	 * @return whether the mail server took the message
	 */
	public static boolean sendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients,
			String[] bccRecipients, String from, String subject, String emailMessage, boolean isHtml,
			String[] attachments) {
		EmailMetadata metadata = new EmailMetadata(toRecipients, ccRecipients, bccRecipients, from, subject,
				emailMessage, isHtml, attachments);
		boolean successful = false;
		try {
			successful = doSendEmail(emailSession, toRecipients, ccRecipients, bccRecipients, from, subject,
					emailMessage, isHtml, attachments);
			return successful;
		} finally {
			trackEmail(metadata, successful);
		}
	}

	/**
	 * Send one email some other way, and record it here all the same.
	 *
	 * <p>
	 * Mail leaves this instance by more routes than SMTP - Microsoft Graph, Gmail,
	 * a draft somebody sends from their own mailbox - and a tracking table that
	 * held only the relayed ones would read as a complete account while being
	 * nothing of the kind. Every route passes its delivery through here instead, so
	 * there is one place a row is written and no way to add a route that forgets
	 * to.
	 *
	 * <p>
	 * Returning is what counts as sent, and throwing is what counts as failed,
	 * which is what an API client already does. The result is handed back
	 * untouched, so a provider that answers with something worth having is not made
	 * to reduce it to a boolean first.
	 *
	 * @param delivery the call that actually sends
	 * @param metadata what to record about it
	 * @param <T>      whatever the provider answers with
	 * @return that answer, unchanged
	 * @throws Exception whatever the provider threw, after the attempt is recorded
	 */
	public static <T> T sendEmail(Callable<T> delivery, EmailMetadata metadata) throws Exception {
		Objects.requireNonNull(delivery, "The email delivery is required");
		Objects.requireNonNull(metadata, "The email metadata is required");
		boolean successful = false;
		try {
			T result = delivery.call();
			successful = true;
			return result;
		} finally {
			trackEmail(metadata, successful);
		}
	}

	/**
	 * Write the row, and never let doing so change what the caller sees.
	 *
	 * <p>
	 * By the time this runs the mail has already gone, so a tracking failure that
	 * propagated would report a successful send as a failed one. It is logged
	 * instead. Whether tracking is on at all is decided further down, in
	 * {@link UserTrackingUtils#trackEmail}, so nothing here has to ask.
	 *
	 * @param metadata   what was sent
	 * @param successful whether it went
	 */
	private static void trackEmail(EmailMetadata metadata, boolean successful) {
		try {
			UserTrackingUtils.trackEmail(metadata.toRecipients(), metadata.ccRecipients(), metadata.bccRecipients(),
					metadata.from(), metadata.subject(), metadata.emailMessage(), metadata.html(),
					metadata.attachments(), successful);
		} catch (RuntimeException e) {
			// Delivery has already completed. Tracking must not change the reported result.
			classLogger.error("Could not track the email with subject '{}' sent as {}", metadata.subject(),
					metadata.from(), e);
		}
	}

	/**
	 * Build the MIME message and hand it to the mail server.
	 *
	 * <p>
	 * The delivery itself, with no tracking, so the caller can record the attempt
	 * whichever way it turns out. A message with nobody to send it to is refused
	 * before a connection is opened.
	 *
	 * @param emailSession  the mail session to send through
	 * @param toRecipients  the recipients, or null
	 * @param ccRecipients  the copied recipients, or null
	 * @param bccRecipients the blind copied recipients, or null
	 * @param from          the address to send as
	 * @param subject       the subject line
	 * @param emailMessage  the body
	 * @param isHtml        whether the body is html rather than plain text
	 * @param attachments   the files to attach, or null
	 * @return whether the mail server took the message
	 */
	private static boolean doSendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients,
			String[] bccRecipients, String from, String subject, String emailMessage, boolean isHtml,
			String[] attachments) {
		if ((toRecipients == null || toRecipients.length == 0) && (ccRecipients == null || ccRecipients.length == 0)
				&& (bccRecipients == null || bccRecipients.length == 0)) {
			classLogger.info("No recipients to send the email with subject '{}' to", subject);
			return false;
		}

		try {
			// Create an email message we will add multiple parts to this
			Message email = new MimeMessage(emailSession);
			// add from
			email.setFrom(new InternetAddress(from));
			// add email recipients
			if (toRecipients != null) {
				for (String recipient : toRecipients) {
					email.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
				}
			}
			if (ccRecipients != null) {
				for (String recipient : ccRecipients) {
					email.addRecipients(Message.RecipientType.CC, InternetAddress.parse(recipient));
				}
			}
			if (bccRecipients != null) {
				for (String recipient : bccRecipients) {
					email.addRecipients(Message.RecipientType.BCC, InternetAddress.parse(recipient));
				}
			}
			// add email subject
			email.setSubject(subject);
			// Create a multipart message
			Multipart multipart = new MimeMultipart();
			// Create the message part
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			if (emailMessage == null) {
				emailMessage = "";
			}
			if (isHtml) {
				// add email message
				messageBodyPart.setContent(emailMessage, "text/html");
			} else {
				// add email message
				messageBodyPart.setText(emailMessage);
			}
			// set email message
			multipart.addBodyPart(messageBodyPart);

			// add attachments
			if (attachments != null) {
				for (String filePath : attachments) {
					MimeBodyPart attachmentBodyPart = new MimeBodyPart();
					try {
						attachmentBodyPart.attachFile(new File(filePath));
					} catch (IOException e) {
						classLogger.error("Error attaching the file {} to the email with subject '{}'", filePath,
								subject, e);
						throw new IllegalArgumentException(
								"Error adding the attachment " + new File(filePath).getName(), e);
					}
					attachmentBodyPart.setFileName(new File(filePath).getName());
					multipart.addBodyPart(attachmentBodyPart);
				}
			}
			// Send the complete email parts
			email.setContent(multipart);
			// Send email
			Transport.send(email);
			// Log email
			classLogger.info("Email with subject '{}' has been sent. to = {}, cc = {}, bcc = {}", subject,
					Arrays.toString(toRecipients), Arrays.toString(ccRecipients), Arrays.toString(bccRecipients));

			return true;
		} catch (AuthenticationFailedException e) {
			classLogger.error("The mail server {} refused the credentials for {}",
					emailSession.getProperty("mail.smtp.host"), from, e);
		} catch (SendFailedException e) {
			classLogger.error("The mail server would not accept the email with subject '{}' for {}", subject,
					Arrays.toString(toRecipients), e);
			throw new RuntimeException("The mail server would not accept the email. Detailed error: " + e.getMessage(),
					e);
		} catch (MessagingException e) {
			classLogger.error("Error sending the email with subject '{}' from {}", subject, from, e);
		}

		return false;
	}

	/**
	 * Replace dynamic components in the message
	 * 
	 * @param emailTemplate
	 * @param customReplacements
	 * @return
	 */
	public static String fillEmailComponents(String emailTemplate, Map<String, String> customReplacements) {
		if (customReplacements != null && !customReplacements.isEmpty()) {
			for (Map.Entry<String, String> entry : customReplacements.entrySet()) {
				String key = entry.getKey();
				String replacementValue = entry.getValue();
				emailTemplate = emailTemplate.replace(key, replacementValue);
			}
		}

		return emailTemplate;
	}

	/**
	 * Creates access request email and sends notification for project/engine
	 * resources.
	 * 
	 * @param requestingUser
	 * @param resourceId
	 * @param requestedPermission
	 * @param requestComment
	 * @param accessRequestType
	 */
	public static void sendAccessRequestEmailNotification(User requestingUser, String resourceId,
			String requestedPermission, String requestComment, RESOURCE_TYPE accessRequestType) {
		SMTPFunctionEngine mailEngine = SocialPropertiesUtil.getInstance().getSmtpEngine();
		if (mailEngine == null) {
			return;
		}

		if (resourceId == null || resourceId.isEmpty()) {
			return;
		}

		final String templateName;
		final String subject;
		switch (accessRequestType) {
		case PROJECT:
			templateName = PROJECT_ACCESS_REQUEST_TEMPLATE;
			subject = PROJECT_ACCESS_REQUEST_SUBJECT;
			break;
		case ENGINE:
			templateName = ENGINE_ACCESS_REQUEST_TEMPLATE;
			subject = ENGINE_ACCESS_REQUEST_SUBJECT;
			break;
		default:
			return;
		}

		String template = getTemplateString(templateName);
		if (template == null || template.isEmpty()) {
			return;
		}

		AccessToken token = requestingUser.getAccessToken(requestingUser.getPrimaryLogin());
		String userName = token.getName() != null ? token.getName() : "";
		String userEmail = token.getEmail() != null ? token.getEmail() : "";

		String permission = requestedPermission;
		if (permission != null && permission.length() == 1) {
			permission = AccessPermissionEnum.getPermissionValueById(permission);
		}
		if (requestComment == null || requestComment.isEmpty()) {
			requestComment = DEFAULT_REQUEST_REASON;
		}

		Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
		emailReplacements.put(PERMISSION_REPLACEMENT, permission);
		emailReplacements.put(USER_NAME_REPLACEMENT, userName);
		emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
		emailReplacements.put(REQUEST_REASON_REPLACEMENT, requestComment);

		List<String> recipients;
		if (accessRequestType == RESOURCE_TYPE.PROJECT) {
			recipients = SecurityProjectUtils.getProjectOwners(resourceId);
			if (recipients == null || recipients.isEmpty()) {
				return;
			}
			String projectName = SecurityProjectUtils.getProjectAliasForId(resourceId);
			emailReplacements.put(PROJECT_NAME_REPLACEMENT, projectName);
		} else {
			recipients = SecurityEngineUtils.getEngineOwners(resourceId);
			if (recipients == null || recipients.isEmpty()) {
				return;
			}
			String engineName = SecurityEngineUtils.getEngineAliasForId(resourceId);
			emailReplacements.put(ENGINE_NAME_REPLACEMENT, engineName);
		}

		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		mailEngine.sendEmail(recipients.toArray(new String[0]), null, null,
				SocialPropertiesUtil.getInstance().getSmtpSender(), subject, message, true, null);
	}

	/**
	 * Creates insight access request email and sends notification.
	 * 
	 * @param requestingUser
	 * @param projectId
	 * @param insightId
	 * @param requestedPermission
	 * @param requestComment
	 */
	public static void sendInsightAccessRequestEmailNotification(User requestingUser, String projectId,
			String insightId, String requestedPermission, String requestComment) {
		SMTPFunctionEngine mailEngine = SocialPropertiesUtil.getInstance().getSmtpEngine();
		if (mailEngine == null) {
			return;
		}

		if (projectId == null || projectId.isEmpty() || insightId == null || insightId.isEmpty()) {
			return;
		}

		String template = getTemplateString(INSIGHT_ACCESS_REQUEST_TEMPLATE);
		if (template == null || template.isEmpty()) {
			return;
		}

		AccessToken token = requestingUser.getAccessToken(requestingUser.getPrimaryLogin());
		String userName = token.getName() != null ? token.getName() : "";
		String userEmail = token.getEmail() != null ? token.getEmail() : "";

		String permission = requestedPermission;
		if (permission != null && permission.length() == 1) {
			permission = AccessPermissionEnum.getPermissionValueById(permission);
		}
		if (requestComment == null || requestComment.isEmpty()) {
			requestComment = DEFAULT_REQUEST_REASON;
		}

		List<String> recipients = SecurityInsightUtils.getInsightOwners(projectId, insightId);
		if (recipients == null || recipients.isEmpty()) {
			return;
		}

		String insightName = SecurityInsightUtils.getInsightAliasForId(projectId, insightId);
		Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
		emailReplacements.put(INSIGHT_NAME_REPLACEMENT, insightName);
		emailReplacements.put(PERMISSION_REPLACEMENT, permission);
		emailReplacements.put(USER_NAME_REPLACEMENT, userName);
		emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
		emailReplacements.put(REQUEST_REASON_REPLACEMENT, requestComment);

		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		mailEngine.sendEmail(recipients.toArray(new String[0]), null, null,
				SocialPropertiesUtil.getInstance().getSmtpSender(), INSIGHT_ACCESS_REQUEST_SUBJECT, message, true,
				null);
	}

	/**
	 * Creates access request approval email and sends notification for
	 * project/engine resources.
	 * 
	 * @param currentUser
	 * @param affectedUserId
	 * @param engineId
	 * @param affectedUserPermission
	 * @param accessRequestType
	 */
	public static void sendAccessRequestApprovalEmailNotification(User currentUser, String affectedUserId,
			String engineId, String affectedUserPermission, RESOURCE_TYPE accessRequestType) {
		SMTPFunctionEngine mailEngine = SocialPropertiesUtil.getInstance().getSmtpEngine();
		if (mailEngine == null) {
			return;
		}

		final String templateName;
		final String subject;
		switch (accessRequestType) {
		case PROJECT:
			templateName = PROJECT_ACCESS_APPROVAL_TEMPLATE;
			subject = PROJECT_ACCESS_APPROVAL_SUBJECT;
			break;
		case ENGINE:
			templateName = ENGINE_ACCESS_APPROVAL_TEMPLATE;
			subject = ENGINE_ACCESS_APPROVAL_SUBJECT;
			break;
		default:
			return;
		}

		String template = getTemplateString(templateName);
		if (template == null || template.isEmpty()) {
			return;
		}

		List<Map<String, Object>> userInfo = SecurityUserUtils.getUserNameEmailByUserId(affectedUserId);
		String userName = (String) userInfo.get(0).get("userName");
		String userEmail = (String) userInfo.get(0).get("userEmail");
		String createdBy = currentUser.getAccessToken(currentUser.getLogins().get(0)).getName();

		Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
		emailReplacements.put(PERMISSION_REPLACEMENT, affectedUserPermission);
		emailReplacements.put(USER_NAME_REPLACEMENT, userName);
		emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
		emailReplacements.put(ACTION_CREATEDBY_USERNAME_REPLACEMENT, createdBy);

		List<String> recipients;
		if (accessRequestType == RESOURCE_TYPE.PROJECT) {
			recipients = SecurityProjectUtils.getProjectOwners(engineId);
			if (recipients == null || recipients.isEmpty()) {
				return;
			}
			String projectName = SecurityProjectUtils.getProjectAliasForId(engineId);
			emailReplacements.put(PROJECT_NAME_REPLACEMENT, projectName);
		} else {
			recipients = SecurityEngineUtils.getEngineOwners(engineId);
			if (recipients == null || recipients.isEmpty()) {
				return;
			}
			String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
			String engineType = String.valueOf(SecurityEngineUtils.getEngineType(engineId)).toLowerCase();
			emailReplacements.put(ENGINE_NAME_REPLACEMENT, engineName);
			emailReplacements.put(ENGINE_TYPE_REPLACEMENT, engineType);
		}

		if (!recipients.contains(userEmail)) {
			recipients.add(userEmail);
		}

		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		mailEngine.sendEmail(recipients.toArray(new String[0]), null, null,
				SocialPropertiesUtil.getInstance().getSmtpSender(), subject, message, true, null);
	}

	/**
	 * Creates SMSS update email and sends notification for project/engine
	 * resources.
	 * 
	 * @param currentUser
	 * @param engineId
	 * @param accessRequestType
	 */
	public static void sendSmssUpdateEmailNotification(User currentUser, String engineId,
			RESOURCE_TYPE accessRequestType) {
		SMTPFunctionEngine mailEngine = SocialPropertiesUtil.getInstance().getSmtpEngine();
		if (mailEngine == null) {
			return;
		}

		if (engineId == null || engineId.isEmpty()) {
			return;
		}

		final String subject;
		switch (accessRequestType) {
		case PROJECT:
			subject = PROJECT_SMSS_UPDATE_SUBJECT;
			break;
		case ENGINE:
			subject = ENGINE_SMSS_UPDATE_SUBJECT;
			break;
		default:
			return;
		}

		String template = getTemplateString(SMSS_UPDATE_TEMPLATE);
		if (template == null || template.isEmpty()) {
			return;
		}

		String createdBy = currentUser.getAccessToken(currentUser.getLogins().get(0)).getName();
		Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
		emailReplacements.put(ACTION_CREATEDBY_USERNAME_REPLACEMENT, createdBy);

		List<String> recipients;
		if (accessRequestType == RESOURCE_TYPE.PROJECT) {
			recipients = SecurityProjectUtils.getProjectOwners(engineId);
			if (recipients == null || recipients.isEmpty()) {
				return;
			}
			String projectBlock = "The SMSS file of project <strong>" + engineId
					+ "</strong> has been updated by <strong>" + createdBy + "</strong>.";
			emailReplacements.put(PROJECT_BLOCK_REPLACEMENT, projectBlock);
			emailReplacements.put(ENGINE_BLOCK_REPLACEMENT, "");
		} else {
			recipients = SecurityEngineUtils.getEngineOwners(engineId);
			if (recipients == null || recipients.isEmpty()) {
				return;
			}
			String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
			String engineType = String.valueOf(SecurityEngineUtils.getEngineType(engineId)).toLowerCase();
			String engineBlock = "The SMSS file of the <strong>" + engineType + "</strong> engine <strong>" + engineName
					+ "</strong> has been updated by <strong>" + createdBy + "</strong>.";
			emailReplacements.put(ENGINE_BLOCK_REPLACEMENT, engineBlock);
			emailReplacements.put(PROJECT_BLOCK_REPLACEMENT, "");
		}

		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		mailEngine.sendEmail(recipients.toArray(new String[0]), null, null,
				SocialPropertiesUtil.getInstance().getSmtpSender(), subject, message, true, null);
	}

	/**
	 * Returns template path
	 * 
	 * @param emailTemplate
	 * @return
	 */
	private static String getTemplateString(String templateName) {
		String template = null;
		String templatePath = Utility.getDIHelperProperty(Constants.EMAIL_TEMPLATES);
		if (templatePath.endsWith("\\") || templatePath.endsWith("/")) {
			templatePath += templateName;
		} else {
			templatePath += "/" + templateName;
		}
		File templateFile = new File(templatePath);
		if (templateFile.exists() && templateFile.isFile()) {
			try {
				template = FileUtils.readFileToString(templateFile, "UTF-8");
			} catch (IOException e) {
				classLogger.error("Error reading the email template {}", templatePath, e);
			}
		}
		return template;
	}
}
