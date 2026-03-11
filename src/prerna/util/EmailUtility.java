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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.usertracking.UserTrackingUtils;

public class EmailUtility {

	private static final Logger logger = LogManager.getLogger(EmailUtility.class);

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
	 * 
	 * @param emailSession
	 * @param toRecipients
	 * @param ccRecipients
	 * @param bccRecipients
	 * @param from
	 * @param subject
	 * @param emailMessage
	 * @param isHtml
	 * @param attachments
	 * @return
	 */
	public static boolean sendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients,
			String[] bccRecipients, String from, String subject, String emailMessage, boolean isHtml,
			String[] attachments) {

		boolean successful = doSendEmail(emailSession, toRecipients, ccRecipients, bccRecipients, from, subject,
				emailMessage, isHtml, attachments);
		UserTrackingUtils.trackEmail(toRecipients, ccRecipients, bccRecipients, from, subject, emailMessage, isHtml,
				attachments, successful);
		return successful;
	}

	/**
	 * 
	 * @param emailSession
	 * @param toRecipients
	 * @param ccRecipients
	 * @param bccRecipients
	 * @param from
	 * @param subject
	 * @param emailMessage
	 * @param isHtml
	 * @param attachments
	 * @return
	 */
	private static boolean doSendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients,
			String[] bccRecipients, String from, String subject, String emailMessage, boolean isHtml,
			String[] attachments) {
		if ((toRecipients == null || toRecipients.length == 0) && (ccRecipients == null || ccRecipients.length == 0)
				&& (bccRecipients == null || bccRecipients.length == 0)) {
			logger.info("No receipients to send an email to");
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
						logger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error adding attachment");
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
			StringBuilder logMessage = new StringBuilder("Email subject = '" + subject).append("' has been sent: ");
			if (toRecipients != null) {
				logMessage.append("to ").append(Arrays.toString(toRecipients)).append(". ");
			}
			if (ccRecipients != null) {
				logMessage.append("cc ").append(Arrays.toString(ccRecipients)).append(". ");
			}
			if (bccRecipients != null) {
				logMessage.append("bcc ").append(Arrays.toString(bccRecipients)).append(". ");
			}
			logger.info(logMessage.toString());

			return true;
		} catch (SendFailedException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new RuntimeException("Bad SMTP Connection");
		} catch (MessagingException me) {
			logger.error(Constants.STACKTRACE, me);
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
		if (!SocialPropertiesUtil.getInstance().isEmailSessionActive()) {
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

		Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		EmailUtility.sendEmail(emailSession, recipients.toArray(new String[0]), null, null,
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
		if (!SocialPropertiesUtil.getInstance().isEmailSessionActive()) {
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

		Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		EmailUtility.sendEmail(emailSession, recipients.toArray(new String[0]), null, null,
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
		if (!SocialPropertiesUtil.getInstance().isEmailSessionActive()) {
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

		Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		EmailUtility.sendEmail(emailSession, recipients.toArray(new String[0]), null, null,
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
		if (!SocialPropertiesUtil.getInstance().isEmailSessionActive()) {
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

		Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
		String message = EmailUtility.fillEmailComponents(template, emailReplacements);
		EmailUtility.sendEmail(emailSession, recipients.toArray(new String[0]), null, null,
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
				logger.error(Constants.STACKTRACE, e);
			}
		}
		return template;
	}
}
