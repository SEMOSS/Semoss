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
package prerna.io.connector.google.gmail;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserTrackingUtils;

public class GoogleGmailSendEmailReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailSendEmailReactor.class);

	private static final String EMAIL_SUBJECT = "subject";
	private static final String EMAIL_TO_RECEIVER = "to";
	private static final String EMAIL_MESSAGE = "message";

	public GoogleGmailSendEmailReactor() {
		this.keysToGet = new String[] { EMAIL_SUBJECT, EMAIL_TO_RECEIVER, EMAIL_MESSAGE };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String subject = this.keyValue.get(EMAIL_SUBJECT);
		String to = this.keyValue.get(EMAIL_TO_RECEIVER);
		String body = this.keyValue.get(EMAIL_MESSAGE);
		if (subject == null || subject.trim().isEmpty()) {
			throw new SemossPixelException("Email subject is required.");
		}
		if (to == null || to.trim().isEmpty()) {
			throw new SemossPixelException("Recipient email address is required.");
		}
		if (body == null || body.trim().isEmpty()) {
			throw new SemossPixelException("Email body is required.");
		}

		String from = null;
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			AccessToken googleToken = user == null ? null : user.getAccessToken(AuthProvider.GOOGLE);
			from = googleToken == null ? null : googleToken.getEmail();

			Map<String, Object> retMap = GoogleGmailHelper.sendEmail(accessToken, subject, body, to);
			trackEmail(from, to, subject, body, true);
			return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while sending Gmail email", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to send Gmail email", e);
			// a refused send is recorded too, so the table shows the attempt
			trackEmail(from, to, subject, body, false);
			throw new SemossPixelException("An error occurred sending the email. Error message: " + e.getMessage());
		}
	}

	/**
	 * Record the send in the same table everything sent through SEMOSS is recorded
	 * in, whichever mail account it went out of.
	 *
	 * <p>
	 * Recording must never be the reason a send is reported as having failed, since
	 * by the time this runs the mail has already gone, so a tracking failure is
	 * logged and swallowed.
	 * </p>
	 *
	 * @param from       the account it was sent from
	 * @param to         the recipients, which may be a comma separated list
	 * @param subject    the subject line
	 * @param body       the body
	 * @param successful whether the send succeeded
	 */
	private static void trackEmail(String from, String to, String subject, String body, boolean successful) {
		try {
			UserTrackingUtils.trackEmail(new String[] { to }, null, null, from, subject, body, false, null, successful);
		} catch (RuntimeException e) {
			classLogger.error("Could not record the Gmail email with subject '{}' sent as {}", subject, from, e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Send an email from the logged-in Gmail account.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(EMAIL_MESSAGE)) {
			return "Body content for the email.";
		} else if (key.equals(EMAIL_TO_RECEIVER)) {
			return "Recipient email address (or comma-separated addresses).";
		} else if (key.equals(EMAIL_SUBJECT)) {
			return "Subject line for the email.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
