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

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			Map<String, Object> retMap = GoogleGmailHelper.sendEmail(accessToken, subject, body, to);
			return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while sending Gmail email", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to send Gmail email", e);
			throw new SemossPixelException("An error occurred sending the email. Error message: " + e.getMessage());
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
