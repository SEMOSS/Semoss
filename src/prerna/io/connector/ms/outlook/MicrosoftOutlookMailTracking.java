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
package prerna.io.connector.ms.outlook;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.usertracking.UserTrackingUtils;

/**
 * Records mail sent through Graph in the same place mail sent through SMTP is
 * recorded.
 *
 * <p>
 * {@code EmailUtility} tracks every send it makes, so anything going out over
 * SMTP lands in {@code EMAIL_TRACKING} without a caller thinking about it.
 * Graph does not go through {@code EmailUtility} at all - there is no jakarta
 * session and no relay - so the same record has to be written deliberately,
 * which is what this is for. Both write the same row, so the table is a
 * complete account of what left the instance rather than of what left it one
 * particular way.
 * </p>
 *
 * <p>
 * Tracking is off unless the instance turned it on, and that check lives in
 * {@link UserTrackingUtils#trackEmail}, so nothing here has to ask.
 * </p>
 */
public class MicrosoftOutlookMailTracking {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookMailTracking.class);

	private MicrosoftOutlookMailTracking() {

	}

	/**
	 * Record a send.
	 *
	 * <p>
	 * Recording must never be the reason a send is reported as having failed, since
	 * by the time this runs the mail has already gone, so a tracking failure is
	 * logged and swallowed.
	 *
	 * @param to          the recipients, or null
	 * @param cc          the copied recipients, or null
	 * @param bcc         the blind copied recipients, or null
	 * @param from        the address it was sent as
	 * @param subject     the subject line
	 * @param body        the body
	 * @param html        whether the body is html
	 * @param attachments the files attached, or null
	 * @param successful  whether the send succeeded
	 */
	public static void trackSend(String[] to, String[] cc, String[] bcc, String from, String subject, String body,
			boolean html, String[] attachments, boolean successful) {
		try {
			UserTrackingUtils.trackEmail(to, cc, bcc, from, subject, body, html, attachments, successful);
		} catch (RuntimeException e) {
			classLogger.error("Could not record the email with subject '{}' sent as {}", subject, from, e);
		}
	}

	/**
	 * Record a send of a message that Graph already holds, such as a draft that was
	 * read back before being sent.
	 *
	 * <p>
	 * The body is recorded as its readable text rather than as markup, so
	 * {@code IS_HTML} is false whatever the message itself is. Attachments are not
	 * named, since listing them is another call for something already sent.
	 *
	 * @param message    the message as Graph returned it, or null
	 * @param from       the address it was sent as
	 * @param successful whether the send succeeded
	 */
	public static void trackSend(Map<String, Object> message, String from, boolean successful) {
		if (message == null) {
			return;
		}
		trackSend(MicrosoftOutlookMessageMapper.addressArray(message.get("toRecipients")),
				MicrosoftOutlookMessageMapper.addressArray(message.get("ccRecipients")),
				MicrosoftOutlookMessageMapper.addressArray(message.get("bccRecipients")), from,
				message.get("subject") == null ? null : message.get("subject").toString(),
				MicrosoftOutlookMessageMapper.bodyOf(message), false, null, successful);
	}

	/**
	 * The address of the signed in user's Microsoft account, which is the address
	 * anything they send delegated goes out as.
	 *
	 * @param user the signed in user
	 * @return the address, or null when it cannot be read
	 */
	public static String signedInAddress(User user) {
		if (user == null) {
			return null;
		}
		// null when they signed in some other way, which is not an error here: the
		// reactors have already refused the call by the time this is asked
		AccessToken token = user.getAccessToken(AuthProvider.MICROSOFT);
		return token == null ? null : token.getEmail();
	}

}
