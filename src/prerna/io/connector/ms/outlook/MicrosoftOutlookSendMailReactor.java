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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Sends mail as whoever is signed in.
 *
 * <p>
 * The message goes out as the signed in user because the token says who that
 * is, so this cannot be used to send as somebody else, and a copy lands in
 * their own Sent Items where they can see what was sent on their behalf. The
 * mail function engines are the other way round: they hold an app
 * registration's credentials and send as a mailbox named in their SMSS, which
 * is why they carry the guardrails this does not need.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Send} for {@code POST /me/sendMail}</li>
 * </ul>
 *
 * <p>
 * Attachments are named relative to the insight folder and are resolved inside
 * it, so a caller cannot mail an arbitrary file off the server. Graph accepts
 * about 4MB on a single send, counted on the whole encoded request rather than
 * on any one file.
 * </p>
 *
 * <p>
 * For a message somebody should look at before it goes out, save it with
 * {@code MicrosoftOutlookSaveDraft} instead.
 * </p>
 */
public class MicrosoftOutlookSendMailReactor extends AbstractMicrosoftOutlookComposeReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookSendMailReactor.class);

	private static final String SAVE_TO_SENT_ITEMS = "saveToSentItems";

	public MicrosoftOutlookSendMailReactor() {
		this.keysToGet = new String[] { TO, CC, BCC, SUBJECT, MESSAGE, HTML, ATTACHMENTS, SAVE_TO_SENT_ITEMS };
		this.keyRequired = new int[] { 0, 0, 0, 0, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		// keeping the copy is the default because the sent mail is the user's own
		// record of what went out under their name
		String saveToSentItems = this.keyValue.get(SAVE_TO_SENT_ITEMS);
		boolean keepCopy = saveToSentItems == null || Boolean.parseBoolean(saveToSentItems);

		ComposedMail composed = null;
		String from = null;
		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			from = MicrosoftOutlookMailTracking.signedInAddress(user);

			composed = compose(true, "send");
			// a null mailbox addresses /me, so this sends as the signed in user
			new MicrosoftOutlookMailHelper().sendMail(accessToken, null, composed.message, keepCopy);
			track(composed, from, true);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("sent", true);
			composed.describe(output);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while sending mail as the signed in user", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to send mail as the signed in user", e);
			// a message that was assembled and then refused is still worth recording,
			// so the table shows the attempt rather than nothing at all
			track(composed, from, false);
			throw new SemossPixelException("An error occurred sending the email. Error message: " + e.getMessage());
		}
	}

	/**
	 * Record the send alongside everything that goes out over SMTP.
	 *
	 * @param composed   what was assembled, or null when it never got that far
	 * @param from       the address it was sent as
	 * @param successful whether the send succeeded
	 */
	private void track(ComposedMail composed, String from, boolean successful) {
		if (composed == null) {
			return;
		}
		MicrosoftOutlookMailTracking.trackSend(composed.to, composed.cc, composed.bcc, from, composed.subject,
				composed.body, composed.html, composed.attachments, successful);
	}

	@Override
	public String getReactorDescription() {
		return "Send an email as the signed in user from their own Microsoft 365 mailbox.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TO)) {
			return "Recipients of the email. At least one of to, cc or bcc is required.";
		} else if (key.equals(SAVE_TO_SENT_ITEMS)) {
			return "Optional boolean for whether a copy is kept in the sender's Sent Items. Defaults to true.";
		}
		return super.getDescriptionForKey(key);
	}
}
