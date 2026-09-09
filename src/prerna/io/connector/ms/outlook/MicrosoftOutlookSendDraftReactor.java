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
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EmailUtility;
import prerna.util.EmailUtility.EmailMetadata;

/**
 * Sends a draft the signed in user already has, once they have decided it
 * should go out.
 *
 * <p>
 * The other half of {@code MicrosoftOutlookSaveDraft}. The draft can equally be
 * sent from Outlook, which is often the point of saving one, so this exists for
 * the case where the reviewing happens in SEMOSS instead.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Send} for {@code POST /me/messages/{id}/send}</li>
 * <li>{@code Mail.Read} to read the draft back before sending, which is what is
 * recorded as having been sent</li>
 * </ul>
 */
public class MicrosoftOutlookSendDraftReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookSendDraftReactor.class);

	private static final String DRAFT_ID = "draftId";

	public MicrosoftOutlookSendDraftReactor() {
		this.keysToGet = new String[] { DRAFT_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String requestedDraftId = this.keyValue.get(DRAFT_ID);
		if (requestedDraftId == null || requestedDraftId.trim().isEmpty()) {
			throw new SemossPixelException("A " + DRAFT_ID + " is required to send a draft.");
		}
		String draftId = requestedDraftId.trim();

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			String from = MicrosoftLoginUtils.getMicrosoftEmail(user);
			MicrosoftOutlookMailHelper mail = new MicrosoftOutlookMailHelper();

			// read it first, because sending moves it to Sent Items under a new id and
			// there would be nothing left at this one to record
			Map<String, Object> draft = mail.getMessage(accessToken, null, draftId);
			EmailMetadata metadata = new EmailMetadata(
					MicrosoftOutlookMessageMapper.addressArray(draft.get("toRecipients")),
					MicrosoftOutlookMessageMapper.addressArray(draft.get("ccRecipients")),
					MicrosoftOutlookMessageMapper.addressArray(draft.get("bccRecipients")), from,
					draft.get("subject") == null ? null : draft.get("subject").toString(),
					MicrosoftOutlookMessageMapper.bodyOf(draft), false, null);
			EmailUtility.sendEmail(() -> {
				mail.sendDraft(accessToken, null, draftId);
				return null;
			}, metadata);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("sent", true);
			output.put(DRAFT_ID, draftId);
			if (draft != null) {
				MicrosoftOutlookMessageMapper.putIfPresent(output, "to",
						MicrosoftOutlookMessageMapper.addressList(draft.get("toRecipients")));
				MicrosoftOutlookMessageMapper.putIfPresent(output, "cc",
						MicrosoftOutlookMessageMapper.addressList(draft.get("ccRecipients")));
				MicrosoftOutlookMessageMapper.putIfPresent(output, "subject", draft.get("subject"));
			}
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while sending a draft for the signed in user", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to send a draft for the signed in user", e);
			throw new SemossPixelException("An error occurred sending the draft. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Send a draft that is already saved in the signed in user's own Microsoft 365 mailbox.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DRAFT_ID)) {
			return "Id of the draft to send, as returned by MicrosoftOutlookSaveDraft or by reading the drafts folder.";
		}
		return super.getDescriptionForKey(key);
	}
}
