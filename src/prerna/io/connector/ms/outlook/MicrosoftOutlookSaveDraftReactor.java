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
 * Writes a message into the signed in user's Drafts folder without sending it.
 *
 * <p>
 * This is the reviewable half of composing mail: something generates a message,
 * it lands in the person's own Drafts where they can read it, change it and
 * decide, and nothing leaves the mailbox unless they say so. The returned
 * {@code webLink} opens the draft in Outlook, and the returned {@code draftId}
 * sends it from here through {@code MicrosoftOutlookSendDraft}.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.ReadWrite} for {@code POST /me/messages}</li>
 * </ul>
 *
 * <p>
 * Unlike sending, nothing here is required. A draft with no recipient or no
 * body is a normal thing to save for somebody to finish.
 * </p>
 */
public class MicrosoftOutlookSaveDraftReactor extends AbstractMicrosoftOutlookComposeReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookSaveDraftReactor.class);

	public MicrosoftOutlookSaveDraftReactor() {
		// cloned rather than shared, since keysToGet belongs to the reactor instance
		this.keysToGet = COMPOSE_KEYS.clone();
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);

			// nothing is required, since a draft is meant to be finished by hand
			ComposedMail composed = compose(false, "save a draft");
			// a null mailbox addresses /me, so the draft is saved in the signed in
			// user's own Drafts folder and nowhere else
			Map<String, Object> draft = new MicrosoftOutlookMailHelper().createDraft(accessToken, null,
					composed.message);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("saved", true);
			if (draft != null) {
				// the id is how the draft is found again, and the link is how a person
				// opens it in Outlook to decide whether it goes out
				MicrosoftOutlookMessageMapper.putIfPresent(output, "draftId", draft.get("id"));
				MicrosoftOutlookMessageMapper.putIfPresent(output, "webLink", draft.get("webLink"));
			}
			composed.describe(output);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while saving a draft for the signed in user", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to save a draft for the signed in user", e);
			throw new SemossPixelException("An error occurred saving the draft. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Save an email as a draft in the signed in user's own Microsoft 365 mailbox, for them to review and send.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TO)) {
			return "Optional recipients of the draft, which can be left for the person reviewing it to fill in.";
		} else if (key.equals(SUBJECT)) {
			return "Optional subject line of the draft.";
		} else if (key.equals(MESSAGE)) {
			return "Optional body of the draft.";
		}
		return super.getDescriptionForKey(key);
	}
}
