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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads one message out of the signed in user's mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Mail.Read} for {@code GET /me/messages/{id}} and for
 * {@code GET /me/messages/{id}/attachments}. {@code Mail.ReadWrite} also
 * satisfies both.</li>
 * </ul>
 *
 * <p>
 * A listing cuts bodies short and says only whether a message has attachments,
 * because reading a hundred messages in full is rarely what was wanted. This is
 * the call that reads one of them properly: the whole body, and what is
 * attached to it by name and id, which is what
 * {@code MicrosoftOutlookDownloadAttachment} takes.
 * </p>
 */
public class MicrosoftOutlookGetMailReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookGetMailReactor.class);

	private static final String INCLUDE_ATTACHMENTS = "includeAttachments";

	public MicrosoftOutlookGetMailReactor() {
		this.keysToGet = new String[] { UID, MAX_BODY_CHARS, INCLUDE_ATTACHMENTS };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String uid = requiredUid("read a message");
		int maxBodyChars = positiveInt(MAX_BODY_CHARS, DEFAULT_MAX_BODY_CHARS, Integer.MAX_VALUE);
		String includeAttachments = trimToNull(this.keyValue.get(INCLUDE_ATTACHMENTS));
		// the attachments are one more call, and somebody reading a message usually
		// wants to know what came with it, so they are described unless refused
		boolean withAttachments = includeAttachments == null || Boolean.parseBoolean(includeAttachments);

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();

			Map<String, Object> message = helper.getMessage(accessToken, null, uid);
			if (message == null) {
				throw new SemossPixelException("No message exists in your mailbox with uid: " + uid);
			}
			Map<String, Object> described = MicrosoftOutlookMessageMapper.toMessage(message, true, maxBodyChars);
			boolean hasAttachments = Boolean.TRUE.equals(message.get("hasAttachments"));
			described.put("hasAttachments", hasAttachments);

			if (withAttachments && hasAttachments) {
				List<Map<String, Object>> attachments = new ArrayList<>();
				for (Map<String, Object> attachment : helper.listAttachments(accessToken, null, uid)) {
					attachments.add(MicrosoftOutlookMessageMapper.toAttachment(attachment));
				}
				described.put("attachments", attachments);
			}
			return new NounMetadata(described, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading the message '{}'", uid, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to read a message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to read the message '{}'", uid, e);
			throw new SemossPixelException("An error occurred reading the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read one message in full from the signed in user's own Microsoft 365 mailbox, with what is attached to it.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(INCLUDE_ATTACHMENTS)) {
			return "Optional boolean for whether what is attached to the message is listed by name and id. Defaults to true.";
		}
		return super.getDescriptionForKey(key);
	}
}
