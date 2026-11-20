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

import java.util.Base64;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.mail.AbstractDownloadAttachmentReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailAttachmentContent;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Downloads a file attached to a message in the signed in user's own Microsoft
 * 365 mailbox into the insight folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.Read}.
 * </p>
 */
public class MicrosoftOutlookDownloadAttachmentReactor extends AbstractDownloadAttachmentReactor {

	private static final String NAME = "name";

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected MailAttachmentContent downloadAttachment(User user, String id, String attachmentId) throws Exception {
		String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
		MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
		Map<String, Object> chosen = findAttachment(helper.listAttachmentSummaries(accessToken, null, id),
				attachmentId);
		if (chosen == null) {
			return null;
		}
		if (!MicrosoftOutlookMessageMapper.isFileAttachment(chosen)) {
			throw new IllegalArgumentException("The attachment '" + chosen.get(NAME)
					+ "' is not a file. An embedded message or a link to a file in a drive has no bytes to save.");
		}
		// only the one attachment is read with its bytes
		Map<String, Object> attachment = helper.getAttachment(accessToken, null, id, String.valueOf(chosen.get("id")));
		Object contentBytes = attachment == null ? null : attachment.get("contentBytes");
		if (contentBytes == null) {
			throw new IllegalArgumentException(
					"Microsoft Graph returned no content for the attachment: " + chosen.get(NAME));
		}
		return new MailAttachmentContent(String.valueOf(chosen.get("id")), String.valueOf(chosen.get(NAME)),
				chosen.get("contentType") == null ? null : chosen.get("contentType").toString(),
				Base64.getDecoder().decode(contentBytes.toString()));
	}

	/**
	 * Finds the attachment a caller asked for among what is attached, by id or by
	 * the name read off the message.
	 *
	 * @param attachments  what is attached, without the bytes
	 * @param attachmentId the id or name asked for, or null for the only one
	 * @return the attachment, or null when there is no such thing to take
	 */
	private static Map<String, Object> findAttachment(List<Map<String, Object>> attachments, String attachmentId) {
		if (attachmentId == null) {
			return attachments.size() == 1 ? attachments.get(0) : null;
		}
		for (Map<String, Object> attachment : attachments) {
			if (attachmentId.equals(String.valueOf(attachment.get("id")))
					|| attachmentId.equalsIgnoreCase(String.valueOf(attachment.get(NAME)))) {
				return attachment;
			}
		}
		return null;
	}
}
