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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.io.connector.mail.AbstractDownloadAttachmentReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailAttachmentContent;

/**
 * Downloads a file attached to a message in the signed in user's own Gmail
 * mailbox into the insight folder.
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code gmail.readonly} or {@code gmail.modify}.
 * </p>
 */
public class GoogleGmailDownloadAttachmentReactor extends AbstractDownloadAttachmentReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected MailAttachmentContent downloadAttachment(User user, String id, String attachmentId) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		List<Map<?, ?>> parts = GoogleGmailMessageMapper.attachmentParts(gmail.getMessage(id));
		Map<?, ?> part = null;
		if (attachmentId == null) {
			part = parts.size() == 1 ? parts.get(0) : null;
		} else {
			for (Map<?, ?> candidate : parts) {
				if (attachmentId.equals(GoogleGmailMessageMapper.partId(candidate))
						|| attachmentId.equals(GoogleGmailMessageMapper.attachmentId(candidate))
						|| attachmentId.equalsIgnoreCase(String.valueOf(candidate.get("filename")))) {
					part = candidate;
					break;
				}
			}
		}
		if (part == null) {
			return null;
		}

		byte[] bytes = GoogleGmailMessageMapper.inlineBytes(part);
		if (bytes == null) {
			// the attachment id from this read of the message, since Gmail hands out a
			// new one on every read
			Map<String, Object> attachment = gmail.getAttachment(id, GoogleGmailMessageMapper.attachmentId(part));
			if (attachment == null || attachment.get("data") == null) {
				throw new IllegalArgumentException(
						"Gmail returned no content for the attachment: " + part.get("filename"));
			}
			bytes = GoogleGmailMessageMapper.decode(attachment.get("data").toString());
		}
		return new MailAttachmentContent(GoogleGmailMessageMapper.partId(part), String.valueOf(part.get("filename")),
				part.get("mimeType") == null ? null : part.get("mimeType").toString(), bytes);
	}
}
