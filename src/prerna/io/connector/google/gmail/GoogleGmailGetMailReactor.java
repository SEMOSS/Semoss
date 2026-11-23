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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.io.connector.mail.AbstractGetMailReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailInlineImages;
import prerna.io.connector.mail.MailMessage;
import prerna.util.ValueUtils;

/**
 * Reads one message in full from the signed in user's own Gmail mailbox, with
 * what is attached to it, without marking it read.
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code gmail.readonly} or {@code gmail.modify}.
 * </p>
 */
public class GoogleGmailGetMailReactor extends AbstractGetMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected MailMessage getMail(User user, GetMailRequest request) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		Map<String, Object> message = gmail.getMessage(request.id());
		MailMessage mail = GoogleGmailMessageMapper.toMailMessage(message, accountEmail(user),
				request.includeAttachments(), request.includeDisplayBody(), request.includeReplyRecipients(), false);
		Map<String, Object> displayBody = mail.displayBody();
		if (request.readsInlineImages() && displayBody != null && "html".equals(displayBody.get("contentType"))) {
			// the display body was built for this read alone, so its pictures join it
			displayBody.putAll(MailInlineImages.collect(String.valueOf(displayBody.get("content")),
					inlinePictures(gmail, request.id(), message)));
		}
		return mail;
	}

	/**
	 * The pictures a message's body can show by content id, each read only if the
	 * body names it: from the part itself when Gmail sent its bytes along, or else
	 * with the attachment download.
	 *
	 * @param gmail     the Gmail calls
	 * @param messageId the message
	 * @param message   the message as Gmail returned it in full
	 * @return the pictures to offer {@link MailInlineImages#collect}
	 */
	private static List<MailInlineImages.Candidate> inlinePictures(GoogleGmailHelper gmail, String messageId,
			Map<String, Object> message) {
		List<MailInlineImages.Candidate> candidates = new ArrayList<>();
		for (Map<?, ?> part : GoogleGmailMessageMapper.contentIdParts(message)) {
			Map<?, ?> body = part.get("body") instanceof Map<?, ?> map ? map : Map.of();
			candidates.add(new MailInlineImages.Candidate(GoogleGmailMessageMapper.contentId(part),
					ValueUtils.toStringOrNull(part.get("mimeType")),
					body.get("size") instanceof Number size ? size.longValue() : null, () -> {
						byte[] bytes = GoogleGmailMessageMapper.inlineBytes(part);
						if (bytes == null) {
							String attachmentId = GoogleGmailMessageMapper.attachmentId(part);
							Map<String, Object> attachment = attachmentId == null ? null
									: gmail.getAttachment(messageId, attachmentId);
							if (attachment == null || attachment.get("data") == null) {
								return null;
							}
							bytes = GoogleGmailMessageMapper.decode(attachment.get("data").toString());
						}
						return new MailInlineImages.Picture(null, bytes);
					}));
		}
		return candidates;
	}
}
