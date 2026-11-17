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
import java.util.Base64;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.mail.AbstractGetMailReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailAttachment;
import prerna.io.connector.mail.MailInlineImages;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.MailRecipientRules;
import prerna.io.connector.mail.MailRecipients;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftMessageDisplay;
import prerna.util.ValueUtils;

/**
 * Reads one message in full from the signed in user's own Microsoft 365
 * mailbox, with what is attached to it.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.Read}.
 * </p>
 */
public class MicrosoftOutlookGetMailReactor extends AbstractGetMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected MailMessage getMail(User user, GetMailRequest request) throws Exception {
		String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
		MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();

		Map<String, Object> message = helper.getMessage(accessToken, null, request.id());
		if (message == null) {
			throw new IllegalArgumentException("No message exists in your mailbox with id: " + request.id());
		}

		boolean hasFiles = Boolean.TRUE.equals(message.get("hasAttachments"));
		// graph leaves pictures shown in the body out of hasAttachments, so a message
		// with only those is still read for them
		List<Map<String, Object>> summaries = (request.includeAttachments() && hasFiles) || request.readsInlineImages()
				? helper.listAttachmentSummaries(accessToken, null, request.id())
				: List.of();
		List<MailAttachment> attachments = null;
		if (request.includeAttachments()) {
			attachments = new ArrayList<>();
			if (hasFiles) {
				for (Map<String, Object> attachment : summaries) {
					attachments.add(MicrosoftOutlookMessageMapper.toMailAttachment(attachment));
				}
			}
		}
		Map<String, Object> displayBody = null;
		if (request.includeDisplayBody()) {
			// the message itself was read as Outlook's text rendering, so the body as it
			// was written is read on its own
			Map<String, Object> written = helper.getMessageBody(accessToken, null, request.id());
			displayBody = MicrosoftMessageDisplay.body(
					Map.of("body", written != null && written.get("body") instanceof Map<?, ?> body ? body : Map.of()),
					MicrosoftOutlookMessageMapper.bodyOf(message));
			if (request.readsInlineImages() && "html".equals(displayBody.get("contentType"))) {
				displayBody.putAll(MailInlineImages.collect(String.valueOf(displayBody.get("content")),
						inlinePictures(helper, accessToken, request.id(), summaries)));
			}
		}
		MailRecipients replyRecipients = request.includeReplyRecipients()
				? MailRecipientRules.replyDefaults(MicrosoftOutlookMessageMapper.addresses(message.get("replyTo")),
						MicrosoftOutlookMessageMapper.addressOf(message.get("from")),
						MicrosoftOutlookMessageMapper.addresses(message.get("toRecipients")),
						MicrosoftOutlookMessageMapper.addresses(message.get("ccRecipients")), accountEmail(user))
				: null;
		return MicrosoftOutlookMessageMapper.toMailMessage(message, attachments, displayBody, replyRecipients);
	}

	/**
	 * The pictures a message shows in its body, each read only if the body names
	 * it. Graph says which content id a picture has only once it is read.
	 *
	 * @param helper      the Graph calls
	 * @param accessToken the token to read with
	 * @param messageId   the message
	 * @param summaries   what is attached to it, without the bytes
	 * @return the pictures to offer {@link MailInlineImages#collect}
	 */
	private static List<MailInlineImages.Candidate> inlinePictures(MicrosoftOutlookMailHelper helper,
			String accessToken, String messageId, List<Map<String, Object>> summaries) {
		List<MailInlineImages.Candidate> candidates = new ArrayList<>();
		for (Map<String, Object> summary : summaries) {
			String attachmentId = ValueUtils.toStringOrNull(summary.get("id"));
			if (attachmentId == null || !Boolean.TRUE.equals(summary.get("isInline"))) {
				continue;
			}
			candidates.add(new MailInlineImages.Candidate(null, ValueUtils.toStringOrNull(summary.get("contentType")),
					summary.get("size") instanceof Number size ? size.longValue() : null, () -> {
						Map<String, Object> attachment = helper.getAttachment(accessToken, null, messageId,
								attachmentId);
						if (attachment == null || !(attachment.get("contentBytes") instanceof String bytes)) {
							return null;
						}
						return new MailInlineImages.Picture(ValueUtils.toStringOrNull(attachment.get("contentId")),
								Base64.getDecoder().decode(bytes));
					}));
		}
		return candidates;
	}
}
