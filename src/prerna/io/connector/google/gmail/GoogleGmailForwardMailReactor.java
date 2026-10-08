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
import prerna.io.connector.mail.AbstractForwardMailReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.OutgoingMail;

/**
 * Forwards a message from the signed in user's own Gmail mailbox, attachments
 * and all.
 *
 * <p>
 * Gmail does not carry a message's attachments onto a forward itself, so each
 * one is read off the original and attached again, alongside whatever files the
 * user added.
 * </p>
 *
 * <p>
 * Required Google scopes, each under {@code https://www.googleapis.com/auth/}:
 * {@code gmail.modify}, or both {@code gmail.readonly} to read the message and
 * its attachments and {@code gmail.compose} to write and send the forward.
 * </p>
 */
public class GoogleGmailForwardMailReactor extends AbstractForwardMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected ComposedMail draftForward(User user, ForwardMailRequest request) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		String account = accountEmail(user);
		Map<String, Object> raw = gmail.getMessage(request.id());
		MailMessage original = GoogleGmailMessageMapper.toMailMessage(raw, account, false, false, false, false);

		List<GoogleGmailMime.CarriedFile> carried = new ArrayList<>();
		for (Map<?, ?> part : GoogleGmailMessageMapper.attachmentParts(raw)) {
			byte[] bytes = GoogleGmailMessageMapper.inlineBytes(part);
			if (bytes == null) {
				Map<String, Object> attachment = gmail.getAttachment(request.id(),
						GoogleGmailMessageMapper.attachmentId(part));
				if (attachment == null || attachment.get("data") == null) {
					continue;
				}
				bytes = GoogleGmailMessageMapper.decode(attachment.get("data").toString());
			}
			carried.add(new GoogleGmailMime.CarriedFile(String.valueOf(part.get("filename")),
					part.get("mimeType") == null ? null : part.get("mimeType").toString(), bytes));
		}

		String subject = GoogleGmailMime.forwardSubject(original.subject());
		String body = GoogleGmailMime.quoteForward(request.body(), request.html(), original,
				GoogleGmailMessageMapper.htmlBody(raw));
		String mime = GoogleGmailMime.write(account, request.to(), List.of(), List.of(), subject, body, request.html(),
				request.attachments(), carried, null, null);

		// a forward starts a thread of its own, since the people it goes to were not
		// on the one it came from
		Map<String, Object> draft = gmail.createDraft(mime, null);
		String threadId = GoogleGmailMessageMapper.threadOf(draft);
		return new ComposedMail(draft == null ? null : String.valueOf(draft.get("id")), threadId,
				GoogleGmailMessageMapper.webLink(account, threadId), request.to(), List.of(), List.of(), subject,
				request.body() == null ? "" : request.body(), request.html(),
				request.attachments().stream().map(OutgoingMail::attachmentName).toList());
	}

	@Override
	protected ComposedMail sendDraft(User user, ComposedMail draft) throws Exception {
		return GoogleGmailDrafts.send(new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user)),
				accountEmail(user), draft);
	}
}
