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
import prerna.io.connector.mail.AbstractReplyMailReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.MailRecipientRules;
import prerna.io.connector.mail.MailRecipients;
import prerna.io.connector.mail.OutgoingMail;

/**
 * Replies to a message in the signed in user's own Gmail mailbox, keeping the
 * answer in its thread.
 *
 * <p>
 * Gmail writes neither the quoted original nor the recipients of a reply
 * itself, so both are written here the way Gmail's own app writes them, and the
 * reply carries the headers that keep it in the thread for everybody on it.
 * </p>
 *
 * <p>
 * Required Google scopes, each under {@code https://www.googleapis.com/auth/}:
 * {@code gmail.modify}, or both {@code gmail.readonly} to read the message
 * being answered and {@code gmail.compose} to write and send the reply.
 * </p>
 */
public class GoogleGmailReplyMailReactor extends AbstractReplyMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected ComposedMail draftReply(User user, ReplyMailRequest request) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		String account = accountEmail(user);
		Map<String, Object> raw = gmail.getMessage(request.id());
		MailMessage original = GoogleGmailMessageMapper.toMailMessage(raw, account, false, false, false, false);

		MailRecipients recipients = request.recipients() != null ? request.recipients()
				: recipients(raw, original, account, request.replyAll());
		String messageId = original.internetMessageId();
		String references = GoogleGmailMessageMapper.messageHeader(raw, "References");
		String subject = GoogleGmailMime.replySubject(original.subject());
		String body = GoogleGmailMime.quoteReply(request.body(), request.html(), original,
				GoogleGmailMessageMapper.htmlBody(raw));
		String mime = GoogleGmailMime.write(account, recipients.to(), recipients.cc(), List.of(), subject, body,
				request.html(), request.attachments(), null, messageId,
				references == null || messageId == null ? messageId : references + " " + messageId);

		Map<String, Object> draft = gmail.createDraft(mime, original.conversationId());
		String threadId = GoogleGmailMessageMapper.threadOf(draft);
		threadId = threadId == null ? original.conversationId() : threadId;
		return new ComposedMail(draft == null ? null : String.valueOf(draft.get("id")), threadId,
				GoogleGmailMessageMapper.webLink(account, threadId), recipients.to(), recipients.cc(), List.of(),
				subject, request.body(), request.html(),
				request.attachments().stream().map(OutgoingMail::attachmentName).toList());
	}

	@Override
	protected ComposedMail sendDraft(User user, ComposedMail draft) throws Exception {
		return GoogleGmailDrafts.send(new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user)),
				accountEmail(user), draft);
	}

	/**
	 * Who the reply goes to on its own: the sender, or for a reply to everybody,
	 * everybody on the message but the user.
	 */
	private static MailRecipients recipients(Map<String, Object> raw, MailMessage original, String account,
			boolean replyAll) {
		List<String> replyTo = GoogleGmailMessageMapper
				.addresses(GoogleGmailMessageMapper.messageHeader(raw, "Reply-To"));
		MailRecipients everybody = MailRecipientRules.replyDefaults(replyTo, original.from(), original.to(),
				original.cc(), account);
		if (replyAll) {
			return everybody;
		}
		List<String> sender = !replyTo.isEmpty() ? replyTo
				: original.from() == null ? List.of() : List.of(original.from());
		// answering a message the user sent themselves goes to who they sent it to,
		// the way Gmail's own app answers it
		boolean toSelf = sender.isEmpty() || sender.stream().allMatch(address -> address.equalsIgnoreCase(account));
		return new MailRecipients(toSelf ? everybody.to() : MailRecipientRules.validate(sender), List.of());
	}
}
