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

import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Entities;

import prerna.auth.User;
import prerna.io.connector.mail.AbstractReplyMailReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailRecipients;
import prerna.io.connector.mail.OutgoingMail;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Replies to a message in the signed in user's own Microsoft 365 mailbox,
 * keeping the answer in its thread.
 *
 * <p>
 * The reply is always written as a native reply draft, so Outlook writes the
 * quoted original and the recipients itself, and is sent from there when it is
 * not being left in Drafts. That is what lets a sent reply carry html,
 * attachments and a chosen set of recipients too.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scopes: {@code Mail.ReadWrite}, and
 * {@code Mail.Send} to send it.
 * </p>
 */
public class MicrosoftOutlookReplyMailReactor extends AbstractReplyMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected ComposedMail draftReply(User user, ReplyMailRequest request) throws Exception {
		String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
		MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
		// the files are read before anything is written to the mailbox, so one that
		// cannot be read leaves no draft behind
		List<Map<String, Object>> files = MicrosoftOutlookMailHelper.fileAttachments(
				request.attachments().stream().map(file -> file.getAbsolutePath()).toArray(String[]::new));

		Map<String, Object> draft;
		MailRecipients recipients = request.recipients();
		if (recipients != null) {
			// graph only takes chosen recipients on the formatted draft, so a plain
			// text reply is written as html that reads the same
			draft = helper.replyHtmlDraft(accessToken, request.id(), asHtml(request.body(), request.html()),
					request.replyAll(), recipients.to().toArray(new String[0]), recipients.cc().toArray(new String[0]));
		} else if (request.html()) {
			draft = helper.replyHtmlDraft(accessToken, request.id(), request.body(), request.replyAll());
		} else {
			draft = helper.reply(accessToken, null, request.id(), request.body(), request.replyAll(), true);
		}
		helper.attachToDraft(accessToken, draft, files);
		return MicrosoftOutlookMessageMapper.toComposedMail(draft, request.body(), request.html(),
				request.attachments().stream().map(OutgoingMail::attachmentName).toList());
	}

	@Override
	protected ComposedMail sendDraft(User user, ComposedMail draft) throws Exception {
		new MicrosoftOutlookMailHelper().sendDraft(MicrosoftLoginUtils.getValidAccessToken(user), null, draft.id());
		// a sent draft moves to Sent Items under a new id, which Graph does not report
		return draft.withIds(null, null, null);
	}

	/**
	 * @param body the body
	 * @param html whether it is html already
	 * @return the body as html, keeping the lines of a plain text one
	 */
	private static String asHtml(String body, boolean html) {
		if (html) {
			return body;
		}
		return Entities.escape(body == null ? "" : body).replace("\n", "<br>");
	}
}
