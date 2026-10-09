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

import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.io.connector.mail.AbstractSaveDraftReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.OutgoingMail;

/**
 * Saves an email as a draft in the signed in user's own Gmail mailbox, for them
 * to review and send.
 *
 * <p>
 * Required Google scope, each under {@code https://www.googleapis.com/auth/}:
 * one of {@code gmail.compose} or {@code gmail.modify}.
 * </p>
 */
public class GoogleGmailSaveDraftReactor extends AbstractSaveDraftReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected ComposedMail saveDraft(User user, OutgoingMail mail) throws Exception {
		String account = accountEmail(user);
		String raw = GoogleGmailMime.write(account, mail.to(), mail.cc(), mail.bcc(), mail.subject(), mail.body(),
				mail.html(), mail.attachments(), null, null, null);
		Map<String, Object> draft = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user)).createDraft(raw,
				null);
		String threadId = GoogleGmailMessageMapper.threadOf(draft);
		return ComposedMail.of(mail, draft == null ? null : String.valueOf(draft.get("id")), threadId,
				GoogleGmailMessageMapper.webLink(account, threadId));
	}
}
