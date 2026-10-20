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
package prerna.collaboration.email;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMessageMapper;
import prerna.util.EmailUtility;
import prerna.util.EmailUtility.EmailMetadata;

/** The signed-in user's Microsoft 365 mailbox, through Graph. */
final class OutlookMailProvider implements MailProvider {

	private final MicrosoftOutlookMailHelper mail = new MicrosoftOutlookMailHelper();
	private final String accessToken;
	private final String account;

	OutlookMailProvider(User user) throws Exception {
		this.accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
		this.account = MicrosoftLoginUtils.getMicrosoftEmail(user);
	}

	@Override
	public String account() {
		return account;
	}

	@Override
	public String saveDraft(OutgoingEmail email) throws Exception {
		Map<String, Object> draft;
		if (email.isReply()) {
			// a reply-all draft keeps the thread; given lists replace the native ones
			draft = email.to().length > 0 || email.cc().length > 0
					? mail.replyHtmlDraft(accessToken, email.replyTo(), html(email.message()), true, email.to(),
							email.cc())
					: mail.reply(accessToken, null, email.replyTo(), email.message(), true, true);
		} else {
			draft = mail.createDraft(accessToken, null, MicrosoftOutlookMailHelper.buildMessage(email.subject(),
					email.message(), false, email.to(), email.cc(), email.bcc(), null, null, null));
		}
		if (draft == null || !(draft.get("id") instanceof String id) || id.isBlank()) {
			throw new IllegalStateException("The draft could not be confirmed. Check Outlook before retrying.");
		}
		return id;
	}

	@Override
	public Map<String, Object> sendDraft(String draftId) throws Exception {
		// read it first: sending moves it to Sent Items under a new id
		Map<String, Object> draft = mail.getMessage(accessToken, null, draftId);
		EmailMetadata metadata = new EmailMetadata(
				MicrosoftOutlookMessageMapper.addressArray(draft.get("toRecipients")),
				MicrosoftOutlookMessageMapper.addressArray(draft.get("ccRecipients")),
				MicrosoftOutlookMessageMapper.addressArray(draft.get("bccRecipients")), account,
				draft.get("subject") == null ? null : draft.get("subject").toString(),
				MicrosoftOutlookMessageMapper.bodyOf(draft), false, null);
		EmailUtility.sendEmail(() -> {
			mail.sendDraft(accessToken, null, draftId);
			return null;
		}, metadata);
		Map<String, Object> sent = new LinkedHashMap<>();
		MicrosoftOutlookMessageMapper.putIfPresent(sent, "to",
				MicrosoftOutlookMessageMapper.addressList(draft.get("toRecipients")));
		MicrosoftOutlookMessageMapper.putIfPresent(sent, "cc",
				MicrosoftOutlookMessageMapper.addressList(draft.get("ccRecipients")));
		MicrosoftOutlookMessageMapper.putIfPresent(sent, "subject", draft.get("subject"));
		return sent;
	}

	// plain text as HTML lines, for the reply draft that carries edited recipients
	private static String html(String text) {
		return StringEscapeUtils.escapeHtml4(text == null ? "" : text).replace("\r\n", "\n").replace("\n", "<br>");
	}
}
