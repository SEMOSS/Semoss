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


import prerna.auth.User;
import prerna.collaboration.BrainDraftFormat;
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
	private final User user;

	OutlookMailProvider(User user) throws Exception {
		this.user = user;
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
			String html = BrainDraftFormat.html(user, email.message());
			draft = email.to().length > 0 || email.cc().length > 0
					? mail.replyHtmlDraft(accessToken, email.replyTo(), html, true, email.to(), email.cc())
					: mail.replyHtmlDraft(accessToken, email.replyTo(), html, true);
		} else {
			// in the owner's font and signature, from their own address so Outlook lists a sender
			String[] sender = BrainDraftFormat.sender(user);
			draft = mail.createDraft(accessToken, null, MicrosoftOutlookMailHelper.buildMessage(email.subject(),
					BrainDraftFormat.html(user, email.message()), true, email.to(), email.cc(), email.bcc(),
					sender == null ? null : sender[0], sender == null ? null : sender[1], null));
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
}
