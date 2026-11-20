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
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.mail.AbstractGetMailReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailAttachment;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.mail.MailRecipientRules;
import prerna.io.connector.mail.MailRecipients;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftMessageDisplay;

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

		List<MailAttachment> attachments = null;
		if (request.includeAttachments()) {
			attachments = new ArrayList<>();
			if (Boolean.TRUE.equals(message.get("hasAttachments"))) {
				for (Map<String, Object> attachment : helper.listAttachmentSummaries(accessToken, null, request.id())) {
					attachments.add(MicrosoftOutlookMessageMapper.toMailAttachment(attachment));
				}
			}
		}
		Map<String, Object> displayBody = request.includeDisplayBody()
				? MicrosoftMessageDisplay.body(message, MicrosoftOutlookMessageMapper.bodyOf(message))
				: null;
		MailRecipients replyRecipients = request.includeReplyRecipients()
				? MailRecipientRules.replyDefaults(MicrosoftOutlookMessageMapper.addresses(message.get("replyTo")),
						MicrosoftOutlookMessageMapper.addressOf(message.get("from")),
						MicrosoftOutlookMessageMapper.addresses(message.get("toRecipients")),
						MicrosoftOutlookMessageMapper.addresses(message.get("ccRecipients")), accountEmail(user))
				: null;
		return MicrosoftOutlookMessageMapper.toMailMessage(message, attachments, displayBody, replyRecipients);
	}
}
