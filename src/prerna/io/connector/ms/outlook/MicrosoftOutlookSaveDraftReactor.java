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

import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.mail.AbstractSaveDraftReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.OutgoingMail;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.util.ValueUtils;

/**
 * Saves an email as a draft in the signed in user's own Microsoft 365 mailbox,
 * for them to review and send.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.ReadWrite}.
 * </p>
 */
public class MicrosoftOutlookSaveDraftReactor extends AbstractSaveDraftReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected ComposedMail saveDraft(User user, OutgoingMail mail) throws Exception {
		// no from address, so the draft belongs to the mailbox the token belongs to
		Map<String, Object> message = MicrosoftOutlookMailHelper.buildMessage(mail.subject(), mail.body(), mail.html(),
				mail.toArray(), mail.ccArray(), mail.bccArray(), null, null, mail.attachmentPaths());
		// a null mailbox addresses /me, so the draft is saved in the signed in user's
		// own Drafts folder and nowhere else
		Map<String, Object> draft = new MicrosoftOutlookMailHelper()
				.createDraft(MicrosoftLoginUtils.getValidAccessToken(user), null, message);
		if (draft == null) {
			return ComposedMail.of(mail, null, null, null);
		}
		return ComposedMail.of(mail, ValueUtils.toStringOrNull(draft.get("id")),
				ValueUtils.toStringOrNull(draft.get("conversationId")),
				ValueUtils.toStringOrNull(draft.get("webLink")));
	}

}
