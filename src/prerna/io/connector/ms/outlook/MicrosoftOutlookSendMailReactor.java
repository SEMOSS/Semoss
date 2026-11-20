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
import prerna.io.connector.mail.AbstractSendMailReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.OutgoingMail;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Sends mail as whoever is signed in, from their own Microsoft 365 mailbox.
 *
 * <p>
 * The mail function engines are the other way round: they hold an app
 * registration's credentials and send as a mailbox named in their SMSS, which
 * is why they carry the send policy this does not need.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.Send}. Graph accepts
 * about 4MB on a single send, counted on the whole encoded request rather than
 * on any one file.
 * </p>
 */
public class MicrosoftOutlookSendMailReactor extends AbstractSendMailReactor {

	public MicrosoftOutlookSendMailReactor() {
		// only Outlook can send without keeping a copy; Gmail always keeps one
		super(SAVE_TO_SENT_ITEMS);
	}

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected ComposedMail sendMail(User user, OutgoingMail mail) throws Exception {
		// keeping the copy is the default because the sent mail is the user's own
		// record of what went out under their name
		boolean keepCopy = readBoolean(SAVE_TO_SENT_ITEMS, true);
		Map<String, Object> message = MicrosoftOutlookMailHelper.buildMessage(mail.subject(), mail.body(), mail.html(),
				mail.toArray(), mail.ccArray(), mail.bccArray(), null, null, mail.attachmentPaths());
		// a null mailbox addresses /me, so this sends as the signed in user
		new MicrosoftOutlookMailHelper().sendMail(MicrosoftLoginUtils.getValidAccessToken(user), null, message,
				keepCopy);
		// sendMail answers with nothing, so the sent message has no id to report
		return ComposedMail.of(mail, null, null, null);
	}
}
