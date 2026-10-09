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
import java.util.stream.Stream;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.io.connector.mail.AbstractMoveMailReactor;
import prerna.io.connector.mail.MailApp;

/**
 * Moves a message into another place of the signed in user's own Gmail mailbox,
 * which in Gmail is a change of labels. The message keeps its id.
 *
 * <p>
 * Required Google scope, under {@code https://www.googleapis.com/auth/}:
 * {@code gmail.modify}.
 * </p>
 */
public class GoogleGmailMoveMailReactor extends AbstractMoveMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.GMAIL;
	}

	@Override
	protected String moveMail(User user, String id, String folder) throws Exception {
		GoogleGmailHelper gmail = new GoogleGmailHelper(GoogleLoginUtils.getValidAccessToken(user));
		GoogleGmailFolders.Target target = GoogleGmailFolders.resolve(gmail, folder);
		if (GoogleGmailFolders.TRASH.equals(target.labelId())) {
			gmail.trash(id);
			return id;
		}

		List<String> labels = GoogleGmailMessageMapper.labelIds(gmail.getMessageLabels(id));
		if (labels.contains(GoogleGmailFolders.TRASH)) {
			// the trash is left by restoring, which a change of labels cannot do
			gmail.untrash(id);
		}
		if (target.archive()) {
			gmail.modify(id, List.of(), List.of(GoogleGmailFolders.INBOX));
			return id;
		}
		// a message moved somewhere leaves the inbox or spam it was in, the way moving
		// between folders does
		List<String> remove = Stream.of(GoogleGmailFolders.INBOX, GoogleGmailFolders.SPAM)
				.filter(label -> !label.equals(target.labelId()) && labels.contains(label)).toList();
		gmail.modify(id, List.of(target.labelId()), remove);
		return id;
	}
}
