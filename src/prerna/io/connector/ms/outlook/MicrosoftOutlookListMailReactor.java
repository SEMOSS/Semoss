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

import java.util.Date;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.mail.AbstractListMailReactor;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.MailMessage;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Reads the mail of the signed in user's own Microsoft 365 mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scope: {@code Mail.Read}.
 * </p>
 */
public class MicrosoftOutlookListMailReactor extends AbstractListMailReactor {

	/** The most messages Graph returns on one page of a listing. */
	private static final int MAX_TOP = 1000;

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected ConnectorPage<MailMessage> listMail(User user, ListMailRequest request) throws Exception {
		String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
		MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
		MicrosoftOutlookMailHelper.MessageQuery query = new MicrosoftOutlookMailHelper.MessageQuery();
		// graph reads a folder by well known name or by id, so a folder somebody
		// named is looked up; a thread is read from every folder
		query.folder = request.conversationId() == null ? helper.resolveDestination(accessToken, null, request.folder())
				: request.folder();
		query.subject = request.subject();
		query.from = request.from();
		query.unreadOnly = request.unreadOnly();
		query.since = request.since() == null ? null : Date.from(request.since());
		query.includeBody = request.includeBody();
		query.conversationId = request.conversationId();

		// graph skips on the server, except while searching text or reading a
		// thread, which read from the first message and are cut to the page here
		boolean serverSkips = request.conversationId() == null && !request.searches();
		if (serverSkips) {
			query.skip = request.offset();
			query.top = request.limit() + 1;
		} else {
			query.top = Math.min(request.needed(), MAX_TOP);
		}

		// a null mailbox addresses /me, so the signed in user is the only mailbox
		// this reactor is able to read
		List<Map<String, Object>> found = helper.listMessages(accessToken, null, query);
		List<MailMessage> messages = found.stream()
				.map(message -> MicrosoftOutlookMessageMapper.toMailMessage(message, null, null, null)).toList();
		return serverSkips ? ConnectorPage.ofWindow(messages, request.limit())
				: ConnectorPage.of(messages, request.offset(), request.limit());
	}
}
