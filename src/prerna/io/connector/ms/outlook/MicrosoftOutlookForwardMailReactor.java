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

import prerna.auth.User;
import prerna.io.connector.mail.AbstractForwardMailReactor;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailApp;
import prerna.io.connector.mail.OutgoingMail;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * Forwards a message from the signed in user's own Microsoft 365 mailbox,
 * attachments and all.
 *
 * <p>
 * The forward is always written as a native forward draft, which carries the
 * original's attachments, and is sent from there when it is not being left in
 * Drafts.
 * </p>
 *
 * <p>
 * Required delegated Microsoft Graph scopes: {@code Mail.ReadWrite}, and
 * {@code Mail.Send} to send it.
 * </p>
 */
public class MicrosoftOutlookForwardMailReactor extends AbstractForwardMailReactor {

	@Override
	protected MailApp getMailApp() {
		return MailApp.OUTLOOK;
	}

	@Override
	protected ComposedMail draftForward(User user, ForwardMailRequest request) throws Exception {
		String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
		MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();
		// the files are read before anything is written to the mailbox, so one that
		// cannot be read leaves no draft behind
		List<Map<String, Object>> files = MicrosoftOutlookMailHelper.fileAttachments(
				request.attachments().stream().map(file -> file.getAbsolutePath()).toArray(String[]::new));
		String[] to = request.to().toArray(new String[0]);
		Map<String, Object> draft = request.html()
				? helper.forwardHtmlDraft(accessToken, request.id(), to, request.body())
				: helper.forward(accessToken, null, request.id(), to, request.body(), true);
		helper.attachToDraft(accessToken, draft, files);
		return MicrosoftOutlookMessageMapper.toComposedMail(draft, request.body() == null ? "" : request.body(),
				request.html(), request.attachments().stream().map(OutgoingMail::attachmentName).toList());
	}

	@Override
	protected ComposedMail sendDraft(User user, ComposedMail draft) throws Exception {
		new MicrosoftOutlookMailHelper().sendDraft(MicrosoftLoginUtils.getValidAccessToken(user), null, draft.id());
		// a sent draft moves to Sent Items under a new id, which Graph does not report
		return draft.withIds(null, null, null);
	}
}
