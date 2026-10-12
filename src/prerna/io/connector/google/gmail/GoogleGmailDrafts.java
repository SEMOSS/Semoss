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

import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.mail.MailMessage;

/**
 * Reads and sends Gmail drafts the same way for every reactor that sends by way
 * of one.
 */
final class GoogleGmailDrafts {

	private GoogleGmailDrafts() {

	}

	/**
	 * Read a draft, by its own id or by the id of the message it holds, since a
	 * listing of the drafts folder names messages and a draft is sent by its own
	 * id.
	 *
	 * @param gmail   the helper
	 * @param account the signed in user's address
	 * @param id      the draft, or the message it holds
	 * @return the draft as saved
	 */
	static ComposedMail read(GoogleGmailHelper gmail, String account, String id) {
		Map<String, Object> draft;
		try {
			draft = gmail.getDraft(id);
		} catch (IllegalArgumentException e) {
			String draftId = gmail.findDraftId(id);
			if (draftId == null) {
				throw new IllegalArgumentException("No draft exists in your mailbox with id: " + id);
			}
			draft = gmail.getDraft(draftId);
		}
		if (draft == null || !(draft.get("message") instanceof Map<?, ?> held)) {
			throw new IllegalArgumentException("No draft exists in your mailbox with id: " + id);
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> message = (Map<String, Object>) held;
		MailMessage parsed = GoogleGmailMessageMapper.toMailMessage(message, account, true, false, false, false);
		return new ComposedMail(String.valueOf(draft.get("id")), parsed.conversationId(), parsed.webLink(), parsed.to(),
				parsed.cc(), GoogleGmailMessageMapper.addresses(GoogleGmailMessageMapper.messageHeader(message, "Bcc")),
				parsed.subject(), parsed.body(), false,
				parsed.attachments().stream().map(attachment -> attachment.name()).toList());
	}

	/**
	 * Send a saved draft.
	 *
	 * @param gmail   the helper
	 * @param account the signed in user's address
	 * @param draft   the draft, as saved
	 * @return the message as sent, under the ids Gmail gave it
	 */
	static ComposedMail send(GoogleGmailHelper gmail, String account, ComposedMail draft) {
		Map<String, Object> sent = gmail.sendDraft(draft.id());
		String threadId = GoogleGmailMessageMapper.threadOf(sent);
		return draft.withIds(sent == null || sent.get("id") == null ? null : sent.get("id").toString(), threadId,
				GoogleGmailMessageMapper.webLink(account, threadId == null ? draft.conversationId() : threadId));
	}
}
