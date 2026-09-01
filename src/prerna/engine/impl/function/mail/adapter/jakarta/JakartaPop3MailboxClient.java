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
package prerna.engine.impl.function.mail.adapter.jakarta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.model.MailSearchCriteria;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;
import prerna.engine.impl.function.mail.spi.MailStoreAuthentication;

/**
 * Reading a mailbox over POP3.
 *
 * <p>
 * POP3 offers one folder, no server side search, no record of what has been
 * read, and no id that survives the connection. Everything the engines ask for
 * beyond listing a mailbox is therefore either done here in memory or not done
 * at all, and the base class already refuses the mutations by default.
 *
 * <p>
 * Choose this only for a mailbox where POP3 is what is on offer. Anywhere IMAP
 * is available it is the better engine, and on Microsoft 365 the two are the
 * same API underneath.
 */
public final class JakartaPop3MailboxClient extends AbstractJakartaMailboxClient {

	private static final Logger classLogger = LogManager.getLogger(JakartaPop3MailboxClient.class);

	/**
	 * @param config          which mail server to reach and how
	 * @param authentication  how to sign in to it
	 * @param policy          how much of the mailbox may be returned
	 * @param attachmentStore where an attachment is written when one is asked for
	 */
	public JakartaPop3MailboxClient(JakartaStoreConfig config, MailStoreAuthentication authentication,
			MailReadPolicy policy, AttachmentStore attachmentStore) {
		super(config, authentication, policy, attachmentStore);
	}

	/**
	 * Match in memory, since there is no server to ask.
	 *
	 * <p>
	 * The walk runs newest first and stops at the engine's own ceiling, so a large
	 * mailbox is not read end to end to answer for a handful of messages. Asking
	 * for unread only is warned about rather than refused: POP3 keeps no read
	 * state, so there is nothing to filter on, and the caller gets everything.
	 */
	@Override
	protected Message[] findMessages(Folder folder, MailSearchCriteria criteria) throws MessagingException {
		if (criteria.unreadOnly()) {
			classLogger.warn("POP3 does not record which messages have been read, so unreadOnly is ignored");
		}
		Message[] all = folder.getMessages();
		if (all == null || all.length == 0 || criteria.isEmpty()) {
			return all;
		}
		List<Message> matches = new ArrayList<>();
		for (int i = all.length - 1; i >= 0 && matches.size() < this.policy.maxMessages(); i--) {
			if (matches(all[i], criteria)) {
				matches.add(all[i]);
			}
		}
		Collections.reverse(matches);
		return matches.toArray(Message[]::new);
	}

	/**
	 * Whether one message matches what was asked for.
	 *
	 * <p>
	 * The date falls back to when the message was received, because a message
	 * without a sent date is not necessarily outside the window somebody asked
	 * about. Text is matched case insensitively on a substring, which is roughly
	 * what an IMAP server does for the same terms.
	 *
	 * @param message  the message to check
	 * @param criteria what to match on
	 * @return true when it matches everything asked for
	 * @throws MessagingException when the message cannot be read
	 */
	private boolean matches(Message message, MailSearchCriteria criteria) throws MessagingException {
		if (criteria.since() != null) {
			Date sent = message.getSentDate() == null ? message.getReceivedDate() : message.getSentDate();
			if (sent == null || sent.toInstant().isBefore(criteria.since())) {
				return false;
			}
		}
		if (criteria.subject() != null
				&& (message.getSubject() == null || !contains(message.getSubject(), criteria.subject()))) {
			return false;
		}
		String from = JakartaMessageMapper.joinAddresses(message.getFrom());
		return criteria.from() == null || (from != null && contains(from, criteria.from()));
	}

	/**
	 * @param value    what the message says
	 * @param expected what the caller asked for
	 * @return true when the one contains the other, whatever case either is in
	 */
	private static boolean contains(String value, String expected) {
		return value.toLowerCase().contains(expected.toLowerCase());
	}
}
