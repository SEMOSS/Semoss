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
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.UIDFolder;
import jakarta.mail.search.AndTerm;
import jakarta.mail.search.ComparisonTerm;
import jakarta.mail.search.FlagTerm;
import jakarta.mail.search.FromStringTerm;
import jakarta.mail.search.ReceivedDateTerm;
import jakarta.mail.search.SearchTerm;
import jakarta.mail.search.SubjectTerm;
import prerna.engine.impl.function.mail.adapter.jakarta.auth.MailStoreAuthentication;
import prerna.engine.impl.function.mail.attachment.AttachmentStore;
import prerna.engine.impl.function.mail.config.JakartaStoreConfig;
import prerna.engine.impl.function.mail.model.MailSearchCriteria;
import prerna.engine.impl.function.mail.model.MailboxActionResult;
import prerna.engine.impl.function.mail.policy.MailReadPolicy;

/**
 * Reading a mailbox over IMAP.
 * 
 * <p>
 * The protocol that can do everything the engines ask for: it searches on the
 * server, has folders, records what has been read, and names each message with
 * a uid that is still valid on a later call. That last one is what makes
 * marking, moving and deleting possible at all, since a caller has to be able
 * to say which message it means.
 */
public final class JakartaImapMailboxClient extends AbstractJakartaMailboxClient {

	private static final Logger classLogger = LogManager.getLogger(JakartaImapMailboxClient.class);
	private final boolean markAsRead;

	/**
	 * @param config          which mail server to reach and how
	 * @param authentication  how to sign in to it
	 * @param policy          how much of the mailbox may be returned
	 * @param attachmentStore where an attachment is written when one is asked for
	 * @param markAsRead      whether reading a message marks it read, which makes
	 *                        an otherwise read only engine change the mailbox
	 */
	public JakartaImapMailboxClient(JakartaStoreConfig config, MailStoreAuthentication authentication,
			MailReadPolicy policy, AttachmentStore attachmentStore, boolean markAsRead) {
		super(config, authentication, policy, attachmentStore);
		this.markAsRead = markAsRead;
	}

	/**
	 * Search on the server, which is what IMAP is for.
	 *
	 * <p>
	 * An empty search returns the folder rather than asking the server to match
	 * everything, and a single term is sent on its own rather than wrapped, since
	 * some servers handle a one term conjunction poorly.
	 */
	@Override
	protected Message[] findMessages(Folder folder, MailSearchCriteria criteria) throws MessagingException {
		if (criteria.isEmpty()) {
			return folder.getMessages();
		}
		List<SearchTerm> terms = new ArrayList<>();
		if (criteria.unreadOnly()) {
			terms.add(new FlagTerm(new Flags(Flags.Flag.SEEN), false));
		}
		if (criteria.from() != null) {
			terms.add(new FromStringTerm(criteria.from()));
		}
		if (criteria.subject() != null) {
			terms.add(new SubjectTerm(criteria.subject()));
		}
		if (criteria.since() != null) {
			terms.add(new ReceivedDateTerm(ComparisonTerm.GE, java.util.Date.from(criteria.since())));
		}
		return terms.size() == 1 ? folder.search(terms.get(0))
				: folder.search(new AndTerm(terms.toArray(SearchTerm[]::new)));
	}

	/**
	 * Read only unless the engine marks what it reads, since a folder opened for
	 * writing cannot have its flags left alone by accident.
	 */
	@Override
	protected int folderOpenMode() {
		return this.markAsRead ? Folder.READ_WRITE : Folder.READ_ONLY;
	}

	@Override
	protected void afterMessageRead(Folder folder, Message message) throws MessagingException {
		if (this.markAsRead) {
			message.setFlag(Flags.Flag.SEEN, true);
		}
	}

	@Override
	protected Object messageUid(Folder folder, Message message) throws MessagingException {
		return folder instanceof UIDFolder uidFolder ? Long.valueOf(uidFolder.getUID(message)) : null;
	}

	@Override
	protected boolean supportsFlags() {
		return true;
	}

	@Override
	public MailboxActionResult mark(String folder, List<String> messageIds, boolean read) {
		List<Long> uids = parseUids(messageIds);
		int changed = apply(folder, uids, false,
				(openFolder, messages) -> openFolder.setFlags(messages, new Flags(Flags.Flag.SEEN), read));
		return MailboxActionResult.of(changed, uids);
	}

	@Override
	public MailboxActionResult move(String folder, List<String> messageIds, String destinationFolder) {
		List<Long> uids = parseUids(messageIds);
		int changed = apply(folder, uids, true, (openFolder, messages) -> {
			Folder destination = openFolder.getStore().getFolder(destinationFolder);
			if (destination == null || !destination.exists()) {
				throw new IllegalArgumentException("The mailbox does not have a folder named " + destinationFolder);
			}
			openFolder.copyMessages(messages, destination);
			openFolder.setFlags(messages, new Flags(Flags.Flag.DELETED), true);
		});
		return MailboxActionResult.of(changed, uids);
	}

	@Override
	public MailboxActionResult delete(String folder, List<String> messageIds) {
		List<Long> uids = parseUids(messageIds);
		int changed = apply(folder, uids, true,
				(openFolder, messages) -> openFolder.setFlags(messages, new Flags(Flags.Flag.DELETED), true));
		return MailboxActionResult.of(changed, uids);
	}

	/**
	 * Open a folder for writing, do something to the named messages, and close it.
	 *
	 * <p>
	 * The three mutations differ only in what they do and whether the folder is
	 * expunged on the way out, so the opening, the lookup and the closing are
	 * shared. Uids that no longer name anything are skipped rather than failing the
	 * call, which is why the count comes from what was found rather than from what
	 * was asked for.
	 *
	 * @param folderName the folder holding the messages
	 * @param uids       the messages to act on
	 * @param expunge    whether deletions are carried out when the folder closes
	 * @param action     what to do to them
	 * @return how many messages were acted on
	 * @throws IllegalArgumentException when the mailbox cannot be changed
	 */
	private int apply(String folderName, List<Long> uids, boolean expunge, FolderAction action) {
		Folder folder = null;
		try {
			folder = openFolder(folderName, Folder.READ_WRITE);
			Message[] messages = messagesByUid(folder, uids);
			if (messages.length == 0) {
				return 0;
			}
			action.apply(folder, messages);
			return messages.length;
		} catch (MessagingException e) {
			classLogger.error("Error updating messages in {}", folderName, e);
			throw new IllegalArgumentException("Error occurred updating the mailbox. Detailed error: " + e.getMessage(),
					e);
		} finally {
			closeFolder(folder, expunge);
		}
	}

	/**
	 * Find the messages a set of uids names.
	 *
	 * <p>
	 * A uid that matches nothing comes back as a null entry, which is ordinary - it
	 * means the message was moved or deleted since the search that returned it - so
	 * those are dropped rather than treated as an error.
	 *
	 * @param folder the open folder
	 * @param uids   the messages to find
	 * @return the ones that are still there
	 * @throws MessagingException       when the folder cannot be read
	 * @throws IllegalArgumentException when the server does not use uids at all, so
	 *                                  no message can be named reliably
	 */
	private Message[] messagesByUid(Folder folder, List<Long> uids) throws MessagingException {
		if (!(folder instanceof UIDFolder uidFolder)) {
			throw new IllegalArgumentException(
					"The mail server does not identify messages by uid, so they cannot be acted on one at a time");
		}
		long[] values = uids.stream().mapToLong(Long::longValue).toArray();
		List<Message> messages = new ArrayList<>();
		Message[] found = uidFolder.getMessagesByUID(values);
		if (found != null) {
			for (Message message : found) {
				if (message != null) {
					messages.add(message);
				}
			}
		}
		return messages.toArray(Message[]::new);
	}

	/**
	 * Read the uids a caller passed back in.
	 *
	 * <p>
	 * IMAP names a message with a number, so a value that is not one did not come
	 * from this engine. The usual cause is a uid held over from an engine reading
	 * the same mailbox through Graph, which names messages with an opaque string.
	 *
	 * @param values the uids as the caller passed them
	 * @return the uids
	 * @throws IllegalArgumentException when one of them is not an IMAP uid
	 */
	private static List<Long> parseUids(List<String> values) {
		try {
			return values.stream().map(Long::valueOf).toList();
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("A message uid was not returned by an IMAP search", e);
		}
	}

	/** What to do to a set of messages in an open folder. */
	@FunctionalInterface
	private interface FolderAction {

		/**
		 * @param folder   the open folder
		 * @param messages the messages to act on
		 * @throws MessagingException when they cannot be changed
		 */
		void apply(Folder folder, Message[] messages) throws MessagingException;
	}
}
